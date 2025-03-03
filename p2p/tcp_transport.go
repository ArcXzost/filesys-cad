package p2p

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// This represents a remote node on a TCP connection
type TCPPeer struct {
	// the underlying connection of the peer
	net.Conn
	mu sync.Mutex
	// storageRoot string
	// if we dial a connection => outbound = true
	// if we accept a connection => outbound = false
	outbound    bool
	listenAddr  string
	wg          *sync.WaitGroup
	lastSeen    time.Time
	isAlive     bool
	healthMutex sync.RWMutex
}

func (p *TCPPeer) Write(data []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.Conn.Write(data)
}

func NewTCPPeer(conn net.Conn, outbound bool, listenAddr string) *TCPPeer {
	return &TCPPeer{
		Conn:       conn,
		outbound:   outbound,
		listenAddr: listenAddr,
		// storageRoot: storageRoot,
	}
}

func (p *TCPPeer) CloseStream() {
	if p.wg != nil {
		p.wg.Done()
	}
}

// Send function writes bytes to the connection for the other
// peer to read
func (t *TCPPeer) Send(b []byte) error {
	_, err := t.Conn.Write(b)
	if err != nil {
		log.Printf("[%s] Failed to send data: %v\n", t.ListenAddr(), err)
		return err
	}
	return nil
}

type TCPTransportOpts struct {
	ListenAddr    string
	HandshakeFunc HandshakeFunc
	Decoder       Decoder
	OnPeer        func(Peer) error
}

type TCPTransport struct {
	TCPTransportOpts
	listener net.Listener
	rpcch    chan RPC
}

func NewTCPTransport(opts TCPTransportOpts) *TCPTransport {
	return &TCPTransport{
		TCPTransportOpts: opts,
		rpcch:            make(chan RPC, 1024),
	}
}

func (t *TCPTransport) Addr() string {
	return t.ListenAddr
}

func (p *TCPPeer) SetListenAddr(addr string) {
	p.listenAddr = addr
}

func (p *TCPPeer) ListenAddr() string {
	return p.listenAddr
}

// Consume returns a read-only channel for reading incoming
// messages from another peer in the network
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcch
}

// Close implements the Transport interface
func (t *TCPTransport) Close() error {
	return t.listener.Close()
}

// Dial implements the Transport interface
func (t *TCPTransport) Dial(addr string) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	go t.handleConn(conn, true)
	return nil
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error
	t.listener, err = net.Listen("tcp", t.ListenAddr)
	if err != nil {
		return err
	}
	go t.startAcceptLoop()
	log.Printf("TCP transport listening on port: %s\n", t.ListenAddr)
	return nil
}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			fmt.Println("TCP Accept error", err)
		}
		go t.handleConn(conn, false)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	var peer *TCPPeer

	if outbound {
		// Outbound peer: Send our listen address to the remote peer
		NewTCPPeer(conn, outbound, t.ListenAddr)
		if _, err := conn.Write([]byte(t.ListenAddr + "\n")); err != nil {
			log.Printf("Failed to send listen address: %v", err)
			conn.Close()
		}
		return
	} else {
		// Inbound peer: Read the remote peer's listen address
		addr, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			log.Printf("Failed to read peer's listen address: %v", err)
			conn.Close()
			return
		}
		addr = strings.TrimSpace(addr)
		peer = NewTCPPeer(conn, outbound, addr)
	}

	// Trigger OnPeer callback with the peer's listen address
	if t.OnPeer != nil {
		if err := t.OnPeer(peer); err != nil {
			log.Printf("Error handling peer: %v", err)
			conn.Close()
			return
		}
	}

	// Read loop to keep the connection alive
	for {
		rpc := RPC{}
		if err := t.Decoder.Decode(conn, &rpc); err != nil {
			log.Printf("Decode error: %v", err)
			break
		}
		t.rpcch <- rpc // Send the decoded RPC to the channel
	}
}

func (p *TCPPeer) MarkAlive() {
	p.healthMutex.Lock()
	defer p.healthMutex.Unlock()
	p.lastSeen = time.Now()
	p.isAlive = true
}

func (p *TCPPeer) IsAlive() bool {
	p.healthMutex.RLock()
	defer p.healthMutex.RUnlock()
	return p.isAlive && time.Since(p.lastSeen) < 2*time.Minute
}
