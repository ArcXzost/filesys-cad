package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"filesys-cad/crypto"
	"filesys-cad/p2p"
	"filesys-cad/store"
)

type FileServerOpts struct {
	ID                string
	EncKey            []byte
	StorageRoot       string
	PathTransformFunc store.PathTransformFunc
	Transport         p2p.Transport
	BootstrapNodes    []string
}

type FileServer struct {
	FileServerOpts

	peerLock       sync.RWMutex // Use RWMutex for better performance
	peers          map[string]p2p.Peer
	peerServers    map[string]*FileServer // Map of peer addresses to FileServer instances
	store          *store.Store
	metadataStore  *store.MetadataStore // Add metadata store
	quitch         chan struct{}
	healthMessages chan string
}

func NewFileServer(opts FileServerOpts) *FileServer {
	storeOpts := store.StoreOpts{
		Root:              opts.StorageRoot,
		PathTransformFunc: opts.PathTransformFunc,
	}

	if len(opts.ID) == 0 {
		opts.ID = crypto.GenerateID()
	}

	// Ensure the storage root directory exists for this peer
	if err := os.MkdirAll(opts.StorageRoot, os.ModePerm); err != nil {
		log.Fatalf("Failed to create storage root directory for peer: %v", err)
	}

	return &FileServer{
		FileServerOpts: opts,
		store:          store.NewStore(storeOpts),
		metadataStore:  store.NewMetadataStore(),
		quitch:         make(chan struct{}),
		peers:          make(map[string]p2p.Peer),
		peerServers:    make(map[string]*FileServer),
		healthMessages: make(chan string, 100),
	}
}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	ID   string
	Key  string
	Size int64
}

type MessageGetFile struct {
	ID  string
	Key string
}

type MessageDeleteFile struct {
	ID  string
	Key string
}

type readCloser struct {
	io.Reader
}

func (r readCloser) Close() error { return nil }

func (s *FileServer) Get(key, versionID string) (io.ReadCloser, error) {
	// Check if the file exists locally
	if s.store.Has(s.ID, key) {
		log.Printf("[%s] serving file (%s) from local disk\n", s.Transport.Addr(), key)
		_, r, err := s.store.Read(s.EncKey, s.ID, key)
		return readCloser{r}, err
	}

	// Get metadata for the file
	s.peerLock.RLock()
	metadata, exists := s.metadataStore.GetMetadataForFile(key)
	s.peerLock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no metadata found for key: %s", key)
	}

	responsiblePeerAddr := metadata.ResponsiblePeer
	log.Printf("[%s] responsible peer for key %s: %s\n", s.Transport.Addr(), key, responsiblePeerAddr)

	// Get the responsible peer's FileServer instance
	s.peerLock.RLock()
	responsibleFileServer, exists := s.peerServers[responsiblePeerAddr]
	s.peerLock.RUnlock()
	if !exists {
		return nil, fmt.Errorf("no FileServer instance found for responsible peer: %s", responsiblePeerAddr)
	}

	// If versionID is specified, get that specific version
	if versionID != "" {
		versions, exists := s.metadataStore.GetVersions(key)
		if !exists {
			return nil, fmt.Errorf("no versions found for key: %s", key)
		}

		var versionPath string
		for _, v := range versions {
			if v.VersionID == versionID {
				versionPath = fmt.Sprintf("%s_%s", key, v.VersionID)
				break
			}
		}

		if versionPath == "" {
			return nil, fmt.Errorf("version %s not found for key: %s", versionID, key)
		}

		// Read the specific version
		_, r, err := responsibleFileServer.store.Read(s.EncKey, s.ID, versionPath)
		return readCloser{r}, err
	}

	// Read from the responsible peer's store
	_, r, err := responsibleFileServer.store.Read(s.EncKey, s.ID, key)
	if err != nil {
		return nil, fmt.Errorf("failed to read from responsible peer: %v", err)
	}

	return readCloser{r}, nil
}

func (s *FileServer) getPeerByAddr(addr string) p2p.Peer {
	s.peerLock.RLock()
	defer s.peerLock.RUnlock()
	for _, peer := range s.peers {
		if peer.(*p2p.TCPPeer).ListenAddr() == addr {
			return peer
		}
	}
	return nil
}
func (s *FileServer) Store(key string, r io.Reader) error {
	// Check if file already exists
	metadata, exists := s.metadataStore.GetMetadataForFile(key)
	if exists {
		// File exists, create new version
		return s.storeNewVersion(key, r, metadata)
	}

	// New file, proceed with normal storage
	return s.storeNewFile(key, r)
}

func (s *FileServer) storeNewFile(key string, r io.Reader) error {
	// Determine the responsible peer using consistent hashing
	s.peerLock.RLock()
	responsiblePeer := s.getResponsiblePeer(key)
	s.peerLock.RUnlock()

	if responsiblePeer == nil {
		return fmt.Errorf("no responsible peer found for key: %s", key)
	}

	// Read the entire file data into a buffer
	fileBuffer := new(bytes.Buffer)
	if _, err := io.Copy(fileBuffer, r); err != nil {
		return fmt.Errorf("failed to read file data into buffer: %v", err)
	}

	// Write to the responsible peer's store and get the file size
	s.peerLock.RLock()
	responsibleFileServer, exists := s.peerServers[responsiblePeer.(*p2p.TCPPeer).ListenAddr()]
	s.peerLock.RUnlock()
	if !exists {
		return fmt.Errorf("no FileServer instance found for responsible peer: %s", responsiblePeer.RemoteAddr().String())
	}

	// Create a new reader from the buffer for the responsible peer
	responsibleReader := bytes.NewReader(fileBuffer.Bytes())
	fileSize, err := responsibleFileServer.store.WriteEncrypt(
		s.EncKey, // Use the encryption key
		s.ID,
		key,
		responsibleReader,
	)
	if err != nil {
		return fmt.Errorf("failed to write to responsible peer's store: %v", err)
	}
	log.Printf("[%s] File size: %d bytes\n", s.Transport.Addr(), fileSize-16)

	// Send the file to the responsible peer
	if err := s.sendFileToPeer(responsiblePeer, key, bytes.NewReader(fileBuffer.Bytes()), fileSize); err != nil {
		return err
	}

	// Replicate the file to replica peers
	s.peerLock.RLock()
	replicaPeers := s.getReplicaPeers(key, 2, responsiblePeer.(*p2p.TCPPeer).ListenAddr())
	s.peerLock.RUnlock()

	for _, peer := range replicaPeers {
		if peer.(*p2p.TCPPeer).ListenAddr() == s.Transport.Addr() {
			continue // Skip self
		}

		// Write to the replica's store from the buffer
		s.peerLock.RLock()
		replicaFileServer, exists := s.peerServers[peer.(*p2p.TCPPeer).ListenAddr()]
		s.peerLock.RUnlock()
		if !exists {
			return fmt.Errorf("no FileServer instance found for replica peer: %s", peer.RemoteAddr().String())
		}

		// Create a new reader from the buffer for the replica peer
		replicaReader := bytes.NewReader(fileBuffer.Bytes())
		if _, err := replicaFileServer.store.WriteEncrypt(
			s.EncKey, // Use the encryption key
			s.ID,
			key,
			replicaReader,
		); err != nil {
			return fmt.Errorf("failed to write to replica peer's store: %v", err)
		}

		// Send the file to the replica peer
		if err := s.sendFileToPeer(peer, key, bytes.NewReader(fileBuffer.Bytes()), fileSize); err != nil {
			return err
		}
	}

	// Update metadata store
	replicaPeerAddrs := make([]string, len(replicaPeers))
	for i, peer := range replicaPeers {
		replicaPeerAddrs[i] = peer.(*p2p.TCPPeer).ListenAddr()
	}
	s.metadataStore.AddFile(key, responsiblePeer.(*p2p.TCPPeer).ListenAddr(), replicaPeerAddrs)

	// Calculate file hash
	hash := sha256.New()
	if _, err := io.Copy(hash, bytes.NewReader(fileBuffer.Bytes())); err != nil {
		return fmt.Errorf("failed to calculate file hash: %v", err)
	}
	fileHash := hex.EncodeToString(hash.Sum(nil))

	// Get existing metadata
	metadata, exists := s.metadataStore.GetMetadataForFile(key)
	if !exists {
		metadata = store.FileMetadata{
			Key:             key,
			ResponsiblePeer: responsiblePeer.(*p2p.TCPPeer).ListenAddr(),
			ReplicaPeers:    replicaPeerAddrs,
			Versions:        []store.FileVersion{},
		}
	}

	// Add new version
	newVersion := store.FileVersion{
		VersionID: crypto.GenerateID(),
		Timestamp: time.Now(),
		Size:      fileSize,
		Hash:      fileHash,
	}
	metadata.Versions = append(metadata.Versions, newVersion)

	// Update metadata
	s.metadataStore.AddFile(key, metadata.ResponsiblePeer, metadata.ReplicaPeers)
	s.metadataStore.UpdateVersions(key, metadata.Versions)

	return nil
}

func (s *FileServer) storeNewVersion(key string, r io.Reader, metadata store.FileMetadata) error {
	// Read the entire file data into a buffer
	fileBuffer := new(bytes.Buffer)
	if _, err := io.Copy(fileBuffer, r); err != nil {
		return fmt.Errorf("failed to read file data into buffer: %v", err)
	}

	// Calculate file hash
	hash := sha256.New()
	if _, err := io.Copy(hash, bytes.NewReader(fileBuffer.Bytes())); err != nil {
		return fmt.Errorf("failed to calculate file hash: %v", err)
	}
	fileHash := hex.EncodeToString(hash.Sum(nil))

	// Create new version
	newVersion := store.FileVersion{
		VersionID: crypto.GenerateID(),
		Timestamp: time.Now(),
		Size:      int64(fileBuffer.Len()),
		Hash:      fileHash,
	}

	// Store the new version
	responsibleFileServer, exists := s.peerServers[metadata.ResponsiblePeer]
	if !exists {
		return fmt.Errorf("no FileServer instance found for responsible peer: %s", metadata.ResponsiblePeer)
	}

	_, err := responsibleFileServer.store.WriteEncrypt(
		s.EncKey,
		s.ID,
		key,
		bytes.NewReader(fileBuffer.Bytes()),
	)
	if err != nil {
		return fmt.Errorf("failed to store new version: %v", err)
	}

	// Update metadata with new version
	metadata.Versions = append(metadata.Versions, newVersion)
	s.metadataStore.UpdateVersions(key, metadata.Versions)

	return nil
}

func (s *FileServer) sendFileToPeer(peer p2p.Peer, key string, fileData io.Reader, fileSize int64) error {
	// Prepare the message
	msg := Message{
		Payload: MessageStoreFile{
			ID:   s.ID,
			Key:  crypto.HashKey(key),
			Size: fileSize,
		},
	}

	// Encode the message
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}

	// Send the message
	if err := peer.Send([]byte{p2p.IncomingMessage}); err != nil {
		return fmt.Errorf("failed to send IncomingMessage to peer: %v", err)
	}
	if err := peer.Send(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to send message to peer: %v", err)
	}

	// Send the IncomingStream byte
	mw := io.Writer(peer)
	if _, err := mw.Write([]byte{p2p.IncomingStream}); err != nil {
		return fmt.Errorf("failed to send IncomingStream byte: %v", err)
	}

	// Stream encrypted data
	n, err := crypto.CopyEncrypt(s.EncKey, fileData, mw)
	if err != nil {
		return fmt.Errorf("failed to stream file to peer: %v", err)
	}

	log.Printf("[%s] Sent file (%s) to peer [%s] with size %d bytes", s.Transport.Addr(), key, peer.(*p2p.TCPPeer).ListenAddr(), n)
	return nil
}

func (s *FileServer) getResponsiblePeer(key string) p2p.Peer {
	hash := sha256.Sum256([]byte(key))
	hashInt := new(big.Int).SetBytes(hash[:])

	var responsiblePeer p2p.Peer
	minDistance := new(big.Int).SetInt64(math.MaxInt64)

	// Lock the peer map for reading
	s.peerLock.RLock()
	defer s.peerLock.RUnlock()

	if len(s.peers) == 0 {
		log.Printf("No peers available")
		return nil
	}

	// Log the peers in the map
	log.Printf("Peers in the map:")
	for addr := range s.peers {
		log.Printf("- Peer: %s", addr)
	}

	for addr, peer := range s.peers {
		peerHash := sha256.Sum256([]byte(addr))
		peerHashInt := new(big.Int).SetBytes(peerHash[:])
		distance := new(big.Int).Abs(new(big.Int).Sub(hashInt, peerHashInt))

		// Handle wrap-around case in consistent hashing ring
		if distance.Cmp(minDistance) < 0 || responsiblePeer == nil {
			minDistance = distance
			responsiblePeer = peer
		}
	}

	if responsiblePeer != nil {
		log.Printf("Responsible peer for key %s: %s\n", key, responsiblePeer.(*p2p.TCPPeer).ListenAddr())
	} else {
		log.Printf("No responsible peer found for key %s\n", key)
	}

	return responsiblePeer
}

func (s *FileServer) getReplicaPeers(key string, numReplicas int, responsiblePeerAddr string) []p2p.Peer {
	hash := sha256.Sum256([]byte(key))
	hashInt := new(big.Int).SetBytes(hash[:])

	s.peerLock.RLock()
	defer s.peerLock.RUnlock()

	// Sort peers by their hash distance to the key
	peers := make([]p2p.Peer, 0, len(s.peers))
	for addr, peer := range s.peers {
		if addr != responsiblePeerAddr {
			peers = append(peers, peer)
		}
	}

	sort.Slice(peers, func(i, j int) bool {
		peerHashI := sha256.Sum256([]byte(peers[i].RemoteAddr().String()))
		peerHashIntI := new(big.Int).SetBytes(peerHashI[:])
		distanceI := new(big.Int).Abs(new(big.Int).Sub(hashInt, peerHashIntI))

		peerHashJ := sha256.Sum256([]byte(peers[j].RemoteAddr().String()))
		peerHashIntJ := new(big.Int).SetBytes(peerHashJ[:])
		distanceJ := new(big.Int).Abs(new(big.Int).Sub(hashInt, peerHashIntJ))

		return distanceI.Cmp(distanceJ) < 0
	})

	// Select the next `numReplicas` peers as replicas
	if len(peers) > numReplicas {
		return peers[:numReplicas]
	}
	return peers
}

func (s *FileServer) redistributeFiles(unhealthyPeerAddr string) {
	// Get all files stored on the unhealthy peer
	files := s.getFilesForPeer(unhealthyPeerAddr)
	if len(files) == 0 {
		return
	}

	// Redistribute each file to a new responsible peer
	for _, file := range files {
		// Determine the new responsible peer for the file
		newResponsiblePeer := s.getResponsiblePeer(file.Key)
		if newResponsiblePeer == nil {
			log.Printf("No responsible peer found for file: %s\n", file.Key)
			continue
		}

		// Fetch the file from the unhealthy peer (if possible)
		fileData, err := s.fetchFileFromPeer(unhealthyPeerAddr, file.Key)
		if err != nil {
			log.Printf("Failed to fetch file %s from unhealthy peer %s: %v\n", file.Key, unhealthyPeerAddr, err)
			continue
		}

		// Store the file on the new responsible peer
		if err := newResponsiblePeer.Send([]byte{p2p.IncomingMessage}); err != nil {
			log.Printf("Failed to send request to new responsible peer %s: %v\n", newResponsiblePeer.RemoteAddr().String(), err)
			continue
		}

		msg := Message{
			Payload: MessageStoreFile{
				ID:   s.ID,
				Key:  crypto.HashKey(file.Key),
				Size: int64(len(fileData)),
			},
		}
		if err := gob.NewEncoder(newResponsiblePeer).Encode(msg); err != nil {
			log.Printf("Failed to encode message for new responsible peer %s: %v\n", newResponsiblePeer.RemoteAddr().String(), err)
			continue
		}

		// Stream the file to the new responsible peer
		if _, err := newResponsiblePeer.Write(fileData); err != nil {
			log.Printf("Failed to stream file to new responsible peer %s: %v\n", newResponsiblePeer.RemoteAddr().String(), err)
			continue
		}

		// Update the metadata store with the new responsible peer
		s.metadataStore.AddFile(file.Key, newResponsiblePeer.RemoteAddr().String(), file.ReplicaPeers)

		log.Printf("Redistributed file %s from unhealthy peer %s to new responsible peer %s\n", file.Key, unhealthyPeerAddr, newResponsiblePeer.RemoteAddr().String())
	}
}

func (s *FileServer) getFilesForPeer(peerAddr string) []store.FileMetadata {
	// Query the metadata store for files stored on the given peer
	fileKeys := s.metadataStore.GetFilesForPeer(peerAddr)

	// Convert file keys to FileMetadata structs
	var files []store.FileMetadata
	for _, key := range fileKeys {
		metadata, exists := s.metadataStore.GetMetadataForFile(key)
		if exists {
			files = append(files, metadata)
		}
	}

	return files
}

func (s *FileServer) fetchFileFromPeer(peerAddr, key string) ([]byte, error) {
	// Find the peer by address
	peer, ok := s.peers[peerAddr]
	if !ok {
		return nil, fmt.Errorf("peer %s not found", peerAddr)
	}

	// Send a MessageGetFile request to the peer
	msg := Message{
		Payload: MessageGetFile{
			ID:  s.ID,
			Key: crypto.HashKey(key),
		},
	}

	// Send the request to the peer
	if err := peer.Send([]byte{p2p.IncomingMessage}); err != nil {
		return nil, fmt.Errorf("failed to send request to peer: %v", err)
	}
	if err := gob.NewEncoder(peer).Encode(msg); err != nil {
		return nil, fmt.Errorf("failed to encode message: %v", err)
	}

	// Read the file size from the peer
	var fileSize int64
	if err := binary.Read(peer, binary.LittleEndian, &fileSize); err != nil {
		return nil, fmt.Errorf("failed to read file size: %v", err)
	}

	// Read the file data from the peer
	fileData := make([]byte, fileSize)
	if _, err := io.ReadFull(peer, fileData); err != nil {
		return nil, fmt.Errorf("failed to read file data: %v", err)
	}

	return fileData, nil
}

/*
Delete will delete the specified key in the current node
and across all other nodes in the network. It sends the
current node's ID and the file key to be deleted and waits
for a response from all the other nodes - whether the deletion
was successful or not.
*/
func (s *FileServer) Delete(key string) error {
	// Get the responsible peer
	s.peerLock.RLock()
	metadata, exists := s.metadataStore.GetMetadataForFile(key)
	s.peerLock.RUnlock()
	if !exists {
		return fmt.Errorf("no responsible peer found for key: %s", key)
	}

	// Delete from responsible peer
	s.peerLock.RLock()
	responsibleFileServer, exists := s.peerServers[metadata.ResponsiblePeer]
	s.peerLock.RUnlock()
	if !exists {
		return fmt.Errorf("no FileServer instance found for responsible peer: %s", metadata.ResponsiblePeer)
	}

	if err := responsibleFileServer.store.Delete(s.ID, key); err != nil {
		return fmt.Errorf("failed to delete data from responsible peer: %v", err)
	}

	// Delete from replica peers
	s.peerLock.RLock()
	replicaPeers := s.getReplicaPeers(key, 2, metadata.ResponsiblePeer)
	s.peerLock.RUnlock()

	for _, peer := range replicaPeers {
		s.peerLock.RLock()
		replicaFileServer, exists := s.peerServers[peer.(*p2p.TCPPeer).ListenAddr()]
		s.peerLock.RUnlock()
		if !exists {
			continue
		}

		if err := replicaFileServer.store.Delete(s.ID, key); err != nil {
			log.Printf("Warning: failed to delete from replica %s: %v", peer.(*p2p.TCPPeer).ListenAddr(), err)
		}
	}

	// Remove from metadata store
	s.metadataStore.RemoveFile(key)
	return nil
}

func (s *FileServer) Stop() {
	close(s.quitch)
}

func (s *FileServer) onPeer(p p2p.Peer) error {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	tcpPeer, ok := p.(*p2p.TCPPeer)
	if !ok {
		return fmt.Errorf("peer is not a TCPPeer")
	}

	listenAddr := tcpPeer.ListenAddr()
	if listenAddr == "" || !strings.HasPrefix(listenAddr, ":") {
		return fmt.Errorf("invalid listen address: %s", listenAddr)
	}

	// Ensure the peer is not already in the map
	if _, exists := s.peers[listenAddr]; !exists {
		s.peers[listenAddr] = p
		log.Printf("Connected with peer: %s", listenAddr)
	} else {
		log.Printf("Peer already connected: %s", listenAddr)
	}

	return nil
}

func (s *FileServer) loop() {
	defer func() {
		fmt.Println("file server stopped due to error or user quit action")
		s.Transport.Close()
	}()
	for {
		select {
		case rpc := <-s.Transport.Consume():
			if string(rpc.Payload) == "PING" {
				peer := s.getPeerByAddr(rpc.From) // Get peer from address
				if peer != nil {
					s.handleKeepAlive(peer)
				}
				continue
			}
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println("error while decoding received message:", err)
			}
			if err := s.handleMessage(rpc.From, &msg); err != nil {
				log.Println("handle message error:", err)
			}
		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) handleMessage(from string, msg *Message) error {
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		return s.handleMessageStoreFile(from, v)
	case MessageGetFile:
		return s.handleMessageGetFile(from, v)
	case MessageDeleteFile:
		return s.handleMessageDeleteFile(from, v)
	}
	return nil
}

func (s *FileServer) handleMessageGetFile(from string, msg MessageGetFile) error {
	// Read the file from the local store instead of the responsible peer's store
	fileSize, fileReader, err := s.store.Read(s.EncKey, msg.ID, msg.Key)
	if err != nil {
		return fmt.Errorf("failed to read file from local store: %v", err)
	}

	// Write the file to the local store
	if _, err := s.store.WriteEncrypt(s.EncKey, s.ID, msg.Key, fileReader); err != nil {
		return fmt.Errorf("failed to write file to local store: %v", err)
	}

	log.Printf("[%s] Received (%d) bytes over the network from [%s]\n", s.Transport.Addr(), fileSize, from)

	return nil
}

func (s *FileServer) handleMessageStoreFile(from string, msg MessageStoreFile) error {
	// Find the responsible peer
	responsiblePeer := s.getPeerByAddr(from)
	if responsiblePeer == nil {
		return fmt.Errorf("responsible peer not found for key: %s", msg.Key)
	}

	// Read the file data from the peer
	fileData := make([]byte, msg.Size)
	if _, err := io.ReadFull(responsiblePeer, fileData); err != nil {
		return fmt.Errorf("failed to read file data: %v", err)
	}

	// Write the file to disk
	n, err := s.store.Write(msg.ID, msg.Key, bytes.NewReader(fileData))
	if err != nil {
		return fmt.Errorf("[%s] Failed to write file to disk: %v", s.Transport.Addr(), err)
	}

	log.Printf("[%s] Written %d bytes to disk\n", s.Transport.Addr(), n)

	return nil
}

/* This function contains logic to delete the specified file from peers
 */
func (s *FileServer) handleMessageDeleteFile(from string, msg MessageDeleteFile) error {
	if !s.store.Has(msg.ID, msg.Key) {
		return fmt.Errorf("[%s] need to delete file (%s) but it does not exist on disk", s.Transport.Addr(), msg.Key)
	}

	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer %s not in map", peer)
	}

	log.Printf("[%s] found file (%s), deleting it...\n", s.Transport.Addr(), msg.Key)

	if err := s.store.Delete(msg.ID, msg.Key); err != nil {
		return fmt.Errorf("[%s] error while deleting file (%s): %v", s.Transport.Addr(), msg.Key, err)
	}

	log.Printf("[%s] successfully deleted file (%s)", s.Transport.Addr(), msg.Key)

	return nil
}

func (s *FileServer) startHealthMonitor() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go s.checkPeerHealth() // Run health check in separate goroutine
		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) checkPeerHealth() {
	s.peerLock.RLock()
	defer s.peerLock.RUnlock()

	for addr, peer := range s.peers {
		if err := peer.Send([]byte("PING")); err != nil {
			s.handleUnhealthyPeer(addr)
		}
	}
}

func (s *FileServer) handleUnhealthyPeer(addr string) {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	// Redistribute files from unhealthy peer
	s.redistributeFiles(addr)

	// Remove unhealthy peer
	delete(s.peers, addr)
	delete(s.peerServers, addr)

	log.Printf("Removed unhealthy peer: %s\n", addr)
}

func (s *FileServer) handleKeepAlive(peer p2p.Peer) {
	if tcpPeer, ok := peer.(*p2p.TCPPeer); ok {
		tcpPeer.MarkAlive()
		if err := peer.Send([]byte("PONG")); err != nil {
			log.Printf("Failed to send PONG to peer %s: %v\n", tcpPeer.ListenAddr(), err)
		}
	}
}

func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapNodes {
		if len(addr) == 0 {
			continue
		}
		go func(addr string) {
			fmt.Printf("[%s] attempting to connect with remote:%s\n", s.Transport.Addr(), addr)
			if err := s.Transport.Dial(addr); err != nil {
				fmt.Println("dial error", err)
			}
		}(addr)
	}
	return nil
}

func (s *FileServer) Start() error {
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}
	s.bootstrapNetwork()
	go s.startHealthMonitor()
	s.loop()
	return nil
}

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
	gob.Register(MessageDeleteFile{})
}

func (s *FileServer) ListFiles() []store.FileMetadata {
	return s.metadataStore.ListFiles()
}

func (s *FileServer) Edit(key string) error {
	reader, err := s.Get(key, "")
	if err != nil {
		return fmt.Errorf("failed to get file: %v", err)
	}
	defer reader.Close()

	// Read the existing content
	content, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read file content: %v", err)
	}

	// Create a temporary file with the existing content
	tmpFile, err := os.CreateTemp("", "dfs_edit_*.txt")
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(content); err != nil {
		return fmt.Errorf("failed to write to temporary file: %v", err)
	}
	tmpFile.Close()

	// Open the file in VSCode
	cmd := exec.Command("code", "--wait", tmpFile.Name())
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("editor failed: %v", err)
	}

	// Read the edited content
	editedContent, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		return fmt.Errorf("failed to read edited file: %v", err)
	}

	// Store the updated content using the responsible peer's FileServer
	if err := s.Store(key, bytes.NewReader(editedContent)); err != nil {
		return fmt.Errorf("failed to store updated file: %v", err)
	}

	return nil
}

func (s *FileServer) ListVersions(key string) ([]store.FileVersion, error) {
	metadata, exists := s.metadataStore.GetMetadataForFile(key)
	if !exists {
		return nil, fmt.Errorf("file with key %s not found", key)
	}
	return metadata.Versions, nil
}

func (s *FileServer) RestoreVersion(key, versionID string) error {
	// Get the specific version
	reader, err := s.Get(key, versionID)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Store it as the latest version
	return s.Store(key, reader)
}

func (s *FileServer) showHealthMessages() {
	for {
		select {
		case msg := <-s.healthMessages:
			fmt.Print(msg)
		default:
			return
		}
	}
}
