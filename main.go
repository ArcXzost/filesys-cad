package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"filesys-cad/crypto"
	"filesys-cad/p2p"
	"filesys-cad/store"

	"github.com/urfave/cli/v2"
)

var s *FileServer // Global FileServer instance

func makeServer(listenAddr string, nodes ...string) *FileServer {
	tcpTransportOpts := p2p.TCPTransportOpts{
		ListenAddr:    listenAddr,
		HandshakeFunc: p2p.NOPHandshakeFunc,
		Decoder:       p2p.DefaultDecoder{},
	}
	tcpTransport := p2p.NewTCPTransport(tcpTransportOpts)
	filename := strings.Split(listenAddr, ":")[1]
	fileServerOpts := FileServerOpts{
		EncKey:            crypto.NewEncryptionKey(),
		StorageRoot:       "F" + filename + "_network",
		PathTransformFunc: store.CASPathTransformFunc,
		Transport:         tcpTransport,
		BootstrapNodes:    nodes,
	}
	fs := NewFileServer(fileServerOpts)
	tcpTransport.OnPeer = fs.onPeer

	go fs.loop() // Ensure the loop method is called

	return fs
}

func main() {
	// Initialize the FileServer
	s = makeServer(":3000", "")
	go func() {
		if err := s.Start(); err != nil {
			log.Fatal(err)
		}
	}()
	time.Sleep(time.Second * 1) // Wait for the server to start

	// CLI Application
	app := &cli.App{
		Name:  "dfs",
		Usage: "Distributed File System CLI",
		Commands: []*cli.Command{
			{
				Name:  "store",
				Usage: "Store custom data in the system",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "data",
						Usage: "Custom data to store",
					},
				},
				Action: func(c *cli.Context) error {
					data := c.String("data")
					if data == "" {
						return fmt.Errorf("please provide data to store")
					}
					// Call the FileServer's Store method
					key := fmt.Sprintf("custom_data_%d", time.Now().Unix())
					err := s.Store(key, strings.NewReader(data))
					if err != nil {
						return fmt.Errorf("failed to store data: %v", err)
					}
					fmt.Printf("Data stored with key: %s\n", key)
					return nil
				},
			},
			{
				Name:  "upload",
				Usage: "Upload a file to the system",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "path",
						Usage: "Path to the file to upload",
					},
				},
				Action: func(c *cli.Context) error {
					filePath := c.String("path")
					if filePath == "" {
						return fmt.Errorf("please provide a file path")
					}
					file, err := os.Open(filePath)
					if err != nil {
						return fmt.Errorf("failed to open file: %v", err)
					}
					defer file.Close()

					// Call the FileServer's Store method
					key := fmt.Sprintf("file_%d", time.Now().Unix())
					err = s.Store(key, file)
					if err != nil {
						return fmt.Errorf("failed to upload file: %v", err)
					}
					fmt.Printf("File uploaded with key: %s\n", key)
					return nil
				},
			},
			{
				Name:  "get",
				Usage: "Retrieve data from the system",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "key",
						Usage: "Key of the data to retrieve",
					},
				},
				Action: func(c *cli.Context) error {
					key := c.String("key")
					if key == "" {
						return fmt.Errorf("please provide a key")
					}
					// Call the FileServer's Get method
					reader, err := s.Get(key)
					if closer, ok := reader.(io.Closer); ok {
						defer closer.Close()
					}
					if err != nil {
						return fmt.Errorf("failed to retrieve data: %v", err)
					}

					data, err := io.ReadAll(reader)
					if err != nil {
						return fmt.Errorf("failed to read data: %v", err)
					}
					fmt.Printf("Retrieved data:\n%s\n", string(data))
					return nil
				},
			},
			{
				Name:  "delete",
				Usage: "Delete data from the system",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "key",
						Usage: "Key of the data to delete",
					},
				},
				Action: func(c *cli.Context) error {
					key := c.String("key")
					if key == "" {
						return fmt.Errorf("please provide a key")
					}
					// Call the FileServer's Delete method
					err := s.Delete(key)
					if err != nil {
						return fmt.Errorf("failed to delete data: %v", err)
					}
					fmt.Printf("Data with key %s deleted\n", key)
					return nil
				},
			},
			{
				Name:  "list",
				Usage: "List all files in the system",
				Action: func(c *cli.Context) error {
					// Implement a method in FileServer to list files
					// For now, this is a placeholder
					fmt.Println("Listing files is not yet implemented.")
					return nil
				},
			},
			{
				Name:  "start-peers",
				Usage: "Start multiple peers",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:  "count",
						Usage: "Number of peers to start",
					},
				},
				Action: func(c *cli.Context) error {
					count := c.Int("count")
					for i := 0; i < count; i++ {
						port := 3000 + i + 1                                  // Start from :3001
						peer := makeServer(fmt.Sprintf(":%d", port), ":3000") // Connect to the initial peer
						s.peerServers[fmt.Sprintf(":%d", port)] = peer
						go peer.Start()
					}
					return nil
				},
			},
		},
	}

	// Interactive CLI loop
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("dfs> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		input = strings.TrimSpace(input)
		if input == "quit" || input == "exit" {
			fmt.Println("Exiting...")
			break
		}

		// Split the input into command and arguments
		args := strings.Fields(input)
		if len(args) == 0 {
			continue
		}

		// Run the command
		if err := app.Run(append([]string{"dfs"}, args...)); err != nil {
			fmt.Println("Error:", err)
		}
	}
}
