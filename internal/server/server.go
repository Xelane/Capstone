package server

import (
	"fmt"
	"net"

	"github.com/Xelane/Capstone/internal/protocol"
	"github.com/Xelane/Capstone/internal/storage"
)

// Server handles TCP connections
type Server struct {
	address string
	handler *protocol.Handler
}

// New creates a new server instance
func New(address string, store storage.Storage) *Server {
	return &Server{
		address: address,
		handler: protocol.NewHandler(store),
	}
}

// Start begins listening for connections
func (s *Server) Start() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}

		// Handle each client in a goroutine
		go s.handler.HandleConnection(conn)
	}
}
