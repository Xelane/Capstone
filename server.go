package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
)

// Our in-memory database
type Store struct {
	data map[string]string
	mu   sync.RWMutex // Protects the map from concurrent access
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.data[key]
	return value, exists
}

func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

func main() {
	store := NewStore()

	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	fmt.Println("Key-Value Server listening on :8080")
	fmt.Println("Commands: PUT key value | GET key | DELETE key")

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleClient(conn, store)
	}
}

func handleClient(conn net.Conn, store *Store) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line) // Split by spaces

		if len(parts) == 0 {
			continue
		}

		command := strings.ToUpper(parts[0])

		switch command {
		case "PUT":
			if len(parts) < 3 {
				conn.Write([]byte("ERROR: PUT requires key and value\n"))
				continue
			}
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			store.Put(key, value)
			conn.Write([]byte("OK\n"))

		case "GET":
			if len(parts) < 2 {
				conn.Write([]byte("ERROR: GET requires key\n"))
				continue
			}
			key := parts[1]
			value, exists := store.Get(key)
			if exists {
				conn.Write([]byte(value + "\n"))
			} else {
				conn.Write([]byte("NOT_FOUND\n"))
			}

		case "DELETE":
			if len(parts) < 2 {
				conn.Write([]byte("ERROR: DELETE requires key\n"))
				continue
			}
			key := parts[1]
			store.Delete(key)
			conn.Write([]byte("OK\n"))

		default:
			conn.Write([]byte("ERROR: Unknown command\n"))
		}
	}
}
