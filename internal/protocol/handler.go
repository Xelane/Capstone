package protocol

import (
	"bufio"
	"net"
	"strings"
)

// Storage interface that handler needs
type Storage interface {
	Put(key, value string)
	Get(key string) (string, bool)
	Delete(key string)
}

// Handler processes client commands
type Handler struct {
	store Storage
}

// NewHandler creates a new command handler
func NewHandler(store Storage) *Handler {
	return &Handler{store: store}
}

// HandleConnection processes a client connection
func (h *Handler) HandleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		response := h.processCommand(line)
		conn.Write([]byte(response + "\n"))
	}
}

// processCommand parses and executes a command
func (h *Handler) processCommand(line string) string {
	parts := strings.Fields(line)

	if len(parts) == 0 {
		return ""
	}

	command := strings.ToUpper(parts[0])

	switch command {
	case "PUT":
		return h.handlePut(parts)
	case "GET":
		return h.handleGet(parts)
	case "DELETE":
		return h.handleDelete(parts)
	default:
		return "ERROR: Unknown command"
	}
}

func (h *Handler) handlePut(parts []string) string {
	if len(parts) < 3 {
		return "ERROR: PUT requires key and value"
	}

	key := parts[1]
	value := strings.Join(parts[2:], " ")
	h.store.Put(key, value)

	return "OK"
}

func (h *Handler) handleGet(parts []string) string {
	if len(parts) < 2 {
		return "ERROR: GET requires key"
	}

	key := parts[1]
	value, exists := h.store.Get(key)

	if exists {
		return value
	}
	return "NOT_FOUND"
}

func (h *Handler) handleDelete(parts []string) string {
	if len(parts) < 2 {
		return "ERROR: DELETE requires key"
	}

	key := parts[1]
	h.store.Delete(key)

	return "OK"
}
