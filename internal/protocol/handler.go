package protocol

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/Xelane/Capstone/internal/storage"
)

// ReplicationFunc is called to replicate data to followers
type ReplicationFunc func(key, value, op string) error

// GetLeaderAddressFunc returns the address of the current leader
type GetLeaderAddressFunc func() string

// Handler processes client commands
type Handler struct {
	store                storage.Storage
	replicateFunc        ReplicationFunc
	isLeaderFunc         func() bool
	getLeaderAddressFunc GetLeaderAddressFunc
}

// NewHandler creates a new command handler
func NewHandler(store storage.Storage) *Handler {
	return &Handler{
		store:                store,
		replicateFunc:        nil,
		isLeaderFunc:         func() bool { return true }, // Default: always leader
		getLeaderAddressFunc: func() string { return "" }, // Default: no leader
	}
}

// SetReplicationFunc sets the replication callback
func (h *Handler) SetReplicationFunc(f ReplicationFunc) {
	h.replicateFunc = f
}

// SetIsLeaderFunc sets the function to check if this node is leader
func (h *Handler) SetIsLeaderFunc(f func() bool) {
	h.isLeaderFunc = f
}

// SetLeaderAddressFunc sets the function to get the leader's address
func (h *Handler) SetLeaderAddressFunc(f GetLeaderAddressFunc) {
	h.getLeaderAddressFunc = f
}

// HandleConnection processes a client connection
func (h *Handler) HandleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		response := h.ProcessCommand(line)
		conn.Write([]byte(response + "\n"))
	}
}

// ProcessCommand parses and executes a command
func (h *Handler) ProcessCommand(line string) string {
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

	// Check if leader (for writes)
	if !h.isLeaderFunc() {
		// Forward to leader if we know it
		if leaderAddr := h.getLeaderAddressFunc(); leaderAddr != "" {
			return h.forwardToLeader(leaderAddr, strings.Join(parts, " "))
		}
		return "ERROR: Not leader, cannot accept writes"
	}

	// Apply locally
	if err := h.store.Put(key, value); err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}

	// Replicate to followers
	if h.replicateFunc != nil {
		if err := h.replicateFunc(key, value, "PUT"); err != nil {
			return fmt.Sprintf("ERROR: Replication failed: %v", err)
		}
	}

	return "OK"
}

func (h *Handler) handleGet(parts []string) string {
	if len(parts) < 2 {
		return "ERROR: GET requires key"
	}

	// Reads can happen on any node
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

	// Check if leader (for writes)
	if !h.isLeaderFunc() {
		// Forward to leader if we know it
		if leaderAddr := h.getLeaderAddressFunc(); leaderAddr != "" {
			return h.forwardToLeader(leaderAddr, strings.Join(parts, " "))
		}
		return "ERROR: Not leader, cannot accept writes"
	}

	key := parts[1]

	// Apply locally
	if err := h.store.Delete(key); err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}

	// Replicate to followers
	if h.replicateFunc != nil {
		if err := h.replicateFunc(key, "", "DELETE"); err != nil {
			return fmt.Sprintf("ERROR: Replication failed: %v", err)
		}
	}

	return "OK"
}

// forwardToLeader proxies a client command to the leader
func (h *Handler) forwardToLeader(leaderAddr, command string) string {
	conn, err := net.DialTimeout("tcp", leaderAddr, 2*time.Second)
	if err != nil {
		return fmt.Sprintf("ERROR: Cannot reach leader at %s: %v", leaderAddr, err)
	}
	defer conn.Close()

	// Send command to leader
	if _, err := conn.Write([]byte(command + "\n")); err != nil {
		return fmt.Sprintf("ERROR: Failed to send to leader: %v", err)
	}

	// Read response from leader
	scanner := bufio.NewScanner(conn)
	if scanner.Scan() {
		return scanner.Text()
	}

	return "ERROR: No response from leader"
}
