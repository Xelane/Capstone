package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// Node represents a cluster member
type Node struct {
	ID      string
	Address string
	peers   map[string]*Peer
	mu      sync.RWMutex

	listener net.Listener
	stopCh   chan struct{}

	// Add these new fields
	isLeader      bool
	replicateFunc func(key, value, op string) error // Callback to apply replicated data
}

// NewNode creates a new cluster node
func NewNode(id, address string, peerConfigs []NodeConfig) *Node {
	n := &Node{
		ID:       id,
		Address:  address,
		peers:    make(map[string]*Peer),
		stopCh:   make(chan struct{}),
		isLeader: false,
	}

	// Create peer connections
	for _, peerCfg := range peerConfigs {
		peer := NewPeer(peerCfg.ID, peerCfg.Address)
		n.peers[peerCfg.ID] = peer
	}

	return n
}

// SetLeader sets whether this node is the leader
func (n *Node) SetLeader(leader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.isLeader = leader
	if leader {
		fmt.Printf("[%s] I am now the LEADER\n", n.ID)
	} else {
		fmt.Printf("[%s] I am a FOLLOWER\n", n.ID)
	}
}

// IsLeader returns whether this node is the leader
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.isLeader
}

// SetReplicateFunc sets the callback for applying replicated data
func (n *Node) SetReplicateFunc(f func(key, value, op string) error) {
	n.replicateFunc = f
}

// Start begins listening for peer connections
func (n *Node) Start() error {
	listener, err := net.Listen("tcp", n.Address)
	if err != nil {
		return fmt.Errorf("failed to start node listener: %w", err)
	}

	n.listener = listener
	fmt.Printf("[%s] Cluster node listening on %s\n", n.ID, n.Address)

	// Accept peer connections
	go n.acceptConnections()

	// Connect to peers
	go n.connectToPeers()

	// Send heartbeats
	go n.sendHeartbeats()

	return nil
}

// acceptConnections handles incoming peer connections
func (n *Node) acceptConnections() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		conn, err := n.listener.Accept()
		if err != nil {
			continue
		}

		go n.handlePeerConnection(conn)
	}
}

// handlePeerConnection processes messages from a peer
func (n *Node) handlePeerConnection(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(bufio.NewReader(conn))
	encoder := json.NewEncoder(conn)

	for {
		// Read message type first
		var msgType struct {
			Type string `json:"type"`
		}

		if err := decoder.Decode(&msgType); err != nil {
			return
		}

		switch msgType.Type {
		case "ping":
			// Decode as ping
			var req PingRequest
			decoder = json.NewDecoder(bufio.NewReader(conn))
			if err := decoder.Decode(&req); err != nil {
				return
			}

			resp := PingResponse{
				FromNode: n.ID,
				Term:     0,
				Success:  true,
			}

			if err := encoder.Encode(resp); err != nil {
				return
			}

		case "replicate":
			// Decode as replication request
			var req ReplicateRequest
			decoder = json.NewDecoder(bufio.NewReader(conn))
			if err := decoder.Decode(&req); err != nil {
				return
			}

			// Apply entries
			success := true
			errorMsg := ""

			for _, entry := range req.Entries {
				if n.replicateFunc != nil {
					if err := n.replicateFunc(entry.Key, entry.Value, entry.Op); err != nil {
						success = false
						errorMsg = err.Error()
						break
					}
				}
			}

			resp := ReplicateResponse{
				Success: success,
				Error:   errorMsg,
			}

			if err := encoder.Encode(resp); err != nil {
				return
			}

			fmt.Printf("[%s] Replicated %d entries from %s\n", n.ID, len(req.Entries), req.FromNode)
		}
	}
}

// connectToPeers attempts to connect to all peers
func (n *Node) connectToPeers() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.RLock()
			for _, peer := range n.peers {
				if !peer.IsAlive() {
					if err := peer.Connect(); err != nil {
						// Only log occasionally to reduce spam
					} else {
						fmt.Printf("[%s] Connected to %s\n", n.ID, peer.ID)
					}
				}
			}
			n.mu.RUnlock()
		}
	}
}

// sendHeartbeats sends periodic pings to all peers
func (n *Node) sendHeartbeats() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.mu.RLock()
			for _, peer := range n.peers {
				if peer.IsAlive() {
					go func(p *Peer) {
						_, err := p.SendPing(n.ID, 0)
						if err != nil {
							// Peer died
						}
					}(peer)
				}
			}
			n.mu.RUnlock()
		}
	}
}

// ReplicateToFollowers sends data to all followers
func (n *Node) ReplicateToFollowers(key, value, op string) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if !n.isLeader {
		return fmt.Errorf("not the leader")
	}

	entry := ReplicateEntry{
		Key:   key,
		Value: value,
		Op:    op,
	}

	req := ReplicateRequest{
		FromNode: n.ID,
		Entries:  []ReplicateEntry{entry},
	}

	// Send to all peers
	successCount := 0
	for _, peer := range n.peers {
		if peer.IsAlive() {
			if err := peer.SendReplicate(req); err != nil {
				fmt.Printf("[%s] Failed to replicate to %s: %v\n", n.ID, peer.ID, err)
			} else {
				successCount++
			}
		}
	}

	fmt.Printf("[%s] Replicated to %d/%d followers\n", n.ID, successCount, len(n.peers))

	// For now, succeed if we replicated to at least one
	if successCount > 0 {
		return nil
	}

	return fmt.Errorf("failed to replicate to any followers")
}

// Stop stops the node
func (n *Node) Stop() {
	close(n.stopCh)

	if n.listener != nil {
		n.listener.Close()
	}

	n.mu.Lock()
	for _, peer := range n.peers {
		peer.Close()
	}
	n.mu.Unlock()
}

// GetAlivePeers returns list of alive peer IDs
func (n *Node) GetAlivePeers() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var alive []string
	for id, peer := range n.peers {
		if peer.IsAlive() {
			alive = append(alive, id)
		}
	}
	return alive
}
