package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// Peer represents a connection to another node
type Peer struct {
	ID       string
	Address  string
	conn     net.Conn
	mu       sync.Mutex
	alive    bool
	lastSeen time.Time
}

// NewPeer creates a new peer connection
func NewPeer(id, address string) *Peer {
	return &Peer{
		ID:      id,
		Address: address,
		alive:   false,
	}
}

// Connect establishes connection to peer
func (p *Peer) Connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close existing connection if any
	if p.conn != nil {
		p.conn.Close()
	}

	// Try to connect
	conn, err := net.DialTimeout("tcp", p.Address, 2*time.Second)
	if err != nil {
		p.alive = false
		return fmt.Errorf("failed to connect to %s: %w", p.ID, err)
	}

	p.conn = conn
	p.alive = true
	p.lastSeen = time.Now()

	return nil
}

// SendPing sends a heartbeat to the peer
func (p *Peer) SendPing(from string, term int64) (*PingResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.alive || p.conn == nil {
		return nil, fmt.Errorf("peer %s not connected", p.ID)
	}

	// Send message type first
	msgType := map[string]string{"type": "ping"}
	encoder := json.NewEncoder(p.conn)
	if err := encoder.Encode(msgType); err != nil {
		p.alive = false
		return nil, fmt.Errorf("failed to send message type: %w", err)
	}

	// Create request
	req := PingRequest{
		FromNode: from,
		Term:     term,
	}

	// Send request
	if err := encoder.Encode(req); err != nil {
		p.alive = false
		return nil, fmt.Errorf("failed to send ping: %w", err)
	}

	// Read response
	decoder := json.NewDecoder(p.conn)
	var resp PingResponse
	if err := decoder.Decode(&resp); err != nil {
		p.alive = false
		return nil, fmt.Errorf("failed to read ping response: %w", err)
	}

	p.lastSeen = time.Now()
	return &resp, nil
}

// IsAlive returns whether peer is reachable
func (p *Peer) IsAlive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.alive
}

// Close closes the connection
func (p *Peer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
	p.alive = false
}

// SendReplicate sends replication request to peer
func (p *Peer) SendReplicate(req ReplicateRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.alive || p.conn == nil {
		return fmt.Errorf("peer %s not connected", p.ID)
	}

	// Send message type first
	msgType := map[string]string{"type": "replicate"}
	encoder := json.NewEncoder(p.conn)
	if err := encoder.Encode(msgType); err != nil {
		p.alive = false
		return fmt.Errorf("failed to send message type: %w", err)
	}

	// Send request
	if err := encoder.Encode(req); err != nil {
		p.alive = false
		return fmt.Errorf("failed to send replicate request: %w", err)
	}

	// Read response
	decoder := json.NewDecoder(p.conn)
	var resp ReplicateResponse
	if err := decoder.Decode(&resp); err != nil {
		p.alive = false
		return fmt.Errorf("failed to read replicate response: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("replication failed: %s", resp.Error)
	}

	p.lastSeen = time.Now()
	return nil
}

// RequestVote asks peer to vote for us
func (p *Peer) RequestVote(candidateID string, term int64) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.alive || p.conn == nil {
		return false, fmt.Errorf("peer %s not connected", p.ID)
	}

	req := VoteRequest{
		Type:        "vote",
		Term:        term,
		CandidateID: candidateID,
	}

	encoder := json.NewEncoder(p.conn)
	if err := encoder.Encode(req); err != nil {
		p.alive = false
		return false, fmt.Errorf("failed to send vote request: %w", err)
	}

	decoder := json.NewDecoder(p.conn)
	var resp VoteResponse
	if err := decoder.Decode(&resp); err != nil {
		p.alive = false
		return false, fmt.Errorf("failed to read vote response: %w", err)
	}

	p.lastSeen = time.Now()
	return resp.VoteGranted, nil
}
