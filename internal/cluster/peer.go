package cluster

import (
	"bufio"
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
	encoder  *json.Encoder
	decoder  *json.Decoder
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
	p.encoder = json.NewEncoder(conn)
	p.decoder = json.NewDecoder(conn)
	p.alive = true
	p.lastSeen = time.Now()

	return nil
}

// SendPing sends a heartbeat to the peer
func (p *Peer) SendPing(from string, term int64) (*PingResponse, error) {
	// Use a short-lived connection for this RPC to avoid competing readers on
	// a shared long-lived socket.
	req := PingRequest{
		Type:     "ping",
		FromNode: from,
		Term:     term,
	}

	var resp PingResponse
	if err := p.sendOnce(req, &resp); err != nil {
		return nil, err
	}

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
	req.Type = "replicate"
	var resp ReplicateResponse
	if err := p.sendOnce(req, &resp); err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("replication failed: %s", resp.Error)
	}

	return nil
}

// RequestVote asks peer to vote for us
func (p *Peer) RequestVote(candidateID string, term int64) (bool, error) {
	req := VoteRequest{
		Type:        "vote",
		Term:        term,
		CandidateID: candidateID,
	}

	var resp VoteResponse
	if err := p.sendOnce(req, &resp); err != nil {
		return false, err
	}

	return resp.VoteGranted, nil
}

// SendAppendEntries sends an AppendEntries RPC to the peer
func (p *Peer) SendAppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error) {
	var resp AppendEntriesResponse
	if err := p.sendOnce(req, &resp); err != nil {
		return AppendEntriesResponse{}, err
	}
	return resp, nil
}

// sendOnce dials the peer, sends a single request and decodes a single response.
func (p *Peer) sendOnce(req interface{}, resp interface{}) error {
	conn, err := net.DialTimeout("tcp", p.Address, 2*time.Second)
	if err != nil {
		// Mark as not alive if dial fails
		p.mu.Lock()
		p.alive = false
		p.mu.Unlock()
		return fmt.Errorf("failed to dial %s: %w", p.ID, err)
	}
	defer conn.Close()

	enc := json.NewEncoder(conn)
	dec := json.NewDecoder(bufio.NewReader(conn))

	if err := enc.Encode(req); err != nil {
		// Mark as not alive if send fails
		p.mu.Lock()
		p.alive = false
		p.mu.Unlock()
		return fmt.Errorf("encode error: %w", err)
	}

	if err := dec.Decode(resp); err != nil {
		// Mark as not alive if receive fails
		p.mu.Lock()
		p.alive = false
		p.mu.Unlock()
		return fmt.Errorf("decode error: %w", err)
	}

	return nil
}
