package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

// Raft node states
const (
	Follower  = "follower"
	Candidate = "candidate"
	Leader    = "leader"
)

// Node represents a cluster member with Raft consensus
type Node struct {
	ID      string
	Address string
	peers   map[string]*Peer
	mu      sync.RWMutex

	listener net.Listener
	stopCh   chan struct{}

	// Raft state
	state         string // Follower, Candidate, or Leader
	currentTerm   int64
	votedFor      string
	lastHeartbeat time.Time

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	replicateFunc func(key, value, op string) error
}

// NewNode creates a new cluster node
func NewNode(id, address string, peerConfigs []NodeConfig) *Node {
	n := &Node{
		ID:            id,
		Address:       address,
		peers:         make(map[string]*Peer),
		stopCh:        make(chan struct{}),
		state:         Follower,
		currentTerm:   0,
		votedFor:      "",
		lastHeartbeat: time.Now(),
	}

	// Create peer connections
	for _, peerCfg := range peerConfigs {
		peer := NewPeer(peerCfg.ID, peerCfg.Address)
		n.peers[peerCfg.ID] = peer
	}

	return n
}

// Start begins listening for peer connections
func (n *Node) Start() error {
	listener, err := net.Listen("tcp", n.Address)
	if err != nil {
		return fmt.Errorf("failed to start node listener: %w", err)
	}

	n.listener = listener
	fmt.Printf("[%s] Cluster node listening on %s\n", n.ID, n.Address)

	// Start election timer
	n.resetElectionTimer()

	// Accept peer connections
	go n.acceptConnections()

	// Connect to peers
	go n.connectToPeers()

	return nil
}

// resetElectionTimer resets the election timeout
func (n *Node) resetElectionTimer() {
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	n.electionTimer = time.AfterFunc(timeout, func() {
		n.startElection()
	})
}

// startElection begins a new election
func (n *Node) startElection() {
	n.mu.Lock()

	// Become candidate
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.ID
	currentTerm := n.currentTerm

	fmt.Printf("🗳️  [%s] Starting election for term %d\n", n.ID, currentTerm)

	n.mu.Unlock()

	// Vote for self
	votesReceived := 1
	votesNeeded := (len(n.peers)+1)/2 + 1

	var voteMu sync.Mutex

	// Request votes from all peers
	n.mu.RLock()
	for _, peer := range n.peers {
		go func(p *Peer) {
			if !p.IsAlive() {
				return
			}

			granted, err := p.RequestVote(n.ID, currentTerm)
			if err != nil {
				return
			}

			if granted {
				voteMu.Lock()
				votesReceived++
				votes := votesReceived
				voteMu.Unlock()

				// Check if we have majority
				if votes >= votesNeeded {
					n.becomeLeader()
				}
			}
		}(peer)
	}
	n.mu.RUnlock()

	// Reset election timer
	n.resetElectionTimer()
}

// becomeLeader transitions to leader state
func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Candidate {
		return // Already transitioned
	}

	n.state = Leader
	fmt.Printf("👑 [%s] Became LEADER for term %d\n", n.ID, n.currentTerm)

	// Stop election timer
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

	// Start sending heartbeats
	n.sendHeartbeatsLoop()
}

// sendHeartbeatsLoop sends periodic heartbeats as leader
func (n *Node) sendHeartbeatsLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)

	go func() {
		for {
			select {
			case <-n.stopCh:
				ticker.Stop()
				return
			case <-ticker.C:
				n.mu.RLock()
				if n.state != Leader {
					n.mu.RUnlock()
					ticker.Stop()
					return
				}
				term := n.currentTerm
				n.mu.RUnlock()

				// Send heartbeat to all followers
				for _, peer := range n.peers {
					go func(p *Peer) {
						if p.IsAlive() {
							p.SendPing(n.ID, term)
						}
					}(peer)
				}
			}
		}
	}()
}

// handlePeerConnection processes messages from a peer
func (n *Node) handlePeerConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		decoder := json.NewDecoder(reader)

		var rawMsg json.RawMessage
		if err := decoder.Decode(&rawMsg); err != nil {
			return
		}

		var msgWithType struct {
			Type string `json:"type"`
		}

		if err := json.Unmarshal(rawMsg, &msgWithType); err != nil {
			return
		}

		switch msgWithType.Type {
		case "ping":
			var req PingRequest
			if err := json.Unmarshal(rawMsg, &req); err != nil {
				return
			}

			// Update term if higher
			n.mu.Lock()
			if req.Term > n.currentTerm {
				n.currentTerm = req.Term
				n.state = Follower
				n.votedFor = ""
			}
			n.lastHeartbeat = time.Now()
			n.resetElectionTimer()
			n.mu.Unlock()

			resp := PingResponse{
				FromNode: n.ID,
				Term:     n.currentTerm,
				Success:  true,
			}

			encoder := json.NewEncoder(conn)
			if err := encoder.Encode(resp); err != nil {
				return
			}

		case "vote":
			var req VoteRequest
			if err := json.Unmarshal(rawMsg, &req); err != nil {
				return
			}

			granted := n.handleVoteRequest(req)

			resp := VoteResponse{
				Term:        n.currentTerm,
				VoteGranted: granted,
			}

			encoder := json.NewEncoder(conn)
			if err := encoder.Encode(resp); err != nil {
				return
			}

		case "replicate":
			var req ReplicateRequest
			if err := json.Unmarshal(rawMsg, &req); err != nil {
				return
			}

			success := true
			errorMsg := ""

			for _, entry := range req.Entries {
				if n.replicateFunc != nil {
					if err := n.replicateFunc(entry.Key, entry.Value, entry.Op); err != nil {
						success = false
						errorMsg = err.Error()
						break
					}
					fmt.Printf("📥 [%s] REPLICATED: %s %s\n", n.ID, entry.Op, entry.Key)
				}
			}

			resp := ReplicateResponse{
				Success: success,
				Error:   errorMsg,
			}

			encoder := json.NewEncoder(conn)
			if err := encoder.Encode(resp); err != nil {
				return
			}
		}
	}
}

// handleVoteRequest processes a vote request
func (n *Node) handleVoteRequest(req VoteRequest) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Reject if term is old
	if req.Term < n.currentTerm {
		return false
	}

	// Update term if higher
	if req.Term > n.currentTerm {
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
	}

	// Grant vote if haven't voted or already voted for this candidate
	if n.votedFor == "" || n.votedFor == req.CandidateID {
		n.votedFor = req.CandidateID
		n.resetElectionTimer()
		fmt.Printf("✅ [%s] Voted for %s in term %d\n", n.ID, req.CandidateID, req.Term)
		return true
	}

	return false
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
					if err := peer.Connect(); err == nil {
						fmt.Printf("[%s] Connected to %s\n", n.ID, peer.ID)
					}
				}
			}
			n.mu.RUnlock()
		}
	}
}

// IsLeader returns whether this node is the leader
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == Leader
}

// SetReplicateFunc sets the callback for applying replicated data
func (n *Node) SetReplicateFunc(f func(key, value, op string) error) {
	n.replicateFunc = f
}

// ReplicateToFollowers sends data to all followers
func (n *Node) ReplicateToFollowers(key, value, op string) error {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.state != Leader {
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

	successCount := 0
	for _, peer := range n.peers {
		if peer.IsAlive() {
			if err := peer.SendReplicate(req); err == nil {
				successCount++
			}
		}
	}

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

	if n.electionTimer != nil {
		n.electionTimer.Stop()
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

// GetState returns current Raft state
func (n *Node) GetState() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// GetTerm returns current term
func (n *Node) GetTerm() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}
