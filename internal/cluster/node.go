package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
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

	peers    map[string]*Peer
	mu       sync.RWMutex
	listener net.Listener
	stopCh   chan struct{}

	// Raft state
	state          string
	currentTerm    int64
	votedFor       string
	lastHeartbeat  time.Time
	leaderID       string
	peerAddresses  map[string]string
	peerPorts      map[string]string
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	replicateFunc  func(key, value, op string) error
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
		leaderID:      "",
		peerAddresses: make(map[string]string),
		peerPorts:     make(map[string]string),
	}

	// Create peer connections and map addresses
	for _, peerCfg := range peerConfigs {
		peer := NewPeer(peerCfg.ID, peerCfg.Address)

		n.peers[peerCfg.ID] = peer
		n.peerAddresses[peerCfg.ID] = peerCfg.Address
		n.peerPorts[peerCfg.ID] = peerCfg.ClientPort
	}

	return n
}

// Start begins listening for peer connections
func (n *Node) Start() error {
	// Try to bind to the configured address. If that fails (for example the
	// configured host isn't available on this machine), fall back to binding
	// on all interfaces for the same port so the node can still accept
	// connections.
	host, port, err := net.SplitHostPort(n.Address)
	if err != nil {
		return fmt.Errorf("invalid node address %q: %w", n.Address, err)
	}

	listener, err := net.Listen("tcp", n.Address)
	if err != nil {
		fallback := ":" + port
		listener, err = net.Listen("tcp", fallback)
		if err != nil {
			return fmt.Errorf("failed to start node listener on %s (fallback %s): %w", n.Address, fallback, err)
		}

		n.listener = listener

		fmt.Printf("[%s] Warning: could not bind to %s (host %s); bound to %s instead\n", n.ID, n.Address, host, fallback)
	} else {
		n.listener = listener
		fmt.Printf("[%s] Cluster node listening on %s\n", n.ID, n.Address)
	}

	n.resetElectionTimer()

	go n.acceptConnections()
	go n.connectToPeers()

	return nil
}

// resetElectionTimer resets the election timeout
func (n *Node) resetElectionTimer() {
	timeout := time.Duration(1000+rand.Intn(1000)) * time.Millisecond

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

	// Don't start election if already leader
	if n.state == Leader {
		n.mu.Unlock()
		return
	}

	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.ID

	currentTerm := n.currentTerm

	fmt.Printf("🗳️  [%s] Starting election for term %d\n", n.ID, currentTerm)

	n.mu.Unlock()

	n.resetElectionTimer()

	votesReceived := 1

	// Quorum is always (total nodes / 2) + 1.
	// Total nodes = len(peers) + 1 (the local node itself)
	totalNodes := len(n.peers) + 1
	votesNeeded := (totalNodes / 2) + 1

	var voteMu sync.Mutex
	var once sync.Once

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

				if votes >= votesNeeded {
					once.Do(func() {
						n.becomeLeader(currentTerm)
					})
				}
			}
		}(peer)
	}

	n.mu.RUnlock()

	// For single-node quorum (2-node cluster), become leader immediately
	if votesNeeded == 1 {
		once.Do(func() {
			n.becomeLeader(currentTerm)
		})

		return
	}
}

// becomeLeader transitions to leader state for the given term
func (n *Node) becomeLeader(electedTerm int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Stale: term has already advanced
	if n.state != Candidate || n.currentTerm != electedTerm {
		return
	}

	n.state = Leader
	n.leaderID = n.ID

	fmt.Printf("👑 [%s] Became LEADER for term %d\n", n.ID, n.currentTerm)

	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}

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

	decoder := json.NewDecoder(bufio.NewReader(conn))
	encoder := json.NewEncoder(conn)

	for {
		var rawMsg json.RawMessage

		if err := decoder.Decode(&rawMsg); err != nil {
			// Treat EOF and closed-connection errors as normal remote disconnects.
			if err == io.EOF || strings.Contains(err.Error(), "use of closed network connection") || strings.Contains(err.Error(), "forcibly closed by the remote host") {
				return
			}

			return
		}

		var msgWithType struct {
			Type string `json:"type"`
		}

		if err := json.Unmarshal(rawMsg, &msgWithType); err != nil {
			return
		}

		// Note: do not associate accepted connections with `Peer` objects.
		// Keeping separate incoming sockets avoids race conditions where two
		// goroutines attempt to read from the same connection (causing EOFs)
		// or write concurrently. Outgoing connections created by `Peer.Connect`
		// will be used for requests; accepted connections are only used to
		// receive requests and send responses on that socket.

		switch msgWithType.Type {

		case "ping":
			var req PingRequest

			if err := json.Unmarshal(rawMsg, &req); err != nil {
				return
			}

			// Update term if higher
			n.mu.Lock()

			// Ignore stale heartbeats
			if req.Term < n.currentTerm {
				n.mu.Unlock()
				return
			}

			// Only clear vote on newer term
			if req.Term > n.currentTerm {
				n.currentTerm = req.Term
				n.votedFor = ""
			}

			n.state = Follower
			n.leaderID = req.FromNode
			n.lastHeartbeat = time.Now()

			n.resetElectionTimer()

			n.mu.Unlock()

			resp := PingResponse{
				FromNode: n.ID,
				Term:     n.currentTerm,
				Success:  true,
			}

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
			// 1. Identify disconnected peers while holding the lock
			var disconnectedPeers []*Peer
			n.mu.RLock()
			for _, peer := range n.peers {
				if !peer.IsAlive() {
					disconnectedPeers = append(disconnectedPeers, peer)
				}
			}
			n.mu.RUnlock() // <--- UNLOCK IMMEDIATELY!

			// 2. Perform network I/O freely without freezing the Node
			for _, peer := range disconnectedPeers {
				if err := peer.Connect(); err == nil {
					fmt.Printf("[%s] Connected to %s\n", n.ID, peer.ID)
				}
			}
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
	if n.state != Leader {
		n.mu.RUnlock()
		return fmt.Errorf("not the leader")
	}

	// 1. Gather active peers
	var activePeers []*Peer
	for _, peer := range n.peers {
		if peer.IsAlive() {
			activePeers = append(activePeers, peer)
		}
	}
	n.mu.RUnlock() // <--- UNLOCK BEFORE SENDING DATA!

	// 2. Prepare request
	req := ReplicateRequest{
		FromNode: n.ID,
		Entries: []ReplicateEntry{
			{Key: key, Value: value, Op: op},
		},
	}

	// 3. Send over network safely
	successCount := 0
	for _, peer := range activePeers {
		if err := peer.SendReplicate(req); err == nil {
			successCount++
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

// GetLeaderID returns current leader ID
func (n *Node) GetLeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.leaderID
}

// GetLeaderAddress returns the address of the current leader
func (n *Node) GetLeaderAddress() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.leaderID == "" {
		return ""
	}

	return n.peerAddresses[n.leaderID]
}

// GetLeaderClientAddress returns the client address of the current leader
func (n *Node) GetLeaderClientAddress() string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.leaderID == "" {
		return ""
	}

	clusterAddr := n.peerAddresses[n.leaderID]
	clientPort := n.peerPorts[n.leaderID]

	if clientPort == "" {
		return ""
	}

	// Extract host from cluster address
	host := clusterAddr

	if idx := len(clusterAddr) - 1; idx > 0 {
		for i := idx; i >= 0; i-- {
			if clusterAddr[i] == ':' {
				host = clusterAddr[:i]
				break
			}
		}
	}

	return host + ":" + clientPort
}
