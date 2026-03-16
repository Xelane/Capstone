package cluster

// Message types for inter-node communication

// PingRequest is a heartbeat message
type PingRequest struct {
	FromNode string `json:"from_node"`
	Term     int64  `json:"term"` // Will use this later for Raft
}

// PingResponse is the reply to a ping
type PingResponse struct {
	FromNode string `json:"from_node"`
	Term     int64  `json:"term"`
	Success  bool   `json:"success"`
}

// ReplicateRequest sends data to replicate (we'll implement this in Week 9-10)
type ReplicateRequest struct {
	FromNode string           `json:"from_node"`
	Entries  []ReplicateEntry `json:"entries"`
}

type ReplicateEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	Op    string `json:"op"` // "PUT" or "DELETE"
}

type ReplicateResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}
