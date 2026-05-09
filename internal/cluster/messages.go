package cluster

// VoteRequest asks for a vote
type VoteRequest struct {
	Type        string `json:"type"` // "vote"
	Term        int64  `json:"term"`
	CandidateID string `json:"candidate_id"`
}

// VoteResponse grants or denies vote
type VoteResponse struct {
	Term        int64 `json:"term"`
	VoteGranted bool  `json:"vote_granted"`
}

// PingRequest is a heartbeat message
type PingRequest struct {
	Type     string `json:"type"` // "ping"
	FromNode string `json:"from_node"`
	Term     int64  `json:"term"`
}

// PingResponse is the reply to a ping
type PingResponse struct {
	FromNode string `json:"from_node"`
	Term     int64  `json:"term"`
	Success  bool   `json:"success"`
}

// ReplicateRequest sends data to replicate
type ReplicateRequest struct {
	Type     string           `json:"type"` // "replicate"
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

// AppendEntriesRequest is the Raft AppendEntries RPC used to replicate logs
type AppendEntriesRequest struct {
	Type         string     `json:"type"` // "append_entries"
	Term         int64      `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int64      `json:"prev_log_index"`
	PrevLogTerm  int64      `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int64      `json:"leader_commit"`
}

// AppendEntriesResponse is the reply to AppendEntries
type AppendEntriesResponse struct {
	Term       int64 `json:"term"`
	Success    bool  `json:"success"`
	MatchIndex int64 `json:"match_index"`
}
