package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// LogEntry represents a single replicated command
type LogEntry struct {
	Index   int64  `json:"index"`
	Term    int64  `json:"term"`
	Command string `json:"command"`
}

// RaftLog is a simple append-only durable log backed by a file.
// Each entry is stored as a newline-delimited JSON object.
type RaftLog struct {
	path    string
	file    *os.File
	mu      sync.Mutex
	entries []LogEntry
}

// NewRaftLog opens or creates the log file and loads existing entries.
func NewRaftLog(path string) (*RaftLog, error) {
	// Ensure file exists
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open raft log: %w", err)
	}

	rl := &RaftLog{
		path: path,
		file: file,
	}

	// Load existing entries
	if err := rl.load(); err != nil {
		file.Close()
		return nil, err
	}

	return rl, nil
}

// load reads the entire file and populates in-memory entries
func (r *RaftLog) load() error {
	// Seek to beginning
	if _, err := r.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek raft log: %w", err)
	}

	scanner := bufio.NewScanner(r.file)
	var entries []LogEntry

	for scanner.Scan() {
		var e LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return fmt.Errorf("failed to parse raft log entry: %w", err)
		}
		entries = append(entries, e)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading raft log: %w", err)
	}

	r.entries = entries
	// Seek to end for future appends
	if _, err := r.file.Seek(0, 2); err != nil {
		return fmt.Errorf("failed to seek to end of raft log: %w", err)
	}

	return nil
}

// Append adds a new entry with the given term and command, returns its index.
func (r *RaftLog) Append(term int64, command string) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx := int64(len(r.entries)) + 1
	e := LogEntry{Index: idx, Term: term, Command: command}

	b, err := json.Marshal(e)
	if err != nil {
		return 0, fmt.Errorf("failed to marshal log entry: %w", err)
	}

	if _, err := r.file.Write(append(b, '\n')); err != nil {
		return 0, fmt.Errorf("failed to write log entry: %w", err)
	}

	if err := r.file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync log file: %w", err)
	}

	r.entries = append(r.entries, e)

	return idx, nil
}

// EntriesFrom returns all entries starting at (inclusive) startIndex.
// startIndex is 1-based; if startIndex is <=1 it returns all entries.
func (r *RaftLog) EntriesFrom(startIndex int64) ([]LogEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if startIndex <= 1 {
		return append([]LogEntry(nil), r.entries...), nil
	}

	if startIndex > int64(len(r.entries)) {
		return []LogEntry{}, nil
	}

	// convert to 0-based
	off := startIndex - 1
	out := make([]LogEntry, len(r.entries)-int(off))
	copy(out, r.entries[off:])
	return out, nil
}

// LastIndex returns the index of the last entry (0 if none)
func (r *RaftLog) LastIndex() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int64(len(r.entries))
}

// LastTerm returns the term of the last entry (0 if none)
func (r *RaftLog) LastTerm() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.entries) == 0 {
		return 0
	}
	return r.entries[len(r.entries)-1].Term
}

// Close closes the underlying file
func (r *RaftLog) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.file.Close()
}
