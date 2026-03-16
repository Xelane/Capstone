package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// SSTable represents a sorted string table file on disk
type SSTable struct {
	path  string
	index map[string]int64 // Key -> file offset (for faster lookups)
}

// SSTableEntry represents a key-value pair in an SSTable
type SSTableEntry struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Deleted bool   `json:"deleted"`
}

// NewSSTable creates a new SSTable from memtable entries
func NewSSTable(dir string, id int, entries []entry) (*SSTable, error) {
	// Create directory if doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// File path: data/node1/sstable_0001.db
	filename := fmt.Sprintf("sstable_%04d.db", id)
	path := filepath.Join(dir, filename)

	// Create file
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create sstable: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	index := make(map[string]int64)

	var offset int64 = 0

	// Write entries (already sorted)
	for _, e := range entries {
		sstEntry := SSTableEntry{
			Key:     e.key,
			Value:   e.value,
			Deleted: e.deleted,
		}

		// Remember offset for this key
		index[e.key] = offset

		// Write as JSON line
		data, err := json.Marshal(sstEntry)
		if err != nil {
			return nil, err
		}

		n, err := writer.Write(data)
		if err != nil {
			return nil, err
		}

		// Write newline
		writer.WriteByte('\n')

		offset += int64(n + 1) // +1 for newline
	}

	if err := writer.Flush(); err != nil {
		return nil, err
	}

	return &SSTable{
		path:  path,
		index: index,
	}, nil
}

// OpenSSTable opens an existing SSTable
func OpenSSTable(path string) (*SSTable, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	index := make(map[string]int64)

	var offset int64 = 0

	// Build index
	for scanner.Scan() {
		line := scanner.Bytes()

		var entry SSTableEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, err
		}

		index[entry.Key] = offset
		offset += int64(len(line) + 1) // +1 for newline
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return &SSTable{
		path:  path,
		index: index,
	}, nil
}

// Get retrieves a value by key
func (s *SSTable) Get(key string) (string, bool, error) {
	// Check index first
	offset, exists := s.index[key]
	if !exists {
		return "", false, nil
	}

	// Open file and seek to offset
	file, err := os.Open(s.path)
	if err != nil {
		return "", false, err
	}
	defer file.Close()

	if _, err := file.Seek(offset, 0); err != nil {
		return "", false, err
	}

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		return "", false, fmt.Errorf("failed to read entry")
	}

	var entry SSTableEntry
	if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
		return "", false, err
	}

	if entry.Deleted {
		return "", false, nil
	}

	return entry.Value, true, nil
}

// Path returns the file path
func (s *SSTable) Path() string {
	return s.path
}

// AllEntries returns all entries (for compaction)
func (s *SSTable) AllEntries() ([]SSTableEntry, error) {
	file, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []SSTableEntry
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var entry SSTableEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}
