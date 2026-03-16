package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	// MemTable flush threshold (1MB)
	memTableMaxSize = 1 * 1024 * 1024

	// Max number of SSTables before compaction
	maxSSTables = 4
)

// LSMStore implements LSM tree storage
type LSMStore struct {
	dataDir  string
	memTable *MemTable
	sstables []*SSTable
	wal      *WAL
	mu       sync.RWMutex
	nextID   int // Next SSTable ID
}

// Verify LSMStore implements Storage interface
var _ Storage = (*LSMStore)(nil)

// NewLSMStore creates a new LSM tree store
func NewLSMStore(dataDir string) (*LSMStore, error) {
	// Create directory
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	// Open WAL
	walPath := filepath.Join(dataDir, "wal.log")
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	store := &LSMStore{
		dataDir:  dataDir,
		memTable: NewMemTable(),
		sstables: make([]*SSTable, 0),
		wal:      wal,
		nextID:   0,
	}

	// Load existing SSTables
	if err := store.loadSSTables(); err != nil {
		return nil, err
	}

	// Replay WAL
	if err := store.recoverFromWAL(walPath); err != nil {
		return nil, err
	}

	return store, nil
}

// loadSSTables loads existing SSTable files
func (l *LSMStore) loadSSTables() error {
	files, err := os.ReadDir(l.dataDir)
	if err != nil {
		return err
	}

	for _, file := range files {
		if strings.HasPrefix(file.Name(), "sstable_") && strings.HasSuffix(file.Name(), ".db") {
			path := filepath.Join(l.dataDir, file.Name())
			sstable, err := OpenSSTable(path)
			if err != nil {
				return err
			}
			l.sstables = append(l.sstables, sstable)

			// Track highest ID
			var id int
			fmt.Sscanf(file.Name(), "sstable_%04d.db", &id)
			if id >= l.nextID {
				l.nextID = id + 1
			}
		}
	}

	fmt.Printf("Loaded %d SSTables\n", len(l.sstables))
	return nil
}

// recoverFromWAL replays WAL to rebuild memtable
func (l *LSMStore) recoverFromWAL(walPath string) error {
	commands, err := Replay(walPath)
	if err != nil {
		return err
	}

	fmt.Printf("Replaying %d commands from WAL...\n", len(commands))

	for _, cmd := range commands {
		parts := strings.Fields(cmd)
		if len(parts) == 0 {
			continue
		}

		operation := strings.ToUpper(parts[0])

		switch operation {
		case "PUT":
			if len(parts) >= 3 {
				key := parts[1]
				value := strings.Join(parts[2:], " ")
				l.memTable.Put(key, value)
			}
		case "DELETE":
			if len(parts) >= 2 {
				key := parts[1]
				l.memTable.Delete(key)
			}
		}
	}

	return nil
}

// Put stores a key-value pair
func (l *LSMStore) Put(key, value string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Write to WAL first
	cmd := fmt.Sprintf("PUT %s %s", key, value)
	if err := l.wal.Append(cmd); err != nil {
		return err
	}

	// Add to memtable
	l.memTable.Put(key, value)

	// Check if memtable is full
	if l.memTable.Size() >= memTableMaxSize {
		if err := l.flushMemTable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
	}

	return nil
}

// Get retrieves a value by key
func (l *LSMStore) Get(key string) (string, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Check memtable first (most recent data)
	if value, found := l.memTable.Get(key); found {
		return value, true
	}

	// Check SSTables (newest to oldest)
	for i := len(l.sstables) - 1; i >= 0; i-- {
		value, found, err := l.sstables[i].Get(key)
		if err != nil {
			fmt.Printf("Error reading from SSTable: %v\n", err)
			continue
		}
		if found {
			return value, true
		}
	}

	return "", false
}

// Delete removes a key
func (l *LSMStore) Delete(key string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Write to WAL first
	cmd := fmt.Sprintf("DELETE %s", key)
	if err := l.wal.Append(cmd); err != nil {
		return err
	}

	// Add tombstone to memtable
	l.memTable.Delete(key)

	// Check if memtable is full
	if l.memTable.Size() >= memTableMaxSize {
		if err := l.flushMemTable(); err != nil {
			return fmt.Errorf("failed to flush memtable: %w", err)
		}
	}

	return nil
}

// flushMemTable writes memtable to disk as SSTable
func (l *LSMStore) flushMemTable() error {
	fmt.Println("Flushing memtable to SSTable...")

	// Get sorted entries
	entries := l.memTable.GetSortedEntries()

	if len(entries) == 0 {
		return nil
	}

	// Create SSTable
	sstable, err := NewSSTable(l.dataDir, l.nextID, entries)
	if err != nil {
		return err
	}

	l.nextID++
	l.sstables = append(l.sstables, sstable)

	// Clear memtable
	l.memTable.Clear()

	// Clear WAL (data is now in SSTable)
	l.wal.Close()
	walPath := filepath.Join(l.dataDir, "wal.log")
	os.Remove(walPath)

	// Reopen WAL
	wal, err := NewWAL(walPath)
	if err != nil {
		return err
	}
	l.wal = wal

	fmt.Printf("Flushed to SSTable (now have %d SSTables)\n", len(l.sstables))

	// Check if compaction needed
	if len(l.sstables) >= maxSSTables {
		if err := l.compact(); err != nil {
			fmt.Printf("Compaction failed: %v\n", err)
		}
	}

	return nil
}

// compact merges SSTables
func (l *LSMStore) compact() error {
	fmt.Println("Compacting SSTables...")

	// Collect all entries from all SSTables
	allEntries := make(map[string]SSTableEntry)

	for _, sstable := range l.sstables {
		entries, err := sstable.AllEntries()
		if err != nil {
			return err
		}

		// Newer entries override older ones
		for _, entry := range entries {
			allEntries[entry.Key] = entry
		}
	}

	// Convert to sorted slice
	var sortedEntries []entry
	for _, e := range allEntries {
		sortedEntries = append(sortedEntries, entry{
			key:     e.Key,
			value:   e.Value,
			deleted: e.Deleted,
		})
	}

	sort.Slice(sortedEntries, func(i, j int) bool {
		return sortedEntries[i].key < sortedEntries[j].key
	})

	// Delete old SSTables
	for _, sstable := range l.sstables {
		os.Remove(sstable.Path())
	}

	// Create new compacted SSTable
	sstable, err := NewSSTable(l.dataDir, l.nextID, sortedEntries)
	if err != nil {
		return err
	}

	l.nextID++
	l.sstables = []*SSTable{sstable}

	fmt.Printf("Compaction complete (merged to 1 SSTable)\n")
	return nil
}

// Close closes the store
func (l *LSMStore) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Flush memtable if it has data
	if l.memTable.Size() > 0 {
		l.flushMemTable()
	}

	return l.wal.Close()
}
