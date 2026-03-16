package storage

import (
	"sort"
	"sync"
)

// MemTable is an in-memory sorted key-value store
type MemTable struct {
	entries map[string]*entry
	mu      sync.RWMutex
	size    int // Size in bytes
}

type entry struct {
	key     string
	value   string
	deleted bool // Tombstone for deletes
}

// NewMemTable creates a new MemTable
func NewMemTable() *MemTable {
	return &MemTable{
		entries: make(map[string]*entry),
		size:    0,
	}
}

// Put adds or updates a key-value pair
func (m *MemTable) Put(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove old size if key exists
	if old, exists := m.entries[key]; exists {
		m.size -= len(old.key) + len(old.value)
	}

	m.entries[key] = &entry{
		key:     key,
		value:   value,
		deleted: false,
	}

	m.size += len(key) + len(value)
}

// Get retrieves a value by key
func (m *MemTable) Get(key string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	e, exists := m.entries[key]
	if !exists || e.deleted {
		return "", false
	}

	return e.value, true
}

// Delete marks a key as deleted (tombstone)
func (m *MemTable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove old size if exists
	if old, exists := m.entries[key]; exists {
		m.size -= len(old.key) + len(old.value)
	}

	m.entries[key] = &entry{
		key:     key,
		value:   "",
		deleted: true,
	}

	m.size += len(key)
}

// Size returns the approximate size in bytes
func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// GetSortedEntries returns all entries sorted by key
// Used when flushing to SSTable
func (m *MemTable) GetSortedEntries() []entry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]entry, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, *e)
	}

	// Sort by key
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].key < entries[j].key
	})

	return entries
}

// Clear removes all entries (used after flush)
func (m *MemTable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = make(map[string]*entry)
	m.size = 0
}
