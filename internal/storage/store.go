package storage

import "sync"

// Store provides thread-safe in-memory key-value storage
type Store struct {
	data map[string]string
	mu   sync.RWMutex
}

// NewStore creates a new Store instance
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Put stores a key-value pair
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get retrieves a value by key
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.data[key]
	return value, exists
}

// Delete removes a key-value pair
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}
