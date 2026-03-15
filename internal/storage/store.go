package storage

import (
	"fmt"
	"strings"
	"sync"
	"unicode"
)

// Store provides thread-safe key-value storage with durability
type Store struct {
	data map[string]string
	mu   sync.RWMutex
	wal  *WAL
}

// Verify Store implements Storage interface at compile time
var _ Storage = (*Store)(nil)

// NewStore creates a new Store instance with WAL
func NewStore(walPath string) (*Store, error) {
	// Open WAL
	wal, err := NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	// Create store
	store := &Store{
		data: make(map[string]string),
		wal:  wal,
	}

	// Replay WAL to recover state
	if err := store.recoverFromWAL(walPath); err != nil {
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}

	return store, nil
}

// recoverFromWAL replays all commands from the WAL
func (s *Store) recoverFromWAL(walPath string) error {
	commands, err := Replay(walPath)
	if err != nil {
		return err
	}

	fmt.Printf("Replaying %d commands from WAL...\n", len(commands))

	for _, cmd := range commands {
		s.applyCommand(cmd)
	}

	return nil
}

// cleanString removes control characters
func cleanString(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsPrint(r) || unicode.IsSpace(r) {
			return r
		}
		return -1 // Remove non-printable characters
	}, s)
}

// applyCommand applies a command without writing to WAL
// Used during recovery
func (s *Store) applyCommand(cmd string) {
	cmd = cleanString(strings.TrimSpace(cmd))
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return
	}

	operation := strings.ToUpper(parts[0])

	switch operation {
	case "PUT":
		if len(parts) >= 3 {
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			s.data[key] = value
		}
	case "DELETE":
		if len(parts) >= 2 {
			key := parts[1]
			delete(s.data, key)
		}
	}
}

// Put stores a key-value pair (with WAL)
func (s *Store) Put(key, value string) error {
	// Clean inputs
	key = cleanString(key)
	value = cleanString(value)

	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Write to WAL FIRST (before applying to memory)
	cmd := fmt.Sprintf("PUT %s %s", key, value)
	if err := s.wal.Append(cmd); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Now apply to memory
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value

	return nil
}

// Get retrieves a value by key (no WAL needed for reads)
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, exists := s.data[key]
	return value, exists
}

// Delete removes a key-value pair (with WAL)
func (s *Store) Delete(key string) error {
	// Clean input
	key = cleanString(key)

	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Write to WAL FIRST
	cmd := fmt.Sprintf("DELETE %s", key)
	if err := s.wal.Append(cmd); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Now delete from memory
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)

	return nil
}

// Close closes the WAL
func (s *Store) Close() error {
	return s.wal.Close()
}
