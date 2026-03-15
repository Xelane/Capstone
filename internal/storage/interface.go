package storage

// Storage defines the interface for key-value storage operations
type Storage interface {
	Put(key, value string) error
	Get(key string) (string, bool)
	Delete(key string) error
	Close() error
}
