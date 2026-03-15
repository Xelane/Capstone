package storage

import (
	"bufio"
	"fmt"
	"os"
)

// WAL provides durable logging of operations
type WAL struct {
	file *os.File
	path string
}

// NewWAL creates or opens a write-ahead log
func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	return &WAL{
		file: file,
		path: path,
	}, nil
}

// Append writes a command to the log
func (w *WAL) Append(command string) error {
	_, err := fmt.Fprintf(w.file, "%s\n", command)
	if err != nil {
		return err
	}

	// CRITICAL: Force write to disk for durability
	return w.file.Sync()
}

// Close closes the WAL file
func (w *WAL) Close() error {
	return w.file.Close()
}

// Replay reads the entire log and returns all commands
func Replay(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil // No WAL file yet, that's ok
		}
		return nil, fmt.Errorf("failed to open WAL for replay: %w", err)
	}
	defer file.Close()

	var commands []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		commands = append(commands, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading WAL: %w", err)
	}

	return commands, nil
}
