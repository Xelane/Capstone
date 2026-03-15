package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Xelane/Capstone/internal/server"
	"github.com/Xelane/Capstone/internal/storage"
)

func main() {
	// Create storage with WAL
	store, err := storage.NewStore("data/wal.log")
	if err != nil {
		log.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Create and start server
	srv := server.New(":8080", store)

	fmt.Println("Key-Value Server listening on :8080")
	fmt.Println("Commands: PUT key value | GET key | DELETE key")
	fmt.Println("WAL enabled at data/wal.log")

	// Handle graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		fmt.Println("\nShutting down gracefully...")
		store.Close()
		os.Exit(0)
	}()

	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
