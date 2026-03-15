package main

import (
	"fmt"
	"log"

	"github.com/Xelane/Capstone/internal/server"
	"github.com/Xelane/Capstone/internal/storage"
)

func main() {
	// Create storage
	store := storage.NewStore()

	// Create and start server
	srv := server.New(":8080", store)

	fmt.Println("Key-Value Server listening on :8080")
	fmt.Println("Commands: PUT key value | GET key | DELETE key")

	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
