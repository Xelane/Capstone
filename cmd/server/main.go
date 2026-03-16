package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Xelane/Capstone/internal/cluster"
	"github.com/Xelane/Capstone/internal/server"
	"github.com/Xelane/Capstone/internal/storage"
)

func main() {
	// Command line flags
	nodeID := flag.String("id", "node1", "Node ID")
	configPath := flag.String("config", "config/cluster.yaml", "Cluster config file")
	leader := flag.Bool("leader", false, "Is this node the leader?")
	flag.Parse()

	// Load cluster configuration
	config, err := cluster.LoadConfig(*configPath)
	if err != nil {
		log.Fatal("Failed to load config:", err)
	}

	// Get this node's config
	nodeConfig, err := config.GetNode(*nodeID)
	if err != nil {
		log.Fatal("Node not found in config:", err)
	}

	// Create LSM store
	store, err := storage.NewLSMStore(nodeConfig.DataDir)
	if err != nil {
		log.Fatal("Failed to create store:", err)
	}
	defer store.Close()

	// Create cluster node
	peers := config.GetPeers(*nodeID)
	clusterNode := cluster.NewNode(*nodeID, nodeConfig.Address, peers)

	// Set leader status
	clusterNode.SetLeader(*leader)

	// Set replication callback (how followers apply replicated data)
	clusterNode.SetReplicateFunc(func(key, value, op string) error {
		switch op {
		case "PUT":
			return store.Put(key, value)
		case "DELETE":
			return store.Delete(key)
		default:
			return fmt.Errorf("unknown operation: %s", op)
		}
	})

	// Start cluster node (peer-to-peer communication)
	if err := clusterNode.Start(); err != nil {
		log.Fatal("Failed to start cluster node:", err)
	}
	defer clusterNode.Stop()

	// Create and start client server
	srv := server.New(":"+nodeConfig.ClientPort, store)

	// Wire up replication to the handler
	srv.Handler.SetReplicationFunc(func(key, value, op string) error {
		return clusterNode.ReplicateToFollowers(key, value, op)
	})

	// Wire up leader check
	srv.Handler.SetIsLeaderFunc(func() bool {
		return clusterNode.IsLeader()
	})

	fmt.Printf("========================================\n")
	fmt.Printf("Node ID: %s\n", *nodeID)
	if *leader {
		fmt.Printf("Role: LEADER\n")
	} else {
		fmt.Printf("Role: FOLLOWER\n")
	}
	fmt.Printf("Client port: %s (for PUT/GET/DELETE)\n", nodeConfig.ClientPort)
	fmt.Printf("Cluster port: %s (for peer communication)\n", nodeConfig.Address)
	fmt.Printf("Data directory: %s\n", nodeConfig.DataDir)
	fmt.Printf("========================================\n")

	// Handle graceful shutdown
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		<-sigChan

		fmt.Println("\nShutting down gracefully...")
		clusterNode.Stop()
		store.Close()
		os.Exit(0)
	}()

	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}
}
