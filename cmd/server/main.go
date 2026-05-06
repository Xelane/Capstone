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
	"github.com/Xelane/Capstone/internal/web"
)

func main() {
	// Command line flags
	nodeID := flag.String("id", "node1", "Node ID")
	configPath := flag.String("config", "config/cluster.yaml", "Cluster config file")
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

	// Set replication callback
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

	// Start cluster node
	if err := clusterNode.Start(); err != nil {
		log.Fatal("Failed to start cluster node:", err)
	}
	defer clusterNode.Stop()

	// Determine dashboard port based on node ID
	dashboardPort := "7001"
	switch *nodeID {
	case "node1":
		dashboardPort = "7001"
	case "node2":
		dashboardPort = "7002"
	case "node3":
		dashboardPort = "7003"
	}

	// Start dashboard
	dashboard := web.NewDashboard(dashboardPort, func() web.NodeStatus {
		return web.NodeStatus{
			NodeID:      *nodeID,
			State:       clusterNode.GetState(),
			CurrentTerm: clusterNode.GetTerm(),
			AlivePeers:  clusterNode.GetAlivePeers(),
			TotalPeers:  len(peers),
		}
	})
	dashboard.Start()

	// Create and start client server
	srv := server.New(":"+nodeConfig.ClientPort, store)

	// Wire up replication
	srv.Handler.SetReplicationFunc(func(key, value, op string) error {
		return clusterNode.ReplicateToFollowers(key, value, op)
	})

	// Wire up leader check
	srv.Handler.SetIsLeaderFunc(func() bool {
		return clusterNode.IsLeader()
	})

	fmt.Printf("========================================\n")
	fmt.Printf("Node ID: %s\n", *nodeID)
	fmt.Printf("Client port: %s (for PUT/GET/DELETE)\n", nodeConfig.ClientPort)
	fmt.Printf("Cluster port: %s (for Raft)\n", nodeConfig.Address)
	fmt.Printf("Dashboard: http://localhost:%s\n", dashboardPort)
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
