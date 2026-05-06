package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	// Add port flag
	port := flag.String("port", "8001", "Server port to connect to")
	host := flag.String("host", "localhost", "Server host")
	flag.Parse()

	// Connect to server
	address := fmt.Sprintf("%s:%s", *host, *port)

	fmt.Printf("Connecting to %s...\n", address)

	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Printf("❌ Error connecting to %s: %v\n", address, err)
		fmt.Println("\nMake sure the server is running on that port!")
		return
	}
	defer conn.Close()

	fmt.Printf("✅ Connected to %s\n", address)
	fmt.Println("Commands: PUT key value | GET key | DELETE key")
	fmt.Println("Type 'quit' to exit")

	// Create reader for responses
	reader := bufio.NewReader(conn)

	// Read from stdin
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")

		// Read user input
		if !scanner.Scan() {
			break
		}

		command := scanner.Text()
		command = strings.TrimSpace(command)

		if command == "" {
			continue
		}

		if command == "quit" {
			fmt.Println("Goodbye!")
			break
		}

		// Send to server
		_, err := fmt.Fprintf(conn, "%s\n", command)
		if err != nil {
			fmt.Printf("❌ Error sending command: %v\n", err)
			break
		}

		// Read response
		response, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("❌ Error reading response: %v\n", err)
			fmt.Println("Server may have disconnected")
			break
		}

		// Print response
		fmt.Printf("Server: %s", response)
	}
}
