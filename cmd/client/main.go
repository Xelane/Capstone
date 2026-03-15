package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	// Connect to server
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connected to server. Type commands (PUT key value, GET key, DELETE key):")
	fmt.Println("Type 'quit' to exit")

	// Read from stdin
	scanner := bufio.NewScanner(os.Stdin)
	reader := bufio.NewReader(conn)

	for {
		fmt.Print("> ")

		// Read user input
		if !scanner.Scan() {
			break
		}
		command := scanner.Text()

		// Clean the command - remove any control characters
		command = strings.TrimSpace(command)

		if command == "" {
			continue
		}

		if command == "quit" {
			break
		}

		// Send to server
		fmt.Fprintf(conn, "%s\n", command)

		// Read response
		response, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading response:", err)
			break
		}

		fmt.Print("Server: ", response)
	}
}
