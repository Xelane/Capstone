package main

import (
	"bufio"
	"fmt"
	"net"
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

	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)

	fmt.Println("Sending data to trigger memtable flush...")

	// Send 2000 entries
	for i := 1; i <= 2000; i++ {
		// Create a larger value (repeat string to make it bigger)
		value := strings.Repeat("This_is_a_longer_value_with_lots_of_text_", 20)
		value = fmt.Sprintf("%s_entry_%d", value, i)

		command := fmt.Sprintf("PUT key%d %s\n", i, value)

		// Send command
		_, err := writer.WriteString(command)
		if err != nil {
			fmt.Println("Error writing:", err)
			return
		}
		writer.Flush()

		// Read response
		response, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading response:", err)
			return
		}

		if i%100 == 0 {
			fmt.Printf("Sent %d entries... (Response: %s)", i, strings.TrimSpace(response))
		}
	}

	fmt.Println("\nDone! Check server output for 'Flushing memtable to SSTable...'")
}
