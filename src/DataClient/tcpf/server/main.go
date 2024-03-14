package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Create a new file to save the received .mp4 file
	file, err := os.Create("received.mp4")
	if err != nil {
		fmt.Println("Error creating file:", err.Error())
		return
	}
	defer file.Close()

	// Receive .mp4 file data from client and save it to the file
	_, err = io.Copy(file, conn)
	if err != nil {
		fmt.Println("Error receiving file:", err.Error())
		return
	}

	fmt.Println("File received and saved: received.mp4")
}

func main() {
	// Listen for incoming connections on port 8080
	listener, err := net.Listen("tcp", "localhost:30000")
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		return
	}
	defer listener.Close()
	fmt.Println("Server started. Listening on port 8080...")

	// Accept incoming connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			return
		}
		fmt.Println("Client connected:", conn.RemoteAddr())

		// Handle incoming connection in a separate goroutine
		go handleConnection(conn)
	}
}
