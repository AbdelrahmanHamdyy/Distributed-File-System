package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	// Connect to server
	conn, err := net.Dial("tcp", "localhost:30000")
	if err != nil {
		fmt.Println("Error connecting:", err.Error())
		return
	}
	defer conn.Close()

	// Open the .mp4 file to be sent
	file, err := os.Open("test.mp4") // Change "example.mp4" to the path of the .mp4 file you want to send
	if err != nil {
		fmt.Println("Error opening file:", err.Error())
		return
	}
	defer file.Close()

	// Send file data to server
	_, err = io.Copy(conn, file)
	if err != nil {
		fmt.Println("Error sending file:", err.Error())
		return
	}

	fmt.Println("File sent successfully!")
}
