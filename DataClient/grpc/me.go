package main

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

func main() {
	// Load the environment variables from the .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Retrieve the value of the environment variable named "MASTER_PORT"
	masterPort := os.Getenv("MASTER_PORT")
	fmt.Println("Master Port:", masterPort)
}
