// main.go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"./communication"
	"./keepalive"
	"./replication"
)

func main() {
	// Initialize gRPC server for communication
	go func() {
		if err := communication.StartServer("50051"); err != nil {
			log.Fatalf("Failed to start gRPC server: %v", err)
		}
	}()

	// Initialize keepalive client
	keepaliveClient, err := keepalive.NewKeepaliveClient("master-tracker-address", "data-keeper-id")
	if err != nil {
		log.Fatalf("Failed to create keepalive client: %v", err)
	}
	defer keepaliveClient.Close()

	// Send keepalive pings to Master Tracker every second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go keepaliveClient.SendPing(ctx, time.Second)

	// Initialize replication client
	replicationClient, err := replication.NewReplicationClient("data-keeper-address")
	if err != nil {
		log.Fatalf("Failed to create replication client: %v", err)
	}
	defer replicationClient.Close()

	// Implement replication logic here

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down...")
}
