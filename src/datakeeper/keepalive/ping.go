package keepalive

import (
	"context"
	"log"
	"time"

	pb "wireless_lab_1/grpc/capitalize" // Import the generated package

	"google.golang.org/grpc"
)

// KeepaliveClient represents a gRPC client for sending keepalive pings.
type KeepaliveClient struct {
	conn    *grpc.ClientConn
	service pb.MasterTrackerServiceClient
	nodeID  string // ID of the Data Keeper node
}

// NewKeepaliveClient creates a new gRPC client for sending keepalive pings.
func NewKeepaliveClient(address, nodeID string) (*KeepaliveClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := pb.NewMasterTrackerServiceClient(conn)
	return &KeepaliveClient{conn, client, nodeID}, nil
}

// Close closes the gRPC client connection.
func (c *KeepaliveClient) Close() {
	c.conn.Close()
}

// SendPing sends a keepalive ping to the Master Tracker node at regular intervals.
func (c *KeepaliveClient) SendPing(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_, err := c.service.SendKeepalivePing(ctx, &pb.KeepalivePing{NodeID: c.nodeID})
			if err != nil {
				log.Printf("Failed to send keepalive ping: %v", err)
			} else {
				log.Println("Keepalive ping sent successfully")
			}
		case <-ctx.Done():
			return
		}
	}
}
