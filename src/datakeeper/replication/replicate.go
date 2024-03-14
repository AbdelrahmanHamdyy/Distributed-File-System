package replication

import (
	"context"
	"log"

	pb "wireless_lab_1/grpc/capitalize" // Import the generated package

	"google.golang.org/grpc"
)

// ReplicationClient represents a gRPC client for replicating files to other Data Keeper nodes.
type ReplicationClient struct {
	conn    *grpc.ClientConn
	service pb.DataKeeperServiceClient
}

// NewReplicationClient creates a new gRPC client for replication.
func NewReplicationClient(address string) (*ReplicationClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := pb.NewDataKeeperServiceClient(conn)
	return &ReplicationClient{conn, client}, nil
}

// Close closes the gRPC client connection.
func (c *ReplicationClient) Close() {
	c.conn.Close()
}

// ReplicateFile replicates a file to other Data Keeper nodes as per the replication algorithm.
func (c *ReplicationClient) ReplicateFile(ctx context.Context, file *pb.FileMetadata, targetNodes []string) error {
	for _, node := range targetNodes {
		_, err := c.service.Store(ctx, file)
		if err != nil {
			log.Printf("Failed to replicate file to node %s: %v", node, err)
		} else {
			log.Printf("File replicated successfully to node %s", node)
		}
	}
	return nil
}
