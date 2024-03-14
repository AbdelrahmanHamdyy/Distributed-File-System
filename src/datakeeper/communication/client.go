package communication

import (
	"context"
	"log"

	pb "datakeeper/datakeeper"

	"google.golang.org/grpc"
)

// DataKeeperClient represents a gRPC client for communicating with the Data Keeper node.
type DataKeeperClient struct {
	conn    *grpc.ClientConn
	service pb.DataKeeperServiceClient
}

// NewDataKeeperClient creates a new gRPC client for the Data Keeper node.
func NewDataKeeperClient(address string) (*DataKeeperClient, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := pb.NewDataKeeperServiceClient(conn)
	return &DataKeeperClient{conn, client}, nil
}

// Close closes the gRPC client connection.
func (c *DataKeeperClient) Close() {
	c.conn.Close()
}

// StoreFile sends a request to store a file to the Data Keeper node.
func (c *DataKeeperClient) StoreFile(ctx context.Context, metadata *pb.FileMetadata) error {
	_, err := c.service.Store(ctx, metadata)
	if err != nil {
		log.Printf("Failed to store file: %v", err)
		return err
	}
	log.Printf("File stored successfully: %s", metadata.Name)
	return nil
}

// RetrieveFile sends a request to retrieve a file from the Data Keeper node.
func (c *DataKeeperClient) RetrieveFile(ctx context.Context, filename string) (*pb.FileMetadata, error) {
	resp, err := c.service.Retrieve(ctx, &pb.StringValue{Value: filename})
	if err != nil {
		log.Printf("Failed to retrieve file: %v", err)
		return nil, err
	}
	log.Printf("File retrieved successfully: %s", filename)
	return resp, nil
}

// SendKeepalivePing sends a keepalive ping to the Data Keeper node.
func (c *DataKeeperClient) SendKeepalivePing(ctx context.Context, nodeID string) error {
	_, err := c.service.SendPing(ctx, &pb.KeepalivePing{NodeID: nodeID})
	if err != nil {
		log.Printf("Failed to send keepalive ping: %v", err)
		return err
	}
	log.Printf("Keepalive ping sent successfully to node: %s", nodeID)
	return nil
}
