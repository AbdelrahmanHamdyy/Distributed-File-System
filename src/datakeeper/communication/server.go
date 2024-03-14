package communication

import (
	"context"
	"log"
	"net"

	pb "datakeeper/datakeeper"

	"google.golang.org/grpc"
)

// Server implements the gRPC server for communication with the Master Tracker.
type Server struct{}

// Store implements the Store RPC method.
func (s *Server) Store(ctx context.Context, req *pb.FileMetadata) (*pb.Empty, error) {
	// Implement file storage logic
	log.Printf("Storing file: %s\n", req.Name)
	// Example: Save file to disk or database
	return &pb.Empty{}, nil
}

// Retrieve implements the Retrieve RPC method.
func (s *Server) Retrieve(ctx context.Context, req *pb.StringValue) (*pb.FileMetadata, error) {
	// Implement file retrieval logic
	log.Printf("Retrieving file: %s\n", req.Value)
	//  Retrieve file from disk or database
	return &pb.FileMetadata{}, nil
}

// SendPing implements the SendPing RPC method.
func (s *Server) SendPing(ctx context.Context, req *pb.KeepalivePing) (*pb.Empty, error) {
	// Implement keepalive ping logic
	log.Printf("Received ping from node: %s\n", req.NodeID)
	// Update node status
	return &pb.Empty{}, nil
}

// StartServer starts the gRPC server.
func StartServer(port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterDataKeeperServiceServer(s, &Server{})
	log.Printf("Data Keeper server started on port %s", port)
	if err := s.Serve(lis); err != nil {
		return err
	}
	return nil
}
