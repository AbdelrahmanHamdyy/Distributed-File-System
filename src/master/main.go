package main

import (
	"fmt"
	"net"

	pb "src/grpc" // Import the generated package

	"google.golang.org/grpc"
)

type masterServer struct {
	pb.UnimplementedMasterTrackerServiceServer
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("failed to listen:", err)
		return
	}
	s := grpc.NewServer()
	pb.RegisterMasterTrackerServiceServer(s, &masterServer{})
	fmt.Println("Server started. Listening on port 8080...")
	if err := s.Serve(lis); err != nil {
		fmt.Println("failed to serve:", err)
	}
}
