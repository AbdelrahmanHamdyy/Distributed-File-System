package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"wireless_lab_1/grpc/filetransfer"

	"google.golang.org/grpc"
)

// Define a struct to implement the PortNumberService server
type portNumberServer struct{
		filetransfer.UnimplementedPortNumberServiceServer
}

// Implement the SendPortNumber RPC method
func (s *portNumberServer) SendPortNumber(ctx context.Context, request *filetransfer.PortNumberRequest) (*filetransfer.SuccessResponse, error) {
    port := request.GetPortNumber()
    fmt.Println("Received port number:", port)
    
    // Perform any necessary processing here
    
    return &filetransfer.SuccessResponse{Success: true}, nil
}

// Define a struct to implement the SuccessService server
type successServer struct{
		filetransfer.UnimplementedSuccessServiceServer
}

// Implement the ReportSuccess RPC method
func (s *successServer) ReportSuccess(ctx context.Context, request *filetransfer.SuccessRequest) (*filetransfer.SuccessResponse, error) {
    success := request.GetSuccess()
    if success {
        fmt.Println("Operation was successful")
    } else {
        fmt.Println("Operation failed")
    }
    
    // Perform any necessary processing here
    
    return &filetransfer.SuccessResponse{Success: true}, nil
}

func main() {
    // Create a TCP listener on port 8080
    lis, err := net.Listen("tcp", ":8081")
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }
    
    // Create a gRPC server
    grpcServer := grpc.NewServer()
    
    // Register the PortNumberService server
    filetransfer.RegisterPortNumberServiceServer(grpcServer, &portNumberServer{})
    
    // Register the SuccessService server
    filetransfer.RegisterSuccessServiceServer(grpcServer, &successServer{})
    
		// print 
		fmt.Println("Server started. Listening on port 8081...")
    // Start the gRPC server
    if err := grpcServer.Serve(lis); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}
