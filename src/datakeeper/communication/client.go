package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"

	pb "src/grpc/datakeeper"
	"src/grpc/filetransfer"

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
	// resp, err := c.service.Retrieve(ctx, &pb.FileMetadata{Value: filename})
	// if err != nil {
	// 	log.Printf("Failed to retrieve file: %v", err)
	// 	return nil, err
	// }
	// log.Printf("File retrieved successfully: %s", filename)
	// return resp, nil
	return &pb.FileMetadata{}, nil
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

// define a main function
func waitAndPrint(portNumber string) {
	for {
		time.Sleep(time.Second)
		fmt.Println("1 second has passed", portNumber)
		// call service and send my portnumber
	}
}
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

// function listen on a port for downloading
func listenForDownload(port string) {
	// Listen on the given port
	listener, err := net.Listen("tcp", port)
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

type server struct {
	filetransfer.UnimplementedFileTransferServiceServer
}

func uploadFileToPort(filePath string, dataKeeperPort string) {
	conn, err := net.Dial("tcp", dataKeeperPort)
	if err != nil {
		fmt.Println("Error connecting:", err.Error())
		return
	}
	defer conn.Close()

	// Open the .mp4 file to be sent
	file, err := os.Open(filePath) // Change "example.mp4" to the path of the .mp4 file you want to send
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
func (s *server) TransferFile(ctx context.Context, req *filetransfer.FilePortRequest) (*filetransfer.SuccessResponse, error) {
	log.Printf("Received file %s to transfer on port %s\n", req.Filename, req.PortNumber)
	// Here you can implement your file transfer logic
	// For demonstration purposes, let's just return a success response
	/////////////////////////////////////////////////////////////////
	// here must be replaced with the correct file path
	filepath := req.Filename
	//////////////////////////////
	uploadFileToPort(filepath, req.PortNumber)
	////////////////////////////////////////////////////////////////
	return &filetransfer.SuccessResponse{Success: true}, nil
}
func uploadFile(port string) {
	// Connect to the server
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	filetransfer.RegisterFileTransferServiceServer(s, &server{})
	log.Println("Server started. Listening on port 8080...")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
func main() {
	fmt.Println("Hello, playground")
	// for downloading
	portNumber1 := "localhost:50051"
	// for uploading
	portNumber2 := "localhost:50050"
	// 1- notify the master with alive message
	// call service and send my portnumber
	go waitAndPrint(portNumber1)
	//////////////////////////////////////////
	// download from a client
	go listenForDownload(portNumber1)
	//////////////////////////////////////////
	// upload to client
	go uploadFile(portNumber2)

	for {
	}
}
