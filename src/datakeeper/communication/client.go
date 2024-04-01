package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	pb "src/grpc/datakeeper"

	ms "src/grpc"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

var fileName = "received.mp4"

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
 
// Implement the FileSaveService gRPC


// Close closes the gRPC client connection.
func (c *DataKeeperClient) Close() {
	c.conn.Close()
}

// define a main function
func waitAndPrint(id int) {
	for {
		time.Sleep(time.Second)
		// fmt.Println("1 second has passed", portNumber)
		// call service and send my portnumber
		err2 := godotenv.Load()
		if err2 != nil {
			log.Fatal("Error loading .env file")
		}
		masterPort := os.Getenv("MASTER_PORT")
		conn, err := grpc.Dial(masterPort, grpc.WithInsecure())
		if err != nil {
			fmt.Println("did not connect:", err)
			return
		}
		defer conn.Close()
		c := ms.NewMasterTrackerServiceClient(conn)

		// Calling RegisterFile service
		_, err = c.Heartbeat(context.Background(), &ms.HeartbeatRequest{DataNodeId: int32(id)})
		if err != nil {
			fmt.Println("Error calling Heartbeat:", err)
			return
		}
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Create a new file to save the received .mp4 file
	file, err := os.Create(fileName + ".mp4")
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

	fmt.Println("File received and saved:" , fileName + ".mp4")

	err = godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Connecting with master
	masterPort := os.Getenv("MASTER_PORT")
	fmt.Printf("Master port: %s\n", masterPort)
	conn2, err2 := grpc.Dial(masterPort, grpc.WithInsecure())
	if err2 != nil {
		fmt.Println("did not connect:", err2)
		return
	}
	defer conn2.Close()
	c := ms.NewMasterTrackerServiceClient(conn2)

	// Calling RegisterFile service
	_, err = c.RegisterFile(context.Background(), &ms.RegisterFileRequest{
		FileName: fileName,
		DataNodeId: 0,
		FilePath: fileName,
	})
	if err != nil {
		fmt.Println("Error calling Capitalize:", err)
		return
	}
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
	fmt.Println("[DOWNLOAD] Server started. Listening on port", port)

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
	pb.UnimplementedDataKeeperServiceServer
}

func uploadFileToPort(filePath string, clientPort string) {
	conn, err := net.Dial("tcp", clientPort)
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
func (s *server) TransferFile(ctx context.Context, req *pb.FilePortRequest) (*pb.SuccessResponse, error) {
	log.Printf("Received file %s to transfer on port %s\n", req.Filename, req.PortNumber)
	// Here you can implement your file transfer logic
	// For demonstration purposes, let's just return a success response
	/////////////////////////////////////////////////////////////////
	// here must be replaced with the correct file path
	filepath := req.Filename
	//////////////////////////////
	uploadFileToPort(filepath, req.PortNumber)
	////////////////////////////////////////////////////////////////
	return &pb.SuccessResponse{Success: true}, nil
}

type fileSaveNameServer struct {
	pb.UnimplementedFileSaveServiceServer
}

func (s *fileSaveNameServer) SaveFile(ctx context.Context, req *pb.FileSaveRequest) (*pb.SuccessResponse, error) {
	log.Printf("Received file %s to save\n", req.Filename)
	fileName = req.Filename
	////////////////////////////////////////////////////////////////
	return &pb.SuccessResponse{Success: true}, nil
}

func uploadFile(port string) {
	// Connect to the server
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterDataKeeperServiceServer(s, &server{})
	pb.RegisterFileSaveServiceServer(s, &fileSaveNameServer{})
	
	log.Println("[UPLOAD] Server started. Listening on port", port)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
func main() {
	fmt.Println("Hello, playground")
	if len(os.Args) != 4 {
			fmt.Println("Usage: program_name id arg1 arg2")
			return
	}

	id := os.Args[1]
	portNumber1 := os.Args[2]
	portNumber2 := os.Args[3]

	fmt.Println("ID:", id)
	fmt.Println("Download port number:", portNumber1)
	fmt.Println("Upload port number:", portNumber2)

	idInt, err := strconv.Atoi(id)
	if err != nil {
		fmt.Println("Invalid id:", err)
		return
	}
	go waitAndPrint(idInt)
	//////////////////////////////////////////
	// download from a client
	go listenForDownload(portNumber1)
	//////////////////////////////////////////
	// upload to client
	go uploadFile(portNumber2)

	for {
	}
}
