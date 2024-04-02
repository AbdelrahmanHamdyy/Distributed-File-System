package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"src/grpc/filetransfer" // Import the generated package

	pb "src/grpc/master"

	dk "src/grpc/datakeeper"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

var grpcPortNumber = ":3000"

func getUserChoice() string {
	var text string
	for {
		// Read input from user
		fmt.Println("For Uploading a file, please enter: 1")
		fmt.Println("For Downloading a file, please enter: 2")
		fmt.Print("Your choice: ")
		fmt.Scanln(&text)

		// Trim any leading or trailing whitespace
		text = strings.TrimSpace(text)

		// Check if the user wants to upload a file or download a file
		if text == "1" || text == "2" {
			return text // Return the user's choice
		}

		fmt.Println("Invalid input. Please enter either '1' or '2'.")
	}
}

// function upload file to server
func uploadFile(filePath string, dataKeeperPort string) {
	conn, err := net.Dial("tcp", dataKeeperPort)
	if err != nil {
		fmt.Println("Error connecting:", err.Error())
		return
	}
	defer conn.Close()

	// Open the .mp4 file to be sent
	file, err := os.Open(filePath)
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

// function handle connection
func handleConnection(conn net.Conn, fileName string) {
	defer conn.Close()

	// Create a new file to save the received .mp4 file
	file, err := os.Create("client/" + fileName + ".mp4")
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

	fmt.Println("File received and saved:", fileName + ".mp4")
}

// function download file from server
func downloadFile(port string, fileName string) {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		return
	}
	defer listener.Close()
	fmt.Println("TCP Server started. Listening on port", port)

	// Accept incoming connections
	conn, err := listener.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err.Error())
		return
	}
	fmt.Println("Client connected:", conn.RemoteAddr())

	// Handle incoming connection in a separate goroutine
	go handleConnection(conn, fileName)
}

type portNumberServer struct {
	filetransfer.UnimplementedPortNumberServiceServer
}

// Define a struct to implement the SuccessService server
type successServer struct {
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

	return &filetransfer.SuccessResponse{Success: true}, nil
}

func myServer() {
	lis, err := net.Listen("tcp", grpcPortNumber)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fmt.Printf("GRPC server started on port localhost%s\n", grpcPortNumber)

	// Create a gRPC server
	grpcServer := grpc.NewServer()

	// Register the PortNumberService server
	filetransfer.RegisterPortNumberServiceServer(grpcServer, &portNumberServer{})

	// Register the SuccessService server
	filetransfer.RegisterSuccessServiceServer(grpcServer, &successServer{})

	// Start the gRPC server
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func main() {
	// Start the gRPC server in a separate goroutine
	go myServer()

	// Load the environment variables from the .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	masterPort := os.Getenv("MASTER_PORT")
	myPortNumber := os.Getenv("CLIENT_PORT")

	conn, err := grpc.Dial(masterPort, grpc.WithInsecure())
	if err != nil {
		fmt.Println("did not connect:", err)
		return
	}
	defer conn.Close()
	c := pb.NewMasterTrackerServiceClient(conn)
	
	for {
		// Read input from user
		userChoice := getUserChoice()

		resp := &pb.UploadFileResponse{}
		err = nil
		if userChoice == "1" {
			resp, err = c.UploadFile(context.Background(), &pb.UploadFileRequest{
				ClientPort: "localhost" + grpcPortNumber,
			})
			if err != nil {
				fmt.Println("Error calling UploadFile:", err)
				return
			}
			dataKeeperPort := resp.GetUploadAddress()
			dataKeeperPortGrpc := resp.GetGrpcAddress()
			fmt.Println("Your data keeper port number:", dataKeeperPort)

			// Ask the user for the file path
			fmt.Print("Enter the file path: ")
			var filePath string
			fmt.Scanln(&filePath)

			// Ask the user for the saving filename
			fmt.Print("Enter the saving filename: ")
			var fileName string
			fmt.Scanln(&fileName)

			_, err := os.Stat(filePath)
			
			if os.IsNotExist(err) {
				fmt.Println("File does not exist.")
				return
			}

			// Connect to the data keeper
			conn1,err1 := grpc.Dial(dataKeeperPortGrpc, grpc.WithInsecure())
			if err1 != nil {
				fmt.Println("did not connect:", err1)
				return
			}
			defer conn1.Close()
			d := dk.NewFileSaveServiceClient(conn1)
			// for sending filename
			resp1, err1 := d.SaveFile(context.Background(), &dk.FileSaveRequest{Filename: fileName})
			if err1 != nil {
				fmt.Println("Error calling SaveFile:", err1)
				return
			}
			successMsg := resp1.GetSuccess()
			if !successMsg {
				fmt.Println("Error sending file name ")
			}
			uploadFile(filePath, dataKeeperPort)
			//////////////////////////////////////
		} else {
			fmt.Print("Enter the file name: ")
			var fileName string
			fmt.Scanln(&fileName)

			go downloadFile(myPortNumber, fileName)

			resp2 := &pb.DownloadFileResponse{}
			err = nil
			resp2, err = c.DownloadFile(context.Background(), &pb.DownloadFileRequest{FileName: fileName})
			if err != nil {
				fmt.Println("Error calling DownloadFile:", err)
				return
			}
			ports := resp2.GetAddresses()
			dataKeeperPort := ports[0]
			fmt.Println("Data keeper port:", dataKeeperPort)

			conn, err = grpc.Dial(dataKeeperPort, grpc.WithInsecure())
			if err != nil {
				fmt.Println("did not connect:", err)
				return
			}
			defer conn.Close()
			d := dk.NewDataKeeperServiceClient(conn)
			resp3, err3 := d.TransferFile(context.Background(), &dk.FilePortRequest{Filename: fileName, PortNumber: myPortNumber})
			if err3 != nil {
				fmt.Println("Error calling DownloadFile:", err3)
				return
			}
			successMsg := resp3.GetSuccess()
			if successMsg {
				fmt.Println("File downloaded successfully!")
			} else {
				fmt.Println("Error downloading file")
			}
		}
	}
}
