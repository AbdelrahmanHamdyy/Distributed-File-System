package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"src/DataClient/DataClient/filetransfer" // Import the generated package

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

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

// function handle connection
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

// function download file from server
func downloadFile(dataKeeperPort string) {
	// Listen for incoming connections on port 8080
	listener, err := net.Listen("tcp", dataKeeperPort)
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

// filetransfer service

func test() {
	conn, err := grpc.Dial("localhost:8081", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	// Create a client for the PortNumberService
	portClient := filetransfer.NewPortNumberServiceClient(conn)

	// Send a port number to the server
	port := "8080" // Example port number
	portResponse, err := portClient.SendPortNumber(context.Background(), &filetransfer.PortNumberRequest{PortNumber: port})
	if err != nil {
		log.Fatalf("could not send port number: %v", err)
	}

	// Print the response from the server
	if portResponse.Success {
		fmt.Println("Port number sent successfully")
	} else {
		fmt.Println("Failed to send port number")
	}

	// Create a client for the SuccessService
	successClient := filetransfer.NewSuccessServiceClient(conn)

	// Report success to the server
	successResponse, err := successClient.ReportSuccess(context.Background(), &filetransfer.SuccessRequest{Success: true})
	if err != nil {
		log.Fatalf("could not report success: %v", err)
	}

	// Print the response from the server
	if successResponse.Success {
		fmt.Println("Success reported to the server")
	} else {
		fmt.Println("Failed to report success to the server")
	}
}

////////////////////////PROTO//////////////////////////////////
// Define a struct to implement the PortNumberService server
var portNum string = ""

type portNumberServer struct {
	filetransfer.UnimplementedPortNumberServiceServer
}

// Implement the SendPortNumber RPC method
func (s *portNumberServer) SendPortNumber(ctx context.Context, request *filetransfer.PortNumberRequest) (*filetransfer.SuccessResponse, error) {
	port := request.GetPortNumber()
	fmt.Println("Received port number:", port)

	// Perform any necessary processing here
	portNum = port

	return &filetransfer.SuccessResponse{Success: true}, nil
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

	// Perform any necessary processing here

	return &filetransfer.SuccessResponse{Success: true}, nil
}

func myServer() {
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

///////////////////////////////////////////////////////
func main() {

	///////////////////
	// test()
	go myServer()
	//////////////////
	// Load the environment variables from the .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Retrieve the value of the environment variable named "MASTER_PORT"
	masterPort := os.Getenv("MASTER_PORT")
	myPortNumber := os.Getenv("CLIENT_PORT")
	fmt.Println("Master Port:", masterPort)

	conn, err := grpc.Dial(masterPort, grpc.WithInsecure())
	if err != nil {
		fmt.Println("did not connect:", err)
		return
	}
	defer conn.Close()
	// c := pb.NewTextServiceClient(conn)

	// Read input from user
	userChoice := getUserChoice()

	// resp := &pb.TextResponse{}
	err = nil
	if userChoice == "1" {
		fmt.Println("You chose to upload a file.")
		// Call your upload file function here
		/////////////////////////////////////////////////////////////////////////////////
		// fake rpc will be replaced with the actual rpc call will get the port number of the data keeper
		// resp, err = c.Capitalize(context.Background(), &pb.TextRequest{Text: userChoice})
		// if err != nil {
		// 	fmt.Println("Error calling Capitalize:", err)
		// 	return
		// }
		// dataKeeperPort := resp.GetCapitalizedText()
		///////////////////////////////////////////////////////////////////////////////////
		//  data keeper port number
		fmt.Println("Your data keeper port number :")

		// Ask the user for the file path
		fmt.Print("Enter the file path: ")
		var filePath string
		fmt.Scanln(&filePath)
		_, err := os.Stat(filePath)
		if os.IsNotExist(err) {
			fmt.Println("File does not exist.")
			return
		}
		// print the file path
		fmt.Println("Your File path:", filePath)
		////////////////////////////////////////////////////////
		// request data keeper for uploading the file
		fmt.Println("Connecting to the data keeper...")
		respK := true
		////////////////////////////////////////////////////////
		if respK {
			// Connect to the data keeper
			fmt.Println("Uploading file...")
			// uploadFile(filePath, dataKeeperPort)
			//////////////////////////////////////
		} else {
			fmt.Println("Error connecting to the data keeper")
		}

	} else {
		fmt.Println("You chose to download a file.")
		// Call your download file function here
		// ask the user for the file name
		fmt.Print("Enter the file name: ")
		var fileName string
		fmt.Scanln(&fileName)
		// print the file name
		fmt.Println("Your File name:", fileName)
		// fake rpc will be replaced with the actual rpc call will get the port number of the data keeper
		// resp, err = c.Capitalize(context.Background(), &pb.TextRequest{Text: userChoice})
		// if err != nil {
		// 	fmt.Println("Error calling Capitalize:", err)
		// 	return
		// }
		// dataKeeperPort := resp.GetCapitalizedText()
		//  data keeper port number
		fmt.Println("Your data keeper port number :")
		// Connect to the data keeper
		fmt.Println("Connecting to the data keeper...")
		// download file from the data keeper
		//////////////////////////////////////
		//
		//  here will request the data keeper to download the file
		// where dataKeeperPort is the port number of the data keeper
		// and fileName is the name of the file to be downloaded
		// myPortNumber is the port number of the client
		////////////////////////////////////////
		// here will listen to the port number of the data keeper
		downloadFile(myPortNumber)
	}

}
