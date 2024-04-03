package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"

	pb "src/grpc/master" // Import the generated package

	cl "src/grpc/filetransfer"

	dk "src/grpc/datakeeper"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
)

var clientPort string

type masterServer struct {
	pb.UnimplementedMasterTrackerServiceServer
}

type dataNode struct {
	address    string
	downloadAddress string
	isAlive    bool
}
type FileMetadata struct {
	FileName   string // File name
	DataNodeId int32  // Data Keeper node where the file is stored
	FilePath   string // File path on the Data Keeper node
	Size 	   int64  // File size
}

var dataNodesHeartbeats = make(map[int32]int32)
var fileLookupTable = make([]FileMetadata, 0)
var dataNodeLookupTable = make(map[int32]dataNode)

func (s *masterServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	id := req.GetDataNodeId()
	dataNodesHeartbeats[id] += 1
	return &pb.HeartbeatResponse{}, nil
}

func (s *masterServer) UploadFile(ctx context.Context, req *pb.UploadFileRequest) (*pb.UploadFileResponse, error) {
	numDataNodes := len(dataNodeLookupTable)
	dataNodeId := int32(rand.Intn(numDataNodes))
	for !dataNodeLookupTable[dataNodeId].isAlive {
		dataNodeId = int32(rand.Intn(numDataNodes))
	}
	nodeAddr := dataNodeLookupTable[dataNodeId].address
	grpcAddr := dataNodeLookupTable[dataNodeId].downloadAddress
	clientPort = req.GetClientPort()
	return &pb.UploadFileResponse{UploadAddress: nodeAddr , GrpcAddress: grpcAddr }, nil
}

func notifyClient() {
	conn, err := grpc.Dial(clientPort, grpc.WithInsecure())
	if err != nil {
		fmt.Println("Did not connect:", err)
		return
	}
	defer conn.Close()
	c := cl.NewSuccessServiceClient(conn)
	resp, err := c.ReportSuccess(context.Background(), &cl.SuccessRequest{Success: true})
	if err != nil {
		fmt.Println("Error notifying the client:", err)
		return
	}
	fmt.Println("Client Status:", resp.GetSuccess())
}

func chooseOneRandomNode(dataNodeId int32) int32 {
	numDataNodes := len(dataNodeLookupTable)
	nodeId := int32(rand.Intn(numDataNodes))
	for nodeId == dataNodeId || !dataNodeLookupTable[nodeId].isAlive {
		nodeId = int32(rand.Intn(numDataNodes))
	}
	return int32(nodeId)
}

func chooseTwoRandomNodes(dataNodeId int32) []int32 {
	numDataNodes := len(dataNodeLookupTable)
	// Pick the first Id
	nodeIds := make([]int32, 2)
	nodeIds[0] = chooseOneRandomNode(dataNodeId)

	// Pick the second Id
	nodeId := int32(rand.Intn(numDataNodes))
	for nodeId == dataNodeId || nodeId == nodeIds[0] || !dataNodeLookupTable[nodeId].isAlive {
		nodeId = int32(rand.Intn(numDataNodes))
	}
	nodeIds[1] = int32(nodeId)

	return nodeIds
}

func getAliveNodesCount() int {
	count := 0
	for _, node := range dataNodeLookupTable {
		if node.isAlive {
			count++
		}
	}
	return count
}

func chooseNodesToReplicate(fileName string, dataNodeId int32){
	// Check that The number of alive data nodes must be atleast 3
	aliveCount := getAliveNodesCount()

	if aliveCount == 1 {
		fmt.Println("There is only one alive data node. Cannot replicate the file.")
		return
	} else if aliveCount == 2 {
		fmt.Println("There are only two alive data nodes. Replicating the file to the other node.")
		nodeId := chooseOneRandomNode(dataNodeId)
		conn, err := grpc.Dial(dataNodeLookupTable[dataNodeId].downloadAddress, grpc.WithInsecure())
		if err != nil {
			fmt.Println("Did not connect:", err)
			return
		}
		defer conn.Close()
		c := dk.NewDataKeeperServiceClient(conn)
		address := dataNodeLookupTable[nodeId].address
		grpcAddr := dataNodeLookupTable[nodeId].downloadAddress
		resp, err := c.ReplicateFile(context.Background(), &dk.ReplicateFileRequest{FileName: fileName, TcpAddr: address, GrpcAddr: grpcAddr})
		if err != nil {
			fmt.Println("Error sending to datakeeper:", err)
			return
		}
		fmt.Println("Data Node response:", resp.GetSuccess())
		return
	} else {
		nodeIds := chooseTwoRandomNodes(dataNodeId)
		fmt.Printf("Sending the 2 nodes %d & %d to Data node %d\n", nodeIds[0], nodeIds[1], dataNodeId)
		conn, err := grpc.Dial(dataNodeLookupTable[dataNodeId].downloadAddress, grpc.WithInsecure())
		if err != nil {
			fmt.Println("Did not connect:", err)
			return
		}
		defer conn.Close()
		c := dk.NewDataKeeperServiceClient(conn)
		address := dataNodeLookupTable[nodeIds[0]].address
		grpcAddr := dataNodeLookupTable[nodeIds[0]].downloadAddress
		resp, err := c.ReplicateFile(context.Background(), &dk.ReplicateFileRequest{FileName: fileName, TcpAddr: address, GrpcAddr: grpcAddr})
		if err != nil {
			fmt.Println("Error sending to datakeeper:", err)
			return
		}
		fmt.Println("First Data Node response:", resp.GetSuccess())
	
		address = dataNodeLookupTable[nodeIds[1]].address
		grpcAddr = dataNodeLookupTable[nodeIds[1]].downloadAddress
		resp, err = c.ReplicateFile(context.Background(), &dk.ReplicateFileRequest{FileName: fileName, TcpAddr: address, GrpcAddr: grpcAddr})
		if err != nil {
			fmt.Println("Error sending to datakeeper:", err)
			return
		}
		fmt.Println("Second Data Node response:", resp.GetSuccess())
	}
}

func (s *masterServer) RegisterFile(ctx context.Context, req *pb.RegisterFileRequest) (*pb.RegisterFileResponse, error) {
	fileName := req.GetFileName()
	fmt.Println("Saving file:", fileName)
	
	replicate := false
	// If fileName is found in the fileLookupTable, then the file is being replicated
	for _, file := range fileLookupTable {
		if file.FileName == fileName {
			replicate = true
			break
		}
	}

	dataNodeId := req.GetDataNodeId()
	filePath := req.GetFilePath()
	fileSize := req.GetFileSize()
	fileMetadata := FileMetadata{FileName: fileName, DataNodeId: dataNodeId, FilePath: filePath, Size: fileSize}
	fileLookupTable = append(fileLookupTable, fileMetadata)

	if !replicate {
		notifyClient()
		chooseNodesToReplicate(fileName, dataNodeId)
	}
	return &pb.RegisterFileResponse{}, nil
}

func (s *masterServer) DownloadFile(ctx context.Context, req *pb.DownloadFileRequest) (*pb.DownloadFileResponse, error) {
	fileName := req.GetFileName()
	addresses := make([]string, 0)
	fileSize := int64(0)
	for _, file := range fileLookupTable {
		if file.FileName == fileName && dataNodeLookupTable[file.DataNodeId].isAlive {
			addresses = append(addresses, dataNodeLookupTable[file.DataNodeId].downloadAddress)
			fileSize = file.Size
		}
	}
	return &pb.DownloadFileResponse{Addresses: addresses, FileSize: fileSize}, nil
}

func checkAliveDataNodes() {
	for {
		time.Sleep(2 * time.Second)
		for id := range dataNodesHeartbeats {
			node := dataNodeLookupTable[id]
			if dataNodesHeartbeats[id] == 0 {
				fmt.Printf("Node %d is dead\n", id)
				node.isAlive = false
			} else {
				fmt.Printf("Node %d is alive\n", id)
				node.isAlive = true
			}
			dataNodesHeartbeats[id] = 0
			dataNodeLookupTable[id] = node
		}
	}
}

func Replication() {
	for {
		time.Sleep(10 * time.Second)
		// Each file should exist on atleast 3 alive data nodes. If not, replicate it to one of the alive nodes.
		// create a map with key filename and value will be an array of ids of the data nodes where the file is stored
		fileMap := make(map[string][]int32)
		toBeRemoved := make(map[string][]int32)
		for _, file := range fileLookupTable {
			// call grpc to check if file still exists
			if dataNodeLookupTable[file.DataNodeId].isAlive {
				conn, err := grpc.Dial(dataNodeLookupTable[file.DataNodeId].downloadAddress, grpc.WithInsecure())
				if err != nil {
					fmt.Println("Did not connect:", err)
					continue
				}
				c := dk.NewDataKeeperServiceClient(conn)
				resp, err := c.CheckFileExists(context.Background(), &dk.CheckFileExistsRequest{Filepath: file.FilePath})
				if err != nil {
					fmt.Println("Error checking file:", err)
					continue
				}
				if resp.GetSuccess() {
					fileMap[file.FileName] = append(fileMap[file.FileName], file.DataNodeId)
				} else {
					fmt.Printf("File %s no longer exists on Data Keeper %d\n", file.FileName, file.DataNodeId)
					toBeRemoved[file.FileName] = append(toBeRemoved[file.FileName], file.DataNodeId)
				}
				conn.Close()
			}
		}

		// remove the files that no longer exist
		for fileName, nodes := range toBeRemoved {
			for _, node := range nodes {
				for i, file := range fileLookupTable {
					if file.FileName == fileName && file.DataNodeId == node {
						fileLookupTable = append(fileLookupTable[:i], fileLookupTable[i+1:]...)
					}
				}
			}
		}

		// print fileMap
		if len(fileMap) > 0 {
			fmt.Println("------File Map------")
			for key, value := range fileMap {
				fmt.Println(key, value)
			}
		}
		
		if len(fileLookupTable) > 0 {
			// print fileLookupTable
			fmt.Println("------File Lookup Table------")
			for _, file := range fileLookupTable {
				fmt.Println(file.FileName, file.DataNodeId)
			}
		}
		
		numDataNodes := len(dataNodeLookupTable)

		for fileName, nodes := range fileMap {
			if len(nodes) < 3 {
				// choose new data node ids for the fileName until the count is restored to 3, then notify the source and destination nodes to start copying
				if len(nodes) == 2 {
					aliveCount := getAliveNodesCount()
					if aliveCount == 2 {
						fmt.Print("[REPLICATION] There are only two alive data nodes.\n")
						continue
					}
					destinationId := int32(rand.Intn(numDataNodes))
					for destinationId == nodes[0] || destinationId == nodes[1] || !dataNodeLookupTable[destinationId].isAlive {
						destinationId = int32(rand.Intn(numDataNodes))
					}
					fmt.Printf("Destination ID: %d\n", destinationId)
					conn, err := grpc.Dial(dataNodeLookupTable[nodes[0]].downloadAddress, grpc.WithInsecure())
					if err != nil {
						fmt.Println("Did not connect:", err)
						continue
					}
					c := dk.NewDataKeeperServiceClient(conn)
					tcpAddr := dataNodeLookupTable[destinationId].address
					grpcAddr := dataNodeLookupTable[destinationId].downloadAddress
					_, err = c.ReplicateFile(context.Background(), &dk.ReplicateFileRequest{FileName: fileName, TcpAddr: tcpAddr, GrpcAddr: grpcAddr})
					if err != nil {
						fmt.Println("Error replicating file:", err)
						continue
					}
					conn.Close()
				} else if len(nodes) == 1 {
					chooseNodesToReplicate(fileName, nodes[0])
				} else {
					fmt.Println("[REPLICATION] File does not exist on any data node")
				}
			}
		}
	}
}

func (s *masterServer) Join(ctx context.Context, req *pb.JoinRequest) (*pb.SuccessResponse, error) {
	id := req.GetId()
	address := req.GetAddress()
	grpcAddress := req.GetGrpcAddress()
	// Check if the data node is already in the lookup table
	for dataNodeId, node := range dataNodeLookupTable {
		if dataNodeId == id && !node.isAlive {
			// If the data node is already in the lookup table, update the address and set isAlive to true
			node.address = address
			node.downloadAddress = grpcAddress
			node.isAlive = true
			dataNodeLookupTable[id] = node
			fmt.Printf("Data Node %d rejoined\n", id)
			return &pb.SuccessResponse{Success: true}, nil
		} else if dataNodeId == id && node.isAlive {
			return &pb.SuccessResponse{Success: false}, nil
		} else if node.address == address || node.downloadAddress == grpcAddress {
			return &pb.SuccessResponse{Success: false}, nil
		}
	}
	dataNodeData := dataNode{address: address, downloadAddress: grpcAddress, isAlive: true}
	dataNodeLookupTable[id] = dataNodeData
	dataNodesHeartbeats[id] = 0
	fmt.Printf("Data Node %d joined\n", id)
	return &pb.SuccessResponse{Success: true}, nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	masterPort := os.Getenv("MASTER_PORT")

	lis, err := net.Listen("tcp", masterPort)
	if err != nil {
		fmt.Println("failed to listen:", err)
		return
	}

	s := grpc.NewServer()
	pb.RegisterMasterTrackerServiceServer(s, &masterServer{})
	fmt.Println("Server started. Listening on port", masterPort)
	
	go checkAliveDataNodes()
	go Replication()

	if err := s.Serve(lis); err != nil {
		fmt.Println("Failed to serve:", err)
	}
}
