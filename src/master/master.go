package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	pb "src/grpc/master" // Import the generated package

	cl "src/grpc/filetransfer"

	dk "src/grpc/datakeeper"

	"google.golang.org/grpc"
)

const numDataNodes = 3
var clientPort string

type masterServer struct {
	pb.UnimplementedMasterTrackerServiceServer
}

type dataNode struct {
	dataNodeId int32
	address    string
	downloadAddress string
	isAlive    bool
}
type FileMetadata struct {
	FileName   string // File name
	DataNodeId int32  // Data Keeper node where the file is stored
	FilePath   string // File path on the Data Keeper node
}

var dataNodesHeartbeats = make([]int, numDataNodes)
var fileLookupTable = make([]FileMetadata, 0)
var dataNodeLookupTable = make([]dataNode, numDataNodes)

func (s *masterServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	id := req.GetDataNodeId()
	dataNodesHeartbeats[id] += 1
	return &pb.HeartbeatResponse{}, nil
}

func (s *masterServer) UploadFile(ctx context.Context, req *pb.UploadFileRequest) (*pb.UploadFileResponse, error) {
	dataNodeId := rand.Intn(numDataNodes)
	for !dataNodeLookupTable[dataNodeId].isAlive {
		dataNodeId = rand.Intn(numDataNodes)
	}
	nodeAddr := dataNodeLookupTable[dataNodeId].address
	grpcAddr := dataNodeLookupTable[dataNodeId].downloadAddress
	clientPort = req.GetClientPort()
	return &pb.UploadFileResponse{UploadAddress: nodeAddr , GrpcAddress: grpcAddr }, nil
}

func notifyClient(fileName string) {
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

func chooseTwoRandomNodes(dataNodeId int32) []int32 {
	// Pick the first Id
	nodeIds := make([]int32, 2)
	nodeId := rand.Intn(numDataNodes)
	for nodeId == int(dataNodeId) || !dataNodeLookupTable[nodeId].isAlive {
		nodeId = rand.Intn(numDataNodes)
	}
	nodeIds[0] = int32(nodeId)

	// Pick the second Id
	nodeId = rand.Intn(numDataNodes)
	for nodeId == int(dataNodeId) || nodeId == int(nodeIds[0]) || !dataNodeLookupTable[nodeId].isAlive {
		nodeId = rand.Intn(numDataNodes)
	}
	nodeIds[1] = int32(nodeId)

	return nodeIds
}

func chooseNodesToReplicate(fileName string, dataNodeId int32) {
	nodeIds := chooseTwoRandomNodes(dataNodeId)
	fmt.Printf("Sending the 2 nodes to Data node %d\n", dataNodeId)
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
	fileMetadata := FileMetadata{FileName: fileName, DataNodeId: dataNodeId, FilePath: filePath}
	fileLookupTable = append(fileLookupTable, fileMetadata)

	if !replicate {
		notifyClient(fileName)
		chooseNodesToReplicate(fileName, dataNodeId)
	}
	return &pb.RegisterFileResponse{}, nil
}

func (s *masterServer) DownloadFile(ctx context.Context, req *pb.DownloadFileRequest) (*pb.DownloadFileResponse, error) {
	fileName := req.GetFileName()
	addresses := make([]string, 0)
	for _, file := range fileLookupTable {
		if file.FileName == fileName && dataNodeLookupTable[file.DataNodeId].isAlive {
			addresses = append(addresses, dataNodeLookupTable[file.DataNodeId].downloadAddress)
		}
	}
	return &pb.DownloadFileResponse{Addresses: addresses}, nil
}

func checkAliveDataNodes() {
	for {
		time.Sleep(5 * time.Second)
		for i := 0; i < numDataNodes; i++ {
			if dataNodesHeartbeats[i] == 0 {
				fmt.Printf("Data Keeper node %d is dead\n", i)
				dataNodeLookupTable[i].isAlive = false
			} else {
				fmt.Printf("Data Keeper node %d is alive\n", i)
				dataNodeLookupTable[i].isAlive = true
			}
			dataNodesHeartbeats[i] = 0
		}
	}
}

func Replication() {
	for {
		time.Sleep(10 * time.Second)
		// Each file should exist on at least 3 alive data nodes. If not, replicate it to one of the alive nodes.
		// create a map with key filename and value will be an array of ids of the data nodes where the file is stored
		fileMap := make(map[string][]int32)
		for _, file := range fileLookupTable {
			if dataNodeLookupTable[file.DataNodeId].isAlive {
				fileMap[file.FileName] = append(fileMap[file.FileName], file.DataNodeId)
			}
		}

		for _, file := range fileLookupTable {
			if len(fileMap[file.FileName]) < 3 {
				// choose new data node ids for the fileName until the count is restored to 3, then notify the source and destination nodes to start copying
				if len(fileMap[file.FileName]) == 2 {
					destinationId := rand.Intn(numDataNodes)
					for int32(destinationId) == fileMap[file.FileName][0] || int32(destinationId) == fileMap[file.FileName][1] || !dataNodeLookupTable[destinationId].isAlive {
						destinationId = rand.Intn(numDataNodes)
					}
					// conn, err := grpc.Dial(dataNodeLookupTable[fileMap[file.FileName][0]].address, grpc.WithInsecure())
					// if err != nil {
					// 	fmt.Println("Did not connect:", err)
					// 	return
					// }
					// defer conn.Close()
					// c := pb.NewDataNodeServiceKeeper(conn)
					// address := dataNodeLookupTable[destinationId].address
					// resp, err := c.ReplicateFile(context.Background(), &pb.ReplicationRequest{FileName: file.FileName, Address: address})
					// if err != nil {
					// 	fmt.Println("Error notifying the client:", err)
					// 	return
					// }
					// fmt.Println("Replication Status:", resp.GetReplicationStatus())
				} else if len(fileMap[file.FileName]) == 1 {
					chooseNodesToReplicate(file.FileName, fileMap[file.FileName][0])
				}
			}
		}
	}
}

func populateDataKeepers() {
	dataNodeData := dataNode{dataNodeId: 0, address: "localhost:50051", downloadAddress: "localhost:50050", isAlive: true}
	dataNodeLookupTable[0] = dataNodeData

	dataNodeData = dataNode{dataNodeId: 1, address: "localhost:49152", downloadAddress: "localhost:49153", isAlive: true}
	dataNodeLookupTable[1] = dataNodeData

	dataNodeData = dataNode{dataNodeId: 2, address: "localhost:49154", downloadAddress: "localhost:49155", isAlive: true}
	dataNodeLookupTable[2] = dataNodeData
}

func main() {
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("failed to listen:", err)
		return
	}

	populateDataKeepers()

	s := grpc.NewServer()
	pb.RegisterMasterTrackerServiceServer(s, &masterServer{})
	fmt.Println("Server started. Listening on port 8080...")
	
	go checkAliveDataNodes()
	// go Replication()

	if err := s.Serve(lis); err != nil {
		fmt.Println("Failed to serve:", err)
	}
}
