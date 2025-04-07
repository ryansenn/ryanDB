package core

import (
	"context"
	"encoding/json"
	"log"
	"net"

	pb "github.com/ryansenn/ryanDB/proto/nodepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type server struct {
	pb.UnimplementedNodeServer
	node *Node
}

func (n *Node) StartServer() {
	lis, err := net.Listen("tcp", n.Peers[n.Id])

	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterNodeServer(grpcServer, &server{})
	go grpcServer.Serve(lis)
	log.Printf(n.Id + " has started gRPC server")
}

func (n *Node) StartClients() {
	n.Clients = map[string]pb.NodeClient{}

	for key, addr := range n.Peers {
		var conn *grpc.ClientConn
		var err error
		for {
			conn, err = grpc.NewClient(
				addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)

			if err == nil {
				break
			}
		}

		client := pb.NewNodeClient(conn)
		n.Clients[key] = client
	}

	log.Printf("%s successfully connected to %d peers", n.Id, len(n.Peers))
}

func (s *server) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	node := s.node
	node.ReceiveHeartbeat()

	return &pb.AppendResponse{Term: node.Term, Success: true}, nil
}

func (s *server) RequestVote(context.Context, *pb.VoteRequest) (*pb.VoteResponse, error) {
	return nil, nil
}

func (s *server) ForwardToLeader(ctx context.Context, command *pb.Command) (*pb.CommandResponse, error) {
	var cmd Command
	var res pb.CommandResponse
	res.Success = true

	err := json.Unmarshal(command.Command, &cmd)

	if err != nil {
		res.Success = false
		return &res, err
	}

	if cmd.Op == "get" {
		res.Result = []byte(s.node.Get(cmd.Key))
	}

	if cmd.Op == "put" {
		s.node.Put(cmd.Key, cmd.Value)
	}

	return &res, nil
}
