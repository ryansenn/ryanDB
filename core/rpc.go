package core

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"time"

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
	pb.RegisterNodeServer(grpcServer, &server{node: n})
	go grpcServer.Serve(lis)
}

func (n *Node) StartClients() {
	n.Clients = map[string]pb.NodeClient{}

	for key, addr := range n.Peers {
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)

		if err != nil {
			log.Fatal(err)
		}

		client := pb.NewNodeClient(conn)
		n.Clients[key] = client

		for {
			dummyReq := pb.VoteRequest{
				Term:         -1,
				CandidateId:  n.Id,
				LastLogIndex: -1,
				LastLogTerm:  -1,
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err = client.RequestVote(ctx, &dummyReq)
			cancel()

			if err == nil {
				break
			}
		}
	}
}

func (s *server) AppendEntries(ctx context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	node := s.node
	node.ReceiveHeartbeat()

	return &pb.AppendResponse{Term: node.Term, Success: true}, nil
}

func (s *server) RequestVote(ctx context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	if s.node.Term < req.Term {
		s.node.State = Follower
		s.node.Term = req.Term
		s.node.VoteFor = ""
	}

	resp := pb.VoteResponse{Term: s.node.Term, VoteGranted: false}

	if s.node.VoteFor != "" && s.node.VoteFor != req.CandidateId {
		return &resp, nil
	}

	if s.node.Term > req.Term {
		return &resp, nil
	}

	if s.node.GetLogTerm(-1) > req.LastLogTerm || (s.node.GetLogTerm(-1) == req.LastLogTerm && int64(len(s.node.Log)) > req.LastLogIndex) {
		return &resp, nil
	}

	resp.VoteGranted = true
	s.node.VoteFor = req.CandidateId
	log.Printf("%s has granted vote to %s in term %d", s.node.Id, req.CandidateId, s.node.Term)
	return &resp, nil
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
