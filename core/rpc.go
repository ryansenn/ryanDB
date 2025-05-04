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
	s.node.ReceiveHeartbeat()
	s.node.LeaderId = req.LeaderId
	resp := pb.AppendResponse{Term: s.node.Term, Success: false}

	if s.node.Term > req.Term {
		return &resp, nil
	}

	if req.Term > s.node.Term {
		s.node.Term = req.Term
		s.node.State = Follower
	}

	if len(s.node.Log)-1 < int(req.PrevLogIndex) {
		return &resp, nil
	}

	if s.node.GetLogTerm(int(req.PrevLogIndex)) != req.PrevLogTerm {
		return &resp, nil
	}

	var entries []*LogEntry

	for _, entry := range req.Entries {
		var cmd Command
		json.Unmarshal(entry.Command, cmd)
		entries = append(entries, NewLogEntry(entry.Term, &cmd))
	}

	s.node.AppendLogs(req.PrevLogIndex, entries)

	resp.Success = true
	resp.Term = s.node.Term
	return &resp, nil
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

	if s.node.GetLogTerm(-1) > req.LastLogTerm || (s.node.GetLogTerm(-1) == req.LastLogTerm && int64(len(s.node.Log))-1 > req.LastLogIndex) {
		return &resp, nil
	}

	resp.VoteGranted = true
	s.node.VoteFor = req.CandidateId
	log.Printf("%s has granted vote to %s in term %d", s.node.Id, req.CandidateId, s.node.Term)
	return &resp, nil
}

func (s *server) ForwardToLeader(ctx context.Context, command *pb.Command) (*pb.CommandResponse, error) {
	if s.node.State != Leader {
		return s.node.Clients[s.node.LeaderId].ForwardToLeader(context.Background(), command)
	}

	var cmd Command
	var res pb.CommandResponse
	res.Success = true

	err := json.Unmarshal(command.Command, &cmd)

	if err != nil {
		res.Success = false
		return &res, err
	}

	s.node.AppendLog(NewLogEntry(s.node.Term, NewCommand(cmd.Op, cmd.Key, cmd.Value)))

	return &res, nil
}
