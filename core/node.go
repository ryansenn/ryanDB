package core

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	pb "github.com/ryansenn/ryanDB/proto/nodepb"
	"github.com/ryansenn/ryanDB/storage"
)

type NodeState int

const (
	Follower  NodeState = 0
	Candidate NodeState = 1
	Leader    NodeState = 2
)

type Node struct {
	Id      string
	Port    string
	Peers   map[string]string
	Clients map[string]pb.NodeClient

	State              NodeState
	Term               int64
	LogIndex           int64
	LastLogTerm        int64
	ResetElectionTimer chan struct{}
	VoteFor            string
	LeaderId           string

	Logger  *Logger
	Storage *storage.Engine
}

func NewNode(id, port string, peers map[string]string) *Node {
	return &Node{
		Id:          id,
		Port:        port,
		Peers:       peers,
		Clients:     make(map[string]pb.NodeClient),
		State:       Follower,
		Term:        0,
		LogIndex:    0,
		LastLogTerm: 0,
		VoteFor:     "",
		Logger:      newLogger(id),
		Storage:     storage.NewEngine(),
	}
}

func (n *Node) Get(key string) string {
	return ""
}

func (n *Node) Put(key string, value string) {
	command := NewCommand("put", key, value)
	serializedCommand, err := json.Marshal(command)

	if err != nil {
		log.Fatal(err)
	}

	entry := pb.LogEntry{Term: n.Term, Command: serializedCommand}
	n.Logger.append(&entry)
	n.LogIndex += 1
	log.Printf(n.Id + " added new log " + command.Op + " " + command.Key + " " + command.Value)
}

func (n *Node) ForwardToLeader(command *Command) string {
	serializedCommand, err := json.Marshal(*command)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(n.Id + " has forwarded command to leader")
	response, err := n.Clients[n.LeaderId].ForwardToLeader(context.Background(), &pb.Command{Command: serializedCommand})

	if err != nil {
		log.Fatal(err)
	}

	return string(response.Result)
}

func (n *Node) StartElection() {
	n.Term += 1
	n.VoteFor = ""
	n.State = Candidate
	yesVote := 1

	log.Printf("Node %s started election for term %d", n.Id, n.Term)

	for id, client := range n.Clients {
		if id != n.Id {

			voteReq := pb.VoteRequest{
				Term:         n.Term,
				CandidateId:  n.Id,
				LastLogIndex: n.LogIndex,
				LastLogTerm:  n.LastLogTerm,
			}

			voteResp, err := client.RequestVote(context.Background(), &voteReq)

			if err != nil {
				log.Fatal(err)
			}

			if voteResp.VoteGranted {
				yesVote += 1
			}
		}
	}

	if yesVote > len(n.Peers)/2 {
		n.State = Leader
		log.Printf("Node %s becomes Leader for term %d", n.Id, n.Term)
	} else {
		n.State = Follower
		log.Printf("Node %s becomes Follower for term %d", n.Id, n.Term)
	}
}

func (n *Node) StartElectionTimer() {
	for {
		timeout := rand.Intn(151) + 150

		select {
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			n.StartElection()
			return

		case <-n.ResetElectionTimer:

		}
	}
}

func (n *Node) ReceiveHeartbeat() {
	select {
	case n.ResetElectionTimer <- struct{}{}:
		// sent successfully
	default:
		// channel full, skip
	}
}
