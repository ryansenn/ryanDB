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
	State   NodeState

	Term        int64
	VoteFor     string
	CommitIndex int64
	LastApplied int64
	NextIndex   map[string]int64
	MatchIndex  map[string]int64
	Log         []*pb.LogEntry

	LeaderId           string
	ResetElectionTimer chan struct{}
	Logger             *Logger
	Storage            *storage.Engine
}

func NewNode(id, port string, peers map[string]string) *Node {
	return &Node{
		Id:                 id,
		Port:               port,
		Peers:              peers,
		Clients:            make(map[string]pb.NodeClient),
		State:              Follower,
		Term:               0,
		VoteFor:            "",
		CommitIndex:        0,
		LastApplied:        0,
		NextIndex:          make(map[string]int64),
		MatchIndex:         make(map[string]int64),
		Log:                make([]*pb.LogEntry, 0),
		LeaderId:           "",
		ResetElectionTimer: make(chan struct{}, 1),
		Logger:             newLogger(id),
		Storage:            storage.NewEngine(),
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
	n.Log = append(n.Log, &entry)
}

func (n *Node) Init() {
	n.StartServer()
	n.StartClients()
	log.Printf(n.Id + " is now running.")
	n.StartElectionTimer()
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

	log.Printf("%s started election for term %d", n.Id, n.Term)

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
		n.StartHeartbeat()
		log.Printf("%s becomes Leader for term %d", n.Id, n.Term)
	} else {
		n.State = Follower
		log.Printf("%s becomes Follower for term %d", n.Id, n.Term)
	}
}

func (n *Node) StartHeartbeat() {

	for n.State == Leader {

		for _, client := range n.Clients {
			emptyEntries := pb.AppendRequest{
				Term: n.Term, LeaderId: n.Id,
				PrevLogIndex: n.LogIndex,
				PrevLogTerm:  n.LastLogTerm,
			}

			client.AppendEntries(context.Background(), &emptyEntries)
		}

		time.Sleep(50 * time.Microsecond)
	}
}

func (n *Node) StartElectionTimer() {
	for {
		timeout := rand.Intn(151) + 150

		select {
		case <-time.After(time.Duration(timeout) * time.Millisecond):
			n.StartElection()

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
