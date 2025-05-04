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

func (n *Node) Put(key string, value string) string {
	command := NewCommand("put", key, value)
	serializedCommand, err := json.Marshal(command)

	if err != nil {
		log.Fatal(err)
		return "false"
	}

	entry := pb.LogEntry{Term: n.Term, Command: serializedCommand}
	n.Logger.append(&entry)
	n.Log = append(n.Log, &entry)

	return "true"
}

func (n *Node) Init() {
	log.Printf(n.Id + " has been initialized.")
	n.StartServer()
	n.StartClients()
	n.StartElectionTimer()
}

func (n *Node) GetLogTerm(index int) int64 {
	if index == -1 {
		if len(n.Log) > 0 {
			return n.Log[len(n.Log)-1].Term
		}
		return 0
	}

	return n.Log[index].Term
}

func (n *Node) ForwardToLeader(command *Command) string {
	serializedCommand, err := json.Marshal(*command)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(n.Id + " has forwarded command to leader " + n.LeaderId)
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
			prevIndex := int64(len(n.Log) - 1)
			prevTerm := int64(0)

			if prevIndex >= 0 && prevIndex < int64(len(n.Log)) {
				prevTerm = n.GetLogTerm(int(prevIndex))
			}

			voteReq := pb.VoteRequest{
				Term:         n.Term,
				CandidateId:  n.Id,
				LastLogIndex: prevIndex,
				LastLogTerm:  prevTerm,
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
		go n.StartHeartbeat()
		go n.StartReplicationWorkers()
		log.Printf("%s becomes Leader for term %d", n.Id, n.Term)
	} else {
		n.State = Follower
		log.Printf("%s becomes Follower for term %d", n.Id, n.Term)
	}
}

func (n *Node) ReplicateToFollower(id string) {
	for n.State == Leader {
		startIndex := n.NextIndex[id]

		if startIndex < int64(len(n.Log)) {
			prevIndex := int64(startIndex - 1)
			prevTerm := int64(0)

			if prevIndex >= 0 && prevIndex < int64(len(n.Log)) {
				prevTerm = n.GetLogTerm(int(prevIndex))
			}

			req := pb.AppendRequest{
				Term:         n.Term,
				LeaderId:     n.Id,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      n.Log[startIndex:],
			}

			resp, _ := n.Clients[id].AppendEntries(context.Background(), &req)

			if resp.Success {
				added := int64(len(req.Entries))
				n.NextIndex[id] += added
				n.MatchIndex[id] = n.NextIndex[id] - 1
			} else {
				if n.NextIndex[id] > 0 {
					n.NextIndex[id]--
				}
			}
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (n *Node) StartReplicationWorkers() {
	for key, _ := range n.MatchIndex {
		n.MatchIndex[key] = 0
	}

	for key, _ := range n.NextIndex {
		n.NextIndex[key] = int64(len(n.Log))
	}

	for id := range n.Peers {
		go n.ReplicateToFollower(id)
	}
}

func (n *Node) StartHeartbeat() {

	for n.State == Leader {

		for _, client := range n.Clients {
			emptyEntries := pb.AppendRequest{
				Term:         n.Term,
				LeaderId:     n.Id,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
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
