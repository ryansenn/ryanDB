package core

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/ryansenn/ryanDB/proto/nodepb"
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

	Term        atomic.Int64
	VoteFor     string
	CommitIndex atomic.Int64
	LastApplied atomic.Int64
	ApplyMu     sync.Mutex
	NextIndex   map[string]*atomic.Int64
	MatchIndex  map[string]*atomic.Int64
	Log         []*LogEntry
	LogMu       sync.Mutex
	CommitCond  *sync.Cond
	ApplyCond   *sync.Cond

	LeaderId           string
	ResetElectionTimer chan struct{}
	Logger             *Logger
	Storage            *Engine
}

func NewNode(id, port string, peers map[string]string) *Node {
	n := &Node{
		Id:                 id,
		Port:               port,
		Peers:              peers,
		Clients:            make(map[string]pb.NodeClient),
		State:              Follower,
		VoteFor:            "",
		NextIndex:          make(map[string]*atomic.Int64),
		MatchIndex:         make(map[string]*atomic.Int64),
		Log:                make([]*LogEntry, 0),
		CommitCond:         sync.NewCond(&sync.Mutex{}),
		ApplyCond:          sync.NewCond(&sync.Mutex{}),
		LeaderId:           "",
		ResetElectionTimer: make(chan struct{}, 1),
		Logger:             newLogger(id),
		Storage:            NewEngine(),
	}
	n.CommitIndex.Store(-1)
	n.LastApplied.Store(-1)

	for key, _ := range n.Peers {
		n.NextIndex[key] = &atomic.Int64{}
		n.MatchIndex[key] = &atomic.Int64{}
	}

	return n
}

func (n *Node) Init() {
	log.Printf(n.Id + " has been initialized.")
	n.StartServer()
	n.StartClients()
	n.StartElectionTimer()
}

func (n *Node) HandleCommand(cmd *Command) string {
	if n.State == Follower {
		return n.ForwardToLeader(cmd)
	}

	switch cmd.Op {
	case "get":
		return n.Get(cmd.Key)
	case "put":
		n.Commit(cmd)
		return "success"
	}

	return "unknown command"
}

func (n *Node) AppendLog(cmd *Command) int {
	entry := NewLogEntry(n.Term.Load(), cmd)
	n.LogMu.Lock()
	defer n.LogMu.Unlock()
	n.Logger.AppendLog(entry)
	n.Log = append(n.Log, entry)
	log.Printf(n.Id + " has appended 1 new log")
	return len(n.Log) - 1
}

func (n *Node) AppendLogs(PrevLogIndex int64, entries []*LogEntry) {
	n.LogMu.Lock()
	// in memory
	n.Log = n.Log[:PrevLogIndex+1]
	n.Log = append(n.Log, entries...)

	//persistent
	n.Logger.AppendLogs(entries, PrevLogIndex+1)
	n.LogMu.Unlock()
	log.Printf(n.Id+" has appended %d new log", len(entries))
}

func (n *Node) Commit(cmd *Command) {
	index := int64(n.AppendLog(cmd))
	n.CommitCond.L.Lock()
	for index > n.CommitIndex.Load() {
		n.CommitCond.Wait()
	}
	n.CommitCond.L.Unlock()
}

// Provide linearizable reads
func (n *Node) Get(key string) string {
	readIndex := n.CommitIndex.Load()

	n.ApplyCond.L.Lock()
	for n.LastApplied.Load() < readIndex {
		n.ApplyCond.Wait()
	}
	n.ApplyCond.L.Unlock()
	return n.Storage.Get(key)
}

func (n *Node) Execute(cmd *Command) {

}

func (n *Node) GetLogSize() int {
	n.LogMu.Lock()
	defer n.LogMu.Unlock()
	return len(n.Log)
}

func (n *Node) GetLogTerm(index int) int64 {
	n.LogMu.Lock()
	defer n.LogMu.Unlock()
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

func (n *Node) StartElectionTimer() {
	randTimeout := func() time.Duration {
		return time.Duration(rand.Intn(151)+150) * time.Millisecond
	}
	timer := time.NewTimer(randTimeout())

	for n.State == Follower {
		select {
		case <-timer.C:
			n.StartElection()
			timer.Reset(randTimeout())

		case <-n.ResetElectionTimer:
			if !timer.Stop() {
				<-timer.C //drain
			}
			timer.Reset(randTimeout())
		}
	}
}

func (n *Node) StartElection() {
	n.Term.Add(1)
	n.VoteFor = ""
	n.State = Candidate
	yesVote := 1

	log.Printf("%s started election for term %d", n.Id, n.Term.Load())

	for id, client := range n.Clients {
		if id != n.Id {
			LogSize := int64(n.GetLogSize())
			prevIndex := int64(LogSize - 1)
			prevTerm := int64(0)

			if prevIndex >= 0 && prevIndex < LogSize {
				prevTerm = n.GetLogTerm(int(prevIndex))
			}

			voteReq := pb.VoteRequest{
				Term:         n.Term.Load(),
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
		go n.StartReplicationWorkers()
		log.Printf("%s becomes Leader for term %d", n.Id, n.Term.Load())
	} else {
		n.State = Follower
		log.Printf("%s becomes Follower for term %d", n.Id, n.Term.Load())
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
