package core

import (
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
	ResetElectionTimer chan struct{}

	Logger  *Logger
	Storage *storage.Engine
}

func NewNode(id, port string, peers map[string]string) *Node {
	return &Node{
		Id:      id,
		Port:    port,
		Peers:   peers,
		Clients: make(map[string]pb.NodeClient),
		State:   Follower,
		Term:    0,
		Logger:  newLogger(id),
		Storage: storage.NewEngine(),
	}
}

func (n *Node) Get(key string) string {
	return ""
}

func (n *Node) Put(key string, value string) {
	command := newCommand("put", key, value, n.Term)
	n.Logger.append(command)
	log.Printf(n.Id + " added new log " + command.Op + " " + command.Key + " " + command.Value)
}

func (n *Node) StartElection() {
	n.Term += 1
	n.State = Candidate
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
