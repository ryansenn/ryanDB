package core

import (
	"log"

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

	Logger  *Logger
	Storage *storage.Engine
}

func NewNode(id string, port string, peers map[string]string) *Node {
	return &Node{Id: id, Port: port, Peers: peers, State: Follower, Logger: newLogger(id), Storage: storage.NewEngine()}
}

func (n *Node) Get(key string) string {
	return ""
}

func (n *Node) Put(key string, value string) {
	command := newCommand("put", key, value)
	n.Logger.append(command)
	log.Printf(n.Id + " added new log " + command.Op + " " + command.Key + " " + command.Value)
}
