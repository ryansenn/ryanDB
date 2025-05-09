package test

import (
	"io"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

type Node struct {
	id    string
	port  string
	peers string
	cmd   *exec.Cmd
}

func NewNodes(n int) []*Node {
	var nodes []*Node
	peers := ""
	for i := 0; i < n; i++ {
		id := "node" + strconv.Itoa(i+1)
		addr := "localhost:" + strconv.Itoa(9001+i)
		peers += id + "=" + addr
		if i != n-1 {
			peers += ","
		}
	}

	for i := 0; i < n; i++ {
		node := &Node{
			id:    "node" + strconv.Itoa(i+1),
			port:  strconv.Itoa(8001 + i),
			peers: peers,
		}
		nodes = append(nodes, node)
	}

	return nodes
}

func (n *Node) StartNode(t *testing.T) {
	cmd := exec.Command("go", "run", "../main.go", "--id="+n.id, "--port="+n.port, "--peers="+n.peers)
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start node %s: %v", n.id, err)
	}
	go io.Copy(io.Discard, stdout)
	go io.Copy(io.Discard, stderr)
	time.Sleep(2 * time.Second)
	n.cmd = cmd
}

func (n *Node) StopNode() {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
		_ = n.cmd.Wait()
	}
}

func StartNodes(t *testing.T, nodes []*Node) {
	for _, node := range nodes {
		node.StartNode(t)
	}
}

func StopNodes(nodes []*Node) {
	for _, node := range nodes {
		node.StopNode()
	}
}
