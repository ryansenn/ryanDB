package test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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

type Status struct {
	id       string `json:"id"`
	leaderId string `json:"leaderId"`
	state    int    `json:"state"`
	term     int    `json:"term"`
}

func (n *Node) status(t *testing.T) *Status {
	url := fmt.Sprintf("http://localhost:%s/status", n.port)
	resp, err := http.Get(url)

	if err != nil {
		t.Fatalf("HTTP request failed for %s: %v", n.id, err)
	}

	defer resp.Body.Close()

	var status Status
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("invalid JSON from %s: %v", n.id, err)
	}

	return &status
}

func (n *Node) get(t *testing.T, key string) string {
	baseURL := fmt.Sprintf("http://localhost:%s/get", n.port)
	params := url.Values{}
	params.Add("key", key)

	fullURL := baseURL + "?" + params.Encode()

	resp, err := http.Get(fullURL)

	if err != nil {
		t.Fatalf("HTTP request failed for %s: %v", n.id, err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("invalid string from %s: %v", n.id, err)
	}

	return string(body)
}

func (n *Node) put(t *testing.T, key string, value string) string {
	baseURL := fmt.Sprintf("http://localhost:%s/put", n.port)
	params := url.Values{}
	params.Add("key", key)
	params.Add("value", value)

	fullURL := baseURL + "?" + params.Encode()

	resp, err := http.Get(fullURL)

	if err != nil {
		t.Fatalf("HTTP request failed for %s: %v", n.id, err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("invalid string from %s: %v", n.id, err)
	}

	return string(body)
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
