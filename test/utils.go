package test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func KillPorts(n int) {
	for i := 0; i < n; i++ {
		for _, port := range []int{8001 + i, 9001 + i} {
			out, err := exec.Command("lsof", "-ti", fmt.Sprintf(":%d", port)).Output()
			if err != nil {
				continue
			}
			pids := strings.Fields(string(out))
			for _, pid := range pids {
				_ = exec.Command("kill", "-9", pid).Run()
			}
		}
	}
}

type Node struct {
	id      string
	port    string
	peers   string
	cmd     *exec.Cmd
	running bool
}

type Status struct {
	Id       string `json:"id"`
	LeaderId string `json:"leaderId"`
	State    int    `json:"state"`
	Term     int    `json:"term"`
}

func (n *Node) Status(t *testing.T) *Status {
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

func (n *Node) Get(t *testing.T, key string) string {
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

func (n *Node) Put(t *testing.T, key string, value string) string {
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
			id:      "node" + strconv.Itoa(i+1),
			port:    strconv.Itoa(8001 + i),
			peers:   peers,
			running: false,
		}
		nodes = append(nodes, node)
	}

	return nodes
}

func (n *Node) StartNode(t *testing.T) {
	cmd := exec.Command("../raftnode", "--id="+n.id, "--port="+n.port, "--peers="+n.peers)
	//stdout, _ := cmd.StdoutPipe()
	//stderr, _ := cmd.StderrPipe()
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start node %s: %v", n.id, err)
	}
	n.running = true

	//go io.Copy(io.Discard, stdout)
	//go io.Copy(io.Discard, stderr)
	n.cmd = cmd
}

func (n *Node) StopNode() {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Kill()
		n.running = false
		_ = n.cmd.Wait()
	}
}

func StartNodes(t *testing.T, nodes []*Node) {
	for _, node := range nodes {
		node.StartNode(t)
	}
	time.Sleep(10 * time.Second)
}

func StopNodes(nodes []*Node) {
	for _, node := range nodes {
		node.StopNode()
	}
}

func CountLeader(t *testing.T, nodes []*Node) (*Node, int) {
	leaderCount := 0
	var leader *Node

	for _, node := range nodes {
		if node.running {
			status := node.Status(t)
			if status.State == 2 {
				leaderCount += 1
				leader = node
			}
		}
	}

	return leader, leaderCount
}
