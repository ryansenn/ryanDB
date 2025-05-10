package test

import (
	"testing"
	"time"
)

func CountLeader(t *testing.T, nodes []*Node) (*Node, int) {
	leaderCount := 0
	var leader *Node

	for _, node := range nodes {
		status := node.Status(t)
		if status.State == 2 {
			leaderCount += 1
			leader = node
		}
	}

	return leader, leaderCount
}

func TestElection(t *testing.T) {
	nodes := NewNodes(5)
	defer StopNodes(nodes)
	StartNodes(t, nodes)
	time.Sleep(5 * time.Second)

	leader, leaderCount := CountLeader(t, nodes)
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader, got %d", leaderCount)
	}

	leader.StopNode()
	time.Sleep(5 * time.Second)

	_, leaderCount = CountLeader(t, nodes)
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader, got %d", leaderCount)
	}
}
