package test

import (
	"testing"
	"time"
)

func TestElection(t *testing.T) {
	nodes := NewNodes(5)
	defer StopNodes(nodes)
	StartNodes(t, nodes)
	time.Sleep(1 * time.Second)

	leader, leaderCount := CountLeader(t, nodes)
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader, got %d", leaderCount)
	}

	t.Logf("%s has been killed", leader.id)
	leader.StopNode()
	time.Sleep(1 * time.Second)

	_, leaderCount = CountLeader(t, nodes)
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader, got %d", leaderCount)
	}
}
