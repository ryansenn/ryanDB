package test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var N = 5

func InitNodes(t *testing.T) []*Node {
	KillPorts(N)
	nodes := NewNodes(N)
	StartNodes(t, nodes, "true")
	time.Sleep(1 * time.Second)

	return nodes
}

func TestElection(t *testing.T) {
	nodes := InitNodes(t)
	defer StopNodes(nodes)

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

func TestLogReplication(t *testing.T) {
	nodes := InitNodes(t)
	defer StopNodes(nodes)

	nodes[1].Put(t, "key1", "value1")

	for _, node := range nodes {
		val := node.Get(t, "key1")
		//t.Logf("%s returned raw: [%s]\n", node.id, val)
		if val != "value1" {
			t.Fatalf("%s has wrong value: %s", node.id, val)
		}
	}
}

func Test100LogReplication(t *testing.T) {
	nodes := InitNodes(t)
	defer StopNodes(nodes)

	for i := 1; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		nodes[rand.Intn(len(nodes))].Put(t, key, value)
	}

	for _, node := range nodes {
		for i := 1; i < 100; i++ {
			key := fmt.Sprintf("key%d", i)
			expectedValue := fmt.Sprintf("value%d", i)
			value := node.Get(t, key)

			//t.Logf("%s returned raw: [%s]\n", node.id, val)
			if value != expectedValue {
				t.Fatalf("%s has wrong value: %s", node.id, value)
			}
		}

	}
}

func TestLogDiskRecovery(t *testing.T) {
	nodes := InitNodes(t)
	defer StopNodes(nodes)

	for i := 1; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		nodes[rand.Intn(len(nodes))].Put(t, key, value)
	}

	time.Sleep(5 * time.Second)

	for _, node := range nodes {
		node.StopNode()
		time.Sleep(1 * time.Second)
		node.StartNode(t, "false")
		time.Sleep(5 * time.Second)

		for i := 1; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			expectedValue := fmt.Sprintf("value%d", i)
			value := node.Get(t, key)

			if value != expectedValue {
				t.Fatalf("%s has wrong value: %s", node.id, value)
			}
		}
	}
}

// test missed logs
