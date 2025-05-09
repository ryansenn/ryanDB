package test

import "testing"

func TestMain(t *testing.T) {
	nodes := NewNodes(5)
	defer StopNodes(nodes)
	StartNodes(t, nodes)

	for _, node := range nodes {
		t.Logf("%+v", node.Status(t))
	}
}
