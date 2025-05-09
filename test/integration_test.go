package test

import (
	"io"
	"os/exec"
	"testing"
	"time"
)

func StartNode(t *testing.T, id string, port string, peers string) *exec.Cmd {
	cmd := exec.Command("go", "run", "../main.go", "--id="+id, "--port="+port, "--peers="+peers)
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()
	if err := cmd.Start(); err != nil {
		t.Fatalf("failed to start node %s: %v", id, err)
	}
	go io.Copy(io.Discard, stdout)
	go io.Copy(io.Discard, stderr)
	time.Sleep(2 * time.Second) // wait for node startup
	return cmd
}
