package core

import (
	"context"
	"encoding/json"
	"log"
	"time"

	pb "github.com/ryansenn/ryanDB/proto/nodepb"
)

// Used by leader to append a command from client
func (n *Node) AppendLog(cmd *Command) int {
	entry := NewLogEntry(n.Term.Load(), cmd)
	n.LogMu.Lock()
	defer n.LogMu.Unlock()
	n.Logger.AppendLog(entry)
	n.Log = append(n.Log, entry)
	log.Printf(n.Id + " has appended 1 new log")
	return len(n.Log) - 1
}

// Used by leader to append a command and block until committed
func (n *Node) Commit(cmd *Command) {
	index := int64(n.AppendLog(cmd))
	n.CommitCond.L.Lock()
	for index > n.CommitIndex.Load() {
		n.CommitCond.Wait()
	}
	n.CommitCond.L.Unlock()
}

func (n *Node) StartReplicationWorkers() {
	for key, _ := range n.MatchIndex {
		n.MatchIndex[key].Store(0)
	}

	for key, _ := range n.NextIndex {
		n.NextIndex[key].Store(int64(n.GetLogSize()))
	}

	for id := range n.Peers {
		if id != n.Id {
			go n.ReplicateToFollower(id)
		}
	}
}

func (n *Node) ReplicateToFollower(id string) {
	for n.State == Leader {
		startIndex := n.NextIndex[id].Load()
		prevIndex := startIndex - 1
		prevTerm := int64(0)
		var snapshot []*LogEntry
		n.LogMu.Lock()
		if startIndex < int64(len(n.Log)) {
			snapshot = append(snapshot, n.Log[startIndex:]...)
		}
		if prevIndex >= 0 && prevIndex < int64(len(n.Log)) {
			prevTerm = int64(n.Log[prevIndex].Term)
		}
		n.LogMu.Unlock()

		var entries []*pb.LogEntry

		for _, entry := range snapshot {
			serializedCommand, err := json.Marshal(entry.Command)
			if err != nil {
				log.Print(err)
			}
			entries = append(entries, &pb.LogEntry{Term: entry.Term, Command: serializedCommand})
		}

		req := pb.AppendRequest{
			Term:         n.Term.Load(),
			LeaderId:     n.Id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: n.CommitIndex.Load(),
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		resp, err := n.Clients[id].AppendEntries(ctx, &req)
		cancel()

		if err == nil && len(entries) > 0 {
			if resp.Success {
				added := int64(len(req.Entries))
				n.NextIndex[id].Add(added)
				n.MatchIndex[id].Store(n.NextIndex[id].Load() - 1)
				n.UpdateCommitIndex()
			} else {
				if n.NextIndex[id].Load() > 0 {
					n.NextIndex[id].Add(-1)
				}
			}
		}

		time.Sleep(20 * time.Millisecond)
	}
}

func (n *Node) UpdateCommitIndex() {
	for i := int64(n.GetLogSize()) - 1; i > n.CommitIndex.Load(); i-- {
		if n.GetLogTerm(int(i)) != n.Term.Load() {
			continue
		}
		count := 1

		for id, val := range n.MatchIndex {
			if id != n.Id && val.Load() >= i {
				count++
			}
		}

		if i > n.CommitIndex.Load() && count > len(n.MatchIndex)/2 {
			n.CommitCond.L.Lock()
			n.CommitIndex.Store(i)
			n.CommitCond.Broadcast()
			n.CommitCond.L.Unlock()
			log.Printf(n.Id+" has updated commit index to %d", i)
			n.ApplyCommitted()
			return
		}
	}
}

func (n *Node) ApplyLogEntry(index int64) {
	n.LogMu.Lock()
	cmd := n.Log[index].Command
	switch cmd.Op {
	case "put":
		n.Storage.Put(cmd.Key, cmd.Value)
	}
	n.LogMu.Unlock()
}
