package core

import (
	"context"
	"encoding/json"
	"log"
	"time"

	pb "github.com/ryansenn/ryanDB/proto/nodepb"
)

func (n *Node) ReplicateToFollower(id string) {
	for n.State == Leader {
		startIndex := n.NextIndex[id]
		prevIndex := startIndex - 1
		prevTerm := int64(0)
		var snapshot []*LogEntry
		n.LogMu.Lock()
		if startIndex < int64(len(n.Log)) {
			snapshot = append(snapshot, n.Log[startIndex:]...)
			if prevIndex >= 0 && prevIndex < int64(len(n.Log)) {
				prevTerm = int64(n.Log[prevIndex].Term)
			}
		}
		n.LogMu.Unlock()

		var entries []*pb.LogEntry

		for _, entry := range snapshot {
			serializedCommand, err := json.Marshal(entry.Command)
			if err != nil {
				log.Fatal(err)
			}
			entries = append(entries, &pb.LogEntry{Term: entry.Term, Command: serializedCommand})
		}

		req := pb.AppendRequest{
			Term:         n.Term,
			LeaderId:     n.Id,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
		}

		resp, _ := n.Clients[id].AppendEntries(context.Background(), &req)

		if resp.Success {
			added := int64(len(req.Entries))
			n.NextIndex[id] += added
			n.MatchIndex[id] = n.NextIndex[id] - 1
			n.UpdateCommitIndex()
		} else {
			if n.NextIndex[id] > 0 {
				n.NextIndex[id]--
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (n *Node) StartReplicationWorkers() {
	for key, _ := range n.MatchIndex {
		n.MatchIndex[key] = 0
	}

	for key, _ := range n.NextIndex {
		n.NextIndex[key] = int64(n.GetLogSize())
	}

	for id := range n.Peers {
		if id != n.Id {
			go n.ReplicateToFollower(id)
		}
	}
}

func (n *Node) UpdateCommitIndex() {
	for i := int64(n.GetLogSize()) - 1; i > n.CommitIndex.Load(); i-- {
		if n.GetLogTerm(int(i)) != n.Term {
			continue
		}
		count := 1

		for id, val := range n.MatchIndex {
			if id != n.Id && val >= i {
				count++
			}
		}

		if count > len(n.MatchIndex)/2 {
			n.CommitCond.L.Lock()
			n.CommitIndex.Store(i)
			n.CommitCond.Broadcast()
			n.CommitCond.L.Unlock()
			log.Printf(n.Id+" has updated commit index to %d", i)
			return
		}
	}
}
