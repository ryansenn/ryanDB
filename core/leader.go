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

		if startIndex < int64(len(n.Log)) {
			prevIndex := int64(startIndex - 1)
			prevTerm := int64(0)

			if prevIndex >= 0 && prevIndex < int64(len(n.Log)) {
				prevTerm = n.GetLogTerm(int(prevIndex))
			}

			var entries []*pb.LogEntry
			for _, entry := range n.Log[startIndex:] {
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
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (n *Node) StartReplicationWorkers() {
	for key, _ := range n.MatchIndex {
		n.MatchIndex[key] = 0
	}

	for key, _ := range n.NextIndex {
		n.NextIndex[key] = int64(len(n.Log))
	}

	for id := range n.Peers {
		if id != n.Id {
			go n.ReplicateToFollower(id)
		}
	}
}

func (n *Node) StartHeartbeat() {

	for n.State == Leader {

		for _, client := range n.Clients {
			emptyEntries := pb.AppendRequest{
				Term:         n.Term,
				LeaderId:     n.Id,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
			}

			client.AppendEntries(context.Background(), &emptyEntries)
		}

		time.Sleep(50 * time.Microsecond)
	}
}

func (n *Node) UpdateCommitIndex() {
	for i := int64(len(n.Log)) - 1; i > n.CommitIndex; i-- {
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
			n.CommitIndex = i
			log.Printf(n.Id+" has updated commit index to %d", i)
			return
		}
	}
}
