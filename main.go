package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"strings"

	"github.com/ryansenn/ryanDB/core"
)

var node *core.Node

func get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	key := r.URL.Query().Get("key")
	cmd := core.NewCommand("get", key, "")
	w.Write([]byte(node.HandleCommand(cmd)))
}

func put(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")
	cmd := core.NewCommand("put", key, value)
	w.Write([]byte(node.HandleCommand(cmd)))
}

func status(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"id":       node.Id,
		"state":    node.State, // 0-Follower, 1-Candidate, 2-Leader
		"term":     node.Term.Load(),
		"leaderId": node.LeaderId.Load(),
	})
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic: %v", r)
			debug.PrintStack()
		}
	}()

	id := flag.String("id", "", "Unique node ID")
	port := flag.String("port", "8000", "Port to listen on")
	peersStr := flag.String("peers", "", "Comma-separated list of id=addr pairs (e.g., node1=localhost:8001,node2=localhost:8002,node3=localhost:8003)")
	reset := flag.Bool("reset", false, "Reset logs and metadata")

	flag.Parse()

	if *id == "" || *peersStr == "" {
		fmt.Println("Usage: go run main.go --id=node1 --port=8001 --peers=node1=localhost:8001,node2=localhost:8002,node3=localhost:8003")
		return
	}

	node = core.NewNode(*id, *port, parsePeers(*peersStr))

	if *reset {
		node.Logger.ClearData()
	} else {
		node.RecoverState()
	}

	go node.Init()

	http.HandleFunc("/get", get)
	http.HandleFunc("/put", put)
	http.HandleFunc("/status", status)

	//log.Printf("Server ID: %s | Listening on: %s | Peers: %s", *id, *port, *peersStr)
	log.Fatal(*id+" %s", http.ListenAndServe(":"+*port, nil))
}

func parsePeers(peersStr string) map[string]string {
	res := map[string]string{}

	for _, pair := range strings.Split(peersStr, ",") {
		kv := strings.Split(pair, "=")
		res[kv[0]] = kv[1]
	}

	return res
}
