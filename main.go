package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/ryansenn/ryanDB/core"
)

var node *core.Node

func get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	key := r.URL.Query().Get("key")
	var value string
	if node.State == core.Follower {
		value = node.ForwardToLeader(core.NewCommand("get", key, ""))
		w.Write([]byte(value))
		return
	}

	w.Write([]byte(value))
}

func put(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	res := "true"
	if node.State == core.Follower {
		res = node.ForwardToLeader(core.NewCommand("put", key, value))
	} else {
		node.AppendLogWait(core.NewCommand("put", key, value))
	}

	w.Write([]byte(res + "\n"))
}

func parsePeers(peersStr string) map[string]string {
	res := map[string]string{}

	for _, pair := range strings.Split(peersStr, ",") {
		kv := strings.Split(pair, "=")
		res[kv[0]] = kv[1]
	}

	return res
}

func main() {
	id := flag.String("id", "", "Unique node ID")
	port := flag.String("port", "8000", "Port to listen on")
	peersStr := flag.String("peers", "", "Comma-separated list of id=addr pairs (e.g., node1=localhost:8001,node2=localhost:8002,node3=localhost:8003)")

	flag.Parse()

	if *id == "" || *peersStr == "" {
		fmt.Println("Usage: go run main.go --id=node1 --port=8001 --peers=node1=localhost:8001,node2=localhost:8002,node3=localhost:8003")
		return
	}

	node = core.NewNode(*id, *port, parsePeers(*peersStr))
	go node.Init()

	http.HandleFunc("/get", get)
	http.HandleFunc("/put", put)

	//log.Printf("Server ID: %s | Listening on: %s | Peers: %s", *id, *port, *peersStr)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
