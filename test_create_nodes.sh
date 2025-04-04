#!/bin/bash

PEERS="node1=localhost:8001,node2=localhost:8002,node3=localhost:8003"

go run main.go --id=node1 --port=8001 --peers=$PEERS &
PID1=$!

go run main.go --id=node2 --port=8002 --peers=$PEERS &
PID2=$!

go run main.go --id=node3 --port=8003 --peers=$PEERS &
PID3=$!

wait $PID1 $PID2 $PID3