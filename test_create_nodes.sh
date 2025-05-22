#!/bin/bash

RESET_FLAG=$1

PEERS="node1=127.0.0.1:9001,node2=127.0.0.1:9002,node3=127.0.0.1:9003"

go run main.go --id=node1 --port=8001 --peers=$PEERS --reset=$RESET_FLAG &
PID1=$!

go run main.go --id=node2 --port=8002 --peers=$PEERS --reset=$RESET_FLAG &
PID2=$!

go run main.go --id=node3 --port=8003 --peers=$PEERS --reset=$RESET_FLAG &
PID3=$!

wait $PID1 $PID2 $PID3