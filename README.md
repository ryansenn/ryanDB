## ryanDB

Distributed key-value store implementing the [Raft consensus algorithm](https://raft.github.io/raft.pdf) built from scratch

### Supports:
- Linearizable reads and writes (handled by the leader; followers forward client requests)
- Leader election and heartbeats
- Log replication with commit tracking
- Recovery from crashes with persistent logs and metadata
- HTTP API for external clients, gRPC for internal cluster communication

### Running a node:

    ./ryanDB --id=<node_id> --port=<port> --peers=<id=addr,...> [--reset=true|false]

### HTTP API:
- GET /get?key=<key>: Fetch value for key
- GET /put?key=<key>&value=<value>: Store key-value pair
- GET /status: Get node status (id, term, state, leader ID)

### Cluster communication:
gRPC services: RequestVote (for elections), AppendEntries (for log replication), ForwardToLeader (for forwarding client commands)

### Future goals:
- Log compaction, stress test, performance benchmark
