# RAFT: Distributed Key-Value Store using the Raft Consensus Algorithm

A fault-tolerant distributed key-value store built using the **Raft Consensus Algorithm** in Python with **gRPC** for inter-node communication.

This project implements the core components of Raft including leader election, heartbeat-based failure detection, log replication, leader lease optimization, and replicated state machine execution across a cluster of servers.

---

## Features

- Leader Election using randomized election timeouts
- Leader Lease mechanism to prevent stale leaders
- Heartbeat-based cluster management
- Log Replication across followers
- Persistent metadata and log storage
- Fault-tolerant key-value store
- Automatic client redirection to current leader
- gRPC-based RPC communication
- Replicated state machine
- Three-node distributed cluster

---

## Architecture

```
                  Client
                     │
              gRPC Requests
                     │
          ┌──────────┴──────────┐
          │                     │
      Leader Node         Followers
           │              │      │
           │──────────────┼──────│
           │ AppendEntries│
           │ Heartbeats   │
           ▼              ▼
      Replicated Logs
           │
           ▼
     State Machine
           │
     Key-Value Store
```

---

## Tech Stack

- Python 3
- gRPC
- Protocol Buffers
- Multithreading
- Concurrent Futures
- Raft Consensus Algorithm

---

## Repository Structure

```
.
├── client.py
├── main_server.py
├── node_1.py
├── node_2.py
├── node_3.py
├── server.py
├── rpcs.proto
├── logs_*.txt
├── committed_logs_*.txt
├── meta_*.txt
└── dump_*.txt
```

---

## How it Works

### Leader Election

- Every node starts as a **Follower**.
- Followers wait for heartbeats.
- If the election timeout expires:
  - the node becomes a Candidate
  - increments its term
  - requests votes from peers
- After receiving majority votes, it becomes the Leader.

---

### Leader Lease

The implementation extends standard Raft with a **Leader Lease** mechanism.

A leader periodically renews its lease through successful heartbeat acknowledgements.

Candidates delay leadership until any existing leader lease expires, preventing split-brain scenarios.

---

### Log Replication

Client write requests are accepted only by the Leader.

The Leader:

1. Appends the entry locally.
2. Sends AppendEntries RPCs.
3. Waits for majority acknowledgement.
4. Commits the entry.
5. Followers apply committed entries to their state machines.

---

### Read Requests

Read requests are served only by the current Leader.

Followers redirect the client to the latest known leader.

---

## Persistent Storage

Each node maintains:

### Metadata

```
meta_<id>.txt
```

Stores:

- Current Term
- Voted For
- Commit Length

---

### Logs

```
logs_<id>.txt
```

Contains replicated log entries.

---

### Committed Logs

```
committed_logs_<id>.txt
```

Represents the replicated state machine.

---

### Debug Dump

```
dump_<id>.txt
```

Stores runtime events including:

- Elections
- Votes
- Heartbeats
- Commits
- Leader transitions

---

## Running the Project

### Install Dependencies

```bash
pip install grpcio grpcio-tools protobuf
```

---

### Start the Cluster

Open three terminals.

Terminal 1

```bash
python node_1.py
```

Terminal 2

```bash
python node_2.py
```

Terminal 3

```bash
python node_3.py
```

One node will automatically become the leader.

---

### Start Client

```bash
python client.py
```

The client provides a simple CLI:

```
1. Set Value
2. Read Value
3. Exit
```

---

## Example

```
SET

Key: name
Value: Alice

✓ Value stored successfully
```

```
GET

Key: name

Alice
```

If a follower receives a request:

```
This node is not the leader.
Try contacting Node 0
```

The client reconnects automatically.

---

## Raft Components Implemented

- Randomized Election Timeout
- RequestVote RPC
- AppendEntries RPC
- Heartbeat Messages
- Leader Election
- Log Replication
- Commit Index
- State Machine Application
- Leader Lease Extension
- Persistent Metadata
- Client Request Routing

---

## Learning Outcomes

In this project, I did practical implementation of:

- Distributed Consensus
- Fault Tolerance
- Replicated State Machines
- Leader Election Algorithms
- RPC-based Distributed Systems
- Distributed Storage
- Concurrency in Python

---

## References

- Raft: In Search of an Understandable Consensus Algorithm
- gRPC Documentation
- Protocol Buffers Documentation

---

## Authors

**Shagun**

Developed as part of a Distributed Systems project implementing the Raft Consensus Algorithm from scratch in Python.
