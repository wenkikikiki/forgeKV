# ADR-003: Linearizable Reads via ReadIndex

## Status

Accepted

## Context

ForgeKV must provide linearizable reads. The options considered were:

1. **Read from leader without coordination** - Fast but not linearizable
2. **Read through Raft log** - Linearizable but expensive (log entry per read)
3. **Lease-based reads** - Fast but requires clock synchronization
4. **ReadIndex** - Linearizable, one round-trip, no log entry

## Decision

We implement **linearizable reads via ReadIndex**.

## Rationale

### Why ReadIndex

1. **Correctness**: Guarantees linearizability without relying on clocks
2. **Efficiency**: No log entry per read (saves disk I/O and replication)
3. **Latency**: Only requires one heartbeat round-trip to confirm leadership
4. **Simplicity**: Built into etcd/raft, minimal implementation

### How ReadIndex Works

```
1. Client sends Get(key) to leader
2. Leader records current commit index as "read index"
3. Leader sends heartbeat to confirm it's still leader (quorum responds)
4. Leader waits until applied index >= read index
5. Leader reads from local storage
6. Leader returns value to client
```

### Why Not Alternatives

**Read from leader without coordination:**
- A stale leader might serve reads after being partitioned
- Violates linearizability

**Read through Raft log:**
- Every read creates a log entry
- Unnecessary disk I/O and replication overhead
- Adds latency equal to write latency

**Lease-based reads:**
- Requires synchronized clocks (not reliable on all systems)
- Complex to implement correctly
- Clock drift can cause correctness issues

### Invariant

> A read returns a value that was current at some point between the request
> arriving at the leader and the response being sent.

This is enforced by:
1. Confirming leadership (heartbeat round-trip)
2. Waiting for applied index to catch up
3. Reading from local storage after both conditions are met

## Implementation

```go
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
    // 1. Request ReadIndex from Raft
    reqID := generateUniqueID()
    n.node.ReadIndex(ctx, reqID)

    // 2. Wait for ReadState response (confirms leadership)
    readIndex := <-n.readIndexResponse[reqID]

    // 3. Wait for applied index to catch up
    for n.appliedIndex < readIndex {
        wait()
    }

    return readIndex, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
    // Only leader serves reads
    if !s.raftNode.IsLeader() {
        return &pb.GetResponse{Error: NOT_LEADER}
    }

    // ReadIndex barrier
    _, err := s.raftNode.ReadIndex(ctx)
    if err != nil {
        return &pb.GetResponse{Error: INTERNAL}
    }

    // Read from storage (linearizable)
    value, found, err := s.sm.Get(ctx, req.Key)
    return &pb.GetResponse{Value: value, Found: found}
}
```

## Consequences

### Positive

- Linearizable reads without log entries
- Lower latency than log-based reads
- No clock dependencies
- Simpler than lease-based reads

### Negative

- Reads only served by leader (no read scaling)
- One heartbeat round-trip latency per read
- Leader must wait for applied index to catch up

### Trade-offs Accepted

- **No follower reads**: We prioritize correctness over read throughput
- **Heartbeat latency**: Acceptable for our use case (single-region)

## References

- [Raft ReadIndex](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf) (Section 6.4)
- [etcd linearizable reads](https://etcd.io/docs/v3.5/learning/api_guarantees/#linearizable-reads)
