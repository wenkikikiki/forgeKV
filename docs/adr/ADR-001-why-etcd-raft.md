# ADR-001: Why etcd/raft

## Status

Accepted

## Context

ForgeKV needs a consensus mechanism to replicate data across 3 nodes with strong consistency guarantees. The options considered were:

1. **Implement Raft from scratch** - Full control but high effort and risk
2. **Use etcd/raft library** - Proven implementation, requires integration work
3. **Use HashiCorp Raft** - Higher-level API but less flexibility
4. **Use Paxos** - More complex, less common in Go ecosystem

## Decision

We chose **etcd/raft** (`go.etcd.io/etcd/raft/v3`) as the consensus library.

## Rationale

### Why etcd/raft

1. **Battle-tested**: Powers etcd, which is used by Kubernetes in production at massive scale
2. **Correct**: Extensively tested for correctness, including Jepsen testing
3. **Flexible**: Low-level API allows us to control:
   - Storage backend (we use Pebble)
   - Transport mechanism (we use gRPC)
   - State machine application timing
4. **Go-native**: Written in Go, natural fit for Go projects
5. **Well-documented**: Clear separation of concerns and good documentation

### What We Implement vs Reuse

**Reused from etcd/raft:**
- Core Raft algorithm (leader election, log replication, safety)
- Configuration change handling
- ReadIndex mechanism
- Snapshot coordination

**Implemented by ForgeKV:**
- Persistent storage (Raft WAL via Pebble)
- Network transport (gRPC)
- State machine (apply logic, sessions)
- Snapshot creation/restoration (Pebble checkpoint)
- Quorum fail-fast gate
- Client-facing API

### Why Not Alternatives

**HashiCorp Raft:**
- Higher-level API reduces control
- Bundled storage and transport harder to customize
- Less mature ReadIndex support

**Implement from scratch:**
- High risk of correctness bugs
- Significant development time
- No benefit over battle-tested library

## Consequences

### Positive

- Reduced risk of consensus bugs
- Faster development time
- Access to proven features (ReadIndex, pre-vote)
- Community support and updates

### Negative

- Must understand etcd/raft's async/tick model
- Requires manual integration of storage and transport
- API requires careful handling of Ready struct

## References

- [etcd/raft documentation](https://pkg.go.dev/go.etcd.io/etcd/raft/v3)
- [Raft paper](https://raft.github.io/raft.pdf)
- [etcd Jepsen analysis](https://jepsen.io/analyses/etcd-3.4.3)
