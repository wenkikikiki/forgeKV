# ADR-004: Idempotency via Replicated Sessions + Apply-Index GC

## Status

Accepted

## Context

Clients may retry requests after timeouts, even if the original request was committed. Without idempotency, retries can cause duplicate effects (e.g., incrementing a counter twice).

Options considered:

1. **No idempotency** - Simple but incorrect under retries
2. **Client-side deduplication** - Shifts complexity to clients
3. **TTL-based sessions** - Simple but non-deterministic (wall-clock in state machine)
4. **Replicated sessions with apply-index GC** - Deterministic, correct

## Decision

We implement **idempotency via replicated sessions with deterministic apply-index-based GC**.

## Rationale

### Why Replicated Sessions

1. **Correctness**: Guarantees exactly-once semantics for (client_id, seq) pairs
2. **Server-side**: Clients don't need special deduplication logic
3. **Survives failover**: Sessions replicated via Raft, survive leader changes
4. **Survives restarts**: Sessions persisted in Pebble, survive node restarts

### Why Apply-Index GC (No TTL)

**The problem with TTL:**
- Wall-clock time is non-deterministic
- Different nodes may have different clocks
- Same log entry could have different effects on different nodes
- Violates state machine determinism requirement

**Apply-index GC:**
- Uses logical time (Raft apply index)
- Deterministic across all nodes
- Same eviction decisions on all replicas

### Session Mechanism

Every write request includes `(client_id, seq)`.

State machine maintains per-client session:
```go
type Session struct {
    ClientID      string
    LastSeq       uint64
    LastReply     *CachedReply
    LastSeenIndex uint64  // Raft apply index when last seen
}
```

**Apply rules:**
```go
if seq == session.LastSeq {
    // Duplicate - return cached reply
    return session.LastReply
}
if seq < session.LastSeq {
    // Out of order - client bug or replay
    return OUT_OF_ORDER
}
if seq == session.LastSeq + 1 {
    // Valid next sequence
    execute()
    cache(reply)
    return reply
}
// seq > lastSeq+1 => OUT_OF_ORDER (gap)
```

### Deterministic GC Algorithm

**Configuration:**
- `MaxSessions = 100,000`
- `SessionWindowEntries = 1,000,000`

**On each applied entry at applyIndex = i:**

```go
// Step 1: Evict by window
threshold := i - SessionWindowEntries
for session in sessions {
    if session.LastSeenIndex < threshold {
        evict(session)
    }
}

// Step 2: Evict by count (if still over limit)
while len(sessions) > MaxSessions {
    // Evict oldest by (LastSeenIndex, ClientID)
    oldest := heap.Pop()  // min-heap ordered by (LastSeenIndex, ClientID)
    delete(sessions, oldest)
}
```

**Determinism guarantee:**
- Eviction order uses `(LastSeenIndex, ClientID)` as sort key
- `ClientID` as tie-breaker ensures consistent ordering
- No iteration over Go maps for eviction (use min-heap)

## Implementation Details

### Session Storage

Sessions stored in Pebble with prefix `0x02`:
```
Key:   0x02 | client_id
Value: SessionRecord (protobuf)
```

### Min-Heap for Eviction

```go
type sessionHeapEntry struct {
    clientID      string
    lastSeenIndex uint64
    heapIndex     int
}

// Ordered by (lastSeenIndex, clientID)
func (h sessionHeap) Less(i, j int) bool {
    if h[i].lastSeenIndex != h[j].lastSeenIndex {
        return h[i].lastSeenIndex < h[j].lastSeenIndex
    }
    return h[i].clientID < h[j].clientID
}
```

### Recovery

On snapshot restore:
1. Iterate session prefix keys in Pebble
2. Deserialize each session
3. Rebuild heap with same ordering

## Consequences

### Positive

- Exactly-once semantics for writes
- Deterministic across all nodes
- Survives leader changes and restarts
- No clock synchronization required

### Negative

- Memory overhead for session state
- CPU overhead for heap operations
- Requires client cooperation (client_id, seq)

### Client Requirements

Clients must:
1. Generate unique `client_id` (e.g., UUID)
2. Maintain monotonically increasing `seq` per client
3. Use same `(client_id, seq)` for retries of same request

## References

- [Raft paper - Client interaction](https://raft.github.io/raft.pdf) (Section 8)
- [etcd client protocol](https://etcd.io/docs/v3.5/learning/api/#response-header)
