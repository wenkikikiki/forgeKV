# ADR-005: Snapshot Strategy via Pebble Checkpoint + Streaming Install

## Status

Accepted

## Context

ForgeKV needs snapshots for:
1. Log compaction (prevent unbounded WAL growth)
2. Fast follower catch-up (when log is truncated)
3. Crash recovery (restore full state)

Options considered:

1. **Serialize state to single blob** - Simple but blocks writes during serialization
2. **Copy-on-write fork** - Fast but OS-dependent
3. **Pebble checkpoint** - Consistent, non-blocking, built-in

## Decision

We use **Pebble checkpoint** for snapshot creation and **gRPC streaming** for transfer.

## Rationale

### Why Pebble Checkpoint

1. **Consistency**: Creates point-in-time consistent view
2. **Non-blocking**: Uses hardlinks, doesn't block writes
3. **Efficient**: Minimal disk space (hardlinks share data)
4. **Built-in**: Native Pebble feature, well-tested

### How Checkpoints Work

```go
// Creates a consistent snapshot directory
db.Checkpoint(destPath)
```

Internally:
1. Flushes memtable to ensure all data is on disk
2. Creates hardlinks to all SST files
3. Copies manifest and other metadata
4. Result is a complete, openable Pebble directory

### Snapshot Contents

The checkpoint captures:
- User key-value data (prefix `0x01`)
- Session records (prefix `0x02`)
- Internal metadata (prefix `0x03`)

Raft metadata (HardState, entries) is stored separately and not included.

### Snapshot Triggers

Snapshot created when either:
```go
if appliedIndex - lastSnapshotIndex >= 50_000 {
    createSnapshot()
}
// OR
if walSize > 512 * 1024 * 1024 {  // 512MB
    createSnapshot()
}
```

### Streaming Transfer

Snapshots transferred via gRPC streaming to handle large sizes:

```protobuf
service RaftTransport {
    rpc SendSnapshot(stream SnapshotChunk) returns (SnapshotResponse);
}

message SnapshotChunk {
    uint64 index = 1;
    uint64 term = 2;
    bytes data = 3;
    bool done = 4;
}
```

**Sending:**
1. Create checkpoint directory
2. Tar the directory
3. Stream chunks (64KB each)
4. Send done marker

**Receiving:**
1. Receive chunks to temp file
2. Extract tar to temp directory
3. Stop state machine
4. Replace data directory
5. Reopen Pebble
6. Resume

## Restore Procedure

```go
func installSnapshot(snap raftpb.Snapshot, data io.Reader) error {
    // 1. Stop applying entries
    sm.Lock()
    defer sm.Unlock()

    // 2. Extract snapshot to temp directory
    tempDir := extractTar(data)

    // 3. Close current Pebble
    sm.storage.Close()

    // 4. Replace data directory
    os.RemoveAll(dataDir)
    os.Rename(tempDir, dataDir)

    // 5. Reopen Pebble
    sm.storage = storage.NewStorage(dataDir)

    // 6. Rebuild session heap
    sm.sessions.LoadSessions(sm.storage.IterateSessions)

    // 7. Update applied index
    sm.appliedIndex = snap.Metadata.Index

    return nil
}
```

## Invariants

1. **Snapshot consistency**: Checkpoint captures consistent state at a single point
2. **No data loss**: Only entries before snapshot index are compacted
3. **Deterministic restore**: All nodes restore to identical state from same snapshot

## Consequences

### Positive

- Non-blocking snapshot creation
- Efficient disk usage (hardlinks)
- Handles large state (streaming)
- Built-in Pebble feature

### Negative

- Requires disk space for checkpoint (briefly)
- Network transfer for large snapshots
- Must coordinate with Raft log compaction

### Trade-offs

- **Simplicity over optimization**: We use tar instead of more efficient formats
- **Correctness over speed**: Full state transfer instead of incremental

## References

- [Pebble Checkpoint](https://pkg.go.dev/github.com/cockroachdb/pebble#DB.Checkpoint)
- [Raft snapshots](https://raft.github.io/raft.pdf) (Section 7)
