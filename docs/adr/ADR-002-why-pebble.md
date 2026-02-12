# ADR-002: Why Pebble (Not Custom LSM)

## Status

Accepted

## Context

ForgeKV needs a persistent storage engine for:
1. User key-value data
2. Session records for idempotency
3. Raft WAL and snapshots

The options considered were:

1. **Implement custom LSM** - Full control but massive effort
2. **Use RocksDB via CGO** - Mature but CGO complexity
3. **Use Pebble** - Pure Go, inspired by RocksDB/LevelDB
4. **Use BoltDB/bbolt** - Simple but B+tree may not scale

## Decision

We chose **Pebble** (`github.com/cockroachdb/pebble`) as the storage engine.

## Rationale

### Why Pebble

1. **Pure Go**: No CGO, simplifies builds and deployment
2. **Proven**: Powers CockroachDB in production
3. **Performance**: Designed for write-heavy workloads (LSM architecture)
4. **Compaction visibility**: Exposes L0 file counts and compaction metrics
5. **Checkpointing**: Native checkpoint support for snapshots
6. **Event hooks**: Write stall detection for backpressure

### Why Not Alternatives

**Custom LSM:**
- Would take months to implement correctly
- No benefit for ForgeKV's goals (not demonstrating LSM expertise)
- High risk of bugs affecting durability

**RocksDB:**
- CGO complexity (build issues, debugging difficulty)
- Harder to deploy (native library dependencies)
- Less Go-idiomatic API

**BoltDB/bbolt:**
- B+tree not optimal for write-heavy workloads
- Single-writer bottleneck
- No native compaction metrics

### What We Focus On

Since we're not implementing LSM internals, we focus on:

1. **Compaction pressure visibility** - Monitor L0 files/bytes
2. **Backpressure/admission control** - React to compaction pressure
3. **Disk stall simulation** - Inject delays via wrapped FS
4. **Benchmarks and tail-latency analysis** - Understand performance characteristics

## Consequences

### Positive

- Reduced development time (no LSM implementation)
- Proven durability guarantees
- Access to native checkpoint for snapshots
- Pure Go simplifies operations

### Negative

- Less control over storage internals
- Dependent on Pebble's API for metrics
- Must understand Pebble's write stall behavior

## Pebble Integration Details

### Key Prefixes

We use prefixed keys to avoid collisions:
- `0x01` - User data
- `0x02` - Session records
- `0x03` - Internal metadata

### Backpressure Signals

We monitor:
- `metrics.Levels[0].NumFiles` - L0 file count
- `metrics.Levels[0].Size` - L0 bytes
- Write stall events via `EventListener`

### Snapshot via Checkpoint

```go
// Create consistent snapshot
db.Checkpoint(destPath)
```

This creates a hardlink-based copy that's consistent without stopping writes.

## References

- [Pebble documentation](https://github.com/cockroachdb/pebble)
- [Pebble design doc](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md)
- [CockroachDB storage layer](https://www.cockroachlabs.com/blog/cockroachdb-on-rocksd/)
