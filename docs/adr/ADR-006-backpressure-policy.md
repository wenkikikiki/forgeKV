# ADR-006: Backpressure Policy Based on Pebble L0 Pressure Signals

## Status

Accepted

## Context

Even with Raft consensus working correctly, the system can fail under high write load:
- Pebble compaction falls behind
- L0 files accumulate
- Write stalls occur
- Tail latency spikes
- Replication lag grows
- Retry storms cascade

We need admission control to prevent overload.

## Decision

We implement **compaction-aware backpressure** using Pebble L0 health signals.

## Rationale

### Why L0-Based Backpressure

1. **Leading indicator**: L0 growth predicts future write stalls
2. **Measurable**: Pebble exposes L0 metrics directly
3. **Actionable**: Can delay or reject before actual stall
4. **Local**: Each node makes independent decisions

### Pebble L0 Behavior

In LSM-tree storage:
- Writes go to memtable, then flush to L0
- L0 files are unsorted, expensive to read
- Compaction merges L0 files to lower levels
- Too many L0 files → compaction can't keep up → write stall

### Health Signals

We sample every 250ms:
```go
metrics := db.Metrics()
l0Files := metrics.Levels[0].NumFiles
l0Bytes := metrics.Levels[0].Size
writeStalled := // from EventListener
```

### Backpressure Policy

**Thresholds (locked):**
| Parameter | Value | Action |
|-----------|-------|--------|
| L0SoftFiles | 20 | Delay |
| L0HardFiles | 64 | Reject |
| L0SoftBytes | 256MB | Delay |
| L0HardBytes | 1GB | Reject |

**Decision logic:**
```go
func (bc *BackpressureController) Check() (result, delay) {
    l0Files, l0Bytes, writeStalled := storage.GetHealth()

    // Hard limit: reject immediately
    if l0Files >= L0HardFiles || l0Bytes >= L0HardBytes || writeStalled {
        return BackpressureReject, 0
    }

    // Soft limit: apply delay
    if l0Files >= L0SoftFiles || l0Bytes >= L0SoftBytes {
        delayMs := min(25, 2 * (l0Files - L0SoftFiles))
        return BackpressureDelay, delayMs
    }

    return BackpressureNone, 0
}
```

### Response Codes

- **DELAY**: Apply bounded delay, then proceed
- **REJECT**: Return `OVERLOADED` error immediately

Clients receiving `OVERLOADED` should:
1. Back off exponentially
2. Retry to same or different node
3. Alert if sustained

## Implementation

### Sampling

```go
func (s *Storage) sampleMetrics() {
    ticker := time.NewTicker(250 * time.Millisecond)
    for range ticker.C {
        metrics := s.db.Metrics()
        s.l0Files = int(metrics.Levels[0].NumFiles)
        s.l0Bytes = metrics.Levels[0].Size
        observability.UpdatePebbleMetrics(s.l0Files, s.l0Bytes)
    }
}
```

### Write Path Integration

```go
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
    // Check leader
    if !s.raftNode.IsLeader() {
        return &pb.PutResponse{Error: NOT_LEADER}
    }

    // Apply backpressure
    if !s.bp.ApplyBackpressure() {
        return &pb.PutResponse{Error: OVERLOADED}
    }

    // Proceed with proposal
    ...
}
```

## Invariants

1. **Local-only**: Backpressure is not replicated (each node decides independently)
2. **Correctness preserved**: Only affects availability/latency, not consistency
3. **Bounded delay**: Maximum 25ms delay in soft region

## Observability

Metrics exposed:
- `forgekv_pebble_l0_files` - Current L0 file count
- `forgekv_pebble_l0_bytes` - Current L0 size
- `forgekv_pebble_write_stall_total` - Write stall events
- `forgekv_backpressure_delay_ms_total` - Total delay applied
- `forgekv_backpressure_rejects_total` - Total rejections

Dashboard panels:
- L0 files/bytes over time
- Backpressure delay/reject rates
- Correlation with P99 latency

## Consequences

### Positive

- Prevents cascading failures under load
- Bounded tail latency
- Clients get clear signal to back off
- Protects cluster stability

### Negative

- Some requests rejected under load
- Added latency in soft region
- Requires client retry logic

### Trade-offs

- **Availability vs latency**: We prefer rejecting requests over unbounded latency
- **Simplicity vs precision**: Fixed thresholds vs adaptive algorithms

## Tuning

The thresholds were chosen based on:
- Pebble defaults (L0 compaction trigger at 4 files)
- Safety margin before Pebble's internal write stall (100 files)
- Observed behavior under benchmark load

For production, tune based on:
- Disk I/O capacity
- Compaction throughput
- Acceptable P99 latency

## References

- [Pebble compaction](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md)
- [RocksDB write stalls](https://github.com/facebook/rocksdb/wiki/Write-Stalls)
- [CockroachDB admission control](https://www.cockroachlabs.com/docs/stable/admission-control.html)
