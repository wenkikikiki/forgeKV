# ForgeKV

A 3-node replicated key-value store using etcd/raft for consensus and Pebble for storage.

## Features

- **Linearizable Writes**: All writes go through Raft consensus and are committed by a quorum before acknowledgment
- **Linearizable Reads**: Reads use Raft ReadIndex to ensure consistency
- **Idempotency**: Client sessions with (client_id, seq) ensure exactly-once semantics across retries
- **Crash Recovery**: Persistent Raft WAL and Pebble snapshots enable recovery after crashes
- **Backpressure**: Compaction-aware admission control prevents overload
- **Observability**: Prometheus metrics, structured JSON logs, OpenTelemetry traces (Jaeger)
- **Fault Injection**: Mac-friendly chaos testing without root privileges

## Quick Start

### One-Command Demo

```bash
# Build and start the cluster with observability stack
make build
make demo-up

# Test basic operations
./bin/forgekvctl status --any
./bin/forgekvctl put --key hello --value world --client c1 --seq 1
./bin/forgekvctl get --key hello

# Stop the cluster
make demo-down
```

### Requirements

- Go 1.22+
- Docker and Docker Compose (for observability stack)
- protoc with Go plugins (for proto generation)

### Install Dependencies

```bash
make deps
```

## API

ForgeKV exposes a gRPC API with the following operations:

### KV Service

- `Put(key, value, client_id, seq)` - Store a key-value pair
- `Get(key)` - Retrieve a value (linearizable)
- `Delete(key, client_id, seq)` - Delete a key
- `CAS(key, expected, desired, client_id, seq)` - Compare-and-swap
- `Status()` - Get node status

### Admin Service

- `SetLinkRule(src, dst, drop_pct, delay_ms, jitter_ms)` - Inject network faults
- `ClearLinkRules()` - Remove all network faults
- `SetDiskStall(fsync_delay_ms)` - Inject disk stall
- `Crash(exit_code)` - Crash the node

## Error Codes

| Code | Description |
|------|-------------|
| `OK` | Success |
| `NOT_LEADER` | Node is not the leader (includes leader hint) |
| `NO_QUORUM` | Leader cannot confirm quorum |
| `OVERLOADED` | System under backpressure |
| `OUT_OF_ORDER` | Sequence number out of order |
| `INVALID_ARGUMENT` | Invalid request |
| `INTERNAL` | Internal error |

## Guarantees

### Consistency
- **Writes**: Linearizable - acknowledged only after commit by quorum + apply
- **Reads**: Linearizable - served only by leader after ReadIndex barrier

### Durability
- All committed entries are persisted to Raft WAL
- Snapshots capture full state for recovery

### Idempotency
- Retry-safe writes via replicated client sessions
- Deterministic session GC based on apply index (no wall-clock TTL)
- Client write sequences must start at 1 and increment by 1; gaps/out-of-order return OUT_OF_ORDER

### Availability
- Requires majority (2 of 3 nodes) for writes
- Quorum fail-fast: partitioned leader rejects writes immediately

## Configuration

### Node Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--id` | n1 | Node ID (n1, n2, n3) |
| `--data-dir` | /tmp/forgekv/n1 | Data directory |
| `--client-addr` | 127.0.0.1:9001 | Client API address |
| `--raft-addr` | 127.0.0.1:9101 | Raft transport address |
| `--admin-addr` | 127.0.0.1:9201 | Admin API address |
| `--metrics-addr` | 127.0.0.1:9301 | Metrics endpoint |
| `--peers` | - | Peer addresses (n1=host:port,...) |
| `--peer-client-addrs` | - | Peer client addresses (n1=host:port,...) |

### Backpressure Thresholds

| Parameter | Value | Description |
|-----------|-------|-------------|
| L0SoftFiles | 20 | Soft limit for L0 file count (delay) |
| L0HardFiles | 64 | Hard limit for L0 file count (reject) |
| L0SoftBytes | 256MB | Soft limit for L0 size (delay) |
| L0HardBytes | 1GB | Hard limit for L0 size (reject) |

### Session Management

| Parameter | Value | Description |
|-----------|-------|-------------|
| MaxSessions | 100,000 | Maximum concurrent sessions |
| SessionWindowEntries | 1,000,000 | Session retention window in entries |

## Observability

### Metrics (Prometheus)

Access at `http://localhost:9301/metrics` (per node) or `http://localhost:9090` (Prometheus).

Key metrics:
- `forgekv_rpc_requests_total{method,code}` - Request counts
- `forgekv_rpc_latency_ms{method,code}` - Request latency histogram
- `forgekv_raft_is_leader` - Leader status (0/1)
- `forgekv_raft_commit_index` - Commit index
- `forgekv_pebble_l0_files` - L0 file count
- `forgekv_backpressure_rejects_total` - Backpressure rejections

### Tracing (Jaeger)

Access at `http://localhost:16686`.

Spans per request:
- Writes: `rpc.decode` → `raft.propose` → `raft.wait_commit` → `sm.apply` → `pebble.batch_commit`
- Reads: `rpc.decode` → `raft.read_index` → `pebble.get`

### Logging

Structured JSON logs to stdout with fields:
- `ts`, `level`, `msg`
- `node_id`, `term`, `is_leader`
- `trace_id` (when available)
- `client_id`, `seq`, `key_len`, `op` (for writes)

## Testing

```bash
# Unit tests
make test

# Integration tests
make test-integration

# Chaos tests with linearizability checking
make chaos-test

# Benchmarks
make bench
```

## Documentation

- [Architecture](docs/architecture.md) - System design and invariants
- [Failure Modes](docs/failure-modes.md) - Behavior under various failures
- [ADRs](docs/adr/) - Architecture Decision Records

## Project Structure

```
.
├── api/                    # Protobuf definitions
├── cmd/
│   ├── forgekv/           # Node binary
│   ├── forgekvctl/        # CLI client
│   └── forgekvbench/      # Benchmark tool
├── internal/
│   ├── config/            # Configuration
│   ├── observability/     # Metrics, tracing, logging
│   ├── raft/              # Raft wrapper, storage, transport
│   ├── server/            # gRPC server
│   ├── sm/                # State machine, sessions
│   └── storage/           # Pebble wrapper, backpressure
├── deploy/                # Docker Compose
├── dashboards/            # Grafana dashboards
├── demo/                  # Demo scripts
├── docs/                  # Documentation
├── bench/                 # Benchmark reports
└── tests/                 # Integration and chaos tests
```

## License

MIT
