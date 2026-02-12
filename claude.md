# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Source of Truth

`docs/internal/spec.md` is the ultimate source of truth for project design. Always reference that for design decisions, invariants, and expected behavior.

## Build & Development Commands

```bash
make build              # Build all 3 binaries (forgekv, forgekvctl, forgekvbench) into ./bin/
make proto              # Regenerate protobuf code (auto-runs before build)
make deps               # Install Go deps + protoc plugins
make lint               # Run golangci-lint
make clean              # Remove binaries, generated proto, /tmp/forgekv data
```

## Testing

```bash
make test                  # Unit tests with -race and -cover (./internal/...)
make test-integration      # Integration tests (-tags=integration ./tests/...)
make chaos-test            # Linearizability tests with Porcupine (-tags=chaos, 5min timeout)

# Run a single test
go test -v -race -run TestName ./internal/sm/
go test -v -race -tags=integration -run TestName ./tests/integration/
go test -v -race -tags=chaos -timeout 5m -run TestName ./tests/chaos/
```

Integration tests use build tag `integration`, chaos tests use `chaos`.

Before opening a PR, run at least `make test`, `make test-integration`, and `make lint`. Run `make chaos-test` for changes touching raft, storage, transport, or failover behavior.

## Demo Cluster

```bash
make demo-up       # Start 3 nodes + Docker observability stack (Prometheus/Grafana/Jaeger)
make demo-down     # Stop everything and clean /tmp/forgekv
```

Node ports: n1=9001/9101/9201/9301, n2=9002/9102/9202/9302, n3=9003/9103/9203/9303 (client/raft/admin/metrics).

## Architecture Overview

ForgeKV is a 3-node replicated key-value store built for demonstrating distributed systems on a single MacBook. It provides linearizable reads and writes.

### Request Flow

```
Client → gRPC Server → Raft Consensus → State Machine → Pebble Storage
                           ↕
                    Peer Raft Transport (gRPC)
```

**Write path**: gRPC → propose to Raft → quorum commit → state machine apply → Pebble batch commit → respond
**Read path**: gRPC → Raft ReadIndex barrier → Pebble get → respond

### Core Modules (`internal/`)

- **`raft/`** — Wraps etcd/raft. `node.go` handles proposals, leader election, ReadIndex, snapshots. `storage.go` is the Pebble-backed WAL. `transport.go` is gRPC peer messaging with injectable link rules (drop%, delay, jitter).
- **`sm/`** — Deterministic state machine. `statemachine.go` applies Put/Delete/CAS proposals. `sessions.go` tracks per-client (client_id, seq) for exactly-once idempotency with apply-index-based GC (no wall-clock TTL) using a min-heap for eviction.
- **`storage/`** — `pebble.go` wraps CockroachDB's Pebble with prefix-based key namespaces (0x01=user, 0x02=sessions, 0x03=metadata) and a StallableFS for disk fault injection. `backpressure.go` does L0-based admission control (soft delay at 20 files/256MB, hard reject at 64 files/1GB).
- **`server/`** — `server.go` orchestrates everything: creates storage → state machine → raft node → transport, registers three gRPC servers (client KV, admin fault injection, raft transport), exposes metrics HTTP endpoint.
- **`config/`** — CLI flag parsing, defaults, peer discovery. Key tunables: raft tick/election intervals, backpressure thresholds, session limits, snapshot triggers.
- **`observability/`** — Prometheus metrics (RPC, raft, storage), OpenTelemetry traces (write/read spans), structured JSON logging with trace_id.

### API (defined in `api/forgekv.proto`)

- **KV Service**: Put, Get, Delete, CAS, Status
- **Admin Service**: SetLinkRule, ClearLinkRules, SetDiskStall, Crash (fault injection)
- **RaftTransport Service**: SendMessage, SendSnapshot (internal)

Error codes: OK, NOT_LEADER, NO_QUORUM, OVERLOADED, OUT_OF_ORDER, INVALID_ARGUMENT, INTERNAL.

### Key Design Invariants

- Linearizable writes via Raft quorum commit; linearizable reads via ReadIndex barrier
- Exactly-once semantics via replicated sessions keyed by (client_id, seq)
- Session GC is deterministic (apply-index window, not wall-clock TTL)
- Backpressure uses Pebble L0 file/byte counts, not fixed rate limits
- Fault injection is Mac-friendly (transport-level network faults, FS-level disk stalls) — no root required
- All chaos tests validate linearizability using Porcupine checker

## Conventions

- Commit subjects use prefixes: `feat(...)`, `fix`, `docs`, `ci`, `chore`. Keep them concise and imperative.
- Do not hand-edit generated `*.pb.go` files — update `api/forgekv.proto` and rerun `make proto`.
