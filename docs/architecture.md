# ForgeKV Architecture

## Overview

ForgeKV is a 3-node replicated key-value store designed for:
- **Correctness**: Linearizable reads and writes
- **Durability**: Crash recovery via persistent Raft log and snapshots
- **Observability**: Full visibility into system behavior
- **Testability**: Mac-friendly fault injection

```
┌─────────────────────────────────────────────────────────────────┐
│                         ForgeKV Cluster                          │
│                                                                   │
│   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐     │
│   │    Node 1   │      │    Node 2   │      │    Node 3   │     │
│   │   (Leader)  │◄────►│  (Follower) │◄────►│  (Follower) │     │
│   └──────┬──────┘      └──────┬──────┘      └──────┬──────┘     │
│          │                    │                    │             │
│          ▼                    ▼                    ▼             │
│   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐     │
│   │   Pebble    │      │   Pebble    │      │   Pebble    │     │
│   └─────────────┘      └─────────────┘      └─────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

## Node Architecture

Each ForgeKV node consists of:

```
┌─────────────────────────────────────────────────────────────┐
│                        ForgeKV Node                          │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                     gRPC Server                       │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐     │   │
│  │  │  KV API    │  │ Admin API  │  │Raft Transport│   │   │
│  │  │(Put/Get/   │  │(Fault Inj) │  │ (Messages)  │     │   │
│  │  │CAS/Delete) │  └────────────┘  └────────────┘     │   │
│  │  └─────┬──────┘                                       │   │
│  └────────┼──────────────────────────────────────────────┘   │
│           │                                                   │
│  ┌────────▼──────────────────────────────────────────────┐   │
│  │                     Raft Node                          │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│  │  │   Propose   │  │  ReadIndex  │  │   Apply     │   │   │
│  │  │   Channel   │  │   Handler   │  │   Loop      │   │   │
│  │  └─────────────┘  └─────────────┘  └──────┬──────┘   │   │
│  └───────────────────────────────────────────┼───────────┘   │
│                                              │               │
│  ┌───────────────────────────────────────────▼───────────┐   │
│  │                   State Machine                        │   │
│  │  ┌─────────────┐  ┌─────────────┐                     │   │
│  │  │   Session   │  │   Apply     │                     │   │
│  │  │   Manager   │  │   Logic     │                     │   │
│  │  └─────────────┘  └──────┬──────┘                     │   │
│  └──────────────────────────┼────────────────────────────┘   │
│                             │                                 │
│  ┌──────────────────────────▼────────────────────────────┐   │
│  │                      Storage                           │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│  │  │   Pebble    │  │ Backpressure│  │    FS       │   │   │
│  │  │     DB      │  │  Controller │  │  Wrapper    │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘   │   │
│  └───────────────────────────────────────────────────────┘   │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐   │
│  │                  Observability                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│  │  │  Prometheus │  │   Tracing   │  │  Structured │   │   │
│  │  │   Metrics   │  │   (OTLP)    │  │   Logging   │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘   │   │
│  └───────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────┘
```

## Data Flow

### Write Path

```
Client                Leader                    Followers
  │                     │                           │
  │  Put(k,v,cid,seq)   │                           │
  │────────────────────►│                           │
  │                     │                           │
  │              ┌──────┴──────┐                    │
  │              │ 1. Check    │                    │
  │              │    Leader   │                    │
  │              │ 2. Check    │                    │
  │              │    Quorum   │                    │
  │              │ 3. Check    │                    │
  │              │  Backpressure                    │
  │              └──────┬──────┘                    │
  │                     │                           │
  │                     │  AppendEntries            │
  │                     │──────────────────────────►│
  │                     │                           │
  │                     │  AppendEntriesResp        │
  │                     │◄──────────────────────────│
  │                     │                           │
  │              ┌──────┴──────┐                    │
  │              │ 4. Commit   │                    │
  │              │    (quorum) │                    │
  │              │ 5. Apply    │                    │
  │              │    to SM    │                    │
  │              └──────┬──────┘                    │
  │                     │                           │
  │  PutResponse(OK)    │                           │
  │◄────────────────────│                           │
```

### Read Path (Linearizable via ReadIndex)

```
Client                Leader                    Followers
  │                     │                           │
  │  Get(key)           │                           │
  │────────────────────►│                           │
  │                     │                           │
  │              ┌──────┴──────┐                    │
  │              │ 1. Check    │                    │
  │              │    Leader   │                    │
  │              │ 2. ReadIndex│                    │
  │              │    Request  │                    │
  │              └──────┬──────┘                    │
  │                     │                           │
  │                     │  Heartbeat (confirm lead) │
  │                     │──────────────────────────►│
  │                     │                           │
  │                     │  HeartbeatResp            │
  │                     │◄──────────────────────────│
  │                     │                           │
  │              ┌──────┴──────┐                    │
  │              │ 3. Wait for │                    │
  │              │    applied  │                    │
  │              │    >= read  │                    │
  │              │    index    │                    │
  │              │ 4. Read     │                    │
  │              │    Pebble   │                    │
  │              └──────┬──────┘                    │
  │                     │                           │
  │  GetResponse(value) │                           │
  │◄────────────────────│                           │
```

## Key Invariants

### 1. Linearizability

**Write Invariant**: A write is acknowledged only after:
- Entry is committed by Raft (replicated to majority)
- Entry is applied to the state machine
- Response is sent to client

**Read Invariant**: A read returns a value that was current at some point between request and response:
- Leader confirms leadership via ReadIndex
- Applied index catches up to read index
- Value is read from Pebble

### 2. Idempotency

**Session Invariant**: For any (client_id, seq) pair, the state machine mutation occurs at most once.

Apply rules:
- `seq == last_seq`: Return cached reply (no re-execution)
- `seq < last_seq`: Return OUT_OF_ORDER error
- `seq == last_seq + 1`: Execute operation, cache reply
- `seq > last_seq + 1`: Return OUT_OF_ORDER error (gap)

### 3. Deterministic Session GC

**Eviction Invariant**: Session eviction is deterministic across all nodes.

Eviction rules (in order):
1. Evict sessions where `last_seen_index < apply_index - SessionWindowEntries`
2. If `len(sessions) > MaxSessions`, evict oldest by `(last_seen_index, client_id)`

No wall-clock TTL is used in replicated state.

### 4. Quorum Fail-Fast

**Quorum Invariant**: A partitioned leader rejects writes immediately.

- Leader tracks `millisSinceQuorum` from heartbeat responses
- If `millisSinceQuorum > QuorumTimeout (500ms)`:
  - Reject writes with `NO_QUORUM`
  - No indefinite hangs

### 5. Backpressure

**Backpressure Invariant**: Backpressure decisions are local-only and don't affect correctness.

Thresholds:
- `L0Files >= L0HardFiles (64)` OR `L0Bytes >= L0HardBytes (1GB)`: REJECT
- `L0Files >= L0SoftFiles (20)` OR `L0Bytes >= L0SoftBytes (256MB)`: DELAY

## Data Model

### Key Prefixes

| Prefix | Purpose |
|--------|---------|
| `0x01` | User key-value data |
| `0x02` | Session records |
| `0x03` | Internal metadata |

### Raft Storage Keys

| Key | Purpose |
|-----|---------|
| `raft_hard_state` | Persisted HardState (term, vote, commit) |
| `raft_conf_state` | Configuration state |
| `raft_snapshot_meta` | Snapshot metadata |
| `raft_entry_<index>` | Log entries |

## Snapshot Strategy

### Trigger Conditions

Snapshot is created when either:
- `appliedIndex - lastSnapshotIndex >= 50,000 entries`
- `WAL size exceeds 512MB`

### Snapshot Contents

- Full Pebble checkpoint (user keys + sessions + metadata)
- Packaged as tar archive for streaming transfer

### Restore Procedure

1. Stop applying new entries
2. Replace Pebble directory with checkpoint
3. Reopen Pebble
4. Rebuild session heap from session prefix keys
5. Update Raft storage with snapshot index/term
6. Resume Raft

## Fault Injection

### Network Faults (Transport Layer)

- **Partition**: `drop_pct=100` on both directions
- **Delay**: Fixed delay + jitter per link
- **Loss**: Configurable `drop_pct`

Applied to Raft transport traffic only (client traffic unaffected).

### Disk Faults (FS Layer)

- Artificial `fsync_delay_ms` via wrapped filesystem
- Toggleable per node via Admin API

### Process Crash

- `Admin.Crash` exits process immediately
- Tests crash recovery and leader election
