# ADR-007: Mac-Friendly Chaos Injection at Transport/FS Layer

## Status

Accepted

## Context

ForgeKV must be testable on macOS without root privileges. Traditional chaos tools require:
- `tc` / `netem` for network faults (Linux only, root required)
- `iptables` for partitions (Linux only, root required)
- Kernel modules for disk faults

We need Mac-compatible alternatives.

## Decision

We implement **application-level chaos injection** at the transport and filesystem layers.

## Rationale

### Why Application-Level

1. **No root required**: Works in userspace
2. **Cross-platform**: Same code on macOS and Linux
3. **Precise control**: Can target specific node pairs
4. **Observable**: Easy to verify injection is working

### Implementation Layers

#### Network Faults (Transport Layer)

Inject faults in the Raft transport, not at OS level:

```go
type Transport struct {
    linkRules map[string]*LinkRule  // "src:dst" -> rule
}

type LinkRule struct {
    DropPct  uint32  // 0-100
    DelayMs  uint32  // Fixed delay
    JitterMs uint32  // +/- jitter
}

func (t *Transport) sendOne(msg raftpb.Message) {
    dstNode := nodeIDFromRaftID(msg.To)
    rule := t.linkRules[t.nodeID + ":" + dstNode]

    if rule != nil {
        // Apply drop
        if rand.Uint32() % 100 < rule.DropPct {
            return  // Drop message
        }

        // Apply delay
        delay := rule.DelayMs + randomJitter(rule.JitterMs)
        time.Sleep(delay)
    }

    // Send message
    t.client.SendMessage(msg)
}
```

**Fault types:**
| Fault | Implementation |
|-------|----------------|
| Partition | `drop_pct=100` both directions |
| Delay | `delay_ms > 0` |
| Jitter | `jitter_ms > 0` |
| Loss | `0 < drop_pct < 100` |

**Scope**: Only affects Raft transport. Client traffic remains direct for demo clarity.

#### Disk Faults (Filesystem Layer)

Wrap Pebble's filesystem interface:

```go
type StallableFS struct {
    vfs.FS
    storage *Storage
}

type StallableFile struct {
    vfs.File
    storage *Storage
}

func (f *StallableFile) Sync() error {
    delay := f.storage.fsyncDelay.Load()
    if delay > 0 {
        time.Sleep(time.Duration(delay) * time.Millisecond)
    }
    return f.File.Sync()
}
```

**Configuration**: `fsync_delay_ms` via Admin API

#### Process Crash

Simple implementation via Admin API:

```go
func (s *Server) Crash(ctx context.Context, req *pb.CrashRequest) (*pb.CrashResponse, error) {
    exitCode := req.ExitCode
    if exitCode == 0 {
        exitCode = 137  // Default: SIGKILL
    }
    go func() {
        time.Sleep(100 * time.Millisecond)
        os.Exit(int(exitCode))
    }()
    return &pb.CrashResponse{Error: OK}, nil
}
```

### Admin API

```protobuf
service Admin {
    // Network faults
    rpc SetLinkRule(SetLinkRuleRequest) returns (SetLinkRuleResponse);
    rpc ClearLinkRules(ClearLinkRulesRequest) returns (ClearLinkRulesResponse);

    // Disk faults
    rpc SetDiskStall(SetDiskStallRequest) returns (SetDiskStallResponse);

    // Process fault
    rpc Crash(CrashRequest) returns (CrashResponse);
}
```

## Test Scenarios

### Partition Test

```bash
# Partition leader
LEADER=$(./bin/forgekvctl status --any --json | jq -r .leader_id)
./bin/forgekvctl admin partition-leader --leader "$LEADER"

# Verify writes fail fast
./bin/forgekvctl put --key test --value val  # Should return NO_QUORUM

# Heal partition
./bin/forgekvctl admin heal

# Verify writes succeed
./bin/forgekvctl put --key test --value val  # Should succeed
```

### Crash Recovery Test

```bash
# Crash leader
./bin/forgekvctl admin crash --node n1

# Wait for election
sleep 2

# Verify new leader elected
./bin/forgekvctl status --any  # Should show different leader

# Restart crashed node
./bin/forgekv node --id n1 ...

# Verify it rejoins
./bin/forgekvctl status --any  # Should show n1 as follower
```

### Disk Stall Test

```bash
# Inject disk stall
./bin/forgekvctl admin diskstall --node n2 --fsync-delay-ms 150

# Run writes
./bin/forgekvctl bench --workload writeheavy --duration 30s

# Observe backpressure in Grafana
# L0 files should increase, backpressure counters should increase
```

## Linearizability Testing

Integration with porcupine for linearizability checking:

```go
func TestLinearizability(t *testing.T) {
    cluster := startCluster()
    defer cluster.Stop()

    // Run concurrent operations
    history := runConcurrentOps(cluster, 60*time.Second)

    // Inject partition during test
    cluster.PartitionLeader()
    time.Sleep(10 * time.Second)
    cluster.Heal()

    // Check linearizability
    ok := porcupine.CheckOperations(kvModel, history)
    if !ok {
        t.Fatal("linearizability violation detected")
    }
}
```

## Consequences

### Positive

- Works on macOS without root
- Easy to use via CLI/API
- Reproducible in CI
- Precise fault targeting

### Negative

- Doesn't test OS-level issues
- Application must be chaos-aware
- Faults only affect ForgeKV traffic

### Accepted Limitations

- **No true network partition**: Messages could theoretically bypass transport layer
- **No true disk failure**: Fsync delay isn't same as disk error
- **No memory pressure**: Would require OS-level tools

These limitations are acceptable for demonstrating correctness in local development scenarios.

## References

- [Jepsen testing](https://jepsen.io/)
- [Chaos engineering principles](https://principlesofchaos.org/)
- [CockroachDB chaos testing](https://www.cockroachlabs.com/blog/diy-jepsen-testing-cockroachdb/)
