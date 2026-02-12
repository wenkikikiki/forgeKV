# ForgeKV Failure Modes

This document describes ForgeKV's behavior under various failure scenarios.

## Failure Matrix

| Failure Type | Detection | Impact | Recovery |
|--------------|-----------|--------|----------|
| Leader crash | Election timeout (1s) | Writes blocked until new leader | Automatic re-election |
| Follower crash | N/A | No impact (quorum maintained) | Rejoins via log or snapshot |
| Network partition (1 node) | Heartbeat timeout | Partitioned node steps down if leader | Heals when network recovers |
| Network partition (leader isolated) | Quorum timeout (500ms) | Leader rejects writes immediately | New leader elected by majority |
| Disk stall (leader) | Write timeout | Increased latency, potential leader change | Recovers when disk resumes |
| Disk stall (follower) | Replication lag | No immediate impact | Catches up when disk resumes |
| Disk full | Write errors | Writes fail | Manual intervention required |
| Memory pressure | OOM killer | Node crash | Automatic restart + recovery |

## Detailed Scenarios

### 1. Leader Crash

**Scenario**: The leader node crashes unexpectedly.

**Detection**:
- Followers detect missing heartbeats
- Election timeout triggers after 1 second (10 ticks)

**Behavior**:
- In-flight writes receive no response (timeout)
- Reads fail with NOT_LEADER
- Followers start election

**Recovery**:
- New leader elected within ~1-2 seconds
- Clients retry with idempotent (client_id, seq)
- Duplicate writes detected, cached replies returned

**Client Experience**:
```
Put request → timeout → retry with same (client_id, seq) → OK (applied once)
```

### 2. Follower Crash

**Scenario**: A follower node crashes.

**Detection**:
- Leader notices missing heartbeat responses
- Replication lag metrics increase

**Behavior**:
- Quorum maintained (2 of 3 nodes)
- Writes and reads continue normally
- Replication proceeds to remaining follower

**Recovery**:
- Crashed follower restarts
- Loads state from Raft WAL
- Catches up via log replication
- If too far behind, receives snapshot

**Client Experience**:
```
All operations continue normally (no impact)
```

### 3. Network Partition (Leader Isolated)

**Scenario**: Network partition isolates the leader from both followers.

**Detection**:
- Leader: No heartbeat responses for 500ms (QuorumTimeout)
- Followers: No heartbeats for 1s (ElectionTimeout)

**Behavior**:
- Leader immediately rejects writes with NO_QUORUM
- Followers elect new leader among themselves
- Old leader eventually steps down

**Recovery**:
- Partition heals
- Old leader discovers new term, becomes follower
- Cluster resumes normal operation

**Client Experience**:
```
Put to old leader → NO_QUORUM (fast, <500ms)
Put to new leader → OK
```

### 4. Network Partition (Follower Isolated)

**Scenario**: One follower is partitioned from the cluster.

**Detection**:
- Leader: Heartbeat to partitioned follower times out
- Partitioned follower: No heartbeats, starts election (fails - no quorum)

**Behavior**:
- Quorum maintained (leader + 1 follower)
- Writes and reads continue
- Partitioned follower repeatedly times out elections

**Recovery**:
- Partition heals
- Follower discovers higher term, becomes follower
- Catches up via log replication

**Client Experience**:
```
All operations continue normally (no impact)
```

### 5. Disk Stall (Leader)

**Scenario**: Leader experiences slow disk (e.g., fsync delays).

**Detection**:
- Write latency increases
- Backpressure metrics spike
- Potential heartbeat delays

**Behavior**:
- Backpressure may trigger (DELAY or REJECT)
- If delays exceed heartbeat interval, leader may lose leadership
- Followers may elect new leader

**Recovery**:
- Disk performance recovers
- If still leader: resumes normal operation
- If lost leadership: becomes follower, catches up

**Client Experience**:
```
Put → OVERLOADED (if backpressure)
OR
Put → timeout → retry → OK (possibly to new leader)
```

### 6. Disk Stall (Follower)

**Scenario**: A follower experiences slow disk.

**Detection**:
- Replication lag increases for that follower
- Peer replication lag metrics

**Behavior**:
- Writes continue (quorum doesn't require all nodes)
- Stalled follower falls behind
- May trigger snapshot transfer if too far behind

**Recovery**:
- Disk recovers
- Follower catches up via log or snapshot

**Client Experience**:
```
All operations continue normally (no impact)
```

### 7. Split Brain Prevention

**Scenario**: Network partition creates two groups, each trying to operate.

**Prevention Mechanisms**:
1. **Quorum requirement**: Writes require majority (2 of 3)
2. **Quorum fail-fast**: Minority partition rejects writes immediately
3. **Term tracking**: Old leader discovers higher term, steps down

**Behavior**:
- Only majority partition can make progress
- Minority partition rejects writes with NO_QUORUM
- No split-brain possible

**Client Experience**:
```
Minority side: Put → NO_QUORUM
Majority side: Put → OK
```

### 8. Idempotency Under Failure

**Scenario**: Client retries after timeout, but original write succeeded.

**Mechanism**:
1. Client sends Put(k, v, client_id=c1, seq=1)
2. Leader commits and applies
3. Response lost (network issue)
4. Client retries Put(k, v, client_id=c1, seq=1)
5. State machine detects seq == last_seq
6. Returns cached reply (no re-execution)

**Verification**:
```
Before: key=k → value=v (applied once)
After retry: key=k → value=v (same, not v+v or error)
```

### 9. Crash Recovery

**Scenario**: Node crashes and restarts.

**Recovery Steps**:
1. Load HardState from Raft storage (term, vote, commit)
2. Load log entries from WAL
3. Replay any uncommitted entries
4. Load snapshot if exists
5. Rebuild session manager from session keys
6. Resume Raft participation

**Durability Guarantee**:
- All committed entries survive crash
- Applied state matches committed state after recovery

### 10. Snapshot and Log Compaction

**Scenario**: Follower far behind, needs snapshot.

**Trigger**:
- Leader's log doesn't contain entries follower needs
- Follower requests entries that are compacted

**Process**:
1. Leader creates Pebble checkpoint
2. Streams checkpoint to follower
3. Follower installs snapshot
4. Follower resumes normal replication

**Invariant**:
- Snapshot contains all committed state
- No data loss during snapshot transfer

## Testing Recommendations

### Unit Tests

- Session rules (seq == last_seq, <, +1)
- Deterministic eviction order
- Quorum fail-fast gate
- Backpressure thresholds

### Integration Tests

- Leader election timing
- Replication correctness
- Snapshot transfer

### Chaos Tests

Run with `make chaos-test`:
- Random partitions + linearizability check
- Random crashes + recovery verification
- Combined faults + correctness validation

## Monitoring Alerts

### Critical

| Condition | Alert |
|-----------|-------|
| `forgekv_raft_is_leader == 0` for all nodes | No leader |
| `forgekv_raft_peer_replication_lag > 10000` | Severe replication lag |
| `forgekv_backpressure_rejects_total` increasing | Sustained overload |

### Warning

| Condition | Alert |
|-----------|-------|
| `forgekv_raft_leader_changes_total` > 3/hour | Unstable leadership |
| `forgekv_pebble_l0_files > 40` | Compaction pressure |
| `forgekv_rpc_latency_ms{quantile="0.99"} > 100` | High tail latency |
