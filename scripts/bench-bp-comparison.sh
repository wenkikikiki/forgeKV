#!/usr/bin/env bash
# bench-bp-comparison.sh — A/B benchmark comparing backpressure ON vs OFF
# under write-heavy load with lowered L0 thresholds and injected fsync delay.
#
# Usage: ./scripts/bench-bp-comparison.sh [--duration 60s] [--fsync-delay-ms 5]
#
# Produces:
#   bench/REPORT-bp-on.md   (backpressure enabled, low thresholds)
#   bench/REPORT-bp-off.md  (backpressure disabled, same conditions)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$ROOT_DIR/bin"
BENCH_DIR="$ROOT_DIR/bench"
DATA_DIR="/tmp/forgekv-bp-bench"

# Defaults — tuned to trigger backpressure on short benchmarks.
# The key insight: Pebble compaction on SSDs keeps L0 file count near 0-1 even
# under write-heavy load. To reliably trigger the backpressure controller, we
# lower the L0 byte thresholds so that even a single L0 file's worth of data
# exceeds the soft limit. A small memtable forces frequent flushes.
DURATION="${DURATION:-60s}"
FSYNC_DELAY_MS="${FSYNC_DELAY_MS:-5}"
CONCURRENCY="${CONCURRENCY:-64}"
VALUE_SIZE="${VALUE_SIZE:-256}"
L0_SOFT="${L0_SOFT:-4}"
L0_HARD="${L0_HARD:-8}"
L0_SOFT_BYTES="${L0_SOFT_BYTES:-4096}"       # 4KB — triggers delay during memtable flushes
L0_HARD_BYTES="${L0_HARD_BYTES:-16384}"     # 16KB — triggers reject during L0 flush spikes
MEMTABLE_SIZE="${MEMTABLE_SIZE:-65536}"     # 64KB — forces frequent L0 flushes
SETTLE_SECS="${SETTLE_SECS:-5}"

# Node addresses
CLIENT_ADDRS="127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003"
ADMIN_ADDRS="127.0.0.1:9201,127.0.0.1:9202,127.0.0.1:9203"
METRICS_ADDRS="127.0.0.1:9301,127.0.0.1:9302,127.0.0.1:9303"
PEERS="n1=127.0.0.1:9101,n2=127.0.0.1:9102,n3=127.0.0.1:9103"
PEER_CLIENTS="n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003"

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --duration) DURATION="$2"; shift 2 ;;
    --fsync-delay-ms) FSYNC_DELAY_MS="$2"; shift 2 ;;
    --concurrency) CONCURRENCY="$2"; shift 2 ;;
    --l0-soft) L0_SOFT="$2"; shift 2 ;;
    --l0-hard) L0_HARD="$2"; shift 2 ;;
    --memtable-size) MEMTABLE_SIZE="$2"; shift 2 ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
done

cleanup() {
  echo "Cleaning up cluster..."
  pkill -f "forgekv node" 2>/dev/null || true
  sleep 1
  rm -rf "$DATA_DIR"
}

start_cluster() {
  local extra_flags="${1:-}"
  echo "Starting 3-node cluster${extra_flags:+ with flags: $extra_flags}..."
  mkdir -p "$DATA_DIR/n1" "$DATA_DIR/n2" "$DATA_DIR/n3"

  for i in 1 2 3; do
    local client_port=$((9000 + i))
    local raft_port=$((9100 + i))
    local admin_port=$((9200 + i))
    local metrics_port=$((9300 + i))

    # shellcheck disable=SC2086
    "$BIN_DIR/forgekv" node \
      --id "n${i}" \
      --data-dir "$DATA_DIR/n${i}" \
      --client-addr "127.0.0.1:${client_port}" \
      --raft-addr "127.0.0.1:${raft_port}" \
      --admin-addr "127.0.0.1:${admin_port}" \
      --metrics-addr "127.0.0.1:${metrics_port}" \
      --peers "$PEERS" \
      --peer-client-addrs "$PEER_CLIENTS" \
      --jaeger-addr "localhost:4317" \
      --pebble-memtable-size "$MEMTABLE_SIZE" \
      $extra_flags \
      > "$DATA_DIR/n${i}.log" 2>&1 &
  done
}

wait_for_leader() {
  echo "Waiting for leader election..."
  local attempts=0
  local max_attempts=30
  while [ $attempts -lt $max_attempts ]; do
    # Use --json output and check that leader_id is non-empty.
    # Plain "forgekvctl status" returns success as soon as any node responds,
    # even before a leader is elected (connectToLeader falls through when
    # LeaderClientAddr is empty). Parsing the JSON ensures we only proceed
    # once Raft has actually elected a leader.
    local output
    if output=$("$BIN_DIR/forgekvctl" status --json --timeout 2s 2>/dev/null); then
      local leader_id
      leader_id=$(echo "$output" | grep -o '"leader_id": *"[^"]*"' | head -1 | sed 's/"leader_id": *"//;s/"//')
      if [ -n "$leader_id" ]; then
        echo "Leader elected: $leader_id"
        return 0
      fi
    fi
    attempts=$((attempts + 1))
    sleep 1
  done
  echo "ERROR: Leader not elected after ${max_attempts}s"
  return 1
}

inject_fsync_delay() {
  local delay_ms="$1"
  echo "Injecting fsync delay of ${delay_ms}ms on all nodes..."
  for node in n1 n2 n3; do
    "$BIN_DIR/forgekvctl" admin diskstall --node "$node" --fsync-delay-ms "$delay_ms" 2>/dev/null || true
  done
}

run_bench() {
  local output_file="$1"
  echo "Running writeheavy benchmark (duration=$DURATION, concurrency=$CONCURRENCY)..."
  "$BIN_DIR/forgekvbench" \
    --workload writeheavy \
    --duration "$DURATION" \
    --concurrency "$CONCURRENCY" \
    --value-size "$VALUE_SIZE" \
    --addrs "$CLIENT_ADDRS" \
    --admin-addrs "$ADMIN_ADDRS" \
    --metrics-addrs "$METRICS_ADDRS" \
    --measure-failover=false \
    --output "$output_file"
}

# ─── Main ───────────────────────────────────────────────────────────────────

trap cleanup EXIT

echo "============================================"
echo " ForgeKV Backpressure A/B Comparison"
echo "============================================"
echo "  L0 soft files:   $L0_SOFT"
echo "  L0 hard files:   $L0_HARD"
echo "  L0 soft bytes:   ${L0_SOFT_BYTES}B"
echo "  L0 hard bytes:   ${L0_HARD_BYTES}B"
echo "  Fsync delay:     ${FSYNC_DELAY_MS}ms"
echo "  MemTable size:   ${MEMTABLE_SIZE}B"
echo "  Duration:        $DURATION"
echo "  Concurrency:     $CONCURRENCY"
echo "  Value size:      ${VALUE_SIZE}B"
echo ""

mkdir -p "$BENCH_DIR"

# ─── Run A: Backpressure ON ────────────────────────────────────────────────

echo ""
echo "===== Phase A: Backpressure ON (L0 soft=$L0_SOFT, hard=$L0_HARD) ====="
cleanup
start_cluster "--l0-soft-files $L0_SOFT --l0-hard-files $L0_HARD --l0-soft-bytes $L0_SOFT_BYTES --l0-hard-bytes $L0_HARD_BYTES"
wait_for_leader
inject_fsync_delay "$FSYNC_DELAY_MS"
echo "Settling for ${SETTLE_SECS}s..."
sleep "$SETTLE_SECS"
run_bench "$BENCH_DIR/REPORT-bp-on.md"
echo "Report A saved to $BENCH_DIR/REPORT-bp-on.md"

# ─── Run B: Backpressure OFF ──────────────────────────────────────────────

echo ""
echo "===== Phase B: Backpressure OFF (same thresholds, disabled) ====="
cleanup
start_cluster "--l0-soft-files $L0_SOFT --l0-hard-files $L0_HARD --l0-soft-bytes $L0_SOFT_BYTES --l0-hard-bytes $L0_HARD_BYTES --disable-backpressure"
wait_for_leader
inject_fsync_delay "$FSYNC_DELAY_MS"
echo "Settling for ${SETTLE_SECS}s..."
sleep "$SETTLE_SECS"
run_bench "$BENCH_DIR/REPORT-bp-off.md"
echo "Report B saved to $BENCH_DIR/REPORT-bp-off.md"

# ─── Summary ──────────────────────────────────────────────────────────────

echo ""
echo "============================================"
echo " A/B Comparison Summary"
echo "============================================"
echo ""

extract_metric() {
  local file="$1"
  local label="$2"
  grep -m1 "^- ${label}:" "$file" 2>/dev/null | sed 's/.*: //' || echo "N/A"
}

# Extract write latency from the "### Write Latency" section specifically
extract_write_latency() {
  local file="$1"
  local percentile="$2"
  # Extract from Write Latency section until next section header
  sed -n '/^### Write Latency/,/^###/p' "$file" 2>/dev/null | grep -m1 "^- ${percentile}:" | sed 's/.*: //' || echo "N/A"
}

echo "Metric                  | BP ON              | BP OFF"
echo "------------------------|--------------------|--------------------"

for metric in "Throughput" "Writes" "Errors"; do
  val_on=$(extract_metric "$BENCH_DIR/REPORT-bp-on.md" "$metric")
  val_off=$(extract_metric "$BENCH_DIR/REPORT-bp-off.md" "$metric")
  printf "%-24s| %-19s| %s\n" "$metric" "$val_on" "$val_off"
done

echo ""
echo "Write Latency:"
for percentile in "P50" "P95" "P99"; do
  val_on=$(extract_write_latency "$BENCH_DIR/REPORT-bp-on.md" "$percentile")
  val_off=$(extract_write_latency "$BENCH_DIR/REPORT-bp-off.md" "$percentile")
  printf "  %-22s| %-19s| %s\n" "$percentile" "$val_on" "$val_off"
done

echo ""
echo "Backpressure Counters:"
for metric in "Total Backpressure Delay" "Total Backpressure Rejects"; do
  val_on=$(extract_metric "$BENCH_DIR/REPORT-bp-on.md" "$metric")
  val_off=$(extract_metric "$BENCH_DIR/REPORT-bp-off.md" "$metric")
  printf "  %-22s| %-19s| %s\n" "$metric" "$val_on" "$val_off"
done

echo ""
echo "Full reports: $BENCH_DIR/REPORT-bp-on.md, $BENCH_DIR/REPORT-bp-off.md"
echo "Done."
