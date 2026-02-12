package observability

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	metricsOnce sync.Once
	registry    *prometheus.Registry

	// RPC metrics
	RPCRequestsTotal *prometheus.CounterVec
	RPCLatency       *prometheus.HistogramVec
	RPCInflight      *prometheus.GaugeVec

	// Raft metrics
	RaftIsLeader          prometheus.Gauge
	RaftTerm              prometheus.Gauge
	RaftCommitIndex       prometheus.Gauge
	RaftAppliedIndex      prometheus.Gauge
	RaftLeaderChanges     prometheus.Counter
	RaftPeerReplicationLag *prometheus.GaugeVec

	// Pebble/Storage metrics
	PebbleL0Files           prometheus.Gauge
	PebbleL0Bytes           prometheus.Gauge
	PebbleWriteStallTotal   prometheus.Counter
	BackpressureDelayTotal  prometheus.Counter
	BackpressureRejectsTotal prometheus.Counter
)

// InitMetrics initializes all Prometheus metrics.
func InitMetrics(nodeID string) {
	metricsOnce.Do(func() {
		registry = prometheus.NewRegistry()

		// Add default Go collectors
		registry.MustRegister(collectors.NewGoCollector())
		registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

		factory := promauto.With(registry)

		// RPC metrics
		RPCRequestsTotal = factory.NewCounterVec(prometheus.CounterOpts{
			Name: "forgekv_rpc_requests_total",
			Help: "Total number of RPC requests",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}, []string{"method", "code"})

		RPCLatency = factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "forgekv_rpc_latency_ms",
			Help:    "RPC latency in milliseconds",
			Buckets: []float64{0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
			ConstLabels: prometheus.Labels{"node": nodeID},
		}, []string{"method", "code"})

		RPCInflight = factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "forgekv_rpc_inflight",
			Help: "Number of in-flight RPC requests",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}, []string{"method"})

		// Raft metrics
		RaftIsLeader = factory.NewGauge(prometheus.GaugeOpts{
			Name: "forgekv_raft_is_leader",
			Help: "Whether this node is the Raft leader (0/1)",
			ConstLabels: prometheus.Labels{"node": nodeID},
		})

		RaftTerm = factory.NewGauge(prometheus.GaugeOpts{
			Name: "forgekv_raft_term",
			Help: "Current Raft term",
			ConstLabels: prometheus.Labels{"node": nodeID},
		})

		RaftCommitIndex = factory.NewGauge(prometheus.GaugeOpts{
			Name: "forgekv_raft_commit_index",
			Help: "Current Raft commit index",
			ConstLabels: prometheus.Labels{"node": nodeID},
		})

		RaftAppliedIndex = factory.NewGauge(prometheus.GaugeOpts{
			Name: "forgekv_raft_applied_index",
			Help: "Current Raft applied index",
			ConstLabels: prometheus.Labels{"node": nodeID},
		})

		RaftLeaderChanges = factory.NewCounter(prometheus.CounterOpts{
			Name: "forgekv_raft_leader_changes_total",
			Help: "Total number of leader changes observed",
			ConstLabels: prometheus.Labels{"node": nodeID},
		})

		RaftPeerReplicationLag = factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "forgekv_raft_peer_replication_lag_entries",
			Help: "Replication lag in entries for each peer",
			ConstLabels: prometheus.Labels{"node": nodeID},
		}, []string{"peer"})

		// Pebble/Storage metrics
		PebbleL0Files = factory.NewGauge(prometheus.GaugeOpts{
			Name: "forgekv_pebble_l0_files",
			Help: "Number of L0 files in Pebble",
			ConstLabels: prometheus.Labels{"node": nodeID},
		})

		PebbleL0Bytes = factory.NewGauge(prometheus.GaugeOpts{
			Name: "forgekv_pebble_l0_bytes",
			Help: "Size of L0 in bytes in Pebble",
			ConstLabels: prometheus.Labels{"node": nodeID},
		})

		PebbleWriteStallTotal = factory.NewCounter(prometheus.CounterOpts{
			Name: "forgekv_pebble_write_stall_total",
			Help: "Total number of write stalls in Pebble",
			ConstLabels: prometheus.Labels{"node": nodeID},
		})

		BackpressureDelayTotal = factory.NewCounter(prometheus.CounterOpts{
			Name: "forgekv_backpressure_delay_ms_total",
			Help: "Total milliseconds of backpressure delay applied",
			ConstLabels: prometheus.Labels{"node": nodeID},
		})

		BackpressureRejectsTotal = factory.NewCounter(prometheus.CounterOpts{
			Name: "forgekv_backpressure_rejects_total",
			Help: "Total number of requests rejected due to backpressure",
			ConstLabels: prometheus.Labels{"node": nodeID},
		})
	})
}

// MetricsHandler returns the HTTP handler for Prometheus metrics.
func MetricsHandler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

// RecordRPCRequest records an RPC request with its result.
func RecordRPCRequest(method, code string, latencyMs float64) {
	RPCRequestsTotal.WithLabelValues(method, code).Inc()
	RPCLatency.WithLabelValues(method, code).Observe(latencyMs)
}

// IncInflight increments the in-flight count for a method.
func IncInflight(method string) {
	RPCInflight.WithLabelValues(method).Inc()
}

// DecInflight decrements the in-flight count for a method.
func DecInflight(method string) {
	RPCInflight.WithLabelValues(method).Dec()
}

// UpdateRaftState updates Raft-related metrics.
func UpdateRaftState(isLeader bool, term, commitIndex, appliedIndex uint64) {
	if isLeader {
		RaftIsLeader.Set(1)
	} else {
		RaftIsLeader.Set(0)
	}
	RaftTerm.Set(float64(term))
	RaftCommitIndex.Set(float64(commitIndex))
	RaftAppliedIndex.Set(float64(appliedIndex))
}

// UpdatePeerLag updates the replication lag metric for a peer.
func UpdatePeerLag(peer string, lag uint64) {
	RaftPeerReplicationLag.WithLabelValues(peer).Set(float64(lag))
}

// UpdatePebbleMetrics updates Pebble storage metrics.
func UpdatePebbleMetrics(l0Files int, l0Bytes int64) {
	PebbleL0Files.Set(float64(l0Files))
	PebbleL0Bytes.Set(float64(l0Bytes))
}

// RecordBackpressureDelay records backpressure delay applied.
func RecordBackpressureDelay(delayMs float64) {
	BackpressureDelayTotal.Add(delayMs)
}

// RecordBackpressureReject records a request rejected due to backpressure.
func RecordBackpressureReject() {
	BackpressureRejectsTotal.Inc()
}

// RecordLeaderChange records a leader change event.
func RecordLeaderChange() {
	RaftLeaderChanges.Inc()
}
