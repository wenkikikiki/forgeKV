// Package config provides configuration management for ForgeKV nodes.
package config

import (
	"fmt"
	"strings"
	"time"
)

// Config holds all configuration for a ForgeKV node.
type Config struct {
	// Node identity
	NodeID string

	// Network addresses
	ClientAddr  string
	RaftAddr    string
	AdminAddr   string
	MetricsAddr string

	// Peer configuration (map of nodeID -> raft address)
	Peers map[string]string
	// Peer client addresses (map of nodeID -> client address)
	PeerClientAddrs map[string]string

	// Storage
	DataDir string

	// Raft timing
	TickInterval     time.Duration
	ElectionTimeout  int // in ticks
	HeartbeatTimeout int // in ticks

	// Quorum
	QuorumTimeout time.Duration

	// Backpressure thresholds
	L0SoftFiles         int
	L0HardFiles         int
	L0SoftBytes         int64
	L0HardBytes         int64
	DisableBackpressure bool

	// Pebble tuning (0 = use Pebble defaults)
	PebbleMemTableSize int

	// Session management
	MaxSessions          int
	SessionWindowEntries uint64

	// Snapshot triggers
	SnapshotEntries uint64
	SnapshotWALSize int64

	// Observability
	JaegerEndpoint string
	EnableTracing  bool
}

// DefaultConfig returns a configuration with default values.
func DefaultConfig() *Config {
	return &Config{
		NodeID:          "n1",
		ClientAddr:      "127.0.0.1:9001",
		RaftAddr:        "127.0.0.1:9101",
		AdminAddr:       "127.0.0.1:9201",
		MetricsAddr:     "127.0.0.1:9301",
		Peers:           make(map[string]string),
		PeerClientAddrs: make(map[string]string),
		DataDir:         "/tmp/forgekv/n1",

		// Raft timing (from spec)
		TickInterval:     100 * time.Millisecond,
		ElectionTimeout:  10, // 1s = 10 ticks
		HeartbeatTimeout: 1,  // 100ms = 1 tick

		// Quorum timeout (spec: ElectionTimeout / 2)
		QuorumTimeout: 500 * time.Millisecond,

		// Backpressure thresholds (from spec)
		L0SoftFiles: 20,
		L0HardFiles: 64,
		L0SoftBytes: 256 * 1024 * 1024,  // 256MB
		L0HardBytes: 1024 * 1024 * 1024, // 1GB

		// Session management (from spec)
		MaxSessions:          100_000,
		SessionWindowEntries: 1_000_000,

		// Snapshot triggers (from spec)
		SnapshotEntries: 50_000,
		SnapshotWALSize: 512 * 1024 * 1024, // 512MB

		// Observability
		JaegerEndpoint: "localhost:4317",
		EnableTracing:  true,
	}
}

// ParsePeers parses a comma-separated peer string into a map.
// Format: n1=host:port,n2=host:port,n3=host:port
func ParsePeers(peersStr string) (map[string]string, error) {
	return parseNodeAddrMap(peersStr)
}

// ParsePeerClientAddrs parses a comma-separated client address string into a map.
// Format: n1=host:port,n2=host:port,n3=host:port
func ParsePeerClientAddrs(peersStr string) (map[string]string, error) {
	return parseNodeAddrMap(peersStr)
}

func parseNodeAddrMap(peersStr string) (map[string]string, error) {
	peers := make(map[string]string)
	if peersStr == "" {
		return peers, nil
	}

	parts := strings.Split(peersStr, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid peer format: %s (expected nodeID=host:port)", part)
		}
		nodeID := strings.TrimSpace(kv[0])
		addr := strings.TrimSpace(kv[1])
		if nodeID == "" || addr == "" {
			return nil, fmt.Errorf("invalid peer format: %s", part)
		}
		peers[nodeID] = addr
	}
	return peers, nil
}

// Validate checks the configuration for errors.
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node ID is required")
	}
	if c.ClientAddr == "" {
		return fmt.Errorf("client address is required")
	}
	if c.RaftAddr == "" {
		return fmt.Errorf("raft address is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}
	if len(c.Peers) < 3 {
		return fmt.Errorf("at least 3 peers are required for a quorum")
	}
	if _, ok := c.Peers[c.NodeID]; !ok {
		return fmt.Errorf("this node (%s) must be included in peers", c.NodeID)
	}
	if len(c.PeerClientAddrs) < len(c.Peers) {
		return fmt.Errorf("peer client addresses must be provided for all peers")
	}
	if _, ok := c.PeerClientAddrs[c.NodeID]; !ok {
		return fmt.Errorf("this node (%s) must be included in peer client addresses", c.NodeID)
	}
	for peerID := range c.Peers {
		if _, ok := c.PeerClientAddrs[peerID]; !ok {
			return fmt.Errorf("missing peer client address for node %s", peerID)
		}
	}
	return nil
}

// NodeIDToRaftID converts a string node ID to a uint64 Raft ID.
// This uses a simple hash-based approach for consistency.
func NodeIDToRaftID(nodeID string) uint64 {
	// Simple mapping: n1->1, n2->2, n3->3
	// For more complex setups, use a proper hash
	switch nodeID {
	case "n1":
		return 1
	case "n2":
		return 2
	case "n3":
		return 3
	default:
		// Fallback: use string hash
		var h uint64
		for _, c := range nodeID {
			h = h*31 + uint64(c)
		}
		if h == 0 {
			h = 1
		}
		return h
	}
}

// RaftIDToNodeID converts a uint64 Raft ID to a string node ID.
func RaftIDToNodeID(raftID uint64) string {
	switch raftID {
	case 1:
		return "n1"
	case 2:
		return "n2"
	case 3:
		return "n3"
	default:
		return fmt.Sprintf("node-%d", raftID)
	}
}
