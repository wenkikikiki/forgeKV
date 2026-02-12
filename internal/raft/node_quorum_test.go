package raft

import (
	"testing"
	"time"

	"github.com/forgekv/forgekv/internal/config"
)

func TestHasQuorum_RecentAcks(t *testing.T) {
	now := time.Now()
	n := &Node{
		cfg:            &config.Config{QuorumTimeout: 100 * time.Millisecond},
		raftID:         1,
		quorumSize:     2,
		peerAck:        map[uint64]time.Time{2: now},
		lastQuorumTime: now.Add(-time.Second),
	}

	if !n.HasQuorum() {
		t.Fatalf("expected quorum with recent ack")
	}
	if n.lastQuorumTime.Before(now) {
		t.Fatalf("expected lastQuorumTime to update")
	}
}

func TestHasQuorum_StaleAcks(t *testing.T) {
	now := time.Now()
	n := &Node{
		cfg:            &config.Config{QuorumTimeout: 50 * time.Millisecond},
		raftID:         1,
		quorumSize:     2,
		peerAck:        map[uint64]time.Time{2: now.Add(-time.Second)},
		lastQuorumTime: now,
	}

	if n.HasQuorum() {
		t.Fatalf("expected quorum to be lost with stale acks")
	}
	if n.lastQuorumTime != now {
		t.Fatalf("expected lastQuorumTime to remain unchanged")
	}
}
