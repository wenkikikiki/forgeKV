package sm

import (
	"testing"

	pb "github.com/forgekv/forgekv/api/forgekv"
)

func TestSessionManager_NewSession(t *testing.T) {
	sm := NewSessionManager(100, 1000)

	// First request for new client should execute
	execute, cached, outOfOrder := sm.CheckAndUpdate("client1", 1)
	if !execute {
		t.Error("expected execute=true for new session")
	}
	if cached != nil {
		t.Error("expected no cached reply for new session")
	}
	if outOfOrder {
		t.Error("expected outOfOrder=false for new session")
	}

	// New session with seq != 1 should be out of order
	execute, cached, outOfOrder = sm.CheckAndUpdate("client2", 2)
	if execute {
		t.Error("expected execute=false for new session with seq gap")
	}
	if cached != nil {
		t.Error("expected no cached reply for out-of-order new session")
	}
	if !outOfOrder {
		t.Error("expected outOfOrder=true for new session with seq gap")
	}
}

func TestSessionManager_DuplicateDetection(t *testing.T) {
	sm := NewSessionManager(100, 1000)

	// First request
	sm.CheckAndUpdate("client1", 1)
	reply := &pb.CachedReply{Code: pb.Code_OK, Message: "success"}
	recordReply(t, sm, "client1", 1, reply, 1)

	// Duplicate request (same seq)
	execute, cached, outOfOrder := sm.CheckAndUpdate("client1", 1)
	if execute {
		t.Error("expected execute=false for duplicate")
	}
	if cached == nil {
		t.Fatal("expected cached reply for duplicate")
	}
	if cached.Code != pb.Code_OK {
		t.Errorf("expected cached reply code OK, got %v", cached.Code)
	}
	if outOfOrder {
		t.Error("expected outOfOrder=false for duplicate")
	}
}

func TestSessionManager_OutOfOrder(t *testing.T) {
	sm := NewSessionManager(100, 1000)

	// First request with seq=1
	sm.CheckAndUpdate("client1", 1)
	recordReply(t, sm, "client1", 1, &pb.CachedReply{Code: pb.Code_OK}, 1)

	// Out of order request (seq < last_seq)
	execute, cached, outOfOrder := sm.CheckAndUpdate("client1", 0)
	if execute {
		t.Error("expected execute=false for out-of-order seq")
	}
	if cached != nil {
		t.Error("expected no cached reply for out-of-order seq")
	}
	if !outOfOrder {
		t.Error("expected outOfOrder=true for out-of-order seq")
	}

	// Gap request (seq > last_seq+1) should also be out of order
	execute, cached, outOfOrder = sm.CheckAndUpdate("client1", 3)
	if execute {
		t.Error("expected execute=false for seq gap")
	}
	if cached != nil {
		t.Error("expected no cached reply for seq gap")
	}
	if !outOfOrder {
		t.Error("expected outOfOrder=true for seq gap")
	}
}

func TestSessionManager_ValidNextSeq(t *testing.T) {
	sm := NewSessionManager(100, 1000)

	// First request
	sm.CheckAndUpdate("client1", 1)
	recordReply(t, sm, "client1", 1, &pb.CachedReply{Code: pb.Code_OK}, 1)

	// Valid next sequence
	execute, cached, outOfOrder := sm.CheckAndUpdate("client1", 2)
	if !execute {
		t.Error("expected execute=true for valid next seq")
	}
	if cached != nil {
		t.Error("expected no cached reply for new seq")
	}
	if outOfOrder {
		t.Error("expected outOfOrder=false")
	}
}

func TestSessionManager_EvictionByWindow(t *testing.T) {
	// Small window for testing
	sm := NewSessionManager(1000, 10)

	// Create session at index 1
	sm.CheckAndUpdate("client1", 1)
	recordReply(t, sm, "client1", 1, &pb.CachedReply{Code: pb.Code_OK}, 1)

	// Verify session exists
	if sm.Count() != 1 {
		t.Errorf("expected 1 session, got %d", sm.Count())
	}

	// Create another session at index 20 (beyond window)
	sm.CheckAndUpdate("client2", 1)
	recordReply(t, sm, "client2", 1, &pb.CachedReply{Code: pb.Code_OK}, 20)

	// client1 should be evicted (lastSeenIndex 1 < 20 - 10 = 10)
	if sm.Count() != 1 {
		t.Errorf("expected 1 session after eviction, got %d", sm.Count())
	}

	// Verify client1 is gone and client2 exists
	if _, ok := sm.GetSession("client1"); ok {
		t.Error("client1 should have been evicted")
	}
	if _, ok := sm.GetSession("client2"); !ok {
		t.Error("client2 should still exist")
	}
}

func TestSessionManager_EvictionByMaxSessions(t *testing.T) {
	// Very small max for testing
	sm := NewSessionManager(2, 1000000)

	// Create 3 sessions
	sm.CheckAndUpdate("client1", 1)
	recordReply(t, sm, "client1", 1, &pb.CachedReply{Code: pb.Code_OK}, 1)

	sm.CheckAndUpdate("client2", 1)
	recordReply(t, sm, "client2", 1, &pb.CachedReply{Code: pb.Code_OK}, 2)

	sm.CheckAndUpdate("client3", 1)
	recordReply(t, sm, "client3", 1, &pb.CachedReply{Code: pb.Code_OK}, 3)

	// Should have evicted oldest (client1)
	if sm.Count() != 2 {
		t.Errorf("expected 2 sessions after max eviction, got %d", sm.Count())
	}

	if _, ok := sm.GetSession("client1"); ok {
		t.Error("client1 (oldest) should have been evicted")
	}
}

func TestSessionManager_DeterministicEviction(t *testing.T) {
	// Test that eviction order is deterministic
	sm1 := NewSessionManager(2, 1000000)
	sm2 := NewSessionManager(2, 1000000)

	// Add same sessions in same order
	for _, sm := range []*SessionManager{sm1, sm2} {
		sm.CheckAndUpdate("clientA", 1)
		recordReply(t, sm, "clientA", 1, &pb.CachedReply{Code: pb.Code_OK}, 1)

		sm.CheckAndUpdate("clientB", 1) // Same index as clientA
		recordReply(t, sm, "clientB", 1, &pb.CachedReply{Code: pb.Code_OK}, 1)

		sm.CheckAndUpdate("clientC", 1)
		recordReply(t, sm, "clientC", 1, &pb.CachedReply{Code: pb.Code_OK}, 2)
	}

	// Both should have evicted the same client (alphabetically first with same index)
	_, hasA1 := sm1.GetSession("clientA")
	_, hasB1 := sm1.GetSession("clientB")
	_, hasA2 := sm2.GetSession("clientA")
	_, hasB2 := sm2.GetSession("clientB")

	if hasA1 != hasA2 || hasB1 != hasB2 {
		t.Error("eviction was not deterministic between two session managers")
	}
}

func recordReply(t *testing.T, sm *SessionManager, clientID string, seq uint64, reply *pb.CachedReply, index uint64) {
	t.Helper()
	if _, _, err := sm.RecordReply(clientID, seq, reply, index); err != nil {
		t.Fatalf("RecordReply failed: %v", err)
	}
}
