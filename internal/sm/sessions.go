// Package sm implements the state machine and session management for ForgeKV.
package sm

import (
	"container/heap"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/forgekv/forgekv/api/forgekv"
)

// Session represents a client session for idempotency.
type Session struct {
	ClientID      string
	LastSeq       uint64 // Highest sequence number seen
	LastReply     *pb.CachedReply
	LastSeenIndex uint64
}

// sessionHeapEntry is an entry in the session eviction heap.
type sessionHeapEntry struct {
	clientID      string
	lastSeenIndex uint64
	index         int // index in the heap
}

// sessionHeap is a min-heap for session eviction ordered by (lastSeenIndex, clientID).
type sessionHeap []*sessionHeapEntry

func (h sessionHeap) Len() int { return len(h) }

func (h sessionHeap) Less(i, j int) bool {
	if h[i].lastSeenIndex != h[j].lastSeenIndex {
		return h[i].lastSeenIndex < h[j].lastSeenIndex
	}
	return h[i].clientID < h[j].clientID
}

func (h sessionHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *sessionHeap) Push(x interface{}) {
	n := len(*h)
	entry := x.(*sessionHeapEntry)
	entry.index = n
	*h = append(*h, entry)
}

func (h *sessionHeap) Pop() interface{} {
	old := *h
	n := len(old)
	entry := old[n-1]
	old[n-1] = nil
	entry.index = -1
	*h = old[0 : n-1]
	return entry
}

// SessionManager manages client sessions for idempotency.
type SessionManager struct {
	mu sync.RWMutex

	// Map of clientID -> Session
	sessions map[string]*Session

	// Min-heap for eviction ordering
	evictionHeap sessionHeap
	heapIndex    map[string]*sessionHeapEntry

	// Configuration
	maxSessions          int
	sessionWindowEntries uint64
}

// NewSessionManager creates a new session manager.
func NewSessionManager(maxSessions int, sessionWindowEntries uint64) *SessionManager {
	sm := &SessionManager{
		sessions:             make(map[string]*Session),
		evictionHeap:         make(sessionHeap, 0),
		heapIndex:            make(map[string]*sessionHeapEntry),
		maxSessions:          maxSessions,
		sessionWindowEntries: sessionWindowEntries,
	}
	heap.Init(&sm.evictionHeap)
	return sm
}

// CheckAndUpdate checks if an operation should be executed or returns cached reply.
// Returns:
// - execute: true if the operation should be executed
// - cachedReply: the cached reply if seq was already executed
// - outOfOrder: true if seq is out of order or has gaps
func (sm *SessionManager) CheckAndUpdate(clientID string, seq uint64) (execute bool, cachedReply *pb.CachedReply, outOfOrder bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, exists := sm.sessions[clientID]

	if !exists {
		// New session - only seq==1 is valid
		if seq != 1 {
			return false, nil, true
		}
		return true, nil, false
	}

	switch {
	case seq == session.LastSeq:
		// Duplicate request, return cached reply
		return false, session.LastReply, false
	case seq < session.LastSeq:
		return false, nil, true
	case seq == session.LastSeq+1:
		return true, nil, false
	default:
		// Gap detected
		return false, nil, true
	}
}

// RecordReply records the reply for a completed operation.
func (sm *SessionManager) RecordReply(clientID string, seq uint64, reply *pb.CachedReply, applyIndex uint64) ([]byte, []string, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	session, exists := sm.sessions[clientID]
	if !exists {
		session = &Session{
			ClientID: clientID,
		}
		sm.sessions[clientID] = session
	}

	session.LastSeq = seq
	session.LastReply = reply
	session.LastSeenIndex = applyIndex

	// Update or add to eviction heap
	if entry, ok := sm.heapIndex[clientID]; ok {
		entry.lastSeenIndex = applyIndex
		heap.Fix(&sm.evictionHeap, entry.index)
	} else {
		entry := &sessionHeapEntry{
			clientID:      clientID,
			lastSeenIndex: applyIndex,
		}
		heap.Push(&sm.evictionHeap, entry)
		sm.heapIndex[clientID] = entry
	}

	// Run eviction
	evicted := sm.evictLocked(applyIndex)

	data, err := session.Serialize()
	if err != nil {
		return nil, nil, err
	}
	return data, evicted, nil
}

// evictLocked runs the eviction logic (must hold lock).
func (sm *SessionManager) evictLocked(applyIndex uint64) []string {
	evicted := make([]string, 0)
	// Evict sessions outside the window
	threshold := uint64(0)
	if applyIndex > sm.sessionWindowEntries {
		threshold = applyIndex - sm.sessionWindowEntries
	}

	for sm.evictionHeap.Len() > 0 {
		entry := sm.evictionHeap[0]
		if entry.lastSeenIndex >= threshold {
			break
		}
		sm.evictSession(entry.clientID)
		evicted = append(evicted, entry.clientID)
	}

	// Evict oldest if over max sessions
	for sm.evictionHeap.Len() > sm.maxSessions {
		entry := heap.Pop(&sm.evictionHeap).(*sessionHeapEntry)
		delete(sm.heapIndex, entry.clientID)
		delete(sm.sessions, entry.clientID)
		evicted = append(evicted, entry.clientID)
	}
	return evicted
}

// evictSession removes a session.
func (sm *SessionManager) evictSession(clientID string) {
	if entry, ok := sm.heapIndex[clientID]; ok {
		heap.Remove(&sm.evictionHeap, entry.index)
		delete(sm.heapIndex, clientID)
	}
	delete(sm.sessions, clientID)
}

// GetSession returns a session by client ID.
func (sm *SessionManager) GetSession(clientID string) (*Session, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	session, ok := sm.sessions[clientID]
	return session, ok
}

// Count returns the number of active sessions.
func (sm *SessionManager) Count() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.sessions)
}

// Serialize serializes a session to bytes for storage.
func (s *Session) Serialize() ([]byte, error) {
	record := &pb.SessionRecord{
		LastSeq:       s.LastSeq,
		LastSeenIndex: s.LastSeenIndex,
	}
	if s.LastReply != nil {
		data, err := proto.Marshal(s.LastReply)
		if err != nil {
			return nil, err
		}
		record.LastReply = data
	}
	return proto.Marshal(record)
}

// DeserializeSession deserializes a session from bytes.
func DeserializeSession(clientID string, data []byte) (*Session, error) {
	record := &pb.SessionRecord{}
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, err
	}

	session := &Session{
		ClientID:      clientID,
		LastSeq:       record.LastSeq,
		LastSeenIndex: record.LastSeenIndex,
	}

	if len(record.LastReply) > 0 {
		reply := &pb.CachedReply{}
		if err := proto.Unmarshal(record.LastReply, reply); err != nil {
			return nil, err
		}
		session.LastReply = reply
	}

	return session, nil
}

// LoadSessions loads sessions from an iterator function.
func (sm *SessionManager) LoadSessions(iter func(fn func(clientID string, data []byte) error) error) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Clear existing state
	sm.sessions = make(map[string]*Session)
	sm.evictionHeap = make(sessionHeap, 0)
	sm.heapIndex = make(map[string]*sessionHeapEntry)
	heap.Init(&sm.evictionHeap)

	return iter(func(clientID string, data []byte) error {
		session, err := DeserializeSession(clientID, data)
		if err != nil {
			return err
		}

		sm.sessions[clientID] = session
		entry := &sessionHeapEntry{
			clientID:      clientID,
			lastSeenIndex: session.LastSeenIndex,
		}
		heap.Push(&sm.evictionHeap, entry)
		sm.heapIndex[clientID] = entry
		return nil
	})
}

// SessionsForSnapshot returns all sessions for snapshot creation.
func (sm *SessionManager) SessionsForSnapshot() []*Session {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	sessions := make([]*Session, 0, len(sm.sessions))
	for _, s := range sm.sessions {
		sessions = append(sessions, s)
	}
	return sessions
}
