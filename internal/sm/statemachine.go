package sm

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/forgekv/forgekv/api/forgekv"
	"github.com/forgekv/forgekv/internal/config"
	"github.com/forgekv/forgekv/internal/observability"
	"github.com/forgekv/forgekv/internal/storage"
	"go.uber.org/zap"
)

// StateMachine implements the ForgeKV replicated state machine.
type StateMachine struct {
	mu sync.RWMutex

	storage  *storage.Storage
	sessions *SessionManager
	cfg      *config.Config
	logger   *zap.Logger

	appliedIndex uint64
}

// NewStateMachine creates a new state machine.
func NewStateMachine(store *storage.Storage, cfg *config.Config, logger *zap.Logger) *StateMachine {
	return &StateMachine{
		storage:  store,
		sessions: NewSessionManager(cfg.MaxSessions, cfg.SessionWindowEntries),
		cfg:      cfg,
		logger:   logger.Named("sm"),
	}
}

// ApplyResult contains the result of applying a proposal.
type ApplyResult struct {
	Code    pb.Code
	Message string
	Swapped bool   // For CAS
	Current []byte // For CAS
	Found   bool   // For CAS/Get
	Value   []byte // For Get
}

// Apply applies a committed proposal to the state machine.
func (sm *StateMachine) Apply(ctx context.Context, index uint64, data []byte) (*ApplyResult, error) {
	ctx, span := observability.StartSpan(ctx, "sm.apply")
	defer span.End()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Parse the proposal
	proposal := &pb.Proposal{}
	if err := proto.Unmarshal(data, proposal); err != nil {
		return &ApplyResult{Code: pb.Code_INTERNAL, Message: "failed to unmarshal proposal"}, nil
	}

	logger := sm.logger.With(
		zap.Uint64("index", index),
		zap.String("type", proposal.Type.String()),
		zap.String("client_id", proposal.ClientId),
		zap.Uint64("seq", proposal.Seq),
	)

	// Check idempotency
	execute, cachedReply, outOfOrder := sm.sessions.CheckAndUpdate(proposal.ClientId, proposal.Seq)

	if outOfOrder {
		logger.Warn("out of order sequence")
		return &ApplyResult{Code: pb.Code_OUT_OF_ORDER, Message: "sequence number out of order"}, nil
	}

	if !execute {
		// Return cached reply
		logger.Debug("returning cached reply")
		if cachedReply != nil {
			return &ApplyResult{
				Code:    cachedReply.Code,
				Message: cachedReply.Message,
				Swapped: cachedReply.Swapped,
				Current: cachedReply.Current,
				Found:   cachedReply.Found,
			}, nil
		}
		return &ApplyResult{Code: pb.Code_OK}, nil
	}

	// Execute the operation
	var result *ApplyResult
	batch := sm.storage.NewBatch()
	defer func() { _ = batch.Close() }()

	switch proposal.Type {
	case pb.Proposal_PUT:
		result = sm.applyPut(ctx, batch, proposal)
	case pb.Proposal_DELETE:
		result = sm.applyDelete(ctx, batch, proposal)
	case pb.Proposal_CAS:
		result = sm.applyCAS(ctx, batch, proposal)
	default:
		result = &ApplyResult{Code: pb.Code_INVALID_ARGUMENT, Message: "unknown operation type"}
	}

	// Record the reply for idempotency
	cachedReply = &pb.CachedReply{
		Code:    result.Code,
		Message: result.Message,
		Swapped: result.Swapped,
		Current: result.Current,
		Found:   result.Found,
	}

	sessionData, evicted, err := sm.sessions.RecordReply(proposal.ClientId, proposal.Seq, cachedReply, index)
	if err != nil {
		return &ApplyResult{Code: pb.Code_INTERNAL, Message: "failed to serialize session"}, nil
	}

	batch.PutSession(proposal.ClientId, sessionData)
	for _, clientID := range evicted {
		batch.DeleteSession(clientID)
	}

	if err := batch.Commit(); err != nil {
		return &ApplyResult{Code: pb.Code_INTERNAL, Message: err.Error()}, nil
	}

	// Update applied index
	sm.appliedIndex = index

	logger.Debug("applied proposal", zap.String("result", result.Code.String()))
	return result, nil
}

// applyPut applies a Put operation.
func (sm *StateMachine) applyPut(ctx context.Context, batch *storage.Batch, proposal *pb.Proposal) *ApplyResult {
	_, span := observability.StartSpan(ctx, "pebble.batch_commit")
	defer span.End()

	batch.Put(proposal.Key, proposal.Value)
	return &ApplyResult{Code: pb.Code_OK}
}

// applyDelete applies a Delete operation.
func (sm *StateMachine) applyDelete(ctx context.Context, batch *storage.Batch, proposal *pb.Proposal) *ApplyResult {
	_, span := observability.StartSpan(ctx, "pebble.batch_commit")
	defer span.End()

	batch.Delete(proposal.Key)
	return &ApplyResult{Code: pb.Code_OK}
}

// applyCAS applies a Compare-And-Swap operation.
func (sm *StateMachine) applyCAS(ctx context.Context, batch *storage.Batch, proposal *pb.Proposal) *ApplyResult {
	_, span := observability.StartSpan(ctx, "pebble.batch_commit")
	defer span.End()

	// Get current value
	current, found, err := sm.storage.Get(proposal.Key)
	if err != nil {
		return &ApplyResult{Code: pb.Code_INTERNAL, Message: err.Error()}
	}

	// Check expected value
	expectedMatch := false
	if !found && len(proposal.Expected) == 0 {
		// Expected empty, current empty - match
		expectedMatch = true
	} else if found && bytesEqual(current, proposal.Expected) {
		// Values match
		expectedMatch = true
	}

	if !expectedMatch {
		// Precondition failed
		return &ApplyResult{
			Code:    pb.Code_OK,
			Swapped: false,
			Current: current,
			Found:   found,
		}
	}

	// Apply the swap
	if len(proposal.Desired) == 0 {
		// Delete if desired is empty
		batch.Delete(proposal.Key)
	} else {
		batch.Put(proposal.Key, proposal.Desired)
	}

	return &ApplyResult{
		Code:    pb.Code_OK,
		Swapped: true,
		Current: proposal.Desired,
		Found:   len(proposal.Desired) > 0,
	}
}

// Get reads a value from storage (for linearizable reads after ReadIndex).
func (sm *StateMachine) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	_, span := observability.StartSpan(ctx, "pebble.get")
	defer span.End()

	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.storage.Get(key)
}

// AppliedIndex returns the last applied index.
func (sm *StateMachine) AppliedIndex() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.appliedIndex
}

// SetAppliedIndex sets the applied index (for recovery).
func (sm *StateMachine) SetAppliedIndex(index uint64) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.appliedIndex = index
}

// LoadSessions loads session state from storage.
func (sm *StateMachine) LoadSessions() error {
	return sm.sessions.LoadSessions(sm.storage.IterateSessions)
}

// SessionCount returns the number of active sessions.
func (sm *StateMachine) SessionCount() int {
	return sm.sessions.Count()
}

// CreateCheckpoint creates a checkpoint for snapshot.
func (sm *StateMachine) CreateCheckpoint(destPath string) error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.storage.CreateCheckpoint(destPath)
}

// Close closes the state machine.
func (sm *StateMachine) Close() error {
	return sm.storage.Close()
}

// bytesEqual compares two byte slices.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
