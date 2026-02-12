package raft

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/forgekv/forgekv/internal/config"
	"github.com/forgekv/forgekv/internal/observability"
	"github.com/forgekv/forgekv/internal/sm"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pb "github.com/forgekv/forgekv/api/forgekv"
)

// PendingProposal tracks a proposal waiting for commit.
type PendingProposal struct {
	Key     string // ClientId:Seq
	Term    uint64
	Waiters map[uint64]chan *sm.ApplyResult
	nextID  uint64
}

// Node wraps etcd/raft and integrates with the ForgeKV state machine.
type Node struct {
	cfg       *config.Config
	node      raft.Node
	storage   *PebbleStorage
	sm        *sm.StateMachine
	transport *Transport
	logger    *zap.Logger

	// State
	raftID       uint64
	isLeader     atomic.Bool
	leaderID     atomic.Uint64
	term         atomic.Uint64
	commitIndex  atomic.Uint64
	appliedIndex atomic.Uint64

	// Quorum tracking
	lastQuorumTime time.Time
	quorumMu       sync.Mutex
	peerAck        map[uint64]time.Time
	quorumSize     int

	// Pending proposals waiting for commit (key: "ClientId:Seq")
	pendingMu sync.Mutex
	pending   map[string]*PendingProposal

	// Channels
	proposeC    chan []byte
	confChangeC chan raftpb.ConfChange
	stopC       chan struct{}
	doneC       chan struct{}

	// ReadIndex tracking
	readIndexMu  sync.Mutex
	readIndexReq map[string]chan uint64
}

// NewNode creates a new Raft node.
func NewNode(cfg *config.Config, storage *PebbleStorage, stateMachine *sm.StateMachine, transport *Transport, logger *zap.Logger) (*Node, error) {
	n := &Node{
		cfg:          cfg,
		storage:      storage,
		sm:           stateMachine,
		transport:    transport,
		logger:       logger.Named("raft"),
		raftID:       config.NodeIDToRaftID(cfg.NodeID),
		pending:      make(map[string]*PendingProposal),
		proposeC:     make(chan []byte, 1024),
		confChangeC:  make(chan raftpb.ConfChange),
		stopC:        make(chan struct{}),
		doneC:        make(chan struct{}),
		readIndexReq: make(map[string]chan uint64),
	}

	n.peerAck = make(map[uint64]time.Time, len(cfg.Peers))
	n.quorumSize = len(cfg.Peers)/2 + 1

	// Build peer list
	peers := make([]raft.Peer, 0, len(cfg.Peers))
	for nodeID := range cfg.Peers {
		peers = append(peers, raft.Peer{ID: config.NodeIDToRaftID(nodeID)})
	}

	// Get initial state from storage
	hs, cs, err := storage.InitialState()
	if err != nil {
		return nil, err
	}

	// Create Raft config
	c := &raft.Config{
		ID:                        n.raftID,
		ElectionTick:              cfg.ElectionTimeout,
		HeartbeatTick:             cfg.HeartbeatTimeout,
		Storage:                   storage,
		MaxSizePerMsg:             1024 * 1024, // 1MB
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
		PreVote:                   true,
	}

	// Start or restart node
	if raft.IsEmptyHardState(hs) && len(cs.Voters) == 0 {
		n.logger.Info("starting new raft node", zap.Uint64("id", n.raftID))
		n.node = raft.StartNode(c, peers)
	} else {
		n.logger.Info("restarting raft node", zap.Uint64("id", n.raftID))
		n.node = raft.RestartNode(c)
	}

	// Initialize quorum time
	n.lastQuorumTime = time.Now()

	return n, nil
}

// Run starts the main Raft loop.
func (n *Node) Run(ctx context.Context) {
	defer close(n.doneC)

	ticker := time.NewTicker(n.cfg.TickInterval)
	defer ticker.Stop()

	var prevLeader uint64

	for {
		select {
		case <-ctx.Done():
			n.node.Stop()
			return

		case <-n.stopC:
			n.node.Stop()
			return

		case <-ticker.C:
			n.node.Tick()

		case msg := <-n.transport.Receive():
			n.recordPeerAck(msg)
			if err := n.node.Step(ctx, msg); err != nil {
				n.logger.Warn("step failed", zap.Error(err))
			}

		case prop := <-n.proposeC:
			if err := n.node.Propose(ctx, prop); err != nil {
				n.logger.Warn("propose failed", zap.Error(err))
			}

		case cc := <-n.confChangeC:
			if err := n.node.ProposeConfChange(ctx, cc); err != nil {
				n.logger.Warn("propose conf change failed", zap.Error(err))
			}

		case rd := <-n.node.Ready():
			// Save HardState and entries
			if !raft.IsEmptyHardState(rd.HardState) {
				if err := n.storage.SaveHardState(rd.HardState); err != nil {
					n.logger.Error("failed to save hard state", zap.Error(err))
				}
				n.term.Store(rd.Term)
			}

			if len(rd.Entries) > 0 {
				if err := n.storage.Append(rd.Entries); err != nil {
					n.logger.Error("failed to append entries", zap.Error(err))
				}
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := n.storage.ApplySnapshot(rd.Snapshot); err != nil {
					n.logger.Error("failed to apply snapshot", zap.Error(err))
				}
			}

			// Send messages
			n.transport.Send(ctx, rd.Messages)

			// Apply committed entries
			for _, entry := range rd.CommittedEntries {
				n.applyEntry(ctx, entry)
			}

			// Handle ReadIndex responses
			for _, rs := range rd.ReadStates {
				n.handleReadState(rs)
			}

			// Update leader state
			if softState := rd.SoftState; softState != nil {
				leader := softState.Lead
				if leader != prevLeader {
					n.leaderID.Store(leader)
					n.isLeader.Store(leader == n.raftID)
					if leader == n.raftID {
						n.logger.Info("became leader", zap.Uint64("term", n.term.Load()))
						n.resetQuorum()
					} else if leader != 0 {
						n.logger.Info("new leader", zap.String("leader", config.RaftIDToNodeID(leader)))
					}
					observability.RecordLeaderChange()
					prevLeader = leader
				}
			}

			// Update metrics
			n.updateMetrics()

			// Advance the Raft state machine
			n.node.Advance()
		}
	}
}

// applyEntry applies a committed log entry.
func (n *Node) applyEntry(ctx context.Context, entry raftpb.Entry) {
	if entry.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			n.logger.Error("failed to unmarshal conf change", zap.Error(err))
			return
		}
		n.node.ApplyConfChange(cc)
		if err := n.storage.SaveConfState(*n.node.ApplyConfChange(cc)); err != nil {
			n.logger.Error("failed to save conf state", zap.Error(err))
		}
		return
	}

	// Normal entry - apply to state machine
	if len(entry.Data) == 0 {
		// Empty entry (e.g., no-op after election)
		n.appliedIndex.Store(entry.Index)
		n.sm.SetAppliedIndex(entry.Index)
		return
	}

	result, err := n.sm.Apply(ctx, entry.Index, entry.Data)
	if err != nil {
		n.logger.Error("failed to apply entry", zap.Uint64("index", entry.Index), zap.Error(err))
	}

	n.appliedIndex.Store(entry.Index)
	n.commitIndex.Store(entry.Index)

	// Extract ClientId:Seq from entry data to notify waiting proposer(s)
	var proposal pb.Proposal
	if err := proto.Unmarshal(entry.Data, &proposal); err == nil {
		proposalKey := proposal.ClientId + ":" + fmt.Sprintf("%d", proposal.Seq)
		n.pendingMu.Lock()
		if prop, ok := n.pending[proposalKey]; ok {
			for _, waiter := range prop.Waiters {
				select {
				case waiter <- result:
				default:
				}
			}
			delete(n.pending, proposalKey)
		}
		n.pendingMu.Unlock()
	}
}

// handleReadState handles a ReadIndex response.
func (n *Node) handleReadState(rs raft.ReadState) {
	n.readIndexMu.Lock()
	defer n.readIndexMu.Unlock()

	reqID := string(rs.RequestCtx)
	if ch, ok := n.readIndexReq[reqID]; ok {
		select {
		case ch <- rs.Index:
		default:
		}
		delete(n.readIndexReq, reqID)
	}
}

// updateMetrics updates observability metrics.
func (n *Node) updateMetrics() {
	observability.UpdateRaftState(
		n.isLeader.Load(),
		n.term.Load(),
		n.commitIndex.Load(),
		n.appliedIndex.Load(),
	)
}

// recordPeerAck records a heartbeat or append response from a peer.
func (n *Node) recordPeerAck(msg raftpb.Message) {
	if msg.Type != raftpb.MsgHeartbeatResp && msg.Type != raftpb.MsgAppResp {
		return
	}
	if msg.From == 0 {
		return
	}
	n.quorumMu.Lock()
	defer n.quorumMu.Unlock()
	n.peerAck[msg.From] = time.Now()
	n.updateQuorumLocked()
}

func (n *Node) resetQuorum() {
	n.quorumMu.Lock()
	defer n.quorumMu.Unlock()
	n.peerAck = make(map[uint64]time.Time, len(n.cfg.Peers))
	n.lastQuorumTime = time.Now()
}

func (n *Node) updateQuorumLocked() bool {
	now := time.Now()
	count := 1
	for id, ts := range n.peerAck {
		if id == n.raftID {
			continue
		}
		if now.Sub(ts) <= n.cfg.QuorumTimeout {
			count++
		}
	}
	if count >= n.quorumSize {
		n.lastQuorumTime = now
		return true
	}
	return false
}

// Propose proposes a new entry to the Raft cluster.
func (n *Node) Propose(ctx context.Context, proposal *pb.Proposal) (*sm.ApplyResult, error) {
	ctx, span := observability.StartSpan(ctx, "raft.propose")
	defer span.End()

	// Check if we're the leader
	if !n.isLeader.Load() {
		return &sm.ApplyResult{Code: pb.Code_NOT_LEADER}, nil
	}

	// Check quorum
	if !n.HasQuorum() {
		return &sm.ApplyResult{Code: pb.Code_NO_QUORUM, Message: "quorum lost"}, nil
	}

	// Marshal the proposal
	data, err := proto.Marshal(proposal)
	if err != nil {
		return &sm.ApplyResult{Code: pb.Code_INTERNAL, Message: err.Error()}, nil
	}

	// Create pending entry and register BEFORE submitting to avoid race
	resultCh := make(chan *sm.ApplyResult, 1)
	proposalKey := proposal.ClientId + ":" + fmt.Sprintf("%d", proposal.Seq)
	created := false
	var waiterID uint64

	n.pendingMu.Lock()
	if prop, ok := n.pending[proposalKey]; ok {
		waiterID = prop.nextID
		prop.nextID++
		prop.Waiters[waiterID] = resultCh
	} else {
		prop := &PendingProposal{
			Key:     proposalKey,
			Term:    n.term.Load(),
			Waiters: make(map[uint64]chan *sm.ApplyResult),
			nextID:  1,
		}
		waiterID = prop.nextID
		prop.nextID++
		prop.Waiters[waiterID] = resultCh
		n.pending[proposalKey] = prop
		created = true
	}
	n.pendingMu.Unlock()

	if created {
		// Submit to propose channel
		select {
		case n.proposeC <- data:
		case <-ctx.Done():
			n.removePendingWaiter(proposalKey, waiterID)
			return &sm.ApplyResult{Code: pb.Code_INTERNAL, Message: "context canceled"}, nil
		}
	}

	// Wait for commit and apply
	ctx2, span2 := observability.StartSpan(ctx, "raft.wait_commit")
	defer span2.End()

	select {
	case result := <-resultCh:
		return result, nil
	case <-ctx2.Done():
		n.removePendingWaiter(proposalKey, waiterID)
		return &sm.ApplyResult{Code: pb.Code_INTERNAL, Message: "timeout waiting for commit"}, nil
	case <-time.After(10 * time.Second):
		n.removePendingWaiter(proposalKey, waiterID)
		return &sm.ApplyResult{Code: pb.Code_INTERNAL, Message: "timeout waiting for commit"}, nil
	}
}

func (n *Node) removePendingWaiter(proposalKey string, waiterID uint64) {
	n.pendingMu.Lock()
	defer n.pendingMu.Unlock()

	prop, ok := n.pending[proposalKey]
	if !ok {
		return
	}
	delete(prop.Waiters, waiterID)
	if len(prop.Waiters) == 0 {
		delete(n.pending, proposalKey)
	}
}

// ReadIndex initiates a linearizable read using ReadIndex.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	ctx, span := observability.StartSpan(ctx, "raft.read_index")
	defer span.End()

	if !n.isLeader.Load() {
		return 0, &NotLeaderError{}
	}

	// Generate unique request ID
	reqID := time.Now().String()
	resultCh := make(chan uint64, 1)

	n.readIndexMu.Lock()
	n.readIndexReq[reqID] = resultCh
	n.readIndexMu.Unlock()

	// Request ReadIndex
	if err := n.node.ReadIndex(ctx, []byte(reqID)); err != nil {
		n.readIndexMu.Lock()
		delete(n.readIndexReq, reqID)
		n.readIndexMu.Unlock()
		return 0, err
	}

	// Wait for response
	select {
	case readIndex := <-resultCh:
		// Wait until applied index catches up
		for n.appliedIndex.Load() < readIndex {
			time.Sleep(time.Millisecond)
		}
		return readIndex, nil
	case <-ctx.Done():
		n.readIndexMu.Lock()
		delete(n.readIndexReq, reqID)
		n.readIndexMu.Unlock()
		return 0, ctx.Err()
	case <-time.After(5 * time.Second):
		n.readIndexMu.Lock()
		delete(n.readIndexReq, reqID)
		n.readIndexMu.Unlock()
		return 0, &TimeoutError{}
	}
}

// HasQuorum returns true if the leader has recently confirmed quorum.
func (n *Node) HasQuorum() bool {
	n.quorumMu.Lock()
	defer n.quorumMu.Unlock()
	return n.updateQuorumLocked()
}

// IsLeader returns true if this node is the leader.
func (n *Node) IsLeader() bool {
	return n.isLeader.Load()
}

// LeaderID returns the current leader's Raft ID.
func (n *Node) LeaderID() uint64 {
	return n.leaderID.Load()
}

// LeaderNodeID returns the current leader's node ID string.
func (n *Node) LeaderNodeID() string {
	lid := n.leaderID.Load()
	if lid == 0 {
		return ""
	}
	return config.RaftIDToNodeID(lid)
}

// Term returns the current term.
func (n *Node) Term() uint64 {
	return n.term.Load()
}

// CommitIndex returns the current commit index.
func (n *Node) CommitIndex() uint64 {
	return n.commitIndex.Load()
}

// AppliedIndex returns the current applied index.
func (n *Node) AppliedIndex() uint64 {
	return n.appliedIndex.Load()
}

// MillisSinceQuorum returns milliseconds since last quorum confirmation.
func (n *Node) MillisSinceQuorum() uint64 {
	n.quorumMu.Lock()
	defer n.quorumMu.Unlock()
	return uint64(time.Since(n.lastQuorumTime).Milliseconds())
}

// Stop stops the Raft node.
func (n *Node) Stop() {
	close(n.stopC)
	<-n.doneC
}

// NotLeaderError indicates the node is not the leader.
type NotLeaderError struct{}

func (e *NotLeaderError) Error() string { return "not leader" }

// TimeoutError indicates a timeout occurred.
type TimeoutError struct{}

func (e *TimeoutError) Error() string { return "timeout" }
