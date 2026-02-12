// Package server implements the gRPC server for ForgeKV.
package server

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"

	pb "github.com/forgekv/forgekv/api/forgekv"
	"github.com/forgekv/forgekv/internal/config"
	"github.com/forgekv/forgekv/internal/observability"
	raftpkg "github.com/forgekv/forgekv/internal/raft"
	"github.com/forgekv/forgekv/internal/sm"
	"github.com/forgekv/forgekv/internal/storage"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Server is the main ForgeKV server.
type Server struct {
	pb.UnimplementedKVServer
	pb.UnimplementedAdminServer
	pb.UnimplementedRaftTransportServer

	cfg       *config.Config
	logger    *zap.Logger
	storage   *storage.Storage
	sm        *sm.StateMachine
	raftNode  *raftpkg.Node
	transport *raftpkg.Transport
	bp        *storage.BackpressureController

	clientServer  *grpc.Server
	raftServer    *grpc.Server
	adminServer   *grpc.Server
	metricsServer *http.Server
}

// NewServer creates a new ForgeKV server.
func NewServer(cfg *config.Config) (*Server, error) {
	logger := observability.InitLogger(cfg.NodeID, false)

	// Initialize metrics before starting any storage sampling goroutines.
	observability.InitMetrics(cfg.NodeID)

	// Initialize storage
	store, err := storage.NewStorage(cfg.DataDir+"/data", logger, cfg.PebbleMemTableSize)
	if err != nil {
		return nil, err
	}

	// Initialize state machine
	stateMachine := sm.NewStateMachine(store, cfg, logger)

	// Load existing sessions
	if err := stateMachine.LoadSessions(); err != nil {
		logger.Warn("failed to load sessions", zap.Error(err))
	}

	// Initialize Raft storage
	raftStorage, err := raftpkg.NewPebbleStorage(cfg.DataDir + "/raft")
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	// Initialize transport
	transport := raftpkg.NewTransport(cfg.NodeID, cfg.Peers, logger)

	// Initialize Raft node
	raftNode, err := raftpkg.NewNode(cfg, raftStorage, stateMachine, transport, logger)
	if err != nil {
		_ = store.Close()
		_ = raftStorage.Close()
		return nil, err
	}

	// Initialize backpressure controller
	bp := storage.NewBackpressureController(store, cfg)

	s := &Server{
		cfg:       cfg,
		logger:    logger,
		storage:   store,
		sm:        stateMachine,
		raftNode:  raftNode,
		transport: transport,
		bp:        bp,
	}

	return s, nil
}

// Start starts the server.
func (s *Server) Start(ctx context.Context) error {
	// Start Raft loop
	go s.raftNode.Run(ctx)

	// Start gRPC servers
	errC := make(chan error, 4)

	clientLis, err := net.Listen("tcp", s.cfg.ClientAddr)
	if err != nil {
		return err
	}
	s.clientServer = grpc.NewServer()
	pb.RegisterKVServer(s.clientServer, s)
	s.logger.Info("client server listening", zap.String("addr", s.cfg.ClientAddr))
	go func() { errC <- s.clientServer.Serve(clientLis) }()

	raftLis, err := net.Listen("tcp", s.cfg.RaftAddr)
	if err != nil {
		_ = clientLis.Close()
		return err
	}
	s.raftServer = grpc.NewServer()
	pb.RegisterRaftTransportServer(s.raftServer, s)
	s.logger.Info("raft server listening", zap.String("addr", s.cfg.RaftAddr))
	go func() { errC <- s.raftServer.Serve(raftLis) }()

	adminLis, err := net.Listen("tcp", s.cfg.AdminAddr)
	if err != nil {
		_ = clientLis.Close()
		_ = raftLis.Close()
		return err
	}
	s.adminServer = grpc.NewServer()
	pb.RegisterAdminServer(s.adminServer, s)
	s.logger.Info("admin server listening", zap.String("addr", s.cfg.AdminAddr))
	go func() { errC <- s.adminServer.Serve(adminLis) }()

	metricsLis, err := net.Listen("tcp", s.cfg.MetricsAddr)
	if err != nil {
		_ = clientLis.Close()
		_ = raftLis.Close()
		_ = adminLis.Close()
		return err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", observability.MetricsHandler())
	s.metricsServer = &http.Server{Handler: mux}
	s.logger.Info("metrics server listening", zap.String("addr", s.cfg.MetricsAddr))
	go func() { errC <- s.metricsServer.Serve(metricsLis) }()

	select {
	case err := <-errC:
		return err
	case <-ctx.Done():
		return s.Stop()
	}
}

// Stop stops the server.
func (s *Server) Stop() error {
	s.logger.Info("stopping server")

	if s.clientServer != nil {
		s.clientServer.GracefulStop()
	}
	if s.raftServer != nil {
		s.raftServer.GracefulStop()
	}
	if s.adminServer != nil {
		s.adminServer.GracefulStop()
	}
	if s.metricsServer != nil {
		_ = s.metricsServer.Close()
	}

	s.raftNode.Stop()
	_ = s.transport.Close()
	_ = s.sm.Close()

	observability.Sync()
	return nil
}

// makeError creates an Error response.
func (s *Server) makeError(code pb.Code, message string) *pb.Error {
	err := &pb.Error{
		Code:    code,
		Message: message,
	}
	if code == pb.Code_NOT_LEADER {
		leaderID := s.raftNode.LeaderNodeID()
		if leaderID != "" {
			if addr, ok := s.cfg.PeerClientAddrs[leaderID]; ok {
				err.LeaderHint = &pb.LeaderHint{
					NodeId:     leaderID,
					ClientAddr: addr,
				}
			}
		}
	}
	return err
}

func (s *Server) validateWriteRequest(clientID string, seq uint64) *pb.Error {
	if clientID == "" {
		return s.makeError(pb.Code_INVALID_ARGUMENT, "client_id is required")
	}
	if seq == 0 {
		return s.makeError(pb.Code_INVALID_ARGUMENT, "seq is required")
	}
	return nil
}

// KV Service Implementation

// Put implements the Put RPC.
func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	start := time.Now()
	observability.IncInflight("Put")
	defer observability.DecInflight("Put")

	ctx, span := observability.StartSpan(ctx, "rpc.decode")
	defer span.End()

	logger := observability.WithClientOp(s.logger, req.ClientId, req.Seq, "Put", len(req.Key))

	if err := s.validateWriteRequest(req.ClientId, req.Seq); err != nil {
		logger.Warn("invalid request", zap.String("reason", err.Message))
		observability.RecordRPCRequest("Put", err.Code.String(), float64(time.Since(start).Milliseconds()))
		return &pb.PutResponse{Error: err}, nil
	}

	// Check leader
	if !s.raftNode.IsLeader() {
		logger.Debug("not leader")
		observability.RecordRPCRequest("Put", "NOT_LEADER", float64(time.Since(start).Milliseconds()))
		return &pb.PutResponse{Error: s.makeError(pb.Code_NOT_LEADER, "not leader")}, nil
	}

	// Check quorum
	if !s.raftNode.HasQuorum() {
		logger.Warn("no quorum")
		observability.RecordRPCRequest("Put", "NO_QUORUM", float64(time.Since(start).Milliseconds()))
		return &pb.PutResponse{Error: s.makeError(pb.Code_NO_QUORUM, "no quorum")}, nil
	}

	// Apply backpressure
	if !s.bp.ApplyBackpressure() {
		logger.Warn("overloaded")
		observability.RecordRPCRequest("Put", "OVERLOADED", float64(time.Since(start).Milliseconds()))
		return &pb.PutResponse{Error: s.makeError(pb.Code_OVERLOADED, "system overloaded")}, nil
	}

	// Create proposal
	proposal := &pb.Proposal{
		Type:     pb.Proposal_PUT,
		Key:      req.Key,
		Value:    req.Value,
		ClientId: req.ClientId,
		Seq:      req.Seq,
	}

	// Propose
	result, err := s.raftNode.Propose(ctx, proposal)
	if err != nil {
		logger.Error("propose failed", zap.Error(err))
		observability.RecordRPCRequest("Put", "INTERNAL", float64(time.Since(start).Milliseconds()))
		return &pb.PutResponse{Error: s.makeError(pb.Code_INTERNAL, err.Error())}, nil
	}

	code := result.Code.String()
	observability.RecordRPCRequest("Put", code, float64(time.Since(start).Milliseconds()))

	return &pb.PutResponse{Error: &pb.Error{Code: result.Code, Message: result.Message}}, nil
}

// Get implements the Get RPC.
func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	start := time.Now()
	observability.IncInflight("Get")
	defer observability.DecInflight("Get")

	ctx, span := observability.StartSpan(ctx, "rpc.decode")
	defer span.End()

	// Check leader
	if !s.raftNode.IsLeader() {
		observability.RecordRPCRequest("Get", "NOT_LEADER", float64(time.Since(start).Milliseconds()))
		return &pb.GetResponse{Error: s.makeError(pb.Code_NOT_LEADER, "not leader")}, nil
	}

	// ReadIndex for linearizability
	_, err := s.raftNode.ReadIndex(ctx)
	if err != nil {
		observability.RecordRPCRequest("Get", "INTERNAL", float64(time.Since(start).Milliseconds()))
		return &pb.GetResponse{Error: s.makeError(pb.Code_INTERNAL, err.Error())}, nil
	}

	// Read from state machine
	value, found, err := s.sm.Get(ctx, req.Key)
	if err != nil {
		observability.RecordRPCRequest("Get", "INTERNAL", float64(time.Since(start).Milliseconds()))
		return &pb.GetResponse{Error: s.makeError(pb.Code_INTERNAL, err.Error())}, nil
	}

	observability.RecordRPCRequest("Get", "OK", float64(time.Since(start).Milliseconds()))
	return &pb.GetResponse{
		Error: &pb.Error{Code: pb.Code_OK},
		Value: value,
		Found: found,
	}, nil
}

// Delete implements the Delete RPC.
func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	start := time.Now()
	observability.IncInflight("Delete")
	defer observability.DecInflight("Delete")

	ctx, span := observability.StartSpan(ctx, "rpc.decode")
	defer span.End()

	if err := s.validateWriteRequest(req.ClientId, req.Seq); err != nil {
		observability.RecordRPCRequest("Delete", err.Code.String(), float64(time.Since(start).Milliseconds()))
		return &pb.DeleteResponse{Error: err}, nil
	}

	// Check leader
	if !s.raftNode.IsLeader() {
		observability.RecordRPCRequest("Delete", "NOT_LEADER", float64(time.Since(start).Milliseconds()))
		return &pb.DeleteResponse{Error: s.makeError(pb.Code_NOT_LEADER, "not leader")}, nil
	}

	// Check quorum
	if !s.raftNode.HasQuorum() {
		observability.RecordRPCRequest("Delete", "NO_QUORUM", float64(time.Since(start).Milliseconds()))
		return &pb.DeleteResponse{Error: s.makeError(pb.Code_NO_QUORUM, "no quorum")}, nil
	}

	// Apply backpressure
	if !s.bp.ApplyBackpressure() {
		observability.RecordRPCRequest("Delete", "OVERLOADED", float64(time.Since(start).Milliseconds()))
		return &pb.DeleteResponse{Error: s.makeError(pb.Code_OVERLOADED, "system overloaded")}, nil
	}

	// Create proposal
	proposal := &pb.Proposal{
		Type:     pb.Proposal_DELETE,
		Key:      req.Key,
		ClientId: req.ClientId,
		Seq:      req.Seq,
	}

	// Propose
	result, err := s.raftNode.Propose(ctx, proposal)
	if err != nil {
		observability.RecordRPCRequest("Delete", "INTERNAL", float64(time.Since(start).Milliseconds()))
		return &pb.DeleteResponse{Error: s.makeError(pb.Code_INTERNAL, err.Error())}, nil
	}

	code := result.Code.String()
	observability.RecordRPCRequest("Delete", code, float64(time.Since(start).Milliseconds()))

	return &pb.DeleteResponse{Error: &pb.Error{Code: result.Code, Message: result.Message}}, nil
}

// CAS implements the Compare-And-Swap RPC.
func (s *Server) CAS(ctx context.Context, req *pb.CASRequest) (*pb.CASResponse, error) {
	start := time.Now()
	observability.IncInflight("CAS")
	defer observability.DecInflight("CAS")

	ctx, span := observability.StartSpan(ctx, "rpc.decode")
	defer span.End()

	if err := s.validateWriteRequest(req.ClientId, req.Seq); err != nil {
		observability.RecordRPCRequest("CAS", err.Code.String(), float64(time.Since(start).Milliseconds()))
		return &pb.CASResponse{Error: err}, nil
	}

	// Check leader
	if !s.raftNode.IsLeader() {
		observability.RecordRPCRequest("CAS", "NOT_LEADER", float64(time.Since(start).Milliseconds()))
		return &pb.CASResponse{Error: s.makeError(pb.Code_NOT_LEADER, "not leader")}, nil
	}

	// Check quorum
	if !s.raftNode.HasQuorum() {
		observability.RecordRPCRequest("CAS", "NO_QUORUM", float64(time.Since(start).Milliseconds()))
		return &pb.CASResponse{Error: s.makeError(pb.Code_NO_QUORUM, "no quorum")}, nil
	}

	// Apply backpressure
	if !s.bp.ApplyBackpressure() {
		observability.RecordRPCRequest("CAS", "OVERLOADED", float64(time.Since(start).Milliseconds()))
		return &pb.CASResponse{Error: s.makeError(pb.Code_OVERLOADED, "system overloaded")}, nil
	}

	// Create proposal
	proposal := &pb.Proposal{
		Type:     pb.Proposal_CAS,
		Key:      req.Key,
		Expected: req.Expected,
		Desired:  req.Desired,
		ClientId: req.ClientId,
		Seq:      req.Seq,
	}

	// Propose
	result, err := s.raftNode.Propose(ctx, proposal)
	if err != nil {
		observability.RecordRPCRequest("CAS", "INTERNAL", float64(time.Since(start).Milliseconds()))
		return &pb.CASResponse{Error: s.makeError(pb.Code_INTERNAL, err.Error())}, nil
	}

	code := result.Code.String()
	observability.RecordRPCRequest("CAS", code, float64(time.Since(start).Milliseconds()))

	return &pb.CASResponse{
		Error:   &pb.Error{Code: result.Code, Message: result.Message},
		Swapped: result.Swapped,
		Current: result.Current,
		Found:   result.Found,
	}, nil
}

// Status implements the Status RPC.
func (s *Server) Status(ctx context.Context, req *pb.StatusRequest) (*pb.StatusResponse, error) {
	start := time.Now()
	observability.IncInflight("Status")
	defer observability.DecInflight("Status")

	leaderID := s.raftNode.LeaderNodeID()
	var leaderClientAddr string
	if addr, ok := s.cfg.PeerClientAddrs[leaderID]; ok {
		leaderClientAddr = addr
	}

	observability.RecordRPCRequest("Status", "OK", float64(time.Since(start).Milliseconds()))

	return &pb.StatusResponse{
		Error:             &pb.Error{Code: pb.Code_OK},
		NodeId:            s.cfg.NodeID,
		IsLeader:          s.raftNode.IsLeader(),
		LeaderId:          leaderID,
		LeaderClientAddr:  leaderClientAddr,
		Term:              s.raftNode.Term(),
		CommitIndex:       s.raftNode.CommitIndex(),
		AppliedIndex:      s.raftNode.AppliedIndex(),
		MillisSinceQuorum: s.raftNode.MillisSinceQuorum(),
	}, nil
}

// Admin Service Implementation

// SetLinkRule implements the SetLinkRule RPC.
func (s *Server) SetLinkRule(ctx context.Context, req *pb.SetLinkRuleRequest) (*pb.SetLinkRuleResponse, error) {
	s.transport.SetLinkRule(req.SrcNodeId, req.DstNodeId, &raftpkg.LinkRule{
		DropPct:  req.DropPct,
		DelayMs:  req.DelayMs,
		JitterMs: req.JitterMs,
	})
	return &pb.SetLinkRuleResponse{Error: &pb.Error{Code: pb.Code_OK}}, nil
}

// ClearLinkRules implements the ClearLinkRules RPC.
func (s *Server) ClearLinkRules(ctx context.Context, req *pb.ClearLinkRulesRequest) (*pb.ClearLinkRulesResponse, error) {
	s.transport.ClearLinkRules()
	return &pb.ClearLinkRulesResponse{Error: &pb.Error{Code: pb.Code_OK}}, nil
}

// SetDiskStall implements the SetDiskStall RPC.
func (s *Server) SetDiskStall(ctx context.Context, req *pb.SetDiskStallRequest) (*pb.SetDiskStallResponse, error) {
	s.storage.SetFsyncDelay(req.FsyncDelayMs)
	return &pb.SetDiskStallResponse{Error: &pb.Error{Code: pb.Code_OK}}, nil
}

// Crash implements the Crash RPC.
func (s *Server) Crash(ctx context.Context, req *pb.CrashRequest) (*pb.CrashResponse, error) {
	s.logger.Info("crash requested", zap.Uint32("exit_code", req.ExitCode))
	exitCode := int(req.ExitCode)
	if exitCode == 0 {
		exitCode = 137
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		os.Exit(exitCode)
	}()
	return &pb.CrashResponse{Error: &pb.Error{Code: pb.Code_OK}}, nil
}

// Raft Transport Implementation

// SendMessage implements the SendMessage RPC.
func (s *Server) SendMessage(ctx context.Context, req *pb.RaftMessageRequest) (*pb.RaftMessageResponse, error) {
	var msg raftpb.Message
	if err := msg.Unmarshal(req.Message); err != nil {
		return &pb.RaftMessageResponse{Error: &pb.Error{Code: pb.Code_INTERNAL, Message: err.Error()}}, nil
	}
	s.transport.HandleMessage(msg)
	return &pb.RaftMessageResponse{Error: &pb.Error{Code: pb.Code_OK}}, nil
}

// SendSnapshot implements the SendSnapshot RPC.
func (s *Server) SendSnapshot(stream pb.RaftTransport_SendSnapshotServer) error {
	// TODO: Implement snapshot receiving
	return nil
}
