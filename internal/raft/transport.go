package raft

import (
	"context"
	"io"
	"math/rand"
	"sync"
	"time"

	pb "github.com/forgekv/forgekv/api/forgekv"
	"github.com/forgekv/forgekv/internal/config"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// LinkRule defines network fault injection rules between nodes.
type LinkRule struct {
	DropPct  uint32
	DelayMs  uint32
	JitterMs uint32
}

// Transport handles Raft message transport between nodes.
type Transport struct {
	mu sync.RWMutex

	nodeID string
	peers  map[string]string // nodeID -> raft address
	conns  map[string]*grpc.ClientConn
	logger *zap.Logger

	// Link rules for fault injection
	linkRules map[string]*LinkRule // key: "srcNode:dstNode"

	// Message channel for incoming messages
	recvC chan raftpb.Message
}

// NewTransport creates a new Raft transport.
func NewTransport(nodeID string, peers map[string]string, logger *zap.Logger) *Transport {
	return &Transport{
		nodeID:    nodeID,
		peers:     peers,
		conns:     make(map[string]*grpc.ClientConn),
		linkRules: make(map[string]*LinkRule),
		logger:    logger.Named("transport"),
		recvC:     make(chan raftpb.Message, 1024),
	}
}

// Send sends a Raft message to the specified node.
func (t *Transport) Send(ctx context.Context, msgs []raftpb.Message) {
	for _, msg := range msgs {
		go t.sendOne(ctx, msg)
	}
}

func (t *Transport) sendOne(ctx context.Context, msg raftpb.Message) {
	dstNodeID := config.RaftIDToNodeID(msg.To)

	// Check link rules
	t.mu.RLock()
	rule := t.linkRules[t.nodeID+":"+dstNodeID]
	t.mu.RUnlock()

	if rule != nil {
		// Apply drop
		if rule.DropPct > 0 && rand.Uint32()%100 < rule.DropPct {
			t.logger.Debug("dropping message", zap.String("to", dstNodeID), zap.String("type", msg.Type.String()))
			return
		}

		// Apply delay
		if rule.DelayMs > 0 || rule.JitterMs > 0 {
			delay := time.Duration(rule.DelayMs) * time.Millisecond
			if rule.JitterMs > 0 {
				jitter := time.Duration(rand.Uint32()%rule.JitterMs) * time.Millisecond
				if rand.Intn(2) == 0 {
					delay += jitter
				} else if delay > jitter {
					delay -= jitter
				}
			}
			time.Sleep(delay)
		}
	}

	// Get or create connection
	conn, err := t.getConn(dstNodeID)
	if err != nil {
		t.logger.Warn("failed to get connection", zap.String("to", dstNodeID), zap.Error(err))
		return
	}

	// Serialize message
	data, err := msg.Marshal()
	if err != nil {
		t.logger.Error("failed to marshal message", zap.Error(err))
		return
	}

	// Send via gRPC
	client := pb.NewRaftTransportClient(conn)
	_, err = client.SendMessage(ctx, &pb.RaftMessageRequest{Message: data})
	if err != nil {
		t.logger.Debug("failed to send message", zap.String("to", dstNodeID), zap.Error(err))
	}
}

// getConn returns a connection to the specified node, creating one if needed.
func (t *Transport) getConn(nodeID string) (*grpc.ClientConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if conn, ok := t.conns[nodeID]; ok {
		return conn, nil
	}

	addr, ok := t.peers[nodeID]
	if !ok {
		return nil, &NodeNotFoundError{NodeID: nodeID}
	}

	conn, err := grpc.Dial(addr, //nolint:staticcheck // grpc.Dial is deprecated but still supported
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),             //nolint:staticcheck // WithBlock is deprecated but needed here
		grpc.WithTimeout(5*time.Second), //nolint:staticcheck // WithTimeout is deprecated but needed here
	)
	if err != nil {
		return nil, err
	}

	t.conns[nodeID] = conn
	return conn, nil
}

// NodeNotFoundError indicates a node was not found.
type NodeNotFoundError struct {
	NodeID string
}

func (e *NodeNotFoundError) Error() string {
	return "node not found: " + e.NodeID
}

// Receive returns the channel for incoming Raft messages.
func (t *Transport) Receive() <-chan raftpb.Message {
	return t.recvC
}

// HandleMessage processes an incoming Raft message.
func (t *Transport) HandleMessage(msg raftpb.Message) {
	select {
	case t.recvC <- msg:
	default:
		t.logger.Warn("receive channel full, dropping message")
	}
}

// SetLinkRule sets a link rule for fault injection.
func (t *Transport) SetLinkRule(srcNodeID, dstNodeID string, rule *LinkRule) {
	t.mu.Lock()
	defer t.mu.Unlock()

	key := srcNodeID + ":" + dstNodeID
	if rule == nil || (rule.DropPct == 0 && rule.DelayMs == 0 && rule.JitterMs == 0) {
		delete(t.linkRules, key)
	} else {
		t.linkRules[key] = rule
	}

	t.logger.Info("link rule updated",
		zap.String("src", srcNodeID),
		zap.String("dst", dstNodeID),
		zap.Any("rule", rule),
	)
}

// ClearLinkRules removes all link rules.
func (t *Transport) ClearLinkRules() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.linkRules = make(map[string]*LinkRule)
	t.logger.Info("all link rules cleared")
}

// SendSnapshot sends a snapshot to the specified node.
func (t *Transport) SendSnapshot(ctx context.Context, to uint64, snap raftpb.Snapshot, data io.Reader) error {
	dstNodeID := config.RaftIDToNodeID(to)

	conn, err := t.getConn(dstNodeID)
	if err != nil {
		return err
	}

	client := pb.NewRaftTransportClient(conn)
	stream, err := client.SendSnapshot(ctx)
	if err != nil {
		return err
	}

	// Send snapshot in chunks
	buf := make([]byte, 64*1024) // 64KB chunks
	first := true

	for {
		n, err := data.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		chunk := &pb.SnapshotChunk{
			Data: buf[:n],
			Done: false,
		}
		if first {
			chunk.Index = snap.Metadata.Index
			chunk.Term = snap.Metadata.Term
			first = false
		}

		if err := stream.Send(chunk); err != nil {
			return err
		}
	}

	// Send final marker
	if err := stream.Send(&pb.SnapshotChunk{Done: true}); err != nil {
		return err
	}

	_, err = stream.CloseAndRecv()
	return err
}

// Close closes all connections.
func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, conn := range t.conns {
		_ = conn.Close()
	}
	t.conns = make(map[string]*grpc.ClientConn)
	close(t.recvC)
	return nil
}
