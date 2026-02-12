//go:build chaos
// +build chaos

package chaos

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/forgekv/forgekv/api/forgekv"
	"github.com/forgekv/forgekv/internal/config"
	"github.com/forgekv/forgekv/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type chaosCluster struct {
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	clientAddrs map[string]string
	adminAddrs  map[string]string
	tempDir     string
}

var cluster *chaosCluster

func TestMain(m *testing.M) {
	var err error
	cluster, err = startCluster()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start chaos cluster: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	cluster.stop()
	os.Exit(code)
}

func startCluster() (*chaosCluster, error) {
	nodeIDs := []string{"n1", "n2", "n3"}
	clientAddrs := make(map[string]string, len(nodeIDs))
	raftAddrs := make(map[string]string, len(nodeIDs))
	adminAddrs := make(map[string]string, len(nodeIDs))
	metricsAddrs := make(map[string]string, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		clientAddr, err := freeAddr()
		if err != nil {
			return nil, err
		}
		raftAddr, err := freeAddr()
		if err != nil {
			return nil, err
		}
		adminAddr, err := freeAddr()
		if err != nil {
			return nil, err
		}
		metricsAddr, err := freeAddr()
		if err != nil {
			return nil, err
		}

		clientAddrs[nodeID] = clientAddr
		raftAddrs[nodeID] = raftAddr
		adminAddrs[nodeID] = adminAddr
		metricsAddrs[nodeID] = metricsAddr
	}

	tempDir, err := os.MkdirTemp("", "forgekv-chaos-")
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	tc := &chaosCluster{
		cancel:      cancel,
		clientAddrs: clientAddrs,
		adminAddrs:  adminAddrs,
		tempDir:     tempDir,
	}

	errCh := make(chan error, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		cfg := config.DefaultConfig()
		cfg.NodeID = nodeID
		cfg.DataDir = filepath.Join(tempDir, nodeID)
		cfg.ClientAddr = clientAddrs[nodeID]
		cfg.RaftAddr = raftAddrs[nodeID]
		cfg.AdminAddr = adminAddrs[nodeID]
		cfg.MetricsAddr = metricsAddrs[nodeID]
		cfg.Peers = raftAddrs
		cfg.PeerClientAddrs = clientAddrs
		cfg.EnableTracing = false

		if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
			cancel()
			return nil, err
		}

		if err := cfg.Validate(); err != nil {
			cancel()
			return nil, err
		}

		srv, err := server.NewServer(cfg)
		if err != nil {
			cancel()
			return nil, err
		}

		tc.wg.Add(1)
		go func(s *server.Server) {
			defer tc.wg.Done()
			if err := s.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}(srv)
	}

	leaderCtx, leaderCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer leaderCancel()
	if _, err := waitForLeader(leaderCtx, clientAddrs, errCh); err != nil {
		tc.stop()
		return nil, err
	}

	return tc, nil
}

func (c *chaosCluster) stop() {
	if c == nil {
		return
	}
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
	if c.tempDir != "" {
		_ = os.RemoveAll(c.tempDir)
	}
}

func freeAddr() (string, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := lis.Addr().String()
	if err := lis.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

func waitForLeader(ctx context.Context, addrs map[string]string, errCh <-chan error) (string, error) {
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case err := <-errCh:
			return "", err
		default:
		}

		for nodeID, addr := range addrs {
			conn, client, err := dialKV(addr, 500*time.Millisecond)
			if err != nil {
				continue
			}
			resp, err := client.Status(ctx, &pb.StatusRequest{})
			_ = conn.Close()
			if err != nil {
				continue
			}
			if resp.IsLeader {
				if resp.LeaderId != "" && resp.LeaderId != nodeID {
					return resp.LeaderId, nil
				}
				return nodeID, nil
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func dialKV(addr string, timeout time.Duration) (*grpc.ClientConn, pb.KVClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewKVClient(conn), nil
}

func dialAdmin(addr string, timeout time.Duration) (*grpc.ClientConn, pb.AdminClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewAdminClient(conn), nil
}

type clientPool struct {
	mu      sync.Mutex
	addrs   map[string]string
	conns   map[string]*grpc.ClientConn
	clients map[string]pb.KVClient
}

func newClientPool(addrs map[string]string) *clientPool {
	return &clientPool{
		addrs:   addrs,
		conns:   make(map[string]*grpc.ClientConn, len(addrs)),
		clients: make(map[string]pb.KVClient, len(addrs)),
	}
}

func (p *clientPool) client(nodeID string) (pb.KVClient, error) {
	p.mu.Lock()
	if client, ok := p.clients[nodeID]; ok {
		p.mu.Unlock()
		return client, nil
	}
	addr := p.addrs[nodeID]
	p.mu.Unlock()
	if addr == "" {
		return nil, fmt.Errorf("unknown node %q", nodeID)
	}

	conn, client, err := dialKV(addr, 2*time.Second)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if existing, ok := p.clients[nodeID]; ok {
		p.mu.Unlock()
		_ = conn.Close()
		return existing, nil
	}
	p.conns[nodeID] = conn
	p.clients[nodeID] = client
	p.mu.Unlock()
	return client, nil
}

func (p *clientPool) leader(ctx context.Context) (string, pb.KVClient, error) {
	for nodeID := range p.addrs {
		client, err := p.client(nodeID)
		if err != nil {
			continue
		}
		statusCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		resp, err := client.Status(statusCtx, &pb.StatusRequest{})
		cancel()
		if err != nil {
			continue
		}
		if resp.IsLeader {
			return nodeID, client, nil
		}
	}

	for nodeID := range p.addrs {
		client, err := p.client(nodeID)
		if err == nil {
			return nodeID, client, nil
		}
	}

	return "", nil, fmt.Errorf("no available nodes")
}

func (p *clientPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.conns {
		_ = conn.Close()
	}
	p.conns = make(map[string]*grpc.ClientConn)
	p.clients = make(map[string]pb.KVClient)
}
