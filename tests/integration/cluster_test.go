//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
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

type testCluster struct {
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	clientAddrs map[string]string
	tempDir     string
}

var cluster *testCluster

func TestMain(m *testing.M) {
	var err error
	cluster, err = startCluster()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start cluster: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	cluster.stop()
	os.Exit(code)
}

func startCluster() (*testCluster, error) {
	nodeIDs := []string{"n1", "n2", "n3"}
	clientAddrs := map[string]string{
		"n1": "127.0.0.1:19001",
		"n2": "127.0.0.1:19002",
		"n3": "127.0.0.1:19003",
	}
	raftAddrs := map[string]string{
		"n1": "127.0.0.1:19101",
		"n2": "127.0.0.1:19102",
		"n3": "127.0.0.1:19103",
	}
	adminAddrs := map[string]string{
		"n1": "127.0.0.1:19201",
		"n2": "127.0.0.1:19202",
		"n3": "127.0.0.1:19203",
	}
	metricsAddrs := map[string]string{
		"n1": "127.0.0.1:19301",
		"n2": "127.0.0.1:19302",
		"n3": "127.0.0.1:19303",
	}

	tempDir, err := os.MkdirTemp("", "forgekv-integration-")
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	tc := &testCluster{
		cancel:      cancel,
		clientAddrs: clientAddrs,
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

func (c *testCluster) stop() {
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
			conn, client, err := dial(addr, 500*time.Millisecond)
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

func findLeader(t *testing.T) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	leaderID, err := waitForLeader(ctx, cluster.clientAddrs, nil)
	if err != nil {
		t.Fatalf("failed to find leader: %v", err)
	}
	return leaderID, cluster.clientAddrs[leaderID]
}

func dial(addr string, timeout time.Duration) (*grpc.ClientConn, pb.KVClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewKVClient(conn), nil
}
