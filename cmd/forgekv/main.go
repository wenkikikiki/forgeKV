// Package main provides the ForgeKV node binary.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/forgekv/forgekv/internal/config"
	"github.com/forgekv/forgekv/internal/observability"
	"github.com/forgekv/forgekv/internal/server"
	"github.com/spf13/cobra"
)

var (
	nodeID              string
	dataDir             string
	clientAddr          string
	raftAddr            string
	adminAddr           string
	metricsAddr         string
	peersStr            string
	peerClientAddrsStr  string
	jaegerAddr          string
	disableBackpressure bool
	l0SoftFiles         int
	l0HardFiles         int
	l0SoftBytes         int64
	l0HardBytes         int64
	pebbleMemTableSize  int
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "forgekv",
		Short: "ForgeKV - A replicated key-value store",
		Long: `ForgeKV is a 3-node replicated key-value store using etcd/raft for
consensus and Pebble for storage, providing linearizable reads and writes.`,
	}

	nodeCmd := &cobra.Command{
		Use:   "node",
		Short: "Start a ForgeKV node",
		RunE:  runNode,
	}

	nodeCmd.Flags().StringVar(&nodeID, "id", "n1", "Node ID (n1, n2, or n3)")
	nodeCmd.Flags().StringVar(&dataDir, "data-dir", "/tmp/forgekv/n1", "Data directory")
	nodeCmd.Flags().StringVar(&clientAddr, "client-addr", "127.0.0.1:9001", "Client API address")
	nodeCmd.Flags().StringVar(&raftAddr, "raft-addr", "127.0.0.1:9101", "Raft transport address")
	nodeCmd.Flags().StringVar(&adminAddr, "admin-addr", "127.0.0.1:9201", "Admin API address")
	nodeCmd.Flags().StringVar(&metricsAddr, "metrics-addr", "127.0.0.1:9301", "Metrics endpoint address")
	nodeCmd.Flags().StringVar(&peersStr, "peers", "", "Peer addresses (n1=host:port,n2=host:port,...)")
	nodeCmd.Flags().StringVar(&peerClientAddrsStr, "peer-client-addrs", "", "Peer client addresses (n1=host:port,n2=host:port,...)")
	nodeCmd.Flags().StringVar(&jaegerAddr, "jaeger-addr", "localhost:4317", "Jaeger OTLP endpoint")
	nodeCmd.Flags().BoolVar(&disableBackpressure, "disable-backpressure", false, "Disable backpressure admission control")
	nodeCmd.Flags().IntVar(&l0SoftFiles, "l0-soft-files", 0, "Override L0 soft file threshold (0 = use default)")
	nodeCmd.Flags().IntVar(&l0HardFiles, "l0-hard-files", 0, "Override L0 hard file threshold (0 = use default)")
	nodeCmd.Flags().Int64Var(&l0SoftBytes, "l0-soft-bytes", 0, "Override L0 soft bytes threshold (0 = use default)")
	nodeCmd.Flags().Int64Var(&l0HardBytes, "l0-hard-bytes", 0, "Override L0 hard bytes threshold (0 = use default)")
	nodeCmd.Flags().IntVar(&pebbleMemTableSize, "pebble-memtable-size", 0, "Override Pebble MemTableSize in bytes (0 = use default)")

	rootCmd.AddCommand(nodeCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runNode(cmd *cobra.Command, args []string) error {
	// Parse peer configuration
	peers, err := config.ParsePeers(peersStr)
	if err != nil {
		return fmt.Errorf("invalid peers: %w", err)
	}
	peerClientAddrs, err := config.ParsePeerClientAddrs(peerClientAddrsStr)
	if err != nil {
		return fmt.Errorf("invalid peer client addrs: %w", err)
	}

	// Build configuration
	cfg := config.DefaultConfig()
	cfg.NodeID = nodeID
	cfg.DataDir = dataDir
	cfg.ClientAddr = clientAddr
	cfg.RaftAddr = raftAddr
	cfg.AdminAddr = adminAddr
	cfg.MetricsAddr = metricsAddr
	cfg.Peers = peers
	cfg.PeerClientAddrs = peerClientAddrs
	cfg.JaegerEndpoint = jaegerAddr
	cfg.DisableBackpressure = disableBackpressure
	if l0SoftFiles > 0 {
		cfg.L0SoftFiles = l0SoftFiles
	}
	if l0HardFiles > 0 {
		cfg.L0HardFiles = l0HardFiles
	}
	if l0SoftBytes > 0 {
		cfg.L0SoftBytes = l0SoftBytes
	}
	if l0HardBytes > 0 {
		cfg.L0HardBytes = l0HardBytes
	}
	if pebbleMemTableSize > 0 {
		cfg.PebbleMemTableSize = pebbleMemTableSize
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	// Initialize tracing
	ctx := context.Background()
	if cfg.EnableTracing {
		if err := observability.InitTracing(ctx, cfg.NodeID, cfg.JaegerEndpoint); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to initialize tracing: %v\n", err)
		}
	}

	// Create and start server
	srv, err := server.NewServer(cfg)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	// Handle shutdown signals
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigC
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Start server
	fmt.Printf("ForgeKV node %s starting...\n", cfg.NodeID)
	fmt.Printf("  Client API: %s\n", cfg.ClientAddr)
	fmt.Printf("  Raft:       %s\n", cfg.RaftAddr)
	fmt.Printf("  Admin:      %s\n", cfg.AdminAddr)
	fmt.Printf("  Metrics:    %s\n", cfg.MetricsAddr)

	if err := srv.Start(ctx); err != nil && err != context.Canceled {
		return fmt.Errorf("server error: %w", err)
	}

	// Shutdown tracing
	if err := observability.ShutdownTracing(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to shutdown tracing: %v\n", err)
	}

	return nil
}
