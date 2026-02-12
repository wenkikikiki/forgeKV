// Package main provides the ForgeKV CLI client.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/forgekv/forgekv/api/forgekv"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addresses  string
	timeout    time.Duration
	outputJSON bool
	debugLogs  bool
)

func main() {
	debugLogs = os.Getenv("FORGEKVCTL_DEBUG") != ""
	rootCmd := &cobra.Command{
		Use:   "forgekvctl",
		Short: "ForgeKV CLI client",
		Long:  "Command-line client for interacting with ForgeKV clusters.",
	}

	rootCmd.PersistentFlags().StringVar(&addresses, "addrs", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "Comma-separated list of node addresses")
	rootCmd.PersistentFlags().DurationVar(&timeout, "timeout", 5*time.Second, "Request timeout")
	rootCmd.PersistentFlags().BoolVar(&outputJSON, "json", false, "Output in JSON format")

	// Status command
	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Get cluster status",
		RunE:  runStatus,
	}
	var anyNode bool
	statusCmd.Flags().BoolVar(&anyNode, "any", false, "Query any available node")
	rootCmd.AddCommand(statusCmd)

	// Put command
	putCmd := &cobra.Command{
		Use:   "put",
		Short: "Put a key-value pair",
		RunE:  runPut,
	}
	var putKey, putValue, putClientID string
	var putSeq uint64
	var putRetry int
	putCmd.Flags().StringVar(&putKey, "key", "", "Key to put")
	putCmd.Flags().StringVar(&putValue, "value", "", "Value to put")
	putCmd.Flags().StringVar(&putClientID, "client", "", "Client ID for idempotency")
	putCmd.Flags().Uint64Var(&putSeq, "seq", 0, "Sequence number for idempotency")
	putCmd.Flags().IntVar(&putRetry, "retry", 0, "Number of retries")
	_ = putCmd.MarkFlagRequired("key")
	_ = putCmd.MarkFlagRequired("value")
	_ = putCmd.MarkFlagRequired("client")
	rootCmd.AddCommand(putCmd)

	// Get command
	getCmd := &cobra.Command{
		Use:   "get",
		Short: "Get a value by key",
		RunE:  runGet,
	}
	var getKey string
	getCmd.Flags().StringVar(&getKey, "key", "", "Key to get")
	_ = getCmd.MarkFlagRequired("key")
	rootCmd.AddCommand(getCmd)

	// Delete command
	deleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a key",
		RunE:  runDelete,
	}
	var deleteKey, deleteClientID string
	var deleteSeq uint64
	deleteCmd.Flags().StringVar(&deleteKey, "key", "", "Key to delete")
	deleteCmd.Flags().StringVar(&deleteClientID, "client", "", "Client ID for idempotency")
	deleteCmd.Flags().Uint64Var(&deleteSeq, "seq", 0, "Sequence number for idempotency")
	_ = deleteCmd.MarkFlagRequired("key")
	_ = deleteCmd.MarkFlagRequired("client")
	rootCmd.AddCommand(deleteCmd)

	// CAS command
	casCmd := &cobra.Command{
		Use:   "cas",
		Short: "Compare-and-swap a value",
		RunE:  runCAS,
	}
	var casKey, casExpected, casDesired, casClientID string
	var casSeq uint64
	casCmd.Flags().StringVar(&casKey, "key", "", "Key for CAS")
	casCmd.Flags().StringVar(&casExpected, "expected", "", "Expected value")
	casCmd.Flags().StringVar(&casDesired, "desired", "", "Desired value")
	casCmd.Flags().StringVar(&casClientID, "client", "", "Client ID for idempotency")
	casCmd.Flags().Uint64Var(&casSeq, "seq", 0, "Sequence number for idempotency")
	_ = casCmd.MarkFlagRequired("key")
	_ = casCmd.MarkFlagRequired("client")
	rootCmd.AddCommand(casCmd)

	// Admin commands
	adminCmd := &cobra.Command{
		Use:   "admin",
		Short: "Admin operations",
	}

	// Partition command
	partitionCmd := &cobra.Command{
		Use:   "partition-leader",
		Short: "Partition the leader from other nodes",
		RunE:  runPartitionLeader,
	}
	var partitionLeader string
	partitionCmd.Flags().StringVar(&partitionLeader, "leader", "", "Leader node ID to partition")
	_ = partitionCmd.MarkFlagRequired("leader")
	adminCmd.AddCommand(partitionCmd)

	// Heal command
	healCmd := &cobra.Command{
		Use:   "heal",
		Short: "Remove all network partitions",
		RunE:  runHeal,
	}
	adminCmd.AddCommand(healCmd)

	// Disk stall command
	diskstallCmd := &cobra.Command{
		Use:   "diskstall",
		Short: "Inject disk stall on a node",
		RunE:  runDiskStall,
	}
	var diskstallNode string
	var diskstallDelay uint32
	diskstallCmd.Flags().StringVar(&diskstallNode, "node", "", "Node ID")
	diskstallCmd.Flags().Uint32Var(&diskstallDelay, "fsync-delay-ms", 0, "Fsync delay in milliseconds")
	_ = diskstallCmd.MarkFlagRequired("node")
	adminCmd.AddCommand(diskstallCmd)

	// Crash command
	crashCmd := &cobra.Command{
		Use:   "crash",
		Short: "Crash a node",
		RunE:  runCrash,
	}
	var crashNode string
	var crashExitCode uint32
	crashCmd.Flags().StringVar(&crashNode, "node", "", "Node ID to crash")
	crashCmd.Flags().Uint32Var(&crashExitCode, "exit-code", 137, "Exit code")
	_ = crashCmd.MarkFlagRequired("node")
	adminCmd.AddCommand(crashCmd)

	rootCmd.AddCommand(adminCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func getAddrs() []string {
	raw := strings.Split(addresses, ",")
	addrs := make([]string, 0, len(raw))
	for _, addr := range raw {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		addrs = append(addrs, addr)
	}
	return addrs
}

func debugf(format string, args ...any) {
	if !debugLogs {
		return
	}
	ts := time.Now().Format(time.RFC3339Nano)
	fmt.Fprintf(os.Stderr, "[debug %s] %s\n", ts, fmt.Sprintf(format, args...))
}

func connectToLeader(ctx context.Context) (*grpc.ClientConn, pb.KVClient, error) {
	conn, client, err := connectAny(ctx)
	if err != nil {
		debugf("connectToLeader: connectAny failed: %v", err)
		return nil, nil, err
	}

	statusCtx, cancelStatus := context.WithTimeout(ctx, timeout)
	status, err := client.Status(statusCtx, &pb.StatusRequest{})
	cancelStatus()
	if err != nil {
		debugf("connectToLeader: status failed on initial conn: %v", err)
		return conn, client, nil
	}

	if status.IsLeader || status.LeaderClientAddr == "" {
		debugf("connectToLeader: returning initial conn (isLeader=%v leaderAddr=%q)", status.IsLeader, status.LeaderClientAddr)
		return conn, client, nil
	}

	_ = conn.Close()
	debugf("connectToLeader: dialing leader hint %q", status.LeaderClientAddr)
	return connectToAddr(ctx, status.LeaderClientAddr)
}

func connectAny(ctx context.Context) (*grpc.ClientConn, pb.KVClient, error) {
	addrs := getAddrs()

	for _, addr := range addrs {
		debugf("connectAny: dialing %s", addr)
		conn, client, err := connectToAddr(ctx, addr)
		if err != nil {
			debugf("connectAny: dial %s failed: %v", addr, err)
			continue
		}
		debugf("connectAny: connected to %s", addr)
		return conn, client, nil
	}

	return nil, nil, fmt.Errorf("could not connect to any node")
}

func connectToAddr(ctx context.Context, addr string) (*grpc.ClientConn, pb.KVClient, error) {
	if err := ctx.Err(); err != nil {
		debugf("connectToAddr: ctx error before dial %s: %v", addr, err)
		return nil, nil, err
	}
	dialCtx, cancelDial := context.WithTimeout(context.Background(), timeout)
	start := time.Now()
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()) //nolint:staticcheck // grpc.DialContext deprecated but still supported
	cancelDial()
	if err != nil {
		debugf("connectToAddr: dial %s failed after %s: %v", addr, time.Since(start), err)
		return nil, nil, err
	}
	debugf("connectToAddr: dial %s ok in %s", addr, time.Since(start))
	return conn, pb.NewKVClient(conn), nil
}

func runStatus(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	anyFlag, _ := cmd.Flags().GetBool("any")

	var conn *grpc.ClientConn
	var client pb.KVClient
	var err error

	if anyFlag {
		conn, client, err = connectAny(ctx)
	} else {
		conn, client, err = connectToLeader(ctx)
	}
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	status, err := client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		return err
	}

	if outputJSON {
		data, _ := json.MarshalIndent(status, "", "  ")
		fmt.Println(string(data))
	} else {
		fmt.Printf("Node ID:      %s\n", status.NodeId)
		fmt.Printf("Is Leader:    %v\n", status.IsLeader)
		fmt.Printf("Leader ID:    %s\n", status.LeaderId)
		fmt.Printf("Term:         %d\n", status.Term)
		fmt.Printf("Commit Index: %d\n", status.CommitIndex)
		fmt.Printf("Applied Index: %d\n", status.AppliedIndex)
		fmt.Printf("Millis Since Quorum: %d\n", status.MillisSinceQuorum)
	}

	return nil
}

func runPut(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	key, _ := cmd.Flags().GetString("key")
	value, _ := cmd.Flags().GetString("value")
	clientID, _ := cmd.Flags().GetString("client")
	seq, _ := cmd.Flags().GetUint64("seq")
	retryCount, _ := cmd.Flags().GetInt("retry")

	if clientID == "" {
		clientID = fmt.Sprintf("cli-%d", time.Now().UnixNano())
	}
	if seq == 0 {
		seq = uint64(time.Now().UnixNano())
	}

	var lastErr error
	for attempt := 0; attempt <= retryCount; attempt++ {
		conn, client, err := connectToLeader(ctx)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := client.Put(ctx, &pb.PutRequest{
			Key:      []byte(key),
			Value:    []byte(value),
			ClientId: clientID,
			Seq:      seq,
		})
		_ = conn.Close()

		if err != nil {
			lastErr = err
			continue
		}

		if resp.Error != nil && resp.Error.Code != pb.Code_OK {
			if outputJSON {
				data, _ := json.MarshalIndent(resp, "", "  ")
				fmt.Println(string(data))
			} else {
				fmt.Printf("Error: %s - %s\n", resp.Error.Code, resp.Error.Message)
			}
			if resp.Error.Code == pb.Code_NOT_LEADER || resp.Error.Code == pb.Code_NO_QUORUM {
				continue
			}
		} else {
			if outputJSON {
				data, _ := json.MarshalIndent(resp, "", "  ")
				fmt.Println(string(data))
			} else {
				fmt.Println("OK")
			}
		}
		return nil
	}

	return lastErr
}

func runGet(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	key, _ := cmd.Flags().GetString("key")

	conn, client, err := connectToLeader(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	resp, err := client.Get(ctx, &pb.GetRequest{
		Key: []byte(key),
	})
	if err != nil {
		return err
	}

	if outputJSON {
		data, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(data))
	} else {
		if resp.Error != nil && resp.Error.Code != pb.Code_OK {
			fmt.Printf("Error: %s - %s\n", resp.Error.Code, resp.Error.Message)
		} else if !resp.Found {
			fmt.Println("(not found)")
		} else {
			fmt.Printf("%s\n", string(resp.Value))
		}
	}

	return nil
}

func runDelete(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	key, _ := cmd.Flags().GetString("key")
	clientID, _ := cmd.Flags().GetString("client")
	seq, _ := cmd.Flags().GetUint64("seq")

	if clientID == "" {
		clientID = fmt.Sprintf("cli-%d", time.Now().UnixNano())
	}
	if seq == 0 {
		seq = uint64(time.Now().UnixNano())
	}

	conn, client, err := connectToLeader(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	resp, err := client.Delete(ctx, &pb.DeleteRequest{
		Key:      []byte(key),
		ClientId: clientID,
		Seq:      seq,
	})
	if err != nil {
		return err
	}

	if outputJSON {
		data, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(data))
	} else {
		if resp.Error != nil && resp.Error.Code != pb.Code_OK {
			fmt.Printf("Error: %s - %s\n", resp.Error.Code, resp.Error.Message)
		} else {
			fmt.Println("OK")
		}
	}

	return nil
}

func runCAS(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	key, _ := cmd.Flags().GetString("key")
	expected, _ := cmd.Flags().GetString("expected")
	desired, _ := cmd.Flags().GetString("desired")
	clientID, _ := cmd.Flags().GetString("client")
	seq, _ := cmd.Flags().GetUint64("seq")

	if clientID == "" {
		clientID = fmt.Sprintf("cli-%d", time.Now().UnixNano())
	}
	if seq == 0 {
		seq = uint64(time.Now().UnixNano())
	}

	conn, client, err := connectToLeader(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	resp, err := client.CAS(ctx, &pb.CASRequest{
		Key:      []byte(key),
		Expected: []byte(expected),
		Desired:  []byte(desired),
		ClientId: clientID,
		Seq:      seq,
	})
	if err != nil {
		return err
	}

	if outputJSON {
		data, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(data))
	} else {
		if resp.Error != nil && resp.Error.Code != pb.Code_OK {
			fmt.Printf("Error: %s - %s\n", resp.Error.Code, resp.Error.Message)
		} else {
			fmt.Printf("Swapped: %v\n", resp.Swapped)
			if !resp.Swapped {
				fmt.Printf("Current: %s\n", string(resp.Current))
			}
		}
	}

	return nil
}

func runPartitionLeader(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	leader, _ := cmd.Flags().GetString("leader")
	addrs := getAddrs()

	// Set drop=100% for all links from/to leader
	nodes := []string{"n1", "n2", "n3"}
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithTimeout(timeout)) //nolint:staticcheck // grpc.Dial deprecated but still supported
		if err != nil {
			continue
		}
		adminClient := pb.NewAdminClient(conn)

		for _, node := range nodes {
			if node == leader {
				continue
			}
			// Drop traffic from leader to this node
			_, _ = adminClient.SetLinkRule(ctx, &pb.SetLinkRuleRequest{
				SrcNodeId: leader,
				DstNodeId: node,
				DropPct:   100,
			})
			// Drop traffic from this node to leader
			_, _ = adminClient.SetLinkRule(ctx, &pb.SetLinkRuleRequest{
				SrcNodeId: node,
				DstNodeId: leader,
				DropPct:   100,
			})
		}
		_ = conn.Close()
	}

	fmt.Printf("Partitioned leader %s from other nodes\n", leader)
	return nil
}

func runHeal(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	addrs := getAddrs()

	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithTimeout(timeout)) //nolint:staticcheck // grpc.Dial deprecated but still supported
		if err != nil {
			continue
		}
		adminClient := pb.NewAdminClient(conn)
		_, _ = adminClient.ClearLinkRules(ctx, &pb.ClearLinkRulesRequest{})
		_ = conn.Close()
	}

	fmt.Println("All network partitions healed")
	return nil
}

func runDiskStall(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	node, _ := cmd.Flags().GetString("node")
	delay, _ := cmd.Flags().GetUint32("fsync-delay-ms")

	// Find the admin address for the node
	// Simple mapping: n1 -> 9201, n2 -> 9202, n3 -> 9203
	adminAddr := ""
	switch node {
	case "n1":
		adminAddr = "127.0.0.1:9201"
	case "n2":
		adminAddr = "127.0.0.1:9202"
	case "n3":
		adminAddr = "127.0.0.1:9203"
	default:
		return fmt.Errorf("unknown node: %s", node)
	}

	conn, err := grpc.Dial(adminAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithTimeout(timeout)) //nolint:staticcheck // grpc.Dial deprecated but still supported
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	adminClient := pb.NewAdminClient(conn)
	_, err = adminClient.SetDiskStall(ctx, &pb.SetDiskStallRequest{
		FsyncDelayMs: delay,
	})
	if err != nil {
		return err
	}

	fmt.Printf("Disk stall set on %s: %dms fsync delay\n", node, delay)
	return nil
}

func runCrash(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	node, _ := cmd.Flags().GetString("node")
	exitCode, _ := cmd.Flags().GetUint32("exit-code")

	// Find the admin address for the node
	adminAddr := ""
	switch node {
	case "n1":
		adminAddr = "127.0.0.1:9201"
	case "n2":
		adminAddr = "127.0.0.1:9202"
	case "n3":
		adminAddr = "127.0.0.1:9203"
	default:
		return fmt.Errorf("unknown node: %s", node)
	}

	conn, err := grpc.Dial(adminAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithTimeout(timeout)) //nolint:staticcheck // grpc.Dial deprecated but still supported
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	adminClient := pb.NewAdminClient(conn)
	// Crash may cause connection to close, so ignore errors
	_, _ = adminClient.Crash(ctx, &pb.CrashRequest{
		ExitCode: exitCode,
	})
	fmt.Printf("Crash signal sent to %s\n", node)
	return nil
}
