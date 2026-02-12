// Package main provides the ForgeKV benchmark tool.
package main

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/forgekv/forgekv/api/forgekv"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addresses       string
	adminAddrs      string
	metricsAddrs    string
	workload        string
	duration        time.Duration
	concurrency     int
	keyCount        int
	valueSize       int
	outputFile      string
	measureFailover bool
	failoverTimeout time.Duration
	benchDebug      bool

	clientAddrs      []string
	adminAddrsList   []string
	metricsAddrsList []string
)

// Stats tracks benchmark statistics.
type Stats struct {
	TotalOps     int64
	Reads        int64
	Writes       int64
	Errors       int64
	ReadLatency  []time.Duration
	WriteLatency []time.Duration
	mu           sync.Mutex
}

func main() {
	benchDebug = os.Getenv("FORGEKVBENCH_DEBUG") != ""
	rootCmd := &cobra.Command{
		Use:   "forgekvbench",
		Short: "ForgeKV benchmark tool",
		RunE:  runBench,
	}

	rootCmd.Flags().StringVar(&addresses, "addrs", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "Comma-separated list of node addresses")
	rootCmd.Flags().StringVar(&adminAddrs, "admin-addrs", "127.0.0.1:9201,127.0.0.1:9202,127.0.0.1:9203", "Comma-separated list of admin addresses")
	rootCmd.Flags().StringVar(&metricsAddrs, "metrics-addrs", "127.0.0.1:9301,127.0.0.1:9302,127.0.0.1:9303", "Comma-separated list of metrics addresses")
	rootCmd.Flags().StringVar(&workload, "workload", "mixed", "Workload type: writeheavy, mixed, readheavy")
	rootCmd.Flags().DurationVar(&duration, "duration", 60*time.Second, "Benchmark duration")
	rootCmd.Flags().IntVar(&concurrency, "concurrency", 64, "Number of concurrent workers")
	rootCmd.Flags().IntVar(&keyCount, "keys", 10000, "Number of unique keys")
	rootCmd.Flags().IntVar(&valueSize, "value-size", 256, "Value size in bytes")
	rootCmd.Flags().StringVar(&outputFile, "output", "", "Output file for report (default: stdout)")
	rootCmd.Flags().BoolVar(&measureFailover, "measure-failover", true, "Measure leader failover time by crashing the leader after the benchmark")
	rootCmd.Flags().DurationVar(&failoverTimeout, "failover-timeout", 30*time.Second, "Timeout for leader failover measurement")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func benchDebugf(format string, args ...any) {
	if !benchDebug {
		return
	}
	ts := time.Now().Format(time.RFC3339Nano)
	fmt.Fprintf(os.Stderr, "[bench-debug %s] %s\n", ts, fmt.Sprintf(format, args...))
}

func runBench(cmd *cobra.Command, args []string) error {
	clientAddrs = parseAddrs(addresses)
	adminAddrsList = parseAddrs(adminAddrs)
	metricsAddrsList = parseAddrs(metricsAddrs)
	if len(clientAddrs) == 0 {
		return fmt.Errorf("no client addresses provided")
	}

	// Determine read/write ratio
	var readPct int
	switch workload {
	case "writeheavy":
		readPct = 10
	case "mixed":
		readPct = 70
	case "readheavy":
		readPct = 95
	default:
		return fmt.Errorf("unknown workload: %s", workload)
	}

	fmt.Printf("ForgeKV Benchmark\n")
	fmt.Printf("=================\n")
	fmt.Printf("Workload:    %s (%d%% reads, %d%% writes)\n", workload, readPct, 100-readPct)
	fmt.Printf("Duration:    %s\n", duration)
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Keys:        %d\n", keyCount)
	fmt.Printf("Value size:  %d bytes\n", valueSize)
	fmt.Printf("Addresses:   %s\n", addresses)
	if len(metricsAddrsList) > 0 {
		fmt.Printf("Metrics:     %s\n", metricsAddrs)
	}
	if len(adminAddrsList) > 0 {
		fmt.Printf("Admin:       %s\n", adminAddrs)
	}
	if measureFailover {
		fmt.Printf("Failover:    enabled (timeout %s)\n", failoverTimeout)
	} else {
		fmt.Printf("Failover:    disabled\n")
	}
	fmt.Println()

	// Pre-populate some keys
	fmt.Print("Pre-populating keys... ")
	_, leaderAddr, _, err := findLeader(context.Background(), clientAddrs)
	if err != nil {
		return fmt.Errorf("find leader failed: %w", err)
	}
	if err := prepopulate(leaderAddr); err != nil {
		return fmt.Errorf("prepopulate failed: %w", err)
	}
	fmt.Println("done")

	// Run benchmark
	fmt.Printf("Running benchmark for %s...\n", duration)
	stats := &Stats{
		ReadLatency:  make([]time.Duration, 0, 100000),
		WriteLatency: make([]time.Duration, 0, 100000),
	}

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, workerID, readPct, stats, leaderAddr)
		}(i)
	}

	// Progress reporting
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("  Progress: %d ops (%d reads, %d writes, %d errors)\n",
					atomic.LoadInt64(&stats.TotalOps),
					atomic.LoadInt64(&stats.Reads),
					atomic.LoadInt64(&stats.Writes),
					atomic.LoadInt64(&stats.Errors))
			}
		}
	}()

	wg.Wait()
	fmt.Println()

	var failoverResult *failoverReport
	if measureFailover && len(adminAddrsList) > 0 {
		failCtx, cancelFail := context.WithTimeout(context.Background(), failoverTimeout)
		failoverResult = measureLeaderFailover(failCtx, clientAddrs, adminAddrsList)
		cancelFail()
	}

	var backpressureResult *backpressureReport
	if len(metricsAddrsList) > 0 {
		metricsCtx, cancelMetrics := context.WithTimeout(context.Background(), 5*time.Second)
		backpressureResult = fetchBackpressure(metricsCtx, metricsAddrsList)
		cancelMetrics()
	}

	// Generate report
	report := generateReport(stats, failoverResult, backpressureResult)
	if outputFile != "" {
		if err := os.WriteFile(outputFile, []byte(report), 0644); err != nil {
			return fmt.Errorf("failed to write report: %w", err)
		}
		fmt.Printf("Report written to %s\n", outputFile)
	} else {
		fmt.Println(report)
	}

	return nil
}

func prepopulate(leaderAddr string) error {
	conn, client, err := dialKV(context.Background(), leaderAddr, 3*time.Second)
	if err != nil {
		benchDebugf("prepopulate: dial leader %s failed: %v", leaderAddr, err)
		return err
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Pre-populate 10% of keys
	value := make([]byte, valueSize)
	_, _ = cryptorand.Read(value)

	for i := 0; i < keyCount/10; i++ {
		key := fmt.Sprintf("key-%08d", i)
		_, err := client.Put(ctx, &pb.PutRequest{
			Key:      []byte(key),
			Value:    value,
			ClientId: "bench-prepop",
			Seq:      uint64(i + 1),
		})
		if err != nil {
			benchDebugf("prepopulate: put %s failed: %v", key, err)
			// Ignore errors during prepopulation
			continue
		}
	}

	return nil
}

func runWorker(ctx context.Context, workerID int, readPct int, stats *Stats, leaderAddr string) {
	conn, client, err := dialKV(context.Background(), leaderAddr, 3*time.Second)
	if err != nil {
		benchDebugf("worker %d: dial leader %s failed: %v", workerID, leaderAddr, err)
		return
	}
	defer func() { _ = conn.Close() }()
	clientID := fmt.Sprintf("bench-worker-%d", workerID)
	var seq uint64

	value := make([]byte, valueSize)
	_, _ = cryptorand.Read(value)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		key := fmt.Sprintf("key-%08d", rand.Intn(keyCount))
		isRead := rand.Intn(100) < readPct

		start := time.Now()
		var opErr error

		if isRead {
			resp, err := client.Get(ctx, &pb.GetRequest{Key: []byte(key)})
			opErr = classifyRPCError(resp, err)
			if opErr == nil {
				atomic.AddInt64(&stats.Reads, 1)
				stats.mu.Lock()
				stats.ReadLatency = append(stats.ReadLatency, time.Since(start))
				stats.mu.Unlock()
			}
		} else {
			seq++
			resp, err := client.Put(ctx, &pb.PutRequest{
				Key:      []byte(key),
				Value:    value,
				ClientId: clientID,
				Seq:      seq,
			})
			opErr = classifyRPCError(resp, err)
			if opErr == nil {
				atomic.AddInt64(&stats.Writes, 1)
				stats.mu.Lock()
				stats.WriteLatency = append(stats.WriteLatency, time.Since(start))
				stats.mu.Unlock()
			}
		}

		atomic.AddInt64(&stats.TotalOps, 1)
		if opErr != nil {
			atomic.AddInt64(&stats.Errors, 1)
		}
	}
}

type failoverReport struct {
	Elapsed  time.Duration
	LeaderID string
	Error    string
}

type backpressureReport struct {
	DelayMs float64
	Rejects float64
	Errors  []string
}

func generateReport(stats *Stats, failover *failoverReport, backpressure *backpressureReport) string {
	totalOps := atomic.LoadInt64(&stats.TotalOps)
	reads := atomic.LoadInt64(&stats.Reads)
	writes := atomic.LoadInt64(&stats.Writes)
	errors := atomic.LoadInt64(&stats.Errors)

	throughput := float64(totalOps) / duration.Seconds()

	// Calculate latency percentiles
	stats.mu.Lock()
	readP50, readP95, readP99 := percentiles(stats.ReadLatency)
	writeP50, writeP95, writeP99 := percentiles(stats.WriteLatency)
	stats.mu.Unlock()

	report := fmt.Sprintf(`# ForgeKV Benchmark Report

## Configuration

- Workload: %s
- Duration: %s
- Concurrency: %d
- Keys: %d
- Value Size: %d bytes

## Results

### Throughput

- Total Operations: %d
- Reads: %d
- Writes: %d
- Errors: %d
- Throughput: %.2f ops/s

### Read Latency

- P50: %s
- P95: %s
- P99: %s

### Write Latency

- P50: %s
- P95: %s
- P99: %s
`, workload, duration, concurrency, keyCount, valueSize,
		totalOps, reads, writes, errors, throughput,
		readP50, readP95, readP99,
		writeP50, writeP95, writeP99)

	if failover != nil {
		report += "\n### Leader Failover (Crash)\n\n"
		if failover.Error != "" {
			report += fmt.Sprintf("- Result: failed (%s)\n", failover.Error)
		} else {
			report += fmt.Sprintf("- Leader ID: %s\n", failover.LeaderID)
			report += fmt.Sprintf("- Failover Time: %d ms\n", failover.Elapsed.Milliseconds())
		}
	}

	if backpressure != nil {
		report += "\n### Backpressure Counters\n\n"
		if len(backpressure.Errors) > 0 {
			report += fmt.Sprintf("- Metrics errors: %s\n", strings.Join(backpressure.Errors, "; "))
		}
		report += fmt.Sprintf("- Total Backpressure Delay: %.0f ms\n", backpressure.DelayMs)
		report += fmt.Sprintf("- Total Backpressure Rejects: %.0f\n", backpressure.Rejects)
	}

	report += fmt.Sprintf(`
## Environment

- Timestamp: %s
- Addresses: %s
`, time.Now().Format(time.RFC3339), addresses)

	if len(metricsAddrsList) > 0 {
		report += fmt.Sprintf("- Metrics Addresses: %s\n", metricsAddrs)
	}
	if len(adminAddrsList) > 0 {
		report += fmt.Sprintf("- Admin Addresses: %s\n", adminAddrs)
	}

	report += `

---
Generated by forgekvbench
`

	return report
}

func percentiles(latencies []time.Duration) (p50, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}

	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	if len(sorted) > 0 {
		p99 = sorted[len(sorted)*99/100]
	}

	return
}

func parseAddrs(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
}

func classifyRPCError(resp interface{}, err error) error {
	if err != nil {
		return err
	}
	switch r := resp.(type) {
	case *pb.GetResponse:
		if r == nil || r.Error == nil || r.Error.Code == pb.Code_OK {
			return nil
		}
		return fmt.Errorf("%s", r.Error.Message)
	case *pb.PutResponse:
		if r == nil || r.Error == nil || r.Error.Code == pb.Code_OK {
			return nil
		}
		return fmt.Errorf("%s", r.Error.Message)
	default:
		return nil
	}
}

func findLeader(ctx context.Context, addrs []string) (string, string, int, error) {
	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		for idx, addr := range addrs {
			conn, client, err := dialKV(ctx, addr, 500*time.Millisecond)
			if err != nil {
				lastErr = err
				continue
			}
			statusCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			resp, err := client.Status(statusCtx, &pb.StatusRequest{})
			cancel()
			_ = conn.Close()
			if err != nil {
				lastErr = err
				continue
			}
			if resp.IsLeader {
				return resp.NodeId, addr, idx, nil
			}
			if resp.LeaderId != "" && resp.LeaderClientAddr != "" {
				leaderIdx := indexOfAddr(addrs, resp.LeaderClientAddr)
				return resp.LeaderId, resp.LeaderClientAddr, leaderIdx, nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("leader not found")
	}
	return "", "", -1, lastErr
}

func indexOfAddr(addrs []string, addr string) int {
	for i, value := range addrs {
		if value == addr {
			return i
		}
	}
	return -1
}

func dialKV(ctx context.Context, addr string, timeout time.Duration) (*grpc.ClientConn, pb.KVClient, error) {
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()) //nolint:staticcheck // grpc.DialContext deprecated but still supported
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewKVClient(conn), nil
}

func dialAdmin(ctx context.Context, addr string, timeout time.Duration) (*grpc.ClientConn, pb.AdminClient, error) {
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()) //nolint:staticcheck // grpc.DialContext deprecated but still supported
	if err != nil {
		return nil, nil, err
	}
	return conn, pb.NewAdminClient(conn), nil
}

func measureLeaderFailover(ctx context.Context, clientAddrs []string, adminAddrs []string) *failoverReport {
	leaderID, _, leaderIdx, err := findLeader(ctx, clientAddrs)
	if err != nil {
		return &failoverReport{Error: err.Error()}
	}
	if leaderIdx < 0 || leaderIdx >= len(adminAddrs) {
		return &failoverReport{Error: "no admin address for leader"}
	}

	adminConn, adminClient, err := dialAdmin(ctx, adminAddrs[leaderIdx], 2*time.Second)
	if err != nil {
		return &failoverReport{Error: err.Error()}
	}
	_, _ = adminClient.Crash(ctx, &pb.CrashRequest{ExitCode: 137})
	_ = adminConn.Close()

	start := time.Now()
	for {
		if ctx.Err() != nil {
			return &failoverReport{Error: "failover timed out"}
		}
		newLeaderID, newLeaderAddr, _, err := findLeader(ctx, clientAddrs)
		if err == nil && newLeaderID != "" && newLeaderID != leaderID {
			if ok := tryWrite(ctx, newLeaderAddr); ok {
				return &failoverReport{
					Elapsed:  time.Since(start),
					LeaderID: newLeaderID,
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func tryWrite(ctx context.Context, addr string) bool {
	conn, client, err := dialKV(ctx, addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	defer func() { _ = conn.Close() }()
	putCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	resp, err := client.Put(putCtx, &pb.PutRequest{
		Key:      []byte("bench-failover"),
		Value:    []byte("ok"),
		ClientId: "bench-failover",
		Seq:      1,
	})
	if err != nil {
		return false
	}
	if resp.Error != nil && resp.Error.Code != pb.Code_OK {
		return false
	}
	return true
}

func fetchBackpressure(ctx context.Context, addrs []string) *backpressureReport {
	report := &backpressureReport{}
	client := &http.Client{Timeout: 2 * time.Second}
	for _, addr := range addrs {
		url := fmt.Sprintf("http://%s/metrics", addr)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %v", addr, err))
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %v", addr, err))
			continue
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("%s: %v", addr, err))
			continue
		}
		delay, rejects := parseBackpressureMetrics(string(body))
		report.DelayMs += delay
		report.Rejects += rejects
	}
	return report
}

func parseBackpressureMetrics(body string) (delayMs float64, rejects float64) {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "forgekv_backpressure_delay_ms_total") {
			if value, ok := parseMetricValue(line); ok {
				delayMs += value
			}
		}
		if strings.HasPrefix(line, "forgekv_backpressure_rejects_total") {
			if value, ok := parseMetricValue(line); ok {
				rejects += value
			}
		}
	}
	return
}

func parseMetricValue(line string) (float64, bool) {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0, false
	}
	value, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0, false
	}
	return value, true
}
