//go:build chaos
// +build chaos

package chaos

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	pb "github.com/forgekv/forgekv/api/forgekv"
)

type opRecord struct {
	Type     string
	Key      string
	Value    string
	Expected string
	Desired  string
	ClientID string
	Seq      uint64
	Start    time.Time
	End      time.Time
	Response interface{}
	Err      error
}

type workloadConfig struct {
	Clients    int
	KeyCount   int
	OpInterval time.Duration
	PutPct     int
	GetPct     int
	CasPct     int
}

type errorBudget struct {
	MaxErrorRate    float64
	MaxRPCErrorRate float64
	AllowedCodes    map[pb.Code]bool
}

type scenarioConfig struct {
	Name          string
	Duration      time.Duration
	Workload      workloadConfig
	Fault         func(context.Context, *clientPool, *chaosCluster)
	ErrorBudget   errorBudget
	CheckRegister bool
	CheckCAS      bool
}

const opTimeout = 8 * time.Second

func defaultWorkload() workloadConfig {
	return workloadConfig{
		Clients:    3,
		KeyCount:   5,
		OpInterval: 200 * time.Millisecond,
		PutPct:     35,
		GetPct:     45,
		CasPct:     20,
	}
}

func runScenario(t *testing.T, cfg scenarioConfig) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration+25*time.Second)
	defer cancel()

	pool := newClientPool(cluster.clientAddrs)
	defer pool.close()

	workloadCtx, workloadCancel := context.WithTimeout(ctx, cfg.Duration)
	defer workloadCancel()

	clientPrefix := fmt.Sprintf("%s-%d", cfg.Name, time.Now().UnixNano())

	seedCtx, seedCancel := context.WithTimeout(ctx, 10*time.Second)
	seedOps := seedKeys(seedCtx, pool, cfg.Workload, clientPrefix)
	seedCancel()
	assertSeedOps(t, seedOps)

	doneFault := make(chan struct{})
	if cfg.Fault != nil {
		go func() {
			defer close(doneFault)
			cfg.Fault(ctx, pool, cluster)
		}()
	} else {
		close(doneFault)
	}

	workloadOps := runMixedWorkload(workloadCtx, ctx, pool, cfg.Workload, clientPrefix)
	ops := append(seedOps, workloadOps...)
	<-doneFault

	resolveCtx, resolveCancel := context.WithTimeout(context.Background(), 20*time.Second)
	resolveWriteErrors(resolveCtx, pool, ops)
	resolveCancel()

	assertErrorBudget(t, ops[len(seedOps):], cfg.ErrorBudget)
	checkLinearizability(t, ops, cfg.CheckRegister, cfg.CheckCAS)
}

func seedKeys(ctx context.Context, pool *clientPool, cfg workloadConfig, clientPrefix string) []opRecord {
	if cfg.KeyCount == 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	clientID := fmt.Sprintf("%s-seed", clientPrefix)
	leaderID := ""
	var seq uint64

	ops := make([]opRecord, 0, cfg.KeyCount)
	for i := 0; i < cfg.KeyCount; i++ {
		seq++
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("seed-%s-%d", key, rng.Intn(1_000_000))
		op, newLeader := doPutWithRetry(ctx, pool, leaderID, clientID, key, value, seq, rng)
		leaderID = newLeader
		ops = append(ops, op)
	}
	return ops
}

func assertSeedOps(t *testing.T, ops []opRecord) {
	t.Helper()
	for _, op := range ops {
		if op.Err != nil {
			t.Fatalf("seed op failed: %v", op.Err)
		}
		if responseCode(op.Response) != pb.Code_OK {
			t.Fatalf("seed op returned %s", responseCode(op.Response).String())
		}
	}
}

func runMixedWorkload(workloadCtx, opCtx context.Context, pool *clientPool, cfg workloadConfig, clientPrefix string) []opRecord {
	var wg sync.WaitGroup
	opsCh := make(chan opRecord, cfg.Clients*16)

	for i := 0; i < cfg.Clients; i++ {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientNum)))
			clientID := fmt.Sprintf("%s-%d", clientPrefix, clientNum)
			local := make(map[string]string, cfg.KeyCount)
			leaderID := ""
			var seq uint64

			ticker := time.NewTicker(cfg.OpInterval)
			defer ticker.Stop()

			for {
				select {
				case <-workloadCtx.Done():
					return
				case <-ticker.C:
				}

				opType := chooseOpType(rng, cfg)
				key := fmt.Sprintf("key-%d", rng.Intn(cfg.KeyCount))

				switch opType {
				case "get":
					op, newLeader := doGetWithRetry(opCtx, pool, leaderID, clientID, key, rng)
					leaderID = newLeader
					updateLocalFromGet(local, op)
					opsCh <- op
				case "put":
					seq++
					value := randomValue(rng, key)
					op, newLeader := doPutWithRetry(opCtx, pool, leaderID, clientID, key, value, seq, rng)
					leaderID = newLeader
					updateLocalFromPut(local, op)
					opsCh <- op
				case "cas":
					seq++
					expected, desired := casInputs(rng, local, key)
					op, newLeader := doCASWithRetry(opCtx, pool, leaderID, clientID, key, expected, desired, seq, rng)
					leaderID = newLeader
					updateLocalFromCAS(local, op)
					opsCh <- op
				}
			}
		}(i)
	}

	go func() {
		wg.Wait()
		close(opsCh)
	}()

	ops := make([]opRecord, 0, cfg.Clients*64)
	for op := range opsCh {
		ops = append(ops, op)
	}
	return ops
}

func resolveWriteErrors(ctx context.Context, pool *clientPool, ops []opRecord) {
	if ctx.Err() != nil {
		return
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range ops {
		op := &ops[i]
		if op.Type != "put" && op.Type != "cas" {
			continue
		}
		if op.Err == nil && responseCode(op.Response) == pb.Code_OK {
			continue
		}

		switch op.Type {
		case "put":
			resolved, _ := doPutWithRetry(ctx, pool, "", op.ClientID, op.Key, op.Value, op.Seq, rng)
			op.Response = resolved.Response
			op.Err = resolved.Err
			op.End = resolved.End
		case "cas":
			resolved, _ := doCASWithRetry(ctx, pool, "", op.ClientID, op.Key, op.Expected, op.Desired, op.Seq, rng)
			op.Response = resolved.Response
			op.Err = resolved.Err
			op.End = resolved.End
		}
	}
}

func chooseOpType(rng *rand.Rand, cfg workloadConfig) string {
	roll := rng.Intn(100)
	if roll < cfg.PutPct {
		return "put"
	}
	if roll < cfg.PutPct+cfg.GetPct {
		return "get"
	}
	return "cas"
}

func randomValue(rng *rand.Rand, key string) string {
	return fmt.Sprintf("v-%s-%d", key, rng.Intn(1_000_000))
}

func casInputs(rng *rand.Rand, local map[string]string, key string) (string, string) {
	current, ok := local[key]
	useExpected := rng.Float64() < 0.7
	if ok && useExpected {
		return current, randomValue(rng, key)
	}
	if !ok && useExpected {
		return "", randomValue(rng, key)
	}
	return randomValue(rng, key), randomValue(rng, key)
}

func doGetWithRetry(ctx context.Context, pool *clientPool, leaderID, clientID, key string, rng *rand.Rand) (opRecord, string) {
	start := time.Now()
	var resp *pb.GetResponse
	var err error
	attempt := 0

	for {
		if ctx.Err() != nil {
			err = ctx.Err()
			break
		}
		attempt++

		leaderID, client, pickErr := pickLeader(ctx, pool, leaderID)
		if pickErr != nil {
			err = pickErr
			sleepWithJitter(rng, attempt)
			continue
		}

		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		resp, err = client.Get(opCtx, &pb.GetRequest{Key: []byte(key)})
		cancel()
		leaderID = updateLeaderFromResponse(leaderID, resp, err)
		if err != nil {
			sleepWithJitter(rng, attempt)
			continue
		}

		if resp != nil && resp.Error != nil && isRetryableCode(resp.Error.Code) {
			sleepWithJitter(rng, attempt)
			continue
		}
		break
	}

	end := time.Now()
	return opRecord{
		Type:     "get",
		Key:      key,
		ClientID: clientID,
		Start:    start,
		End:      end,
		Response: resp,
		Err:      err,
	}, leaderID
}

func doPutWithRetry(ctx context.Context, pool *clientPool, leaderID, clientID, key, value string, seq uint64, rng *rand.Rand) (opRecord, string) {
	start := time.Now()
	var resp *pb.PutResponse
	var err error
	attempt := 0

	for {
		if ctx.Err() != nil {
			err = ctx.Err()
			break
		}
		attempt++

		leaderID, client, pickErr := pickLeader(ctx, pool, leaderID)
		if pickErr != nil {
			err = pickErr
			sleepWithJitter(rng, attempt)
			continue
		}

		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		resp, err = client.Put(opCtx, &pb.PutRequest{
			Key:      []byte(key),
			Value:    []byte(value),
			ClientId: clientID,
			Seq:      seq,
		})
		cancel()
		leaderID = updateLeaderFromResponse(leaderID, resp, err)
		if err != nil {
			sleepWithJitter(rng, attempt)
			continue
		}
		if resp != nil && resp.Error != nil && isRetryableCode(resp.Error.Code) {
			sleepWithJitter(rng, attempt)
			continue
		}
		break
	}

	end := time.Now()
	return opRecord{
		Type:     "put",
		Key:      key,
		Value:    value,
		ClientID: clientID,
		Seq:      seq,
		Start:    start,
		End:      end,
		Response: resp,
		Err:      err,
	}, leaderID
}

func doCASWithRetry(ctx context.Context, pool *clientPool, leaderID, clientID, key, expected, desired string, seq uint64, rng *rand.Rand) (opRecord, string) {
	start := time.Now()
	var resp *pb.CASResponse
	var err error
	attempt := 0

	for {
		if ctx.Err() != nil {
			err = ctx.Err()
			break
		}
		attempt++

		leaderID, client, pickErr := pickLeader(ctx, pool, leaderID)
		if pickErr != nil {
			err = pickErr
			sleepWithJitter(rng, attempt)
			continue
		}

		opCtx, cancel := context.WithTimeout(ctx, opTimeout)
		resp, err = client.CAS(opCtx, &pb.CASRequest{
			Key:      []byte(key),
			Expected: []byte(expected),
			Desired:  []byte(desired),
			ClientId: clientID,
			Seq:      seq,
		})
		cancel()
		leaderID = updateLeaderFromResponse(leaderID, resp, err)
		if err != nil {
			sleepWithJitter(rng, attempt)
			continue
		}
		if resp != nil && resp.Error != nil && isRetryableCode(resp.Error.Code) {
			sleepWithJitter(rng, attempt)
			continue
		}
		break
	}

	end := time.Now()
	return opRecord{
		Type:     "cas",
		Key:      key,
		Expected: expected,
		Desired:  desired,
		ClientID: clientID,
		Seq:      seq,
		Start:    start,
		End:      end,
		Response: resp,
		Err:      err,
	}, leaderID
}

func isRetryableCode(code pb.Code) bool {
	switch code {
	case pb.Code_NOT_LEADER, pb.Code_NO_QUORUM, pb.Code_INTERNAL:
		return true
	default:
		return false
	}
}

func sleepWithJitter(rng *rand.Rand, attempt int) {
	base := 15 + attempt*5
	jitter := rng.Intn(20)
	if base > 200 {
		base = 200
	}
	time.Sleep(time.Duration(base+jitter) * time.Millisecond)
}

func updateLocalFromPut(local map[string]string, op opRecord) {
	resp, ok := op.Response.(*pb.PutResponse)
	if !ok || op.Err != nil {
		return
	}
	if resp.Error == nil || resp.Error.Code == pb.Code_OK {
		local[op.Key] = op.Value
	}
}

func updateLocalFromGet(local map[string]string, op opRecord) {
	resp, ok := op.Response.(*pb.GetResponse)
	if !ok || op.Err != nil {
		return
	}
	if resp.Error != nil && resp.Error.Code != pb.Code_OK {
		return
	}
	if resp.Found {
		local[op.Key] = string(resp.Value)
		return
	}
	delete(local, op.Key)
}

func updateLocalFromCAS(local map[string]string, op opRecord) {
	resp, ok := op.Response.(*pb.CASResponse)
	if !ok || op.Err != nil {
		return
	}
	if resp.Error != nil && resp.Error.Code != pb.Code_OK {
		return
	}
	if resp.Swapped {
		if len(resp.Current) == 0 || !resp.Found {
			delete(local, op.Key)
			return
		}
		local[op.Key] = string(resp.Current)
		return
	}
	if resp.Found {
		local[op.Key] = string(resp.Current)
		return
	}
	delete(local, op.Key)
}

func assertErrorBudget(t *testing.T, ops []opRecord, budget errorBudget) {
	t.Helper()
	stats := collectStats(ops)
	if stats.Total == 0 {
		t.Fatalf("no operations recorded")
	}

	errorRate := float64(stats.Total-stats.OK) / float64(stats.Total)
	rpcRate := float64(stats.RPCErr) / float64(stats.Total)

	if budget.MaxErrorRate == 0 && errorRate > 0 {
		t.Fatalf("unexpected errors: total=%d ok=%d rpc=%d", stats.Total, stats.OK, stats.RPCErr)
	}
	if budget.MaxErrorRate > 0 && errorRate > budget.MaxErrorRate {
		t.Fatalf("error rate too high: %.2f (max %.2f)", errorRate, budget.MaxErrorRate)
	}
	if budget.MaxRPCErrorRate == 0 && stats.RPCErr > 0 {
		t.Fatalf("unexpected rpc errors: %d", stats.RPCErr)
	}
	if budget.MaxRPCErrorRate > 0 && rpcRate > budget.MaxRPCErrorRate {
		t.Fatalf("rpc error rate too high: %.2f (max %.2f)", rpcRate, budget.MaxRPCErrorRate)
	}

	for code, count := range stats.CodeCounts {
		if code == pb.Code_OK {
			continue
		}
		if budget.AllowedCodes == nil || !budget.AllowedCodes[code] {
			t.Fatalf("unexpected error code %s: %d", code.String(), count)
		}
	}
}

type opStats struct {
	Total      int
	OK         int
	RPCErr     int
	CodeCounts map[pb.Code]int
}

func collectStats(ops []opRecord) opStats {
	stats := opStats{CodeCounts: make(map[pb.Code]int)}
	for _, op := range ops {
		stats.Total++
		if op.Err != nil {
			stats.RPCErr++
			continue
		}
		code := responseCode(op.Response)
		stats.CodeCounts[code]++
		if code == pb.Code_OK {
			stats.OK++
		}
	}
	return stats
}

func responseCode(resp interface{}) pb.Code {
	if resp == nil {
		return pb.Code_INTERNAL
	}
	switch r := resp.(type) {
	case *pb.PutResponse:
		if r.Error == nil {
			return pb.Code_OK
		}
		return r.Error.Code
	case *pb.GetResponse:
		if r.Error == nil {
			return pb.Code_OK
		}
		return r.Error.Code
	case *pb.CASResponse:
		if r.Error == nil {
			return pb.Code_OK
		}
		return r.Error.Code
	default:
		return pb.Code_INTERNAL
	}
}

func checkLinearizability(t *testing.T, ops []opRecord, checkRegister bool, checkCAS bool) {
	t.Helper()

	if checkRegister {
		registerHistory := toPorcupineHistory(filterOps(ops, map[string]bool{"put": true, "get": true}))
		if len(registerHistory) == 0 {
			t.Fatalf("register history is empty")
		}
		if !porcupine.CheckOperations(registerModel(), registerHistory) {
			t.Fatalf("register linearizability violation detected")
		}
	}

	if checkCAS {
		casHistory := toPorcupineHistory(ops)
		if len(casHistory) == 0 {
			t.Fatalf("cas history is empty")
		}
		if !porcupine.CheckOperations(casModel(), casHistory) {
			if os.Getenv("FORGEKV_CHAOS_DEBUG") != "" {
				debugCASFailure(t, casHistory)
			}
			t.Fatalf("cas linearizability violation detected")
		}
	}
}

func debugCASFailure(t *testing.T, history []porcupine.Operation) {
	parts := partitionByKeyMap(history)
	model := casModel()
	model.Partition = nil

	keys := make([]string, 0, len(parts))
	for key := range parts {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		part := parts[key]
		if !porcupine.CheckOperations(model, part) {
			t.Logf("cas failure for key=%s ops=%d", key, len(part))
			sorted := append([]porcupine.Operation(nil), part...)
			sort.Slice(sorted, func(i, j int) bool {
				if sorted[i].Call == sorted[j].Call {
					return sorted[i].Return < sorted[j].Return
				}
				return sorted[i].Call < sorted[j].Call
			})
			logOps(t, sorted, len(sorted))
			dumpCASHistory(t, key, sorted)
			return
		}
	}
}

func logOps(t *testing.T, ops []porcupine.Operation, limit int) {
	if limit <= 0 {
		return
	}
	if len(ops) < limit {
		limit = len(ops)
	}
	for i := 0; i < limit; i++ {
		op := ops[i]
		input, _ := op.Input.(opInput)
		output, _ := op.Output.(opOutput)
		t.Logf("op[%d] call=%d return=%d input=%+v output=%+v", i, op.Call, op.Return, input, output)
	}
}

func dumpCASHistory(t *testing.T, key string, ops []porcupine.Operation) {
	if os.Getenv("FORGEKV_CHAOS_DEBUG") == "" {
		return
	}
	path := fmt.Sprintf("/tmp/forgekv-cas-failure-%s-%d.json", key, time.Now().UnixNano())
	payload := make([]map[string]interface{}, 0, len(ops))
	for _, op := range ops {
		input, _ := op.Input.(opInput)
		output, _ := op.Output.(opOutput)
		payload = append(payload, map[string]interface{}{
			"call":   op.Call,
			"return": op.Return,
			"input":  input,
			"output": output,
		})
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Logf("failed to encode cas history: %v", err)
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Logf("failed to write cas history: %v", err)
		return
	}
	t.Logf("wrote cas history to %s", path)
}

func filterOps(ops []opRecord, allowed map[string]bool) []opRecord {
	out := make([]opRecord, 0, len(ops))
	for _, op := range ops {
		if allowed[op.Type] {
			out = append(out, op)
		}
	}
	return out
}

type opInput struct {
	Op       string
	Key      string
	Value    string
	Expected string
	Desired  string
}

type opOutput struct {
	OK      bool
	Code    pb.Code
	Value   string
	Found   bool
	Swapped bool
}

func toPorcupineHistory(ops []opRecord) []porcupine.Operation {
	clientIDs := make(map[string]int)
	nextID := 0

	history := make([]porcupine.Operation, 0, len(ops))
	for _, op := range ops {
		id, ok := clientIDs[op.ClientID]
		if !ok {
			id = nextID
			nextID++
			clientIDs[op.ClientID] = id
		}

		input := opInput{
			Op:       op.Type,
			Key:      op.Key,
			Value:    op.Value,
			Expected: op.Expected,
			Desired:  op.Desired,
		}
		output := opOutputFromRecord(op)
		if !output.OK {
			continue
		}

		call := op.Start.UnixNano()
		ret := op.End.UnixNano()
		if ret < call {
			ret = call
		}

		history = append(history, porcupine.Operation{
			ClientId: id,
			Input:    input,
			Output:   output,
			Call:     call,
			Return:   ret,
		})
	}

	return history
}

func opOutputFromRecord(op opRecord) opOutput {
	if op.Err != nil {
		return opOutput{OK: false, Code: pb.Code_INTERNAL}
	}

	switch resp := op.Response.(type) {
	case *pb.PutResponse:
		if resp == nil {
			return opOutput{OK: false, Code: pb.Code_INTERNAL}
		}
		if resp.Error == nil {
			return opOutput{OK: true, Code: pb.Code_OK}
		}
		return opOutput{OK: resp.Error.Code == pb.Code_OK, Code: resp.Error.Code}
	case *pb.GetResponse:
		if resp == nil {
			return opOutput{OK: false, Code: pb.Code_INTERNAL}
		}
		if resp.Error == nil {
			return opOutput{OK: true, Code: pb.Code_OK, Value: string(resp.Value), Found: resp.Found}
		}
		if resp.Error.Code != pb.Code_OK {
			return opOutput{OK: false, Code: resp.Error.Code}
		}
		return opOutput{OK: true, Code: resp.Error.Code, Value: string(resp.Value), Found: resp.Found}
	case *pb.CASResponse:
		if resp == nil {
			return opOutput{OK: false, Code: pb.Code_INTERNAL}
		}
		if resp.Error == nil {
			return opOutput{OK: true, Code: pb.Code_OK, Value: string(resp.Current), Found: resp.Found, Swapped: resp.Swapped}
		}
		if resp.Error.Code != pb.Code_OK {
			return opOutput{OK: false, Code: resp.Error.Code}
		}
		return opOutput{OK: true, Code: resp.Error.Code, Value: string(resp.Current), Found: resp.Found, Swapped: resp.Swapped}
	default:
		return opOutput{OK: false, Code: pb.Code_INTERNAL}
	}
}

type kvState map[string]string

func cloneState(state kvState) kvState {
	clone := make(kvState, len(state))
	for k, v := range state {
		clone[k] = v
	}
	return clone
}

func registerModel() porcupine.Model {
	return porcupine.Model{
		Partition: partitionByKey,
		Init: func() interface{} {
			return kvState{}
		},
		Step: func(state, input, output interface{}) (bool, interface{}) {
			kv := state.(kvState)
			in := input.(opInput)
			out := output.(opOutput)

			switch in.Op {
			case "put":
				if !out.OK {
					return true, kv
				}
				next := cloneState(kv)
				next[in.Key] = in.Value
				return true, next
			case "get":
				if !out.OK {
					return true, kv
				}
				val, ok := kv[in.Key]
				if out.Found != ok {
					return false, kv
				}
				if ok && out.Value != val {
					return false, kv
				}
				return true, kv
			default:
				return true, kv
			}
		},
	}
}

func casModel() porcupine.Model {
	return porcupine.Model{
		Partition: partitionByKey,
		Init: func() interface{} {
			return kvState{}
		},
		Step: func(state, input, output interface{}) (bool, interface{}) {
			kv := state.(kvState)
			in := input.(opInput)
			out := output.(opOutput)

			switch in.Op {
			case "put":
				if !out.OK {
					return true, kv
				}
				next := cloneState(kv)
				next[in.Key] = in.Value
				return true, next
			case "get":
				if !out.OK {
					return true, kv
				}
				val, ok := kv[in.Key]
				if out.Found != ok {
					return false, kv
				}
				if ok && out.Value != val {
					return false, kv
				}
				return true, kv
			case "cas":
				if !out.OK {
					return true, kv
				}
				current, ok := kv[in.Key]
				expectedMatch := false
				if in.Expected == "" {
					expectedMatch = !ok
				} else if ok && current == in.Expected {
					expectedMatch = true
				}
				if expectedMatch {
					if !out.Swapped {
						return false, kv
					}
					next := cloneState(kv)
					if in.Desired == "" {
						delete(next, in.Key)
						if out.Found || out.Value != "" {
							return false, kv
						}
						return true, next
					}
					next[in.Key] = in.Desired
					if !out.Found || out.Value != in.Desired {
						return false, kv
					}
					return true, next
				}
				if out.Swapped {
					return false, kv
				}
				if out.Found != ok {
					return false, kv
				}
				if ok && out.Value != current {
					return false, kv
				}
				if !ok && out.Value != "" {
					return false, kv
				}
				return true, kv
			default:
				return true, kv
			}
		},
	}
}

func partitionByKeyMap(history []porcupine.Operation) map[string][]porcupine.Operation {
	if len(history) == 0 {
		return nil
	}
	groups := make(map[string][]porcupine.Operation)
	for _, op := range history {
		input, ok := op.Input.(opInput)
		if !ok {
			continue
		}
		groups[input.Key] = append(groups[input.Key], op)
	}
	return groups
}

func partitionByKey(history []porcupine.Operation) [][]porcupine.Operation {
	groups := partitionByKeyMap(history)
	if len(groups) == 0 {
		return nil
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([][]porcupine.Operation, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, groups[key])
	}
	return parts
}

func applyPartition(ctx context.Context, adminAddrs map[string]string, isolateID string) {
	for nodeID, addr := range adminAddrs {
		conn, adminClient, err := dialAdmin(addr, 500*time.Millisecond)
		if err != nil {
			continue
		}

		for dst := range adminAddrs {
			if dst == nodeID {
				continue
			}
			if nodeID != isolateID && dst != isolateID {
				continue
			}
			_, _ = adminClient.SetLinkRule(ctx, &pb.SetLinkRuleRequest{
				SrcNodeId: nodeID,
				DstNodeId: dst,
				DropPct:   100,
			})
		}

		_ = conn.Close()
	}
}

func clearLinkRules(ctx context.Context, adminAddrs map[string]string) {
	for _, addr := range adminAddrs {
		conn, adminClient, err := dialAdmin(addr, 500*time.Millisecond)
		if err != nil {
			continue
		}
		_, _ = adminClient.ClearLinkRules(ctx, &pb.ClearLinkRulesRequest{})
		_ = conn.Close()
	}
}

func setDiskStall(ctx context.Context, adminAddrs map[string]string, nodeID string, delayMs uint32) {
	addr := adminAddrs[nodeID]
	if addr == "" {
		return
	}
	conn, adminClient, err := dialAdmin(addr, 500*time.Millisecond)
	if err != nil {
		return
	}
	_, _ = adminClient.SetDiskStall(ctx, &pb.SetDiskStallRequest{FsyncDelayMs: delayMs})
	_ = conn.Close()
}

func updateLeaderFromResponse(leaderID string, resp interface{}, err error) string {
	if err != nil {
		return ""
	}

	var responseErr *pb.Error
	switch r := resp.(type) {
	case *pb.PutResponse:
		if r != nil {
			responseErr = r.Error
		}
	case *pb.GetResponse:
		if r != nil {
			responseErr = r.Error
		}
	case *pb.CASResponse:
		if r != nil {
			responseErr = r.Error
		}
	}

	if responseErr == nil {
		return leaderID
	}

	if responseErr.Code == pb.Code_NOT_LEADER {
		if responseErr.LeaderHint != nil && responseErr.LeaderHint.NodeId != "" {
			return responseErr.LeaderHint.NodeId
		}
		return ""
	}

	return leaderID
}

func pickLeader(ctx context.Context, pool *clientPool, leaderID string) (string, pb.KVClient, error) {
	if leaderID != "" {
		client, err := pool.client(leaderID)
		if err == nil {
			return leaderID, client, nil
		}
	}

	newLeaderID, client, err := pool.leader(ctx)
	if err != nil {
		return "", nil, err
	}
	return newLeaderID, client, nil
}
