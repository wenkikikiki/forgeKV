//go:build chaos
// +build chaos

// Package chaos provides chaos testing with linearizability verification.
package chaos

import (
	"context"
	"testing"
	"time"

	pb "github.com/forgekv/forgekv/api/forgekv"
)

func TestLinearizabilityBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	workload := defaultWorkload()
	workload.CasPct = 0
	workload.PutPct = 50
	workload.GetPct = 50

	runScenario(t, scenarioConfig{
		Name:     "basic",
		Duration: 20 * time.Second,
		Workload: workload,
		ErrorBudget: errorBudget{
			MaxErrorRate:    0,
			MaxRPCErrorRate: 0,
		},
		CheckRegister: true,
		CheckCAS:      false,
	})
}

func TestLinearizabilityCASBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	runScenario(t, scenarioConfig{
		Name:     "cas-basic",
		Duration: 20 * time.Second,
		Workload: defaultWorkload(),
		ErrorBudget: errorBudget{
			MaxErrorRate:    0,
			MaxRPCErrorRate: 0,
		},
		CheckRegister: false,
		CheckCAS:      true,
	})
}

func TestLinearizabilityWithPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	allowed := map[pb.Code]bool{
		pb.Code_NOT_LEADER: true,
		pb.Code_NO_QUORUM:  true,
		pb.Code_OVERLOADED: true,
	}

	runScenario(t, scenarioConfig{
		Name:     "partition",
		Duration: 60 * time.Second,
		Workload: defaultWorkload(),
		Fault:    partitionFault,
		ErrorBudget: errorBudget{
			MaxErrorRate:    0.35,
			MaxRPCErrorRate: 0.02,
			AllowedCodes:    allowed,
		},
		CheckRegister: false,
		CheckCAS:      true,
	})
}

func TestLinearizabilityWithDiskStall(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	allowed := map[pb.Code]bool{
		pb.Code_NOT_LEADER: true,
		pb.Code_NO_QUORUM:  true,
		pb.Code_OVERLOADED: true,
	}

	workload := defaultWorkload()
	workload.OpInterval = 250 * time.Millisecond

	runScenario(t, scenarioConfig{
		Name:     "disk-stall",
		Duration: 40 * time.Second,
		Workload: workload,
		Fault:    diskStallFault,
		ErrorBudget: errorBudget{
			MaxErrorRate:    0.2,
			MaxRPCErrorRate: 0.02,
			AllowedCodes:    allowed,
		},
		CheckRegister: false,
		CheckCAS:      true,
	})
}

func partitionFault(ctx context.Context, pool *clientPool, cluster *chaosCluster) {
	defer clearLinkRules(ctx, cluster.adminAddrs)

	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
	}

	for i := 0; i < 3; i++ {
		leaderID, _, err := pool.leader(ctx)
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		applyPartition(ctx, cluster.adminAddrs, leaderID)
		time.Sleep(5 * time.Second)
		clearLinkRules(ctx, cluster.adminAddrs)

		select {
		case <-ctx.Done():
			return
		case <-time.After(8 * time.Second):
		}
	}
}

func diskStallFault(ctx context.Context, pool *clientPool, cluster *chaosCluster) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
	}

	leaderID, _, err := pool.leader(ctx)
	if err != nil {
		return
	}

	setDiskStall(ctx, cluster.adminAddrs, leaderID, 150)
	select {
	case <-ctx.Done():
		setDiskStall(context.Background(), cluster.adminAddrs, leaderID, 0)
		return
	case <-time.After(8 * time.Second):
	}
	setDiskStall(ctx, cluster.adminAddrs, leaderID, 0)
}
