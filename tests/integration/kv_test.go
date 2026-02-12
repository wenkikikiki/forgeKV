//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/forgekv/forgekv/api/forgekv"
)

func TestStatusAny(t *testing.T) {
	conn, client, err := dial(cluster.clientAddrs["n1"], 2*time.Second)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Status(ctx, &pb.StatusRequest{})
	if err != nil {
		t.Fatalf("status failed: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != pb.Code_OK {
		t.Fatalf("expected OK status, got %+v", resp.Error)
	}
}

func TestPutGetDeleteCAS(t *testing.T) {
	_, leaderAddr := findLeader(t)
	conn, client, err := dial(leaderAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial leader failed: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	putResp, err := client.Put(ctx, &pb.PutRequest{
		Key:      []byte("int-key"),
		Value:    []byte("v1"),
		ClientId: "int-client",
		Seq:      1,
	})
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}
	if putResp.Error.Code != pb.Code_OK {
		t.Fatalf("expected OK put, got %v", putResp.Error.Code)
	}

	getResp, err := client.Get(ctx, &pb.GetRequest{Key: []byte("int-key")})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !getResp.Found || string(getResp.Value) != "v1" {
		t.Fatalf("unexpected get result: found=%v value=%s", getResp.Found, string(getResp.Value))
	}

	casResp, err := client.CAS(ctx, &pb.CASRequest{
		Key:      []byte("int-key"),
		Expected: []byte("v1"),
		Desired:  []byte("v2"),
		ClientId: "int-client",
		Seq:      2,
	})
	if err != nil {
		t.Fatalf("cas failed: %v", err)
	}
	if casResp.Error.Code != pb.Code_OK || !casResp.Swapped {
		t.Fatalf("expected cas swap OK, got code=%v swapped=%v", casResp.Error.Code, casResp.Swapped)
	}

	delResp, err := client.Delete(ctx, &pb.DeleteRequest{
		Key:      []byte("int-key"),
		ClientId: "int-client",
		Seq:      3,
	})
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if delResp.Error.Code != pb.Code_OK {
		t.Fatalf("expected delete OK, got %v", delResp.Error.Code)
	}

	getResp, err = client.Get(ctx, &pb.GetRequest{Key: []byte("int-key")})
	if err != nil {
		t.Fatalf("get after delete failed: %v", err)
	}
	if getResp.Found {
		t.Fatalf("expected key to be deleted")
	}
}

func TestLeaderRedirect(t *testing.T) {
	leaderID, leaderAddr := findLeader(t)
	var followerAddr string
	for nodeID, addr := range cluster.clientAddrs {
		if nodeID != leaderID {
			followerAddr = addr
			break
		}
	}
	if followerAddr == "" {
		t.Fatalf("no follower address found")
	}

	conn, client, err := dial(followerAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial follower failed: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Put(ctx, &pb.PutRequest{
		Key:      []byte("redirect-key"),
		Value:    []byte("v"),
		ClientId: "redirect-client",
		Seq:      1,
	})
	if err != nil {
		t.Fatalf("put on follower failed: %v", err)
	}
	if resp.Error.Code != pb.Code_NOT_LEADER {
		t.Fatalf("expected NOT_LEADER, got %v", resp.Error.Code)
	}
	if resp.Error.LeaderHint == nil {
		t.Fatalf("expected leader hint, got nil")
	}
	if resp.Error.LeaderHint.NodeId != leaderID {
		t.Fatalf("expected leader hint node %s, got %s", leaderID, resp.Error.LeaderHint.NodeId)
	}
	if resp.Error.LeaderHint.ClientAddr != leaderAddr {
		t.Fatalf("expected leader hint addr %s, got %s", leaderAddr, resp.Error.LeaderHint.ClientAddr)
	}
}

func TestIdempotencyReplay(t *testing.T) {
	_, leaderAddr := findLeader(t)
	conn, client, err := dial(leaderAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial leader failed: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	first, err := client.Put(ctx, &pb.PutRequest{
		Key:      []byte("idem-key"),
		Value:    []byte("v1"),
		ClientId: "idem-client",
		Seq:      1,
	})
	if err != nil {
		t.Fatalf("first put failed: %v", err)
	}
	if first.Error.Code != pb.Code_OK {
		t.Fatalf("expected OK from first put, got %v", first.Error.Code)
	}

	dup, err := client.Put(ctx, &pb.PutRequest{
		Key:      []byte("idem-key"),
		Value:    []byte("v2"),
		ClientId: "idem-client",
		Seq:      1,
	})
	if err != nil {
		t.Fatalf("duplicate put failed: %v", err)
	}
	if dup.Error.Code != pb.Code_OK {
		t.Fatalf("expected OK from duplicate put, got %v", dup.Error.Code)
	}

	getResp, err := client.Get(ctx, &pb.GetRequest{Key: []byte("idem-key")})
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if !getResp.Found || string(getResp.Value) != "v1" {
		t.Fatalf("expected original value v1, got found=%v value=%s", getResp.Found, string(getResp.Value))
	}
}

func TestDuplicateInflightRequests(t *testing.T) {
	_, leaderAddr := findLeader(t)
	conn, client, err := dial(leaderAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial leader failed: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	errCh := make(chan error, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			resp, err := client.Put(ctx, &pb.PutRequest{
				Key:      []byte("dup-key"),
				Value:    []byte("dup-val"),
				ClientId: "dup-client",
				Seq:      1,
			})
			if err != nil {
				errCh <- err
				return
			}
			if resp.Error.Code != pb.Code_OK {
				errCh <- fmt.Errorf("unexpected code %v", resp.Error.Code)
				return
			}
			errCh <- nil
		}()
	}

	close(start)

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("duplicate inflight request failed: %v", err)
		}
	}
}

func TestInvalidArguments(t *testing.T) {
	_, leaderAddr := findLeader(t)
	conn, client, err := dial(leaderAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial leader failed: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.Put(ctx, &pb.PutRequest{
		Key:      []byte("bad-key"),
		Value:    []byte("bad-val"),
		ClientId: "",
		Seq:      1,
	})
	if err != nil {
		t.Fatalf("put missing client_id failed: %v", err)
	}
	if resp.Error.Code != pb.Code_INVALID_ARGUMENT {
		t.Fatalf("expected INVALID_ARGUMENT for missing client_id, got %v", resp.Error.Code)
	}

	resp, err = client.Put(ctx, &pb.PutRequest{
		Key:      []byte("bad-key-2"),
		Value:    []byte("bad-val-2"),
		ClientId: "bad-client",
		Seq:      0,
	})
	if err != nil {
		t.Fatalf("put missing seq failed: %v", err)
	}
	if resp.Error.Code != pb.Code_INVALID_ARGUMENT {
		t.Fatalf("expected INVALID_ARGUMENT for missing seq, got %v", resp.Error.Code)
	}
}
