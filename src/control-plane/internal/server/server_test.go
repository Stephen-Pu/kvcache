// Phase A-6 — ControlPlane gRPC server tests.
//
// Stands up an in-process embedded etcd (same helper shape the
// membership package uses), plus a gRPC server over a bufconn so the
// test has zero external dependencies. Verifies:
//
//   1. Register publishes the node into etcd via Registry (asserted by
//      observing it through ListNodes).
//   2. RegisterResponse carries a non-empty cluster_id and a monotonic
//      initial_epoch.
//   3. Heartbeat returns the configured lease TTL.
//   4. Sync returns codes.Unimplemented today (Phase A-6 limitation —
//      intentional, exercised so a future implementation that
//      accidentally returns OK without doing the work gets noticed).
//   5. Register rejects an empty node_id.
package server_test

import (
	"context"
	"net"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	myetcd "github.com/Stephen-Pu/kvcache/control-plane/internal/etcd"
	"github.com/Stephen-Pu/kvcache/control-plane/internal/membership"
	pb "github.com/Stephen-Pu/kvcache/control-plane/internal/pb"
	cpserver "github.com/Stephen-Pu/kvcache/control-plane/internal/server"
)

// ---- fixtures --------------------------------------------------------

func startEmbeddedEtcd(t *testing.T) (string, func()) {
	t.Helper()
	dir := t.TempDir()
	cfg := embed.NewConfig()
	cfg.Dir = filepath.Join(dir, "etcd")
	clientURL, _ := url.Parse("http://127.0.0.1:0")
	peerURL, _ := url.Parse("http://127.0.0.1:0")
	cfg.ListenClientUrls = []url.URL{*clientURL}
	cfg.AdvertiseClientUrls = []url.URL{*clientURL}
	cfg.ListenPeerUrls = []url.URL{*peerURL}
	cfg.AdvertisePeerUrls = []url.URL{*peerURL}
	cfg.InitialCluster = "default=" + peerURL.String()
	cfg.LogLevel = "error"

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("embed etcd: %v", err)
	}
	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(20 * time.Second):
		e.Close()
		t.Fatal("embed etcd: not ready in 20s")
	}
	return e.Clients[0].Addr().String(), func() { e.Close() }
}

// startGrpc returns a wired client + a stop func. Server runs over a
// bufconn (in-memory), so no real port is bound.
func startGrpc(t *testing.T, reg *membership.Registry) (pb.ControlPlaneClient, *cpserver.Server, func()) {
	t.Helper()
	svc, err := cpserver.New(cpserver.Options{
		Registry:    reg,
		ClusterID:   "test-cluster",
		LeaseTTLSec: 12,
	})
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterControlPlaneServer(srv, svc)

	lis := bufconn.Listen(1 << 16)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		t.Fatalf("grpc dial: %v", err)
	}
	cli := pb.NewControlPlaneClient(conn)
	return cli, svc, func() {
		_ = conn.Close()
		srv.GracefulStop()
	}
}

// ---- tests -----------------------------------------------------------

func TestServer_Register_PublishesNodeToEtcd(t *testing.T) {
	ep, stopEtcd := startEmbeddedEtcd(t)
	defer stopEtcd()
	cli, err := myetcd.New(myetcd.Config{Endpoints: []string{ep}})
	if err != nil {
		t.Fatalf("dial etcd: %v", err)
	}
	defer cli.Close()
	reg := membership.NewRegistry(cli)

	rpc, svc, stop := startGrpc(t, reg)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := rpc.Register(ctx, &pb.RegisterRequest{
		NodeId:               "node-a",
		Host:                 "10.0.0.1",
		Version:              "test",
		SupportedTransports:  []string{"tcp"},
		Capacity: &pb.NodeCapacity{
			PinnedBytes: 1 << 30,
			DramBytes:   2 << 30,
			NvmeBytes:   100 << 30,
			Vcpus:       8,
		},
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if resp.GetClusterId() != "test-cluster" {
		t.Errorf("cluster_id = %q, want test-cluster", resp.GetClusterId())
	}
	if resp.GetInitialEpoch() != 1 {
		t.Errorf("initial_epoch = %d, want 1 for first register", resp.GetInitialEpoch())
	}
	if got := svc.RegisterCount(); got != 1 {
		t.Errorf("RegisterCount = %d, want 1", got)
	}

	// Observe the node via the same Registry the server published into.
	nodes, err := reg.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	var found bool
	for _, n := range nodes {
		if n.NodeID == "node-a" {
			found = true
			if n.Host != "10.0.0.1" {
				t.Errorf("Host = %q, want 10.0.0.1", n.Host)
			}
			if n.PinnedBytes != (1 << 30) {
				t.Errorf("PinnedBytes = %d, want %d", n.PinnedBytes, 1<<30)
			}
		}
	}
	if !found {
		t.Errorf("node-a not visible via ListNodes; got %d nodes", len(nodes))
	}
}

func TestServer_Register_MonotonicEpochAcrossCalls(t *testing.T) {
	ep, stopEtcd := startEmbeddedEtcd(t)
	defer stopEtcd()
	cli, _ := myetcd.New(myetcd.Config{Endpoints: []string{ep}})
	defer cli.Close()
	reg := membership.NewRegistry(cli)
	rpc, _, stop := startGrpc(t, reg)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, nid := range []string{"a", "b", "c"} {
		resp, err := rpc.Register(ctx, &pb.RegisterRequest{NodeId: nid, Host: "h"})
		if err != nil {
			t.Fatalf("Register %s: %v", nid, err)
		}
		wantEpoch := uint64(i + 1)
		if resp.GetInitialEpoch() != wantEpoch {
			t.Errorf("Register %s: initial_epoch = %d, want %d", nid, resp.GetInitialEpoch(), wantEpoch)
		}
	}
}

func TestServer_Heartbeat_ReturnsConfiguredTTL(t *testing.T) {
	ep, stopEtcd := startEmbeddedEtcd(t)
	defer stopEtcd()
	cli, _ := myetcd.New(myetcd.Config{Endpoints: []string{ep}})
	defer cli.Close()
	reg := membership.NewRegistry(cli)
	rpc, _, stop := startGrpc(t, reg)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := rpc.Heartbeat(ctx, &pb.HeartbeatRequest{NodeId: "node-a", Epoch: 1})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if resp.GetLeaseTtlSeconds() != 12 {
		t.Errorf("lease_ttl_seconds = %d, want 12 (configured)", resp.GetLeaseTtlSeconds())
	}
}

func TestServer_Register_RejectsEmptyNodeID(t *testing.T) {
	ep, stopEtcd := startEmbeddedEtcd(t)
	defer stopEtcd()
	cli, _ := myetcd.New(myetcd.Config{Endpoints: []string{ep}})
	defer cli.Close()
	reg := membership.NewRegistry(cli)
	rpc, _, stop := startGrpc(t, reg)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rpc.Register(ctx, &pb.RegisterRequest{NodeId: ""}); err == nil {
		t.Fatal("Register with empty node_id: want error, got nil")
	}
}

func TestServer_Sync_StaysUnimplemented(t *testing.T) {
	// Sync is intentionally not wired in Phase A-6. This test makes sure
	// the next person who tries to "implement Sync" can't accidentally
	// ship a no-op success — the explicit assertion fails if they do.
	ep, stopEtcd := startEmbeddedEtcd(t)
	defer stopEtcd()
	cli, _ := myetcd.New(myetcd.Config{Endpoints: []string{ep}})
	defer cli.Close()
	reg := membership.NewRegistry(cli)
	rpc, _, stop := startGrpc(t, reg)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := rpc.Sync(ctx)
	if err != nil {
		t.Fatalf("Sync open: %v", err)
	}
	// The Unimplemented error surfaces on the first send/recv, not at
	// stream-open time. Trigger it by recv'ing and assert the code.
	if _, err := stream.Recv(); err == nil {
		t.Fatal("Sync.Recv: want Unimplemented error, got nil")
	} else if st, ok := status.FromError(err); !ok || st.Code() != codes.Unimplemented {
		t.Fatalf("Sync.Recv: want code Unimplemented, got %v", err)
	}
}

func TestServer_New_RejectsNilRegistry(t *testing.T) {
	if _, err := cpserver.New(cpserver.Options{}); err == nil {
		t.Fatal("server.New with nil Registry: want error, got nil")
	}
}
