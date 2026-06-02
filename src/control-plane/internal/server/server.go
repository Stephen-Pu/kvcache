// Package server implements the gRPC ControlPlane service defined in
// src/core/proto/cp.proto.
//
// Phase A-6 (LLD §4.1 / §5.1) — first cut: Register + Heartbeat are
// real (drive the existing membership.Registry → etcd path); Sync is
// stubbed at Unimplemented for now since its bidi-stream semantics
// touch quota + bloom-fanout subsystems that haven't been scaffolded
// yet (CP-internal/quota, CP-internal/routing). The service can be
// extended in place — the wiring + lifecycle is the part that didn't
// exist before.
//
// Lifecycle:
//   - main.go constructs one Server per process and registers it with
//     a grpc.Server bound to the same `--listen` port the HTTP probe
//     server already uses (cmux-style sharing isn't worth it yet;
//     we open a sibling listener instead).
//   - The Server is goroutine-safe — the embedded Registry already
//     serialises access to etcd internally.
package server

import (
	"context"
	"fmt"
	"sync/atomic"

	pb "github.com/Stephen-Pu/kvcache/control-plane/internal/pb"
	"github.com/Stephen-Pu/kvcache/control-plane/internal/membership"
)

// Server implements pb.ControlPlaneServer.
type Server struct {
	pb.UnimplementedControlPlaneServer

	registry  *membership.Registry
	clusterID string

	// Bumped on every Register so the operator can spot duplicate node
	// registrations in the logs without grepping for the rid.
	registerCount atomic.Uint64

	// Default lease TTL handed back to the node in HeartbeatResponse.
	// 15s matches the kvstore-node NodeRegistrar default; CP can shrink
	// it under cluster-wide pressure later.
	leaseTTLSec uint64
}

// Options carries server config the caller (main.go) controls.
type Options struct {
	Registry  *membership.Registry
	ClusterID string
	// Optional override; defaults to 15s when zero.
	LeaseTTLSec uint64
}

// New constructs a Server. Registry must be non-nil — Register and
// Heartbeat both go through it.
func New(opts Options) (*Server, error) {
	if opts.Registry == nil {
		return nil, fmt.Errorf("server.New: Registry is required")
	}
	ttl := opts.LeaseTTLSec
	if ttl == 0 {
		ttl = 15
	}
	cid := opts.ClusterID
	if cid == "" {
		cid = "kvcache" // placeholder until CRD-derived cluster ids land
	}
	return &Server{
		registry:    opts.Registry,
		clusterID:   cid,
		leaseTTLSec: ttl,
	}, nil
}

// Register is the node's join handshake. We translate the protobuf
// RegisterRequest into a membership.NodeDescriptor and let the existing
// registry publish it to etcd under /kvcache/nodes/<node_id> with a
// fresh lease.
func (s *Server) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if req == nil || req.GetNodeId() == "" {
		return nil, fmt.Errorf("Register: node_id is required")
	}
	n := membership.NodeDescriptor{
		NodeID:              req.GetNodeId(),
		Host:                req.GetHost(),
		Version:             req.GetVersion(),
		SupportedTransports: req.GetSupportedTransports(),
	}
	if c := req.GetCapacity(); c != nil {
		n.PinnedBytes = c.GetPinnedBytes()
		n.DramBytes = c.GetDramBytes()
		n.NvmeBytes = c.GetNvmeBytes()
		n.NumaNodes = c.GetNumaNodes()
		n.Vcpus = c.GetVcpus()
	}
	if _, err := s.registry.RegisterNode(ctx, n, int64(s.leaseTTLSec)); err != nil {
		return nil, fmt.Errorf("Register: etcd publish: %w", err)
	}
	s.registerCount.Add(1)
	return &pb.RegisterResponse{
		ClusterId:    s.clusterID,
		InitialEpoch: s.registerCount.Load(),
		// CaBundle stays empty in this phase — mTLS material is wired
		// in via Phase H-3's operator-managed Secret, not pushed via
		// Register. The field is part of the proto for future per-
		// node CA distribution (e.g. when the CP becomes an internal
		// CA itself in a SPIFFE rollout).
	}, nil
}

// Heartbeat just returns the lease TTL today. The node side already
// drives etcd lease keep-alives directly through its own client (the
// CP and node share an etcd, by design), so this RPC is effectively a
// "what's the current TTL" probe rather than a touch-the-lease verb.
// That's fine for now and matches the proto comment "lease_ttl_seconds";
// when we introduce CP-side authoritative leases (Phase A-6.1), the
// touch happens here.
func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if req == nil || req.GetNodeId() == "" {
		return nil, fmt.Errorf("Heartbeat: node_id is required")
	}
	_ = ctx
	return &pb.HeartbeatResponse{
		LeaseTtlSeconds: s.leaseTTLSec,
	}, nil
}

// RegisterCount is for tests / metrics; exposed so callers can assert
// the server actually saw a Register without reaching into etcd.
func (s *Server) RegisterCount() uint64 {
	return s.registerCount.Load()
}

// Sync is intentionally NOT overridden here — pb.UnimplementedControlPlaneServer
// returns codes.Unimplemented automatically, which is the right behaviour
// until the quota + bloom-fanout subsystems exist. A node calling Sync
// today gets a clean error it can fall back to direct-etcd-Watch from,
// which is exactly what kvstore-node's NodeRegistrar does anyway.
