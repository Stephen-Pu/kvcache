// LLD §4.1 — Operator-side membership query.
//
// The KVCacheCluster reconciler can't get an authoritative member count
// from the K8s API alone — pods can be Ready but not yet registered in
// etcd, and a pod that's terminating may still hold a valid lease. The
// real source of truth is etcd's `/nodes/` prefix, populated by the
// kvstore-node side via the membership FSM (see
// `kvstore-node/src/cluster/membership_fsm.h`).
//
// The reconciler resolves a `MemberCounter` and asks it for the current
// per-cluster counts; the value lands in `.status.nodesActive` etc.
// Failures (etcd unreachable, slow, etc.) are logged and zeroed out —
// the reconciler does not requeue on them, since the next normal
// reconcile will retry. The Phase H-3 production breakdown is just
// "active" (key exists with a valid lease); the joining / draining /
// unreachable split waits for the membership FSM to write a `state`
// field into the value (Phase H-4).
package controller

import (
	"context"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	kvcachev1alpha1 "github.com/alluxio/kvcache/operator/api/v1alpha1"
)

// MemberCounts is the per-cluster snapshot the reconciler wants. Zero
// values mean "not yet observed"; the reconciler treats them the same
// as the K8s-side StatefulSet ReadyReplicas fallback.
type MemberCounts struct {
	Active      int32
	Joining     int32
	Draining    int32
	Unreachable int32
}

// MemberCounter is the abstraction the reconciler uses to read the
// per-cluster etcd `/nodes/` prefix. Implementations:
//
//   * EtcdMemberCounter — production; dials the in-cluster etcd Service.
//   * fakeMemberCounter — used by unit tests (see *_test.go).
type MemberCounter interface {
	Count(ctx context.Context, cluster *kvcachev1alpha1.KVCacheCluster) (MemberCounts, error)
}

// EtcdMemberCounter dials etcd via the cluster's headless Service DNS
// (or the user-provided EtcdEndpoints under BYO etcd) and counts the
// number of keys under `/nodes/`.
type EtcdMemberCounter struct {
	// DialTimeout is the per-call timeout for both dial and the range
	// read. Kept tight so a flaky etcd never blocks the reconcile loop.
	DialTimeout time.Duration
}

// Compile-time interface check.
var _ MemberCounter = (*EtcdMemberCounter)(nil)

// Count returns active member count from etcd. Returns zeros + nil
// when etcd is unreachable so the reconciler can fall back gracefully.
func (e *EtcdMemberCounter) Count(ctx context.Context,
	cluster *kvcachev1alpha1.KVCacheCluster) (MemberCounts, error) {
	timeout := e.DialTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	endpoints := EtcdEndpointsFor(cluster)
	if len(endpoints) == 0 {
		return MemberCounts{}, nil
	}
	// Strip the http:// scheme — clientv3 takes host:port.
	bare := make([]string, 0, len(endpoints))
	for _, e := range endpoints {
		bare = append(bare, stripScheme(e))
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   bare,
		DialTimeout: timeout,
	})
	if err != nil {
		return MemberCounts{}, nil  // best effort; reconciler falls back to STS counts
	}
	defer cli.Close()

	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	resp, err := cli.Get(cctx, "/nodes/", clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return MemberCounts{}, nil
	}
	return MemberCounts{Active: int32(len(resp.Kvs))}, nil
}

func stripScheme(s string) string {
	for _, p := range []string{"https://", "http://"} {
		if strings.HasPrefix(s, p) {
			return s[len(p):]
		}
	}
	return s
}
