// LLD §8.2 / §5.1 — Operator-side KVCacheTenant → etcd publisher.
//
// H-3 left the KVCacheTenant reconciler at validation only: it set a
// `Validated` condition and called it done. Phase H-4 closes the loop
// by actually shipping the validated tenant spec to etcd, where the
// kvstore-node and control-plane processes watch
// `/kvcache/tenants/<cluster>/<tenant_id>` for quota updates.
//
// Why operator-side and not CP-side: the operator already runs against
// the K8s API and dials etcd for the membership status read
// (etcd_status.go). Sharing the dial here keeps the
// "K8s-API-aware-process" footprint to one binary; the CP can stay a
// pure data-plane control plane and pick up tenants via etcd watch.
//
// Failure handling mirrors etcd_status.go: errors are returned but the
// reconciler treats them as transient — the next normal reconcile (or
// the periodic rotation tick) will retry.
package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	kvcachev1alpha1 "github.com/alluxio/kvcache/operator/api/v1alpha1"
)

// TenantPublisher writes one tenant's validated spec into the per-cluster
// etcd that kvstore-node / control-plane watch. Implementations:
//
//   * EtcdTenantPublisher — production; dials the cluster's etcd.
//   * Tests inject a fake to drive the Published condition path
//     without spinning a real etcd.
type TenantPublisher interface {
	Publish(ctx context.Context,
		cluster *kvcachev1alpha1.KVCacheCluster,
		tenant  *kvcachev1alpha1.KVCacheTenant) error
}

// TenantEtcdPayload is the JSON document written to
// /kvcache/tenants/<cluster>/<tenant_id>. Kept small and stable so the
// watchers don't break across operator upgrades.
type TenantEtcdPayload struct {
	TenantID                string `json:"tenant_id"`
	ClusterRef              string `json:"cluster_ref"`
	CapacityBytes           string `json:"capacity_bytes"`
	QPS                     uint32 `json:"qps"`
	BandwidthBytesPerSecond string `json:"bandwidth_bytes_per_second"`
	DefaultPriority         string `json:"default_priority"`
	DeletionPending         bool   `json:"deletion_pending,omitempty"`
	// SchemaVersion lets the watcher reject documents from a newer
	// operator that added required fields. Bumped on incompatible
	// payload changes.
	SchemaVersion int `json:"schema_version"`
}

const tenantEtcdSchemaVersion = 1

// TenantEtcdKey returns the per-cluster, per-tenant etcd key.
func TenantEtcdKey(clusterName, tenantID string) string {
	return fmt.Sprintf("/kvcache/tenants/%s/%s", clusterName, tenantID)
}

// EtcdTenantPublisher dials the cluster's etcd headless Service (same
// endpoint discovery as EtcdMemberCounter) and writes the tenant key.
type EtcdTenantPublisher struct {
	// DialTimeout is the per-call timeout for dial + put. Kept tight
	// so a flaky etcd never blocks the reconcile loop.
	DialTimeout time.Duration
}

var _ TenantPublisher = (*EtcdTenantPublisher)(nil)

func (p *EtcdTenantPublisher) Publish(ctx context.Context,
	cluster *kvcachev1alpha1.KVCacheCluster,
	tenant  *kvcachev1alpha1.KVCacheTenant) error {

	timeout := p.DialTimeout
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	endpoints := EtcdEndpointsFor(cluster)
	if len(endpoints) == 0 {
		return fmt.Errorf("etcd endpoints unavailable for cluster %q",
			cluster.Name)
	}
	bare := make([]string, 0, len(endpoints))
	for _, e := range endpoints {
		bare = append(bare, stripScheme(e))
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   bare,
		DialTimeout: timeout,
	})
	if err != nil {
		return fmt.Errorf("etcd dial: %w", err)
	}
	defer cli.Close()

	payload := TenantEtcdPayload{
		TenantID:                tenant.Spec.TenantID,
		ClusterRef:              tenant.Spec.ClusterRef,
		CapacityBytes:           tenant.Spec.Quota.CapacityBytes,
		QPS:                     tenant.Spec.Quota.QPS,
		BandwidthBytesPerSecond: tenant.Spec.Quota.BandwidthBytesPerSecond,
		DefaultPriority:         tenant.Spec.DefaultPriority,
		DeletionPending:         tenant.Spec.DeletionPending,
		SchemaVersion:           tenantEtcdSchemaVersion,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	_, err = cli.Put(cctx,
		TenantEtcdKey(cluster.Name, tenant.Spec.TenantID),
		string(body))
	if err != nil {
		return fmt.Errorf("etcd put: %w", err)
	}
	return nil
}

// stripScheme is declared in etcd_status.go — same package, so we
// pick it up without import.
