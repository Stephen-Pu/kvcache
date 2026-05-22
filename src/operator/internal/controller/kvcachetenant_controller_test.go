// KVCacheTenant controller — validation reconciler unit tests.
package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kvcachev1alpha1 "github.com/alluxio/kvcache/operator/api/v1alpha1"
)

func sampleTenant(name, clusterRef string) *kvcachev1alpha1.KVCacheTenant {
	return &kvcachev1alpha1.KVCacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "kvcache"},
		Spec: kvcachev1alpha1.KVCacheTenantSpec{
			ClusterRef: clusterRef,
			TenantID:   "0123456789abcdef0123456789abcdef",
			Quota: kvcachev1alpha1.QuotaSpec{
				CapacityBytes:           "100Gi",
				QPS:                     500,
				BandwidthBytesPerSecond: "1Gi",
			},
			DefaultPriority: "P1",
		},
	}
}

func newTenantReconciler(t *testing.T, objs ...client.Object) (*KVCacheTenantReconciler, client.Client) {
	t.Helper()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kvcachev1alpha1.KVCacheTenant{},
			&kvcachev1alpha1.KVCacheCluster{}).
		WithObjects(objs...).
		Build()
	return &KVCacheTenantReconciler{Client: cli, Scheme: scheme}, cli
}

func reconcileTenant(t *testing.T, r *KVCacheTenantReconciler, ten *kvcachev1alpha1.KVCacheTenant) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: ten.Name, Namespace: ten.Namespace},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

func getValidatedCondition(t *testing.T, cli client.Client,
	name, ns string) *metav1.Condition {
	t.Helper()
	var live kvcachev1alpha1.KVCacheTenant
	if err := cli.Get(context.Background(),
		client.ObjectKey{Name: name, Namespace: ns}, &live); err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	for i := range live.Status.Conditions {
		if live.Status.Conditions[i].Type == validatedConditionType {
			return &live.Status.Conditions[i]
		}
	}
	return nil
}

func TestTenantValidatedWhenSpecAndClusterAreOK(t *testing.T) {
	cluster := sampleCluster()
	tenant := sampleTenant("acme", cluster.Name)
	r, cli := newTenantReconciler(t, cluster, tenant)
	reconcileTenant(t, r, tenant)

	c := getValidatedCondition(t, cli, tenant.Name, tenant.Namespace)
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("expected Validated=True, got %+v", c)
	}
}

func TestTenantFailsValidationOnMissingCluster(t *testing.T) {
	tenant := sampleTenant("acme", "nonexistent")
	r, cli := newTenantReconciler(t, tenant)
	reconcileTenant(t, r, tenant)

	c := getValidatedCondition(t, cli, tenant.Name, tenant.Namespace)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "ClusterNotFound" {
		t.Errorf("expected Validated=False ClusterNotFound, got %+v", c)
	}
}

func TestTenantFailsValidationOnBadTenantID(t *testing.T) {
	cluster := sampleCluster()
	tenant := sampleTenant("acme", cluster.Name)
	tenant.Spec.TenantID = "not-hex"
	r, cli := newTenantReconciler(t, cluster, tenant)
	reconcileTenant(t, r, tenant)

	c := getValidatedCondition(t, cli, tenant.Name, tenant.Namespace)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "InvalidTenantID" {
		t.Errorf("expected Validated=False InvalidTenantID, got %+v", c)
	}
}

func TestTenantFailsValidationOnBadQuota(t *testing.T) {
	cluster := sampleCluster()
	tenant := sampleTenant("acme", cluster.Name)
	tenant.Spec.Quota.CapacityBytes = "100Banana"
	r, cli := newTenantReconciler(t, cluster, tenant)
	reconcileTenant(t, r, tenant)

	c := getValidatedCondition(t, cli, tenant.Name, tenant.Namespace)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "InvalidQuota" {
		t.Errorf("expected Validated=False InvalidQuota, got %+v", c)
	}
}

func TestTenantFailsValidationOnBadPriority(t *testing.T) {
	cluster := sampleCluster()
	tenant := sampleTenant("acme", cluster.Name)
	tenant.Spec.DefaultPriority = "P9"
	r, cli := newTenantReconciler(t, cluster, tenant)
	reconcileTenant(t, r, tenant)

	c := getValidatedCondition(t, cli, tenant.Name, tenant.Namespace)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "InvalidPriority" {
		t.Errorf("expected Validated=False InvalidPriority, got %+v", c)
	}
}

func TestTenantValidationIsIdempotent(t *testing.T) {
	cluster := sampleCluster()
	tenant := sampleTenant("acme", cluster.Name)
	r, _ := newTenantReconciler(t, cluster, tenant)
	reconcileTenant(t, r, tenant)
	reconcileTenant(t, r, tenant) // must not error on no-op pass
}

func TestTenantValidatedDefaultsEmptyPriority(t *testing.T) {
	cluster := sampleCluster()
	tenant := sampleTenant("acme", cluster.Name)
	tenant.Spec.DefaultPriority = ""
	r, cli := newTenantReconciler(t, cluster, tenant)
	reconcileTenant(t, r, tenant)

	c := getValidatedCondition(t, cli, tenant.Name, tenant.Namespace)
	if c == nil || c.Status != metav1.ConditionTrue {
		t.Errorf("expected Validated=True for empty default priority, got %+v", c)
	}
}

// ---- Phase H-4 B: tenant publisher --------------------------------------

// fakeTenantPublisher records every Publish call. Optionally fails the
// next call to exercise the error/retry path.
type fakeTenantPublisher struct {
	mu       sync.Mutex
	calls    []TenantEtcdPayload
	nextErr  error
}

func (f *fakeTenantPublisher) Publish(_ context.Context,
	cluster *kvcachev1alpha1.KVCacheCluster,
	tenant  *kvcachev1alpha1.KVCacheTenant) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.nextErr; err != nil {
		f.nextErr = nil
		return err
	}
	f.calls = append(f.calls, TenantEtcdPayload{
		TenantID:                tenant.Spec.TenantID,
		ClusterRef:              tenant.Spec.ClusterRef,
		CapacityBytes:           tenant.Spec.Quota.CapacityBytes,
		QPS:                     tenant.Spec.Quota.QPS,
		BandwidthBytesPerSecond: tenant.Spec.Quota.BandwidthBytesPerSecond,
		DefaultPriority:         tenant.Spec.DefaultPriority,
		SchemaVersion:           1,
	})
	return nil
}

func getCondition(t *testing.T, cli client.Client, name, ns, typ string) *metav1.Condition {
	t.Helper()
	var live kvcachev1alpha1.KVCacheTenant
	if err := cli.Get(context.Background(),
		client.ObjectKey{Name: name, Namespace: ns}, &live); err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	for i := range live.Status.Conditions {
		if live.Status.Conditions[i].Type == typ {
			return &live.Status.Conditions[i]
		}
	}
	return nil
}

func TestValidTenantPublishesToEtcd(t *testing.T) {
	cluster := sampleCluster()
	tenant := sampleTenant("t-publish", cluster.Name)
	pub := &fakeTenantPublisher{}
	r, cli := newTenantReconciler(t, cluster, tenant)
	r.Publisher = pub
	reconcileTenant(t, r, tenant)

	if got := len(pub.calls); got != 1 {
		t.Fatalf("expected 1 Publish call, got %d", got)
	}
	if pub.calls[0].TenantID != tenant.Spec.TenantID {
		t.Errorf("payload TenantID = %q, want %q",
			pub.calls[0].TenantID, tenant.Spec.TenantID)
	}
	pubCond := getCondition(t, cli, tenant.Name, tenant.Namespace, publishedConditionType)
	if pubCond == nil || pubCond.Status != metav1.ConditionTrue {
		t.Errorf("expected Published=True, got %+v", pubCond)
	}
}

func TestInvalidTenantDoesNotPublish(t *testing.T) {
	cluster := sampleCluster()
	tenant := sampleTenant("t-publish", cluster.Name)
	tenant.Spec.TenantID = "not-hex"  // forces InvalidTenantID
	pub := &fakeTenantPublisher{}
	r, _ := newTenantReconciler(t, cluster, tenant)
	r.Publisher = pub
	reconcileTenant(t, r, tenant)

	if got := len(pub.calls); got != 0 {
		t.Errorf("invalid tenant should not be published, got %d calls", got)
	}
}

func TestPublishFailureSurfacesConditionAndRequeues(t *testing.T) {
	cluster := sampleCluster()
	tenant := sampleTenant("t-publish", cluster.Name)
	pub := &fakeTenantPublisher{nextErr: errors.New("etcd unreachable")}
	r, cli := newTenantReconciler(t, cluster, tenant)
	r.Publisher = pub

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: tenant.Name, Namespace: tenant.Namespace},
	})
	if err != nil {
		t.Fatalf("reconcile returned error (should surface via condition): %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected RequeueAfter on publish failure, got %+v", res)
	}
	pubCond := getCondition(t, cli, tenant.Name, tenant.Namespace, publishedConditionType)
	if pubCond == nil || pubCond.Status != metav1.ConditionFalse {
		t.Errorf("expected Published=False after error, got %+v", pubCond)
	}
	if pubCond != nil && !strings.Contains(pubCond.Message, "etcd unreachable") {
		t.Errorf("expected error message to mention 'etcd unreachable', got %q",
			pubCond.Message)
	}
}

func TestPublishRetrySucceedsOnNextReconcile(t *testing.T) {
	cluster := sampleCluster()
	tenant := sampleTenant("t-publish", cluster.Name)
	pub := &fakeTenantPublisher{nextErr: errors.New("transient")}
	r, cli := newTenantReconciler(t, cluster, tenant)
	r.Publisher = pub
	reconcileTenant(t, r, tenant)
	// First pass failed → Published=False. Refetch and reconcile again
	// with no injected error.
	reconcileTenant(t, r, tenant)
	if got := len(pub.calls); got != 1 {
		t.Fatalf("expected 1 successful Publish after retry, got %d", got)
	}
	pubCond := getCondition(t, cli, tenant.Name, tenant.Namespace, publishedConditionType)
	if pubCond == nil || pubCond.Status != metav1.ConditionTrue {
		t.Errorf("expected Published=True after retry, got %+v", pubCond)
	}
}

func TestTenantEtcdKeyShape(t *testing.T) {
	// Lock the key format so downstream watchers can rely on it.
	got := TenantEtcdKey("demo", "deadbeefcafebabe1234567890abcdef")
	want := "/kvcache/tenants/demo/deadbeefcafebabe1234567890abcdef"
	if got != want {
		t.Errorf("TenantEtcdKey = %q, want %q", got, want)
	}
}
