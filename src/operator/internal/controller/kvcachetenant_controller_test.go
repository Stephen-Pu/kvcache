// KVCacheTenant controller — validation reconciler unit tests.
package controller

import (
	"context"
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
