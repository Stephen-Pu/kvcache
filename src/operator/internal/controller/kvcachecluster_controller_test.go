// LLD §8.2 — KVCacheCluster controller unit tests.
//
// We drive Reconcile against a controller-runtime fake client (no
// envtest, no real kube-apiserver). That covers the desired-state
// fan-out logic without paying for a kube-apiserver dependency in the
// developer build; integration tests against a real kind cluster live
// in tests/e2e (Phase H-2).
package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kvcachev1alpha1 "github.com/alluxio/kvcache/operator/api/v1alpha1"
)

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kvcachev1alpha1.AddToScheme(scheme))
	return scheme
}

func sampleCluster() *kvcachev1alpha1.KVCacheCluster {
	return &kvcachev1alpha1.KVCacheCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo",
			Namespace: "kvcache",
		},
		Spec: kvcachev1alpha1.KVCacheClusterSpec{
			NodeReplicas: 3,
			Image:        "ghcr.io/alluxio/kvcache:dev",
			NixlBackend:  "tcp",
			Tier: kvcachev1alpha1.TierSpec{
				PinnedBytes: "32Gi",
				DramBytes:   "128Gi",
				NvmePath:    "/var/lib/kvcache/nvme.bin",
				NvmeBytes:   "1Ti",
			},
		},
	}
}

func newReconciler(t *testing.T, objs ...client.Object) (*KVCacheClusterReconciler, client.Client) {
	t.Helper()
	scheme := newScheme(t)
	cli := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&kvcachev1alpha1.KVCacheCluster{}).
		WithObjects(objs...).
		Build()
	return &KVCacheClusterReconciler{Client: cli, Scheme: scheme}, cli
}

func reconcileOnce(t *testing.T, r *KVCacheClusterReconciler, cluster *kvcachev1alpha1.KVCacheCluster) {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
}

func TestReconcileMissingClusterIsNoop(t *testing.T) {
	r, _ := newReconciler(t)
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "missing", Namespace: "default"},
	})
	if err != nil {
		t.Fatalf("Reconcile on missing cluster should not error, got %v", err)
	}
}

func TestReconcileEmitsAllResources(t *testing.T) {
	cluster := sampleCluster()
	r, cli := newReconciler(t, cluster)
	reconcileOnce(t, r, cluster)

	ctx := context.Background()
	mustExist := func(obj client.Object, suffix string) {
		t.Helper()
		key := client.ObjectKey{Name: childName(cluster.Name, suffix), Namespace: cluster.Namespace}
		if err := cli.Get(ctx, key, obj); err != nil {
			t.Fatalf("expected %T %s to exist: %v", obj, key, err)
		}
	}
	mustExist(&corev1.ServiceAccount{}, "sa")
	mustExist(&corev1.ConfigMap{}, "config")
	mustExist(&corev1.Service{}, "nodes")
	mustExist(&appsv1.StatefulSet{}, "nodes")
}

func TestStatefulSetCarriesSpec(t *testing.T) {
	cluster := sampleCluster()
	r, cli := newReconciler(t, cluster)
	reconcileOnce(t, r, cluster)

	var sts appsv1.StatefulSet
	if err := cli.Get(context.Background(),
		client.ObjectKey{Name: childName(cluster.Name, "nodes"), Namespace: cluster.Namespace}, &sts); err != nil {
		t.Fatalf("StatefulSet not found: %v", err)
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != cluster.Spec.NodeReplicas {
		t.Errorf("replicas = %v, want %d", sts.Spec.Replicas, cluster.Spec.NodeReplicas)
	}
	if got := sts.Spec.Template.Spec.Containers[0].Image; got != cluster.Spec.Image {
		t.Errorf("image = %q, want %q", got, cluster.Spec.Image)
	}
	// NVMe was configured → VolumeClaimTemplate should exist.
	if len(sts.Spec.VolumeClaimTemplates) != 1 {
		t.Errorf("expected 1 VolumeClaimTemplate for NVMe tier, got %d",
			len(sts.Spec.VolumeClaimTemplates))
	}
	if sts.Spec.ServiceName != childName(cluster.Name, "nodes") {
		t.Errorf("ServiceName = %q, want %q",
			sts.Spec.ServiceName, childName(cluster.Name, "nodes"))
	}
}

func TestStatefulSetSkipsPVCWhenNoNvme(t *testing.T) {
	cluster := sampleCluster()
	cluster.Spec.Tier.NvmePath = ""
	cluster.Spec.Tier.NvmeBytes = ""
	r, cli := newReconciler(t, cluster)
	reconcileOnce(t, r, cluster)

	var sts appsv1.StatefulSet
	_ = cli.Get(context.Background(),
		client.ObjectKey{Name: childName(cluster.Name, "nodes"), Namespace: cluster.Namespace}, &sts)
	if len(sts.Spec.VolumeClaimTemplates) != 0 {
		t.Errorf("expected zero VCTs when NVMe tier is unset, got %d",
			len(sts.Spec.VolumeClaimTemplates))
	}
}

func TestConfigMapEncodesTierAndBackend(t *testing.T) {
	cluster := sampleCluster()
	r, cli := newReconciler(t, cluster)
	reconcileOnce(t, r, cluster)

	var cm corev1.ConfigMap
	if err := cli.Get(context.Background(),
		client.ObjectKey{Name: childName(cluster.Name, "config"), Namespace: cluster.Namespace}, &cm); err != nil {
		t.Fatalf("ConfigMap not found: %v", err)
	}
	body := cm.Data[configFileName]
	for _, want := range []string{
		"cluster_name: " + cluster.Name,
		"nixl_backend: " + cluster.Spec.NixlBackend,
		"pinned_bytes: " + cluster.Spec.Tier.PinnedBytes,
		"dram_bytes: " + cluster.Spec.Tier.DramBytes,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("ConfigMap body missing %q:\n%s", want, body)
		}
	}
}

func TestServiceIsHeadlessAndExposesPorts(t *testing.T) {
	cluster := sampleCluster()
	r, cli := newReconciler(t, cluster)
	reconcileOnce(t, r, cluster)

	var svc corev1.Service
	if err := cli.Get(context.Background(),
		client.ObjectKey{Name: childName(cluster.Name, "nodes"), Namespace: cluster.Namespace}, &svc); err != nil {
		t.Fatalf("Service not found: %v", err)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("expected headless service (ClusterIP=None), got %q", svc.Spec.ClusterIP)
	}
	gotPorts := map[string]int32{}
	for _, p := range svc.Spec.Ports {
		gotPorts[p.Name] = p.Port
	}
	if gotPorts[grpcPortName] != grpcPort {
		t.Errorf("missing or wrong grpc port: %+v", gotPorts)
	}
	if gotPorts[metricsPortName] != metricsPort {
		t.Errorf("missing or wrong metrics port: %+v", gotPorts)
	}
}

func TestReconcileIsIdempotent(t *testing.T) {
	cluster := sampleCluster()
	r, cli := newReconciler(t, cluster)
	reconcileOnce(t, r, cluster)
	reconcileOnce(t, r, cluster)  // second pass must not error or duplicate

	var sts appsv1.StatefulSet
	if err := cli.Get(context.Background(),
		client.ObjectKey{Name: childName(cluster.Name, "nodes"), Namespace: cluster.Namespace}, &sts); err != nil {
		t.Fatalf("StatefulSet missing after second Reconcile: %v", err)
	}
}

func TestReconcileReplicasDrift(t *testing.T) {
	cluster := sampleCluster()
	r, cli := newReconciler(t, cluster)
	reconcileOnce(t, r, cluster)

	// User edits the CR — bump replicas. Next reconcile must apply the
	// change to the live StatefulSet.
	if err := cli.Get(context.Background(),
		client.ObjectKey{Name: cluster.Name, Namespace: cluster.Namespace}, cluster); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	cluster.Spec.NodeReplicas = 5
	if err := cli.Update(context.Background(), cluster); err != nil {
		t.Fatalf("update cluster: %v", err)
	}
	reconcileOnce(t, r, cluster)

	var sts appsv1.StatefulSet
	_ = cli.Get(context.Background(),
		client.ObjectKey{Name: childName(cluster.Name, "nodes"), Namespace: cluster.Namespace}, &sts)
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 5 {
		t.Errorf("expected replicas=5 after drift, got %v", sts.Spec.Replicas)
	}
}

func TestChildResourcesArentLeftWhenClusterIsDeleted(t *testing.T) {
	// Foreground cascading deletion isn't simulated by the fake client,
	// but we can verify the OwnerReference points back to the cluster —
	// real K8s GC will follow it.
	cluster := sampleCluster()
	r, cli := newReconciler(t, cluster)
	reconcileOnce(t, r, cluster)

	var sts appsv1.StatefulSet
	if err := cli.Get(context.Background(),
		client.ObjectKey{Name: childName(cluster.Name, "nodes"), Namespace: cluster.Namespace}, &sts); err != nil {
		t.Fatalf("get sts: %v", err)
	}
	if len(sts.OwnerReferences) != 1 ||
		sts.OwnerReferences[0].Kind != "KVCacheCluster" ||
		sts.OwnerReferences[0].Name != cluster.Name {
		t.Errorf("expected OwnerReference back to the cluster, got %+v", sts.OwnerReferences)
	}
}

func TestStatusReadyConditionFlipsWhenAllReplicasReady(t *testing.T) {
	cluster := sampleCluster()
	r, cli := newReconciler(t, cluster)
	reconcileOnce(t, r, cluster)

	// Simulate the apps controller having marked all replicas ready by
	// patching the live StatefulSet.Status.
	var sts appsv1.StatefulSet
	key := client.ObjectKey{Name: childName(cluster.Name, "nodes"), Namespace: cluster.Namespace}
	if err := cli.Get(context.Background(), key, &sts); err != nil {
		t.Fatalf("get sts: %v", err)
	}
	sts.Status.Replicas = cluster.Spec.NodeReplicas
	sts.Status.ReadyReplicas = cluster.Spec.NodeReplicas
	if err := cli.Status().Update(context.Background(), &sts); err != nil {
		t.Fatalf("update sts status: %v", err)
	}

	reconcileOnce(t, r, cluster)

	if err := cli.Get(context.Background(),
		client.ObjectKey{Name: cluster.Name, Namespace: cluster.Namespace}, cluster); err != nil {
		t.Fatalf("get cluster after reconcile: %v", err)
	}
	if cluster.Status.NodesActive != cluster.Spec.NodeReplicas {
		t.Errorf("nodesActive = %d, want %d",
			cluster.Status.NodesActive, cluster.Spec.NodeReplicas)
	}
	var ready *metav1.Condition
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == "Ready" {
			ready = &cluster.Status.Conditions[i]
		}
	}
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Errorf("expected Ready=True condition, got %+v", ready)
	}
}

func TestReconcileWithoutNvmeStillReturnsReadyServiceAccount(t *testing.T) {
	cluster := sampleCluster()
	cluster.Spec.Tier.NvmePath = ""
	cluster.Spec.Tier.NvmeBytes = ""
	r, cli := newReconciler(t, cluster)
	reconcileOnce(t, r, cluster)

	var sa corev1.ServiceAccount
	if err := cli.Get(context.Background(),
		client.ObjectKey{Name: childName(cluster.Name, "sa"), Namespace: cluster.Namespace}, &sa); err != nil {
		if apierrors.IsNotFound(err) {
			t.Fatal("ServiceAccount missing")
		}
		t.Fatalf("get sa: %v", err)
	}
}
