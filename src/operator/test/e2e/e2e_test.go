// Phase K-1 — kind-cluster e2e for the KVCacheCluster reconciler.
//
// What this validates that the fake-client unit tests (in
// internal/controller/) can't:
//   - CRD schema is accepted by a real apiserver (kubectl validation
//     would reject a malformed openAPIV3Schema).
//   - The controller's Watch / Reconcile loop wires up correctly
//     against a real apiserver — including OwnerReference cascade
//     deletion via the real garbage collector.
//   - RBAC labels / namespaces / owner-refs survive a round trip
//     through etcd's actual storage layer (the fake client mocks all
//     of these).
//
// What this deliberately doesn't validate:
//   - That kvstore-node / control-plane pods actually run. main.cpp
//     for kvstore-node is a stub (it prints + exits), so the
//     StatefulSet always CrashLoopBackOffs. We only verify the
//     StatefulSet object itself is created with the expected spec —
//     same shape the fake-client tests assert against, but now
//     against a real apiserver.
//
// Invocation: see test/e2e/run.sh — it spins up a kind cluster,
// exports KUBECONFIG, then runs `go test -tags=e2e`. The build tag
// keeps this file out of the default `go test ./...` so a developer
// without docker / kind can still iterate normally.
//
//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	kvcachev1alpha1 "github.com/alluxio/kvcache/operator/api/v1alpha1"
	"github.com/alluxio/kvcache/operator/internal/controller"
)

// Setup brings up the controller-manager in-process against the kind
// cluster's apiserver. Returns a client + a teardown closure.
func setup(t *testing.T) (client.Client, func()) {
	t.Helper()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, _ := os.UserHomeDir()
		kubeconfig = filepath.Join(home, ".kube", "config")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatalf("load kubeconfig %s: %v", kubeconfig, err)
	}

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kvcachev1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme: scheme,
	})
	if err != nil {
		t.Fatalf("create manager: %v", err)
	}
	if err := (&controller.KVCacheClusterReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("setup controller: %v", err)
	}

	// Plain cancelable context — controller-runtime's
	// SetupSignalHandler can only be called once per process, but
	// every test in this file calls setup() fresh.
	ctx, cancel := context.WithCancel(context.Background())
	managerDone := make(chan struct{})
	go func() {
		defer close(managerDone)
		if err := mgr.Start(ctx); err != nil {
			t.Logf("manager exited: %v", err)
		}
	}()

	// Wait for the cache to populate so subsequent Get/List see live data.
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		cancel()
		<-managerDone
		t.Fatal("cache sync timeout")
	}

	_ = log.FromContext(context.Background())
	return mgr.GetClient(), func() {
		cancel()
		<-managerDone
	}
}

// pollUntil retries `fn` every 500ms up to `timeout`, returning the
// first nil it sees. Used for "child object eventually appears".
func pollUntil(t *testing.T, timeout time.Duration, fn func() error) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if err := fn(); err == nil {
			return nil
		} else {
			last = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out after %v: last error: %v", timeout, last)
}

// uniqueNS gives each test its own namespace so concurrent runs (or
// reruns inside the same kind cluster) don't collide.
func uniqueNS(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("e2e-%s-%d",
		strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")),
		time.Now().UnixNano()%1_000_000)
	if len(name) > 53 {
		name = name[:53]
	}
	return name
}

func ensureNamespace(t *testing.T, c client.Client, name string) func() {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := c.Create(context.Background(), ns); err != nil {
		t.Fatalf("create ns %s: %v", name, err)
	}
	return func() {
		_ = c.Delete(context.Background(), ns)
	}
}

func sampleCluster(ns string) *kvcachev1alpha1.KVCacheCluster {
	return &kvcachev1alpha1.KVCacheCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "e2e",
			Namespace: ns,
		},
		Spec: kvcachev1alpha1.KVCacheClusterSpec{
			NodeReplicas: 3,
			Image:        "ghcr.io/alluxio/kvcache:e2e",
			NixlBackend:  "tcp",
			Tier: kvcachev1alpha1.TierSpec{
				PinnedBytes: "1Gi",
				DramBytes:   "1Gi",
			},
		},
	}
}

// TestReconcilerFanout: apply a CR, wait for the eight child resources
// (SA + ConfigMap + nodes-Svc + nodes-STS + etcd-Svc + etcd-STS +
// cp-Svc + cp-STS) to appear with correct shape. Pods CrashLoopBackOff
// — that's the kvstore-node main.cpp stub talking — but the object
// shape is what we're testing here.
func TestReconcilerFanout(t *testing.T) {
	c, teardown := setup(t)
	defer teardown()

	ns := uniqueNS(t)
	dropNS := ensureNamespace(t, c, ns)
	defer dropNS()

	cluster := sampleCluster(ns)
	ctx := context.Background()
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("apply cluster: %v", err)
	}

	for _, name := range []string{"e2e-sa", "e2e-config", "e2e-nodes",
		"e2e-etcd", "e2e-cp"} {
		key := types.NamespacedName{Name: name, Namespace: ns}
		err := pollUntil(t, 30*time.Second, func() error {
			// We don't know if `name` is a SA / CM / Svc / STS — try
			// each kind the reconciler emits. A real e2e harness
			// would split these but we're keeping it compact.
			for _, obj := range []client.Object{
				&corev1.ServiceAccount{}, &corev1.ConfigMap{},
				&corev1.Service{}, &appsv1.StatefulSet{},
			} {
				if err := c.Get(ctx, key, obj); err == nil {
					return nil
				}
			}
			return fmt.Errorf("not found yet: %s/%s", ns, name)
		})
		if err != nil {
			t.Errorf("waiting for %s: %v", name, err)
		}
	}

	// Nodes StatefulSet shape spot-check.
	var sts appsv1.StatefulSet
	if err := c.Get(ctx, types.NamespacedName{Name: "e2e-nodes", Namespace: ns}, &sts); err != nil {
		t.Fatalf("get nodes STS: %v", err)
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 3 {
		t.Errorf("replicas = %v, want 3", sts.Spec.Replicas)
	}
	if sts.Spec.ServiceName != "e2e-nodes" {
		t.Errorf("ServiceName = %q, want e2e-nodes", sts.Spec.ServiceName)
	}
	if len(sts.OwnerReferences) != 1 || sts.OwnerReferences[0].Name != "e2e" {
		t.Errorf("OwnerReferences = %+v, want one ref to e2e", sts.OwnerReferences)
	}
}

// TestRealWorkloadPodReady — Phase L-2. Only runs when the env var
// `E2E_IMAGE` is set (typically by `make e2e-operator-workload`,
// which builds + kind-loads the kvstore-node image first). The test:
//
//   1. Applies a KVCacheCluster whose `.spec.image` points at the
//      pre-loaded image, downsizing the rest of the cluster to one
//      replica each so we don't fight kind's tiny default node for
//      scheduling slots.
//   2. Waits for the kvstore-node StatefulSet to report
//      ReadyReplicas == NodeReplicas. That validates: the image was
//      built right, the binary starts, the readiness TCP probe on
//      the grpc port passes — i.e. the L-1 + M-1 work is real, not
//      just unit-tested.
//   3. Tears the cluster down.
//
// Skipped when E2E_IMAGE is unset so a `make e2e-operator` run still
// completes in ~45s.
func TestRealWorkloadPodReady(t *testing.T) {
	image := os.Getenv("E2E_IMAGE")
	if image == "" {
		t.Skip("E2E_IMAGE not set; skipping the kvstore-node workload check")
	}

	c, teardown := setup(t)
	defer teardown()

	ns := uniqueNS(t)
	dropNS := ensureNamespace(t, c, ns)
	defer dropNS()

	cluster := sampleCluster(ns)
	cluster.Spec.Image = image
	// kind's default control-plane node has limited CPU/memory; the
	// default 3-replica STS + 3-replica etcd + 3-replica CP exceeds
	// what fits without tuning. Shrink to 1 each for the workload
	// liveness probe — the shape is the same.
	cluster.Spec.NodeReplicas = 1
	cluster.Spec.Etcd = &kvcachev1alpha1.EtcdSpec{Replicas: 1}
	cluster.Spec.ControlPlane = &kvcachev1alpha1.ControlPlaneSpec{
		Image:    image, // reuse — the binary inside ignores `--cp-mode`
		Replicas: 1,
	}

	ctx := context.Background()
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// 5 minutes is generous — kind pulls each image off the host's
	// docker once, then containerd starts the pod (~5s). The slow
	// case is the FIRST run on a cold machine: kind itself bootstraps
	// (~30s) before we even get here.
	stsKey := types.NamespacedName{Name: "e2e-nodes", Namespace: ns}
	err := pollUntil(t, 5*time.Minute, func() error {
		var sts appsv1.StatefulSet
		if err := c.Get(ctx, stsKey, &sts); err != nil {
			return err
		}
		want := cluster.Spec.NodeReplicas
		if sts.Status.ReadyReplicas != want {
			return fmt.Errorf("ReadyReplicas=%d, want %d",
				sts.Status.ReadyReplicas, want)
		}
		return nil
	})
	if err != nil {
		// Drop the pod's logs into the test output so kubectl-less
		// debugging is feasible from CI.
		var pods corev1.PodList
		_ = c.List(ctx, &pods, client.InNamespace(ns))
		for _, p := range pods.Items {
			t.Logf("pod %s: phase=%s reason=%s",
				p.Name, p.Status.Phase, p.Status.Reason)
			for _, cs := range p.Status.ContainerStatuses {
				if cs.State.Waiting != nil {
					t.Logf("  %s waiting: %s — %s",
						cs.Name, cs.State.Waiting.Reason,
						cs.State.Waiting.Message)
				}
			}
		}
		t.Fatalf("kvstore-node pod never reached Ready: %v", err)
	}
}

// TestCascadeDeleteRemovesChildren: real apiserver-driven test that the
// fake client can't simulate. Apply a CR, wait for an STS, delete the
// CR with foreground cascading, expect the STS to disappear.
func TestCascadeDeleteRemovesChildren(t *testing.T) {
	c, teardown := setup(t)
	defer teardown()

	ns := uniqueNS(t)
	dropNS := ensureNamespace(t, c, ns)
	defer dropNS()

	cluster := sampleCluster(ns)
	ctx := context.Background()
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	stsKey := types.NamespacedName{Name: "e2e-nodes", Namespace: ns}
	if err := pollUntil(t, 30*time.Second, func() error {
		return c.Get(ctx, stsKey, &appsv1.StatefulSet{})
	}); err != nil {
		t.Fatalf("STS never appeared: %v", err)
	}

	// Foreground cascade so K8s GC removes the STS before the cluster
	// itself disappears.
	policy := metav1.DeletePropagationForeground
	if err := c.Delete(ctx, cluster, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil {
		t.Fatalf("delete cluster: %v", err)
	}

	if err := pollUntil(t, 60*time.Second, func() error {
		err := c.Get(ctx, stsKey, &appsv1.StatefulSet{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("STS still present")
	}); err != nil {
		t.Errorf("cascade delete failed: %v", err)
	}
}
