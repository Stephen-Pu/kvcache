// LLD §8.2 — KVCacheCluster controller.
//
// Reconcile drives a four-resource desired-state tree per cluster CR:
//
//   1. ServiceAccount  (the SA every kvstore-node pod runs under)
//   2. ConfigMap       (cluster identity, NIXL backend, tier sizes,
//                       etcd endpoints — rendered as YAML)
//   3. Headless Service (per-pod DNS for the StatefulSet pods)
//   4. StatefulSet     (kvstore-node replicas; per-pod PVC for NVMe
//                       when configured)
//
// Each resource is built by a pure function in resources.go and applied
// via the "ensure" loop below: GET → if absent CREATE, else PATCH the
// spec subset we own. Drift on fields we don't touch (e.g. operator
// labels added by other controllers) is preserved.
//
// Status (.status.nodesActive etc.) is currently derived from the
// StatefulSet's ReadyReplicas; the etcd-backed counts will land once
// the membership FSM exposes a richer health signal (Phase H-2).
package controller

import (
	"context"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kvcachev1alpha1 "github.com/alluxio/kvcache/operator/api/v1alpha1"
)

// KVCacheClusterReconciler reconciles a KVCacheCluster object.
type KVCacheClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=kvcache.alluxio.io,resources=kvcacheclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=kvcache.alluxio.io,resources=kvcacheclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets;daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;configmaps;secrets;serviceaccounts,verbs=get;list;watch;create;update;patch;delete

func (r *KVCacheClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	var cluster kvcachev1alpha1.KVCacheCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 1. ServiceAccount
	sa := DesiredServiceAccount(&cluster)
	if err := r.ensureOwned(ctx, &cluster, sa, mergeServiceAccount); err != nil {
		return ctrl.Result{}, err
	}

	// 2. ConfigMap
	cm := DesiredConfigMap(&cluster)
	if err := r.ensureOwned(ctx, &cluster, cm, mergeConfigMap); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Headless Service
	svc := DesiredHeadlessService(&cluster)
	if err := r.ensureOwned(ctx, &cluster, svc, mergeService); err != nil {
		return ctrl.Result{}, err
	}

	// 4. StatefulSet
	sts := DesiredStatefulSet(&cluster)
	if err := r.ensureOwned(ctx, &cluster, sts, mergeStatefulSet); err != nil {
		return ctrl.Result{}, err
	}

	// 5. Status — current implementation reads from the StatefulSet's
	//    Ready/Current/UpdatedReplicas. The richer "joining / draining /
	//    unreachable" breakdown waits for etcd-backed membership in
	//    Phase H-2.
	if err := r.updateStatus(ctx, &cluster, sts); err != nil {
		logger.Error(err, "status update failed (non-fatal)")
	}

	return ctrl.Result{}, nil
}

// ensureOwned is the GET / CREATE / PATCH loop shared by every dependent
// resource. The `merge` callback gets (existing, desired) and applies
// the operator-owned spec subset onto `existing`; the returned bool
// signals whether an Update should be issued.
func (r *KVCacheClusterReconciler) ensureOwned(
	ctx context.Context,
	cluster *kvcachev1alpha1.KVCacheCluster,
	desired client.Object,
	merge func(existing, desired client.Object) bool,
) error {
	if err := controllerutil.SetControllerReference(cluster, desired, r.Scheme); err != nil {
		return err
	}
	// Resolve the live object into the same concrete type as `desired`.
	existing := desired.DeepCopyObject().(client.Object)
	err := r.Get(ctx, client.ObjectKeyFromObject(desired), existing)
	if apierrors.IsNotFound(err) {
		return r.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	if merge(existing, desired) {
		return r.Update(ctx, existing)
	}
	return nil
}

// --- per-kind merge helpers ------------------------------------------------
//
// Each returns true iff `existing` was mutated (i.e. an Update is
// required). They scope the diff to fields the operator owns; everything
// else on `existing` is preserved.

func mergeServiceAccount(existing, _ client.Object) bool {
	// SAs are name-only here; nothing to reconcile beyond existence.
	_ = existing
	return false
}

func mergeConfigMap(existing, desired client.Object) bool {
	e := existing.(*corev1.ConfigMap)
	d := desired.(*corev1.ConfigMap)
	if reflect.DeepEqual(e.Data, d.Data) {
		return false
	}
	e.Data = d.Data
	return true
}

func mergeService(existing, desired client.Object) bool {
	e := existing.(*corev1.Service)
	d := desired.(*corev1.Service)
	changed := false
	if !reflect.DeepEqual(e.Spec.Ports, d.Spec.Ports) {
		e.Spec.Ports = d.Spec.Ports
		changed = true
	}
	if !reflect.DeepEqual(e.Spec.Selector, d.Spec.Selector) {
		e.Spec.Selector = d.Spec.Selector
		changed = true
	}
	return changed
}

func mergeStatefulSet(existing, desired client.Object) bool {
	e := existing.(*appsv1.StatefulSet)
	d := desired.(*appsv1.StatefulSet)
	changed := false
	if e.Spec.Replicas == nil || *e.Spec.Replicas != *d.Spec.Replicas {
		e.Spec.Replicas = d.Spec.Replicas
		changed = true
	}
	// Template — replace wholesale on spec drift. The K8s controller
	// will roll the pods.
	if !reflect.DeepEqual(e.Spec.Template.Spec.Containers, d.Spec.Template.Spec.Containers) {
		e.Spec.Template = d.Spec.Template
		changed = true
	}
	return changed
}

func (r *KVCacheClusterReconciler) updateStatus(
	ctx context.Context,
	cluster *kvcachev1alpha1.KVCacheCluster,
	sts *appsv1.StatefulSet,
) error {
	// Re-fetch the StatefulSet so .Status fields reflect what the apps
	// controller has reported (the `sts` we built is desired-state only).
	var live appsv1.StatefulSet
	if err := r.Get(ctx, client.ObjectKeyFromObject(sts), &live); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}

	newStatus := kvcachev1alpha1.KVCacheClusterStatus{
		NodesActive:  live.Status.ReadyReplicas,
		NodesJoining: live.Status.Replicas - live.Status.ReadyReplicas,
	}
	// Refresh a single Ready condition so kubectl prints it nicely.
	readyCondition := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             "BelowReplicaTarget",
		Message:            "kvstore-node StatefulSet has fewer Ready replicas than desired",
	}
	if live.Status.ReadyReplicas == cluster.Spec.NodeReplicas &&
		cluster.Spec.NodeReplicas > 0 {
		readyCondition.Status = metav1.ConditionTrue
		readyCondition.Reason = "AllNodesReady"
		readyCondition.Message = ""
	}
	newStatus.Conditions = []metav1.Condition{readyCondition}

	if reflect.DeepEqual(cluster.Status, newStatus) {
		return nil
	}
	cluster.Status = newStatus
	return r.Status().Update(ctx, cluster)
}

func (r *KVCacheClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kvcachev1alpha1.KVCacheCluster{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}
