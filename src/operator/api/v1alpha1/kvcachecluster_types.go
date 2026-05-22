// LLD §8.2 — KVCacheCluster CRD types.
package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KVCacheClusterSpec declares one logical cluster.
type KVCacheClusterSpec struct {
	// Number of kvstore-node replicas (DaemonSet on labeled GPU hosts).
	// +kubebuilder:validation:Minimum=1
	NodeReplicas int32 `json:"nodeReplicas"`

	// Image used for kvstore-node and kvagent.
	Image string `json:"image"`

	// Tier capacities — apply per node.
	Tier TierSpec `json:"tier"`

	// NIXL backend selection (LLD §3.5, D-NET-1).
	// +kubebuilder:validation:Enum=ucx;gdr;gds;tcp;nvlink;loopback
	NixlBackend string `json:"nixlBackend,omitempty"`

	// Bring-your-own etcd. When false (default) the operator creates an
	// internal 3-replica etcd StatefulSet (LLD §7.2).
	ByoEtcd       bool     `json:"byoEtcd,omitempty"`
	EtcdEndpoints []string `json:"etcdEndpoints,omitempty"`

	// Tunables for the in-cluster etcd StatefulSet emitted when
	// `byoEtcd: false`. All fields have sensible defaults.
	Etcd *EtcdSpec `json:"etcd,omitempty"`

	// Tunables for the in-cluster control-plane StatefulSet.
	ControlPlane *ControlPlaneSpec `json:"controlPlane,omitempty"`

	// Alluxio binding for the T4 cold tier.
	AlluxioBinding *AlluxioBinding `json:"alluxioBinding,omitempty"`

	// Extra resource requests / limits applied to node pods.
	NodeResources corev1.ResourceRequirements `json:"nodeResources,omitempty"`
}

// EtcdSpec configures the in-cluster etcd StatefulSet. Only honoured
// when `KVCacheClusterSpec.ByoEtcd == false`.
type EtcdSpec struct {
	// Image. Default: "quay.io/coreos/etcd:v3.5.13".
	Image string `json:"image,omitempty"`
	// Replicas. Default: 3 (production etcd minimum for quorum).
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas,omitempty"`
	// Per-replica storage size (e.g. "10Gi"). Default: "10Gi".
	StorageBytes string `json:"storageBytes,omitempty"`
}

// ControlPlaneSpec configures the in-cluster control-plane StatefulSet.
type ControlPlaneSpec struct {
	// Image. Default: same image as the node pods (built from the same
	// repo at this stage).
	Image string `json:"image,omitempty"`
	// Replicas. Default: 3 for HA via etcd leader election.
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas,omitempty"`
}

type TierSpec struct {
	PinnedBytes  string `json:"pinnedBytes"`            // e.g. "32Gi"
	DramBytes    string `json:"dramBytes"`              // e.g. "128Gi"
	NvmePath     string `json:"nvmePath,omitempty"`     // e.g. "/var/lib/kvcache/nvme.bin"
	NvmeBytes    string `json:"nvmeBytes,omitempty"`    // e.g. "1Ti"
	EnableCold   bool   `json:"enableCold,omitempty"`
}

type AlluxioBinding struct {
	// Mount path of the alluxio-fuse mount on each node host.
	MountPath string `json:"mountPath"`
	// Subdirectory under the mount for KV cold-tier data.
	Subdir string `json:"subdir,omitempty"`
}

// KVCacheClusterStatus is reported by the operator.
type KVCacheClusterStatus struct {
	// Conditions follow the standard meta.Condition convention.
	Conditions []metav1.Condition `json:"conditions,omitempty"`
	// Counts of nodes in each membership state.
	NodesActive      int32 `json:"nodesActive"`
	NodesJoining     int32 `json:"nodesJoining"`
	NodesUnreachable int32 `json:"nodesUnreachable"`
	NodesDraining    int32 `json:"nodesDraining"`
	// NotAfter timestamp of the currently-issued mTLS leaf certificate
	// (Phase H-4). When unset, no Secret has been issued yet. The
	// rotation reconciler regenerates the leaf when this drops below
	// `RotationFraction` of the configured lifetime.
	MtlsCertNotAfter *metav1.Time `json:"mtlsCertNotAfter,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Node Replicas",type="integer",JSONPath=".spec.nodeReplicas"
// +kubebuilder:printcolumn:name="Active",type="integer",JSONPath=".status.nodesActive"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type KVCacheCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KVCacheClusterSpec   `json:"spec,omitempty"`
	Status KVCacheClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type KVCacheClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KVCacheCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KVCacheCluster{}, &KVCacheClusterList{})
}
