// LLD §8.2 — Desired-state builders for the KVCacheCluster reconciler.
//
// Each builder takes the parent `KVCacheCluster` (and any computed helper
// state) and returns a fresh K8s object pre-labelled with the canonical
// kvcache labels and an OwnerReference back to the parent. The
// reconciler then drives an "ensure" loop: GET → if absent create, else
// patch the spec subset we own.
package controller

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	kvcachev1alpha1 "github.com/alluxio/kvcache/operator/api/v1alpha1"
)

const (
	// Container / port conventions. Kept private here so the StatefulSet
	// and Service builders can't drift apart.
	containerName     = "kvstore-node"
	grpcPortName      = "grpc"
	grpcPort          = int32(7000)
	metricsPortName   = "metrics"
	metricsPort       = int32(9090)
	configMountPath   = "/etc/kvcache"
	configMapVolume   = "config"
	pvcDataVolume     = "data"
	configFileName    = "kvcache-node.yaml"
)

// labelsFor returns the canonical label set every dependent resource
// carries. ``kvcache.alluxio.io/cluster`` is the selector used by the
// headless Service and the StatefulSet spec.
func labelsFor(cluster *kvcachev1alpha1.KVCacheCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "kvcache",
		"app.kubernetes.io/instance": cluster.Name,
		"app.kubernetes.io/part-of":  "kvcache",
		"kvcache.alluxio.io/cluster": cluster.Name,
	}
}

// childObjectMeta returns the standard ObjectMeta for any child resource:
// scoped to the parent's namespace, named ``<cluster>-<suffix>``, labelled,
// and tagged with an OwnerReference so deletion of the cluster cascades.
func childObjectMeta(cluster *kvcachev1alpha1.KVCacheCluster, suffix string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      childName(cluster.Name, suffix),
		Namespace: cluster.Namespace,
		Labels:    labelsFor(cluster),
	}
}

func childName(cluster, suffix string) string {
	if suffix == "" {
		return cluster
	}
	return cluster + "-" + suffix
}

// DesiredServiceAccount — the SA every kvstore-node pod runs under.
// RBAC bindings live in the Helm chart (cluster-scoped); we just create
// the namespaced SA the pods reference.
func DesiredServiceAccount(cluster *kvcachev1alpha1.KVCacheCluster) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: childObjectMeta(cluster, "sa"),
	}
}

// DesiredConfigMap — node-local config rendered as YAML under the key
// ``kvcache-node.yaml``. Holds cluster identity, NIXL backend selection,
// tier capacities, and the etcd endpoints (either BYO or the in-cluster
// etcd StatefulSet to be added in a follow-up).
func DesiredConfigMap(cluster *kvcachev1alpha1.KVCacheCluster) *corev1.ConfigMap {
	etcd := cluster.Spec.EtcdEndpoints
	if !cluster.Spec.ByoEtcd && len(etcd) == 0 {
		// Placeholder — Phase H-2 will emit a real in-cluster etcd
		// StatefulSet and rewrite this list with its Service DNS.
		etcd = []string{"http://" + childName(cluster.Name, "etcd") + ":2379"}
	}
	cfg := fmt.Sprintf(
		"cluster_name: %s\nnixl_backend: %s\ntier:\n  pinned_bytes: %s\n  dram_bytes: %s\n  nvme_path: %s\n  nvme_bytes: %s\n  enable_cold: %t\netcd_endpoints:\n",
		cluster.Name,
		cluster.Spec.NixlBackend,
		cluster.Spec.Tier.PinnedBytes,
		cluster.Spec.Tier.DramBytes,
		cluster.Spec.Tier.NvmePath,
		cluster.Spec.Tier.NvmeBytes,
		cluster.Spec.Tier.EnableCold,
	)
	for _, e := range etcd {
		cfg += "  - " + e + "\n"
	}
	return &corev1.ConfigMap{
		ObjectMeta: childObjectMeta(cluster, "config"),
		Data:       map[string]string{configFileName: cfg},
	}
}

// DesiredHeadlessService — the headless Service the StatefulSet uses
// for stable per-pod DNS. We use ``ClusterIP: None`` (headless) so each
// pod gets ``<pod-name>.<svc-name>.<ns>.svc.cluster.local``, which the
// inter-node gRPC paths rely on.
func DesiredHeadlessService(cluster *kvcachev1alpha1.KVCacheCluster) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: childObjectMeta(cluster, "nodes"),
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  labelsFor(cluster),
			Ports: []corev1.ServicePort{
				{
					Name:       grpcPortName,
					Port:       grpcPort,
					TargetPort: intstr.FromInt(int(grpcPort)),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       metricsPortName,
					Port:       metricsPort,
					TargetPort: intstr.FromInt(int(metricsPort)),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			PublishNotReadyAddresses: true,
		},
	}
	return svc
}

// DesiredStatefulSet — the kvstore-node StatefulSet. Replicas come from
// the CR spec; per-pod identity is taken care of by the headless
// Service above. Pods mount the ConfigMap at /etc/kvcache and (when
// the NVMe tier is configured) a per-pod PVC at the NVMe path.
func DesiredStatefulSet(cluster *kvcachev1alpha1.KVCacheCluster) *appsv1.StatefulSet {
	labels := labelsFor(cluster)
	replicas := cluster.Spec.NodeReplicas

	container := corev1.Container{
		Name:            containerName,
		Image:           cluster.Spec.Image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Args: []string{
			"--config", configMountPath + "/" + configFileName,
		},
		Ports: []corev1.ContainerPort{
			{Name: grpcPortName, ContainerPort: grpcPort, Protocol: corev1.ProtocolTCP},
			{Name: metricsPortName, ContainerPort: metricsPort, Protocol: corev1.ProtocolTCP},
		},
		Env: []corev1.EnvVar{
			{Name: "KVCACHE_CLUSTER_NAME", Value: cluster.Name},
			{Name: "KVCACHE_NODE_NAME", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			}},
			{Name: "KVCACHE_POD_IP", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
			}},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: configMapVolume, MountPath: configMountPath, ReadOnly: true},
		},
		Resources: cluster.Spec.NodeResources,
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(grpcPortName)},
			},
			PeriodSeconds:    5,
			TimeoutSeconds:   3,
			FailureThreshold: 6,
		},
	}
	volumes := []corev1.Volume{
		{
			Name: configMapVolume,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: childName(cluster.Name, "config"),
					},
				},
			},
		},
	}

	// PVC for the NVMe tier — only when the user opted in. We model it
	// as a per-pod VolumeClaimTemplate so each replica gets its own
	// backing volume; the StorageClass / size come from NvmeBytes.
	var volumeClaimTemplates []corev1.PersistentVolumeClaim
	if cluster.Spec.Tier.NvmePath != "" && cluster.Spec.Tier.NvmeBytes != "" {
		container.VolumeMounts = append(container.VolumeMounts,
			corev1.VolumeMount{Name: pvcDataVolume, MountPath: cluster.Spec.Tier.NvmePath})
		size := resource.MustParse(cluster.Spec.Tier.NvmeBytes)
		volumeClaimTemplates = []corev1.PersistentVolumeClaim{{
			ObjectMeta: metav1.ObjectMeta{Name: pvcDataVolume},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: size},
				},
			},
		}}
	}

	return &appsv1.StatefulSet{
		ObjectMeta: childObjectMeta(cluster, "nodes"),
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: childName(cluster.Name, "nodes"),
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: childName(cluster.Name, "sa"),
					Containers:         []corev1.Container{container},
					Volumes:            volumes,
				},
			},
			VolumeClaimTemplates: volumeClaimTemplates,
			PodManagementPolicy:  appsv1.ParallelPodManagement,
		},
	}
}
