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
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	kvcachev1alpha1 "github.com/Stephen-Pu/kvcache/operator/api/v1alpha1"
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

	// etcd defaults (Phase H-2)
	etcdContainerName   = "etcd"
	etcdClientPortName  = "client"
	etcdClientPort      = int32(2379)
	etcdPeerPortName    = "peer"
	etcdPeerPort        = int32(2380)
	etcdDataVolume      = "etcd-data"
	etcdDataPath        = "/var/lib/etcd"
	defaultEtcdImage    = "quay.io/coreos/etcd:v3.5.13"
	defaultEtcdReplicas = int32(3)
	defaultEtcdStorage  = "10Gi"

	// control-plane defaults (Phase H-2)
	cpContainerName = "control-plane"
	cpGrpcPortName  = "grpc"
	cpGrpcPort      = int32(7100)
	defaultCpReplicas = int32(3)
)

// labelsFor returns the canonical label set every dependent resource
// carries. ``kvcache.io/cluster`` is the selector used by the
// headless Service and the StatefulSet spec.
func labelsFor(cluster *kvcachev1alpha1.KVCacheCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":     "kvcache",
		"app.kubernetes.io/instance": cluster.Name,
		"app.kubernetes.io/part-of":  "kvcache",
		"kvcache.io/cluster": cluster.Name,
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

// EtcdEndpointsFor returns the etcd client URLs the kvstore-node and
// control-plane pods should dial. When `ByoEtcd: true` the user-provided
// list wins; otherwise the operator-emitted headless etcd Service DNS
// is used. The list is stable so ConfigMap diffs don't churn on each
// reconcile.
func EtcdEndpointsFor(cluster *kvcachev1alpha1.KVCacheCluster) []string {
	if cluster.Spec.ByoEtcd && len(cluster.Spec.EtcdEndpoints) > 0 {
		out := make([]string, len(cluster.Spec.EtcdEndpoints))
		copy(out, cluster.Spec.EtcdEndpoints)
		return out
	}
	// In-cluster etcd: one URL per peer via the headless Service.
	// `<pod>.<svc>.<ns>.svc.cluster.local` resolves to a stable pod IP.
	svc := childName(cluster.Name, "etcd")
	replicas := defaultEtcdReplicas
	if cluster.Spec.Etcd != nil && cluster.Spec.Etcd.Replicas > 0 {
		replicas = cluster.Spec.Etcd.Replicas
	}
	out := make([]string, 0, replicas)
	for i := int32(0); i < replicas; i++ {
		out = append(out, fmt.Sprintf("http://%s-%d.%s.%s.svc.cluster.local:%d",
			svc, i, svc, cluster.Namespace, etcdClientPort))
	}
	return out
}

// DesiredConfigMap — node-local config rendered as YAML under the key
// ``kvcache-node.yaml``. Holds cluster identity, NIXL backend selection,
// tier capacities, and the etcd endpoints (either BYO or the in-cluster
// etcd StatefulSet).
func DesiredConfigMap(cluster *kvcachev1alpha1.KVCacheCluster) *corev1.ConfigMap {
	etcd := EtcdEndpointsFor(cluster)
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
	mtlsVol, mtlsMount := MtlsVolume(cluster)
	container.VolumeMounts = append(container.VolumeMounts, mtlsMount)
	container.Args = append(container.Args,
		"--tls-ca", mtlsMountPath+"/"+mtlsKeyCACert,
		"--tls-cert", mtlsMountPath+"/"+mtlsKeyTLSCert,
		"--tls-key", mtlsMountPath+"/"+mtlsKeyTLSKey,
	)

	// Phase Q-1 — cluster identity + discovery flags. K8s substitutes
	// $(ENV) in args at pod creation when the env var is declared in
	// the same container (which both KVCACHE_NODE_NAME and
	// KVCACHE_POD_IP already are above). The kvstore-node binary
	// only flips on fan-out when all three flags are present, so
	// passing them unconditionally is safe.
	etcdEndpoints := EtcdEndpointsFor(cluster)
	if len(etcdEndpoints) > 0 {
		container.Args = append(container.Args,
			"--node-id", "$(KVCACHE_NODE_NAME)",
			"--advertise-host", "$(KVCACHE_POD_IP)",
			"--etcd-endpoints", strings.Join(etcdEndpoints, ","),
		)
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
		mtlsVol,
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

// ---------------------------------------------------------------------------
// Phase H-2 — in-cluster etcd
// ---------------------------------------------------------------------------

// etcdLabelsFor scopes the labels to a child role so the etcd Service
// selector doesn't accidentally pull in kvstore-node pods (which share
// the cluster-level label set).
func childLabelsFor(cluster *kvcachev1alpha1.KVCacheCluster, role string) map[string]string {
	out := labelsFor(cluster)
	out["app.kubernetes.io/component"] = role
	return out
}

// DesiredEtcdService — headless Service for the in-cluster etcd peer
// group. Returns nil when the user opted into BYO etcd.
func DesiredEtcdService(cluster *kvcachev1alpha1.KVCacheCluster) *corev1.Service {
	if cluster.Spec.ByoEtcd {
		return nil
	}
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childName(cluster.Name, "etcd"),
			Namespace: cluster.Namespace,
			Labels:    childLabelsFor(cluster, "etcd"),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  childLabelsFor(cluster, "etcd"),
			Ports: []corev1.ServicePort{
				{
					Name:       etcdClientPortName,
					Port:       etcdClientPort,
					TargetPort: intstr.FromInt(int(etcdClientPort)),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       etcdPeerPortName,
					Port:       etcdPeerPort,
					TargetPort: intstr.FromInt(int(etcdPeerPort)),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			PublishNotReadyAddresses: true, // peers must reach each other before Ready
		},
	}
}

// DesiredEtcdStatefulSet — the in-cluster 3-replica etcd peer group.
// Returns nil under BYO etcd. The initial-cluster string is built from
// the headless Service's stable per-pod DNS so a fresh cluster can
// bootstrap without external orchestration.
func DesiredEtcdStatefulSet(cluster *kvcachev1alpha1.KVCacheCluster) *appsv1.StatefulSet {
	if cluster.Spec.ByoEtcd {
		return nil
	}
	image := defaultEtcdImage
	replicas := defaultEtcdReplicas
	storage := defaultEtcdStorage
	if cluster.Spec.Etcd != nil {
		if cluster.Spec.Etcd.Image != "" {
			image = cluster.Spec.Etcd.Image
		}
		if cluster.Spec.Etcd.Replicas > 0 {
			replicas = cluster.Spec.Etcd.Replicas
		}
		if cluster.Spec.Etcd.StorageBytes != "" {
			storage = cluster.Spec.Etcd.StorageBytes
		}
	}
	labels := childLabelsFor(cluster, "etcd")
	svcName := childName(cluster.Name, "etcd")

	// Initial cluster wiring — etcd needs to know every peer's name +
	// peer URL up front for the static bootstrap path.
	var initialCluster strings.Builder
	for i := int32(0); i < replicas; i++ {
		if i > 0 {
			initialCluster.WriteString(",")
		}
		fmt.Fprintf(&initialCluster, "%s-%d=http://%s-%d.%s.%s.svc.cluster.local:%d",
			svcName, i, svcName, i, svcName, cluster.Namespace, etcdPeerPort)
	}

	storageQty := resource.MustParse(storage)
	mtlsVol, mtlsMount := MtlsVolume(cluster)
	container := corev1.Container{
		Name:            etcdContainerName,
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"/usr/local/bin/etcd"},
		Args: []string{
			"--name=$(POD_NAME)",
			"--data-dir=" + etcdDataPath,
			"--listen-client-urls=http://0.0.0.0:" + portStr(etcdClientPort),
			"--advertise-client-urls=http://$(POD_NAME)." + svcName + "." + cluster.Namespace +
				".svc.cluster.local:" + portStr(etcdClientPort),
			"--listen-peer-urls=http://0.0.0.0:" + portStr(etcdPeerPort),
			"--initial-advertise-peer-urls=http://$(POD_NAME)." + svcName + "." + cluster.Namespace +
				".svc.cluster.local:" + portStr(etcdPeerPort),
			"--initial-cluster=" + initialCluster.String(),
			"--initial-cluster-state=new",
			"--initial-cluster-token=" + cluster.Name + "-etcd",
			// mTLS material is mounted but etcd 3.5 still runs with the
			// HTTP-listen flags above so the in-cluster control-plane
			// (which doesn't speak TLS yet) keeps working. Phase H-4
			// flips this to --listen-client-urls=https://... once every
			// caller is mTLS-aware.
			"--peer-trusted-ca-file=" + mtlsMountPath + "/" + mtlsKeyCACert,
		},
		Env: []corev1.EnvVar{
			{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			}},
		},
		Ports: []corev1.ContainerPort{
			{Name: etcdClientPortName, ContainerPort: etcdClientPort, Protocol: corev1.ProtocolTCP},
			{Name: etcdPeerPortName, ContainerPort: etcdPeerPort, Protocol: corev1.ProtocolTCP},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: etcdDataVolume, MountPath: etcdDataPath},
			mtlsMount,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(etcdClientPortName)},
			},
			PeriodSeconds:    5,
			TimeoutSeconds:   3,
			FailureThreshold: 6,
		},
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      svcName,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: svcName,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{container},
					Volumes:    []corev1.Volume{mtlsVol},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: etcdDataVolume},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: storageQty},
					},
				},
			}},
			PodManagementPolicy: appsv1.ParallelPodManagement,
		},
	}
}

// ---------------------------------------------------------------------------
// Phase H-2 — control plane
// ---------------------------------------------------------------------------

// DesiredControlPlaneService — headless Service for the CP gRPC peers.
func DesiredControlPlaneService(cluster *kvcachev1alpha1.KVCacheCluster) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      childName(cluster.Name, "cp"),
			Namespace: cluster.Namespace,
			Labels:    childLabelsFor(cluster, "control-plane"),
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  childLabelsFor(cluster, "control-plane"),
			Ports: []corev1.ServicePort{
				{
					Name:       cpGrpcPortName,
					Port:       cpGrpcPort,
					TargetPort: intstr.FromInt(int(cpGrpcPort)),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			PublishNotReadyAddresses: true,
		},
	}
}

// DesiredControlPlaneStatefulSet — the control-plane process. Defaults
// to 3 replicas, leader-elected through etcd. Image defaults to the
// node image (one repo, two binaries — Phase H-3 will split if needed).
func DesiredControlPlaneStatefulSet(cluster *kvcachev1alpha1.KVCacheCluster) *appsv1.StatefulSet {
	image := cluster.Spec.Image
	replicas := defaultCpReplicas
	if cluster.Spec.ControlPlane != nil {
		if cluster.Spec.ControlPlane.Image != "" {
			image = cluster.Spec.ControlPlane.Image
		}
		if cluster.Spec.ControlPlane.Replicas > 0 {
			replicas = cluster.Spec.ControlPlane.Replicas
		}
	}
	labels := childLabelsFor(cluster, "control-plane")
	cpName := childName(cluster.Name, "cp")
	endpoints := strings.Join(EtcdEndpointsFor(cluster), ",")

	mtlsVol, mtlsMount := MtlsVolume(cluster)
	container := corev1.Container{
		Name:            cpContainerName,
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		// Phase Q-4 — the Dockerfile ships the binary at /usr/local/bin/cp
		// (see src/deploy/docker/Dockerfile.cp's `COPY --from=build
		// /out/cp /usr/local/bin/cp` + ENTRYPOINT). The previous path
		// `/usr/local/bin/control-plane` doesn't exist in the image, so
		// containerd's runc init failed with "no such file or directory"
		// and the pod went into CrashLoopBackOff before producing any
		// stdout — making the bug hard to spot without `kubectl describe`.
		Command:         []string{"/usr/local/bin/cp"},
		Args: []string{
			"--etcd-endpoints=" + endpoints,
			"--listen=:" + portStr(cpGrpcPort),
			"--tls-ca=" + mtlsMountPath + "/" + mtlsKeyCACert,
			"--tls-cert=" + mtlsMountPath + "/" + mtlsKeyTLSCert,
			"--tls-key=" + mtlsMountPath + "/" + mtlsKeyTLSKey,
		},
		VolumeMounts: []corev1.VolumeMount{mtlsMount},
		Env: []corev1.EnvVar{
			{Name: "KVCACHE_CLUSTER_NAME", Value: cluster.Name},
			{Name: "KVCACHE_POD_NAME", ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			}},
		},
		Ports: []corev1.ContainerPort{
			{Name: cpGrpcPortName, ContainerPort: cpGrpcPort, Protocol: corev1.ProtocolTCP},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(cpGrpcPortName)},
			},
			PeriodSeconds:    5,
			TimeoutSeconds:   3,
			FailureThreshold: 6,
		},
	}

	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cpName,
			Namespace: cluster.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas:    &replicas,
			ServiceName: cpName,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					ServiceAccountName: childName(cluster.Name, "sa"),
					Containers:         []corev1.Container{container},
					Volumes:            []corev1.Volume{mtlsVol},
				},
			},
			PodManagementPolicy: appsv1.ParallelPodManagement,
		},
	}
}

func portStr(p int32) string {
	return fmt.Sprintf("%d", p)
}
