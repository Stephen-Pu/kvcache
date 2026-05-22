// LLD §5.2 / §8.2 — Self-signed mTLS material for the kvstore-node,
// control-plane, and in-cluster etcd pods.
//
// The operator emits a single Secret per cluster, ``<cluster>-mtls``, with:
//
//   ca.crt   — PEM-encoded CA self-signed by the operator
//   ca.key   — PEM-encoded CA private key (retained so the operator can
//              re-sign rotated leaf certs in Phase H-4)
//   tls.crt  — PEM-encoded leaf cert, SAN-listed for every in-cluster DNS
//              the cluster exposes (node, etcd, cp headless services + pods)
//   tls.key  — PEM-encoded leaf private key
//
// Generation is *one-shot*: once the Secret exists, the operator leaves it
// alone. That's the "build the cert on first boot, never touch again"
// posture — sufficient for the trusted-network demo path and easy to swap
// for cert-manager later (Phase H-4 will add a `useCertManager: true`
// branch that emits Certificate CRs instead of generating bytes here).
package controller

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kvcachev1alpha1 "github.com/alluxio/kvcache/operator/api/v1alpha1"
)

const (
	mtlsSuffix       = "mtls"
	mtlsMountPath    = "/etc/kvcache/tls"
	mtlsVolume       = "mtls"
	mtlsCertValidity = 10 * 365 * 24 * time.Hour // 10 years; rotation is Phase H-4

	mtlsKeyCACert  = "ca.crt"
	mtlsKeyCAKey   = "ca.key"
	mtlsKeyTLSCert = "tls.crt"
	mtlsKeyTLSKey  = "tls.key"

	rsaBits = 2048
)

// MtlsSecretName is the name of the per-cluster Secret holding the
// CA + leaf material. Exported so tests can target it directly.
func MtlsSecretName(cluster string) string {
	return childName(cluster, mtlsSuffix)
}

// DesiredMtlsSecret returns nil when a previous Secret already exists
// (one-shot generation). When a fresh material set is needed, it
// generates a CA + leaf cert and packs them into a corev1.Secret.
//
// `existing` is the live Secret, or nil if the reconciler couldn't find
// one. When `existing` carries all four expected keys we treat it as
// authoritative and return nil — the reconciler will then skip the
// Create/Update entirely.
func DesiredMtlsSecret(cluster *kvcachev1alpha1.KVCacheCluster,
	existing *corev1.Secret) (*corev1.Secret, error) {
	if mtlsSecretComplete(existing) {
		return nil, nil
	}
	ca, caKey, err := generateCA(cluster.Name)
	if err != nil {
		return nil, fmt.Errorf("mtls: generate CA: %w", err)
	}
	leaf, leafKey, err := generateLeaf(cluster, ca, caKey)
	if err != nil {
		return nil, fmt.Errorf("mtls: generate leaf: %w", err)
	}

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      MtlsSecretName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    childLabelsFor(cluster, "mtls"),
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			mtlsKeyCACert:  ca,
			mtlsKeyCAKey:   caKey,
			mtlsKeyTLSCert: leaf,
			mtlsKeyTLSKey:  leafKey,
		},
	}, nil
}

func mtlsSecretComplete(s *corev1.Secret) bool {
	if s == nil {
		return false
	}
	for _, k := range []string{mtlsKeyCACert, mtlsKeyCAKey, mtlsKeyTLSCert, mtlsKeyTLSKey} {
		if len(s.Data[k]) == 0 {
			return false
		}
	}
	return true
}

// dnsNamesFor returns the in-cluster DNS SANs every leaf cert covers.
// Wildcards keep the per-pod hostnames (`<sts>-<ordinal>...`) inside the
// same cert; one cert covers the whole cluster.
func dnsNamesFor(cluster *kvcachev1alpha1.KVCacheCluster) []string {
	ns := cluster.Namespace
	svcs := []string{
		childName(cluster.Name, "nodes"),
		childName(cluster.Name, "etcd"),
		childName(cluster.Name, "cp"),
	}
	var out []string
	for _, svc := range svcs {
		out = append(out,
			svc,
			fmt.Sprintf("%s.%s", svc, ns),
			fmt.Sprintf("%s.%s.svc", svc, ns),
			fmt.Sprintf("%s.%s.svc.cluster.local", svc, ns),
			fmt.Sprintf("*.%s.%s.svc.cluster.local", svc, ns),
		)
	}
	return out
}

func generateCA(clusterName string) ([]byte, []byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, rsaBits)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}
	tpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   fmt.Sprintf("kvcache-ca-%s", clusterName),
			Organization: []string{"kvcache.alluxio.io"},
		},
		NotBefore:             time.Now().Add(-1 * time.Minute),
		NotAfter:              time.Now().Add(mtlsCertValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return certPEM, keyPEM, nil
}

func generateLeaf(cluster *kvcachev1alpha1.KVCacheCluster,
	caCertPEM, caKeyPEM []byte) ([]byte, []byte, error) {
	caBlock, _ := pem.Decode(caCertPEM)
	if caBlock == nil {
		return nil, nil, fmt.Errorf("ca cert: PEM decode failed")
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	caKeyBlock, _ := pem.Decode(caKeyPEM)
	if caKeyBlock == nil {
		return nil, nil, fmt.Errorf("ca key: PEM decode failed")
	}
	caKey, err := x509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}

	leafKey, err := rsa.GenerateKey(rand.Reader, rsaBits)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}
	tpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: fmt.Sprintf("kvcache-%s", cluster.Name),
		},
		NotBefore:   time.Now().Add(-1 * time.Minute),
		NotAfter:    time.Now().Add(mtlsCertValidity),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    dnsNamesFor(cluster),
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(leafKey)})
	return certPEM, keyPEM, nil
}

// MtlsVolume returns the Volume and VolumeMount that each pod needs to
// see the Secret at `/etc/kvcache/tls/`. Caller wires both into the
// pod spec for whichever StatefulSet it's building.
func MtlsVolume(cluster *kvcachev1alpha1.KVCacheCluster) (corev1.Volume, corev1.VolumeMount) {
	return corev1.Volume{
			Name: mtlsVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: MtlsSecretName(cluster.Name),
				},
			},
		},
		corev1.VolumeMount{
			Name:      mtlsVolume,
			MountPath: mtlsMountPath,
			ReadOnly:  true,
		}
}
