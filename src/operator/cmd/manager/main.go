// Operator manager binary.
//
// Boot: register the v1alpha1 scheme, attach the KVCacheCluster reconciler,
// start the controller-runtime manager (which owns the leader-election
// lease, the cache, and the webhook server).
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	kvcachev1alpha1 "github.com/alluxio/kvcache/operator/api/v1alpha1"
	"github.com/alluxio/kvcache/operator/internal/controller"
)

var (
	scheme = runtime.NewScheme()
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(kvcachev1alpha1.AddToScheme(scheme))
}

func main() {
	var (
		metricsAddr      string
		probeAddr        string
		enableLeaderElec bool
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint addr")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe addr")
	flag.BoolVar(&enableLeaderElec, "leader-elect", false, "enable leader election for HA")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElec,
		LeaderElectionID:       "kvcache-operator.kvcache.alluxio.io",
	})
	if err != nil {
		setupExit("unable to start manager", err)
	}

	if err := (&controller.KVCacheClusterReconciler{
		Client:  mgr.GetClient(),
		Scheme:  mgr.GetScheme(),
		Members: &controller.EtcdMemberCounter{},
	}).SetupWithManager(mgr); err != nil {
		setupExit("unable to set up KVCacheCluster controller", err)
	}

	if err := (&controller.KVCacheTenantReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupExit("unable to set up KVCacheTenant controller", err)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupExit("unable to set up healthz", err)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupExit("unable to set up readyz", err)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupExit("problem running manager", err)
	}
}

func setupExit(msg string, err error) {
	ctrl.Log.Error(err, msg)
	os.Exit(1)
}
