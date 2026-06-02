// Control Plane entrypoint. LLD §4.1.
//
// Boot sequence:
//   1. Read config from env / flags (etcd endpoints, TLS material, listen addr).
//   2. Open the --listen TCP + HTTP socket IMMEDIATELY so the K8s readiness
//      probe passes before we even try to dial etcd. This matters in kind /
//      production: the in-cluster etcd StatefulSet takes a few seconds to
//      elect a leader, and we don't want the CP pod to enter
//      CrashLoopBackOff while we're waiting on it.
//   3. Dial etcd in a retry loop (was: log.Fatalf — would crash the pod).
//   4. Campaign for leadership via etcd Election (lease TTL 10 s by default).
//   5. As leader: start the membership reconciler + bloom fan-out + config push.
//   6. As follower: serve read-only RPCs (delegate writes to leader).
//
// The gRPC server surface mapping the cp.proto schema lives in
// internal/server/ — wired in Phase A-6 (Register + Heartbeat real;
// Sync stays Unimplemented until quota + bloom-fanout scaffolds land).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"

	myetcd "github.com/Stephen-Pu/kvcache/control-plane/internal/etcd"
	"github.com/Stephen-Pu/kvcache/control-plane/internal/membership"
	pb "github.com/Stephen-Pu/kvcache/control-plane/internal/pb"
	cpserver "github.com/Stephen-Pu/kvcache/control-plane/internal/server"
)

// Set to true once etcd is reachable; /readyz flips from 503 -> 200.
var etcdConnected atomic.Bool

func main() {
	endpoints := flag.String("etcd-endpoints", env("KVCACHE_ETCD_ENDPOINTS", "127.0.0.1:2379"),
		"comma-separated list of etcd endpoints")
	caPath := flag.String("etcd-ca", env("KVCACHE_ETCD_CA", ""), "path to etcd CA cert")
	certPath := flag.String("etcd-cert", env("KVCACHE_ETCD_CERT", ""), "client cert")
	keyPath := flag.String("etcd-key", env("KVCACHE_ETCD_KEY", ""), "client key")
	leaderElectionPath := flag.String("election", "/cp/leader", "etcd path for leader election")
	leaseTTL := flag.Int("lease-ttl", 10, "leader-election lease TTL in seconds")
	listenAddr := flag.String("listen", env("KVCACHE_CP_LISTEN", ":7100"),
		"address the CP HTTP probe + metrics server binds (host:port; :0 = OS-picked)")
	grpcListenAddr := flag.String("grpc-listen", env("KVCACHE_CP_GRPC_LISTEN", ":7101"),
		"address the CP gRPC ControlPlane service binds (host:port; :0 = OS-picked)")
	// Operator-emitted but currently unused — record so the log line
	// makes the configured paths visible.
	_ = flag.String("tls-ca", env("KVCACHE_TLS_CA", ""), "path to mTLS CA bundle (logged only)")
	_ = flag.String("tls-cert", env("KVCACHE_TLS_CERT", ""), "path to mTLS leaf cert (logged only)")
	_ = flag.String("tls-key", env("KVCACHE_TLS_KEY", ""), "path to mTLS leaf key (logged only)")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start the listener before anything else — the K8s readiness probe
	// is a TCPSocket check on this port (see operator resources.go).
	httpSrv, err := startHttpServer(*listenAddr)
	if err != nil {
		log.Fatalf("control-plane: listen %s: %v", *listenAddr, err)
	}
	log.Printf("control-plane: listening on %s", httpSrv.Addr)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	// Retry etcd dial until it succeeds or we're asked to shut down.
	// The pod stays Ready throughout (the listener is already open) so
	// kubelet doesn't restart us if etcd is slow to settle.
	cli, err := dialEtcdWithRetry(ctx, *endpoints, *caPath, *certPath, *keyPath)
	if err != nil {
		log.Printf("control-plane: shutting down before etcd was reachable: %v", err)
		return
	}
	defer cli.Close()
	log.Printf("control-plane: connected to etcd at %s", *endpoints)
	etcdConnected.Store(true)

	registry := membership.NewRegistry(cli)

	// Phase A-6 — gRPC ControlPlane service. Bound on a sibling port
	// to the HTTP probe so K8s readiness can stay on a simple TCPSocket
	// check; production wraps the gRPC port in mTLS via the existing
	// TLS flags (Phase H-3 material). MVP serves Register + Heartbeat;
	// Sync returns Unimplemented until quota/routing scaffolds land.
	grpcSrv, grpcLn, err := startGrpcServer(*grpcListenAddr, registry)
	if err != nil {
		log.Fatalf("control-plane: grpc listen %s: %v", *grpcListenAddr, err)
	}
	log.Printf("control-plane: gRPC listening on %s", grpcLn.Addr())
	defer func() {
		// GracefulStop drains in-flight RPCs first; bounded by the
		// shutdown deadline of the surrounding signal context.
		stopped := make(chan struct{})
		go func() { grpcSrv.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			grpcSrv.Stop()
		}
	}()

	// Leader election. Followers still serve read-only queries (list nodes,
	// stream watches) but writes (quota updates, bloom fan-out) are leader-only.
	go runElection(ctx, cli.Raw(), *leaderElectionPath, int64(*leaseTTL), registry)

	<-ctx.Done()
	log.Println("control-plane: shutting down")
}

// startHttpServer binds `addr` and serves the readiness + metrics
// endpoints. Returns the live server (with .Addr set to the resolved
// "host:port" so :0 callers can read the OS-picked port).
func startHttpServer(addr string) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		// 200 once etcd is dialed; 503 until then. K8s' default
		// TCPSocket readiness probe ignores the body — but a
		// human can curl this and tell whether the CP is fully up.
		if etcdConnected.Load() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("waiting on etcd\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		// Minimal Prometheus exposition body — replace with the
		// runtime metrics registry once the CP grows one.
		var ec int
		if etcdConnected.Load() {
			ec = 1
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "kv_cp_etcd_connected %d\n", ec)
	})

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{
		Addr:         ln.Addr().String(),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("control-plane: http server: %v", err)
		}
	}()
	return srv, nil
}

// dialEtcdWithRetry retries `myetcd.New` until it succeeds or `ctx` is
// done. Used at boot so the CP pod stays Ready while in-cluster etcd is
// still warming up.
func dialEtcdWithRetry(ctx context.Context, endpoints, caPath, certPath, keyPath string) (*myetcd.Client, error) {
	cfg := myetcd.Config{
		Endpoints:      strings.Split(endpoints, ","),
		DialTimeout:    5 * time.Second,
		CAPath:         caPath,
		ClientCertPath: certPath,
		ClientKeyPath:  keyPath,
	}
	backoff := 500 * time.Millisecond
	for ctx.Err() == nil {
		cli, err := myetcd.New(cfg)
		if err == nil {
			return cli, nil
		}
		log.Printf("control-plane: etcd dial failed: %v (retrying in %v)", err, backoff)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
	return nil, ctx.Err()
}

func env(name, fallback string) string {
	if v, ok := os.LookupEnv(name); ok && v != "" {
		return v
	}
	return fallback
}

func runElection(ctx context.Context, cli *clientv3.Client, electionPath string,
	leaseTTL int64, registry *membership.Registry) {
	for ctx.Err() == nil {
		sess, err := concurrency.NewSession(cli, concurrency.WithTTL(int(leaseTTL)))
		if err != nil {
			log.Printf("control-plane: election session: %v", err)
			time.Sleep(time.Second)
			continue
		}
		e := concurrency.NewElection(sess, electionPath)
		hostname, _ := os.Hostname()
		ident := fmt.Sprintf("cp-%s-%d", hostname, time.Now().UnixNano())
		log.Printf("control-plane: campaigning for leadership as %s", ident)
		if err := e.Campaign(ctx, ident); err != nil {
			log.Printf("control-plane: campaign failed: %v", err)
			sess.Close()
			continue
		}
		log.Printf("control-plane: ELECTED leader as %s", ident)
		runLeaderDuties(ctx, registry, ident, sess.Lease())
		_ = e.Resign(context.Background())
		sess.Close()
	}
}

// runLeaderDuties is the work loop that only the leader executes.
//
// Phase K-2 — the leader publishes a coherent `ClusterView` snapshot
// to /kvcache/cluster/view every time membership changes (debounced).
// Consumers (kvstore-node NodeDirectory, future kvagent router) Watch
// that one key instead of fanning out over the whole /kvcache/nodes/
// prefix. The view's lease is the election session's lease, so the
// key auto-expires on leader loss and the new leader rewrites it.
func runLeaderDuties(ctx context.Context, registry *membership.Registry,
	leaderID string, lease clientv3.LeaseID) {
	// Phase K-2 — coherent cluster view.
	pub := &membership.ViewPublisher{
		Registry: registry,
		Lease:    lease,
		LeaderID: leaderID,
	}
	// Phase K-7 — cluster-wide bloom-sketch aggregation. Runs in a
	// sibling goroutine so view + sketch publish independently.
	agg := &membership.SketchAggregator{
		Etcd:     registry.EtcdClient(),
		Lease:    lease,
		LeaderID: leaderID,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := agg.Run(ctx); err != nil {
			log.Printf("control-plane: sketch aggregator: %v", err)
		}
	}()
	if err := pub.Run(ctx); err != nil {
		log.Printf("control-plane: cluster-view publisher: %v", err)
	}
	wg.Wait()
}

// startGrpcServer binds `addr`, constructs a cpserver.Server backed by
// the shared membership.Registry, registers it with a fresh grpc.Server,
// and starts serving in a sibling goroutine. Returns the server + the
// resolved listener (with .Addr() populated so :0 callers can read
// the OS-picked port). Plain text for now — mTLS gets bolted on once
// the operator-provisioned key material reaches the CP pod via the
// same Secret mechanism the kvstore-node side uses (Phase H-3).
func startGrpcServer(addr string, registry *membership.Registry) (*grpc.Server, net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	svc, err := cpserver.New(cpserver.Options{Registry: registry})
	if err != nil {
		_ = ln.Close()
		return nil, nil, err
	}
	srv := grpc.NewServer()
	pb.RegisterControlPlaneServer(srv, svc)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Printf("control-plane: grpc server: %v", err)
		}
	}()
	return srv, ln, nil
}
