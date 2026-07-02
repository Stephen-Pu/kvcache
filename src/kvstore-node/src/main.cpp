// kvstore-node entrypoint.
//
// Phase M-1 scope: bind the NodeData gRPC service (LLD §3) onto the
// container's grpc port. The agent talks to the binary through this
// service; metadata flows over gRPC, KV bytes flow over NIXL.
//
// What this binary does today:
//
//   1. Parses --config / --grpc-port / --metrics-port flags. The
//      operator-emitted ConfigMap path is stored for future use; the
//      consumer isn't wired yet (Phase M-2 reads it).
//   2. Opens a kv_ctx_t for the "default" tenant — the agent's
//      auth context is bound at ctx-open time inside libkvcache.
//      For Phase M-1 we use a single default ctx for all RPCs.
//   3. Starts a NodeData gRPC server bound to the grpc port and a
//      NodeRuntime running the metrics / healthz HTTP server on
//      the metrics port. NodeRuntime is told to skip the grpc-port
//      placeholder so the grpc::Server can claim it.
//   4. Blocks on SIGTERM (K8s pod evict) / SIGINT (Ctrl-C).
//
// Builds without grpc still produce a working binary — the grpc
// section is gated on KVCACHE_HAVE_GRPC. Without grpc the binary
// falls back to the L-1 accept-and-close placeholder (pods still
// pass readiness; the agent just can't talk to it).
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "runtime/node_runtime.h"
// A10 — Regulated Mode flag plumbing (Task 3).
#include "runtime/regulated_flags.h"
#include "security/boundary_deny_observer.h"
#include "security/boundary_guard.h"
#include "security/boundary_policy_builder.h"
#include "tier/cold_tier.h"
#include "tier/guarded_transport.h"

#if defined(KVCACHE_HAVE_GRPC)
#include "cluster/etcd_client.h"
#include "cluster/bloom_publisher.h"
#include "cluster/node_directory.h"
#include "cluster/node_registrar.h"
#include "kvcache/kv_abi.h"
#include "grpc/grpc_server.h"
#include "grpc/node_data_service.h"
#include "routing/hrw.h"
#endif

namespace {

void Usage(const char* argv0) {
    std::fprintf(stderr,
        "usage: %s [--config PATH] [--grpc-port N] [--metrics-port N]\n"
        "       [--tls-ca PEM] [--tls-cert PEM] [--tls-key PEM]\n"
        "       [--node-id ID --etcd-endpoints ep1,ep2 --advertise-host H]\n"
        "       [--regulated-mode] [--boundary-allow RULE ...]\n"
        "       [--cold-base-url URL]\n",
        argv0);
}

bool TryFlag(int& i, int argc, char** argv,
              const char* flag, std::string* out) {
    if (std::strcmp(argv[i], flag) != 0) return false;
    if (i + 1 >= argc) return false;
    *out = argv[i + 1];
    ++i;
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    kvcache::node::runtime::NodeRuntime::Options o;
    std::string grpc_port_s    = "7000";
    std::string metrics_port_s = "9090";
    // mTLS paths — the operator-emitted StatefulSet passes the trio
    // because Phase H-3 wired a self-signed Secret into the pod spec.
    // The kvstore-node binary records the paths for diagnostics today;
    // actual TLS termination is a future phase (a follow-up will pass
    // these into the grpc::ServerCredentials at server start).
    std::string tls_ca, tls_cert, tls_key;
    // Phase Q-1 — cluster identity / discovery flags. Empty values
    // mean "single-node mode" (no etcd registration, no fan-out); the
    // binary still works for tests that don't need clustering.
    std::string node_id, etcd_endpoints, advertise_host;
    // A10 — Regulated Mode flags (Task 3).
    //   --regulated-mode          : activate the boundary gate (R4: off by default)
    //   --boundary-allow <rule>   : repeatable; operator allowlist (host glob / CIDR)
    //   --cold-base-url <url>     : cold-tier object-store base URL (used to derive
    //                               the kColdTier egress sink for the startup gate)
    kvcache::node::runtime::RegulatedFlags reg_flags;
    std::string cold_base_url;
    for (int i = 1; i < argc; ++i) {
        if (TryFlag(i, argc, argv, "--config",          &o.config_path)) continue;
        if (TryFlag(i, argc, argv, "--grpc-port",       &grpc_port_s))   continue;
        if (TryFlag(i, argc, argv, "--metrics-port",    &metrics_port_s)) continue;
        if (TryFlag(i, argc, argv, "--tls-ca",          &tls_ca))   continue;
        if (TryFlag(i, argc, argv, "--tls-cert",        &tls_cert)) continue;
        if (TryFlag(i, argc, argv, "--tls-key",         &tls_key))  continue;
        if (TryFlag(i, argc, argv, "--node-id",         &node_id))          continue;
        if (TryFlag(i, argc, argv, "--etcd-endpoints",  &etcd_endpoints))   continue;
        if (TryFlag(i, argc, argv, "--advertise-host",  &advertise_host))   continue;
        // A10 — parse boundary flags.
        if (std::strcmp(argv[i], "--regulated-mode") == 0) {
            reg_flags.enabled = true;
            continue;
        }
        {
            std::string rule;
            if (TryFlag(i, argc, argv, "--boundary-allow", &rule)) {
                reg_flags.allow_rules.push_back(std::move(rule));
                continue;
            }
        }
        if (TryFlag(i, argc, argv, "--cold-base-url",   &cold_base_url))    continue;
        if (std::strcmp(argv[i], "-h") == 0 ||
            std::strcmp(argv[i], "--help") == 0) {
            Usage(argv[0]);
            return 0;
        }
        std::fprintf(stderr, "unknown arg: %s\n", argv[i]);
        Usage(argv[0]);
        return 2;
    }
    if (!tls_ca.empty() || !tls_cert.empty() || !tls_key.empty()) {
        std::fprintf(stderr,
            "kvstore-node: mTLS configured (ca=%s cert=%s key=%s)\n",
            tls_ca.c_str(), tls_cert.c_str(), tls_key.c_str());
    }
    o.grpc_port    = static_cast<uint16_t>(std::atoi(grpc_port_s.c_str()));
    o.metrics_port = static_cast<uint16_t>(std::atoi(metrics_port_s.c_str()));

    // ---- A10: Regulated Mode — assemble egress sinks + build shared guard ----
    //
    // Sinks: every statically-configured egress endpoint the node dials at runtime.
    //   * kColdTier  — the object-store URL supplied via --cold-base-url.
    //   * kEtcd      — the etcd cluster supplied via --etcd-endpoints (first entry).
    // Additional seams (telemetry exporter, gRPC peer dials, replication) follow
    // the same pattern; add their Endpoints here when those flags land.
    std::vector<kvcache::node::security::Endpoint> egress_sinks;
    if (!cold_base_url.empty()) {
        uint16_t cold_port = 0;
        std::string cold_host = kvcache::node::tier::HostFromUrl(cold_base_url, &cold_port);
        if (!cold_host.empty()) {
            egress_sinks.push_back({.host    = std::move(cold_host),
                                    .port    = cold_port,
                                    .purpose = kvcache::node::security::Purpose::kColdTier});
        }
    }
    if (!etcd_endpoints.empty()) {
        // etcd_endpoints is a comma-separated list; take the first entry.
        std::string first_etcd = etcd_endpoints;
        if (const auto comma = first_etcd.find(','); comma != std::string::npos) {
            first_etcd = first_etcd.substr(0, comma);
        }
        if (first_etcd.find("://") == std::string::npos) {
            first_etcd = "http://" + first_etcd;
        }
        uint16_t etcd_port = 0;
        std::string etcd_host = kvcache::node::tier::HostFromUrl(first_etcd, &etcd_port);
        if (!etcd_host.empty()) {
            egress_sinks.push_back({.host    = std::move(etcd_host),
                                    .port    = etcd_port,
                                    .purpose = kvcache::node::security::Purpose::kEtcd});
        }
    }

    auto reg_cfg = kvcache::node::runtime::BuildRegulatedModeConfig(reg_flags, egress_sinks);
    o.regulated  = reg_cfg;   // Task 2 seam: startup gate reads this in NodeRuntime ctor.

    // Build the shared guard + deny observer when Regulated Mode is active.
    // Both are injected into every guarded egress seam (Task 1 cold-tier transport,
    // and any future per-dial seam that wraps its own transport).
    std::shared_ptr<const kvcache::node::security::BoundaryGuard> guard;
    kvcache::node::tier::GuardedHttpTransport::DenyObserver deny_obs;
    if (reg_cfg.enabled) {
        guard = std::make_shared<kvcache::node::security::BoundaryGuard>(
            kvcache::node::security::BuildPolicy(reg_cfg.allow_rules, /*default_deny=*/true));
        // MakeBoundaryDenyObserver: pass {} for the metric-increment lambda (wire
        // the real counter from the metrics::Registry when main owns one) and
        // nullptr for the AuditLog (wire the real log here when main owns one).
        deny_obs = kvcache::node::security::MakeBoundaryDenyObserver(
            /*incr_metric=*/{}, /*audit=*/nullptr);
        std::fprintf(stderr,
            "kvstore-node: regulated-mode ON (%zu allow-rules, %zu sinks)\n",
            reg_cfg.allow_rules.size(), reg_cfg.configured_sinks.size());
    }

    // Task 1 seam: when a cold tier is constructed directly by this binary
    // (e.g., via TierManager::Create), populate its guard + deny_observer fields
    // from the shared objects built above.  Today main.cpp routes the cold tier
    // through the C ABI (kv_ctx_open → HeadlessNode → TierManager); the direct
    // wiring below is the seam that future phases will connect to.
    kvcache::node::tier::ColdTierOptions cold_opts;
    if (!cold_base_url.empty()) {
        cold_opts.type           = "native-rest";
        cold_opts.rest.base_url  = cold_base_url;
    }
    cold_opts.guard         = guard;      // nullptr when regulated-mode is off
    cold_opts.deny_observer = deny_obs;   // empty when regulated-mode is off
    // (cold_opts is fully wired for when CreateColdTier / TierManager::Create is
    // called directly from this binary; currently unused because the C ABI path
    // owns the TierManager.)
    (void)cold_opts;  // suppress unused-variable warning until the seam is connected
    // ---- end A10 wiring ----

    std::fprintf(stderr,
        "kvstore-node: starting (config=%s grpc=%u metrics=%u)\n",
        o.config_path.c_str(), o.grpc_port, o.metrics_port);

#if defined(KVCACHE_HAVE_GRPC)
    // Open a default kv_ctx_t for the agent — Phase M-2 will cache one
    // per (tenant, model) keyed off the request fields. The default
    // tenant_id / model_id are placeholders today.
    kv_ctx_config_t cfg{};
    cfg.abi_version    = KVCACHE_ABI_VERSION;
    cfg.agent_endpoint = nullptr;
    cfg.tenant_id      = "kvstore-node-default";
    cfg.model_id       = "kvstore-node-default";
    cfg.flags          = 0;
    kv_ctx_t* ctx = nullptr;
    if (const int rc = kv_ctx_open(&cfg, &ctx); rc != 0 || !ctx) {
        std::fprintf(stderr,
            "kvstore-node: kv_ctx_open failed (status=%d)\n", rc);
        return 1;
    }

    kvcache::node::grpc_server::NodeDataServiceImpl svc(ctx);
    // TODO(A9 multi-cluster follow-on): wire SetReplicaFetchBackend to the
    // in-process HeadlessNode; blocked by dylib hidden visibility —
    // HeadlessNode::Active() / ::ReplicaFetch() are compiled into libkvcache
    // with CXX_VISIBILITY_PRESET hidden and are unreachable from this TU.
    // Fix: add a wiring TU that compiles headless_node.cpp directly (not
    // via the dylib) into kvstore-node, or export a thin C-ABI shim.
    // Tracked with the deferred cross-cluster E2E.
    // Until wired, ReplicaFetch returns INTERNAL "no backend installed".
    kvcache::node::grpc_server::GrpcServer::Options grpc_opts;
    grpc_opts.bind_host         = o.bind_host;
    grpc_opts.port              = o.grpc_port;
    grpc_opts.tls_ca_pem_path   = tls_ca;
    grpc_opts.tls_cert_pem_path = tls_cert;
    grpc_opts.tls_key_pem_path  = tls_key;
    kvcache::node::grpc_server::GrpcServer grpc(grpc_opts, &svc);
    if (!grpc.Ok()) {
        std::fprintf(stderr, "kvstore-node: grpc: %s\n", grpc.error().c_str());
        kv_ctx_close(ctx);
        return 1;
    }
    o.skip_grpc_listener = true;
    o.grpc_port          = grpc.BoundPort();  // for log clarity
    std::fprintf(stderr, "kvstore-node: grpc tls=%s\n",
                  grpc.TlsEnabled() ? "on" : "off");

    // ---- Phase Q-1: optional cluster registration + fan-out ---------
    std::unique_ptr<kvcache::node::cluster::HttpEtcdClient>     etcd;
    std::unique_ptr<kvcache::node::routing::HrwRing>            ring;
    std::unique_ptr<kvcache::node::cluster::NodeDirectory>      directory;
    std::unique_ptr<kvcache::node::cluster::NodeRegistrar>      registrar;
    // Phase K-8 — bloom-sketch publisher. Lives next to the registrar
    // so it can reuse the same node identity + (eventually) the same
    // etcd lease.
    std::unique_ptr<kvcache::node::cluster::BloomPublisher>     publisher;
    if (!node_id.empty() && !etcd_endpoints.empty() &&
        !advertise_host.empty()) {
        // HttpEtcdClient takes a single endpoint URL; the flag accepts
        // a comma-separated list for forward-compat with the gRPC
        // client, but we use the first entry here. Schemes are auto-
        // prefixed with http:// when missing.
        std::string first = etcd_endpoints;
        if (const auto comma = first.find(','); comma != std::string::npos) {
            first = first.substr(0, comma);
        }
        if (first.find("://") == std::string::npos) {
            first = "http://" + first;
        }
        kvcache::node::cluster::HttpEtcdClient::Options eo;
        eo.endpoint = first;
        std::string eerr;
        // Phase Q-3 — retry etcd dial up to ~30 s before giving up.
        // The in-cluster etcd StatefulSet may still be pulling its image
        // when the kvstore-node pod gets scheduled; without this retry
        // the registrar permanently falls back to single-node mode and
        // the whole fan-out story is silently broken on cold starts.
        for (int attempt = 0; attempt < 15 && !etcd; ++attempt) {
            etcd = kvcache::node::cluster::HttpEtcdClient::Create(eo, &eerr);
            if (etcd) break;
            std::fprintf(stderr,
                "kvstore-node: etcd dial attempt %d failed (%s); "
                "retrying in 2s\n", attempt + 1, eerr.c_str());
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
        if (!etcd) {
            std::fprintf(stderr,
                "kvstore-node: etcd dial exhausted; running single-node\n");
        } else {
            ring      = std::make_unique<kvcache::node::routing::HrwRing>();
            directory = std::make_unique<
                kvcache::node::cluster::NodeDirectory>(etcd.get(), ring.get());
            std::string dirr;
            if (!directory->Start(&dirr)) {
                std::fprintf(stderr,
                    "kvstore-node: directory start failed: %s\n", dirr.c_str());
                directory.reset();
            }
            kvcache::node::cluster::NodeRegistrar::Options ro;
            ro.node_id        = node_id;
            ro.advertise_host = advertise_host;
            ro.grpc_port      = grpc.BoundPort();
            registrar = std::make_unique<
                kvcache::node::cluster::NodeRegistrar>(etcd.get(), ro);
            std::string rerr;
            if (!registrar->Start(&rerr)) {
                std::fprintf(stderr,
                    "kvstore-node: node registrar failed: %s\n", rerr.c_str());
                registrar.reset();
            } else {
                std::fprintf(stderr,
                    "kvstore-node: registered as '%s' at %s:%u\n",
                    node_id.c_str(), advertise_host.c_str(),
                    grpc.BoundPort());
            }
            if (directory) {
                svc.EnableForwarding(node_id, ring.get(), directory.get());
                std::fprintf(stderr,
                    "kvstore-node: cross-node Lookup fan-out enabled\n");

                // Phase N-2 — re-use the same TLS material the server
                // listens with for OUTBOUND peer dials, so cross-node
                // forwarding is encrypted + mutually-authenticated
                // instead of falling through to InsecureChannel.
                if (!tls_ca.empty() && !tls_cert.empty() && !tls_key.empty()) {
                    auto slurp = [](const std::string& p) {
                        std::FILE* f = std::fopen(p.c_str(), "rb");
                        if (!f) return std::string{};
                        std::string out;
                        char buf[4096];
                        while (auto n = std::fread(buf, 1, sizeof(buf), f)) {
                            out.append(buf, n);
                        }
                        std::fclose(f);
                        return out;
                    };
                    const auto ca_pem   = slurp(tls_ca);
                    const auto cert_pem = slurp(tls_cert);
                    const auto key_pem  = slurp(tls_key);
                    if (!ca_pem.empty() && !cert_pem.empty() && !key_pem.empty()) {
                        svc.EnableMtlsClient(ca_pem, cert_pem, key_pem,
                                              /*target_override=*/"");
                        std::fprintf(stderr,
                            "kvstore-node: peer-stub mTLS enabled\n");
                    }
                }
            }

            // Phase K-8 — bloom publisher. Bound to the same lease as
            // the NodeRegistrar so it dies with the node identity.
            if (registrar) {
                kvcache::node::cluster::BloomPublisher::Options bo;
                bo.node_id = node_id;
                bo.lease   = registrar->Lease();
                publisher = std::make_unique<
                    kvcache::node::cluster::BloomPublisher>(etcd.get(), bo);
                std::string berr;
                if (!publisher->Start(&berr)) {
                    std::fprintf(stderr,
                        "kvstore-node: bloom publisher start failed: %s\n",
                        berr.c_str());
                    publisher.reset();
                } else {
                    svc.EnableSketchPublishing(publisher.get());
                    std::fprintf(stderr,
                        "kvstore-node: bloom-sketch fan-out enabled\n");
                }
            }
        }
    }
#endif

    kvcache::node::runtime::NodeRuntime rt(o);
    if (!rt.Ok()) {
        std::fprintf(stderr, "kvstore-node: %s\n", rt.error().c_str());
#if defined(KVCACHE_HAVE_GRPC)
        kv_ctx_close(ctx);
#endif
        return 1;
    }
    rt.Start();
    std::fprintf(stderr,
        "kvstore-node: listening (grpc=%u metrics=%u); SIGTERM to stop\n",
        rt.GrpcPort(), rt.MetricsPort());
    const int sig = rt.Wait();
    std::fprintf(stderr, "kvstore-node: stopped (signal=%d)\n", sig);

#if defined(KVCACHE_HAVE_GRPC)
    // Tear down in reverse order. Registrar.Stop revokes the etcd
    // lease so peers stop routing to us within the next watch tick;
    // directory.Stop cancels the watcher; then the gRPC server.
    if (registrar)  registrar->Stop();
    if (directory)  directory->Stop();
    grpc.Stop();
    kv_ctx_close(ctx);
#endif
    return 0;
}
