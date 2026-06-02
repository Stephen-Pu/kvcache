// kvagent entrypoint — LLD §6.1.5.
//
// Phase A1.5 — bring up the engine-facing rings + the cluster-view
// machinery and drain the SQ in an event-loop until a SIGTERM/SIGINT
// arrives. This is intentionally a thin orchestrator: every meaningful
// behaviour lives in a sibling module (shmem_ring/, routing_cache/,
// bloom_view/) so unit tests cover the hard parts and main.cpp stays
// a wiring diagram.
//
// Boot sequence (per the LLD):
//   1. Parse flags (engine ring path, optional refresh interval).
//   2. Create the SQ + CQ rings under /dev/shm/.
//   3. Install signal handlers (SIGINT, SIGTERM → graceful shutdown).
//   4. Boot the BloomView refresher (loader is a stub today —
//      etcd-backed loader lands when we wire the agent into a real
//      kvstore-node cluster as part of the integration phase).
//   5. Block on the doorbell; drain whatever the engine posted to SQ
//      and (for this phase) just echo back to CQ so an integration
//      test can verify the round-trip. The real "translate to gRPC
//      call into kvstore-node" handler hangs off the same edge.
//
// Multi-engine support is a TODO(A1.6) — today we own one ring pair.
#include <signal.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "bloom_view/bloom_view.h"
#include "cluster/etcd_client.h"
#include "router/cluster_view.h"
#include "router/etcd_view_loader.h"
#include "router/hrw_resolver.h"
#include "router/router.h"
#include "routing_cache/routing_cache.h"
#include "shmem_ring/doorbell.h"
#include "shmem_ring/sq_cq.h"

namespace {

std::atomic<bool> g_running{true};

void SignalHandler(int sig) {
    (void)sig;
    g_running.store(false, std::memory_order_relaxed);
}

struct Flags {
    std::string sq_path = "/dev/shm/kvcache-agent-default-sq";
    std::string cq_path = "/dev/shm/kvcache-agent-default-cq";
    uint32_t    slot_count = 1024;
    uint32_t    slot_bytes = 256;
    int         bloom_refresh_ms = 30'000;
};

Flags ParseFlags(int argc, char** argv) {
    Flags f;
    for (int i = 1; i < argc; ++i) {
        const char* a = argv[i];
        auto take = [&](const char* prefix, std::string& out) {
            const std::size_t n = std::strlen(prefix);
            if (std::strncmp(a, prefix, n) == 0) {
                out = a + n;
                return true;
            }
            return false;
        };
        auto take_int = [&](const char* prefix, auto& out) {
            const std::size_t n = std::strlen(prefix);
            if (std::strncmp(a, prefix, n) == 0) {
                out = static_cast<std::remove_reference_t<decltype(out)>>(
                    std::atoll(a + n));
                return true;
            }
            return false;
        };
        if (take("--sq=", f.sq_path)) continue;
        if (take("--cq=", f.cq_path)) continue;
        if (take_int("--slots=", f.slot_count)) continue;
        if (take_int("--slot-bytes=", f.slot_bytes)) continue;
        if (take_int("--bloom-refresh-ms=", f.bloom_refresh_ms)) continue;
        std::fprintf(stderr, "kvagent: unknown arg: %s\n", a);
        std::exit(2);
    }
    return f;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace kvcache::agent;

    Flags f = ParseFlags(argc, argv);

    // ---- 1. Bring up SQ/CQ rings + doorbells.
    std::string err;
    auto sq = shmem_ring::SqCq::Create({
        .path = f.sq_path,
        .kind = shmem_ring::RingKind::Sq,
        .slot_count = f.slot_count,
        .slot_bytes = f.slot_bytes,
    }, &err);
    if (!sq) {
        std::fprintf(stderr, "kvagent: create SQ %s: %s\n", f.sq_path.c_str(), err.c_str());
        return 1;
    }
    auto cq = shmem_ring::SqCq::Create({
        .path = f.cq_path,
        .kind = shmem_ring::RingKind::Cq,
        .slot_count = f.slot_count,
        .slot_bytes = f.slot_bytes,
    }, &err);
    if (!cq) {
        std::fprintf(stderr, "kvagent: create CQ %s: %s\n", f.cq_path.c_str(), err.c_str());
        return 1;
    }
    auto sq_bell = shmem_ring::Doorbell::Create(&err);
    if (!sq_bell) {
        std::fprintf(stderr, "kvagent: create SQ doorbell: %s\n", err.c_str());
        return 1;
    }
    std::fprintf(stderr, "kvagent: SQ=%s CQ=%s slots=%u slot_bytes=%u\n",
                 f.sq_path.c_str(), f.cq_path.c_str(),
                 f.slot_count, f.slot_bytes);

    // ---- 2. RoutingCache + BloomView (loader stub).
    routing_cache::RoutingCache rcache;
    bloom_view::BloomView bloom({
        .refresh_interval = std::chrono::milliseconds(f.bloom_refresh_ms),
        .loader = [](const std::vector<std::string>& /*tenants*/) {
            // Stub: no etcd wiring yet — return empty so MaybeContains
            // is permissive ("might exist") for every tenant. The real
            // etcd loader is the integration phase.
            return std::vector<bloom_view::TenantSketch>{};
        },
    });
    bloom.Start();

    // ---- 3. Signals.
    struct sigaction sa{};
    sa.sa_handler = SignalHandler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,  &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    // ---- 4. Event loop — drain SQ, route, push CQ.
    // Phase A1.7 — the RequestRouter's slow path is now backed by an
    // HrwResolver: on a routing-cache miss it computes the rendezvous-
    // hash primary over the cluster node set. The node set is seeded
    // from the KVCACHE_NODES env (comma-separated node ids) for now;
    // production refreshes it from etcd's /kvcache/cluster/view on the
    // same 30s tick as the bloom view. An empty set → resolver misses
    // and the engine recomputes (no node to pull from yet).
    router::HrwResolver hrw;
    if (const char* nodes_env = std::getenv("KVCACHE_NODES")) {
        std::vector<std::string> ids;
        std::string s(nodes_env);
        std::size_t start = 0;
        while (start <= s.size()) {
            const auto comma = s.find(',', start);
            const auto end = (comma == std::string::npos) ? s.size() : comma;
            if (end > start) ids.emplace_back(s.substr(start, end - start));
            if (comma == std::string::npos) break;
            start = comma + 1;
        }
        hrw.SetNodes(ids);
        std::fprintf(stderr, "kvagent: HRW resolver seeded with %zu node(s)\n",
                     hrw.NodeCount());
    }

    // Phase A1.8/A1.9/A1.10 — keep the node set fresh from the CP's
    // published cluster view at etcd /kvcache/cluster/view. Loader
    // selection, in priority order:
    //   1. KVCACHE_ETCD_ENDPOINTS — dial a real etcd over the HTTP/JSON
    //      gateway (A1.10) and read the live key cross-process.
    //   2. KVCACHE_CLUSTER_VIEW_FILE — read a file a sidecar refreshes
    //      (A1.8 fallback for deployments without direct etcd access).
    //   3. neither — watcher not started; the KVCACHE_NODES seed stands.
    // etcd_client is declared before view_watcher so it outlives it
    // (members destruct bottom-up: watcher Stops first, then the client).
    std::unique_ptr<kvcache::node::cluster::HttpEtcdClient> etcd_client;
    std::unique_ptr<router::ClusterViewWatcher> view_watcher;

    router::ClusterViewWatcher::Options vopts;
    bool have_loader = false;

    if (const char* endpoints = std::getenv("KVCACHE_ETCD_ENDPOINTS")) {
        // HttpEtcdClient is single-endpoint — take the first.
        std::string ep(endpoints);
        if (const auto comma = ep.find(','); comma != std::string::npos) {
            ep = ep.substr(0, comma);
        }
        if (ep.find("://") == std::string::npos) ep = "http://" + ep;
        kvcache::node::cluster::HttpEtcdClient::Options eo;
        eo.endpoint = ep;
        std::string err;
        etcd_client = kvcache::node::cluster::HttpEtcdClient::Create(eo, &err);
        if (etcd_client) {
            vopts.loader = router::MakeEtcdViewLoader(*etcd_client);
            have_loader = true;
            std::fprintf(stderr, "kvagent: cluster-view watcher → etcd %s\n",
                         ep.c_str());
        } else {
            std::fprintf(stderr, "kvagent: etcd client init failed (%s); "
                         "falling back to file/seed\n", err.c_str());
        }
    }
    if (!have_loader) {
        if (const char* view_file = std::getenv("KVCACHE_CLUSTER_VIEW_FILE")) {
            std::string path(view_file);
            vopts.loader = [path]() -> std::optional<std::string> {
                std::ifstream in(path, std::ios::binary);
                if (!in) return std::string{};  // absent → empty set, not error
                return std::string(std::istreambuf_iterator<char>(in),
                                   std::istreambuf_iterator<char>());
            };
            have_loader = true;
            std::fprintf(stderr, "kvagent: cluster-view watcher polling %s\n",
                         path.c_str());
        }
    }
    if (have_loader) {
        view_watcher = std::make_unique<router::ClusterViewWatcher>(hrw, vopts);
        view_watcher->Start();
    }

    router::RequestRouter request_router(rcache, bloom, hrw.AsCallback());

    std::vector<uint8_t> buf;
    while (g_running.load(std::memory_order_relaxed)) {
        // Wait on the doorbell. Engine writes to SQ then Rings().
        int r = sq_bell->Wait(/*timeout_ms=*/250);
        if (r < 0) continue;  // EINTR / shutdown
        // Drain everything ready, routing each request.
        while (sq->try_pop(&buf)) {
            auto resp_bytes = request_router.HandleRaw({buf.data(), buf.size()});
            (void)cq->try_push({resp_bytes.data(), resp_bytes.size()});
        }
    }

    std::fprintf(stderr, "kvagent: shutting down\n");
    if (view_watcher) view_watcher->Stop();
    bloom.Stop();
    // SqCq dtors unlink the shm files since we created them.
    return 0;
}
