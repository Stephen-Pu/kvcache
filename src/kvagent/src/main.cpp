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

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "bloom_view/bloom_view.h"
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

    // ---- 4. Event loop — drain SQ, echo to CQ.
    // Phase-A1.5 placeholder handler: the real handler dispatches
    // through routing_cache → NodeDirectory + gRPC. The echo path
    // proves the ring round-trip is wired correctly and lets the
    // integration smoke test assert end-to-end without a node.
    std::vector<uint8_t> buf;
    while (g_running.load(std::memory_order_relaxed)) {
        // Wait on the doorbell. Engine writes to SQ then Rings().
        int r = sq_bell->Wait(/*timeout_ms=*/250);
        if (r < 0) continue;  // EINTR / shutdown
        // Drain everything ready.
        while (sq->try_pop(&buf)) {
            (void)cq->try_push({buf.data(), buf.size()});
        }
    }

    std::fprintf(stderr, "kvagent: shutting down\n");
    bloom.Stop();
    // SqCq dtors unlink the shm files since we created them.
    return 0;
}
