// Phase S-3 — priority-class preemption micro-benchmark.
//
// Three concurrent threads:
//
//   * P2 (background): tight loop, soak the scheduler.
//   * P1 (default):    moderate cadence, simulates engine fetches.
//   * P0 (control):    sparse interactive probes — should be near-
//                      instant even while P2 is saturating the link.
//
// Each thread owns its own kv_ctx_t pinned to a distinct tenant_id
// so per-tenant lanes don't interfere with the class-level
// reservation. Reports per-class p50/p99 latency. A scheduler that
// honours LLD §5.1 priorities should produce:
//
//   p99(P0) < p99(P1) ≤ p99(P2)
//
// with P0 living roughly in the unloaded single-call regime
// (~bench_fetch p50) while P2 absorbs the queue delay.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "bench_common.h"
#include "kvcache/kv_abi.h"
#include "kvcache/kv_errors.h"

using kvcache::bench::LatencyStats;
using kvcache::bench::NowNs;
using kvcache::bench::PrintLatencyTable;
using kvcache::bench::Summarise;

namespace {

constexpr std::size_t kPrefixTokens  = 32;
constexpr std::size_t kBytesPerToken = 64 * 1024;          // 64 KiB
constexpr auto        kDuration      = std::chrono::milliseconds(1200);
// Background pumps full-tilt; the others throttle to keep the queue
// from filling beyond what scheduling decisions can affect.
constexpr auto        kInteractiveGap = std::chrono::milliseconds(50);

class StartBarrier {
   public:
    explicit StartBarrier(std::size_t n) : n_(n) {}
    void Wait() {
        std::unique_lock<std::mutex> lk(mu_);
        if (++count_ == n_) { ready_ = true; cv_.notify_all(); }
        else cv_.wait(lk, [&] { return ready_; });
    }
   private:
    std::mutex             mu_;
    std::condition_variable cv_;
    std::size_t            n_;
    std::size_t            count_ = 0;
    bool                   ready_ = false;
};

struct ClassResult {
    std::string  name;
    kv_priority_t prio;
    std::size_t  ops = 0;
    LatencyStats lat;
};

void SealOne(kv_ctx_t* ctx, const std::vector<uint32_t>& tokens,
             std::size_t bytes_total) {
    kv_locator_t loc{};
    std::memset(&loc, 0, sizeof(loc));
    loc.range.token_count = static_cast<uint32_t>(tokens.size());
    loc.version = 1;
    kv_handle_t      h = 0;
    kv_buffer_desc_t slot{};
    if (kv_reserve(ctx, &loc, bytes_total, &h, &slot) != KV_OK) return;
    if (slot.addr) std::memset(slot.addr, 0xA0, bytes_total);
    kv_buffer_desc_t empty{};
    kv_publish(ctx, h, empty, bytes_total);
    kv_seal(ctx, h, tokens.data(), tokens.size());
}

}  // namespace

int main(int argc, char** argv) {
    // Phase S-7 — `--strict` flips the bench into CI regression gate.
    // Today only a starvation check fires (P0 must complete >= N ops).
    // The P2/P0 p99 ratio reported below is informational because
    // loopback NIXL doesn't generate enough scheduler-queue depth for
    // priorities to differentiate latency; a real-network bench is
    // needed to gate that ratio (TODO: S-7.1).
    bool strict_mode = false;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--strict") strict_mode = true;
    }
    const std::size_t bytes_per_fetch = kPrefixTokens * kBytesPerToken;
    std::printf("kvcache bench: priority-class preemption\n");
    std::printf("  duration: %lld ms\n  bytes/fetch: %zu KiB\n\n",
                  static_cast<long long>(kDuration.count()),
                  bytes_per_fetch / 1024);

    std::vector<uint32_t> tokens(kPrefixTokens);
    for (std::size_t i = 0; i < kPrefixTokens; ++i) {
        tokens[i] = 0xC0FFu * static_cast<uint32_t>(i + 1);
    }

    // One thread per (tenant, priority) so per-tenant lanes don't
    // mask class-level effects. Multiple P2 saturators so the
    // scheduler queue actually fills — without that the dispatcher
    // runs one-at-a-time inline and priorities never get a chance
    // to bite.
    const std::vector<std::pair<std::string, kv_priority_t>> spec = {
        {"prio-p0-ctrl", KV_PRIORITY_P0},
        {"prio-p1-data", KV_PRIORITY_P1},
        {"prio-p2-bg-0", KV_PRIORITY_P2},
        {"prio-p2-bg-1", KV_PRIORITY_P2},
        {"prio-p2-bg-2", KV_PRIORITY_P2},
        {"prio-p2-bg-3", KV_PRIORITY_P2},
    };
    {
        kv_ctx_config_t cfg{};
        cfg.abi_version = KVCACHE_ABI_VERSION;
        cfg.model_id    = "prio-model";
        for (const auto& [tid, _] : spec) {
            cfg.tenant_id = tid.c_str();
            kv_ctx_t* seed = nullptr;
            if (kv_ctx_open(&cfg, &seed) != KV_OK) {
                std::fprintf(stderr, "seed open %s failed\n", tid.c_str());
                return 1;
            }
            SealOne(seed, tokens, bytes_per_fetch);
            kv_ctx_close(seed);
        }
    }

    StartBarrier barrier(spec.size());
    std::vector<ClassResult> results(spec.size());
    std::vector<std::thread> threads;
    threads.reserve(spec.size());
    std::atomic<bool> stop{false};

    for (std::size_t i = 0; i < spec.size(); ++i) {
        const auto [tid, prio] = spec[i];
        threads.emplace_back([&, i, tid, prio] {
            results[i].name = tid;
            results[i].prio = prio;

            kv_ctx_config_t cfg{};
            cfg.abi_version = KVCACHE_ABI_VERSION;
            cfg.tenant_id   = tid.c_str();
            cfg.model_id    = "prio-model";
            kv_ctx_t* ctx = nullptr;
            if (kv_ctx_open(&cfg, &ctx) != KV_OK) {
                std::fprintf(stderr, "open %s failed\n", tid.c_str());
                return;
            }
            std::vector<uint8_t> dst(bytes_per_fetch);
            uint32_t dst_mr = 0;
            if (kv_register_local_mr(ctx, dst.data(), dst.size(), &dst_mr) != KV_OK) {
                std::fprintf(stderr, "register %s failed\n", tid.c_str());
                kv_ctx_close(ctx);
                return;
            }

            std::vector<uint64_t> ns;
            ns.reserve(1024);

            barrier.Wait();
            while (!stop.load(std::memory_order_relaxed)) {
                kv_locator_t meta{};
                kv_handle_t  h = 0;
                uint32_t     m = 0;
                const auto t0 = NowNs();
                if (kv_lookup(ctx, tokens.data(), tokens.size(),
                                &meta, &h, &m) != KV_OK) break;
                kv_buffer_desc_t d{};
                d.addr   = dst.data();
                d.len    = dst.size();
                d.mr_key = dst_mr;
                kv_completion_t cid = 0;
                if (kv_fetch_with_priority(ctx, h, nullptr, 0, d,
                                            prio, &cid) != KV_OK) {
                    kv_release(ctx, h);
                    break;
                }
                kv_wait(ctx, cid, 5000);
                kv_release(ctx, h);
                ns.push_back(NowNs() - t0);

                // P0 / P1 throttle so they don't also saturate the
                // link. The QUESTION is: while P2 is saturating, do
                // the higher-prio probes get served quickly?
                if (prio != KV_PRIORITY_P2) {
                    std::this_thread::sleep_for(kInteractiveGap);
                }
            }
            kv_unregister_local_mr(ctx, dst_mr);
            kv_ctx_close(ctx);
            results[i].ops = ns.size();
            results[i].lat = Summarise(std::move(ns));
        });
    }

    std::this_thread::sleep_for(kDuration);
    stop.store(true, std::memory_order_relaxed);
    for (auto& th : threads) th.join();

    std::printf("Per-class latency (lower is better; P0 should be near unloaded baseline)\n");
    std::printf("-----------------------------------------------------------------------------------\n");
    for (const auto& r : results) {
        std::printf("  %-14s  ops=%5zu   ", r.name.c_str(), r.ops);
        PrintLatencyTable("(lookup+fetch+wait)", r.lat);
    }
    std::printf("-----------------------------------------------------------------------------------\n");
    // The simple verdict: how much slower is P2's p99 vs P0's p99?
    const double p99_p0 = results[0].lat.p99_ns / 1000.0;
    // Average p99 across all P2 saturators (results[2..]).
    double p99_p2_sum = 0;
    std::size_t p2_count = 0;
    for (std::size_t i = 2; i < results.size(); ++i) {
        p99_p2_sum += results[i].lat.p99_ns / 1000.0;
        ++p2_count;
    }
    const double p99_p2 = p2_count > 0 ? (p99_p2_sum / p2_count) : 0;
    if (p99_p0 > 0) {
        std::printf("\nP2 p99 (avg of %zu saturators) / P0 p99 ratio: %.2f×  "
                      "(higher = more preemption)\n",
                      p2_count, p99_p2 / p99_p0);
    }

    // Phase S-7 — CI regression gate. The only currently-reliable
    // assertion is that P0 isn't starved: it must complete at least
    // N ops within the bench window. (The p99 ratio above stays
    // informational — see the TODO at the top of main(); loopback
    // NIXL doesn't generate enough queue depth for priorities to
    // differentiate latency.) A starvation regression would mean
    // the scheduler stopped dispatching the P0 lane at all.
    constexpr std::size_t kStrictMinP0Ops = 5;
    if (strict_mode) {
        const std::size_t p0_ops = results[0].ops;
        if (p0_ops < kStrictMinP0Ops) {
            std::fprintf(stderr,
                "\nPRIORITY STARVATION (--strict): P0 completed only %zu ops "
                "< %zu min over %lld ms.\n"
                "  The scheduler should always dispatch the P0 lane even "
                "while %zu P2 saturators are full-tilt.\n",
                p0_ops, kStrictMinP0Ops,
                static_cast<long long>(kDuration.count()),
                p2_count);
            return 1;
        }
        std::printf("\n--strict: PASS (P0 ops=%zu >= %zu)\n",
                      p0_ops, kStrictMinP0Ops);
    }
    return 0;
}
