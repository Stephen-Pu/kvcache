// Phase S-2 — multi-tenant fairness micro-benchmark.
//
// Spawns T concurrent threads, each holding its own kv_ctx_t pinned
// to a distinct tenant_id (so the PriorityScheduler routes each
// thread's fetches into its own per-tenant FIFO). All threads share
// one sealed prefix (the fairness question is about scheduler lane
// allocation, not about data placement). They sync-start on a
// barrier, hammer kv_lookup+fetch+wait for `kIterations` rounds, then
// report:
//
//   * Per-tenant throughput (MiB/s) and p99 latency
//   * Jain's fairness index over throughputs:
//       J(x) = (Σx)^2 / (n · Σx^2)   ∈ [1/n, 1]; 1.0 = perfectly fair.
//
// A scheduler that round-robins between tenants should produce J ≈ 1.
// A starvation bug shows up as J ≪ 1 (one tenant gets most of the
// link, others get crumbs).
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

constexpr std::size_t kTenants    = 4;
constexpr std::size_t kIterations = 200;
constexpr std::size_t kPrefixTokens  = 32;
constexpr std::size_t kBytesPerToken = 64 * 1024;  // 64 KiB

// Tiny CV-backed barrier so we know all threads enter the hot loop
// at the same instant. Sleeps in a wait so we don't burn CPU between
// barrier arrivals.
class StartBarrier {
   public:
    explicit StartBarrier(std::size_t n) : n_(n) {}
    void Wait() {
        std::unique_lock<std::mutex> lk(mu_);
        if (++count_ == n_) {
            ready_ = true;
            cv_.notify_all();
        } else {
            cv_.wait(lk, [&] { return ready_; });
        }
    }
   private:
    std::mutex             mu_;
    std::condition_variable cv_;
    std::size_t            n_;
    std::size_t            count_ = 0;
    bool                   ready_ = false;
};

struct TenantResult {
    std::string tenant_id;
    std::size_t bytes      = 0;
    double      wall_sec   = 0;
    LatencyStats lat;
};

void SealOne(kv_ctx_t* ctx, const std::vector<uint32_t>& tokens,
             std::size_t bytes_total) {
    kv_locator_t loc{};
    std::memset(&loc, 0, sizeof(loc));
    loc.range.token_count = static_cast<uint32_t>(tokens.size());
    loc.version           = 1;
    kv_handle_t      h    = 0;
    kv_buffer_desc_t slot{};
    if (kv_reserve(ctx, &loc, bytes_total, &h, &slot) != KV_OK) return;
    if (slot.addr) std::memset(slot.addr, 0x55, bytes_total);
    kv_buffer_desc_t empty{};
    kv_publish(ctx, h, empty, bytes_total);
    kv_seal(ctx, h, tokens.data(), tokens.size());
}

}  // namespace

int main(int argc, char** argv) {
    // Phase S-7 — `--strict` flips the bench from informational into
    // a CI regression gate (non-zero exit on Jain index below the
    // LLD §5.1 minimum).
    bool strict_mode = false;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--strict") strict_mode = true;
    }
    const std::size_t bytes_per_fetch = kPrefixTokens * kBytesPerToken;
    std::printf("kvcache bench: multi-tenant fairness\n");
    std::printf("  tenants: %zu\n  iterations/tenant: %zu\n"
                  "  bytes/fetch: %zu (%zu KiB)\n\n",
                  kTenants, kIterations, bytes_per_fetch,
                  bytes_per_fetch / 1024);

    std::vector<uint32_t> tokens(kPrefixTokens);
    for (std::size_t i = 0; i < kPrefixTokens; ++i) {
        tokens[i] = 0xFA1Fu * static_cast<uint32_t>(i + 1);
    }

    // Q-5 — per-tenant ART namespace isolation means each tenant
    // must seal its OWN copy of the prefix; cross-tenant sharing of
    // a single seed prefix produces lookup-miss for every other
    // tenant. The seal phase runs ahead of the timed loop so
    // measurements only cover steady-state fetch traffic.
    {
        kv_ctx_config_t cfg{};
        cfg.abi_version = KVCACHE_ABI_VERSION;
        cfg.model_id    = "fair-model";
        for (std::size_t t = 0; t < kTenants; ++t) {
            const std::string tid = "fair-t" + std::to_string(t);
            cfg.tenant_id = tid.c_str();
            kv_ctx_t* seed = nullptr;
            if (kv_ctx_open(&cfg, &seed) != KV_OK) {
                std::fprintf(stderr, "seed ctx_open %s failed\n",
                              tid.c_str());
                return 1;
            }
            SealOne(seed, tokens, bytes_per_fetch);
            kv_ctx_close(seed);
        }
    }

    StartBarrier              barrier(kTenants);
    std::vector<TenantResult> results(kTenants);
    std::vector<std::thread>  threads;
    threads.reserve(kTenants);

    for (std::size_t t = 0; t < kTenants; ++t) {
        threads.emplace_back([&, t] {
            const std::string tid = "fair-t" + std::to_string(t);
            kv_ctx_config_t cfg{};
            cfg.abi_version = KVCACHE_ABI_VERSION;
            cfg.tenant_id   = tid.c_str();
            cfg.model_id    = "fair-model";
            kv_ctx_t* ctx = nullptr;
            if (kv_ctx_open(&cfg, &ctx) != KV_OK) {
                std::fprintf(stderr, "ctx_open %s failed\n", tid.c_str());
                return;
            }
            std::vector<uint8_t> dst(bytes_per_fetch);
            uint32_t dst_mr = 0;
            if (kv_register_local_mr(ctx, dst.data(), dst.size(), &dst_mr) != KV_OK) {
                std::fprintf(stderr, "register_local_mr %s failed\n",
                              tid.c_str());
                kv_ctx_close(ctx);
                return;
            }

            std::vector<uint64_t> lat_ns;
            lat_ns.reserve(kIterations);

            barrier.Wait();
            const auto wall_t0 = NowNs();
            std::size_t bytes = 0;
            for (std::size_t i = 0; i < kIterations; ++i) {
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
                if (kv_fetch(ctx, h, nullptr, 0, d, &cid) != KV_OK) {
                    kv_release(ctx, h);
                    break;
                }
                if (kv_wait(ctx, cid, 5000) != KV_OK) {
                    kv_release(ctx, h);
                    break;
                }
                kv_release(ctx, h);
                lat_ns.push_back(NowNs() - t0);
                bytes += bytes_per_fetch;
            }
            const auto wall_elapsed_ns = NowNs() - wall_t0;
            results[t].tenant_id = tid;
            results[t].bytes     = bytes;
            results[t].wall_sec  = wall_elapsed_ns / 1.0e9;
            results[t].lat       = Summarise(std::move(lat_ns));

            kv_unregister_local_mr(ctx, dst_mr);
            kv_ctx_close(ctx);
        });
    }
    for (auto& th : threads) th.join();

    // Throughputs + Jain's index.
    std::printf("Per-tenant results\n");
    std::printf("-----------------------------------------------------------------------------------\n");
    double sum_mibs    = 0;
    double sum_mibs_sq = 0;
    for (const auto& r : results) {
        const double mibs = (r.bytes / (1024.0 * 1024.0)) / r.wall_sec;
        sum_mibs    += mibs;
        sum_mibs_sq += mibs * mibs;
        std::printf("  %-10s  %6.1f MiB/s   wall=%.3f s   ",
                      r.tenant_id.c_str(), mibs, r.wall_sec);
        PrintLatencyTable("(lookup+fetch+wait)", r.lat);
    }
    std::printf("-----------------------------------------------------------------------------------\n");
    const double jain = (sum_mibs * sum_mibs) /
                        (static_cast<double>(kTenants) * sum_mibs_sq);
    const double aggregate = sum_mibs;
    std::printf("\nAggregate throughput: %.1f MiB/s\n", aggregate);
    std::printf("Jain's fairness index: %.4f  (1.0 = perfect, %.4f = max unfair)\n",
                  jain, 1.0 / static_cast<double>(kTenants));

    // Phase S-7 — CI regression gate. With `--strict`, the bench exits
    // non-zero if fairness slips below the LLD §5.1 target. On the
    // dev rig this currently sits at 1.0000 (the in-tree
    // PriorityScheduler's per-tenant round-robin is exact under
    // loopback NIXL), so the 0.85 threshold leaves comfortable
    // headroom for run-to-run jitter on slower CI hardware.
    constexpr double kStrictJainMin = 0.85;
    if (strict_mode) {
        if (jain < kStrictJainMin) {
            std::fprintf(stderr,
                "\nFAIRNESS REGRESSION (--strict): Jain %.4f < %.4f min\n"
                "  per-tenant throughput should not skew across the %zu "
                "lanes by more than ~15%%.\n",
                jain, kStrictJainMin, kTenants);
            return 1;
        }
        std::printf("\n--strict: PASS (Jain %.4f >= %.4f)\n",
                      jain, kStrictJainMin);
    }
    return 0;
}
