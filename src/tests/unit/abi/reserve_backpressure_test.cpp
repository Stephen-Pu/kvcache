// Phase G-3 — Reserve backpressure surfaces through Prometheus
// metrics so operators can detect saturation BEFORE it produces a
// silent KV_E_NOMEM cliff. The C ABI is the only externally-visible
// surface; we drive it from a real consumer's point of view and
// scrape /metrics (via Registry::Default().Scrape()) to assert:
//
//   * kv_pinned_tier_slots_total reports the pool size.
//   * kv_pinned_tier_slots_in_use climbs with each Reserve and falls
//     back with each Release.
//   * kv_reserves_total counts only successes.
//   * kv_reserve_nomem_total counts only pool-exhausted rejects.
//   * kv_pinned_tier_slots_utilization_ratio reaches 1.0 at full pool.
#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

#include "kvcache/kv_abi.h"
#include "kvcache/kv_errors.h"

namespace {

// Pull a single sample value out of the scrape blob. We accept either
// `kv_reserves_total 5` or `kv_reserves_total{} 5` so the test stays
// robust if the registry decides to print empty label braces.
double SampleValue(const std::string& scrape, const std::string& metric) {
    const std::string p1 = "\n" + metric + " ";
    const std::string p2 = "\n" + metric + "{} ";
    const auto* p = &p1;
    auto pos = scrape.find(*p);
    if (pos == std::string::npos) {
        p   = &p2;
        pos = scrape.find(*p);
    }
    if (pos == std::string::npos) return -1.0;
    const auto start = pos + p->size();
    const auto end   = scrape.find('\n', start);
    return std::stod(scrape.substr(start, end - start));
}

double Sample(const std::string& metric) {
    // Use the dylib's kv_metrics_scrape entrypoint so the test sees
    // the SAME Registry::Default() singleton libkvcache writes to.
    // Static-linking `kvcache_common` directly would land us in a
    // duplicated-singleton trap (the test binary and the dylib would
    // each have their own).
    size_t need = 0;
    if (kv_metrics_scrape(nullptr, 0, &need) != KV_OK) return -1.0;
    std::string blob(need + 1, '\0');
    if (kv_metrics_scrape(blob.data(), blob.size(), nullptr) != KV_OK) return -1.0;
    // Prepend '\n' so the helper finds a line-start match for the FIRST
    // sample too.
    blob.insert(blob.begin(), '\n');
    return SampleValue(blob, metric);
}

kv_locator_t TinyLocator() {
    kv_locator_t loc{};
    std::memset(&loc, 0, sizeof(loc));
    loc.range.token_count = 16;
    loc.version           = 1;
    return loc;
}

}  // namespace

// Verifies the gauges + counters move in lockstep with Reserve/Release
// over the same kv_ctx_t. We can't shrink the singleton's pool between
// tests, so we just observe deltas — anything else (other tests
// running, env defaults) just shifts the baseline.
TEST(ReserveBackpressure, GaugesTrackInUseAndReleasesAreReported) {
    kv_ctx_config_t cfg{};
    cfg.abi_version = KVCACHE_ABI_VERSION;
    cfg.tenant_id   = "g3-tenant";
    cfg.model_id    = "g3-model";
    kv_ctx_t* ctx = nullptr;
    ASSERT_EQ(kv_ctx_open(&cfg, &ctx), KV_OK);
    ASSERT_NE(ctx, nullptr);

    // Capacity gauge is set at Init — must report > 0 before we touch
    // anything. The default headless pool is 32 MiB / 4 MiB slots → 8.
    const double slots_total = Sample("kv_pinned_tier_slots_total");
    EXPECT_GT(slots_total, 0.0);

    const double in_use_before     = Sample("kv_pinned_tier_slots_in_use");
    const double reserves_before   = Sample("kv_reserves_total");
    const double nomem_before      = Sample("kv_reserve_nomem_total");

    const auto loc = TinyLocator();
    kv_handle_t h0 = 0;
    kv_buffer_desc_t s0{};
    ASSERT_EQ(kv_reserve(ctx, &loc, 1024 * 1024, &h0, &s0), KV_OK);
    kv_handle_t h1 = 0;
    kv_buffer_desc_t s1{};
    ASSERT_EQ(kv_reserve(ctx, &loc, 1024 * 1024, &h1, &s1), KV_OK);

    EXPECT_GE(Sample("kv_pinned_tier_slots_in_use"),
              in_use_before + 2.0 - 0.001)
        << "two successful Reserves should bump in_use gauge by 2";
    EXPECT_GE(Sample("kv_reserves_total"), reserves_before + 2.0);
    // No NOMEM yet.
    EXPECT_NEAR(Sample("kv_reserve_nomem_total"), nomem_before, 0.001);

    // Release one — gauge drops by exactly 1 (counter is monotonic).
    ASSERT_EQ(kv_release(ctx, h0), KV_OK);
    EXPECT_NEAR(Sample("kv_pinned_tier_slots_in_use"),
                in_use_before + 1.0, 0.001)
        << "Release should decrement in_use gauge";

    // Release the second so we don't leak handles into other tests.
    ASSERT_EQ(kv_release(ctx, h1), KV_OK);
    kv_ctx_close(ctx);
}

// Exhaust the pinned-slot pool and verify the NOMEM counter pops.
//
// The shared singleton's slot count depends on the in-tree default,
// but it is finite. We Reserve in a tight loop until KV_E_NOMEM hits
// at least once, then assert (a) the counter actually moved and
// (b) the utilization ratio reached 1.0 at the saturation moment.
TEST(ReserveBackpressure, NomemCounterFiresAtSaturation) {
    kv_ctx_config_t cfg{};
    cfg.abi_version = KVCACHE_ABI_VERSION;
    cfg.tenant_id   = "g3-saturate";
    cfg.model_id    = "g3-model";
    kv_ctx_t* ctx = nullptr;
    ASSERT_EQ(kv_ctx_open(&cfg, &ctx), KV_OK);

    const double nomem_before = Sample("kv_reserve_nomem_total");
    const double slots_total  = Sample("kv_pinned_tier_slots_total");

    std::vector<kv_handle_t> held;
    held.reserve(64);
    bool hit_nomem = false;
    const auto loc = TinyLocator();
    for (int i = 0; i < 64; ++i) {
        kv_handle_t h = 0;
        kv_buffer_desc_t slot{};
        const int rc = kv_reserve(ctx, &loc, 1024 * 1024, &h, &slot);
        if (rc == KV_E_NOMEM) { hit_nomem = true; break; }
        ASSERT_EQ(rc, KV_OK);
        held.push_back(h);
    }
    ASSERT_TRUE(hit_nomem) << "pool should saturate within 64 attempts; "
                              "actual slots_total=" << slots_total
                          << ", held=" << held.size();
    EXPECT_GE(Sample("kv_reserve_nomem_total"), nomem_before + 1.0);
    EXPECT_NEAR(Sample("kv_pinned_tier_slots_utilization_ratio"),
                1.0, 0.001)
        << "utilization should be 1.0 at the saturation moment";

    // Clean up so we don't leak slots into the singleton.
    for (auto h : held) kv_release(ctx, h);
    kv_ctx_close(ctx);
}

// Regression for the D-3 follow-on: kv_seal MUST release its
// pinned-tier slot even when the underlying ART Insert returns
// kPathConflict (which the headless `!rocks_` branch surfaces as
// KV_E_INTERNAL). Before the fix, the cleanup tail (wm_->Drop +
// buffers_->Release) lived only on the success path, so every
// conflict permanently leaked one pinned slot. With the default
// pool of ~8-64 slots, the pool was exhausted within ~30-60
// sequential Reserve→Publish→Seal cycles against a single ctx,
// and the bench_art_gc seed loop only got 30-60 out of 4096 seals
// through. Trigger: Seal a single-chunk path [A], then Seal a
// two-chunk path [A, B] whose first chunk is exactly A. The walk
// for the second insert reaches the terminal leaf at the
// path[0] slot and returns kPathConflict (LLD §3.2: MVP keeps
// leaves terminal — no edge-split). We then drive many more
// Reserve→Publish→Seal cycles and verify the in-use gauge
// returns to baseline (no cumulative leak).
TEST(ReserveBackpressure, SealKPathConflictReleasesSlot) {
    kv_ctx_config_t cfg{};
    cfg.abi_version = KVCACHE_ABI_VERSION;
    cfg.tenant_id   = "g3-pathconflict";
    cfg.model_id    = "g3-model";
    kv_ctx_t* ctx = nullptr;
    ASSERT_EQ(kv_ctx_open(&cfg, &ctx), KV_OK);

    const double in_use_baseline = Sample("kv_pinned_tier_slots_in_use");
    const double slots_total     = Sample("kv_pinned_tier_slots_total");
    ASSERT_GT(slots_total, 0.0);

    // Helper — drive Reserve → Publish → Seal for ``n_tokens`` tokens,
    // starting at ``token_seed``. Returns the kv_seal return code.
    auto seal_one = [&](uint32_t token_seed,
                         std::size_t n_tokens) -> int {
        kv_locator_t loc{};
        std::memset(&loc, 0, sizeof(loc));
        loc.range.token_count = static_cast<uint32_t>(n_tokens);
        loc.version           = 1;
        // Unique DRAM key per call so the DRAM tier doesn't reject the
        // staged bytes; the leak we're testing for is in the pinned
        // tier, not the DRAM tier.
        loc.prefix_hash[0] = static_cast<uint8_t>(token_seed       & 0xff);
        loc.prefix_hash[1] = static_cast<uint8_t>((token_seed >> 8) & 0xff);
        loc.prefix_hash[2] = static_cast<uint8_t>((token_seed >> 16) & 0xff);
        loc.prefix_hash[3] = static_cast<uint8_t>((token_seed >> 24) & 0xff);
        const std::size_t bytes_total = n_tokens * 64;
        kv_handle_t h = 0;
        kv_buffer_desc_t slot{};
        if (kv_reserve(ctx, &loc, bytes_total, &h, &slot) != KV_OK) {
            return KV_E_NOMEM;
        }
        if (slot.addr) std::memset(slot.addr, 0xAA, bytes_total);
        kv_buffer_desc_t empty{};
        kv_publish(ctx, h, empty, bytes_total);
        std::vector<uint32_t> tokens(n_tokens);
        // Deterministic, identical for any prefix that overlaps
        // [0, k) — feeds the path-prefix trigger.
        for (std::size_t i = 0; i < n_tokens; ++i) {
            tokens[i] = 0xC0FFEE00u + static_cast<uint32_t>(i) * 31u;
        }
        return kv_seal(ctx, h, tokens.data(), tokens.size());
    };

    // Seal a 16-token (one-chunk) path under seed=1. Leaf lands at the
    // path[0] slot.
    ASSERT_EQ(seal_one(/*token_seed=*/1, /*n_tokens=*/16), KV_OK);

    // Seal a 32-token (two-chunk) path under seed=2. The first chunk
    // (tokens[0..15]) matches the just-sealed path, so the inner-walk
    // reaches a terminal leaf at slot path[0] and returns kPathConflict
    // → headless Seal surfaces KV_E_INTERNAL. Pre-fix this leaked the
    // ingest slot; post-fix the cleanup tail still runs.
    EXPECT_EQ(seal_one(/*token_seed=*/2, /*n_tokens=*/32), KV_E_INTERNAL)
        << "expected kPathConflict to surface as KV_E_INTERNAL — if this "
           "starts returning KV_OK the ART grew edge-split support and "
           "the test no longer exercises the leak path";

    // In-use gauge MUST be at baseline. If the conflicting Seal leaked,
    // the gauge would sit 1 above baseline until ctx_close.
    EXPECT_NEAR(Sample("kv_pinned_tier_slots_in_use"),
                in_use_baseline, 0.001)
        << "kPathConflict Seal leaked its pinned-tier slot";

    // Drive enough additional Seals that the cumulative leak would
    // exhaust the pool (~slots_total + 16 attempts). Pre-fix this
    // started returning NOMEM partway through; post-fix every Reserve
    // succeeds because slots get returned on EVERY Seal exit path.
    const int extra_seals = static_cast<int>(slots_total) + 16;
    int extra_ok = 0;
    int extra_conflict = 0;
    for (int i = 0; i < extra_seals; ++i) {
        const int rc = seal_one(/*token_seed=*/static_cast<uint32_t>(100 + i),
                                  /*n_tokens=*/16);
        if (rc == KV_OK) ++extra_ok;
        else if (rc == KV_E_INTERNAL) ++extra_conflict;
        else FAIL() << "iter " << i << " Reserve/Seal unexpected rc=" << rc;
    }
    // The majority must succeed — collisions on a single-byte
    // first-byte radix happen at the birthday-paradox rate (~N²/512)
    // but the leak-fix means the slot is returned regardless.
    EXPECT_GT(extra_ok, extra_seals / 2)
        << "post-fix the majority of sustained seals must succeed; "
           "extra_ok=" << extra_ok << " extra_conflict=" << extra_conflict;

    // Final in-use snapshot — pool returns to baseline.
    EXPECT_NEAR(Sample("kv_pinned_tier_slots_in_use"),
                in_use_baseline, 0.001)
        << "cumulative leak after " << extra_seals
        << " seals — pool drift = "
        << (Sample("kv_pinned_tier_slots_in_use") - in_use_baseline);

    kv_ctx_close(ctx);
}
