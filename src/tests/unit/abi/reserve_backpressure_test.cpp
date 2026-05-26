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
