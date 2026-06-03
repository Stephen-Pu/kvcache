// Phase ABI-1 — BuildCtxOptions precedence tests.
//
// Pure option-building logic, exercised without the HeadlessNode process
// singleton: built-in defaults < KVCACHE_NIXL_* env < kv_ctx_tuning_t.
#include "ctx_options.h"

#include <gtest/gtest.h>

#include <cstdlib>

#include "kvcache/kv_abi.h"

using kvcache::abi::BuildCtxOptions;

namespace {

// RAII env setter that restores the previous value on scope exit so tests
// don't leak state into each other.
class ScopedEnv {
   public:
    ScopedEnv(const char* k, const char* v) : key_(k) {
        if (const char* old = std::getenv(k)) {
            had_ = true;
            prev_ = old;
        }
        ::setenv(k, v, 1);
    }
    ~ScopedEnv() {
        if (had_) ::setenv(key_, prev_.c_str(), 1);
        else      ::unsetenv(key_);
    }

   private:
    const char* key_;
    bool        had_ = false;
    std::string prev_;
};

// Make sure no ambient NIXL env leaks into the default-path assertions.
void ClearNixlEnv() {
    ::unsetenv("KVCACHE_NIXL_BACKEND");
    ::unsetenv("KVCACHE_NIXL_BIND_HOST");
    ::unsetenv("KVCACHE_NIXL_BIND_PORT");
    ::unsetenv("KVCACHE_NIXL_SEGMENT_BYTES");
}

}  // namespace

TEST(BuildCtxOptions, DefaultsWhenNoTuningNoEnv) {
    ClearNixlEnv();
    auto o = BuildCtxOptions(nullptr);
    EXPECT_EQ(o.nixl_backend, "loopback");
    EXPECT_EQ(o.nixl_bind_host, "127.0.0.1");
    EXPECT_EQ(o.nixl_bind_port, 0u);
    EXPECT_FALSE(o.nixl_segment_bytes_set);
    EXPECT_EQ(o.tier.pinned.pool_bytes, 32ull << 20);
    EXPECT_EQ(o.tier.pinned.slot_bytes, 4ull << 20);
    EXPECT_EQ(o.tier.dram.capacity_bytes, 64ull << 20);
}

TEST(BuildCtxOptions, EnvOverridesDefaults) {
    ClearNixlEnv();
    ScopedEnv b("KVCACHE_NIXL_BACKEND", "tcp");
    ScopedEnv h("KVCACHE_NIXL_BIND_HOST", "10.0.0.5");
    ScopedEnv p("KVCACHE_NIXL_BIND_PORT", "5555");
    ScopedEnv s("KVCACHE_NIXL_SEGMENT_BYTES", "0");  // presence counts
    auto o = BuildCtxOptions(nullptr);
    EXPECT_EQ(o.nixl_backend, "tcp");
    EXPECT_EQ(o.nixl_bind_host, "10.0.0.5");
    EXPECT_EQ(o.nixl_bind_port, 5555u);
    EXPECT_TRUE(o.nixl_segment_bytes_set);
    EXPECT_EQ(o.nixl_segment_bytes, 0u);
}

TEST(BuildCtxOptions, TuningOverridesDefaults) {
    ClearNixlEnv();
    kv_ctx_tuning_t t{};
    t.nixl_backend = "tcp";
    t.nixl_bind_host = "192.168.1.1";
    t.nixl_bind_port = 9001;
    t.nixl_segment_bytes = 65536;
    t.nixl_segment_bytes_set = 1;
    t.pinned_pool_bytes = 8ull << 20;
    t.pinned_slot_bytes = 1ull << 20;
    t.dram_capacity_bytes = 16ull << 20;
    auto o = BuildCtxOptions(&t);
    EXPECT_EQ(o.nixl_backend, "tcp");
    EXPECT_EQ(o.nixl_bind_host, "192.168.1.1");
    EXPECT_EQ(o.nixl_bind_port, 9001u);
    EXPECT_TRUE(o.nixl_segment_bytes_set);
    EXPECT_EQ(o.nixl_segment_bytes, 65536u);
    EXPECT_EQ(o.tier.pinned.pool_bytes, 8ull << 20);
    EXPECT_EQ(o.tier.pinned.slot_bytes, 1ull << 20);
    EXPECT_EQ(o.tier.dram.capacity_bytes, 16ull << 20);
}

TEST(BuildCtxOptions, TuningWinsOverEnv) {
    ClearNixlEnv();
    ScopedEnv b("KVCACHE_NIXL_BACKEND", "tcp");
    ScopedEnv p("KVCACHE_NIXL_BIND_PORT", "1111");
    kv_ctx_tuning_t t{};
    t.nixl_backend = "loopback";  // explicit struct beats ambient env
    t.nixl_bind_port = 2222;
    auto o = BuildCtxOptions(&t);
    EXPECT_EQ(o.nixl_backend, "loopback");
    EXPECT_EQ(o.nixl_bind_port, 2222u);
}

TEST(BuildCtxOptions, UnsetTuningFieldsFallThroughToEnv) {
    ClearNixlEnv();
    ScopedEnv b("KVCACHE_NIXL_BACKEND", "tcp");
    kv_ctx_tuning_t t{};  // all-zero: backend NULL, ports 0
    t.pinned_pool_bytes = 4ull << 20;  // only override the pool
    auto o = BuildCtxOptions(&t);
    EXPECT_EQ(o.nixl_backend, "tcp");           // from env (tuning left NULL)
    EXPECT_EQ(o.tier.pinned.pool_bytes, 4ull << 20);  // from tuning
    EXPECT_EQ(o.tier.pinned.slot_bytes, 4ull << 20);  // default (untouched)
}

TEST(BuildCtxOptions, ZeroSizeFieldsKeepDefaults) {
    ClearNixlEnv();
    kv_ctx_tuning_t t{};  // pinned_pool_bytes = 0 → keep default
    auto o = BuildCtxOptions(&t);
    EXPECT_EQ(o.tier.pinned.pool_bytes, 32ull << 20);
    EXPECT_EQ(o.tier.dram.capacity_bytes, 64ull << 20);
}
