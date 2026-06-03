// Phase ABI-1 — BuildCtxOptions implementation.
#include "ctx_options.h"

#include <cstdint>
#include <cstdlib>

namespace kvcache::abi {

namespace {

// Built-in defaults — the demo / unit-test sizing that kv_ctx_open has
// always used before any env or tuning overrides.
HeadlessNode::Options DefaultOptions() {
    HeadlessNode::Options opts{};
    opts.tier.pinned.pool_bytes = 32ull << 20;  // 32 MiB
    opts.tier.pinned.slot_bytes = 4ull << 20;   //  4 MiB per slot
    opts.tier.pinned.use_mlock  = false;
    opts.tier.dram.capacity_bytes    = 64ull << 20;
    opts.tier.dram.a1out_max_entries = 1024;
    opts.tier.enable_nvme = false;
    opts.tier.enable_cold = false;
    opts.nixl_backend = "loopback";
    return opts;
}

// Layer 2 — KVCACHE_NIXL_* environment overrides (historical behaviour).
void ApplyEnvOverrides(HeadlessNode::Options* opts) {
    if (const char* b = std::getenv("KVCACHE_NIXL_BACKEND"); b && *b) {
        opts->nixl_backend = b;
    }
    if (const char* h = std::getenv("KVCACHE_NIXL_BIND_HOST"); h && *h) {
        opts->nixl_bind_host = h;
    }
    if (const char* p = std::getenv("KVCACHE_NIXL_BIND_PORT"); p && *p) {
        opts->nixl_bind_port = static_cast<uint32_t>(std::atoi(p));
    }
    // Presence (even "0") counts as an explicit override.
    if (const char* s = std::getenv("KVCACHE_NIXL_SEGMENT_BYTES"); s && *s) {
        opts->nixl_segment_bytes_set = true;
        opts->nixl_segment_bytes =
            static_cast<uint64_t>(std::strtoull(s, nullptr, 10));
    }
}

// Layer 3 — explicit kv_ctx_tuning_t fields (an unset sentinel skips the
// field, leaving the value chosen by the env/default layers).
void ApplyTuning(HeadlessNode::Options* opts, const kv_ctx_tuning_t* t) {
    if (t->nixl_backend && *t->nixl_backend)     opts->nixl_backend   = t->nixl_backend;
    if (t->nixl_bind_host && *t->nixl_bind_host) opts->nixl_bind_host = t->nixl_bind_host;
    if (t->nixl_bind_port != 0)                  opts->nixl_bind_port = t->nixl_bind_port;
    if (t->nixl_segment_bytes_set) {
        opts->nixl_segment_bytes_set = true;
        opts->nixl_segment_bytes     = t->nixl_segment_bytes;
    }
    if (t->pinned_pool_bytes != 0)   opts->tier.pinned.pool_bytes   = t->pinned_pool_bytes;
    if (t->pinned_slot_bytes != 0)   opts->tier.pinned.slot_bytes   = t->pinned_slot_bytes;
    if (t->dram_capacity_bytes != 0) opts->tier.dram.capacity_bytes = t->dram_capacity_bytes;
}

}  // namespace

HeadlessNode::Options BuildCtxOptions(const kv_ctx_tuning_t* tuning) {
    HeadlessNode::Options opts = DefaultOptions();
    ApplyEnvOverrides(&opts);
    if (tuning) ApplyTuning(&opts, tuning);
    return opts;
}

}  // namespace kvcache::abi
