// libkvcache.so — implementation of the public C ABI.
// LLD §6.1.2.
//
// Backend: the in-process HeadlessNode (see headless_node.h). The Step-7+
// cross-process backend (shmem ring to kvagent) will replace this TU while
// keeping the public ABI identical.
#include "kvcache/kv_abi.h"

#include <cstring>
#include <string>

#include <cstdlib>

#include "headless_node.h"
#include "kvcache/kv_errors.h"
#include "transport/nixl_wrapper.h"

extern "C" {

#define KV_API __attribute__((visibility("default")))

struct kv_ctx_s {
    std::string  tenant_id;
    std::string  model_id;
    uint64_t     model_id_hash  = 0;
    // FNV-1a 64-bit hash of `tenant_id`, computed at ctx open and used to
    // route the caller's Pulls into a per-tenant bucket inside the
    // PriorityScheduler (Phase E-3). 0 if `tenant_id` is empty — that
    // collides with kSystemTenantHash, which is the intended "system /
    // unscoped traffic" bucket.
    uint64_t     tenant_id_hash = 0;
    kvcache::abi::HeadlessNode* node = nullptr;
    // At most one active subscription per ctx (Phase M-2). The C ABI
    // returns KV_E_BUSY on a second concurrent subscribe.
    kvcache::abi::HeadlessNode::SubscriptionId sub_id = 0;
};

namespace {

// FNV-1a 64-bit hash. Same primitives as model_id_hash; deliberately not
// cryptographic — the scheduler only needs a stable, distinct-per-string
// identifier for fair RR rotation.
uint64_t Fnv1a64(const std::string& s) {
    uint64_t h = 0xcbf29ce484222325ULL;
    for (char ch : s) {
        h ^= static_cast<uint8_t>(ch);
        h *= 0x100000001b3ULL;
    }
    return h;
}

// Phase M-4 — read NIXL backend selection from the environment so
// tests (and on-prem operators) can flip "loopback" -> "tcp" without
// recompiling. Defaults match the demo / unit-test path.
void ApplyNixlEnvOverrides(kvcache::abi::HeadlessNode::Options* opts) {
    if (const char* b = std::getenv("KVCACHE_NIXL_BACKEND"); b && *b) {
        opts->nixl_backend = b;
    }
    if (const char* h = std::getenv("KVCACHE_NIXL_BIND_HOST"); h && *h) {
        opts->nixl_bind_host = h;
    }
    if (const char* p = std::getenv("KVCACHE_NIXL_BIND_PORT"); p && *p) {
        opts->nixl_bind_port = static_cast<uint32_t>(std::atoi(p));
    }
}

}  // namespace

KV_API int kv_ctx_open(const kv_ctx_config_t* cfg, kv_ctx_t** out_ctx) {
    if (!cfg || !out_ctx) return KV_E_INVAL;
    if (cfg->abi_version != KVCACHE_ABI_VERSION) return KV_E_VERSION_MISMATCH;

    kvcache::abi::HeadlessNode::Options opts{};
    // Sensible defaults for headless / demo bring-up. Production callers
    // override via environment variables (TODO(stephen): expose options on
    // kv_ctx_config_t).
    opts.tier.pinned.pool_bytes = 32ull << 20;   // 32 MiB
    opts.tier.pinned.slot_bytes =  4ull << 20;   //  4 MiB per slot
    opts.tier.pinned.use_mlock  = false;
    opts.tier.dram.capacity_bytes    = 64ull << 20;
    opts.tier.dram.a1out_max_entries = 1024;
    opts.tier.enable_nvme = false;
    opts.tier.enable_cold = false;
    opts.nixl_backend = "loopback";
    ApplyNixlEnvOverrides(&opts);

    std::string err;
    auto* node = kvcache::abi::HeadlessNode::GetOrCreate(opts, &err);
    if (!node) return KV_E_INTERNAL;

    auto c = new kv_ctx_s();
    if (cfg->tenant_id) c->tenant_id = cfg->tenant_id;
    if (cfg->model_id)  c->model_id  = cfg->model_id;
    c->model_id_hash  = Fnv1a64(c->model_id);
    c->tenant_id_hash = c->tenant_id.empty() ? 0 : Fnv1a64(c->tenant_id);
    c->node = node;
    *out_ctx = c;
    return KV_OK;
}

KV_API int kv_ctx_open_from_hashes(int32_t abi_version,
                                    uint64_t tenant_id_hash,
                                    uint64_t model_id_hash,
                                    uint32_t /*flags*/,
                                    kv_ctx_t** out_ctx) {
    if (!out_ctx) return KV_E_INVAL;
    if (abi_version != KVCACHE_ABI_VERSION) return KV_E_VERSION_MISMATCH;

    // Same headless-node singleton as kv_ctx_open; identity is carried
    // entirely through ctx->tenant_id_hash + ctx->model_id_hash so the
    // string fields stay empty.
    kvcache::abi::HeadlessNode::Options opts{};
    opts.tier.pinned.pool_bytes = 32ull << 20;
    opts.tier.pinned.slot_bytes =  4ull << 20;
    opts.tier.pinned.use_mlock  = false;
    opts.tier.dram.capacity_bytes    = 64ull << 20;
    opts.tier.dram.a1out_max_entries = 1024;
    opts.tier.enable_nvme = false;
    opts.tier.enable_cold = false;
    opts.nixl_backend = "loopback";
    ApplyNixlEnvOverrides(&opts);

    std::string err;
    auto* node = kvcache::abi::HeadlessNode::GetOrCreate(opts, &err);
    if (!node) return KV_E_INTERNAL;

    auto c = new kv_ctx_s();
    c->model_id_hash  = model_id_hash;
    c->tenant_id_hash = tenant_id_hash;
    c->node           = node;
    *out_ctx = c;
    return KV_OK;
}

KV_API int kv_ctx_close(kv_ctx_t* ctx) {
    if (ctx && ctx->sub_id && ctx->node) {
        ctx->node->UnsubscribeEvents(ctx->sub_id);
        ctx->sub_id = 0;
    }
    delete ctx;
    return KV_OK;
}

KV_API int kv_lookup(kv_ctx_t* ctx, const uint32_t* tokens, size_t n,
                    kv_locator_t* meta, kv_handle_t* handle,
                    uint32_t* matched_tokens) {
    if (!ctx || !ctx->node) return KV_E_INVAL;
    return ctx->node->Lookup(ctx->tenant_id.c_str(), ctx->model_id_hash,
                              tokens, n, meta, handle, matched_tokens);
}

KV_API int kv_reserve(kv_ctx_t* ctx, const kv_locator_t* locator, size_t bytes,
                     kv_handle_t* handle, kv_buffer_desc_t* slot) {
    if (!ctx || !ctx->node) return KV_E_INVAL;
    return ctx->node->Reserve(locator, bytes, handle, slot);
}

KV_API int kv_publish(kv_ctx_t* ctx, kv_handle_t handle, kv_buffer_desc_t src,
                     uint64_t watermark) {
    if (!ctx || !ctx->node) return KV_E_INVAL;
    return ctx->node->Publish(handle, src, watermark);
}

KV_API int kv_fetch(kv_ctx_t* ctx, kv_handle_t handle,
                   const kv_range_t* ranges, size_t n,
                   kv_buffer_desc_t dst, kv_completion_t* completion) {
    if (!ctx || !ctx->node) return KV_E_INVAL;
    return ctx->node->Fetch(handle, ctx->tenant_id_hash,
                             ranges, n, dst, completion);
}

KV_API int kv_wait(kv_ctx_t* ctx, kv_completion_t cid, uint32_t timeout_ms) {
    if (!ctx || !ctx->node) return KV_E_INVAL;
    return ctx->node->Wait(cid, timeout_ms);
}

KV_API int kv_seal(kv_ctx_t* ctx, kv_handle_t handle,
                  const uint32_t* tokens, size_t n_tokens) {
    if (!ctx || !ctx->node) return KV_E_INVAL;
    return ctx->node->Seal(handle, tokens, n_tokens);
}

KV_API int kv_release(kv_ctx_t* ctx, kv_handle_t handle) {
    if (!ctx || !ctx->node) return KV_E_INVAL;
    return ctx->node->Release(handle);
}

KV_API int kv_subscribe_events(kv_ctx_t* ctx, kv_event_callback_t cb,
                                  void* user) {
    if (!ctx || !ctx->node || !cb) return KV_E_INVAL;
    if (ctx->sub_id != 0) return KV_E_BUSY;
    const auto id = ctx->node->SubscribeEvents(cb, user);
    if (id == 0) return KV_E_INTERNAL;
    ctx->sub_id = id;
    return KV_OK;
}

KV_API int kv_unsubscribe_events(kv_ctx_t* ctx) {
    if (!ctx || !ctx->node) return KV_E_INVAL;
    if (ctx->sub_id == 0) return KV_OK;  // idempotent
    ctx->node->UnsubscribeEvents(ctx->sub_id);
    ctx->sub_id = 0;
    return KV_OK;
}

KV_API int kv_export_mr(kv_ctx_t* ctx, uint32_t local_mr_key,
                         uint8_t* out_buf, size_t buf_cap, size_t* out_len) {
    if (!ctx || !ctx->node || !out_len) return KV_E_INVAL;
    auto* be = ctx->node->backend();
    if (!be) return KV_E_INTERNAL;
    kvcache::node::transport::RemoteMrDescriptor desc;
    std::string err;
    if (!be->ExportMr(local_mr_key, &desc, &err)) return KV_E_INVAL;
    *out_len = desc.opaque.size();
    if (!out_buf || buf_cap < desc.opaque.size()) return KV_E_NOMEM;
    std::memcpy(out_buf, desc.opaque.data(), desc.opaque.size());
    return KV_OK;
}

KV_API int kv_import_remote_mr(kv_ctx_t* ctx, const uint8_t* buf,
                                size_t len, uint32_t* out_mr_key) {
    if (!ctx || !ctx->node || !buf || !out_mr_key) return KV_E_INVAL;
    auto* be = ctx->node->backend();
    if (!be) return KV_E_INTERNAL;
    kvcache::node::transport::RemoteMrDescriptor desc;
    desc.opaque.assign(buf, buf + len);
    std::string err;
    auto k = be->ImportRemoteMr(desc, &err);
    if (k == kvcache::node::transport::kInvalidMrKey) return KV_E_INVAL;
    *out_mr_key = k;
    return KV_OK;
}

KV_API int kv_register_local_mr(kv_ctx_t* ctx, void* addr, size_t bytes,
                                 uint32_t* out_mr_key) {
    if (!ctx || !ctx->node || !addr || bytes == 0 || !out_mr_key) {
        return KV_E_INVAL;
    }
    auto* be = ctx->node->backend();
    if (!be) return KV_E_INTERNAL;
    std::string err;
    auto k = be->RegisterRegion(addr, bytes, &err);
    if (k == kvcache::node::transport::kInvalidMrKey) return KV_E_TRANSPORT;
    *out_mr_key = k;
    return KV_OK;
}

KV_API int kv_unregister_local_mr(kv_ctx_t* ctx, uint32_t mr_key) {
    if (!ctx || !ctx->node) return KV_E_INVAL;
    auto* be = ctx->node->backend();
    if (!be) return KV_E_INTERNAL;
    be->UnregisterRegion(mr_key);
    return KV_OK;
}

}  // extern "C"
