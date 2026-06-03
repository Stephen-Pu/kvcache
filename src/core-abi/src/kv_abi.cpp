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

#include "ctx_options.h"  // Phase ABI-1 — BuildCtxOptions
#include "headless_node.h"
#include "kvcache/kv_errors.h"
#include "metrics.h"  // Phase G-3 — kv_metrics_scrape ABI
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

}  // namespace

KV_API int kv_ctx_open(const kv_ctx_config_t* cfg, kv_ctx_t** out_ctx) {
    if (!cfg || !out_ctx) return KV_E_INVAL;
    if (cfg->abi_version != KVCACHE_ABI_VERSION) return KV_E_VERSION_MISMATCH;

    // Defaults → env overrides → explicit kv_ctx_tuning_t (Phase ABI-1).
    auto opts = kvcache::abi::BuildCtxOptions(cfg->tuning);

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
    // string fields stay empty. This entry point has no config struct, so
    // it uses defaults + env only (no per-call tuning).
    auto opts = kvcache::abi::BuildCtxOptions(nullptr);

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
    return ctx->node->Lookup(ctx->tenant_id.c_str(),
                              ctx->tenant_id_hash, ctx->model_id_hash,
                              tokens, n, meta, handle, matched_tokens);
}

KV_API int kv_reserve(kv_ctx_t* ctx, const kv_locator_t* locator, size_t bytes,
                     kv_handle_t* handle, kv_buffer_desc_t* slot) {
    if (!ctx || !ctx->node) return KV_E_INVAL;
    return ctx->node->Reserve(locator, bytes,
                               ctx->tenant_id_hash, ctx->model_id_hash,
                               handle, slot);
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

KV_API int kv_fetch_with_priority(kv_ctx_t* ctx, kv_handle_t handle,
                                   const kv_range_t* ranges, size_t n,
                                   kv_buffer_desc_t dst,
                                   kv_priority_t priority,
                                   kv_completion_t* completion) {
    if (!ctx || !ctx->node) return KV_E_INVAL;
    return ctx->node->FetchWithPriority(
        handle, ctx->tenant_id_hash, ranges, n, dst,
        static_cast<int>(priority), completion);
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

KV_API int kv_metrics_scrape(char* buf, size_t cap, size_t* out_len) {
    if (!out_len && (cap == 0 || !buf)) return KV_E_INVAL;
    // Phase D-3 — refresh ART / epoch-reclamation gauges on demand
    // (they take the EpochManager mutex, so they're not bumped on
    // every Seal / Release). Safe to call when no node is active —
    // the gauges keep their last value (0 after Rm()'s seed pass).
    if (auto* n = kvcache::abi::HeadlessNode::Active()) {
        n->RefreshArtGauges();
    }
    std::string body;
    kvcache::metrics::Registry::Default().Scrape(body);
    if (out_len) *out_len = body.size();
    if (buf && cap > 0) {
        const size_t copy = std::min(cap - 1, body.size());
        std::memcpy(buf, body.data(), copy);
        buf[copy] = '\0';
    }
    return KV_OK;
}

}  // extern "C"
