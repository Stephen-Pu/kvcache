/*
 * KV Cache — stable C ABI.
 *
 * LLD reference: §6.1.2 Core ABI. This is the single integration point for
 * inference engines (vLLM / SGLang / TRT-LLM / AIBrix). All engine-specific
 * adapters call through this surface.
 *
 * Design tenets (LLD §6.1.2):
 *   - Async-first: every data-plane call returns a completion handle.
 *   - Zero-copy: caller-owned buffers are registered with NIXL once and reused.
 *   - Lookup and Fetch are separate calls — lookup is cheap (≤10µs, LLD §3.2),
 *     fetch is the actual transfer.
 *   - Tier is never exposed to the engine; engine sees only "hit" / "miss".
 */
#ifndef KVCACHE_KV_ABI_H
#define KVCACHE_KV_ABI_H

#include "kvcache/kv_errors.h"
#include "kvcache/kv_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ABI version. Bumped on any change to struct layouts or function semantics. */
#define KVCACHE_ABI_VERSION 1

/* ------------------------------------------------------------------------- */
/* Context lifecycle                                                         */
/* ------------------------------------------------------------------------- */

typedef struct {
    int32_t      abi_version;     /* must equal KVCACHE_ABI_VERSION         */
    const char*  agent_endpoint;  /* UNIX socket / shmem path to KVAgent    */
    const char*  tenant_id;       /* mTLS-validated tenant identifier       */
    const char*  model_id;        /* canonical model_id (LLD §2.1)          */
    uint32_t     flags;           /* reserved                               */
} kv_ctx_config_t;

int kv_ctx_open (const kv_ctx_config_t* cfg, kv_ctx_t** out_ctx);
int kv_ctx_close(kv_ctx_t* ctx);

/*
 * kv_ctx_open_from_hashes — variant of kv_ctx_open used by the gRPC
 * NodeData service (Phase M-3 A). The wire only carries
 * `model_id_hash` (fixed64), never the canonical `model_id` string, so
 * the service can't reconstruct the same hash via FNV-1a on its end.
 * This entry point lets the caller supply both hashes directly and
 * leaves the string fields empty.
 *
 * `tenant_id_hash` plumbs through to the PriorityScheduler so distinct
 * tenants land in distinct round-robin buckets. Pass the same value
 * the engine-side ABI would compute via FNV-1a over the tenant string.
 */
int kv_ctx_open_from_hashes(int32_t  abi_version,
                            uint64_t tenant_id_hash,
                            uint64_t model_id_hash,
                            uint32_t flags,
                            kv_ctx_t** out_ctx);

/* ------------------------------------------------------------------------- */
/* Hot-path data-plane operations (LLD §6.1.2)                               */
/* ------------------------------------------------------------------------- */

/*
 * kv_lookup — longest-prefix-match over the in-memory ART (LLD §3.2).
 *
 * Inputs:
 *   tokens, n       : the request's token prefix
 * Outputs:
 *   meta            : Locator of the longest matching sealed prefix
 *   handle          : opaque handle to be passed to subsequent kv_fetch
 *   matched_tokens  : number of tokens covered by the match
 *
 * Returns KV_OK on hit; KV_E_NOT_FOUND on miss.
 * Latency target: p99 ≤ 10µs (LLD §9.1).
 */
int kv_lookup(kv_ctx_t* ctx,
              const uint32_t* tokens, size_t n,
              kv_locator_t* meta,
              kv_handle_t* handle,
              uint32_t* matched_tokens);

/*
 * kv_reserve — allocate a mutable buffer slot on the owning node (LLD §3.4).
 * Used at the start of streaming write. Slot lives in T1 Pinned memory and is
 * exposed to the caller via 'slot' as a NIXL-registered region.
 */
int kv_reserve(kv_ctx_t* ctx,
               const kv_locator_t* locator,
               size_t bytes,
               kv_handle_t* handle,
               kv_buffer_desc_t* slot);

/*
 * kv_publish — advance the watermark for an in-progress streaming write.
 * Caller wrote bytes [0, watermark) into the slot returned by kv_reserve.
 * The server may begin NIXL Pull at any time after publish.
 */
int kv_publish(kv_ctx_t* ctx,
               kv_handle_t handle,
               kv_buffer_desc_t src,
               uint64_t watermark);

/*
 * kv_fetch — async transfer of one or more ranges into 'dst'.
 * Returns immediately with a completion handle. Caller must kv_wait().
 * Internal tier resolution and Priority Scheduler placement happens here
 * (LLD §3.5 / §5.1).
 */
int kv_fetch(kv_ctx_t* ctx,
             kv_handle_t handle,
             const kv_range_t* ranges, size_t n,
             kv_buffer_desc_t dst,
             kv_completion_t* completion);

/*
 * kv_wait — block until 'completion' is done or 'timeout_ms' elapses.
 * Returns KV_OK on completion, KV_E_TIMEOUT otherwise.
 */
int kv_wait(kv_ctx_t* ctx, kv_completion_t completion, uint32_t timeout_ms);

/*
 * kv_seal — atomically seal a streaming-write handle, making it visible to
 * future lookups and emitting a KV_EVENT_ADD (LLD §3.4 atomic sealing).
 *
 * The caller passes the full token sequence covered by this write so the
 * server can compute the chunk_path used for LPM. Only whole 16-token chunks
 * become indexed; any tail < 16 tokens is dropped per LLD §3.2.
 */
int kv_seal(kv_ctx_t* ctx,
            kv_handle_t handle,
            const uint32_t* tokens, size_t n_tokens);

/*
 * kv_release — drop the caller's reference. The owning node decrements the
 * per-node refcount; the chunk becomes eviction-eligible at refcount 0.
 */
int kv_release(kv_ctx_t* ctx, kv_handle_t handle);

/* ------------------------------------------------------------------------- */
/* Event subscription (LLD §2.2)                                             */
/* ------------------------------------------------------------------------- */

typedef void (*kv_event_callback_t)(const kv_event_t* event, void* user);

/* Subscribe to KV events on this context's node. The callback fires on
 * a dedicated poller thread; do not block inside it for more than a
 * few microseconds. Returns KV_OK on success, KV_E_BUSY if a
 * subscription is already active on this context (one subscription
 * per ctx — open a second ctx if you need a second sink).            */
int kv_subscribe_events(kv_ctx_t* ctx,
                        kv_event_callback_t cb,
                        void* user);

/* Stop the active subscription on this context. Idempotent; safe to
 * call even when no subscription exists. Blocks until the poller
 * thread observes the cancel and exits.                              */
int kv_unsubscribe_events(kv_ctx_t* ctx);

/* ------------------------------------------------------------------------- */
/* NIXL RemoteMrDescriptor exchange (Phase M-3 B)                            */
/* ------------------------------------------------------------------------- */

/* Export `local_mr_key` (a key previously returned by Reserve.slot.mr_key
 * or by an internal RegisterRegion) as an opaque descriptor that a peer
 * backend can import via kv_import_remote_mr. On success returns KV_OK
 * and writes the descriptor bytes into `out_buf` up to `buf_cap`, with
 * the true length written to `*out_len`. If `out_buf` is null or
 * `buf_cap < *out_len`, returns KV_E_NOMEM with `*out_len` set so the
 * caller can size a second call. */
int kv_export_mr(kv_ctx_t* ctx,
                 uint32_t  local_mr_key,
                 uint8_t*  out_buf,
                 size_t    buf_cap,
                 size_t*   out_len);

/* Import a peer's descriptor (typically received from kv_export_mr on
 * another node) into this ctx's NIXL backend, yielding a local mr_key
 * usable as `dst.mr_key` in subsequent kv_fetch calls. Returns KV_OK
 * and writes the key to *out_mr_key on success. */
int kv_import_remote_mr(kv_ctx_t*      ctx,
                        const uint8_t* buf,
                        size_t         len,
                        uint32_t*      out_mr_key);

/* Register a local memory region with the ctx's NIXL backend.
 * Engines typically register a long-lived fetch destination buffer
 * once at startup and pass the resulting key in
 * `kv_buffer_desc_t::mr_key` for every kv_fetch — so the hot path
 * never registers / unregisters per call (Phase M-5). The key is
 * stable until kv_unregister_local_mr is called (or kv_ctx_close on
 * the owning ctx). */
int kv_register_local_mr(kv_ctx_t* ctx,
                          void*     addr,
                          size_t    bytes,
                          uint32_t* out_mr_key);

/* Drop a key previously returned by kv_register_local_mr or
 * kv_import_remote_mr. Idempotent on unknown keys. */
int kv_unregister_local_mr(kv_ctx_t* ctx, uint32_t mr_key);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* KVCACHE_KV_ABI_H */
