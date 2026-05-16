/*
 * KV Cache — public type definitions for the Core ABI.
 *
 * LLD reference: §2.1 Locator (64-byte wire format), §6.1.2 Core ABI.
 *
 * This header is consumed by:
 *   - libkvcache.so (C++ implementation in src/core-abi/)
 *   - Engine adapters (Python via cffi, C++ direct include)
 *
 * ABI stability: any change here is a versioned, additive change.
 */
#ifndef KVCACHE_KV_TYPES_H
#define KVCACHE_KV_TYPES_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------------- */
/* Handles                                                                   */
/* ------------------------------------------------------------------------- */

typedef uint64_t kv_handle_t;
typedef uint64_t kv_completion_t;
typedef struct kv_ctx_s kv_ctx_t; /* opaque */

/* ------------------------------------------------------------------------- */
/* Memory descriptor                                                         */
/* ------------------------------------------------------------------------- */

/* Memory types the engine may pass in (LLD §3.5 NIXL backend selection). */
typedef enum {
    KV_MEM_HOST       = 0, /* malloc / regular host memory                  */
    KV_MEM_HOST_PINNED= 1, /* cudaHostAlloc / mlocked                       */
    KV_MEM_CUDA       = 2, /* device pointer                                */
    KV_MEM_ROCM       = 3, /* ROCm device pointer (reserved)                */
} kv_mem_type_t;

typedef struct {
    void*    addr;
    size_t   len;
    int32_t  mem_type;  /* kv_mem_type_t                                    */
    uint32_t mr_key;    /* NIXL-registered memory key, 0 if not registered  */
} kv_buffer_desc_t;

/* ------------------------------------------------------------------------- */
/* Range — three-dimensional slice over (layer, head, token).                */
/* LLD §2.1: shape of every transferable KV chunk.                           */
/* ------------------------------------------------------------------------- */

typedef struct {
    uint16_t layer_start;
    uint16_t layer_count;
    uint16_t head_start;
    uint16_t head_count;
    uint32_t token_start;
    uint32_t token_count;
} kv_range_t;

/* ------------------------------------------------------------------------- */
/* Locator — 64-byte binary identity for any KV chunk (LLD §2.1).            */
/*                                                                           */
/*   tenant_id      : 16B  (UUID)                                            */
/*   model_id_hash  :  8B  (name + weights_hash + quant + tokenizer_hash)    */
/*   prefix_hash    : 16B  (BLAKE3 truncated; identifies token-prefix path)  */
/*   range          : 16B  (layer/head/token slice)                          */
/*   version        :  4B  (schema version, currently 1)                     */
/*   flags          :  4B  (reserved)                                        */
/*                                                                           */
/* Wire-format: little-endian, packed, total = 64 bytes.                     */
/* ------------------------------------------------------------------------- */

#define KV_LOCATOR_SIZE 64

typedef struct {
    uint8_t   tenant_id[16];
    uint64_t  model_id_hash;
    uint8_t   prefix_hash[16];
    kv_range_t range;
    uint32_t  version;
    uint32_t  flags;
} kv_locator_t;

/* Portable static assertion: `_Static_assert` is C11, `static_assert` is
 * C++11. Use the right one based on language. */
#ifdef __cplusplus
static_assert(sizeof(kv_locator_t) == KV_LOCATOR_SIZE,
              "kv_locator_t must be exactly 64 bytes (LLD §2.1)");
#else
_Static_assert(sizeof(kv_locator_t) == KV_LOCATOR_SIZE,
               "kv_locator_t must be exactly 64 bytes (LLD §2.1)");
#endif

/* ------------------------------------------------------------------------- */
/* Events — surfaced via kv_subscribe_events (LLD §2.2 KV Event schema).     */
/* ------------------------------------------------------------------------- */

typedef enum {
    KV_EVENT_ADD     = 1, /* sealed chunk became visible                    */
    KV_EVENT_EVICT   = 2, /* chunk removed from a tier                      */
    KV_EVENT_PROMOTE = 3, /* chunk promoted to a hotter tier                */
    KV_EVENT_DEMOTE  = 4, /* chunk demoted to a colder tier                 */
} kv_event_type_t;

typedef struct {
    int32_t       type;       /* kv_event_type_t                            */
    int32_t       tier;       /* originating tier (0..4); -1 if N/A         */
    kv_locator_t  locator;
    uint64_t      epoch;      /* monotonic, per-node                        */
} kv_event_t;

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* KVCACHE_KV_TYPES_H */
