/*
 * KV Cache — Shmem Ring wire format between libkvcache.so (engine side)
 * and kvagent (sidecar). LLD §6.1.3.
 *
 * This header is shared by:
 *   - libkvcache.so (producer of SQ entries, consumer of CQ entries)
 *   - kvagent       (consumer of SQ entries, producer of CQ entries)
 *
 * All structs are POD, packed, fixed-size, and little-endian. Strings live
 * in the data-buffer pool (not in ring entries). The rings themselves are
 * mmap'd to /dev/shm and share their lifetime with both processes.
 *
 * Layout in shared memory:
 *
 *   +-----------------------------+
 *   | ShmemRingHeader            |  64B, cacheline-aligned
 *   +-----------------------------+
 *   | SQ entries  [SQ_DEPTH]      |  each 128B
 *   +-----------------------------+
 *   | CQ entries  [CQ_DEPTH]      |  each 64B
 *   +-----------------------------+
 *   | Data buffer pool           |  configured at attach time
 *   +-----------------------------+
 *
 * Producer/consumer indices are cacheline-isolated; advance via release/acquire.
 *
 * ABI stability: any change here bumps SHMEM_WIRE_VERSION. KVAgent and
 * libkvcache.so must agree on the version at ring-attach time.
 */
#ifndef KVCACHE_SHMEM_WIRE_H
#define KVCACHE_SHMEM_WIRE_H

#include <stdint.h>
#include "kvcache/kv_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SHMEM_WIRE_VERSION 1
#define SHMEM_RING_MAGIC   0x4B564352494E4731ULL  /* "KVCRING1" */

/* Ring depths are powers of two so masking can replace modulo. */
#define SHMEM_SQ_DEPTH 4096
#define SHMEM_CQ_DEPTH 4096

#define SHMEM_CACHELINE 64

/* ------------------------------------------------------------------------- */
/* Op codes                                                                  */
/* ------------------------------------------------------------------------- */

typedef enum {
    SHMEM_OP_NOP        = 0,
    SHMEM_OP_LOOKUP     = 1,
    SHMEM_OP_RESERVE    = 2,
    SHMEM_OP_PUBLISH    = 3,
    SHMEM_OP_FETCH      = 4,
    SHMEM_OP_WAIT       = 5,
    SHMEM_OP_SEAL       = 6,
    SHMEM_OP_RELEASE    = 7,
    SHMEM_OP_SUBSCRIBE  = 8,
} shmem_op_t;

/* ------------------------------------------------------------------------- */
/* Header                                                                    */
/* ------------------------------------------------------------------------- */

typedef struct __attribute__((aligned(SHMEM_CACHELINE))) {
    uint64_t magic;          /* SHMEM_RING_MAGIC                              */
    uint32_t wire_version;   /* SHMEM_WIRE_VERSION                            */
    uint32_t flags;
    uint32_t sq_depth;       /* must equal SHMEM_SQ_DEPTH at attach time      */
    uint32_t cq_depth;
    uint64_t data_pool_off;  /* offset to data buffer pool                    */
    uint64_t data_pool_size;
    int32_t  doorbell_eventfd; /* set by agent; engine writes to wake agent   */
    int32_t  completion_eventfd;/* set by engine; agent writes to wake engine */
    uint64_t engine_pid;     /* informational                                 */
    uint64_t agent_pid;
    uint8_t  pad[SHMEM_CACHELINE - 56];  /* fill cacheline                    */
} ShmemRingHeader;

/* Producer / consumer counters live on their own cachelines. */
typedef struct __attribute__((aligned(SHMEM_CACHELINE))) {
    _Atomic uint64_t head; /* producer-advanced                              */
    uint8_t pad[SHMEM_CACHELINE - 8];
} ShmemHead;

typedef struct __attribute__((aligned(SHMEM_CACHELINE))) {
    _Atomic uint64_t tail; /* consumer-advanced                              */
    uint8_t pad[SHMEM_CACHELINE - 8];
} ShmemTail;

/* ------------------------------------------------------------------------- */
/* Submission Queue entry — 128 bytes                                        */
/* ------------------------------------------------------------------------- */

typedef struct __attribute__((packed, aligned(8))) {
    uint64_t  request_id;       /* engine-assigned, monotonically increasing */
    uint32_t  op;               /* shmem_op_t                                */
    uint32_t  priority;         /* 0=P0, 1=P1, 2=P2                          */
    kv_handle_t handle;         /* opaque (server-issued for Publish/Fetch/Wait/Seal/Release) */

    /* Op-specific payload. We embed the small ones; large data (tokens,
       ranges) goes in the data buffer pool referenced by data_pool_off. */
    kv_locator_t locator;       /* 64B — used by LOOKUP, RESERVE         */

    /* Bytes 88..127: 40-byte tail used by varying ops:                    */
    uint64_t  data_pool_off;    /* offset into data pool (tokens / ranges array) */
    uint32_t  data_pool_len;
    uint32_t  count;            /* token count, range count, etc.           */
    uint64_t  bytes;            /* RESERVE bytes; FETCH dst bytes           */
    uint64_t  watermark;        /* PUBLISH watermark                        */
    uint64_t  dst_iova;         /* FETCH destination (NIXL-registered)      */
} ShmemSqEntry;

#ifdef __cplusplus
static_assert (sizeof(ShmemSqEntry) == 128, "ShmemSqEntry must be 128 bytes");
#else
_Static_assert(sizeof(ShmemSqEntry) == 128, "ShmemSqEntry must be 128 bytes");
#endif

/* ------------------------------------------------------------------------- */
/* Completion Queue entry — 64 bytes                                         */
/* ------------------------------------------------------------------------- */

typedef struct __attribute__((packed, aligned(8))) {
    uint64_t  request_id;       /* echoes ShmemSqEntry.request_id            */
    int32_t   status;           /* kv_status_t                               */
    uint32_t  flags;            /* bit0: handle valid; bit1: meta valid; ...  */
    kv_handle_t handle;         /* server-issued handle (for LOOKUP/RESERVE) */
    kv_completion_t completion; /* for FETCH (await via WAIT)                */
    uint32_t  matched_tokens;   /* for LOOKUP                                */
    uint32_t  queue_position;   /* informational backpressure signal         */
    uint64_t  result_off;       /* offset into data pool (Locator, slot info)*/
    uint32_t  result_len;
    uint32_t  reserved;
    uint64_t  pad0;
} ShmemCqEntry;

#ifdef __cplusplus
static_assert (sizeof(ShmemCqEntry) == 64,  "ShmemCqEntry must be 64 bytes");
#else
_Static_assert(sizeof(ShmemCqEntry) == 64,  "ShmemCqEntry must be 64 bytes");
#endif

/* ------------------------------------------------------------------------- */
/* Flags                                                                     */
/* ------------------------------------------------------------------------- */

#define SHMEM_CQ_FLAG_HANDLE_VALID  (1u << 0)
#define SHMEM_CQ_FLAG_META_VALID    (1u << 1)
#define SHMEM_CQ_FLAG_COMPLETION_OK (1u << 2)

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* KVCACHE_SHMEM_WIRE_H */
