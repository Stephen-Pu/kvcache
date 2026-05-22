"""cffi-based loader for libkvcache.so.

Locates the shared library (via $KVCACHE_LIB or by walking up to the build
directory), declares the C ABI from include/kvcache/, and exposes a single
``lib`` object for the higher-level adapter to call.
"""

from __future__ import annotations

import os
import pathlib

import cffi  # type: ignore[import-not-found]

_FFI = cffi.FFI()

# Mirror of include/kvcache/kv_types.h + kv_errors.h + kv_abi.h. We keep this
# in sync by hand for MVP — once the build system is wired up the headers can
# be passed to cffi.cdef directly. For now we vendor the minimal subset that
# the adapter needs.
_FFI.cdef(
    """
typedef uint64_t kv_handle_t;
typedef uint64_t kv_completion_t;
typedef struct kv_ctx_s kv_ctx_t;

typedef struct {
    void*    addr;
    size_t   len;
    int32_t  mem_type;
    uint32_t mr_key;
} kv_buffer_desc_t;

typedef struct {
    uint16_t layer_start;
    uint16_t layer_count;
    uint16_t head_start;
    uint16_t head_count;
    uint32_t token_start;
    uint32_t token_count;
} kv_range_t;

typedef struct {
    uint8_t   tenant_id[16];
    uint64_t  model_id_hash;
    uint8_t   prefix_hash[16];
    kv_range_t range;
    uint32_t  version;
    uint32_t  flags;
} kv_locator_t;

typedef struct {
    int32_t      abi_version;
    const char*  agent_endpoint;
    const char*  tenant_id;
    const char*  model_id;
    uint32_t     flags;
} kv_ctx_config_t;

int kv_ctx_open (const kv_ctx_config_t* cfg, kv_ctx_t** out_ctx);
int kv_ctx_close(kv_ctx_t* ctx);

int kv_lookup(kv_ctx_t*, const uint32_t* tokens, size_t n,
              kv_locator_t* meta, kv_handle_t* handle,
              uint32_t* matched_tokens);
int kv_reserve(kv_ctx_t*, const kv_locator_t* locator, size_t bytes,
               kv_handle_t* handle, kv_buffer_desc_t* slot);
int kv_publish(kv_ctx_t*, kv_handle_t handle,
               kv_buffer_desc_t src, uint64_t watermark);
int kv_fetch(kv_ctx_t*, kv_handle_t handle,
             const kv_range_t* ranges, size_t n,
             kv_buffer_desc_t dst, kv_completion_t* completion);
int kv_wait(kv_ctx_t*, kv_completion_t completion, uint32_t timeout_ms);
int kv_seal(kv_ctx_t*, kv_handle_t handle,
            const uint32_t* tokens, size_t n_tokens);
int kv_release(kv_ctx_t*, kv_handle_t handle);

const char* kv_status_str(int status);
"""
)


def _candidate_paths() -> list[pathlib.Path]:
    env = os.environ.get("KVCACHE_LIB")
    if env:
        return [pathlib.Path(env)]

    here = pathlib.Path(__file__).resolve()
    # Walk up to find build/ or build-debug/ alongside src/.
    candidates: list[pathlib.Path] = []
    for parent in here.parents:
        for build_dir in ("build", "build-debug", "build-release"):
            for name in ("libkvcache.so", "libkvcache.dylib"):
                p = parent / build_dir / "core-abi" / name
                candidates.append(p)
        if parent.name == "src":
            break
    return candidates


def _load() -> "cffi.FFI.dlopen":
    for p in _candidate_paths():
        if p.is_file():
            return _FFI.dlopen(str(p))
    raise RuntimeError(
        "libkvcache.so not found. Build the C/C++ tree first, or set "
        "KVCACHE_LIB to the .so/.dylib path."
    )


_lib = None


def lib():
    """Lazily resolve and cache the loaded library handle."""
    global _lib
    if _lib is None:
        _lib = _load()
    return _lib


def ffi() -> cffi.FFI:
    return _FFI
