# sglang adapter

Adapter for the [SGLang](https://github.com/sgl-project/sglang) engine.
LLD §6.1.4.

## What's here

* `kvcache_sglang.SGLangKVBackend` — sync L2 KV backend with the
  `lookup` / `store` / `retrieve` / `drop` method names SGLang's
  RadixAttention external-cache interface expects. Wraps the Core ABI
  (calls `libkvcache.so` via `cffi`) and folds the
  reserve → write → publish → seal sequence into a single `store`.
* `kvcache_sglang.AsyncSGLangKVBackend` — same sync verbs plus an
  async retrieve path (`kick_off` / `finished_ids` / `pop` / `cancel`)
  so an SGLang scheduler can overlap the cache Fetch with the prior
  layer's attention compute instead of blocking inline. Built on the
  reusable `AsyncLoadDriver` (worker pool + future tracking, mirrors
  the vLLM adapter's Phase P-3.2 driver but reshaped for SGLang's
  single-shot retrieve vs. vLLM's layer-by-layer ingest).
* `kvcache_sglang.AsyncLoadDriver` — the executor + lifecycle
  primitive that backs `AsyncSGLangKVBackend`. Connector-protocol-
  driven so it has clean unit coverage with an in-Python fake (no
  live `libkvcache.so` needed for the threading tests).
* `kvcache_sglang.KVCacheConnector` — the engine-agnostic Python
  surface, the same shape as the vLLM adapter's connector. Useful for
  ad-hoc demos or for engines whose call patterns don't fit the
  SGLang shape.

## Usage

```python
from kvcache_sglang import SGLangKVBackend

with SGLangKVBackend(tenant_id="t1",
                     model_id="llama-3-70b",
                     bytes_per_token=64) as kv:
    matched = kv.lookup(token_ids)         # 0 on miss; otherwise a
                                            # chunk-aligned token count
    if matched < len(token_ids):
        ...                                 # recompute the tail
    kv_bytes = kv.retrieve(token_ids)      # None on miss
    ...
    kv.store(token_ids, finished_kv_bytes) # commit when generation ends
```

### Async retrieve

```python
from kvcache_sglang import AsyncSGLangKVBackend

with AsyncSGLangKVBackend(tenant_id="t1", model_id="llama-3-70b",
                            bytes_per_token=64, workers=4) as kv:
    matched = kv.kick_off("req-7", token_ids)   # sync lookup + async fetch
    if matched == 0:
        # cache miss — recompute the whole prompt
        ...
    else:
        # ... start model setup; the fetch runs on a worker thread
        ...
        # later, when the scheduler is ready to splice the cache in:
        kv_bytes = kv.pop("req-7")              # blocks if not done
    kv.store(token_ids, finished_kv_bytes)
```

If the request is cancelled before consumption, call
`kv.cancel("req-7")` — the driver blocks on the in-flight fetch first
(so the worker isn't still touching the inner handle), then releases.

## Tests

```bash
make build                                    # builds libkvcache.{so,dylib}
KVCACHE_LIB=$PWD/build/core-abi/libkvcache.dylib \
    pytest src/adapters/sglang/tests -v
```

Six tests cover store → retrieve round-trip, chunk-aligned LPM,
miss-returns-None, prefix truncation, constructor validation, and the
`drop` no-op. Ten additional tests
(`test_sglang_async.py`) cover the `AsyncLoadDriver` lifecycle:
miss/hit kick_off, the kick_off-is-actually-async timing invariant,
finished_ids polling + idempotency, cancel-releases-handle, same-rid
back-to-back kick_off releasing the prior handle, worker exception
surfacing through finished_ids, close-blocks-and-releases, constructor
validation, plus one e2e round-trip against the real C ABI.

## Versions

Supported SGLang versions: latest stable + one prior. The Core ABI is
stable across SGLang releases; only this adapter's wiring tracks the
engine's connector interface.
