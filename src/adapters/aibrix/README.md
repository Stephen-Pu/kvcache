# aibrix adapter

Adapter for the [AIBrix](https://github.com/vllm-project/aibrix) engine.
LLD §6.1.4.

## What's here

* `kvcache_aibrix.AIBrixKVConnector` — AIBrix KVCache Connector v1
  surface (`get` / `put` / `delete` / `exists`). Thin wrapper over the
  shared :mod:`kvcache_core` connector — calls
  `libkvcache.so` via `cffi`.
* `kvcache_aibrix.KVCacheConnector` — re-export of the engine-agnostic
  connector from `kvcache_core`, for callers that want the lower-level
  reserve / publish / seal verbs.

## Usage

```python
from kvcache_aibrix import AIBrixKVConnector

with AIBrixKVConnector(tenant_id="t1",
                        model_id="llama-3-70b",
                        bytes_per_token=64) as kv:
    if kv.exists(token_ids):
        kv_bytes = kv.get(token_ids)        # None on miss
    ...
    kv.put(token_ids, finished_kv_bytes)    # commit when generation ends
```

## Tests

```bash
make build                                    # builds libkvcache.{so,dylib}
KVCACHE_LIB=$PWD/build/core-abi/libkvcache.dylib \
    pytest src/adapters/aibrix/tests -v
```

Six tests cover put → get round-trip, exists transitions,
miss-returns-None, prefix-only retrieval for extended keys, constructor
validation, and the `delete` no-op.

## Versions

Supported AIBrix versions: latest stable + one prior. The Core ABI is
stable across AIBrix releases; only this adapter's wiring tracks the
engine's connector interface.
