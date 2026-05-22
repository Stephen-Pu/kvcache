# kvcache_core

Engine-agnostic Python wrapper around the KV Cache Core ABI
(`libkvcache.so` / `.dylib`). Calls the C ABI through `cffi`.

Every engine-specific adapter (`kvcache_vllm`, `kvcache_sglang`,
`kvcache_aibrix`, ...) re-exports `KVCacheConnector` from this package
and adds its own engine-shaped wrapper class on top.

LLD reference: §6.1.2 (Core ABI) and §6.1.4 (engine adapter strategy).

## Usage

```python
from kvcache_core import KVCacheConnector

with KVCacheConnector(tenant_id="t1", model_id="llama-3-70b") as c:
    hit = c.lookup(token_ids)
    if hit is not None:
        out = bytearray(...)
        cid = c.fetch(hit.handle, out)
        c.wait(cid)
        c.release(hit.handle)
```

## Tests

Tested indirectly by every adapter's test suite — both `kvcache_vllm`'s
e2e demo and `kvcache_sglang`'s backend tests drive this layer.
