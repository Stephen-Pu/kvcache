"""Phase KVZ-4 — AIBrix adapter lossy KV-tensor compression mode.

Exercises the shared ``kvcache_core.compress_store/compress_retrieve`` helper
through AIBrix's put/get verbs, mirroring the SGLang compress coverage.
"""

from __future__ import annotations

import array
import os
import shutil

import pytest

cffi = pytest.importorskip("cffi", reason="cffi is required for the C ABI tests")

from kvcache_aibrix import AIBrixKVConnector

CHUNK = 16
BYTES_PER_TOKEN = 64
ELEMS = BYTES_PER_TOKEN // 4


def _have_library() -> bool:
    if os.environ.get("KVCACHE_LIB"):
        return os.path.isfile(os.environ["KVCACHE_LIB"])
    return shutil.which("cmake") is not None


def _smooth_kv_bytes(n_tokens: int) -> bytes:
    vals = [1.0 + 0.01 * t + 0.1 * (e % 5)
            for t in range(n_tokens) for e in range(ELEMS)]
    return array.array("f", vals).tobytes()


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_compressed_put_get_round_trip():
    key = list(range(2 * CHUNK))
    payload = _smooth_kv_bytes(len(key))
    with AIBrixKVConnector(tenant_id="ab-zc", model_id="ab-zc-demo",
                           bytes_per_token=BYTES_PER_TOKEN, compress=True) as kv:
        kv.put(key, payload)
        got = kv.get(key)
        assert got is not None and len(got) == len(payload)
        orig = array.array("f"); orig.frombytes(payload)
        rec = array.array("f"); rec.frombytes(got)
        assert max(abs(a - b) for a, b in zip(orig, rec)) < 0.01


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_compressed_stored_smaller_than_raw():
    key = list(range(4 * CHUNK))
    payload = _smooth_kv_bytes(len(key))
    with AIBrixKVConnector(tenant_id="ab-zc", model_id="ab-zc-size",
                           bytes_per_token=BYTES_PER_TOKEN, compress=True) as kv:
        kv.put(key, payload)
        hit = kv._cx.lookup(key)
        assert hit is not None
        stored = kv._cx.stored_bytes(hit.handle)
        kv._cx.release(hit.handle)
        assert stored < len(payload)


def test_compress_requires_fp32_aligned():
    with pytest.raises(ValueError):
        AIBrixKVConnector(tenant_id="t", model_id="m",
                          bytes_per_token=66, compress=True)
