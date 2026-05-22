"""End-to-end coverage of SGLangKVBackend against the loopback C ABI.

Mirrors the vLLM adapter's e2e demo (Reserve → write → Publish → Seal →
Lookup → Fetch → byte verify), but driven through SGLang-shaped
``lookup`` / ``store`` / ``retrieve`` calls rather than the lower-level
connector verbs.

Requires libkvcache.so / .dylib reachable through ``$KVCACHE_LIB`` or
the default build/ search path. Skipped (not failed) if cffi isn't
installed.
"""

from __future__ import annotations

import os
import shutil

import pytest

cffi = pytest.importorskip("cffi", reason="cffi is required for the C ABI tests")

from kvcache_sglang import SGLangKVBackend

CHUNK = 16
BYTES_PER_TOKEN = 64


def _have_library() -> bool:
    if os.environ.get("KVCACHE_LIB"):
        return os.path.isfile(os.environ["KVCACHE_LIB"])
    return shutil.which("cmake") is not None


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_store_then_retrieve_round_trip():
    tokens = list(range(2 * CHUNK))  # 32 tokens = 2 chunks
    payload = bytes(((i * 11) & 0xFF
                      for i in range(len(tokens) * BYTES_PER_TOKEN)))
    with SGLangKVBackend(tenant_id="sglang-tenant",
                         model_id="sglang-demo",
                         bytes_per_token=BYTES_PER_TOKEN) as kv:
        kv.store(tokens, payload)
        got = kv.retrieve(tokens)
        assert got is not None
        assert got == payload


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_lookup_returns_chunk_aligned_match():
    # Store 2 chunks worth of tokens; look up with extra unrelated tail.
    tokens = list(range(2 * CHUNK))
    payload = bytes(len(tokens) * BYTES_PER_TOKEN)  # zeros
    with SGLangKVBackend(tenant_id="sglang-tenant",
                         model_id="sglang-lpm",
                         bytes_per_token=BYTES_PER_TOKEN) as kv:
        kv.store(tokens, payload)
        # Add an unrelated 3-token tail — LPM drops partial chunks.
        n = kv.lookup(tokens + [9001, 9002, 9003])
        assert n == 2 * CHUNK


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_retrieve_miss_returns_none():
    with SGLangKVBackend(tenant_id="sglang-tenant",
                         model_id="sglang-miss",
                         bytes_per_token=BYTES_PER_TOKEN) as kv:
        # Fresh model id — nothing ever stored.
        assert kv.retrieve(list(range(CHUNK))) is None
        assert kv.lookup(list(range(CHUNK))) == 0


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_retrieve_truncates_to_matched_prefix():
    # Store 2 chunks; retrieve with 3 chunks worth of tokens. The first 2
    # chunks should be the cached prefix; the returned bytes cover only
    # those 32 tokens, not the requested 48.
    base = list(range(2 * CHUNK))
    payload = bytes(((i * 17) & 0xFF
                      for i in range(len(base) * BYTES_PER_TOKEN)))
    with SGLangKVBackend(tenant_id="sglang-tenant",
                         model_id="sglang-prefix",
                         bytes_per_token=BYTES_PER_TOKEN) as kv:
        kv.store(base, payload)
        extended = base + list(range(2 * CHUNK, 3 * CHUNK))
        got = kv.retrieve(extended)
        assert got is not None
        assert len(got) == len(payload)
        assert got == payload


def test_constructor_rejects_zero_bytes_per_token():
    with pytest.raises(ValueError):
        SGLangKVBackend(tenant_id="t", model_id="m", bytes_per_token=0)


def test_drop_returns_false():
    # Always-on (no library / ABI calls needed).
    if not _have_library():
        pytest.skip("libkvcache.so not available")
    with SGLangKVBackend(tenant_id="sglang-tenant", model_id="sglang-drop",
                         bytes_per_token=BYTES_PER_TOKEN) as kv:
        assert kv.drop(list(range(CHUNK))) is False
