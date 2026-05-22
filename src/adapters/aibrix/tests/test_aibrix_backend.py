"""AIBrixKVConnector — Connector v1 verbs against the loopback C ABI."""

from __future__ import annotations

import os
import shutil

import pytest

cffi = pytest.importorskip("cffi", reason="cffi is required for the C ABI tests")

from kvcache_aibrix import AIBrixKVConnector

CHUNK = 16
BYTES_PER_TOKEN = 64


def _have_library() -> bool:
    if os.environ.get("KVCACHE_LIB"):
        return os.path.isfile(os.environ["KVCACHE_LIB"])
    return shutil.which("cmake") is not None


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_put_then_get_round_trip():
    tokens = list(range(2 * CHUNK))
    payload = bytes(((i * 13) & 0xFF
                      for i in range(len(tokens) * BYTES_PER_TOKEN)))
    with AIBrixKVConnector(tenant_id="aibrix-tenant",
                            model_id="aibrix-demo",
                            bytes_per_token=BYTES_PER_TOKEN) as kv:
        kv.put(tokens, payload)
        got = kv.get(tokens)
        assert got is not None
        assert got == payload


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_exists_returns_false_then_true():
    # The in-process HeadlessNode singleton accumulates ART state across
    # tests in the same pytest run, and the MVP ART (Node256-only,
    # terminal-leaves) refuses inserts that conflict with an existing
    # path. Use a high-offset 2-chunk path so no earlier test has put
    # anything at this prefix.
    tokens = list(range(2000, 2000 + 2 * CHUNK))
    payload = bytes(len(tokens) * BYTES_PER_TOKEN)
    with AIBrixKVConnector(tenant_id="aibrix-tenant",
                            model_id="aibrix-exists",
                            bytes_per_token=BYTES_PER_TOKEN) as kv:
        assert kv.exists(tokens) is False
        kv.put(tokens, payload)
        assert kv.exists(tokens) is True


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_get_miss_returns_none():
    with AIBrixKVConnector(tenant_id="aibrix-tenant",
                            model_id="aibrix-miss",
                            bytes_per_token=BYTES_PER_TOKEN) as kv:
        assert kv.get(list(range(CHUNK))) is None


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_get_returns_prefix_only_for_extended_key():
    base = list(range(2 * CHUNK))
    payload = bytes(((i * 19) & 0xFF
                      for i in range(len(base) * BYTES_PER_TOKEN)))
    with AIBrixKVConnector(tenant_id="aibrix-tenant",
                            model_id="aibrix-prefix",
                            bytes_per_token=BYTES_PER_TOKEN) as kv:
        kv.put(base, payload)
        extended = base + list(range(2 * CHUNK, 3 * CHUNK))
        got = kv.get(extended)
        assert got is not None
        assert got == payload  # only the 2-chunk cached prefix


def test_constructor_rejects_zero_bytes_per_token():
    with pytest.raises(ValueError):
        AIBrixKVConnector(tenant_id="t", model_id="m", bytes_per_token=0)


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_delete_is_a_noop_returning_false():
    with AIBrixKVConnector(tenant_id="aibrix-tenant",
                            model_id="aibrix-del",
                            bytes_per_token=BYTES_PER_TOKEN) as kv:
        # Distinct high-offset 2-chunk path — see test_exists for why.
        tokens = list(range(3000, 3000 + 2 * CHUNK))
        kv.put(tokens, bytes(len(tokens) * BYTES_PER_TOKEN))
        assert kv.delete(tokens) is False
        # Existing key should still be there afterwards.
        assert kv.exists(tokens) is True
