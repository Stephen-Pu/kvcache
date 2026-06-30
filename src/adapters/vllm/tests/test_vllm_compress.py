"""Phase A6 — VllmKVConnector lossy KV-tensor compression on the hot path.

Unlike the SGLang/AIBrix adapters (whose retrieve is a synchronous
``bytes`` return), vLLM's load path is two-phase — ``start_load_kv``
issues an async fetch into an engine-owned buffer and
``wait_for_layer_load`` blocks for it. The compressed mode therefore
fetches the variable-size codec blob into an internal buffer in
``start_load_kv`` and only decodes + copies into the engine buffer once
``wait_for_layer_load`` confirms the fetch landed. These tests exercise
that two-phase decode through the live C ABI.

Skipped (not failed) if libkvcache.so or cffi aren't available — same
gate the other vLLM tests use.
"""

from __future__ import annotations

import array
import os
import shutil

import pytest

cffi = pytest.importorskip("cffi", reason="cffi is required for the C ABI tests")

from kvcache_vllm import VllmKVConnector

CHUNK = 16
BYTES_PER_TOKEN = 64
ELEMS = BYTES_PER_TOKEN // 4


def _have_library() -> bool:
    if os.environ.get("KVCACHE_LIB"):
        return os.path.isfile(os.environ["KVCACHE_LIB"])
    return shutil.which("cmake") is not None


def _smooth_kv_bytes(n_tokens: int) -> bytes:
    # Smooth along the token axis so the codec's per-token DPCM + int8
    # quantization reconstructs within tolerance.
    vals = [1.0 + 0.01 * t + 0.1 * (e % 5)
            for t in range(n_tokens) for e in range(ELEMS)]
    return array.array("f", vals).tobytes()


pytestmark = pytest.mark.skipif(not _have_library(),
                                 reason="libkvcache.so not available")


def test_compressed_save_then_two_phase_load_round_trip():
    tokens = list(range(82000, 82000 + 2 * CHUNK))
    payload = _smooth_kv_bytes(len(tokens))
    with VllmKVConnector(tenant_id="vllm-zc", model_id="vllm-zc-rt",
                         bytes_per_token=BYTES_PER_TOKEN,
                         compress=True) as conn:
        # Save compresses + seals the codec blob.
        conn.save("seed", tokens, payload)
        conn.wait_for_save("seed")
        conn.release("seed")

        # A second request hits the cached prefix.
        rid = "load"
        extra = conn.get_num_new_matched_tokens(rid, tokens,
                                                 num_computed_tokens=0)
        assert extra == 2 * CHUNK

        # Two-phase load: start_load_kv fetches the blob into an internal
        # buffer; wait_for_layer_load decodes into the engine buffer.
        dst = bytearray(2 * CHUNK * BYTES_PER_TOKEN)
        cid = conn.start_load_kv(rid, dst)
        conn.wait_for_layer_load(rid, cid)

        orig = array.array("f"); orig.frombytes(payload)
        rec = array.array("f"); rec.frombytes(bytes(dst))
        assert len(rec) == len(orig)
        # Lossy: reconstruction within the int8 quantization step.
        assert max(abs(a - b) for a, b in zip(orig, rec)) < 0.01

        loaded, _ = conn.get_finished([rid])
        assert loaded == {rid}
        conn.release(rid)


def test_compressed_stored_smaller_than_raw():
    tokens = list(range(83000, 83000 + 4 * CHUNK))
    payload = _smooth_kv_bytes(len(tokens))
    with VllmKVConnector(tenant_id="vllm-zc", model_id="vllm-zc-size",
                         bytes_per_token=BYTES_PER_TOKEN,
                         compress=True) as conn:
        conn.save("seed", tokens, payload)
        # Peek at the sealed slot via a fresh lookup through the inner cx.
        hit = conn._cx.lookup(tokens)
        assert hit is not None
        stored = conn._cx.stored_bytes(hit.handle)
        conn._cx.release(hit.handle)
        assert stored < len(payload)
        conn.release("seed")


def test_compress_requires_fp32_aligned():
    with pytest.raises(ValueError):
        VllmKVConnector(tenant_id="t", model_id="m",
                        bytes_per_token=66, compress=True)
