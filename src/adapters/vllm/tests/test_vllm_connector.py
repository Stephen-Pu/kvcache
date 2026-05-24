"""Phase P-1 — VllmKVConnector full lifecycle against the live C ABI.

What this exercises:
  1. Scheduler tick (cold request): get_num_new_matched_tokens returns 0;
     update_state_after_alloc creates an empty entry.
  2. Save path: save() commits a 32-token chunk; wait_for_save returns
     without raising; get_finished includes the request in `saved`.
  3. Second request shares the prefix: get_num_new_matched_tokens
     returns 32 (matching what the connector found via Lookup).
  4. Worker forward pass: build_connector_meta surfaces the match;
     start_load_kv fetches into a caller-owned buffer;
     wait_for_layer_load unblocks; bytes match the original save.
  5. get_finished tracks both loaded + saved subsets correctly.
  6. release() drops both per-request state AND the Core ABI handle.

Skipped (not failed) if libkvcache.so or cffi aren't available — same
gate the demo test uses, so this binary stays portable.
"""

from __future__ import annotations

import os
import shutil

import pytest

cffi = pytest.importorskip("cffi", reason="cffi is required for the C ABI tests")

from kvcache_vllm import VllmConnectorMetadata, VllmKVConnector

CHUNK = 16
BYTES_PER_TOKEN = 64


def _have_library() -> bool:
    if os.environ.get("KVCACHE_LIB"):
        return os.path.isfile(os.environ["KVCACHE_LIB"])
    return shutil.which("cmake") is not None


pytestmark = pytest.mark.skipif(not _have_library(),
                                 reason="libkvcache.so not available")


def test_full_vllm_lifecycle():
    with VllmKVConnector(tenant_id="vllm-tenant",
                         model_id="vllm-lifecycle",
                         bytes_per_token=BYTES_PER_TOKEN) as conn:
        # --- Request A: cold, save 32 tokens ----------------------------
        # P-1 isolation: pick token ranges that don't collide with the
        # other adapter tests (e2e_demo uses range(32), sglang uses
        # range(0,32), etc.). The HeadlessNode singleton's ART is
        # currently shared across (tenant, model) — a separate bug to
        # fix, but for now distinct tokens keep these tests
        # order-independent.
        tokens_a = list(range(80000, 80000 + 2 * CHUNK))
        rid_a = "req-a"
        # Cold lookup returns 0; entry created.
        assert conn.get_num_new_matched_tokens(rid_a, tokens_a, 0) == 0
        conn.update_state_after_alloc(rid_a, 0)

        # Worker finishes generating: save the KV bytes.
        payload_a = bytes(((i * 7) & 0xFF
                            for i in range(len(tokens_a) * BYTES_PER_TOKEN)))
        conn.save(rid_a, tokens_a, payload_a)
        conn.wait_for_save(rid_a)

        # get_finished reflects the save.
        loaded, saved = conn.get_finished([rid_a])
        assert saved == {rid_a}
        assert loaded == set()  # nothing was load-driven for rid_a
        conn.release(rid_a)

        # --- Request B: hits the cached prefix --------------------------
        tokens_b = tokens_a + [9001, 9002, 9003]  # +3 tail, dropped by LPM
        rid_b = "req-b"
        extra = conn.get_num_new_matched_tokens(rid_b, tokens_b,
                                                  num_computed_tokens=0)
        assert extra == 2 * CHUNK, (
            f"expected LPM hit of {2 * CHUNK} tokens, got {extra}")
        conn.update_state_after_alloc(rid_b, extra)

        # Worker builds scheduler->worker metadata.
        meta = conn.build_connector_meta([rid_b])
        assert isinstance(meta, VllmConnectorMetadata)
        assert meta.matched_tokens_by_request == {rid_b: 2 * CHUNK}

        # Fetch into engine-owned buffer.
        dst = bytearray(2 * CHUNK * BYTES_PER_TOKEN)
        cid = conn.start_load_kv(rid_b, dst)
        conn.wait_for_layer_load(rid_b, cid)
        assert bytes(dst) == payload_a, "loaded bytes must match the save"

        loaded, saved = conn.get_finished([rid_b])
        assert loaded == {rid_b}
        assert saved == set()  # rid_b never saved
        conn.release(rid_b)


def test_get_num_new_matched_tokens_credits_only_beyond_computed():
    """Phase P-1 invariant: vLLM's caller already counts the prefix it
    has computed locally; the connector must only credit tokens
    BEYOND that. Without the `max(0, ...)` an LPM hit shorter than
    num_computed_tokens would underflow into a negative credit and
    vLLM's scheduler would over-allocate."""
    with VllmKVConnector(tenant_id="vllm-tenant",
                         model_id="vllm-credit",
                         bytes_per_token=BYTES_PER_TOKEN) as conn:
        tokens = list(range(81000, 81000 + 2 * CHUNK))  # distinct namespace
        payload = bytes((i & 0xFF for i in range(len(tokens) * BYTES_PER_TOKEN)))
        conn.save("seed", tokens, payload)
        conn.release("seed")

        # Caller claims it has already computed all 32 tokens locally:
        # the connector must return 0 (no incremental credit).
        extra = conn.get_num_new_matched_tokens("post", tokens,
                                                  num_computed_tokens=32)
        assert extra == 0

        # Caller has computed only 24: the hit covers the remaining 8.
        extra = conn.get_num_new_matched_tokens("partial", tokens,
                                                  num_computed_tokens=24)
        assert extra == 8


def test_release_is_idempotent_on_unknown_request():
    """vLLM's cleanup-on-cancel path calls release() liberally; the
    connector must not blow up if the request id is unknown."""
    with VllmKVConnector(tenant_id="vllm-tenant",
                         model_id="vllm-idem",
                         bytes_per_token=BYTES_PER_TOKEN) as conn:
        conn.release("never-seen")  # no-op, no exception


def test_wait_for_save_raises_without_save():
    """Catches engine bugs where the worker forgets to call save()
    before claiming a request is finished. wait_for_save raising is
    much louder than silently letting it through."""
    with VllmKVConnector(tenant_id="vllm-tenant",
                         model_id="vllm-missed-save",
                         bytes_per_token=BYTES_PER_TOKEN) as conn:
        with pytest.raises(KeyError):
            conn.wait_for_save("not-saved")
