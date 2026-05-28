"""Phase P-2 — KVCacheVllmConnector bridge tests.

Two modes:

  * **vLLM not installed** (the default in this repo's CI/dev rig):
    we verify that ``importlib.import_module("kvcache_vllm.vllm_bridge")``
    fails with an ImportError pointing at vllm — the documented and
    expected behavior. The rest of ``kvcache_vllm`` stays import-clean
    in that case.

  * **vLLM IS installed**: we instantiate ``KVCacheVllmConnector``
    with a minimal in-memory ``vllm_config`` (just enough to feed
    ``kv_connector_extra_config``), drive the canonical lifecycle
    through the subclass surface (save then re-lookup hit), and
    verify forwarding to the underlying VllmKVConnector reaches the
    live C ABI.
"""
from __future__ import annotations

import importlib
import types

import pytest

cffi = pytest.importorskip("cffi", reason="cffi is required for the C ABI tests")


def _vllm_available() -> bool:
    try:
        importlib.import_module(
            "vllm.distributed.kv_transfer.kv_connector.v1.base")
    except Exception:
        return False
    return True


# ---------------------------------------------------------------------------
# Mode 1: vLLM is NOT installed (default).
# ---------------------------------------------------------------------------

@pytest.mark.skipif(_vllm_available(),
                     reason="vllm IS installed; the other path covers this")
def test_bridge_import_fails_cleanly_without_vllm() -> None:
    """Without vLLM the bridge module raises ImportError on import.

    Importantly the rest of ``kvcache_vllm`` (``connector``,
    ``__init__``) MUST still import successfully — operators who
    don't run vLLM should be able to use the in-tree connector
    without taking on a heavy dependency.
    """
    # Sanity: the vLLM-free surface imports.
    import kvcache_vllm  # noqa: F401
    from kvcache_vllm import VllmKVConnector  # noqa: F401

    with pytest.raises(ImportError) as ei:
        importlib.import_module("kvcache_vllm.vllm_bridge")
    # The error chain should mention vllm somewhere.
    msg = repr(ei.value) + "|" + repr(ei.value.__cause__)
    assert "vllm" in msg.lower(), \
        f"expected vllm-related ImportError, got: {msg}"


# ---------------------------------------------------------------------------
# Mode 2: vLLM installed — exercise the bridge end-to-end.
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _vllm_available(),
                     reason="vllm not installed; install kvcache_vllm[vllm]")
def test_bridge_full_lifecycle_with_vllm():
    """Drive the canonical scheduler→worker→scheduler lifecycle
    through the vLLM-shaped subclass and verify a save→re-lookup hit
    survives the round-trip."""
    from kvcache_vllm.vllm_bridge import KVCacheVllmConnector
    from vllm.distributed.kv_transfer.kv_connector.v1.base import (
        KVConnectorRole)

    # Minimal duck-typed vllm_config — the bridge only reads
    # `.kv_transfer_config.kv_connector_extra_config`. We pass a
    # SimpleNamespace tree so the bridge picks the right knobs.
    extra = {"tenant_id":       "p2-tenant",
              "model_id":        "p2-model",
              "bytes_per_token": 64}
    cfg = types.SimpleNamespace(
        kv_transfer_config=types.SimpleNamespace(
            kv_connector_extra_config=extra,
        ),
    )
    conn = KVCacheVllmConnector(cfg, KVConnectorRole.SCHEDULER)

    # Request A: cold save.
    req_a = types.SimpleNamespace(
        request_id="ra",
        prompt_token_ids=list(range(2000, 2032)),
    )
    n0, is_async = conn.get_num_new_matched_tokens(req_a, 0)
    assert n0 == 0
    assert is_async is False

    payload = bytes((i & 0xff for i in range(32 * 64)))
    conn._inner.save("ra", list(range(2000, 2032)), payload)

    # Request B: same tokens — should hit.
    req_b = types.SimpleNamespace(
        request_id="rb",
        prompt_token_ids=list(range(2000, 2032)),
    )
    n_hit, _ = conn.get_num_new_matched_tokens(req_b, 0)
    assert n_hit == 32, f"second request should match all 32 tokens; got {n_hit}"

    # build_connector_meta over a duck-typed scheduler_output.
    sched_out = types.SimpleNamespace(scheduled_new_reqs=[req_b])
    meta = conn.build_connector_meta(sched_out)
    assert "rb" in meta.matched_tokens_by_request
    assert meta.matched_tokens_by_request["rb"] == 32

    # get_finished reports the saved request.
    loaded, saved = conn.get_finished({"ra", "rb"})
    assert "ra" in saved

    # request_finished cleans up.
    conn.request_finished(req_a)
    conn.request_finished(req_b)
