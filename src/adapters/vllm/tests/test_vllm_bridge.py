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


# ---------------------------------------------------------------------------
# Phase P-3 — per-layer save fan-in. The accumulator is vllm-import-free
# so we get real unit coverage on the default dev rig even when the
# bridge module itself can't load.
# ---------------------------------------------------------------------------

def test_p3_layer_accumulator_concat_in_registered_order():
    """Layers fire out-of-order during the forward pass; the
    accumulator MUST concatenate them in the order
    ``register_layer_order`` declared, not insertion order."""
    from kvcache_vllm.layer_accumulator import LayerAccumulator

    acc = LayerAccumulator()
    acc.register_layer_order(["L0", "L1", "L2", "L3"])
    acc.bind_request("rp3", list(range(3000, 3016)))

    layer_bytes = {f"L{i}": bytes(((j + i) & 0xff for j in range(256)))
                    for i in range(4)}
    # Insert in scrambled order to verify ordering is by registration.
    for name in ("L2", "L0", "L3", "L1"):
        acc.accumulate("rp3", name, layer_bytes[name])

    drained = acc.drain_all()
    assert set(drained.keys()) == {"rp3"}
    tokens, concat = drained["rp3"]
    assert tokens == list(range(3000, 3016))
    expected = b"".join(layer_bytes[ln] for ln in ("L0", "L1", "L2", "L3"))
    assert concat == expected
    # Drain emptied the buffer.
    assert not acc.has_pending()


def test_p3_layer_accumulator_skips_unbound_request():
    """A layer accumulates against a request that never got a token
    binding — drain MUST return None so the bridge skips the Save
    (Sealing under an empty token list would corrupt the cache)."""
    from kvcache_vllm.layer_accumulator import LayerAccumulator

    acc = LayerAccumulator()
    acc.register_layer_order(["L0"])
    acc.accumulate("ghost", "L0", b"x" * 16)
    assert acc.drain_request("ghost") is None
    # ... and the per-request buffer cleared even on the skip path.
    assert not acc.has_pending("ghost")


def test_p3_layer_accumulator_round_trips_through_inner_connector():
    """End-to-end: fan in 4 layers via the accumulator, drain, hand the
    concatenated blob to the P-1 inner connector via ``save``, then
    verify a second request matching the same tokens hits all 16."""
    from kvcache_vllm.layer_accumulator import LayerAccumulator
    from kvcache_vllm import VllmKVConnector

    acc = LayerAccumulator()
    acc.register_layer_order(["L0", "L1", "L2", "L3"])
    acc.bind_request("rp3", list(range(3100, 3116)))
    # 16 tokens × 16 B/token/layer = 256 B per layer; 4 layers = 1 KiB total
    # → bytes_per_token across all 4 layers = 64 B.
    for i in range(4):
        acc.accumulate("rp3", f"L{i}",
                        bytes(((j * (i + 1)) & 0xff for j in range(256))))
    drained = acc.drain_all()
    assert "rp3" in drained
    tokens, concat = drained["rp3"]
    assert len(concat) == 16 * 64  # 16 tokens × 64 bytes/token total

    conn = VllmKVConnector(tenant_id="p3-tenant",
                            model_id="p3-model",
                            bytes_per_token=64)
    try:
        conn.save("rp3", tokens, concat)
        n = conn.get_num_new_matched_tokens("rp3-second", tokens, 0)
        assert n == 16, f"expected 16, got {n}"
    finally:
        conn.release("rp3")
        conn.release("rp3-second")
        conn.close()


# ---------------------------------------------------------------------------
# Phase P-3 — bridge wiring. Skip-marked because importing the bridge
# requires vLLM. The accumulator tests above cover the core logic on
# the default rig.
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _vllm_available(),
                     reason="vllm not installed; install kvcache_vllm[vllm]")
def test_p3_bridge_save_kv_layer_fan_in_then_commit():
    """Drive the bridge through the vLLM-shaped per-layer save
    callbacks: ``register_kv_caches`` → ``save_kv_layer`` ×4 →
    ``wait_for_save``, then verify a re-lookup hit."""
    from kvcache_vllm.vllm_bridge import KVCacheVllmConnector
    from vllm.distributed.kv_transfer.kv_connector.v1.base import (
        KVConnectorRole)

    extra = {"tenant_id":       "p3-bridge-tenant",
              "model_id":        "p3-bridge-model",
              "bytes_per_token": 64}
    cfg = types.SimpleNamespace(
        kv_transfer_config=types.SimpleNamespace(
            kv_connector_extra_config=extra,
        ),
    )
    conn = KVCacheVllmConnector(cfg, KVConnectorRole.WORKER)

    # Register 4 layers in canonical order.
    layers = {f"L{i}": object() for i in range(4)}  # tensors are opaque here
    conn.register_kv_caches(layers)

    # Forward pass: one request, 16 tokens.
    tokens = list(range(4000, 4016))
    rid = "rp3-bridge"
    meta = types.SimpleNamespace(
        request_ids=[rid],
        token_ids_by_request={rid: tokens},
    )

    # Fire layers out-of-order.
    for name in ("L2", "L0", "L3", "L1"):
        payload = bytes(((j + ord(name[-1])) & 0xff for j in range(256)))
        conn.save_kv_layer(name, payload, meta)

    conn.wait_for_save()

    # A second request with the same tokens must hit all 16.
    req_b = types.SimpleNamespace(
        request_id="rp3-bridge-second",
        prompt_token_ids=tokens,
    )
    n_hit, _ = conn.get_num_new_matched_tokens(req_b, 0)
    assert n_hit == 16, f"expected full match, got {n_hit}"

    conn.request_finished(types.SimpleNamespace(request_id=rid))
    conn.request_finished(req_b)


# ---------------------------------------------------------------------------
# Phase P-3.1 — per-layer LOAD fan-out. Splitter is vllm-free so the
# slicing logic gets real unit coverage on the default dev rig; the
# bridge wiring is exercised via a skip-marked vllm-present test.
# ---------------------------------------------------------------------------

def test_p3_1_layer_splitter_slices_in_registered_order():
    """A staged blob slices into per-layer destination buffers in the
    order ``register_layer_order`` declared, regardless of dict-key
    insertion order in ``layer_destinations``."""
    from kvcache_vllm.layer_accumulator import LayerSplitter

    splitter = LayerSplitter()
    splitter.register_layer_order(["L0", "L1", "L2", "L3"])

    # Build a blob of four 64-byte layers with distinctive bytes per
    # layer so a mis-ordered slice is easy to detect.
    layer_payloads = [bytes(((j + 0x10 * i) & 0xff for j in range(64)))
                       for i in range(4)]
    blob = b"".join(layer_payloads)
    # Destinations passed in scrambled order — registration order wins.
    dests = {f"L{i}": bytearray(64) for i in (2, 0, 3, 1)}
    splitter.stage_load("rp31", blob, dests)

    for i, name in enumerate(("L0", "L1", "L2", "L3")):
        served = splitter.drain_layer(name)
        assert served == 1
        assert bytes(dests[name]) == layer_payloads[i], (
            f"layer {name} got wrong slice")

    splitter.finish_request("rp31")
    assert not splitter.has_staged()


def test_p3_1_layer_splitter_noops_on_unregistered_layer():
    """Draining a layer the splitter has never heard of MUST be a
    no-op (return 0) — no exception, no destination writes."""
    from kvcache_vllm.layer_accumulator import LayerSplitter

    splitter = LayerSplitter()
    splitter.register_layer_order(["L0", "L1"])
    dest = bytearray(b"\x00" * 16)
    splitter.stage_load("rid", b"\xaa" * 32, {"L0": dest})

    assert splitter.drain_layer("L42") == 0
    assert bytes(dest) == b"\x00" * 16  # untouched


def test_p3_1_round_trip_accumulator_to_splitter_via_inner_connector():
    """Full P-3 → P-3.1 round-trip on the no-vllm rig:

      1. Accumulator fans 4 layers in for a save.
      2. Inner connector commits the concatenated blob.
      3. A second request matching the same tokens hits — we use
         ``start_load_kv`` to fetch the blob back into a staging
         buffer.
      4. Splitter slices it into 4 per-layer destination bytearrays.
      5. Each destination MUST byte-match its original layer payload.
    """
    from kvcache_vllm.layer_accumulator import (LayerAccumulator,
                                                  LayerSplitter)
    from kvcache_vllm import VllmKVConnector

    layer_order = ["L0", "L1", "L2", "L3"]
    # 16 tokens × 64 bytes-per-token-total = 1024 B; 4 layers
    # → 256 B per layer.
    bytes_per_layer = 256
    layer_payloads = {
        ln: bytes(((j * (i + 7) + 0x20 * i) & 0xff
                    for j in range(bytes_per_layer)))
        for i, ln in enumerate(layer_order)
    }
    tokens = list(range(3200, 3216))

    # Fan in via accumulator, drain, commit through inner connector.
    acc = LayerAccumulator()
    acc.register_layer_order(layer_order)
    acc.bind_request("rp31r", tokens)
    for ln, payload in layer_payloads.items():
        acc.accumulate("rp31r", ln, payload)
    _, concat = acc.drain_request("rp31r")
    assert len(concat) == bytes_per_layer * len(layer_order)

    conn = VllmKVConnector(tenant_id="p31-tenant",
                            model_id="p31-model",
                            bytes_per_token=64)
    try:
        conn.save("rp31r", tokens, concat)

        # Second request, same tokens — hit + fetch into a staging buf.
        load_rid = "rp31r-load"
        n = conn.get_num_new_matched_tokens(load_rid, tokens, 0)
        assert n == 16

        staging = bytearray(len(concat))
        cid = conn.start_load_kv(load_rid, staging)
        conn.wait_for_layer_load(load_rid, cid)

        # Splitter slices into per-layer destinations.
        splitter = LayerSplitter()
        splitter.register_layer_order(layer_order)
        dests = {ln: bytearray(bytes_per_layer) for ln in layer_order}
        splitter.stage_load(load_rid, bytes(staging), dests)
        for ln in layer_order:
            assert splitter.drain_layer(ln) == 1
            assert bytes(dests[ln]) == layer_payloads[ln], (
                f"layer {ln} content mismatch after round-trip")
    finally:
        conn.release("rp31r")
        conn.release("rp31r-load")
        conn.close()


@pytest.mark.skipif(not _vllm_available(),
                     reason="vllm not installed; install kvcache_vllm[vllm]")
def test_p3_1_bridge_start_load_kv_fans_out_to_per_layer_dests():
    """Drive the bridge through the full P-3 → P-3.1 wire: save via
    ``save_kv_layer``, then for a second request use ``start_load_kv``
    + ``wait_for_layer_load`` per layer and verify each engine-side
    destination buffer receives the original layer's bytes.
    """
    from kvcache_vllm.vllm_bridge import KVCacheVllmConnector
    from vllm.distributed.kv_transfer.kv_connector.v1.base import (
        KVConnectorRole)

    extra = {"tenant_id":       "p31-bridge-tenant",
              "model_id":        "p31-bridge-model",
              "bytes_per_token": 64}
    cfg = types.SimpleNamespace(
        kv_transfer_config=types.SimpleNamespace(
            kv_connector_extra_config=extra,
        ),
    )
    conn = KVCacheVllmConnector(cfg, KVConnectorRole.WORKER)

    layer_order = ["L0", "L1", "L2", "L3"]
    conn.register_kv_caches({ln: object() for ln in layer_order})

    save_rid = "rp31-bridge-save"
    tokens = list(range(4100, 4116))
    save_meta = types.SimpleNamespace(
        request_ids=[save_rid],
        token_ids_by_request={save_rid: tokens},
    )
    # 256 B per layer × 4 layers = 1024 B = 16 tokens × 64 B/token.
    layer_payloads = {ln: bytes((((j + 1) * (i + 3)) & 0xff
                                  for j in range(256)))
                       for i, ln in enumerate(layer_order)}
    for ln, payload in layer_payloads.items():
        conn.save_kv_layer(ln, payload, save_meta)
    conn.wait_for_save()

    # Second request: same tokens, drive the load fan-out.
    load_rid = "rp31-bridge-load"
    req = types.SimpleNamespace(request_id=load_rid,
                                  prompt_token_ids=tokens)
    n_hit, _ = conn.get_num_new_matched_tokens(req, 0)
    assert n_hit == 16

    dests = {ln: bytearray(256) for ln in layer_order}
    conn.start_load_kv(request_ids=[load_rid],
                        layer_destinations={load_rid: dests})
    for ln in layer_order:
        conn.wait_for_layer_load(ln)

    for ln, expected in layer_payloads.items():
        assert bytes(dests[ln]) == expected, (
            f"bridge load fan-out: layer {ln} mismatch")

    conn.request_finished(types.SimpleNamespace(request_id=save_rid))
    conn.request_finished(req)
