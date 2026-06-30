"""vLLM-shaped KV cache connector over the Core ABI.

vLLM v1's connector surface (``vllm.distributed.kv_transfer.kv_connector
.v1.base.KVConnectorBase``) drives the engine through ~7 methods spread
across the scheduler / model-runner / worker. Phase P-1 ships a thin
in-tree shape that mirrors those names — so wiring this class into a
real vLLM deployment is a sed-rename from ``VllmKVConnector`` to a
subclass of ``KVConnectorBase`` (plus the `__init__(vllm_config, role)`
boilerplate that vllm-side integration owns).

The class is intentionally vllm-import-free so the adapter ships
without a runtime dependency on vLLM. Engines integrate by:

    from kvcache_vllm import VllmKVConnector
    self._kv = VllmKVConnector(tenant_id, model_id, bytes_per_token)

…and forwarding their existing connector callbacks to the matching
methods on this class. The connector keeps a per-request ``handle``
table so vLLM's request-id-based bookkeeping maps cleanly to our
Core ABI handles.

Limitations:
  * No per-layer save (``save_kv_layer``). vLLM normally drives this
    layer-by-layer for pipelined writes; we coalesce all layers into
    one Reserve/Publish/Seal, which is fine for the engine because
    the engine still pays the wait latency only once. A future
    extension can add a layer-by-layer surface.
  * ``get_finished`` is synchronous — every load+save completes
    inside the matching ``wait_*`` call, so the returned sets are
    just the ids the caller asked us to track this batch.

LLD reference: §6.1.4.
"""

from __future__ import annotations

import array
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Set

from kvcache_core import KVCacheConnector, compress_store


@dataclass
class _RequestEntry:
    """Per-request state the connector holds between scheduler and worker.

    vLLM's connector callbacks are spread across scheduler ticks +
    model-runner forward passes; we need somewhere to remember the
    `handle` minted on Lookup so the eventual `start_load_kv` can
    Fetch against the right slot.
    """

    matched_tokens: int = 0
    handle: int = 0
    finished_load: bool = False
    finished_save: bool = False
    # Phase A6 — compressed-mode load is two-phase: start_load_kv fetches the
    # variable-size codec blob into ``comp_buf`` and remembers the engine's
    # ``dst_buffer``; wait_for_layer_load decodes the blob and copies the
    # decompressed matched-prefix bytes into that destination. Both stay None
    # in the uncompressed path (the fetch lands straight in dst_buffer).
    comp_buf: Optional[bytearray] = None
    dst_buffer: Optional[bytearray] = None


@dataclass
class VllmConnectorMetadata:
    """What ``build_connector_meta`` returns: per-request hit info the
    scheduler hands to the worker for the next forward pass.

    Mirrors the shape of vLLM's own ``KVConnectorMetadata`` so a real
    integration can drop this struct into the bus without conversion.
    """

    matched_tokens_by_request: Dict[str, int] = field(default_factory=dict)


class VllmKVConnector:
    """vLLM-shaped wrapper around :class:`KVCacheConnector`.

    Lifecycle (mirrors vLLM v1 connector):

      Scheduler tick (on a new request):
          n = conn.get_num_new_matched_tokens(rid, tokens, num_computed)
          conn.update_state_after_alloc(rid, n)

      Worker forward pass (prefill):
          meta = conn.build_connector_meta([rid])
          conn.start_load_kv(meta, dst_buffer)  # Fetch into engine buffer
          conn.wait_for_layer_load(rid)         # blocks until bytes arrive

      Worker forward pass (decode finish):
          conn.save(rid, all_tokens, kv_bytes)
          conn.wait_for_save(rid)

      Scheduler:
          loaded, saved = conn.get_finished({rid1, rid2})
          conn.release(rid1); conn.release(rid2)
    """

    CHUNK_TOKENS = 16

    def __init__(self, tenant_id: str, model_id: str,
                 bytes_per_token: int, *, compress: bool = False,
                 compress_bits: int = 8) -> None:
        if bytes_per_token <= 0:
            raise ValueError("bytes_per_token must be positive")
        # Phase A6 — opt-in lossy KV-tensor compression on the flagship
        # vLLM hot path (shared kvcache_core helper, same codec as the
        # SGLang/AIBrix adapters). The KV bytes are fp32 [tokens][elems], so
        # bytes_per_token must be a whole number of floats.
        if compress and bytes_per_token % 4 != 0:
            raise ValueError("compress requires bytes_per_token to be a "
                             "multiple of 4 (fp32 elements)")
        self._cx = KVCacheConnector(tenant_id=tenant_id, model_id=model_id)
        self._bytes_per_token = bytes_per_token
        self._compress = compress
        self._compress_bits = compress_bits
        self._reqs: Dict[str, _RequestEntry] = {}
        self._closed = False

    # ---- scheduler-side hooks -------------------------------------------

    def get_num_new_matched_tokens(self, request_id: str,
                                    token_ids: Sequence[int],
                                    num_computed_tokens: int) -> int:
        """Return how many tokens beyond ``num_computed_tokens`` can be
        skipped because they're cached externally. vLLM uses this to
        decide chunked-prefill admission.

        On miss returns 0. On hit, the connector remembers the
        Core ABI handle so a later ``start_load_kv`` can Fetch the
        bytes without re-doing the Lookup.
        """
        result = self._cx.lookup(token_ids)
        if result is None:
            self._reqs[request_id] = _RequestEntry()
            return 0
        # vLLM only credits tokens BEYOND num_computed_tokens.
        extra = max(0, int(result.matched_tokens) - num_computed_tokens)
        self._reqs[request_id] = _RequestEntry(
            matched_tokens=int(result.matched_tokens),
            handle=int(result.handle),
        )
        return extra

    def update_state_after_alloc(self, request_id: str,
                                  num_external_tokens: int) -> None:
        """No-op hook vLLM calls once the worker has allocated the
        destination KV blocks for the externally-served tokens. We
        record nothing extra — :meth:`start_load_kv` reads the handle
        directly from :attr:`_reqs`. Kept so the method name aligns
        with vLLM's expected callback shape.
        """
        del num_external_tokens
        if request_id not in self._reqs:
            self._reqs[request_id] = _RequestEntry()

    # ---- worker-side hooks ----------------------------------------------

    def build_connector_meta(
        self, request_ids: Sequence[str]) -> VllmConnectorMetadata:
        """Hand the worker the per-request match info it needs to drive
        the next forward pass. Mirrors vLLM's ``build_connector_meta``."""
        return VllmConnectorMetadata(matched_tokens_by_request={
            rid: self._reqs[rid].matched_tokens
            for rid in request_ids if rid in self._reqs
        })

    def start_load_kv(self, request_id: str, dst_buffer: bytearray) -> int:
        """Async Fetch the cached KV bytes for ``request_id`` into
        ``dst_buffer``. Returns the Core ABI completion id; vLLM does
        not need this directly, the matching :meth:`wait_for_layer_load`
        consumes it. Raises if the request has no recorded hit.
        """
        entry = self._reqs.get(request_id)
        if not entry or entry.handle == 0:
            raise KeyError(
                f"start_load_kv({request_id!r}): no recorded Lookup hit")
        n_bytes = entry.matched_tokens * self._bytes_per_token
        if len(dst_buffer) < n_bytes:
            raise ValueError(
                f"dst_buffer too small: {len(dst_buffer)} < {n_bytes}")
        if self._compress:
            # A6 — the slot holds a variable-size codec blob, not raw KV. Fetch
            # it into an internal buffer sized via stored_bytes; the decode +
            # copy-into-dst_buffer happens in wait_for_layer_load once the
            # async fetch has completed.
            stored = self._cx.stored_bytes(entry.handle)
            entry.comp_buf = bytearray(stored)
            entry.dst_buffer = dst_buffer
            return self._cx.fetch(entry.handle, entry.comp_buf)
        return self._cx.fetch(entry.handle, dst_buffer)

    def wait_for_layer_load(self, request_id: str,
                             completion_id: int,
                             timeout_ms: int = 5000) -> None:
        """Block until the matching ``start_load_kv`` finishes. The
        layer-by-layer dance vLLM normally does isn't relevant here
        (we batched the layers into one slot at save time) — the name
        stays for callback parity. Marks the request as load-finished
        so :meth:`get_finished` reflects it.
        """
        self._cx.wait(completion_id, timeout_ms=timeout_ms)
        entry = self._reqs.get(request_id)
        if entry and entry.comp_buf is not None:
            # A6 — the async fetch has landed the codec blob; decode it and
            # copy the decompressed matched-prefix KV into the engine buffer
            # the worker handed us in start_load_kv.
            floats, _shape = self._cx.decompress_kv(bytes(entry.comp_buf))
            decoded = array.array("f", floats).tobytes()
            n_bytes = entry.matched_tokens * self._bytes_per_token
            entry.dst_buffer[:n_bytes] = decoded[:n_bytes]
            entry.comp_buf = None
            entry.dst_buffer = None
        if entry:
            entry.finished_load = True

    def save(self, request_id: str, token_ids: Sequence[int],
              kv_bytes: bytes) -> None:
        """Atomic Reserve→Publish→Seal commit at request finish.

        vLLM normally invokes this layer-by-layer (``save_kv_layer``);
        we batch all layers into one slot, which is faithful to the
        Core ABI's chunk granularity (16 tokens × all layers + heads).
        Sets ``finished_save`` for the eventual :meth:`get_finished`.
        """
        if not token_ids:
            raise ValueError("token_ids must be non-empty")
        if not kv_bytes:
            raise ValueError("kv_bytes must be non-empty")
        if self._compress:
            # A6 — compress the fp32 KV and seal the variable-size codec blob
            # under the token prefix. The decompressed-size bookkeeping the
            # load path needs is derived from matched_tokens, not the slot.
            compress_store(self._cx, token_ids, kv_bytes,
                           self._bytes_per_token, bits=self._compress_bits)
        else:
            locator = self._cx.make_locator(token_ids)
            rsv = self._cx.reserve(locator, len(kv_bytes))
            if rsv.slot_bytes < len(kv_bytes):
                raise RuntimeError(
                    f"reserved slot too small: {rsv.slot_bytes} < "
                    f"{len(kv_bytes)}")
            self._cx.write_into_slot(rsv.slot_addr, kv_bytes)
            self._cx.publish(rsv.handle, watermark=len(kv_bytes))
            self._cx.seal(rsv.handle, token_ids)
        # Use setdefault so a save-without-prior-lookup (warm-up path)
        # still produces a tracked entry.
        entry = self._reqs.setdefault(request_id, _RequestEntry())
        entry.finished_save = True

    def wait_for_save(self, request_id: str) -> None:
        """Save is synchronous in the MVP — Reserve/Publish/Seal block
        until done. The hook exists for callback parity; in MVP it's a
        no-op for already-saved requests, and a KeyError otherwise so
        the engine catches a missed save."""
        entry = self._reqs.get(request_id)
        if not entry or not entry.finished_save:
            raise KeyError(
                f"wait_for_save({request_id!r}): no completed save")

    def get_finished(
        self, request_ids: Sequence[str]) -> tuple[Set[str], Set[str]]:
        """Return ``(loaded_ids, saved_ids)`` — the subsets of the
        input that have respectively completed their async load and
        save. Mirrors vLLM's connector method of the same name."""
        loaded: Set[str] = set()
        saved: Set[str] = set()
        for rid in request_ids:
            entry = self._reqs.get(rid)
            if not entry:
                continue
            if entry.finished_load:
                loaded.add(rid)
            if entry.finished_save:
                saved.add(rid)
        return loaded, saved

    def release(self, request_id: str) -> None:
        """Drop the request's recorded state + release the underlying
        Core ABI handle on the owning node. Idempotent — releasing an
        unknown ``request_id`` is a no-op so vLLM's cleanup-on-cancel
        path doesn't have to track which requests we know about."""
        entry = self._reqs.pop(request_id, None)
        if entry and entry.handle:
            try:
                self._cx.release(entry.handle)
            except Exception:
                pass

    # ---- lifecycle ------------------------------------------------------

    def close(self) -> None:
        if not self._closed:
            self._cx.close()
            self._reqs.clear()
            self._closed = True

    def __enter__(self) -> "VllmKVConnector":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
