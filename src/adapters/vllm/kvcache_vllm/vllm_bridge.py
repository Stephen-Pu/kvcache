"""Phase P-2 — vLLM v1 ``KVConnectorBase_V1`` subclass.

This module is the bridge between vLLM's connector lifecycle and the
in-tree :class:`VllmKVConnector` (Phase P-1). Importing it requires
vLLM to be installed; the rest of ``kvcache_vllm`` stays vLLM-free, so
this is the SINGLE module a deployment opts into.

Wiring into a real vLLM deployment (declarative YAML — no code changes
on the engine side):

  kv_transfer_config:
    kv_connector: KVCacheVllmConnector
    kv_connector_module_path: kvcache_vllm.vllm_bridge
    kv_connector_extra_config:
      tenant_id:       tenant-a
      model_id:        llama-3-70b
      bytes_per_token: 32768

vLLM's connector factory loads the class by name from
``kv_connector_module_path`` and constructs it as
``KVCacheVllmConnector(vllm_config, role[, kv_cache_config])``. The
third argument is required from vLLM ≥ v0.10 (Phase P-4.3 shim);
older versions accept the 2-arg form. Same source either way. All
engine-side callbacks fan in through the subclass methods below,
which translate vLLM's argument shapes (``Request`` objects,
scheduler outputs, etc.) into the string ``request_id`` +
``Sequence[int]`` tokens the P-1 core understands.

What's exposed (mirrors vLLM v1 ``KVConnectorBase_V1`` + the
``SupportsHMA`` mixin when present, via conditional inheritance —
Phase D-6):

  * Scheduler role: ``get_num_new_matched_tokens`` (sync OR
    async per the ``async_load`` extra-config knob — Phase P-3.2),
    ``update_state_after_alloc``, ``build_connector_meta``,
    ``get_finished`` (unions inner sync + async-driver
    completions), ``request_finished`` (returns ``(False, None)``
    per Phase P-4.2),
    ``request_finished_all_groups`` (SupportsHMA contract —
    Phase D-6 — delegates to ``request_finished``).

  * Worker role: ``register_kv_caches``, ``start_load_kv``
    (drains the async-load driver OR drives a sync inline
    Fetch — Phase P-3.1), ``wait_for_layer_load`` (slices a
    staged blob into per-layer destinations via
    :class:`LayerSplitter`), ``save_kv_layer`` (fans bytes
    into :class:`LayerAccumulator` keyed by request), and
    ``wait_for_save`` (commits all buffered saves through
    the inner connector — Phase P-3).

Out-of-scope today: real-network priority-class differentiation
(``bench_priority`` can't drive enough queue depth on loopback to
let priorities bite; the structural fix is to drive the bridge
through gRPC NodeDataService, tracked as S-7.1).

LLD reference: §6.1.4.
"""
from __future__ import annotations

import inspect
from typing import Iterable, Optional, Sequence, Set, Tuple

# Importing this module hard-requires vLLM. The ``kvcache_vllm.connector``
# module remains import-clean without vLLM — operators only pull this
# bridge in when they're actually integrating with the engine.
from vllm.distributed.kv_transfer.kv_connector.v1.base import (  # noqa: F401
    KVConnectorBase_V1,
    KVConnectorRole,
)

# Phase D-6 — opt into the SupportsHMA mixin when this vLLM exposes it.
# SupportsHMA was added in vLLM v0.10ish; older versions (v0.8/v0.9)
# don't have it. When present, vLLM workflows may pattern-match on
# `isinstance(conn, SupportsHMA)` to enable heterogeneous-memory-access
# code paths; opting in tells the engine "this connector understands
# the SupportsHMA lifecycle." Our implementation of the mixin's
# abstract method (request_finished_all_groups) delegates to
# request_finished, since the bridge operates at request-level not
# block-level. The placeholder `object` keeps inheritance valid on
# older vLLM versions without forking the class definition.
try:
    from vllm.distributed.kv_transfer.kv_connector.v1.base import (
        SupportsHMA)
    _HAS_SUPPORTS_HMA = True
except ImportError:
    SupportsHMA = type("_NoSupportsHMA", (object,), {})
    _HAS_SUPPORTS_HMA = False

# Phase P-4.3 — cache the base-class ctor's parameter set at module
# load time so __init__ doesn't pay an inspect.signature() call on
# every connector construction. Computed once; read on every
# super().__init__ forwarding decision.
try:
    _BASE_INIT_PARAMS = set(
        inspect.signature(KVConnectorBase_V1.__init__).parameters.keys())
except (TypeError, ValueError):
    _BASE_INIT_PARAMS = set()

from kvcache_vllm.async_load import AsyncLoadDriver
from kvcache_vllm.connector import VllmKVConnector
from kvcache_vllm.layer_accumulator import LayerAccumulator, LayerSplitter


# Default per-token KV byte budget — sized for Llama-3-70B at FP16
# (32 KiB / token across all layers + heads). Operators override via
# ``kv_connector_extra_config.bytes_per_token``.
_DEFAULT_BYTES_PER_TOKEN = 32 * 1024

# Phase P-3.2 async-load defaults.
_DEFAULT_ASYNC_LOAD = False
_DEFAULT_ASYNC_LOAD_WORKERS = 4


def _extract_extra(vllm_config) -> dict:
    """Pull the ``kv_connector_extra_config`` dict out of a vllm_config.

    vLLM's config tree has gone through a few shape changes; we walk
    the canonical v1 path and fall back to an empty dict so a deployment
    without the extra-config block still constructs (with defaults).
    """
    extra = {}
    transfer = getattr(vllm_config, "kv_transfer_config", None)
    if transfer is None:
        return extra
    candidate = getattr(transfer, "kv_connector_extra_config", None)
    if isinstance(candidate, dict):
        extra = candidate
    elif candidate is not None:
        # Some vLLM versions store it as an attribute bag; coerce
        # by reading its public-attribute snapshot.
        extra = {k: v for k, v in vars(candidate).items()
                 if not k.startswith("_")}
    return extra


class KVCacheVllmConnector(KVConnectorBase_V1, SupportsHMA):  # type: ignore[misc]
    """vLLM v1 ``KVConnectorBase_V1`` subclass over P-1's
    :class:`VllmKVConnector`.

    All ``KVConnectorBase_V1`` callbacks fan in here; we translate vLLM's
    object-shaped arguments (``Request``, ``SchedulerOutput``, ...) into
    the string-id + token-list form the underlying core uses and forward
    each call.
    """

    # -- construction ----------------------------------------------------

    def __init__(self, vllm_config, role: "KVConnectorRole",
                  kv_cache_config=None):
        # Build internal state FIRST, before super().__init__. The base
        # class might (now or in a future vLLM release) invoke our own
        # methods during its constructor — e.g. via a virtual hook or a
        # registry callback. Constructing our attributes first means a
        # reentrant call sees a fully-initialised object rather than an
        # AttributeError on ``self._inner``. Cheap defence; the
        # ``super().__init__`` below still happens before we return.
        extra = _extract_extra(vllm_config)
        tenant_id = str(extra.get("tenant_id", "default"))
        model_id  = str(extra.get("model_id",  "default"))
        bytes_per_token = int(extra.get(
            "bytes_per_token", _DEFAULT_BYTES_PER_TOKEN))
        self._inner = VllmKVConnector(
            tenant_id=tenant_id,
            model_id=model_id,
            bytes_per_token=bytes_per_token,
        )
        # Where worker-side ``register_kv_caches`` stashes the engine's
        # KV tensors so ``start_load_kv`` / ``save_kv_layer`` can find a
        # destination buffer. Empty on the SCHEDULER role.
        self._kv_caches: dict = {}
        # Per-layer save buffer (Phase P-3). The worker fires
        # ``save_kv_layer`` once per attention layer during the forward
        # pass; we fan those bytes in here and commit a single
        # concatenated blob through the P-1 inner connector when
        # ``wait_for_save`` fires at end-of-step.
        self._accum = LayerAccumulator()
        # Per-layer LOAD fan-out (Phase P-3.1). The bridge fetches each
        # active request's saved blob into a staging buffer; this
        # splitter slices it into the engine's per-layer destination
        # tensors in registered-layer order.
        self._splitter = LayerSplitter()
        # Phase P-3.2 — async load path. When enabled, the Fetch fires
        # in a background thread on every cache hit and the second
        # tuple element of ``get_num_new_matched_tokens`` flips to
        # ``True`` so vLLM defers admission until ``get_finished``
        # reports the load complete.
        self._async_load = bool(extra.get("async_load",
                                            _DEFAULT_ASYNC_LOAD))
        if self._async_load:
            workers = int(extra.get("async_load_workers",
                                      _DEFAULT_ASYNC_LOAD_WORKERS))
            self._async_driver: Optional[AsyncLoadDriver] = AsyncLoadDriver(
                self._inner, workers=max(1, workers))
        else:
            self._async_driver = None

        # Phase P-4.3 — vLLM ≥ v0.10 added ``kv_cache_config`` as a
        # required third positional arg to
        # ``KVConnectorBase_V1.__init__``. Older versions (v0.8.5,
        # v0.9.x) don't take it. The cached ``_BASE_INIT_PARAMS`` set
        # (computed once at module load) tells us which arity to
        # forward — same source ships against every vLLM in the
        # supported window without paying ``inspect.signature()`` on
        # every connector construction.
        if "kv_cache_config" in _BASE_INIT_PARAMS:
            super().__init__(vllm_config, role, kv_cache_config)
        else:
            super().__init__(vllm_config, role)

    # -- helpers ---------------------------------------------------------

    @staticmethod
    def _request_id(request) -> str:
        """vLLM ``Request`` objects carry ``request_id``; older code
        paths sometimes pass a bare string. Handle both."""
        if isinstance(request, str):
            return request
        rid = getattr(request, "request_id", None)
        if rid is None:
            raise TypeError(
                f"request must have .request_id or be a str; got {type(request)!r}")
        return str(rid)

    @staticmethod
    def _request_tokens(request) -> Sequence[int]:
        """Extract token ids from a vLLM ``Request``. vLLM versions
        differ on the attribute name (``prompt_token_ids`` vs
        ``all_token_ids``); pick the one that's actually populated."""
        if isinstance(request, (list, tuple)):
            return list(request)
        for attr in ("all_token_ids", "prompt_token_ids", "token_ids"):
            v = getattr(request, attr, None)
            if v:
                return list(v)
        raise TypeError(
            f"request has no token-id attribute we recognise: {type(request)!r}")

    # -- scheduler-side callbacks ----------------------------------------

    def get_num_new_matched_tokens(
        self, request, num_computed_tokens: int
    ) -> Tuple[int, bool]:
        """vLLM v1 returns ``(extra_tokens, is_async)``.

        Sync mode (default, Phase P-3.1): always returns
        ``(n, False)`` — vLLM admits the request immediately and the
        eventual ``start_load_kv`` blocks on the Fetch.

        Async mode (P-3.2, opt-in via ``async_load`` extra-config): on
        a hit, the Fetch is kicked off immediately in a worker thread
        and the second tuple element flips to ``True``. vLLM then
        polls ``get_finished`` and only proceeds with the load once
        the request appears in the ``loaded`` set. Misses still
        return ``(0, False)`` — there's nothing to defer.
        """
        rid = self._request_id(request)
        tokens = self._request_tokens(request)
        n = self._inner.get_num_new_matched_tokens(
            rid, tokens, num_computed_tokens)
        if not (n > 0 and self._async_driver is not None):
            return n, False
        # Async hit — pre-fetch into a per-request staging buffer.
        entry = self._inner._reqs.get(rid)
        if not entry or not entry.handle:
            return n, False
        n_bytes = entry.matched_tokens * self._inner._bytes_per_token
        if n_bytes <= 0:
            return n, False
        staging = bytearray(n_bytes)
        self._async_driver.kick_off(rid, staging)
        return n, True

    def update_state_after_alloc(
        self, request, blocks=None, num_external_tokens: int = 0
    ) -> None:
        """vLLM passes the allocated block descriptors here; we don't
        use them (our backing store is external to vLLM's block pool)
        but the callback shape must accept them."""
        del blocks
        self._inner.update_state_after_alloc(
            self._request_id(request), num_external_tokens)

    def build_connector_meta(self, scheduler_output):
        """Translate vLLM's scheduler output into the per-request meta
        the worker side will read. vLLM versions vary on the exact
        attribute name; we look for the obvious ones and fall back to
        iterating the object itself.
        """
        rids: Iterable[str] = ()
        for attr in ("scheduled_new_reqs", "scheduled_requests",
                     "new_requests", "requests"):
            seq = getattr(scheduler_output, attr, None)
            if seq:
                rids = (self._request_id(r) for r in seq)
                break
        else:
            try:
                rids = (self._request_id(r) for r in scheduler_output)
            except TypeError:
                rids = ()
        return self._inner.build_connector_meta(list(rids))

    def get_finished(
        self, finished_req_ids: Set[str]
    ) -> Tuple[Set[str], Set[str]]:
        """vLLM polls this each tick to learn which requests have
        finished their async load / save.

        Phase P-3.2: in async mode this is where vLLM observes that
        a deferred load has completed. We union the inner connector's
        baseline (sync save-completion bookkeeping) with the in-flight
        async-load futures we've created since the last poll. A
        request whose future has resolved goes into the ``loaded``
        set and stays there (flag ``finished=True``) so subsequent
        polls remain idempotent.
        """
        loaded, saved = self._inner.get_finished(tuple(finished_req_ids))
        if self._async_driver is not None:
            loaded = loaded | self._async_driver.finished_ids()
        return loaded, saved

    def request_finished(self, request, *_args, **_kwargs):
        """vLLM calls this when it's about to discard a request — we
        drop our tracking + release the Core ABI handle. Optional in
        the base class; we provide it so leak-on-cancel doesn't bite.
        Also clears any P-3.1 load-staging state for the request so
        the splitter doesn't leak the staged blob.

        Return value (Phase P-4.2): vLLM v1's contract evolved from
        ``-> None`` (pre-v0.9) to ``-> tuple[bool, dict[str, Any] | None]``
        meaning ``(connector_owns_blocks_async, optional_kv_transfer_params)``.
        We always return ``(False, None)`` — the bridge frees its
        Core-ABI handle synchronously inside this call, so vLLM is
        free to release the block group immediately. Returning the
        tuple is forward-compatible with newer vLLM and harmless on
        older versions (which ignored the return).
        """
        rid = self._request_id(request)
        self._splitter.finish_request(rid)
        # P-3.2: cancel any in-flight async load before releasing the
        # underlying handle, so the worker thread isn't still touching
        # it when ``release`` frees it.
        if self._async_driver is not None:
            self._async_driver.cancel(rid)
        self._inner.release(rid)
        return False, None

    # Phase D-6 — SupportsHMA contract. vLLM ≥ v0.10 may call this
    # INSTEAD of ``request_finished`` when the connector subclasses
    # ``SupportsHMA`` (which we do, conditionally — see the
    # placeholder at module-top). Semantically equivalent for us
    # because the bridge operates at request-level, not at
    # per-block-group granularity — we don't differentiate between
    # "blocks for group 0 are freed" vs "blocks for all groups are
    # freed", we just release the Core ABI handle once per
    # request. The ``block_ids`` arg (``tuple[list[int], ...]``)
    # is accepted but ignored. Return value matches
    # ``request_finished``: ``(False, None)`` = "engine owns blocks,
    # release them now; no transfer params."
    def request_finished_all_groups(self, request, block_ids):
        del block_ids
        return self.request_finished(request)

    # -- worker-side callbacks -------------------------------------------

    def register_kv_caches(self, kv_caches) -> None:
        """vLLM calls this once at worker startup with the model's KV
        tensors (one per attention layer). We stash the dict so
        ``save_kv_layer`` knows the canonical layer order; the per-layer
        fan-in itself is handled by :attr:`_accum`.
        """
        if isinstance(kv_caches, dict):
            self._kv_caches = kv_caches
            order = list(kv_caches.keys())
        else:
            # vLLM occasionally passes a list keyed by layer index;
            # normalise to dict.
            self._kv_caches = {str(i): t for i, t in enumerate(kv_caches)}
            order = [str(i) for i in range(len(kv_caches))]
        self._accum.register_layer_order(order)
        self._splitter.register_layer_order(order)

    def start_load_kv(self, forward_context=None, **kwargs) -> None:
        """Phase P-3.1: fetch each active request's saved blob into a
        staging buffer and stage it for per-layer slicing.

        Two argument paths:

          * ``request_ids`` (list[str]) + ``layer_destinations``
            (``{rid: {layer_name: bytearray}}``) passed via ``kwargs``
            — the canonical test-driver path, also usable by any
            engine that owns its destination buffers directly.
          * ``forward_context`` exposing the same two fields as
            attributes — the real-vLLM path.

        For each request we look up the inner connector's recorded
        ``handle`` + ``matched_tokens`` (set by
        :meth:`get_num_new_matched_tokens`), allocate a staging
        ``bytearray`` of the matching size, drive a synchronous
        Fetch through the inner connector, then stage
        ``(rid, bytes, layer_destinations[rid])`` in the splitter so
        each later ``wait_for_layer_load(layer_name)`` slices the
        right range into the engine's per-layer tensor. The fetch is
        synchronous in this phase; an async-aware path (real
        ``is_async=True`` on the matching ``get_num_new_matched_tokens``)
        is Phase P-3.2.
        """
        request_ids = kwargs.get("request_ids")
        layer_destinations = kwargs.get("layer_destinations") or {}
        if not request_ids and forward_context is not None:
            request_ids = getattr(forward_context, "request_ids", None) or []
            layer_destinations = (
                getattr(forward_context, "layer_destinations", {}) or {})
        if not request_ids:
            return
        for rid in request_ids:
            dests = layer_destinations.get(rid, {})
            # P-3.2 async path: a pre-fetched staging buffer is already
            # in-flight (or done) from get_num_new_matched_tokens. The
            # driver blocks on its future defensively in case the
            # engine called start_load_kv before polling get_finished,
            # then we hand the staged bytes to the splitter.
            if self._async_driver is not None and self._async_driver.has(rid):
                staged = self._async_driver.pop_staging(rid)
                if staged is not None:
                    self._splitter.stage_load(rid, staged, dests)
                    continue
            # Sync path (P-3.1): drive the inner Fetch + wait inline.
            entry = self._inner._reqs.get(rid)
            if not entry or not entry.handle:
                continue
            n_bytes = entry.matched_tokens * self._inner._bytes_per_token
            if n_bytes <= 0:
                continue
            staging = bytearray(n_bytes)
            cid = self._inner.start_load_kv(rid, staging)
            self._inner.wait_for_layer_load(rid, cid)
            self._splitter.stage_load(rid, bytes(staging), dests)

    def wait_for_layer_load(self, layer_name: str) -> None:
        """Phase P-3.1: slice this layer's bytes from each staged blob
        into the registered per-request destination buffer. Safe to
        call for layer names the engine didn't stage destinations for
        — the splitter no-ops cleanly. After every layer has drained,
        callers should issue ``request_finished`` (or the splitter
        clears via ``release``) to release the staging memory.
        """
        self._splitter.drain_layer(layer_name)

    def save_kv_layer(
        self, layer_name, kv_layer, attn_metadata=None, **kwargs
    ) -> None:
        """Phase P-3: buffer one layer's KV bytes for each active
        request. The bytes are concatenated in registered-layer order
        when ``wait_for_save`` fires at end-of-forward and committed
        through the P-1 inner connector as a single Reserve→Seal.

        ``attn_metadata`` (or ``kwargs``) MUST expose:
          * ``request_ids`` — iterable of strings, one per request in
            this forward batch
          * ``token_ids_by_request`` — ``{rid: Sequence[int]}`` with the
            token sequence the eventual Save will Seal under

        The bytes-per-request slicing is uniform: ``len(layer_bytes)``
        is divided evenly across ``request_ids``. The single-request
        case (the dominant prefill shape) is exact; mixed-batch sizing
        is the engine's job to thread, and a future P-3.1 wires
        per-request bounds through ``attn_metadata.seq_lens``.
        """
        rid_to_tokens = self._extract_save_targets(attn_metadata, kwargs)
        if not rid_to_tokens:
            return
        layer_bytes = self._to_bytes(kv_layer)
        if not layer_bytes:
            return
        nreq = len(rid_to_tokens)
        per = len(layer_bytes) // nreq
        for i, (rid, tokens) in enumerate(rid_to_tokens.items()):
            chunk = layer_bytes[i * per : (i + 1) * per]
            self._accum.accumulate(rid, layer_name, chunk)
            if tokens:
                self._accum.bind_request(rid, tokens)

    def wait_for_save(self) -> None:
        """Commit every buffered per-layer save through the P-1 inner
        connector. The inner Save is synchronous (Reserve→Publish→Seal
        all block), so this method blocks until every request's bytes
        are sealed and visible to a subsequent Lookup. Idempotent — a
        second call after a drain finds the buffer empty and returns
        immediately.
        """
        for rid, (tokens, concat) in self._accum.drain_all().items():
            self._inner.save(rid, tokens, concat)
        return None

    # -- helpers (P-3) ---------------------------------------------------

    @staticmethod
    def _extract_save_targets(attn_metadata, kwargs) -> "dict":
        """Pull ``{request_id: token_ids}`` out of an attn_metadata or
        kwargs bag. Real vLLM threads this through the metadata struct
        the engine builds per forward step; tests pass it via kwargs
        directly. We try both — kwargs wins so tests can override.
        """
        rids = kwargs.get("request_ids")
        tokens_by_request = kwargs.get("token_ids_by_request") or {}
        if not rids and attn_metadata is not None:
            rids = getattr(attn_metadata, "request_ids", None) or []
            tokens_by_request = (
                getattr(attn_metadata, "token_ids_by_request", {}) or {})
        if not rids:
            return {}
        return {str(rid): list(tokens_by_request.get(rid, []))
                for rid in rids}

    @staticmethod
    def _to_bytes(kv_layer) -> bytes:
        """Coerce a layer payload to ``bytes``. Accepts:
          * ``bytes`` / ``bytearray`` / ``memoryview`` (tests)
          * objects exposing ``.tobytes()`` (numpy arrays, ctypes bufs,
            CPU torch tensors since 1.13)
          * torch CUDA tensors (via the ``.cpu()`` fallback — bare
            ``.tobytes()`` on a CUDA tensor raises
            ``NotImplementedError``, so we catch and fall through)
        """
        if isinstance(kv_layer, (bytes, bytearray, memoryview)):
            return bytes(kv_layer)
        tobytes = getattr(kv_layer, "tobytes", None)
        if callable(tobytes):
            try:
                return tobytes()
            except NotImplementedError:
                # Production audit #4 (post-D-6): torch CUDA tensors
                # define ``.tobytes`` as a callable but raise
                # NotImplementedError when invoked — the unguarded
                # call used to propagate that error and the
                # ``.cpu()`` fallback below was never reached. Catch
                # explicitly (not bare Exception) so genuine errors
                # from other callables still surface.
                pass
        if hasattr(kv_layer, "cpu"):
            try:
                return kv_layer.cpu().contiguous().numpy().tobytes()
            except Exception:
                pass
        raise TypeError(
            f"cannot convert kv_layer of type {type(kv_layer)!r} to bytes")
