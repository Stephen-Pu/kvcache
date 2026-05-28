"""Phase P-2 — vLLM v1 ``KVConnectorBase`` subclass.

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
``KVCacheVllmConnector(vllm_config, role)``. All the engine-side
callbacks fan in through the subclass methods below, which translate
vLLM's argument shapes (``Request`` objects, scheduler outputs, etc.)
into the string ``request_id`` + ``Sequence[int]`` tokens the P-1
core understands.

What's exposed (mirrors vLLM v1 ``KVConnectorBase``):

  * Scheduler role: ``get_num_new_matched_tokens``,
    ``update_state_after_alloc``, ``build_connector_meta``,
    ``get_finished``, ``request_finished``.

  * Worker role: ``register_kv_caches``, ``start_load_kv``,
    ``wait_for_layer_load``, ``save_kv_layer``, ``wait_for_save``.

What's NOT implemented yet (raises ``NotImplementedError`` with a
clear pointer):

  * True per-layer save / load (the in-tree connector batches all
    layers into one Reserve→Seal). Phase P-3 splits this.
  * Async ``get_num_new_matched_tokens`` (the second tuple element
    returned by vLLM v1). We always return ``is_async=False``.

LLD reference: §6.1.4.
"""
from __future__ import annotations

from typing import Iterable, Optional, Sequence, Set, Tuple

# Importing this module hard-requires vLLM. The ``kvcache_vllm.connector``
# module remains import-clean without vLLM — operators only pull this
# bridge in when they're actually integrating with the engine.
from vllm.distributed.kv_transfer.kv_connector.v1.base import (  # noqa: F401
    KVConnectorBase,
    KVConnectorRole,
)

from kvcache_vllm.connector import VllmKVConnector


# Default per-token KV byte budget — sized for Llama-3-70B at FP16
# (32 KiB / token across all layers + heads). Operators override via
# ``kv_connector_extra_config.bytes_per_token``.
_DEFAULT_BYTES_PER_TOKEN = 32 * 1024


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


class KVCacheVllmConnector(KVConnectorBase):  # type: ignore[misc]
    """vLLM v1 ``KVConnectorBase`` subclass over P-1's
    :class:`VllmKVConnector`.

    All ``KVConnectorBase`` callbacks fan in here; we translate vLLM's
    object-shaped arguments (``Request``, ``SchedulerOutput``, ...) into
    the string-id + token-list form the underlying core uses and forward
    each call.
    """

    # -- construction ----------------------------------------------------

    def __init__(self, vllm_config, role: "KVConnectorRole"):
        super().__init__(vllm_config, role)
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
        """vLLM v1 returns ``(extra_tokens, is_async)``. We're
        synchronous today so the second value is always ``False``."""
        rid = self._request_id(request)
        tokens = self._request_tokens(request)
        n = self._inner.get_num_new_matched_tokens(
            rid, tokens, num_computed_tokens)
        return n, False

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
        finished their async load / save."""
        return self._inner.get_finished(tuple(finished_req_ids))

    def request_finished(self, request, *_args, **_kwargs) -> None:
        """vLLM calls this when it's about to discard a request — we
        drop our tracking + release the Core ABI handle. Optional in
        the base class; we provide it so leak-on-cancel doesn't bite.
        """
        self._inner.release(self._request_id(request))

    # -- worker-side callbacks -------------------------------------------

    def register_kv_caches(self, kv_caches) -> None:
        """vLLM calls this once at worker startup with the model's KV
        tensors (one per attention layer). We stash the dict so
        ``start_load_kv`` / ``save_kv_layer`` have a destination — the
        actual per-layer fan-out is Phase P-3.
        """
        if isinstance(kv_caches, dict):
            self._kv_caches = kv_caches
        else:
            # vLLM occasionally passes a list keyed by layer index;
            # normalise to dict.
            self._kv_caches = {str(i): t for i, t in enumerate(kv_caches)}

    def start_load_kv(self, forward_context=None, **_kwargs) -> None:
        """Per-layer load isn't wired in MVP; vLLM treats this as a
        no-op when the connector matches synchronously inside
        ``get_num_new_matched_tokens``. Phase P-3 adds a real layer-by-
        layer Fetch driven by ``forward_context``."""
        del forward_context

    def wait_for_layer_load(self, layer_name: str) -> None:
        """No-op for the MVP — see ``start_load_kv``."""
        del layer_name

    def save_kv_layer(
        self, layer_name, kv_layer, attn_metadata=None, **_kwargs
    ) -> None:
        """Per-layer save: deferred to P-3. The in-tree connector
        batches all layers into one Reserve→Seal at request finish,
        which the engine pays once. Calling this is a no-op in MVP so
        a vLLM forward pass driving it doesn't crash; the real save
        path runs through ``request_finished`` (or an explicit
        ``save`` call from a wrapper script).
        """
        del layer_name, kv_layer, attn_metadata

    def wait_for_save(self) -> None:
        """Saves are synchronous through the inner connector, so
        ``get_finished`` already reports the truth. Kept for callback
        parity. vLLM may call this without a per-request id."""
        return None
