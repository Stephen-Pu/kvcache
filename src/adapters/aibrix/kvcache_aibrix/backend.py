"""AIBrix-shaped KV cache backend over the Core ABI.

AIBrix exposes external KV stores through a KVCache Connector v1
interface with four verbs — ``get`` / ``put`` / ``delete`` / ``exists``.
Match those names here so this class plugs straight into AIBrix's
runtime without an extra translation shim.

The connector is parameterised on ``bytes_per_token`` because AIBrix
calls ``get(key)`` without saying how many bytes it expects back; we
size the destination buffer from the matched-prefix token count.

LLD reference: §6.1.4 (engine adapter strategy).
"""

from __future__ import annotations

from typing import Optional, Sequence, Set

from kvcache_core import KVCacheConnector, compress_retrieve, compress_store

from .async_load import AsyncLoadDriver


class AIBrixKVConnector:
    """KVCache Connector v1 for the AIBrix engine.

    Usage::

        with AIBrixKVConnector(tenant_id="t1",
                                model_id="llama-3-70b",
                                bytes_per_token=64) as kv:
            if kv.exists(tokens):
                buf = kv.get(tokens)
            ...
            kv.put(tokens, kv_bytes)
    """

    CHUNK_TOKENS = 16  # LLD §3.2

    def __init__(self, tenant_id: str, model_id: str,
                 bytes_per_token: int, *, compress: bool = False,
                 compress_bits: int = 8) -> None:
        if bytes_per_token <= 0:
            raise ValueError("bytes_per_token must be positive")
        # KVZ-4 — optional lossy KV-tensor compression (shared helper).
        if compress and bytes_per_token % 4 != 0:
            raise ValueError("compress requires bytes_per_token to be a "
                             "multiple of 4 (fp32 elements)")
        self._cx = KVCacheConnector(tenant_id=tenant_id, model_id=model_id)
        self._bytes_per_token = bytes_per_token
        self._compress = compress
        self._compress_bits = compress_bits
        self._closed = False

    # ----- Connector v1 verbs ----------------------------------------------

    def exists(self, key: Sequence[int]) -> bool:
        """Return True iff *any* prefix of ``key`` is cached.

        AIBrix's contract treats this as a yes/no probe; the caller
        follows up with ``get`` if it needs the matched length.
        """
        hit = self._cx.lookup(key)
        if hit is None:
            return False
        # Release the lookup handle — exists() must not pin a refcount.
        try:
            self._cx.release(hit.handle)
        except Exception:
            pass
        return True

    def get(self, key: Sequence[int]) -> Optional[bytes]:
        """Return the cached bytes for the LPM-matched prefix of ``key``.

        ``None`` on miss. The returned buffer is
        ``matched_tokens * bytes_per_token`` bytes — possibly shorter
        than the caller-supplied ``key`` if only a prefix is cached.
        """
        if self._compress:
            return compress_retrieve(self._cx, key, self._bytes_per_token)
        hit = self._cx.lookup(key)
        if hit is None:
            return None
        n_bytes = int(hit.matched_tokens) * self._bytes_per_token
        buf = bytearray(n_bytes)
        cid = self._cx.fetch(hit.handle, buf)
        self._cx.wait(cid)
        self._cx.release(hit.handle)
        return bytes(buf)

    def put(self, key: Sequence[int], value: bytes) -> None:
        """Atomically commit ``value`` as the cached bytes for ``key``."""
        if not key:
            raise ValueError("key must be non-empty")
        if not value:
            raise ValueError("value must be non-empty")
        if self._compress:
            compress_store(self._cx, key, value, self._bytes_per_token,
                           bits=self._compress_bits)
            return
        locator = self._cx.make_locator(key)
        rsv = self._cx.reserve(locator, len(value))
        if rsv.slot_bytes < len(value):
            raise RuntimeError(
                f"reserved slot too small: {rsv.slot_bytes} < {len(value)}")
        self._cx.write_into_slot(rsv.slot_addr, value)
        self._cx.publish(rsv.handle, watermark=len(value))
        self._cx.seal(rsv.handle, key)

    def delete(self, key: Sequence[int]) -> bool:
        """Hint that ``key`` is no longer hot.

        The MVP Core ABI has no explicit Delete verb — eviction is
        driven by tier capacity and refcount. Surfaced as a no-op so
        AIBrix wiring needs no conditional; always returns ``False``.
        """
        del key
        return False

    # ----- lifecycle -------------------------------------------------------

    def close(self) -> None:
        if not self._closed:
            self._cx.close()
            self._closed = True

    def __enter__(self) -> "AIBrixKVConnector":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class AsyncAIBrixKVConnector:
    """AIBrix KVCache-Connector-v1 surface with an async prefetch path.

    Same sync verbs as :class:`AIBrixKVConnector` (``exists`` / ``get``
    / ``put`` / ``delete``) plus four async verbs the AIBrix runtime
    can drive to overlap KV-restore with model setup:

      * ``prefetch(rid, key) -> matched_tokens`` — sync lookup +
        async fetch; 0 means miss (no work scheduled).
      * ``finished_ids(candidates) -> set`` — poll which rids are
        ready.
      * ``pop(rid) -> bytes`` — block-and-collect; releases the
        inner handle.
      * ``cancel(rid)`` — drop in-flight state; blocks on a running
        future first so the worker is not still touching the inner
        connector when we release.

    Typical AIBrix runtime use::

        with AsyncAIBrixKVConnector(tenant_id="t1", model_id="m",
                                      bytes_per_token=64) as kv:
            matched = kv.prefetch("req-7", key)
            if matched == 0:
                # ... recompute
                ...
            else:
                # ... model setup runs in parallel with the fetch
                ...
                value = kv.pop("req-7")  # blocks if not done
            kv.put(key, finished_value)
    """

    CHUNK_TOKENS = 16  # LLD §3.2

    def __init__(self, tenant_id: str, model_id: str,
                 bytes_per_token: int, *, workers: int = 4) -> None:
        if bytes_per_token <= 0:
            raise ValueError("bytes_per_token must be positive")
        self._cx = KVCacheConnector(tenant_id=tenant_id, model_id=model_id)
        self._bytes_per_token = bytes_per_token
        self._driver = AsyncLoadDriver(
            self._cx, bytes_per_token=bytes_per_token, workers=workers)
        self._closed = False

    # ----- sync Connector v1 verbs ----------------------------------------

    def exists(self, key: Sequence[int]) -> bool:
        hit = self._cx.lookup(key)
        if hit is None:
            return False
        try:
            self._cx.release(hit.handle)
        except Exception:
            pass
        return True

    def get(self, key: Sequence[int]) -> Optional[bytes]:
        hit = self._cx.lookup(key)
        if hit is None:
            return None
        n_bytes = int(hit.matched_tokens) * self._bytes_per_token
        buf = bytearray(n_bytes)
        cid = self._cx.fetch(hit.handle, buf)
        self._cx.wait(cid)
        self._cx.release(hit.handle)
        return bytes(buf)

    def put(self, key: Sequence[int], value: bytes) -> None:
        if not key:
            raise ValueError("key must be non-empty")
        if not value:
            raise ValueError("value must be non-empty")
        locator = self._cx.make_locator(key)
        rsv = self._cx.reserve(locator, len(value))
        if rsv.slot_bytes < len(value):
            raise RuntimeError(
                f"reserved slot too small: {rsv.slot_bytes} < {len(value)}")
        self._cx.write_into_slot(rsv.slot_addr, value)
        self._cx.publish(rsv.handle, watermark=len(value))
        self._cx.seal(rsv.handle, key)

    def delete(self, key: Sequence[int]) -> bool:
        del key
        return False

    # ----- async prefetch path --------------------------------------------

    def prefetch(self, request_id: str, key: Sequence[int]) -> int:
        """Sync lookup + async fetch. Returns matched_tokens; 0 = miss
        (no work scheduled, caller falls through to ``get`` or
        recompute)."""
        return self._driver.prefetch(request_id, key)

    def finished_ids(self, candidates=None) -> Set[str]:
        return self._driver.finished_ids(candidates)

    def pop(self, request_id: str) -> Optional[bytes]:
        return self._driver.pop(request_id)

    def cancel(self, request_id: str) -> None:
        self._driver.cancel(request_id)

    def in_flight(self) -> int:
        return self._driver.in_flight()

    # ----- lifecycle -------------------------------------------------------

    def close(self) -> None:
        if not self._closed:
            self._driver.close(wait=True)
            self._cx.close()
            self._closed = True

    def __enter__(self) -> "AsyncAIBrixKVConnector":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
