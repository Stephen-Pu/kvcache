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

from typing import Optional, Sequence

from kvcache_core import KVCacheConnector


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
                 bytes_per_token: int) -> None:
        if bytes_per_token <= 0:
            raise ValueError("bytes_per_token must be positive")
        self._cx = KVCacheConnector(tenant_id=tenant_id, model_id=model_id)
        self._bytes_per_token = bytes_per_token
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
