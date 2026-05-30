"""SGLang-shaped KV cache backend over the Core ABI.

SGLang's RadixAttention can plug into an external L2 KV store via a
narrow ``lookup`` / ``store`` / ``retrieve`` / ``drop`` interface — match
those names here so this class drops straight into SGLang's engine code
without a translation shim.

This wraps the engine-agnostic :class:`KVCacheConnector` (see
``connector.py``) and folds the reserve → write → publish → seal dance
into a single ``store`` call, since SGLang only commits a token range
when the request finishes generating.

LLD reference: §6.1.4 (engine adapter strategy).
"""

from __future__ import annotations

from typing import Optional, Sequence, Set

from kvcache_core import KVCacheConnector

from .async_load import AsyncLoadDriver


class SGLangKVBackend:
    """L2 KV backend with SGLang's expected method names.

    Lifecycle::

        with SGLangKVBackend(tenant_id="t1", model_id="llama-3-70b",
                              bytes_per_token=64) as kv:
            n = kv.lookup(tokens)            # 0 on miss
            kv.store(tokens, kv_bytes)       # commit after a request finishes
            bytes_back = kv.retrieve(tokens) # None on miss
    """

    # SGLang's RadixAttention chunks at 16 tokens; our ART uses the same.
    CHUNK_TOKENS = 16

    def __init__(self, tenant_id: str, model_id: str,
                 bytes_per_token: int) -> None:
        if bytes_per_token <= 0:
            raise ValueError("bytes_per_token must be positive")
        self._cx = KVCacheConnector(tenant_id=tenant_id, model_id=model_id)
        self._bytes_per_token = bytes_per_token
        self._closed = False

    # ----- SGLang-style methods --------------------------------------------

    def lookup(self, tokens: Sequence[int]) -> int:
        """Longest-prefix-match token count, or 0 on miss.

        SGLang uses this to decide whether a request can skip recomputing
        the matched prefix. The returned count is always a multiple of
        :pyattr:`CHUNK_TOKENS` — partial chunks are dropped by LPM.
        """
        result = self._cx.lookup(tokens)
        if result is None:
            return 0
        # The connector caller is expected to release the handle if not
        # immediately retrieving; lookup-only callers don't get the
        # handle here, so release it now.
        try:
            self._cx.release(result.handle)
        except Exception:
            pass
        return int(result.matched_tokens)

    def store(self, tokens: Sequence[int], kv_bytes: bytes) -> None:
        """Atomically commit ``kv_bytes`` as the cached KV for ``tokens``.

        SGLang calls this when a generation finishes. The bytes are
        treated as opaque — ``bytes_per_token`` controls how
        :py:meth:`retrieve` slices the matched prefix back out.
        """
        if not tokens:
            raise ValueError("tokens must be non-empty")
        if not kv_bytes:
            raise ValueError("kv_bytes must be non-empty")
        locator = self._cx.make_locator(tokens)
        rsv = self._cx.reserve(locator, len(kv_bytes))
        if rsv.slot_bytes < len(kv_bytes):
            raise RuntimeError(
                f"reserved slot too small: {rsv.slot_bytes} < {len(kv_bytes)}")
        self._cx.write_into_slot(rsv.slot_addr, kv_bytes)
        self._cx.publish(rsv.handle, watermark=len(kv_bytes))
        self._cx.seal(rsv.handle, tokens)

    def retrieve(self, tokens: Sequence[int]) -> Optional[bytes]:
        """Pull cached bytes for the LPM-matched prefix of ``tokens``.

        Returns ``None`` on miss. On hit, the returned buffer covers
        ``matched_tokens * bytes_per_token`` bytes — possibly less than
        what was originally stored if ``tokens`` represents a longer
        sequence than the cached one, or vice versa.
        """
        hit = self._cx.lookup(tokens)
        if hit is None:
            return None
        n_bytes = int(hit.matched_tokens) * self._bytes_per_token
        buf = bytearray(n_bytes)
        cid = self._cx.fetch(hit.handle, buf)
        self._cx.wait(cid)
        self._cx.release(hit.handle)
        return bytes(buf)

    def drop(self, tokens: Sequence[int]) -> bool:
        """Best-effort hint that ``tokens`` are no longer hot.

        The MVP Core ABI has no explicit Drop verb — eviction is driven
        by tier capacity and refcount. We surface the SGLang-expected
        signature so that engine wiring doesn't need a conditional; the
        return value is always ``False`` (i.e. "no immediate action").
        """
        del tokens
        return False

    # ----- lifecycle -------------------------------------------------------

    def close(self) -> None:
        if not self._closed:
            self._cx.close()
            self._closed = True

    def __enter__(self) -> "SGLangKVBackend":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class AsyncSGLangKVBackend:
    """SGLang-shaped backend with an async ``retrieve`` path.

    Same verb set as :class:`SGLangKVBackend` plus four new methods —
    ``kick_off`` / ``finished_ids`` / ``pop`` / ``cancel`` — that let
    the SGLang scheduler overlap KV-restore with the prior layer's
    attention compute. ``lookup``, ``store`` and ``drop`` stay sync
    (their critical-path cost is small and the engine wants them
    inline). ``retrieve`` stays sync as a fallback for callers that
    don't yet drive the async lifecycle.

    Typical SGLang scheduler use::

        with AsyncSGLangKVBackend(tenant_id="t1", model_id="m",
                                    bytes_per_token=64) as kv:
            matched = kv.kick_off("req-7", tokens)  # sync lookup,
                                                       # async fetch
            if matched == 0:
                # ... recompute the whole prompt
                ...
            else:
                # ... begin model setup; the fetch runs in parallel
                ...
                # later, when ready to splice the cache bytes in:
                if "req-7" in kv.finished_ids({"req-7"}):
                    blob = kv.pop("req-7")
                else:
                    blob = kv.pop("req-7")  # block-and-collect
    """

    CHUNK_TOKENS = 16

    def __init__(self, tenant_id: str, model_id: str,
                 bytes_per_token: int, *, workers: int = 4) -> None:
        if bytes_per_token <= 0:
            raise ValueError("bytes_per_token must be positive")
        self._cx = KVCacheConnector(tenant_id=tenant_id, model_id=model_id)
        self._bytes_per_token = bytes_per_token
        self._driver = AsyncLoadDriver(
            self._cx, bytes_per_token=bytes_per_token, workers=workers)
        self._closed = False

    # ----- sync verbs (delegate to connector directly) ---------------------

    def lookup(self, tokens: Sequence[int]) -> int:
        result = self._cx.lookup(tokens)
        if result is None:
            return 0
        try:
            self._cx.release(result.handle)
        except Exception:
            pass
        return int(result.matched_tokens)

    def store(self, tokens: Sequence[int], kv_bytes: bytes) -> None:
        if not tokens:
            raise ValueError("tokens must be non-empty")
        if not kv_bytes:
            raise ValueError("kv_bytes must be non-empty")
        locator = self._cx.make_locator(tokens)
        rsv = self._cx.reserve(locator, len(kv_bytes))
        if rsv.slot_bytes < len(kv_bytes):
            raise RuntimeError(
                f"reserved slot too small: {rsv.slot_bytes} < {len(kv_bytes)}")
        self._cx.write_into_slot(rsv.slot_addr, kv_bytes)
        self._cx.publish(rsv.handle, watermark=len(kv_bytes))
        self._cx.seal(rsv.handle, tokens)

    def retrieve(self, tokens: Sequence[int]) -> Optional[bytes]:
        hit = self._cx.lookup(tokens)
        if hit is None:
            return None
        n_bytes = int(hit.matched_tokens) * self._bytes_per_token
        buf = bytearray(n_bytes)
        cid = self._cx.fetch(hit.handle, buf)
        self._cx.wait(cid)
        self._cx.release(hit.handle)
        return bytes(buf)

    def drop(self, tokens: Sequence[int]) -> bool:
        del tokens
        return False

    # ----- async retrieve path ---------------------------------------------

    def kick_off(self, request_id: str, tokens: Sequence[int]) -> int:
        """Sync lookup + async fetch. Returns matched_tokens; 0 = miss
        (no work scheduled, caller falls through to recompute)."""
        return self._driver.kick_off(request_id, tokens)

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

    def __enter__(self) -> "AsyncSGLangKVBackend":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
