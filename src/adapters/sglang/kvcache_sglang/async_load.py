"""Phase Q-SG-1 — async load driver for SGLang.

Mirrors the vLLM adapter's ``async_load`` pattern but reshaped for
SGLang's simpler retrieve verb. SGLang's RadixAttention path does a
single ``lookup → fetch → consume`` per cache hit (vs. vLLM's
layer-by-layer ingest), so the driver's per-rid lifecycle is:

  1. ``kick_off(rid, tokens)`` — bridge calls this from
     the SGLang scheduler hook that runs immediately after a hit. We
     do the lookup synchronously (so we can return the matched_tokens
     count the scheduler needs to decide what to recompute), then
     dispatch a Fetch into a freshly-allocated staging buffer on a
     worker thread. Returns the matched_tokens count; 0 means no
     async work was scheduled and the caller should treat it as a miss.

  2. ``finished_ids(candidates)`` — poll which kicked-off rids have
     resolved their Fetch. Idempotent. Surfaces worker exceptions.

  3. ``pop(rid)`` — block on the future, release the inner handle,
     and return the staged bytes for the engine to splice into its
     attention cache. Removes the entry.

  4. ``cancel(rid)`` — drop in-flight state without returning bytes.
     Blocks on a running future first so the worker isn't still
     touching the inner connector when we release the handle.

  5. ``close(wait)`` — shut down the worker pool.

The driver is **connector-protocol-driven** so it gets clean unit
coverage with a small in-Python fake — no live libkvcache.so needed
for the lifecycle / threading tests.
"""
from __future__ import annotations

import concurrent.futures
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Protocol, Sequence, Set


class _ConnectorLike(Protocol):
    """Minimum slice of ``KVCacheConnector`` the driver depends on."""

    def lookup(self, tokens: Sequence[int]):  # -> Optional[LookupResult]
        ...

    def fetch(self, handle: int, dst: bytearray) -> int:
        ...

    def wait(self, cid: int) -> None:
        ...

    def release(self, handle: int) -> None:
        ...


@dataclass
class _Entry:
    future: "concurrent.futures.Future[None]"
    handle: int
    staging: bytearray
    matched_tokens: int
    finished: bool = False


class AsyncLoadDriver:
    """Worker-pool wrapper around ``KVCacheConnector.fetch`` for
    SGLang-style adapters. The connector is held by reference; the
    driver does NOT own its lifecycle (the caller's backend owns it).
    """

    def __init__(self, connector: _ConnectorLike, *,
                 bytes_per_token: int, workers: int = 4) -> None:
        if bytes_per_token <= 0:
            raise ValueError("bytes_per_token must be positive")
        if workers <= 0:
            raise ValueError("workers must be positive")
        self._cx = connector
        self._bytes_per_token = bytes_per_token
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="kvcache-sglang-load")
        self._state: Dict[str, _Entry] = {}

    # ---- per-request lifecycle ----------------------------------------

    def kick_off(self, request_id: str, tokens: Sequence[int]) -> int:
        """Synchronously look up ``tokens``; on hit, dispatch an async
        Fetch and return the matched_tokens count. On miss returns 0
        and schedules nothing — the caller is the source of truth for
        treating it as a miss.

        Overwrites any prior in-flight state for the same rid: the
        scheduler may re-prefill after a cancel, and we don't want to
        leak the previous handle. Cancel-then-kick-off is the explicit
        contract; same-rid back-to-back without cancel is also handled
        (we release the prior handle before installing the new one).
        """
        # If a prior kick-off is still in flight for this rid, cancel it
        # so we don't leak the inner handle.
        if request_id in self._state:
            self.cancel(request_id)

        hit = self._cx.lookup(tokens)
        if hit is None:
            return 0
        matched = int(hit.matched_tokens)
        if matched == 0:
            # Defensive: connector returned a result with zero match.
            # Release immediately and treat as miss.
            try:
                self._cx.release(hit.handle)
            except Exception:
                pass
            return 0
        staging = bytearray(matched * self._bytes_per_token)
        future = self._executor.submit(
            self._fetch_one, hit.handle, staging)
        self._state[request_id] = _Entry(
            future=future, handle=hit.handle, staging=staging,
            matched_tokens=matched)
        return matched

    def _fetch_one(self, handle: int, dst: bytearray) -> None:
        cid = self._cx.fetch(handle, dst)
        self._cx.wait(cid)

    def finished_ids(
        self, candidates: Optional[Iterable[str]] = None
    ) -> Set[str]:
        """Return the subset of ``candidates`` (or all in-flight rids if
        ``None``) whose Fetch future has resolved. Worker exceptions
        surface here — a failed Fetch turns into a visible engine error
        instead of an infinite poll. Idempotent: once a rid is reported
        finished it stays so until ``pop`` or ``cancel`` removes it.
        """
        out: Set[str] = set()
        rids = (list(self._state.keys())
                if candidates is None else list(candidates))
        for rid in rids:
            state = self._state.get(rid)
            if state is None:
                continue
            if state.finished:
                out.add(rid)
                continue
            if state.future.done():
                state.future.result()  # re-raises worker exception
                state.finished = True
                out.add(rid)
        return out

    def pop(self, request_id: str) -> Optional[bytes]:
        """Block on the request's future, release the inner handle, and
        return the staged bytes. Removes the entry. Returns ``None`` if
        the rid was never kicked off (caller should fall through to the
        sync path).
        """
        state = self._state.pop(request_id, None)
        if state is None:
            return None
        try:
            state.future.result()
        finally:
            # Release the handle whether the Fetch succeeded or not —
            # we already paid the refcount on the lookup-side hit and
            # the caller will not retry through this driver entry.
            try:
                self._cx.release(state.handle)
            except Exception:
                pass
        return bytes(state.staging)

    def cancel(self, request_id: str) -> None:
        """Drop in-flight state without returning bytes. Blocks on a
        running future first so the worker is not still touching the
        inner connector when we release the handle.
        """
        state = self._state.pop(request_id, None)
        if state is None:
            return
        if not state.future.done():
            try:
                state.future.result()
            except Exception:
                # Best-effort cleanup — swallow worker exceptions so
                # cancel doesn't take down the engine's cleanup path.
                pass
        try:
            self._cx.release(state.handle)
        except Exception:
            pass

    # ---- inspection ---------------------------------------------------

    def in_flight(self) -> int:
        return len(self._state)

    def has(self, request_id: str) -> bool:
        return request_id in self._state

    def matched_tokens(self, request_id: str) -> Optional[int]:
        state = self._state.get(request_id)
        return None if state is None else state.matched_tokens

    # ---- shutdown -----------------------------------------------------

    def close(self, wait: bool = True) -> None:
        """Shut down the worker pool. ``wait=True`` blocks until all
        in-flight Fetches complete; ``wait=False`` returns immediately
        (workers finish in the background). All remaining handles are
        released regardless of ``wait``.
        """
        rids = list(self._state.keys())
        for rid in rids:
            self.cancel(rid)
        self._executor.shutdown(wait=wait)
