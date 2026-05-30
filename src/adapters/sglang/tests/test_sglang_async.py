"""AsyncLoadDriver + AsyncSGLangKVBackend tests.

The driver is connector-protocol-driven, so lifecycle/threading tests
use a small in-Python fake — no libkvcache.so needed. The one e2e test
at the bottom uses the real C ABI (skipped when the library isn't
reachable).
"""

from __future__ import annotations

import os
import shutil
import threading
import time
from dataclasses import dataclass

import pytest

from kvcache_sglang import AsyncLoadDriver


# ----- fake connector --------------------------------------------------------

@dataclass
class _Hit:
    handle: int
    matched_tokens: int


class FakeConnector:
    """Minimum implementation of the protocol AsyncLoadDriver expects.

    - ``lookup(tokens)`` returns a hit with matched_tokens equal to the
      length of the longest stored prefix whose tokens match. Each hit
      gets a unique handle so the test can assert release was called.
    - ``fetch`` schedules a deterministic blob into ``dst``; the test
      can hold off completion until it signals via ``allow_fetch``.
    - ``release`` records handle releases in ``released``.
    """

    def __init__(self) -> None:
        self.stored: dict[tuple[int, ...], bytes] = {}
        self.released: list[int] = []
        self._next_handle = 1000
        # Gate so the test can verify async semantics (kick_off returns
        # before fetch completes).
        self.fetch_gate = threading.Event()
        self.fetch_gate.set()  # default: no blocking
        self.fetch_calls = 0
        self.lookup_calls = 0
        self.lookup_fail_next = False
        self.fetch_fail_next = False

    def store(self, tokens, blob: bytes) -> None:
        self.stored[tuple(tokens)] = blob

    # ----- protocol --------------------------------------------------

    def lookup(self, tokens):
        self.lookup_calls += 1
        if self.lookup_fail_next:
            self.lookup_fail_next = False
            return None
        toks = tuple(tokens)
        # Longest stored prefix that matches.
        best = None
        for stored_toks in self.stored:
            n = min(len(toks), len(stored_toks))
            if toks[:n] == stored_toks[:n] and n > 0:
                if best is None or n > best[0]:
                    best = (n, stored_toks)
        if best is None:
            return None
        matched, key = best
        self._next_handle += 1
        return _Hit(handle=self._next_handle, matched_tokens=matched)

    def fetch(self, handle: int, dst: bytearray) -> int:
        self.fetch_calls += 1
        self.fetch_gate.wait(timeout=5.0)
        if self.fetch_fail_next:
            self.fetch_fail_next = False
            raise RuntimeError("fetch boom")
        # Deterministic content: every byte is (handle & 0xff).
        for i in range(len(dst)):
            dst[i] = handle & 0xFF
        return handle  # cid mirrors handle for the fake

    def wait(self, cid: int) -> None:
        # Noop — fetch already did the work.
        del cid

    def release(self, handle: int) -> None:
        self.released.append(handle)


# ----- driver lifecycle tests ------------------------------------------------

BPT = 4  # bytes per token


def test_kick_off_miss_schedules_nothing():
    fc = FakeConnector()
    d = AsyncLoadDriver(fc, bytes_per_token=BPT)
    matched = d.kick_off("req-1", [1, 2, 3])  # nothing stored
    assert matched == 0
    assert d.in_flight() == 0
    assert fc.fetch_calls == 0


def test_kick_off_hit_returns_matched_then_pop_returns_bytes():
    fc = FakeConnector()
    fc.store([1, 2, 3, 4], b"\x00" * (4 * BPT))  # blob shape irrelevant
    d = AsyncLoadDriver(fc, bytes_per_token=BPT)
    matched = d.kick_off("req-1", [1, 2, 3, 4, 99])
    assert matched == 4
    assert d.in_flight() == 1
    assert d.matched_tokens("req-1") == 4
    out = d.pop("req-1")
    assert out is not None
    assert len(out) == 4 * BPT
    # FakeConnector fills with (handle & 0xff); same handle across the blob.
    assert len(set(out)) == 1
    assert d.in_flight() == 0
    assert fc.released, "pop must release the inner handle"


def test_fetch_is_actually_async_kick_off_returns_before_fetch_done():
    fc = FakeConnector()
    fc.store([7], b"x" * BPT)
    fc.fetch_gate.clear()  # block fetch in the worker
    d = AsyncLoadDriver(fc, bytes_per_token=BPT)
    t0 = time.monotonic()
    matched = d.kick_off("r", [7])
    elapsed = time.monotonic() - t0
    assert matched == 1
    # kick_off should NOT have waited on fetch — sub-100ms easily.
    assert elapsed < 0.5, f"kick_off blocked {elapsed:.3f}s"
    # Future should still be in flight.
    assert not d.finished_ids({"r"})
    # Release the gate; pop should now return.
    fc.fetch_gate.set()
    out = d.pop("r")
    assert out is not None
    assert len(out) == BPT


def test_finished_ids_polls_correctly_and_is_idempotent():
    fc = FakeConnector()
    fc.store([1], b"a" * BPT)
    fc.store([2], b"b" * BPT)
    d = AsyncLoadDriver(fc, bytes_per_token=BPT)
    d.kick_off("a", [1])
    d.kick_off("b", [2])
    # Wait for both workers to finish (no fetch_gate held).
    for _ in range(100):
        if d.finished_ids({"a", "b"}) == {"a", "b"}:
            break
        time.sleep(0.005)
    # Idempotent: calling again returns the same set without re-checking.
    assert d.finished_ids({"a", "b"}) == {"a", "b"}
    assert d.finished_ids() == {"a", "b"}
    # Pop removes; subsequent finished_ids should drop "a".
    d.pop("a")
    assert d.finished_ids({"a", "b"}) == {"b"}


def test_cancel_releases_handle_and_drops_in_flight():
    fc = FakeConnector()
    fc.store([1], b"a" * BPT)
    d = AsyncLoadDriver(fc, bytes_per_token=BPT)
    d.kick_off("r", [1])
    d.cancel("r")
    assert d.in_flight() == 0
    assert fc.released, "cancel must release the inner handle"
    # Cancel on unknown rid is a no-op (no exception).
    d.cancel("never-existed")


def test_kick_off_replacing_in_flight_rid_releases_prior_handle():
    fc = FakeConnector()
    fc.store([1], b"a" * BPT)
    fc.store([2], b"b" * BPT)
    d = AsyncLoadDriver(fc, bytes_per_token=BPT)
    d.kick_off("r", [1])
    first_handle = fc._next_handle  # most recent
    d.kick_off("r", [2])  # implicit cancel of prior
    # The first handle must be in the released list — otherwise we'd
    # be leaking a refcount on the prior hit.
    assert first_handle in fc.released


def test_worker_exception_surfaces_through_finished_ids():
    fc = FakeConnector()
    fc.store([1], b"a" * BPT)
    fc.fetch_fail_next = True
    d = AsyncLoadDriver(fc, bytes_per_token=BPT)
    d.kick_off("r", [1])
    # finished_ids re-raises the worker exception once the future resolves.
    deadline = time.monotonic() + 1.0
    raised = None
    while time.monotonic() < deadline:
        try:
            d.finished_ids({"r"})
        except RuntimeError as e:
            raised = e
            break
        if d._state["r"].future.done():
            # Force the call that surfaces it.
            try:
                d.finished_ids({"r"})
            except RuntimeError as e:
                raised = e
            break
        time.sleep(0.005)
    assert raised is not None and "boom" in str(raised)


def test_close_blocks_and_releases_remaining_handles():
    fc = FakeConnector()
    fc.store([1], b"a" * BPT)
    d = AsyncLoadDriver(fc, bytes_per_token=BPT)
    d.kick_off("r", [1])
    d.close(wait=True)
    assert fc.released, "close must release in-flight handles"


def test_constructor_validates_args():
    fc = FakeConnector()
    with pytest.raises(ValueError):
        AsyncLoadDriver(fc, bytes_per_token=0)
    with pytest.raises(ValueError):
        AsyncLoadDriver(fc, bytes_per_token=4, workers=0)


# ----- e2e against real C ABI ------------------------------------------------

CHUNK = 16
BYTES_PER_TOKEN = 64


def _have_library() -> bool:
    if os.environ.get("KVCACHE_LIB"):
        return os.path.isfile(os.environ["KVCACHE_LIB"])
    return shutil.which("cmake") is not None


cffi_avail = pytest.importorskip(
    "cffi", reason="cffi is required for the C ABI tests")


@pytest.mark.skipif(not _have_library(), reason="libkvcache.so not available")
def test_async_backend_kick_off_pop_round_trip_against_real_abi():
    from kvcache_sglang import AsyncSGLangKVBackend
    tokens = list(range(2 * CHUNK))
    payload = bytes(((i * 13) & 0xFF
                      for i in range(len(tokens) * BYTES_PER_TOKEN)))
    with AsyncSGLangKVBackend(tenant_id="sglang-async-tenant",
                                model_id="sglang-async-demo",
                                bytes_per_token=BYTES_PER_TOKEN,
                                workers=2) as kv:
        kv.store(tokens, payload)
        matched = kv.kick_off("req-async-1", tokens)
        assert matched == 2 * CHUNK
        # The fetch is in flight; pop must block-and-return the bytes.
        out = kv.pop("req-async-1")
        assert out == payload
        assert kv.in_flight() == 0
