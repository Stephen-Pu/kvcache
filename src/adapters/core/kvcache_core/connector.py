"""Engine-agnostic Python wrapper around the Core ABI.

This is the shared substrate every engine adapter (vLLM, SGLang, AIBrix,
TRT-LLM, ad-hoc demos) builds on. The engine-flavoured surfaces live in
sibling packages — e.g. ``kvcache_sglang.SGLangKVBackend`` wraps this
class with the lookup/store/retrieve method names SGLang's
RadixAttention connector expects.

LLD reference: §6.1.2 (Core ABI) and §6.1.4 (engine adapter strategy).
"""

from __future__ import annotations

import ctypes
import hashlib
from dataclasses import dataclass
from typing import Sequence

from . import _ffi

_ABI_VERSION = 2  # ABI-1: kv_ctx_config_t gained the trailing `tuning` pointer
_CHUNK_TOKENS = 16  # LLD §3.2


class KVCacheError(RuntimeError):
    """Raised on any non-OK Core ABI return code."""

    def __init__(self, op: str, status: int) -> None:
        msg = _ffi.ffi().string(_ffi.lib().kv_status_str(status)).decode()
        super().__init__(f"{op}: {msg} (status={status})")
        self.status = status


@dataclass
class LookupResult:
    matched_tokens: int
    handle: int


@dataclass
class ReserveResult:
    handle: int
    slot_addr: int
    slot_bytes: int


class KVCacheConnector:
    """Engine-agnostic adapter over the Core ABI.

    Lifecycle:

        c = KVCacheConnector(tenant_id="t1", model_id="llama-3-70b")
        ...
        c.close()
    """

    def __init__(self, tenant_id: str, model_id: str) -> None:
        self._ffi = _ffi.ffi()
        self._lib = _ffi.lib()

        cfg = self._ffi.new("kv_ctx_config_t*")
        cfg.abi_version = _ABI_VERSION
        cfg.agent_endpoint = self._ffi.NULL
        # Pin the bytes lifetime so cffi pointers stay valid.
        self._tid = tenant_id.encode("utf-8")
        self._mid = model_id.encode("utf-8")
        cfg.tenant_id = self._ffi.new("char[]", self._tid)
        cfg.model_id = self._ffi.new("char[]", self._mid)
        cfg.flags = 0

        ctx_pp = self._ffi.new("kv_ctx_t**")
        rc = self._lib.kv_ctx_open(cfg, ctx_pp)
        if rc != 0:
            raise KVCacheError("kv_ctx_open", rc)
        self._ctx = ctx_pp[0]
        self._tenant_id = tenant_id
        self._model_id = model_id

    # ------------------------------------------------------------------ utils

    def make_locator(self, tokens: Sequence[int]) -> "object":
        """Build a Locator: 16B tenant + 8B model + 16B prefix_hash + zeroed range.

        The prefix_hash here is BLAKE2b-128 of the raw token bytes — the
        server's ART keys are derived from the same hash, so the engine and
        server agree without exchanging the tokens themselves.
        """
        loc = self._ffi.new("kv_locator_t*")
        # tenant_id: first 16 bytes of a SHA-1 of the tenant string.
        tid_digest = hashlib.sha1(self._tid).digest()[:16]
        for i, b in enumerate(tid_digest):
            loc.tenant_id[i] = b
        # model_id_hash: 64-bit FNV-1a — must match HeadlessNode's hash in
        # kv_ctx_open (kept identical to keep wiring trivial).
        h = 0xCBF29CE484222325
        for ch in self._mid:
            h ^= ch
            h = (h * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
        loc.model_id_hash = h
        # prefix_hash: BLAKE2b-128 of the LE-packed token bytes.
        tok_bytes = b"".join(int(t).to_bytes(4, "little") for t in tokens)
        ph = hashlib.blake2b(tok_bytes, digest_size=16).digest()
        for i, b in enumerate(ph):
            loc.prefix_hash[i] = b
        loc.version = 1
        loc.flags = 0
        return loc

    # ------------------------------------------------------------------ ABI

    def lookup(self, tokens: Sequence[int]) -> LookupResult | None:
        """Longest-prefix-match. Returns ``None`` on miss."""
        n = len(tokens)
        tok_arr = self._ffi.new("uint32_t[]", list(map(int, tokens)))
        meta = self._ffi.new("kv_locator_t*")
        handle = self._ffi.new("kv_handle_t*")
        matched = self._ffi.new("uint32_t*")
        rc = self._lib.kv_lookup(self._ctx, tok_arr, n, meta, handle, matched)
        if rc == -3:  # KV_E_NOT_FOUND
            return None
        if rc != 0:
            raise KVCacheError("kv_lookup", rc)
        return LookupResult(matched_tokens=int(matched[0]), handle=int(handle[0]))

    def reserve(self, locator, n_bytes: int) -> ReserveResult:
        handle = self._ffi.new("kv_handle_t*")
        slot = self._ffi.new("kv_buffer_desc_t*")
        rc = self._lib.kv_reserve(self._ctx, locator, n_bytes, handle, slot)
        if rc != 0:
            raise KVCacheError("kv_reserve", rc)
        return ReserveResult(
            handle=int(handle[0]),
            slot_addr=int(self._ffi.cast("uintptr_t", slot.addr)),
            slot_bytes=int(slot.len),
        )

    def write_into_slot(self, slot_addr: int, data: bytes) -> None:
        """Copy bytes directly into the server-allocated slot (zero-copy ABI;
        the caller-side memcpy here stands in for what a real engine does with
        a CUDA device->host copy in production)."""
        ctypes.memmove(slot_addr, data, len(data))

    def publish(self, handle: int, watermark: int) -> None:
        empty = self._ffi.new("kv_buffer_desc_t*")
        rc = self._lib.kv_publish(self._ctx, handle, empty[0], watermark)
        if rc != 0:
            raise KVCacheError("kv_publish", rc)

    def seal(self, handle: int, tokens: Sequence[int]) -> None:
        n = len(tokens)
        tok_arr = self._ffi.new("uint32_t[]", list(map(int, tokens)))
        rc = self._lib.kv_seal(self._ctx, handle, tok_arr, n)
        if rc != 0:
            raise KVCacheError("kv_seal", rc)

    def fetch(self, handle: int, dst: bytearray) -> int:
        """Pull bytes for an LPM-matched handle into ``dst``.

        Returns the completion id. Use :py:meth:`wait` to block on it (loopback
        completes inline in MVP, so wait is a formality).
        """
        n = len(dst)
        buf = (ctypes.c_uint8 * n).from_buffer(dst)
        slot = self._ffi.new("kv_buffer_desc_t*")
        slot.addr = self._ffi.cast("void*", ctypes.addressof(buf))
        slot.len = n
        slot.mem_type = 0  # KV_MEM_HOST
        slot.mr_key = 0
        cid = self._ffi.new("kv_completion_t*")
        rc = self._lib.kv_fetch(self._ctx, handle, self._ffi.NULL, 0, slot[0], cid)
        if rc != 0:
            raise KVCacheError("kv_fetch", rc)
        return int(cid[0])

    def wait(self, completion_id: int, timeout_ms: int = 5000) -> None:
        rc = self._lib.kv_wait(self._ctx, completion_id, timeout_ms)
        if rc != 0:
            raise KVCacheError("kv_wait", rc)

    def release(self, handle: int) -> None:
        rc = self._lib.kv_release(self._ctx, handle)
        if rc != 0:
            raise KVCacheError("kv_release", rc)

    # ------------------------------------------------------------------ misc

    def close(self) -> None:
        if getattr(self, "_ctx", None):
            self._lib.kv_ctx_close(self._ctx)
            self._ctx = None

    def __enter__(self) -> "KVCacheConnector":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
