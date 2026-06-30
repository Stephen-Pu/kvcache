"""Shared KV-tensor compression helpers for engine adapters (Phase KVZ-4).

These wrap the CacheGen-class codec (exposed via the C ABI in KVZ-2) plus the
``stored_bytes`` primitive (KVZ-3) into the store / retrieve dance every
adapter does, so any adapter can offer an opt-in lossy-compression mode
without duplicating the pack → compress → reserve → seal and
lookup → size → fetch → decode → slice logic.

The KV bytes are interpreted as fp32 laid out ``[n_tokens][elems_per_token]``,
where ``elems_per_token = bytes_per_token / 4``. Compression is lossy
(int8/int4 quantization); ``retrieve`` reconstructs within the per-token
quantization step and slices to the LPM-matched prefix.
"""

from __future__ import annotations

import array
from typing import Optional, Sequence


def compress_store(cx, tokens: Sequence[int], kv_bytes: bytes,
                   bytes_per_token: int, *, bits: int = 8) -> None:
    """Compress ``kv_bytes`` (fp32 KV for ``tokens``) and seal it under the
    ``tokens`` prefix. ``bytes_per_token`` must be a multiple of 4."""
    if bytes_per_token % 4 != 0:
        raise ValueError("bytes_per_token must be a multiple of 4 (fp32)")
    n_tokens = len(tokens)
    if len(kv_bytes) != n_tokens * bytes_per_token:
        raise ValueError(
            f"kv_bytes {len(kv_bytes)} != tokens*bytes_per_token "
            f"{n_tokens * bytes_per_token}")
    floats = array.array("f")
    floats.frombytes(kv_bytes)
    blob = cx.compress_kv(floats, n_tokens, bytes_per_token // 4, bits=bits)
    locator = cx.make_locator(tokens)
    rsv = cx.reserve(locator, len(blob))
    if rsv.slot_bytes < len(blob):
        raise RuntimeError(
            f"reserved slot too small: {rsv.slot_bytes} < {len(blob)}")
    cx.write_into_slot(rsv.slot_addr, blob)
    cx.publish(rsv.handle, watermark=len(blob))
    cx.seal(rsv.handle, tokens)


def compress_retrieve(cx, tokens: Sequence[int],
                      bytes_per_token: int) -> Optional[bytes]:
    """Look up ``tokens``, fetch the variable-size compressed blob (sized via
    ``stored_bytes``), decode it, and return the matched-prefix KV bytes.
    ``None`` on miss. Lossy reconstruction."""
    hit = cx.lookup(tokens)
    if hit is None:
        return None
    matched = int(hit.matched_tokens)
    stored = cx.stored_bytes(hit.handle)
    buf = bytearray(stored)
    cid = cx.fetch(hit.handle, buf)
    cx.wait(cid)
    cx.release(hit.handle)
    floats, _shape = cx.decompress_kv(bytes(buf))
    out = array.array("f", floats).tobytes()
    return out[:matched * bytes_per_token]
