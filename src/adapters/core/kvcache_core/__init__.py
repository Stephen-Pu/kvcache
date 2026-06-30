"""kvcache_core — the engine-agnostic Python surface over libkvcache.so.

Engine adapter packages (``kvcache_vllm``, ``kvcache_sglang``, ...) all
build on the classes re-exported here. See connector.py for usage.
"""

from .connector import (
    KVCacheConnector,
    KVCacheError,
    LookupResult,
    ReserveResult,
)
from .kv_compress import compress_retrieve, compress_store

__all__ = [
    "KVCacheConnector",
    "KVCacheError",
    "LookupResult",
    "ReserveResult",
    "compress_retrieve",
    "compress_store",
]
