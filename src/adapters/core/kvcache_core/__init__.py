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

__all__ = [
    "KVCacheConnector",
    "KVCacheError",
    "LookupResult",
    "ReserveResult",
]
