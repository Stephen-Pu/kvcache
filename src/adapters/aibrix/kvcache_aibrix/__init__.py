"""aibrix adapter package. See README.md and LLD §6.1.4.

``KVCacheConnector`` and friends are re-exported from :mod:`kvcache_core`
for parity with the other adapters; ``AIBrixKVConnector`` is the
AIBrix-shaped surface (get / put / delete / exists) on top.
"""

from kvcache_core import (
    KVCacheConnector,
    KVCacheError,
    LookupResult,
    ReserveResult,
)

from .backend import AIBrixKVConnector

__all__ = [
    "AIBrixKVConnector",
    "KVCacheConnector",
    "KVCacheError",
    "LookupResult",
    "ReserveResult",
]
