"""vLLM adapter package. See README.md and LLD §6.1.4.

The classes here are re-exports from :mod:`kvcache_core` — every adapter
shares the same thin Python wrapper around the C ABI. vLLM-flavoured
extensions (e.g. a future ``VllmKVConnector`` that matches vLLM's
``KVConnectorBase`` shape) will land in this package as siblings.
"""

from kvcache_core import (
    KVCacheConnector,
    KVCacheError,
    LookupResult,
    ReserveResult,
)

__all__ = ["KVCacheConnector", "KVCacheError", "LookupResult", "ReserveResult"]
