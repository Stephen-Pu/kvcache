"""vLLM adapter package. See README.md and LLD §6.1.4.

The Core ABI re-exports (``KVCacheConnector`` etc.) stay for callers
that want the engine-agnostic surface. The vLLM-flavoured wrapper
``VllmKVConnector`` lives in :mod:`kvcache_vllm.connector` and matches
the method names + lifecycle of vLLM v1's ``KVConnectorBase``.
"""

from kvcache_core import (
    KVCacheConnector,
    KVCacheError,
    LookupResult,
    ReserveResult,
)
from .connector import VllmConnectorMetadata, VllmKVConnector

__all__ = [
    "KVCacheConnector",
    "KVCacheError",
    "LookupResult",
    "ReserveResult",
    "VllmConnectorMetadata",
    "VllmKVConnector",
]
