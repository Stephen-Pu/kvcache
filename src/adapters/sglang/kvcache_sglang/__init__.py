"""sglang adapter package. See README.md and LLD §6.1.4.

``KVCacheConnector`` and friends are re-exported from :mod:`kvcache_core`
so existing callers (``from kvcache_sglang import KVCacheConnector``)
keep working. ``SGLangKVBackend`` is the SGLang-shaped surface on top.
"""

from kvcache_core import (
    KVCacheConnector,
    KVCacheError,
    LookupResult,
    ReserveResult,
)

from .async_load import AsyncLoadDriver
from .backend import AsyncSGLangKVBackend, SGLangKVBackend

__all__ = [
    "AsyncLoadDriver",
    "AsyncSGLangKVBackend",
    "SGLangKVBackend",
    "KVCacheConnector",
    "KVCacheError",
    "LookupResult",
    "ReserveResult",
]
