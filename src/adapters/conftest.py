"""Adapters-wide pytest config.

Makes the in-tree ``kvcache_core``, ``kvcache_vllm``, ``kvcache_sglang``
packages importable without installation. Each adapter's tests pick up
this conftest automatically (pytest walks upward) so per-adapter
conftest.py files are no longer needed.
"""

import pathlib
import sys

_adapters_dir = pathlib.Path(__file__).resolve().parent
for sub in ("core", "vllm", "sglang"):
    p = _adapters_dir / sub
    if p.is_dir() and str(p) not in sys.path:
        sys.path.insert(0, str(p))
