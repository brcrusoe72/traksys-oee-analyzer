"""
Compatibility shim for legacy module name.

The parser implementation now lives in `parse_mes.py`.
This module re-exports the same symbols to avoid breaking existing imports.
"""

from parse_mes import *  # noqa: F401,F403
