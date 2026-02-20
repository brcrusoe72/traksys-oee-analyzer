"""Compatibility shim for the legacy parser module name."""

from warnings import warn
import parse_mes as _parse_mes
from parse_mes import *  # noqa: F401,F403

warn(
    f"Legacy module {__name__!r} is deprecated; import from 'parse_mes' instead.",
    DeprecationWarning,
    stacklevel=2,
)

if hasattr(_parse_mes, "__all__"):
    __all__ = list(_parse_mes.__all__)
else:
    __all__ = [name for name in dir(_parse_mes) if not name.startswith("_")]
