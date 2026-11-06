"""
Shared utilities for appCataloga / RF.Fusion.

This package still exposes a few compatibility re-exports while the codebase
finishes migrating to the context-oriented packages:

- ``host_handler`` for host/SSH concerns
- ``server_handler`` for process/runtime helpers
- ``appAnalise`` for appAnalise-specific adapters

Keeping these aliases here avoids breaking older imports during the
reorganization, while newer entrypoints can import the context packages
directly.
"""

from importlib import import_module
from types import ModuleType

__all__ = [
    "processing_bin",
    "host_connectivity",
    "host_runtime",
    "signal_runtime",
    "sleep",
    "timeout_utils",
    "worker_pool",
]


def __getattr__(name: str) -> ModuleType:
    """Resolve a legacy re-export only when that compatibility name is requested.

    Importing a shared utility must not load domain packages or database handlers.
    Keep the established aliases available to older callers without eager imports.

    Args:
        name (str): Compatibility module name requested from this package.

    Returns:
        ModuleType: Imported module cached under its established shared alias.

    Raises:
        AttributeError: If the name is not a supported compatibility alias.
        ImportError: If the requested module or one of its dependencies is missing.
    """
    match name:
        case "processing_bin":
            package = "appAnalise"
        case "host_connectivity" | "host_runtime":
            package = "host_handler"
        case "signal_runtime" | "sleep" | "timeout_utils" | "worker_pool":
            package = "server_handler"
        case _:
            raise AttributeError(f"Module {__name__!r} has no attribute {name!r}.")
    module = import_module(f"{package}.{name}")
    globals()[name] = module
    return module
