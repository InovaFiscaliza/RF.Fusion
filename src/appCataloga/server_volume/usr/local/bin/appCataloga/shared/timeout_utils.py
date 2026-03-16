"""
Tiny timeout helper built on a shared thread pool.

This module exists for call sites that need timeout control without importing
the broader error-handling utilities.
"""

from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError


# Shared executor reused across timeout-controlled call sites.
_TIMEOUT_EXECUTOR = ThreadPoolExecutor(
    max_workers=8,
    thread_name_prefix="timeout-worker",
)


class TimeoutError(Exception):
    """Raised when a function exceeds the allowed timeout."""
    pass


def run_with_timeout(func, timeout: float):
    """
    Execute `func()` with a timeout using the shared executor.
    """
    future = _TIMEOUT_EXECUTOR.submit(func)

    try:
        return future.result(timeout=timeout)

    except FuturesTimeoutError:
        raise TimeoutError(f"Operation timed out after {timeout} seconds")

    except Exception:
        raise
