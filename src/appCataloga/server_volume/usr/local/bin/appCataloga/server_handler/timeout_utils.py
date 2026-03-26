"""
Tiny timeout helper built on a shared thread pool.

This module exists for call sites that need timeout control without importing
the broader error-handling utilities.

Reading guide:
    The helper is intentionally narrow: submit one callable to a shared
    executor, wait up to `timeout`, then normalize timeout expiry into this
    module's own `TimeoutError`.
"""

from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError


# A small shared executor keeps timeout wrappers cheap to call and avoids each
# module creating its own short-lived thread pool.
_TIMEOUT_EXECUTOR = ThreadPoolExecutor(
    max_workers=8,
    thread_name_prefix="timeout-worker",
)


class TimeoutError(Exception):
    """
    Raised when a function exceeds the allowed timeout budget.

    The name intentionally mirrors the concept rather than the concrete
    `concurrent.futures` exception so callers do not need to care which
    timeout mechanism lives underneath.
    """
    pass


def run_with_timeout(func, timeout: float):
    """
    Execute `func()` with a timeout using the shared executor.

    This helper is best for small isolated operations where the caller wants a
    simple "finished in time or not" boundary without dragging executor code
    into the worker itself.
    """
    future = _TIMEOUT_EXECUTOR.submit(func)

    try:
        return future.result(timeout=timeout)

    except FuturesTimeoutError:
        # Normalize the implementation-specific timeout into the public error
        # this module exposes to the rest of the codebase.
        raise TimeoutError(f"Operation timed out after {timeout} seconds")

    except Exception:
        raise
