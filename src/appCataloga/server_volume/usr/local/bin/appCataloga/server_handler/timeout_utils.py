"""
Tiny timeout helper built on a shared thread pool.

This module remains part of the `server_handler` package as a compatibility
surface for older imports, even though current runtime code rarely calls it
directly.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Any, Callable


_TIMEOUT_EXECUTOR = ThreadPoolExecutor(
    max_workers=8,
    thread_name_prefix="timeout-worker",
)


class TimeoutError(Exception):
    """Raised when a function exceeds the allowed timeout budget."""


def run_with_timeout(func: Callable[[], Any], timeout: float) -> Any:
    """Execute `func()` with a timeout using the shared executor."""
    future = _TIMEOUT_EXECUTOR.submit(func)

    try:
        return future.result(timeout=timeout)
    except FuturesTimeoutError as exc:
        raise TimeoutError(
            f"Operation timed out after {timeout} seconds"
        ) from exc

