from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError


# Shared executor – limited number of worker threads
_TIMEOUT_EXECUTOR = ThreadPoolExecutor(
    max_workers=8,
    thread_name_prefix="timeout-worker",
)


class TimeoutError(Exception):
    """Raised when a function exceeds the allowed timeout."""
    pass


def run_with_timeout(func, timeout: float):
    """
    Execute `func()` with a timeout using a shared ThreadPoolExecutor.

    Benefits:
        - No thread leaking (all threads reused)
        - Real timeout control
        - Exceptions pass-through
    """
    future = _TIMEOUT_EXECUTOR.submit(func)

    try:
        return future.result(timeout=timeout)

    except FuturesTimeoutError:
        raise TimeoutError(f"Operation timed out after {timeout} seconds")

    except Exception:
        raise
