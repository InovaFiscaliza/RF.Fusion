from __future__ import annotations
import sys
import os
from . import constants
from typing import Any, Dict, List, Tuple, Optional, Union
from concurrent.futures import TimeoutError as FuturesTimeoutError



# ---------------------------------------------------------------------
# Config import path (as in original code). We keep the behavior so the
# module remains drop-in compatible with existing deployments.
# ---------------------------------------------------------------------
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402  (must be available at runtime)

class BinValidationError(ValueError):
    """
    Raised when BIN semantic validation fails.
    Domain-level error (fatal validation).
    """
    pass


class ExternalServiceTransientError(Exception):
    """
    Raised when an external dependency fails transiently.

    These errors should not be interpreted as proof that the source
    file is invalid, because a retry may succeed once the dependency
    becomes healthy again.
    """
    pass

class ErrorHandler:
    """
    Centralized error tracking helper for microservices.

    Stores error state across multiple stages and provides utility methods for
    checking, logging, and retrieving structured error messages.

    Usage:
        err = ErrorHandler(log)
        err.set("Discovery failed", stage="DISCOVERY", exc=e)

        if err.triggered:
            err.log_error(host_id=..., task_id=...)
    """

    def __init__(self, log):
        self.logger = log          # <-- RENOMEADO
        self.reason = None
        self.stage = None
        self.exc = None

    def set(self, reason: str, stage: str = None, exc: Exception = None):
        """Register an error once."""
        if not self.reason:
            self.reason = reason
            self.stage = stage
            self.exc = exc

    @property
    def triggered(self) -> bool:
        return self.reason is not None

    @property
    def msg(self) -> str:
        if self.stage:
            return f"{self.stage}: {self.reason}"
        return self.reason or ""

    def log_error(self, host_id=None, task_id=None):
        """Unified logging format for errors."""
        parts = ["[ERROR_HANDLER]"]

        if self.stage:
            parts.append(f"[{self.stage}]")

        if host_id is not None:
            parts.append(f"[HOST={host_id}]")

        if task_id is not None:
            parts.append(f"[TASK={task_id}]")

        parts.append(self.reason or "Unknown error")

        if self.exc:
            parts.append(f"Exception: {repr(self.exc)}")

        self.logger.error(" ".join(parts))
        
    def format_error(self) -> str:
        """
        Return a compact, structured error string
        suitable for persistence (DB, history, audit).
        """
        if not self.triggered:
            return ""

        exc_type = type(self.exc).__name__ if self.exc else "Unknown"

        parts = ["[ERROR]"]

        if self.stage:
            parts.append(f"[stage={self.stage}]")

        parts.append(f"[type={exc_type}]")

        if self.reason:
            parts.append(self.reason)

        return " ".join(parts)



class TimeoutError(Exception):
    """Raised when a function exceeds the allowed timeout."""
    pass

def run_with_timeout(func, timeout: float):
    """
    Execute `func()` with a timeout using a global ThreadPoolExecutor.

    Benefits:
        - No thread leaking (all threads reused)
        - Real timeout control
        - Exceptions pass-through
        - Same signature you were already using

    Raises:
        TimeoutError
        Exception forwarded from func()
    """
    future = constants._TIMEOUT_EXECUTOR.submit(func)

    try:
        return future.result(timeout=timeout)

    except FuturesTimeoutError:
        raise TimeoutError(f"Operation timed out after {timeout} seconds")

    except Exception as e:
        raise e
