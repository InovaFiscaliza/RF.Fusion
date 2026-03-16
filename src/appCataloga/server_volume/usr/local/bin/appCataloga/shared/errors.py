"""
Shared error and timeout helpers for appCataloga.

This module centralizes domain-level exceptions, structured error capture, and
small timeout utilities reused across workers and adapters.
"""

from __future__ import annotations
import errno
import socket
import sys
import os
import paramiko
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


TRANSIENT_SFTP_ERRNOS = {
    errno.ECONNABORTED,
    errno.ECONNREFUSED,
    errno.ECONNRESET,
    errno.ETIMEDOUT,
}

TRANSIENT_SSH_MESSAGE_SNIPPETS = (
    "error reading ssh protocol banner",
    "connection reset by peer",
    "connection timed out",
    "connection closed",
    "no existing session",
)


def is_transient_sftp_init_error(exc: Exception) -> bool:
    """
    Return whether an SSH/SFTP initialization error is safe to retry later.

    Authentication and clearly semantic protocol failures remain fatal. Transport
    setup failures caused by connection contention, resets, or banner timeouts
    are considered transient and may be requeued.
    """
    if isinstance(exc, paramiko.AuthenticationException):
        return False

    if isinstance(exc, (socket.timeout, TimeoutError, EOFError, ConnectionResetError)):
        return True

    if isinstance(exc, paramiko.ssh_exception.NoValidConnectionsError):
        nested = getattr(exc, "errors", {}) or {}
        if not nested:
            return True

        return all(
            is_transient_sftp_init_error(inner_exc)
            for inner_exc in nested.values()
            if isinstance(inner_exc, BaseException)
        )

    if isinstance(exc, OSError):
        return exc.errno in TRANSIENT_SFTP_ERRNOS

    if isinstance(exc, paramiko.SSHException):
        normalized = str(exc).strip().lower()
        return any(
            snippet in normalized
            for snippet in TRANSIENT_SSH_MESSAGE_SNIPPETS
        )

    return False

class ErrorHandler:
    """
    Centralized error tracking helper for long-running services.

    The handler stores the first meaningful failure in a workflow and exposes
    helpers to log or persist that failure later, typically in `finally`
    blocks or broad exception boundaries.

    Usage:
        err = ErrorHandler(log)
        err.set("Discovery failed", stage="DISCOVERY", exc=e)

        if err.triggered:
            err.log_error(host_id=..., task_id=...)
    """

    def __init__(self, log):
        self.logger = log
        self.reason = None
        self.stage = None
        self.exc = None
        self.context: Dict[str, Any] = {}

    def set(
        self,
        reason: str,
        stage: str = None,
        exc: Exception = None,
        **context: Any,
    ):
        """Register an error once and optionally store structured context."""
        if not self.reason:
            self.reason = reason
            self.stage = stage
            self.exc = exc
            self.context = {
                str(key): value
                for key, value in context.items()
                if value is not None
            }

    def capture(
        self,
        reason: str,
        stage: str = None,
        exc: Exception = None,
        **context: Any,
    ):
        """Alias for `set()` used at exception boundaries."""
        self.set(reason=reason, stage=stage, exc=exc, **context)

    @property
    def triggered(self) -> bool:
        return self.reason is not None

    @property
    def msg(self) -> str:
        if self.stage:
            return f"{self.stage}: {self.reason}"
        return self.reason or ""

    def log_error(self, **runtime_context: Any):
        """Emit a structured error log enriched with stored context."""
        merged_context = dict(self.context)
        for key, value in runtime_context.items():
            if value is not None:
                merged_context[str(key)] = value

        payload = {
            "stage": self.stage,
            "reason": self.reason or "Unknown error",
            "error_type": type(self.exc).__name__ if self.exc else "Unknown",
        }
        payload.update(merged_context)

        if self.exc is not None:
            payload["exception"] = repr(self.exc)

        if hasattr(self.logger, "error_event"):
            self.logger.error_event("error_handler_triggered", **payload)
            return

        parts = ["[ERROR_HANDLER]"]

        if self.stage:
            parts.append(f"[{self.stage}]")

        for key, value in merged_context.items():
            parts.append(f"[{key}={value}]")

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

        if self.context:
            parts.extend(
                [f"[{key}={value}]" for key, value in self.context.items()]
            )

        return " ".join(parts)



class TimeoutError(Exception):
    """Raised when a function exceeds the allowed timeout."""
    pass

def run_with_timeout(func, timeout: float):
    """
    Execute `func()` with a timeout using the shared executor from `constants`.

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
