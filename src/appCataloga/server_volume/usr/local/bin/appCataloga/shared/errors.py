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


def _canonicalize_error_reason(
    reason: Optional[str],
    exc: Optional[Exception],
    stage: Optional[str] = None,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Return `(code, canonical_reason, detail)` for persistence and grouping.

    `canonical_reason` is the stable, aggregation-friendly part of the message.
    `detail` preserves volatile specifics (paths, raw source strings, etc.)
    without forcing dashboards to treat every occurrence as a distinct error.
    """
    raw_reason = (reason or "").strip()
    exc_text = str(exc).strip() if exc is not None else ""

    if not raw_reason:
        if isinstance(exc, FileNotFoundError) and exc_text:
            return "FILE_NOT_FOUND", "File not found", exc_text
        return None, None, None

    if "GNSS unavailable sentinel" in raw_reason:
        canonical = "Invalid GPS reading: GNSS unavailable sentinel"
        detail = raw_reason if raw_reason != canonical else None
        return "GPS_GNSS_UNAVAILABLE", canonical, detail

    if raw_reason == "BIN discarded: no valid spectra after validation":
        return "NO_VALID_SPECTRA", raw_reason, None

    if raw_reason == "Spectrum list is empty":
        return "SPECTRUM_LIST_EMPTY", raw_reason, None

    if raw_reason == "Hostname missing or invalid" or raw_reason.startswith(
        "Hostname resolution failed:"
    ):
        canonical = "Hostname missing or invalid"
        detail = raw_reason if raw_reason != canonical else None
        return "HOSTNAME_MISSING", canonical, detail

    if isinstance(exc, KeyError) and raw_reason in {"'hostname'", '"hostname"'}:
        return "HOSTNAME_MISSING", "Hostname missing or invalid", raw_reason

    if isinstance(exc, FileNotFoundError):
        return "FILE_NOT_FOUND", "File not found", raw_reason

    if raw_reason.startswith('Month out of range in datetime string "'):
        return "INVALID_DATETIME_MONTH", "Invalid datetime string: month out of range", raw_reason

    if raw_reason == "buffer size must be a multiple of element size":
        return "INVALID_BUFFER_SIZE", "Invalid binary buffer size", raw_reason

    if (
        raw_reason.startswith("Error inserting site in DIM_SPECTRUM_SITE:")
        and "Error retrieving geographic codes:" in raw_reason
    ):
        return (
            "SITE_GEOGRAPHIC_CODES_NOT_FOUND",
            "Error inserting site in DIM_SPECTRUM_SITE: geographic codes not found",
            raw_reason,
        )

    # Backup and discovery workers often use stable generic reasons and rely on
    # the exception object for the actionable detail. Canonicalize these cases
    # so dashboards can aggregate by code without discarding what Paramiko/OS
    # actually reported.
    if stage == "AUTH" or isinstance(exc, paramiko.AuthenticationException):
        if exc is not None and is_auth_timeout_error(exc):
            detail = exc_text or (
                raw_reason
                if raw_reason != "SSH authentication failed"
                else None
            )
            return "SSH_AUTH_TIMEOUT", "SSH authentication timed out", detail

        detail = exc_text or (
            raw_reason
            if raw_reason not in {
                "Authentication failed (bad credentials)",
                "SSH authentication failed",
            }
            else None
        )
        return "AUTH_FAILED", "Authentication failed", detail

    if stage == "SSH" or raw_reason == "SSH negotiation failed":
        detail = exc_text or (
            raw_reason if raw_reason != "SSH negotiation failed" else None
        )
        return "SSH_NEGOTIATION_FAILED", "SSH negotiation failed", detail

    if stage == "CONNECT":
        if isinstance(exc, (socket.timeout, TimeoutError)):
            return "SSH_CONNECT_TIMEOUT", "SSH/SFTP connection timed out", exc_text or raw_reason

        detail = exc_text or (
            raw_reason if raw_reason != "SSH/SFTP initialization failed" else None
        )
        return "SFTP_INIT_FAILED", "SSH/SFTP initialization failed", detail

    if stage == "TRANSFER":
        if isinstance(exc, FileNotFoundError):
            return "FILE_NOT_FOUND", "File not found", exc_text or raw_reason

        if isinstance(exc, (socket.timeout, TimeoutError)):
            return "TRANSFER_TIMEOUT", "File transfer timed out", exc_text or raw_reason

        if isinstance(exc, PermissionError):
            detail = exc_text or (
                raw_reason if raw_reason != "File transfer failed" else None
            )
            return "TRANSFER_PERMISSION_DENIED", "Permission denied during transfer", detail

        if isinstance(exc, paramiko.SSHException):
            detail = exc_text or (
                raw_reason if raw_reason != "File transfer failed" else None
            )
            return "SSH_TRANSFER_FAILED", "SSH/SFTP transfer failed", detail

        if isinstance(exc, OSError):
            detail = exc_text or (
                raw_reason if raw_reason != "File transfer failed" else None
            )
            return "TRANSFER_IO_ERROR", "Filesystem error during transfer", detail

        if raw_reason == "File transfer failed":
            return "FILE_TRANSFER_FAILED", raw_reason, exc_text or None

    if raw_reason == "Failed to lock HOST or FILE_TASK":
        return "TASK_LOCK_FAILED", raw_reason, exc_text or None

    if raw_reason in {"Failed to lock HOST or HOST_TASK", "Failed to lock task"}:
        canonical = "Failed to lock task"
        detail = raw_reason if raw_reason != canonical else None
        if exc_text:
            detail = exc_text
        return "TASK_LOCK_FAILED", canonical, detail

    if raw_reason == "Host not found in database":
        return "HOST_NOT_FOUND", raw_reason, None

    if raw_reason == "Post-transfer update failed":
        return "FINALIZE_UPDATE_FAILED", raw_reason, exc_text or None

    if stage == "DISCOVERY" or raw_reason == "Discovery failed":
        detail = exc_text or (
            raw_reason if raw_reason != "Discovery failed" else None
        )
        return "DISCOVERY_FAILED", "Discovery failed", detail

    if stage == "BACKLOG" or raw_reason == "Backlog promotion failed":
        detail = exc_text or (
            raw_reason if raw_reason != "Backlog promotion failed" else None
        )
        return "BACKLOG_PROMOTION_FAILED", "Backlog promotion failed", detail

    if stage == "CONNECTIVITY" or raw_reason == "Connectivity test failed":
        detail = exc_text or (
            raw_reason if raw_reason != "Connectivity test failed" else None
        )
        return "CONNECTIVITY_CHECK_FAILED", "Connectivity test failed", detail

    if stage == "TRANSACTION" or raw_reason == "DB transaction failed":
        detail = exc_text or (
            raw_reason if raw_reason != "DB transaction failed" else None
        )
        return "DB_TRANSACTION_FAILED", "DB transaction failed", detail

    if stage == "UPDATE_STATS" or raw_reason == "Statistics update failed":
        detail = exc_text or (
            raw_reason if raw_reason != "Statistics update failed" else None
        )
        return "STATS_UPDATE_FAILED", "Statistics update failed", detail

    if stage == "HOST_CREATE" or raw_reason == "Failed to create/ensure HOST":
        detail = exc_text or (
            raw_reason if raw_reason != "Failed to create/ensure HOST" else None
        )
        return "HOST_CREATE_FAILED", "Failed to create/ensure HOST", detail

    if stage == "QUEUE" or raw_reason == "Failed to queue HOST_TASK":
        detail = exc_text or (
            raw_reason if raw_reason != "Failed to queue HOST_TASK" else None
        )
        return "HOST_TASK_QUEUE_FAILED", "Failed to queue HOST_TASK", detail

    if stage == "READ" and raw_reason == "Empty request":
        return "EMPTY_REQUEST", "Empty request", None

    if stage == "COMMAND" and raw_reason == "Unsupported command":
        return "UNSUPPORTED_COMMAND", "Unsupported command", None

    if stage == "PARSE" and raw_reason == "Invalid host_id":
        return "INVALID_HOST_ID", "Invalid host_id", None

    return None, raw_reason, None

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

AUTH_TIMEOUT_MESSAGE_SNIPPETS = (
    "authentication timeout",
    "auth timeout",
)

UNREACHABLE_ERRNOS = {
    errno.EHOSTUNREACH,
    errno.ENETUNREACH,
    errno.EHOSTDOWN,
    errno.ENETDOWN,
}


def is_auth_timeout_error(exc: Exception) -> bool:
    """
    Return whether a Paramiko authentication failure is timeout-driven.

    Some hosts reach the authentication phase but answer too slowly for a short
    probe. Those cases should be treated as timeout/degraded, not as explicit
    bad credentials.
    """
    if not isinstance(exc, paramiko.AuthenticationException):
        return False

    normalized = str(exc).strip().lower()
    return any(snippet in normalized for snippet in AUTH_TIMEOUT_MESSAGE_SNIPPETS)


def classify_no_valid_connections_error(exc: Exception) -> dict:
    """
    Summarize the wrapped inner failures of NoValidConnectionsError.

    Paramiko uses this exception as a container for one or more low-level
    socket/connect failures. The interesting detail lives in `exc.errors`,
    not in the wrapper message itself.
    """
    if not isinstance(exc, paramiko.ssh_exception.NoValidConnectionsError):
        raise TypeError("exc must be NoValidConnectionsError")

    nested = getattr(exc, "errors", {}) or {}
    entries = []

    for endpoint, inner_exc in nested.items():
        kind = "unknown"
        errno_value = getattr(inner_exc, "errno", None)

        if isinstance(inner_exc, (socket.timeout, TimeoutError)):
            kind = "timeout"
        elif isinstance(inner_exc, ConnectionRefusedError):
            kind = "refused"
        elif isinstance(inner_exc, (ConnectionResetError, BrokenPipeError)):
            kind = "reset"
        elif isinstance(inner_exc, OSError):
            if errno_value == errno.ECONNREFUSED:
                kind = "refused"
            elif errno_value == errno.ETIMEDOUT:
                kind = "timeout"
            elif errno_value in {errno.ECONNRESET, errno.ECONNABORTED}:
                kind = "reset"
            elif errno_value in UNREACHABLE_ERRNOS:
                kind = "unreachable"

        entries.append(
            {
                "endpoint": endpoint,
                "kind": kind,
                "errno": errno_value,
                "error_type": type(inner_exc).__name__,
                "message": str(inner_exc),
            }
        )

    kinds = {entry["kind"] for entry in entries}

    if not entries:
        summary = "unknown"
    elif len(kinds) == 1:
        summary = next(iter(kinds))
    else:
        summary = "mixed"

    return {
        "summary": summary,
        "entries": entries,
        "has_timeout": any(entry["kind"] == "timeout" for entry in entries),
        "has_refused": any(entry["kind"] == "refused" for entry in entries),
        "has_reset": any(entry["kind"] == "reset" for entry in entries),
        "has_unreachable": any(entry["kind"] == "unreachable" for entry in entries),
        "has_unknown": any(entry["kind"] == "unknown" for entry in entries),
    }


def is_transient_sftp_init_error(exc: Exception) -> bool:
    """
    Return whether an SSH/SFTP initialization error is safe to retry later.

    Authentication and clearly semantic protocol failures remain fatal. Transport
    setup failures caused by connection contention, resets, or banner timeouts
    are considered transient and may be requeued.
    """
    if isinstance(exc, paramiko.AuthenticationException):
        return is_auth_timeout_error(exc)

    if isinstance(exc, (socket.timeout, TimeoutError, EOFError, ConnectionResetError)):
        return True

    if isinstance(exc, paramiko.ssh_exception.NoValidConnectionsError):
        # The wrapper only tells us that no TCP family succeeded from this VM.
        # That is too weak to prove the host is dead, so workers should keep
        # the task retryable and let higher-level liveness mechanisms decide.
        return True

    if isinstance(exc, OSError):
        return exc.errno in TRANSIENT_SFTP_ERRNOS

    if isinstance(exc, paramiko.SSHException):
        normalized = str(exc).strip().lower()
        return any(
            snippet in normalized
            for snippet in TRANSIENT_SSH_MESSAGE_SNIPPETS
        )

    return False


def is_timeout_like_sftp_init_error(exc: Exception) -> bool:
    """
    Return whether the SSH/SFTP init failure looks timeout-driven.

    Timeout-like failures are ambiguous: they may indicate a dead SSH service,
    a stalled banner/auth phase, or temporary overload. Callers should avoid
    labeling these cases as simple "busy" contention.
    """
    if isinstance(exc, (socket.timeout, TimeoutError)):
        return True

    if is_auth_timeout_error(exc):
        return True

    if isinstance(exc, OSError):
        return exc.errno == errno.ETIMEDOUT

    if isinstance(exc, paramiko.SSHException):
        normalized = str(exc).strip().lower()
        return (
            "timed out" in normalized
            or "timeout" in normalized
            or "error reading ssh protocol banner" in normalized
        )

    if isinstance(exc, paramiko.ssh_exception.NoValidConnectionsError):
        return classify_no_valid_connections_error(exc)["has_timeout"]

    return False


def should_queue_connection_check_for_sftp_init_error(exc: Exception) -> bool:
    """
    Return whether a transient SFTP init failure is suspicious enough to ask
    host_check for an explicit connectivity confirmation.

    This is intentionally narrower than `is_transient_sftp_init_error()`. Some
    transient init failures are just SSH/SFTP contention or overload and should
    only requeue the current task, not suggest that the host is offline.
    """
    if isinstance(exc, paramiko.AuthenticationException):
        return is_auth_timeout_error(exc)

    if isinstance(exc, (socket.timeout, TimeoutError, ConnectionResetError, EOFError)):
        return True

    if isinstance(exc, paramiko.ssh_exception.NoValidConnectionsError):
        return True

    if isinstance(exc, OSError):
        return exc.errno in TRANSIENT_SFTP_ERRNOS

    return False


def get_transient_sftp_retry_detail(exc: Exception) -> str:
    """
    Return the user-facing retry detail for a transient SSH/SFTP init error.

    Timeouts are ambiguous and deserve a more explicit message than plain SSH
    contention. All other retryable init errors keep the legacy busy wording.
    """
    if is_timeout_like_sftp_init_error(exc):
        return k.SSH_TIMEOUT_RETRY_DETAIL

    return k.SFTP_BUSY_RETRY_DETAIL

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
        error_code, canonical_reason, detail = _canonicalize_error_reason(
            self.reason,
            self.exc,
            stage=self.stage,
        )

        parts = ["[ERROR]"]

        if self.stage:
            parts.append(f"[stage={self.stage}]")

        parts.append(f"[type={exc_type}]")

        if error_code:
            parts.append(f"[code={error_code}]")

        if canonical_reason:
            parts.append(canonical_reason)

        if detail:
            parts.append(f"[detail={detail}]")

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
