"""
Shared SSH/SFTP bootstrap flow used by host-dependent workers.

Discovery and backup both follow the same high-level contract when opening a
remote context:
    1. try to initialize SSH/SFTP once
    2. if the failure looks transient, requeue the current task via a
       worker-specific callback
    3. otherwise capture a stable AUTH/SSH/CONNECT error for finalization

The worker remains the owner of task persistence details. This module only
centralizes the common decision tree around bootstrap success vs retry vs
fatal failure.
"""

from __future__ import annotations

from typing import Any, Callable

import paramiko

from shared import errors

from . import host_context


def capture_bootstrap_error(
    err: errors.ErrorHandler,
    exc: Exception,
    *,
    host_id: int,
    task_id: int,
) -> None:
    """
    Normalize SSH/SFTP bootstrap failures into shared ErrorHandler stages.
    """
    if isinstance(exc, paramiko.AuthenticationException):
        err.capture(
            "SSH authentication failed",
            stage="AUTH",
            exc=exc,
            host_id=host_id,
            task_id=task_id,
        )
        return

    if isinstance(exc, paramiko.SSHException):
        err.capture(
            "SSH negotiation failed",
            stage="SSH",
            exc=exc,
            host_id=host_id,
            task_id=task_id,
        )
        return

    err.capture(
        "SSH/SFTP initialization failed",
        stage="CONNECT",
        exc=exc,
        host_id=host_id,
        task_id=task_id,
    )


def init_host_context_with_retry(
    *,
    task: dict,
    log,
    err: errors.ErrorHandler,
    host_id: int,
    task_id: int,
    transient_retry_handler: Callable[..., bool],
    retry_handler_kwargs: dict[str, Any] | None = None,
    retry_failure_reason: str,
) -> tuple[Any | None, Any | None, bool]:
    """
    Initialize one remote host context or delegate retry/error handling.

    Returns:
        tuple:
            `(sftp_conn, daemon, preserve_host_busy_cooldown)`

    Contract:
        - On success, returns the live `(sftp_conn, daemon, False)`
        - On transient failure, calls `transient_retry_handler(...)` and
          returns `(None, None, preserve_host_busy_cooldown)`
        - On fatal bootstrap failure, captures AUTH/SSH/CONNECT in `err`
          and returns `(None, None, False)`

    Callers can therefore keep their loop linear:
        sftp, daemon, preserve = init_host_context_with_retry(...)
        if sftp is None:
            continue
    """
    retry_handler_kwargs = retry_handler_kwargs or {}

    try:
        sftp_conn, daemon = host_context.init_host_context(task, log)
        return sftp_conn, daemon, False
    except Exception as exc:
        if errors.is_transient_sftp_init_error(exc):
            try:
                preserve_host_busy_cooldown = transient_retry_handler(
                    exc=exc,
                    **retry_handler_kwargs,
                )
                return None, None, preserve_host_busy_cooldown
            except Exception as retry_exc:
                err.capture(
                    retry_failure_reason,
                    stage="RETRY",
                    exc=retry_exc,
                    host_id=host_id,
                    task_id=task_id,
                )
                return None, None, False

        capture_bootstrap_error(
            err,
            exc,
            host_id=host_id,
            task_id=task_id,
        )
        return None, None, False
