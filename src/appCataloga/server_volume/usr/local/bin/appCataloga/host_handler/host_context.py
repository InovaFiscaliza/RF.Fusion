"""
Remote discovery helpers shared by appCataloga workers.

Provides SSH/SFTP context initialization and the `iter_metadata_files` generator
that coordinates remote filesystem traversal for the discovery flow.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from typing import Any, Callable
import paramiko
from shared import errors
from shared.file_metadata import FileMetadata
from shared.filter import Filter
from shared.logging_utils import log

# ---------------------------------------------------------------------
# Ensure config import path (same rule used in legacy)
# ---------------------------------------------------------------------
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402
from .host_ssh_utils import sftpConnection

def iter_metadata_files(
    sftp_conn: sftpConnection,
    log: log,
    hostname: str,
    host_id: int,
    filter_obj: Filter,
    callBackCheckFile,
    callBackGetLastDBDate,
    *,
    batch_size: int = 1000,
) -> Iterator[list[FileMetadata]]:
    """
    High-level metadata discovery orchestrator for one remote host.

    Database-agnostic; delegates deduplication and cutoff decisions to callbacks.
    Memory usage is bounded by `batch_size`.

    Discovery modes (derived from Filter):
        - NONE / DEFAULT:  incremental using the last DB timestamp
        - FILE:            explicit file list (timestamp ignored)
        - REDISCOVERY:     full rescan (timestamp ignored)
    """
    if isinstance(filter_obj, dict):
        filter_obj = Filter(filter_obj, log=log)

    mode = (filter_obj.data.get("mode") or "").upper()
    remote_dir = filter_obj.data.get("file_path", k.DEFAULT_DATA_FOLDER)
    pattern = filter_obj._build_pattern(hostname=hostname)

    newer_than = None
    if mode != Filter.MODE_FILE:
        last_dt = callBackGetLastDBDate(host_id)
        if last_dt:
            newer_than = last_dt.strftime("%Y-%m-%d %H:%M:%S")

    if mode == Filter.MODE_REDISCOVERY:
        newer_than = None

    for batch in sftp_conn.iter_find_files_with_metadata(
        remote_path=remote_dir,
        pattern=pattern,
        newer_than=newer_than,
        batch_size=batch_size,
    ):
        if mode != Filter.MODE_FILE:
            batch = callBackCheckFile(
                host_id=host_id,
                batch=batch,
                batch_size=batch_size,
            )
        else:
            log.entry(
                f"[META] MODE_FILE active \u2014 skipping deduplication for host {host_id}"
            )

        if not batch:
            continue

        batch = filter_obj.evaluate_metadata(batch)

        if batch:
            yield batch


def init_host_context(host: dict, log) -> sftpConnection:
    """Initialize one remote SSH/SFTP session from a host row."""
    try:
        host_uid = host["HOST__NA_HOST_NAME"]
        host_addr = host["HOST__NA_HOST_ADDRESS"]
        port = int(host["HOST__NA_HOST_PORT"])
        user = host["HOST__NA_HOST_USER"]
        password = host["HOST__NA_HOST_PASSWORD"]
    except KeyError as exc:
        missing = str(exc)
        log.error(f"[INIT] Missing field in host metadata: {missing}")
        raise

    return sftpConnection(
        host_uid=host_uid,
        host_addr=host_addr,
        port=port,
        user=user,
        password=password,
        log=log,
    )
            

def capture_bootstrap_error(
    err: errors.ErrorHandler,
    exc: Exception,
    *,
    host_id: int,
    task_id: int,
) -> None:
    """Normalize SSH/SFTP bootstrap failures into shared ErrorHandler stages."""
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
) -> tuple[sftpConnection | None, bool]:
    """
    Initialize one remote host context or delegate retry/error handling.

    Returns (sftp_conn, preserve_host_busy_cooldown).
    On success returns the live connection and False.
    On transient failure calls transient_retry_handler and returns (None, preserve).
    On fatal failure captures AUTH/SSH/CONNECT in err and returns (None, False).
    """
    retry_handler_kwargs = retry_handler_kwargs or {}

    try:
        sftp_conn = init_host_context(task, log)
        return sftp_conn, False
    except Exception as exc:
        if errors.is_transient_sftp_init_error(exc):
            try:
                preserve_host_busy_cooldown = transient_retry_handler(
                    exc=exc,
                    **retry_handler_kwargs,
                )
                return None, preserve_host_busy_cooldown
            except Exception as retry_exc:
                err.capture(
                    retry_failure_reason,
                    stage="RETRY",
                    exc=retry_exc,
                    host_id=host_id,
                    task_id=task_id,
                )
                return None, False

        capture_bootstrap_error(err, exc, host_id=host_id, task_id=task_id)
        return None, False
