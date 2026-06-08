"""
Shared host-lock cleanup helpers for appCataloga workers.

These utilities centralize the two host-release patterns repeated across the
workers: releasing every HOST lock owned by the current PID during shutdown,
and releasing a single claimed host at the end of one loop iteration.
"""

from __future__ import annotations

import os
import sys
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db.dbHandlerBKP import dbHandlerBKP
    from shared.logging_utils import log as logger_type

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)
CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402


def release_busy_hosts_for_current_pid(
    *,
    db_factory,
    database_name: str,
    logger: logger_type,
) -> None:
    """
    Release all HOST rows marked as BUSY by the current worker PID.
    """
    try:
        pid = os.getpid()
        logger.event(
            "cleanup_busy_hosts",
            component="host_runtime",
            operation="release_busy_hosts_by_pid",
            pid=pid,
        )
        # Fresh connection: the regular one may have been left in a dirty
        # state if the worker was interrupted mid-transaction.
        db = db_factory(
            database=database_name,
            log=logger,
            reuse_connection=False,
        )
        db.host_release_by_pid(pid)
    except Exception as exc:
        logger.error_event(
            "cleanup_busy_hosts_failed",
            component="host_runtime",
            operation="release_busy_hosts_by_pid",
            pid=os.getpid(),
            error=exc,
        )


def release_locked_host(
    db: dbHandlerBKP,
    host_id: int | None,
    *,
    logger: logger_type,
    service_name: str,
) -> None:
    """
    Release a single HOST lock claimed by the current loop iteration.
    """
    if host_id is None:
        return

    try:
        db.host_release_safe(
            host_id=host_id,
            current_pid=os.getpid(),
        )
    except Exception as exc:
        logger.warning_event(
            "host_release_failed",
            component="host_runtime",
            operation="release_locked_host",
            service=service_name,
            host_id=host_id,
            error=exc,
        )


def run_update_statistics(
    db: dbHandlerBKP,
    task: dict,
    *,
    service_name: str,
    logger: logger_type,
) -> tuple[int, str]:
    """Refresh host statistics. Returns (status, message) for the caller to close the task."""
    started_at = time.monotonic()
    db.host_update_statistics(host_id=task["host_id"])
    elapsed_sec = round(time.monotonic() - started_at, 3)
    logger.task_phase(
        service_name,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=task["task_type"],
        phase="persist",
        elapsed_sec=elapsed_sec,
        since_start_sec=elapsed_sec,
    )
    return (k.TASK_DONE, f"Host statistics refreshed for host {task['host_id']}")
