"""
Shared host-lock cleanup helpers for appCataloga workers.

These utilities centralize the two host-release patterns repeated across the
workers: releasing every HOST lock owned by the current PID during shutdown,
and releasing a single claimed host at the end of one loop iteration.
"""

from __future__ import annotations

import os
from typing import Any


def release_busy_hosts_for_current_pid(
    *,
    db_factory,
    database_name: str,
    logger: Any,
) -> None:
    """
    Release all HOST rows marked as BUSY by the current worker PID.
    """
    try:
        pid = os.getpid()
        logger.event("cleanup_busy_hosts", pid=pid)
        db = db_factory(database=database_name, log=logger)
        db.host_release_by_pid(pid)
    except Exception as exc:
        logger.error(f"event=cleanup_busy_hosts_failed error={exc}")


def release_locked_host(
    db,
    host_id: int | None,
    *,
    logger: Any,
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
        logger.warning(
            f"event=host_release_failed service={service_name} "
            f"host_id={host_id} error={exc}"
        )
