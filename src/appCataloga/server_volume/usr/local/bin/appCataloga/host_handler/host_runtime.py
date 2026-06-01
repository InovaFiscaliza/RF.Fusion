"""
Shared host-lock cleanup helpers for appCataloga workers.

These utilities centralize the two host-release patterns repeated across the
workers: releasing every HOST lock owned by the current PID during shutdown,
and releasing a single claimed host at the end of one loop iteration.
"""

from __future__ import annotations

import os
import sys
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from db.dbHandlerBKP import dbHandlerBKP

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
    logger: Any,
) -> None:
    """
    Release all HOST rows marked as BUSY by the current worker PID.
    """
    try:
        pid = os.getpid()
        logger.event("cleanup_busy_hosts", pid=pid)
        db = db_factory(
            database=database_name,
            log=logger,
            reuse_connection=False,
        )
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


def update_host_statistics(
    db: dbHandlerBKP,
    task: dict,
    *,
    logger: Any,
) -> None:
    """Refresh host statistics and mark the task done. Raises on DB failure."""
    db.host_update_statistics(host_id=task["host_id"])
    db.host_task_update(
        task_id=task["task_id"],
        NU_STATUS=k.TASK_DONE,
        NU_PID=k.HOST_UNLOCKED_PID,
        DT_HOST_TASK=task["now"],
        NA_MESSAGE=f"Host statistics refreshed for host {task['host_id']}",
    )
    logger.event(
        "task_done",
        host_id=task["host_id"],
        task_id=task["task_id"],
        status="statistics_refreshed",
    )
