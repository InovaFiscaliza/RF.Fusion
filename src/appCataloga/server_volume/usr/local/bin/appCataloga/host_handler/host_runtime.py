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
from datetime import datetime
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


_snapshot_signal_db: dbHandlerSummary | None = None


def _get_snapshot_signal_db(logger: logger_type) -> dbHandlerSummary:
    """Return the process-local summary connection used for GPS signals."""
    global _snapshot_signal_db

    if _snapshot_signal_db is None:
        from db.dbHandlerSummary import dbHandlerSummary

        _snapshot_signal_db = dbHandlerSummary(
            database=k.SUMMARY_DATABASE_NAME,
            log=logger,
            reuse_connection=True,
        )
    return _snapshot_signal_db


def release_busy_hosts_for_current_pid(
    *,
    db_factory,
    database_name: str,
    logger: logger_type,
) -> None:
    """
    Release all HOST rows still owned by the current worker PID.
    """
    try:
        pid = os.getpid()
        logger.event(
            "cleanup_busy_hosts",
            component="host_runtime",
            operation="release_busy_hosts_by_pid",
            pid=pid,
        )
        # Use a fresh connection in case the regular one was interrupted.
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
    Release one HOST lock claimed by the current loop iteration.
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


def record_discovery_outcome(
    host_id: int,
    *,
    completed_at: datetime,
    discovered_file_count: int,
    discovered_volume_kb: float,
    logger: logger_type,
) -> None:
    """Persist the result of one completed discovery run.

    ``DT_LAST_DISCOVERY`` remains the discovery filter watermark. The run
    result uses separate fields so an empty scan never changes that cutoff.
    """
    updates = {
        "DT_LAST_DISCOVERY_COMPLETED_AT": completed_at,
        "NU_LAST_DISCOVERY_FILE_COUNT": discovered_file_count,
        "VL_LAST_DISCOVERY_KB": float(discovered_volume_kb or 0),
    }
    if discovered_file_count > 0:
        updates["DT_LAST_DISCOVERY_WITH_FILES"] = completed_at

    _get_snapshot_signal_db(logger).update_host_current_snapshot_signals(
        host_id=host_id,
        values=updates,
    )


def record_gps_gnss_available(
    host_id: int,
    *,
    evaluated_at: datetime,
    logger: logger_type,
) -> None:
    """Clear the current GPS/GNSS signal after a valid processed file."""
    _get_snapshot_signal_db(logger).update_host_current_snapshot_signals(
        host_id=host_id,
        values={
            "IS_GPS_GNSS_UNAVAILABLE": False,
            "DT_LAST_GPS_GNSS_EVALUATED_AT": evaluated_at,
        },
    )


def record_gps_gnss_unavailable(
    host_id: int,
    *,
    evaluated_at: datetime,
    description: str,
    host_file_name: str,
    logger: logger_type,
) -> None:
    """Persist GPS/GNSS unavailability and retain its latest file evidence."""
    _get_snapshot_signal_db(logger).update_host_current_snapshot_signals(
        host_id=host_id,
        values={
            "IS_GPS_GNSS_UNAVAILABLE": True,
            "DT_LAST_GPS_GNSS_EVALUATED_AT": evaluated_at,
            "DT_LAST_GPS_GNSS_UNAVAILABLE_AT": evaluated_at,
            "NA_LAST_GPS_GNSS_UNAVAILABLE_DESCRIPTION": description,
            "NA_LAST_GPS_GNSS_UNAVAILABLE_HOST_FILE_NAME": host_file_name,
        },
    )


def run_update_statistics(
    db: dbHandlerBKP,
    task: dict,
    *,
    service_name: str,
    logger: logger_type,
) -> tuple[int, str]:
    """Refresh the host summary scope and return the final task result tuple."""
    started_at = time.monotonic()
    db.request_host_summary_refresh(
        host_id=task["host_id"],
        reason="legacy_host_statistics_task",
    )
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
    return (k.TASK_DONE, f"Host summary refresh requested for host {task['host_id']}")
