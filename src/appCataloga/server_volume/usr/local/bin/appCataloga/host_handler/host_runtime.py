"""Runtime support shared by workers that operate on one RF.Fusion host.

This module owns two narrow concerns that must behave the same in every
worker:

* release ``HOST`` locks owned by the current process; and
* write immediate discovery and GPS/GNSS evidence to the authoritative host row.

It does not decide task status, run connectivity probes, or rebuild summary
tables. Queue workers keep those responsibilities while the summary worker
continues to own aggregate refreshes.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shared.logging_utils import log as logger_type

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)
CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

from db import constants as db_constants
import config as k  # noqa: E402
from db.dbHandlerBKP import dbHandlerBKP  # noqa: E402


# Direct signals are written immediately, without waiting for the summary outbox.
# Most workers never emit them, so defer this connection until the first signal.
_signal_db: dbHandlerBKP | None = None


def _get_signal_db(logger: logger_type) -> dbHandlerBKP:
    """Return the process-local connection used for direct operational signals.

    The connection is reused because workers can report many files in one
    process lifetime. It is created only when a worker actually has a signal
    to persist, keeping workers that do not use these metrics lightweight.

    Args:
        logger (logger_type): Caller application logger.
    Returns:
        dbHandlerBKP: Lazily configured, process-owned operational handler.
    """
    global _signal_db

    if _signal_db is None:
        _signal_db = dbHandlerBKP(
            database=k.BKP_DATABASE_NAME,
            conninfo=k.PG_CONNINFO,
            log=logger,
            reuse_connection=True,
        )
    return _signal_db


def release_busy_hosts_for_current_pid(
    *,
    db_factory: type[dbHandlerBKP],
    database_name: str,
    logger: logger_type,
) -> None:
    """Release every host lock still owned by this process during shutdown.

    A fresh database connection is used because the worker connection may be
    interrupted when shutdown handling begins. Failures are logged and never
    prevent the rest of the service cleanup.

    Args:
        db_factory (type[dbHandlerBKP]): Operational handler constructor.
        database_name (str): Case-sensitive operational schema name.
        logger (logger_type): Application logger receiving cleanup events.
    Returns:
        None. The dedicated session is closed on success and failure.
    """
    try:
        pid = os.getpid()
        logger.event(
            "cleanup_busy_hosts",
            component="host_runtime",
            operation="release_busy_hosts_by_pid",
            pid=pid,
        )
        # Shutdown has no safe reference to the worker-local DB handler.
        # Its connection may also be interrupted before locks are released.
        db = db_factory(
            database=database_name,
            log=logger,
            conninfo=k.PG_CONNINFO,
            reuse_connection=False,
        )
        try:
            db.host_release_by_pid(pid)
        finally:
            db.close()
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
    """Release one claimed host after the current work attempt finishes.

    ``host_release_safe`` verifies the PID ownership. A worker therefore
    cannot clear a lock already reclaimed by another process.
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
    """Record one completed discovery run in the authoritative operational host.

    ``DT_LAST_DISCOVERY`` remains the filter watermark owned by discovery
    flow. The host stores separate run evidence so an empty scan does not
    advance the next incremental search cutoff.

    Args:
        host_id (int): Existing operational host identity.
        completed_at (datetime): UTC scan completion.
        discovered_file_count (int): Deduplicated persisted file count.
        discovered_volume_kb (float): Deduplicated source volume in KB.
        logger (logger_type): Caller application logger.
    Returns:
        None.
    """
    updates = {
        "DT_LAST_DISCOVERY_COMPLETED_AT": completed_at,
        "NU_LAST_DISCOVERY_FILE_COUNT": discovered_file_count,
        "VL_LAST_DISCOVERY_KB": float(discovered_volume_kb or 0),
    }
    if discovered_file_count > 0:
        # Keep the last useful discovery separate from empty successful scans.
        updates["DT_LAST_DISCOVERY_WITH_FILES"] = completed_at

    # Keep connection acquisition separate from the host write boundary.
    signal_db = _get_signal_db(logger)
    signal_db.host_update_signals(
        host_id=host_id,
        values=updates,
    )


def record_gps_gnss_available(
    host_id: int,
    *,
    evaluated_at: datetime,
    logger: logger_type,
) -> None:
    """Clear the active GPS/GNSS warning after valid file evidence arrives.

    The evaluation timestamp is updated even when the state was already clear,
    so the interface can distinguish a recent validation from stale data.

    Args:
        host_id (int): Existing operational host identity.
        evaluated_at (datetime): UTC valid evaluation timestamp.
        logger (logger_type): Caller application logger.
    Returns:
        None.
    """
    # The current-state signal must be visible without waiting for the outbox.
    signal_db = _get_signal_db(logger)
    signal_db.host_update_signals(
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
    """Persist the latest GPS/GNSS failure and the file that exposed it.

    The operational host retains only the newest evidence. Detailed file
    lifecycle information remains in the processing history tables.

    Args:
        host_id (int): Existing operational host identity.
        evaluated_at (datetime): UTC valid evaluation timestamp.
        description (str): Failure evidence.
        host_file_name (str): Source artifact that exposed the failure.
        logger (logger_type): Caller application logger.
    Returns:
        None.
    """
    # The latest evidence replaces older operational evidence for this host.
    signal_db = _get_signal_db(logger)
    signal_db.host_update_signals(
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
    """Request a host summary refresh for the legacy statistics task.

    This function only publishes the invalidation scope. The summary worker
    performs the rebuild asynchronously, so a host worker never performs a
    potentially expensive aggregation in its own loop.
    """
    started_at = time.monotonic()
    db.host_request_summary_refresh(
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
    return (db_constants['tasks']['states']['done'], f"Host summary refresh requested for host {task['host_id']}")
