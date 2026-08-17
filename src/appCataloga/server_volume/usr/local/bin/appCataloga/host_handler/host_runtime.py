"""Runtime support shared by workers that operate on one RF.Fusion host.

This module owns two narrow concerns that must behave the same in every
worker:

* release ``HOST`` locks owned by the current process; and
* write immediate discovery and GPS/GNSS evidence to the current host snapshot.

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
    from db.dbHandlerBKP import dbHandlerBKP
    from shared.logging_utils import log as logger_type

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)
CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402
from db.dbHandlerSummary import dbHandlerSummary  # noqa: E402


# Direct signals are written immediately, without waiting for the summary outbox.
# Most workers never emit them, so defer this connection until the first signal.
_snapshot_signal_db: dbHandlerSummary | None = None


def _get_snapshot_signal_db(logger: logger_type) -> dbHandlerSummary:
    """Return the process-local connection used for direct snapshot signals.

    The connection is reused because workers can report many files in one
    process lifetime. It is created only when a worker actually has a signal
    to persist, keeping workers that do not use these metrics lightweight.
    """
    global _snapshot_signal_db

    if _snapshot_signal_db is None:
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
    """Release every host lock still owned by this process during shutdown.

    A fresh database connection is used because the worker connection may be
    interrupted when shutdown handling begins. Failures are logged and never
    prevent the rest of the service cleanup.
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
    """Record one completed discovery run in the current operational snapshot.

    ``DT_LAST_DISCOVERY`` remains the filter watermark owned by discovery
    flow. The snapshot stores separate run evidence so an empty scan does not
    advance the next incremental search cutoff.
    """
    updates = {
        "DT_LAST_DISCOVERY_COMPLETED_AT": completed_at,
        "NU_LAST_DISCOVERY_FILE_COUNT": discovered_file_count,
        "VL_LAST_DISCOVERY_KB": float(discovered_volume_kb or 0),
    }
    if discovered_file_count > 0:
        # Keep the last useful discovery separate from empty successful scans.
        updates["DT_LAST_DISCOVERY_WITH_FILES"] = completed_at

    # Keep connection acquisition separate from the snapshot write boundary.
    snapshot_db = _get_snapshot_signal_db(logger)
    snapshot_db.update_host_current_snapshot_signals(
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
    """
    # The current-state signal must be visible without waiting for the outbox.
    snapshot_db = _get_snapshot_signal_db(logger)
    snapshot_db.update_host_current_snapshot_signals(
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

    The operational snapshot retains only the newest evidence. Detailed file
    lifecycle information remains in the processing history tables.
    """
    # The latest evidence replaces older snapshot evidence for this host.
    snapshot_db = _get_snapshot_signal_db(logger)
    snapshot_db.update_host_current_snapshot_signals(
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
