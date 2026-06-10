#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recurring host maintenance daemon.

This daemon owns periodic maintenance that is not triggered by explicit
HOST_TASK rows:

    - stale operational HOST_TASK cleanup
    - stale HOST lock cleanup
    - background oldest-first ICMP sweep for stale HOST snapshots

Queue-driven HOST_TASK processing stays in `appCataloga_host_check.py`. This
daemon intentionally does not create new HOST_TASK rows; it resolves recurring
checks directly to avoid mixing scheduler work with queue work.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

from db.dbHandlerBKP import dbHandlerBKP
from host_handler import host_connectivity, host_maintenance as maintenance_flow, host_runtime
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import errors, logging_utils
import config as k

# The maintenance daemon is intentionally high-frequency. Reusing the normal
# DB logger would emit one informational "HOST updated successfully" line for
# almost every row touched by the sweep, which quickly drowns the higher-signal
# transition and failure events. Keep DB warnings/errors, but mute routine
# success entries for this daemon only.

SERVICE_NAME = "appCataloga_host_maintenance"
log = logging_utils.log()

db_log = logging_utils.log(
    SERVICE_NAME,
    verbose={"log": False, "warning": True, "error": True},
)
process_status = {"running": True}


def _shutdown_cleanup(signal_name: str) -> None:
    """
    Release BUSY host locks during process shutdown.
    """
    host_runtime.release_busy_hosts_for_current_pid(
        db_factory=dbHandlerBKP,
        database_name=k.BKP_DATABASE_NAME,
        logger=log,
    )


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
    on_shutdown=_shutdown_cleanup,
)


def _init_db() -> dbHandlerBKP:
    """Create the operational DB handler or stop the process early."""
    try:
        return dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=db_log)
    except Exception as e:
        log.error_event(
            "db_init_failed",
            service=SERVICE_NAME,
            error=e,
        )
        sys.exit(1)


def _read_next_cycle(*, now: datetime, last_host_cleanup: datetime) -> dict:
    """Build the minimal cycle context for one loop iteration."""
    return {
        "now": now,
        "cleanup_due": (
            now - last_host_cleanup > timedelta(seconds=k.HOST_CLEANUP_INTERVAL)
        ),
        "last_host_cleanup": last_host_cleanup,
    }


def _do_maintenance(db: dbHandlerBKP, cycle: dict) -> dict:
    """
    Run one maintenance cycle and return the next loop state.

    This daemon owns recurring corrections only.
    Queue-driven host work stays in `appCataloga_host_check.py`.
    """
    next_last_cleanup = cycle["last_host_cleanup"]

    if cycle["cleanup_due"]:
        # Cleanup runs on its own cadence so stale task recovery and stale
        # lock recovery stay decoupled from the slower ICMP sweep.
        maintenance_flow.run_periodic_host_cleanup(
            db=db,
            log=log,
            task_stale_after_sec=k.HOST_TASK_OPERATIONAL_STALE_SEC,
            host_busy_timeout_sec=k.HOST_BUSY_TIMEOUT,
        )
        next_last_cleanup = cycle["now"]

    if k.HOST_CHECK_ALL_ENABLED:
        try:
            # The sweep is intentionally lightweight and oldest-first.
            maintenance_flow.run_host_check_all_batch(
                db=db,
                log=log,
                now=cycle["now"],
                process_status=process_status,
                stale_after_sec=k.HOST_CHECK_ALL_STALE_AFTER_SEC,
                batch_size=k.HOST_CHECK_ALL_BATCH_SIZE,
                icmp_timeout_sec=k.HOST_CHECK_ALL_ICMP_TIMEOUT_SEC,
                connectivity_module=host_connectivity,
            )
        except Exception as e:
            log.error_event(
                "host_check_all_batch_failed",
                component="host_maintenance_daemon",
                operation="run_host_check_all_batch",
                service=SERVICE_NAME,
                error=e,
            )

    return {"last_host_cleanup": next_last_cleanup}


def _classify_cycle_failure(exc: Exception) -> tuple[str, str]:
    """Map a cycle-level exception to the canonical error fields."""
    return "Host maintenance cycle failed", k.STAGE_MAIN


def _finalize_error(err: errors.ErrorHandler | None = None) -> None:
    """Emit the final cycle failure log."""
    if err is None:
        return
    err.log_error()


def main() -> None:
    """
    Run periodic host maintenance until shutdown is requested.

    This loop never consumes queued HOST_TASK rows.
    It only performs recurring reconciliation work.
    """
    log.service_start(SERVICE_NAME)
    last_host_cleanup = datetime.min
    db = _init_db()

    while process_status["running"]:
        err = errors.ErrorHandler(log)

        try:
            # Build one small cycle context so cadence decisions stay explicit.
            cycle = _read_next_cycle(
                now=datetime.now(),
                last_host_cleanup=last_host_cleanup,
            )

            # Run the recurring maintenance pass and keep the next cadence state.
            result = _do_maintenance(db, cycle)
            last_host_cleanup = result["last_host_cleanup"]
        except Exception as e:
            if not err.triggered:
                reason, stage = _classify_cycle_failure(e)
                err.capture(reason=reason, stage=stage, exc=e)
            _finalize_error(err)

        # This daemon is interval-driven, so every loop ends the same way.
        runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # The loop already handles normal cycle failures.
        # Reaching this block means the process itself is unstable.
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal host maintenance daemon crash",
            stage=k.STAGE_MAIN,
            exc=e,
        )
        err.log_error()
        host_runtime.release_busy_hosts_for_current_pid(
            db_factory=dbHandlerBKP,
            database_name=k.BKP_DATABASE_NAME,
            logger=log,
        )
        raise
