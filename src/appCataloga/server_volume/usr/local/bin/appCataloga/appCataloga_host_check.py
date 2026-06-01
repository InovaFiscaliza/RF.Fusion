#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Queued HOST_TASK worker for connectivity checks and statistics refresh.

Consumes HOST_TASK rows in priority order:
  1. CHECK            — confirm connectivity and queue a PROCESSING row on success
  2. CHECK_CONNECTION — reconcile connectivity without queuing follow-up work
  3. UPDATE_STATISTICS — refresh host summary metrics

One constraint: this worker never locks HOST.IS_BUSY, so it never blocks
data-plane workers from running on the same host.
"""

import sys
import os
import time
from datetime import datetime

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

from db.dbHandlerBKP import dbHandlerBKP
from host_handler import host_connectivity, host_runtime
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import errors, logging_utils
import config as k


# --- globals ---

SERVICE_NAME = "appCataloga_host_check"
log = logging_utils.log()
process_status = {"running": True}

# Priority order for reading the HOST_TASK queue.
# CHECK must come before CHECK_CONNECTION so fresh bootstrap work
# is never delayed by lighter reconciliation tasks.
HOST_TASK_PRIORITY = (
    k.HOST_TASK_CHECK_TYPE,
    k.HOST_TASK_CHECK_CONNECTION_TYPE,
    k.HOST_TASK_UPDATE_STATISTICS_TYPE,
)


# --- signal handling ---

def _shutdown_cleanup(signal_name: str) -> None:
    """Release BUSY host locks when the process shuts down."""
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


# --- loop helpers ---

def _read_next_task(db: dbHandlerBKP) -> dict | None:
    """Return the next queued HOST_TASK by priority, or None when the queue is empty."""
    for task_type in HOST_TASK_PRIORITY:
        task = db.host_task_read(task_status=k.TASK_PENDING, task_type=task_type)
        if task:
            return task
    return None


def _claim_task(db: dbHandlerBKP, task: dict) -> bool:
    """
    Atomically flip the task from PENDING to RUNNING.
    Returns False if another worker claimed it first (race lost).
    """
    result = db.host_task_update(
        task_id=task["task_id"],
        expected_status=k.TASK_PENDING,
        NU_STATUS=k.TASK_RUNNING,
        NU_PID=os.getpid(),
        DT_HOST_TASK=datetime.now(),
        NA_MESSAGE="Host check task running",
    )
    if result["rows_affected"] == 1:
        return True
    log.warning(
        f"event=claim_race_lost host_id={task['host_id']} task_id={task['task_id']}"
    )
    return False


# --- work ---

def _do_work(db: dbHandlerBKP, task: dict) -> dict:
    """Dispatch the claimed task by type. Raises on any failure."""
    start = time.monotonic()

    match task["task_type"]:
        case k.HOST_TASK_UPDATE_STATISTICS_TYPE:
            host_runtime.update_host_statistics(db, task, logger=log)
        case k.HOST_TASK_CHECK_TYPE:
            host_connectivity.run_check(
                db, task, logger=log, promote_to_processing=True
            )
        case k.HOST_TASK_CHECK_CONNECTION_TYPE:
            host_connectivity.run_check(
                db, task, logger=log, promote_to_processing=False
            )
        case _:
            raise ValueError(f"Unsupported HOST_TASK type: {task['task_type']}")

    return {"elapsed_sec": time.monotonic() - start}


# --- finalization ---

def _finalize_success(db: dbHandlerBKP, task: dict, result: dict) -> None:
    """Log task completion. Domain functions already wrote the final queue state."""
    log.event(
        "work_completed",
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=task["task_type"],
        elapsed_sec=round(result["elapsed_sec"], 3),
    )


def _finalize_error(
    db: dbHandlerBKP, task: dict | None, err: errors.ErrorHandler
) -> None:
    """Write ERROR to the queue and log the failure. Safe when task is None."""
    if task is None:
        err.log_error()
        return

    err.log_error(host_id=task["host_id"], task_id=task["task_id"])

    try:
        db.host_task_update(
            task_id=task["task_id"],
            NU_STATUS=k.TASK_ERROR,
            NU_PID=k.HOST_UNLOCKED_PID,
            NA_MESSAGE=f"Host Check Error | {err.format_persisted_error()}",
            DT_HOST_TASK=datetime.now(),
        )
    except Exception as e2:
        log.error(f"event=finalize_error_failed task_id={task['task_id']} error={e2}")


# --- main ---

def _init_db() -> dbHandlerBKP:
    """Connect to the operational database. Exits the process on failure."""
    try:
        return dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"event=db_init_failed service={SERVICE_NAME} error={e}")
        sys.exit(1)


def main() -> None:
    log.service_start(SERVICE_NAME)
    db = _init_db()

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        task = None

        try:
            # --- read ---
            task_row = _read_next_task(db)
            if task_row is None:
                runtime_sleep.random_jitter_sleep()
                continue

            task = {
                "host_id"               : task_row["HOST__ID_HOST"],
                "task_id"               : task_row["HOST_TASK__ID_HOST_TASK"],
                "task_type"             : task_row["HOST_TASK__NU_TYPE"],
                "addr"                  : task_row["HOST__NA_HOST_ADDRESS"],
                "port"                  : task_row["HOST__NA_HOST_PORT"],
                "user"                  : task_row["HOST__NA_HOST_USER"],
                "password"              : task_row["HOST__NA_HOST_PASSWORD"],
                "was_offline"           : bool(task_row.get("HOST__IS_OFFLINE")),
                "host_check_error_count": int(task_row.get("HOST__NU_HOST_CHECK_ERROR") or 0),
                "host_filter"           : task_row.get("host_filter") or dict(k.NONE_FILTER),
                "now"                   : datetime.now(),
            }

            # --- claim ---
            if not _claim_task(db, task):
                # Another worker got this task first. Not an error, just skip.
                runtime_sleep.random_jitter_sleep()
                continue

            # --- work ---
            result = _do_work(db, task)

            # --- finalize ---
            _finalize_success(db, task, result)

        except Exception as e:
            if not err.triggered:
                err.capture(
                    reason="Host check task failed",
                    stage=k.STAGE_MAIN,
                    exc=e,
                    host_id=task["host_id"] if task else None,
                    task_id=task["task_id"] if task else None,
                )
            _finalize_error(db, task, err)

        runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME)


# --- entrypoint ---

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # True last-resort crash. The main loop handles per-iteration failures.
        # If we reach here, the process itself is no longer trustworthy.
        err = errors.ErrorHandler(log)
        err.capture(reason="Fatal host check worker crash", stage=k.STAGE_MAIN, exc=e)
        err.log_error()
        host_runtime.release_busy_hosts_for_current_pid(
            db_factory=dbHandlerBKP,
            database_name=k.BKP_DATABASE_NAME,
            logger=log,
        )
        raise

