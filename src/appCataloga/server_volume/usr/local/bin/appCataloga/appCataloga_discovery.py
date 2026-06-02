#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Discovery worker: scans remote hosts for candidate files.

Consumes HOST_TASK rows of type PROCESSING and locks HOST.IS_BUSY during each scan.
One SFTP session per iteration; discovered metadata is persisted in bounded batches.
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime

from bootstrap_paths import bootstrap_app_paths

PROJECT_ROOT = bootstrap_app_paths(__file__)

from db.dbHandlerBKP import dbHandlerBKP
from host_handler import host_context, host_runtime
from host_handler.host_ssh_utils import sftpConnection
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import errors, logging_utils, tools
from shared.filter import Filter
import config as k


SERVICE_NAME = "appCataloga_discovery"
log = logging_utils.log()
process_status = {"running": True}


def _shutdown_cleanup(signal_name: str) -> None:
    """Release BUSY host locks during process shutdown."""
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
    """Return the next PROCESSING host task as a normalized task dict, or None when empty."""
    task_row = db.host_task_read(
        task_type=k.HOST_TASK_PROCESSING_TYPE,
        task_status=k.TASK_PENDING,
        check_host_busy=True,
        check_host_offline=True,
        lock_host=True,
    )
    if task_row is None:
        return None
    return {
        **task_row,
        "host_id"    : task_row["HOST__ID_HOST"],
        "task_id"    : task_row["HOST_TASK__ID_HOST_TASK"],
        "hostname"   : task_row["HOST__NA_HOST_NAME"],
        "host_filter": task_row.get("host_filter") or dict(k.NONE_FILTER),
        "now"        : datetime.now(),
    }


def _claim_task(db: dbHandlerBKP, task: dict) -> bool:
    """
    Atomically flip the HOST_TASK from PENDING to RUNNING.

    Discovery claims the task before opening SSH/SFTP so ownership is deterministic
    and avoids a race with sibling workers for the same host.
    Returns False if another worker claimed this row first.
    """
    result = db.host_task_update(
        task_id=task["task_id"],
        expected_status=k.TASK_PENDING,
        NU_STATUS=k.TASK_RUNNING,
        NU_PID=os.getpid(),
        DT_HOST_TASK=datetime.now(),
        NA_MESSAGE="Discovery task running",
    )
    if result["rows_affected"] == 1:
        log.event(
            "discovery_started",
            host_id=task["host_id"],
            task_id=task["task_id"],
            host=task["hostname"],
        )
        return True
    log.event("host_task_claim_race", host_id=task["host_id"], task_id=task["task_id"])
    return False


# --- work ---

def _stream_discovery_batches(
    db: dbHandlerBKP,
    sftp: sftpConnection,
    task: dict,
) -> int:
    """
    Persist discovered metadata in bounded batches and return rows written.

    Discovery writes each batch twice on purpose:
        - FILE_TASK stores the mutable pipeline queue
        - FILE_TASK_HISTORY stores the immutable audit trail

    The DB callbacks handle deduplication and last-seen cutoffs, so this loop
    can stay focused on streaming batches instead of keeping the full remote
    inventory in memory.
    """
    host_filter = Filter(task["host_filter"], log=log)
    processed = 0

    for batch in host_context.iter_metadata_files(
        sftp,
        log,
        host_id=task["host_id"],
        hostname=task["hostname"],
        filter_obj=host_filter,
        callBackCheckFile=db.filter_existing_file_batch,
        callBackGetLastDBDate=db.get_last_discovery,
        batch_size=k.DISCOVERY_BATCH_SIZE,
    ):
        db.file_task_create(
            host_id=task["host_id"],
            file_metadata=batch,
            task_type=k.FILE_TASK_DISCOVERY,
            task_status=k.TASK_DONE,
        )
        db.file_history_create(
            host_id=task["host_id"],
            file_metadata=batch,
            task_type=k.FILE_TASK_DISCOVERY,
            task_status=k.TASK_DONE,
        )
        processed += len(batch)
        log.event(
            "discovery_progress",
            host_id=task["host_id"],
            processed_files=processed,
        )

    return processed


def _do_work(db: dbHandlerBKP, sftp: sftpConnection, task: dict) -> int:
    """Stream discovered files and queue backlog control. Returns file count."""
    processed = _stream_discovery_batches(db, sftp, task)
    db.queue_host_task(
        host_id=task["host_id"],
        task_type=k.HOST_TASK_BACKLOG_CONTROL_TYPE,
        task_status=k.TASK_PENDING,
        filter_dict=task["host_filter"],
    )
    log.event("backlog_control_queued", host_id=task["host_id"])
    return processed


# --- finalization ---

def _finalize_success(db: dbHandlerBKP, task: dict, *, processed: int, elapsed_sec: float) -> None:
    """Write TASK_DONE, log completion, and schedule deferred statistics."""
    db.host_task_update(
        task_id=task["task_id"],
        NU_STATUS=k.TASK_DONE,
        NU_PID=k.HOST_UNLOCKED_PID,
        DT_HOST_TASK=datetime.now(),
        NA_MESSAGE=tools.compose_message(
            task_type=k.FILE_TASK_DISCOVERY,
            task_status=k.TASK_DONE,
            detail=f"host_id={task['host_id']} queued_backlog_control=1",
        ),
    )
    log.event(
        "discovery_completed",
        host_id=task["host_id"],
        task_id=task["task_id"],
        host=task["hostname"],
        discovered_files=processed,
        queued_backlog_tasks=1,
        elapsed_sec=round(elapsed_sec, 3),
    )
    if processed > 0:
        try:
            db.host_task_statistics_create(host_id=task["host_id"])
        except Exception as e:
            log.event("statistics_update_failed", host_id=task["host_id"], error=e)


def _finalize_error(
    db: dbHandlerBKP,
    task: dict | None,
    err: errors.ErrorHandler,
) -> None:
    """Persist TASK_ERROR and queue a host-check if the failure is bootstrap-class."""
    if task is None:
        err.log_error()
        return

    err.log_error(host_id=task["host_id"], task_id=task["task_id"])

    try:
        db.host_task_update(
            task_id=task["task_id"],
            NU_STATUS=k.TASK_ERROR,
            NU_PID=k.HOST_UNLOCKED_PID,
            NA_MESSAGE=tools.compose_message(
                task_type=k.FILE_TASK_DISCOVERY,
                task_status=k.TASK_ERROR,
                error=err.format_persisted_error(),
            ),
            DT_HOST_TASK=datetime.now(),
        )
    except Exception as e2:
        log.event("finalize_error_failed", task_id=task["task_id"], error=e2)

    # Bootstrap failures need a second opinion from host_check.
    # AUTH joins the same path so credential problems pass through too.
    if err.stage in {k.STAGE_AUTH, k.STAGE_CONNECT, k.STAGE_SSH}:
        try:
            db.queue_host_task(
                host_id=task["host_id"],
                task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE,
                task_status=k.TASK_PENDING,
                filter_dict=k.NONE_FILTER,
            )
        except Exception as e:
            log.event(
                "queue_host_check_failed",
                service=SERVICE_NAME,
                host_id=task["host_id"],
                task_id=task["task_id"],
                error=e,
            )


def _cleanup(
    sftp: sftpConnection | None,
    db: dbHandlerBKP,
    task: dict | None,
) -> None:
    """Close SFTP and release the host lock. Never raises."""
    try:
        if sftp:
            sftp.close()
    except Exception as e:
        log.event("cleanup_sftp_failed", error=e)

    if task is None:
        return

    # This is the single release point for hosts locked by _read_next_task.
    host_runtime.release_locked_host(
        db,
        task["host_id"],
        logger=log,
        service_name=SERVICE_NAME,
    )


def _init_db() -> dbHandlerBKP:
    """Connect to the operational database. Exits the process on failure."""
    try:
        return dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.event("db_init_failed", service=SERVICE_NAME, error=e)
        sys.exit(1)


def main() -> None:
    log.service_start(SERVICE_NAME)
    db = _init_db()

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        task = None
        sftp = None

        try:
            # --- read ---
            task = _read_next_task(db)
            if task is None:
                runtime_sleep.random_jitter_sleep()
                continue

            # --- claim ---
            if not _claim_task(db, task):
                runtime_sleep.random_jitter_sleep()
                continue

            # --- work ---
            sftp = host_context.init_host_context(task, log)

            start = time.monotonic()
            processed = _do_work(db, sftp, task)
            elapsed_sec = time.monotonic() - start

            # --- finalize ---
            _finalize_success(db, task, processed=processed, elapsed_sec=elapsed_sec)

        except Exception as e:
            if not err.triggered:
                # sftp is None after a claimed task means SSH bootstrap failed;
                # classify the exception to route the error to the right stage.
                stage = (
                    errors.classify_ssh_connect_exc(e).stage
                    if sftp is None and task is not None
                    else k.STAGE_MAIN
                )
                err.capture(
                    reason="Discovery task failed",
                    stage=stage,
                    exc=e,
                    host_id=task["host_id"] if task else None,
                    task_id=task["task_id"] if task else None,
                )
            _finalize_error(db, task, err)

        finally:
            _cleanup(sftp, db, task)

        runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err = errors.ErrorHandler(log)
        err.capture(reason="Fatal discovery worker crash", stage=k.STAGE_MAIN, exc=e)
        err.log_error()
        host_runtime.release_busy_hosts_for_current_pid(
            db_factory=dbHandlerBKP,
            database_name=k.BKP_DATABASE_NAME,
            logger=log,
        )
        raise
