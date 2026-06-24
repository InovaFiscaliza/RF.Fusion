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

from utils.bootstrap_paths import bootstrap_app_paths

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
    """Release BUSY host marks owned by this PID during shutdown."""
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
    """Read the next discovery task and normalize the worker context.

    Discovery claims a host-level PROCESSING row because one remote scan
    may produce many file rows, but the host still needs one queue owner.
    """
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
    Atomically move the HOST_TASK from PENDING to RUNNING.

    Discovery claims before opening SSH/SFTP so remote work starts only after
    queue ownership is stable. Another worker may still win this race first.
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
        log.task_claimed(
            SERVICE_NAME,
            host_id=task["host_id"],
            task_id=task["task_id"],
            task_type=k.HOST_TASK_PROCESSING_TYPE,
            host=task["hostname"],
        )
        return True
    log.warning_event(
        "task_claim_race",
        service=SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=k.HOST_TASK_PROCESSING_TYPE,
    )
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

    The DB callbacks own deduplication and date cutoffs.
    This loop stays focused on streaming remote metadata.
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

    return processed


def _do_work(db: dbHandlerBKP, sftp: sftpConnection, task: dict) -> dict:
    """
    Stream discovered files and queue backlog control.

    The entrypoint measures total `_do_work()` duration for `task_done`.
    This function measures only completed internal phases.
    """
    scan_started_at = time.monotonic()
    processed = _stream_discovery_batches(db, sftp, task)
    scan_elapsed_sec = round(time.monotonic() - scan_started_at, 3)
    log.task_phase(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=k.HOST_TASK_PROCESSING_TYPE,
        phase="scan",
        elapsed_sec=scan_elapsed_sec,
        since_start_sec=scan_elapsed_sec,
        host=task["hostname"],
        discovered_files=processed,
    )

    queue_started_at = time.monotonic()
    db.queue_host_task(
        host_id=task["host_id"],
        task_type=k.HOST_TASK_BACKLOG_CONTROL_TYPE,
        task_status=k.TASK_PENDING,
        filter_dict=task["host_filter"],
    )
    queue_elapsed_sec = round(time.monotonic() - queue_started_at, 3)
    total_elapsed_sec = round(scan_elapsed_sec + queue_elapsed_sec, 3)
    log.task_phase(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=k.HOST_TASK_PROCESSING_TYPE,
        phase="queue_backlog",
        elapsed_sec=queue_elapsed_sec,
        since_start_sec=total_elapsed_sec,
        host=task["hostname"],
        queued_backlog_tasks=1,
    )

    return {
        "processed": processed,
        "queued_backlog_tasks": 1,
    }


def _classify_work_failure(
    exc: Exception,
    *,
    task: dict | None,
    sftp: sftpConnection | None,
) -> tuple[str, str]:
    """Map a raised exception to the worker error reason and stage.

    SSH bootstrap failures reuse the shared classifier because the same
    transport errors appear in backup and host-check too.
    """
    if task is not None and sftp is None:
        ssh_failure = errors.classify_ssh_connect_failure(exc)
        if ssh_failure is not None:
            return ssh_failure

    return "Discovery task failed", k.STAGE_MAIN


# --- finalization ---

def _finalize_success(
    db: dbHandlerBKP,
    task: dict,
    *,
    processed: int,
    queued_backlog_tasks: int,
    elapsed_sec: float,
) -> None:
    """Persist TASK_DONE, log completion, and request deferred statistics."""
    db.host_task_update(
        task_id=task["task_id"],
        NU_STATUS=k.TASK_DONE,
        NU_PID=k.HOST_UNLOCKED_PID,
        DT_HOST_TASK=datetime.now(),
        NA_MESSAGE=tools.compose_message(
            task_type=k.FILE_TASK_DISCOVERY,
            task_status=k.TASK_DONE,
            detail=f"host_id={task['host_id']} queued_backlog_control={queued_backlog_tasks}",
        ),
    )
    log.task_done(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=k.HOST_TASK_PROCESSING_TYPE,
        elapsed_sec=round(elapsed_sec, 3),
        host=task["hostname"],
        discovered_files=processed,
        queued_backlog_tasks=queued_backlog_tasks,
    )
    if processed > 0:
        try:
            db.host_task_statistics_create(host_id=task["host_id"])
        except Exception as e:
            log.warning_event(
                "statistics_update_failed",
                service=SERVICE_NAME,
                host_id=task["host_id"],
                error=e,
            )


def _finalize_error(
    db: dbHandlerBKP,
    task: dict | None,
    err: errors.ErrorHandler,
) -> None:
    """Persist TASK_ERROR and request host-check follow-up for bootstrap failures."""
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
        log.error_event(
            "task_finalization_failed",
            service=SERVICE_NAME,
            host_id=task["host_id"],
            task_id=task["task_id"],
            task_type=k.HOST_TASK_PROCESSING_TYPE,
            exception=repr(e2),
        )

    log.task_error(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=k.HOST_TASK_PROCESSING_TYPE,
        stage=err.stage,
        error=err.format_error() or "Discovery failed",
        host=task["hostname"],
    )

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
            log.error_event(
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
    """Close SFTP and release the claimed host lock. Never raises."""
    try:
        if sftp:
            sftp.close()
    except Exception as e:
        log.warning_event("cleanup_sftp_failed", service=SERVICE_NAME, error=e)

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
    """Create the operational DB handler or stop the process early."""
    try:
        return dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error_event("db_init_failed", service=SERVICE_NAME, error=e)
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
                # Idle polls use jitter to avoid synchronized worker wakeups.
                runtime_sleep.random_jitter_sleep()
                continue

            # --- claim ---
            if not _claim_task(db, task):
                # Another worker got here first. This is not a task failure.
                runtime_sleep.random_jitter_sleep()
                continue

            # --- work ---
            # Open one remote session after the queue claim succeeds.
            sftp = host_context.init_host_context(task, log)

            # The entrypoint measures total work time.
            # `_do_work()` measures only completed internal phases.
            work_started_at = time.monotonic()
            result = _do_work(db, sftp, task)
            elapsed_sec = time.monotonic() - work_started_at

            # --- finalize ---
            _finalize_success(
                db,
                task,
                processed=result["processed"],
                queued_backlog_tasks=result["queued_backlog_tasks"],
                elapsed_sec=elapsed_sec,
            )

        except Exception as e:
            if not err.triggered:
                reason, stage = _classify_work_failure(e, task=task, sftp=sftp)
                err.capture(
                    reason=reason,
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
        # The loop already handles normal task failures.
        # Reaching this block means the process itself is unstable.
        err = errors.ErrorHandler(log)
        err.capture(reason="Fatal discovery worker crash", stage=k.STAGE_MAIN, exc=e)
        err.log_error()
        host_runtime.release_busy_hosts_for_current_pid(
            db_factory=dbHandlerBKP,
            database_name=k.BKP_DATABASE_NAME,
            logger=log,
        )
        raise
