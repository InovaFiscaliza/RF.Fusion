#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Discovery worker for the appCataloga ecosystem.

This service scans remote hosts for candidate files, writes immutable discovery
history, and hands off backlog promotion to a dedicated worker.

In the larger pipeline, discovery is the bridge between:
    - one HOST_TASK of type PROCESSING for a host
    - many FILE_TASK rows of type DISCOVERY for the files found on that host

That makes this worker both host-aware and file-aware. It owns one remote
session at a time, but it can emit many downstream file tasks from that single
host pass.

The loop is intentionally linear:
    1. claim PROCESSING host task
    2. bootstrap remote host context
    3. stream discovery batches into task/history tables
    4. queue backlog management for downstream promotion
    5. release the host and schedule statistics refresh

Keeping those steps explicit makes lock handling, retries, and backlog
promotion easier to audit in production.
"""

from __future__ import annotations

import os
import sys
import traceback
from datetime import datetime

from bootstrap_paths import bootstrap_app_paths
# Every worker needs the same app/config/db import paths. Centralizing that
# bootstrap keeps the entrypoint focused on discovery flow instead of sys.path.
PROJECT_ROOT = bootstrap_app_paths(__file__)

# Import customized libs
from db.dbHandlerBKP import dbHandlerBKP
from host_handler import bootstrap_flow, host_runtime
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import (
    errors,
    filter,
    logging_utils,
    tools,
)
import config as k


# ======================================================================
# Globals
# ======================================================================
SERVICE_NAME = "appCataloga_discovery"
log = logging_utils.log()
process_status = {"running": True}


# ============================================================
# Signal handling
# ============================================================
def _shutdown_cleanup(signal_name: str) -> None:
    """
    Release BUSY host locks during process shutdown.

    Discovery does not own any sibling worker pool, so shutdown cleanup is
    intentionally narrow here: release any HOST rows still marked BUSY by this
    PID and let the normal daemon stop path do the rest.
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


def _claim_processing_task(
    db: dbHandlerBKP,
    *,
    task_id: int,
    host_id: int,
    hostname: str | None,
) -> bool:
    """
    Atomically convert one PROCESSING host task from PENDING to RUNNING.

    Discovery claims the HOST_TASK before touching SSH/SFTP on purpose.
    That keeps ownership deterministic and avoids an extra pre-flight probe
    racing with sibling workers for the same host.

    Returns:
        bool: False when another worker already moved this row out of the
        claimable state and the caller should simply fetch another task.
    """
    lock_result = db.host_task_update(
        where_dict={
            "ID_HOST_TASK": task_id,
            "NU_STATUS": k.TASK_PENDING,
        },
        NU_STATUS=k.TASK_RUNNING,
        NU_PID=os.getpid(),
        DT_HOST_TASK=datetime.now(),
        NA_MESSAGE="Discovery task running",
    )

    if lock_result["rows_affected"] != 1:
        log.warning(
            f"event=host_task_claim_race host_id={host_id} task_id={task_id}"
        )
        return False

    log.event(
        "discovery_started",
        host_id=host_id,
        task_id=task_id,
        host=hostname,
    )
    return True


def _requeue_transient_bootstrap_failure(
    db: dbHandlerBKP,
    *,
    host_id: int,
    task_id: int,
    exc: Exception,
) -> bool:
    """
    Return the same HOST_TASK to PENDING after a transient SSH/SFTP failure.

    This helper owns the full retry policy for bootstrap contention:
        1. optionally queue CHECK_CONNECTION for stronger network-like symptoms
        2. requeue the discovery HOST_TASK back to PENDING
        3. start a short BUSY cooldown so backup does not immediately
           reclaim the same host and recreate the same contention

    Returns:
        bool: True when the BUSY flag should be preserved for cooldown.

    Important:
        "transient" here does not mean "proven contention". It means the
        bootstrap failure is weak evidence and should be retried instead of
        immediately converting the task into TASK_ERROR.
    """
    retry_detail = errors.get_transient_sftp_retry_detail(exc)

    # Some transient failures look suspicious enough that host_check should
    # reconcile host state explicitly instead of leaving discovery/backups to
    # keep guessing.
    if errors.should_queue_host_check(exc):
        try:
            db.queue_host_task(
                host_id=host_id,
                task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE,
                task_status=k.TASK_PENDING,
                filter_dict=k.NONE_FILTER,
            )
        except Exception as queue_exc:
            log.error(
                "event=queue_host_check_failed "
                f"service={SERVICE_NAME} host_id={host_id} "
                f"task_id={task_id} error={queue_exc}"
            )

    # The same HOST_TASK row is recycled back to PENDING so discovery remains
    # observable as a stable per-host workflow instead of creating new rows.
    db.host_task_update(
        task_id=task_id,
        NU_STATUS=k.TASK_PENDING,
        DT_HOST_TASK=datetime.now(),
        NA_MESSAGE=tools.compose_message(
            task_type=k.FILE_TASK_DISCOVERY,
            task_status=k.TASK_PENDING,
            detail=retry_detail,
        ),
    )

    preserve_host_busy_cooldown = db.host_start_transient_busy_cooldown(
        host_id=host_id,
        owner_pid=os.getpid(),
        cooldown_seconds=k.SFTP_BUSY_COOLDOWN_SECONDS,
    )

    log.warning_event(
        "sftp_init_retry",
        service=SERVICE_NAME,
        host_id=host_id,
        task_id=task_id,
        timeout_like=errors.is_timeout_like_sftp_init_error(exc),
        retry_detail=retry_detail,
        error=exc,
    )

    # The short cooldown reserves the next slot for discovery recovery instead
    # of letting backup immediately reclaim the same host and recreate the same
    # contention pattern.
    if preserve_host_busy_cooldown:
        log.warning(
            "event=sftp_busy_cooldown_started "
            f"service={SERVICE_NAME} host_id={host_id} "
            f"task_id={task_id} "
            f"cooldown_seconds={k.SFTP_BUSY_COOLDOWN_SECONDS}"
        )

    return preserve_host_busy_cooldown


def _stream_discovery_batches(
    db: dbHandlerBKP,
    daemon,
    *,
    task: dict,
    host_id: int,
    hostname: str | None,
) -> int:
    """
    Persist discovered metadata in bounded batches and return rows written.

    Discovery writes each batch twice on purpose:
        - FILE_TASK stores the mutable pipeline queue
        - FILE_TASK_HISTORY stores the immutable audit trail

    The DB callbacks handle deduplication and last-seen cutoffs, so this loop
    can stay focused on streaming batches instead of trying to keep the full
    remote inventory in memory.
    """
    host_filter = filter.Filter(task["host_filter"], log=log)
    processed = 0

    for batch in daemon.iter_metadata_files(
        host_id=host_id,
        hostname=hostname,
        filter_obj=host_filter,
        callBackCheckFile=db.filter_existing_file_batch,
        callBackGetLastDBDate=db.get_last_discovery,
        batch_size=k.DISCOVERY_BATCH_SIZE,
    ):
        # FILE_TASK is the live queue entry that later workers may still
        # mutate, suspend, resume, or promote.
        db.file_task_create(
            host_id=host_id,
            file_metadata=batch,
            task_type=k.FILE_TASK_DISCOVERY,
            task_status=k.TASK_DONE,
        )

        # FILE_TASK_HISTORY is the immutable audit trail for what discovery
        # observed on the source host during this pass.
        db.file_history_create(
            host_id=host_id,
            file_metadata=batch,
            task_type=k.FILE_TASK_DISCOVERY,
            task_status=k.TASK_DONE,
        )

        processed += len(batch)
        log.event(
            "discovery_progress",
            host_id=host_id,
            processed_files=processed,
        )

    return processed


def _queue_backlog_control_task(
    db: dbHandlerBKP,
    *,
    task: dict,
    task_id: int,
    host_id: int,
    hostname: str | None,
    processed: int,
) -> dict:
    """
    Queue backlog management after discovery and close the HOST_TASK.

    Discovery and backlog control are intentionally separate stages:
        1. discovery records what exists
        2. backlog management decides what should become backup work

    Keeping those concerns separate makes re-runs and backlog diagnosis much
    easier in production.
    """
    db.queue_host_task(
        host_id=host_id,
        task_type=k.HOST_TASK_BACKLOG_CONTROL_TYPE,
        task_status=k.TASK_PENDING,
        filter_dict=task["host_filter"],
    )

    log.event(
        "backlog_control_queued",
        host_id=host_id,
        queued_backlog_tasks=1,
    )

    db.host_task_update(
        task_id=task_id,
        NU_STATUS=k.TASK_DONE,
        NU_PID=k.HOST_UNLOCKED_PID,
        DT_HOST_TASK=datetime.now(),
        NA_MESSAGE=tools.compose_message(
            task_type=k.FILE_TASK_DISCOVERY,
            task_status=k.TASK_DONE,
            detail=(
                f"host_id={host_id} "
                "queued_backlog_control=1"
            ),
        ),
    )

    log.event(
        "discovery_completed",
        host_id=host_id,
        task_id=task_id,
        host=hostname,
        discovered_files=processed,
        queued_backlog_tasks=1,
    )

    return {
        "queued_backlog_tasks": 1,
    }


def _persist_discovery_error(
    db: dbHandlerBKP,
    err: errors.ErrorHandler,
    *,
    host_id: int | None,
    task_id: int,
    hostname: str | None,
    processed: int,
    backlog_result: dict,
    ) -> None:
    """
    Persist TASK_ERROR state and schedule host reconciliation when needed.

    This helper is the single error-finalization path for non-transient
    discovery failures. Keeping it centralized makes it easier to audit which
    failures only affect the current HOST_TASK and which ones also ask the
    host-check worker for deeper reconciliation.
    """
    # Emit one structured operational log before we touch persistence so
    # diagnosis still has context even if the DB update itself fails.
    err.log_error(
        host_id=host_id,
        task_id=task_id,
        host=hostname,
        discovered_files=processed,
        queued_backlog_tasks=backlog_result.get("queued_backlog_tasks", 0),
    )

    try:
        # TASK_ERROR closes the host-level discovery pass, but the row itself
        # stays in place for observability instead of being deleted.
        db.host_task_update(
            task_id=task_id,
            NU_STATUS=k.TASK_ERROR,
            NU_PID=k.HOST_UNLOCKED_PID,
            NA_MESSAGE=tools.compose_message(
                task_type=k.FILE_TASK_DISCOVERY,
                task_status=k.TASK_ERROR,
                error=err.format_error(),
            ),
            DT_HOST_TASK=datetime.now(),
        )
    except Exception as update_exc:
        log.error(
            "event=discovery_error_persist_failed "
            f"service={SERVICE_NAME} host_id={host_id} "
            f"task_id={task_id} error={update_exc}"
        )

    # Bootstrap failures need a second opinion from host_check. AUTH joins the
    # same path so explicit credential problems also pass through the shared
    # host-state reconciliation flow.
    if err.stage in {"AUTH", "CONNECT", "SSH"}:
        try:
            db.queue_host_task(
                host_id=host_id,
                task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE,
                task_status=k.TASK_PENDING,
                filter_dict=k.NONE_FILTER,
            )
        except Exception as queue_exc:
            log.error(
                "event=queue_host_check_failed "
                f"service={SERVICE_NAME} host_id={host_id} "
                f"task_id={task_id} error={queue_exc}"
            )

    log.error_event(
        "discovery_error",
        host_id=host_id,
        task_id=task_id,
        host=hostname,
        discovered_files=processed,
        queued_backlog_tasks=backlog_result.get("queued_backlog_tasks", 0),
        error=err.format_error() or "Discovery failed",
    )


# ======================================================================
# Main daemon loop
# ======================================================================
def main() -> None:
    """
    Run the discovery worker until shutdown is requested.

    The entrypoint deliberately keeps ownership of the daemon loop while
    delegating the dense domain steps to a few meaningful helpers:
        1. claim PROCESSING work
        2. bootstrap SSH/SFTP
        3. persist discovery batches
        4. queue backlog management
        5. finalize HOST_TASK state and release the host

    That split is intentional: the helpers hide repeated local policy, but the
    full lifecycle of one discovery pass still reads top-to-bottom in this
    file.
    """

    log.service_start(SERVICE_NAME)
    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"event=db_init_failed service={SERVICE_NAME} error={e}")
        sys.exit(1)

    # ===============================================================
    # MAIN LOOP
    # ===============================================================
    while process_status["running"]:

        daemon = None
        sftp = None
        err = errors.ErrorHandler(log)
        task = None
        host_id = None
        task_id = None
        hostname = None
        preserve_host_busy_cooldown = False
        processed = 0
        backlog_result = {"queued_backlog_tasks": 0}

        try:
            # ==========================================================
            # ACT I — Fetch the next PROCESSING host task and lock the host
            # ==========================================================
            task = db.host_task_read(
                task_type=k.HOST_TASK_PROCESSING_TYPE,
                task_status=k.TASK_PENDING,
                check_host_busy=True,
                check_host_offline=True,
                lock_host=True,
            )

            if not task:
                # Idle discovery should stay cheap: no extra work, no extra
                # logs, just jitter and poll again on the next loop.
                continue

            host_id = task["HOST__ID_HOST"]
            task_id = task["HOST_TASK__ID_HOST_TASK"]
            hostname = task["HOST__NA_HOST_NAME"]

            # ==========================================================
            # ACT II — Mark the claimed host task as RUNNING
            # ==========================================================
            try:
                if not _claim_processing_task(
                    db,
                    task_id=task_id,
                    host_id=host_id,
                    hostname=hostname,
                ):
                    continue
            except Exception as e:
                err.capture(
                    "Failed to lock HOST or HOST_TASK",
                    "LOCK_TASK",
                    e,
                    host_id=host_id,
                    task_id=task_id,
                )
                continue

            # ==========================================================
            # ACT III — Bootstrap remote SSH/SFTP context
            # ==========================================================
            sftp, daemon, preserve_host_busy_cooldown = (
                bootstrap_flow.init_host_context_with_retry(
                    task=task,
                    log=log,
                    err=err,
                    host_id=host_id,
                    task_id=task_id,
                    transient_retry_handler=_requeue_transient_bootstrap_failure,
                    retry_handler_kwargs={
                        "db": db,
                        "host_id": host_id,
                        "task_id": task_id,
                    },
                    retry_failure_reason="Failed to requeue transient discovery task",
                )
            )
            if sftp is None:
                # The shared bootstrap flow already decided whether this was:
                #   - a transient retryable failure, or
                #   - a fatal AUTH/SSH/CONNECT error stored in `err`
                continue

            # ==========================================================
            # ACT IV — Stream discovery batches into task and history tables
            # ==========================================================
            try:
                processed = _stream_discovery_batches(
                    db,
                    daemon,
                    task=task,
                    host_id=host_id,
                    hostname=hostname,
                )
            except Exception as e:
                err.capture(
                    "Discovery failed",
                    "DISCOVERY",
                    e,
                    host_id=host_id,
                    task_id=task_id,
                )
                continue

            # ==========================================================
            # ACT V — Hand off discovery backlog to the dedicated manager
            # ==========================================================
            try:
                backlog_result = _queue_backlog_control_task(
                    db,
                    task=task,
                    task_id=task_id,
                    host_id=host_id,
                    hostname=hostname,
                    processed=processed,
                )
            except Exception as e:
                err.capture(
                    "Backlog handoff failed",
                    "BACKLOG",
                    e,
                    host_id=host_id,
                    task_id=task_id,
                )
                continue

        # ==============================================================
        # OUTER EXCEPTIONS
        # ==============================================================
        except Exception as e:
            err.capture(
                reason="Unexpected discovery loop failure",
                stage="MAIN",
                exc=e,
                host_id=host_id,
                task_id=task_id,
                traceback=traceback.format_exc(),
            )

        # ==============================================================
        # FINALLY — UNLOCK + CLEANUP (ALWAYS EXECUTED)
        # ==============================================================
        finally:

            # Phase 1 — Persist terminal task failure, if this discovery pass
            # crossed from "retryable uncertainty" into a stable error state.
            if err.triggered and task_id:
                _persist_discovery_error(
                    db,
                    err,
                    host_id=host_id,
                    task_id=task_id,
                    hostname=hostname,
                    processed=processed,
                    backlog_result=backlog_result,
                )

            # Phase 2 — Close transport objects defensively. Cleanup must never
            # replace the real workflow error with a secondary close failure.
            try:
                if sftp:
                    try:
                        sftp.close()
                    except Exception:
                        pass
            except Exception as e:
                log.warning(f"event=cleanup_host_context_failed error={e}")

            # Phase 3 — Release the host unless transient bootstrap retry
            # deliberately kept the BUSY flag for its short cooldown window.
            if host_id is not None and not preserve_host_busy_cooldown:
                # This remains the single normal release point for the host
                # claimed by `host_task_read(..., lock_host=True)`.
                host_runtime.release_locked_host(
                    db,
                    host_id,
                    logger=log,
                    service_name=SERVICE_NAME,
                )

                # Phase 4 — Schedule deferred statistics refresh only after the
                # host is safely released. Statistics are secondary bookkeeping,
                # not part of the critical path of discovery itself.
                try:
                    if (not err.triggered) and processed > 0:
                        # Statistics refresh stays deferred so lock release is
                        # not delayed by host aggregation work.
                        db.host_task_statistics_create(host_id=host_id)
                except Exception as e:
                    log.warning(
                        f"event=statistics_update_failed host_id={host_id} error={e}"
                    )

            # Phase 5 — End every iteration with the same jitter contract so a
            # hot host or a hot error path does not spin the worker too hard.
            runtime_sleep.random_jitter_sleep()




# ======================================================================
# Entry Point
# ======================================================================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # This outer boundary is the last line of defense for the daemon
        # process itself, not for one host pass. By the time we get here the
        # worker is crashing as a service, so we log once, release BUSY locks,
        # and let the exception terminate the process.
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal discovery worker crash",
            stage="MAIN",
            exc=e,
        )
        err.log_error()
        host_runtime.release_busy_hosts_for_current_pid(
            db_factory=dbHandlerBKP,
            database_name=k.BKP_DATABASE_NAME,
            logger=log,
        )
        raise
