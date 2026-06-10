#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Queued backlog-management worker for appCataloga.

This daemon owns only backlog transitions already represented in `FILE_TASK`.
It never talks to remote hosts and never touches the filesystem. Its contract
is intentionally narrow:

    - promote `DISCOVERY / DONE` into `BACKUP / PENDING`
    - roll back `BACKUP / PENDING` into `DISCOVERY / DONE`
    - keep those transitions auditable through dedicated HOST_TASK rows

By keeping backlog movement outside discovery itself, operational actions such
as STOP/rollback can reuse the same worker without abusing the discovery flow.

The loop is intentionally linear:
    1. read the next backlog-control HOST_TASK
    2. claim it atomically
    3. apply one pure-DB backlog transition
    4. persist the durable HOST_TASK outcome

That makes this worker the queue-side counterpart of discovery:
    - discovery records what exists
    - backlog management decides what becomes backup work
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

from db.dbHandlerBKP import dbHandlerBKP
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import errors, logging_utils
import config as k


# ============================================================
# Globals
# ============================================================
SERVICE_NAME = "appCataloga_backlog_management"
log = logging_utils.log()
process_status = {"running": True}
# Rollback gets priority over promotion on purpose: if an operator requested
# STOP, we should drain the already-queued backup work before creating more.
HOST_TASK_PRIORITY = (
    k.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
    k.HOST_TASK_BACKLOG_CONTROL_TYPE,
)


# ============================================================
# Signal handling
# ============================================================
def _shutdown_cleanup(signal_name: str) -> None:
    """
    Keep the shutdown hook explicit even though this worker owns no host locks.

    The other workers already use the same signal-runtime pattern.
    Keeping it here makes the service contract easier to scan.
    """


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
    on_shutdown=_shutdown_cleanup,
)


def _read_next_task(db: dbHandlerBKP) -> dict | None:
    """
    Return the next queued backlog-control HOST_TASK by fixed priority.

    Priority belongs to the worker contract, not to random SQL ordering:
        1. rollback / STOP requests
        2. normal promotion requests
    """
    for task_type in HOST_TASK_PRIORITY:
        task_row = db.host_task_read(
            task_status=k.TASK_PENDING,
            task_type=task_type,
        )
        if task_row:
            return {
                "host_id"    : task_row["HOST__ID_HOST"],
                "task_id"    : task_row["HOST_TASK__ID_HOST_TASK"],
                "task_type"  : task_row["HOST_TASK__NU_TYPE"],
                "host_filter": task_row.get("host_filter") or dict(k.NONE_FILTER),
                "now"        : datetime.now(),
            }
    return None


def _claim_task(db: dbHandlerBKP, task: dict) -> bool:
    """
    Atomically move one backlog-control HOST_TASK to RUNNING.

    Backlog management is DB-only, so there is no `HOST.IS_BUSY` lock here.
    The claim boundary is the HOST_TASK row itself.
    """
    result = db.host_task_update(
        task_id=task["task_id"],
        expected_status=k.TASK_PENDING,
        NU_STATUS=k.TASK_RUNNING,
        NU_PID=os.getpid(),
        DT_HOST_TASK=task["now"],
        NA_MESSAGE="Backlog management task running",
    )

    if result["rows_affected"] == 1:
        log.task_claimed(
            SERVICE_NAME,
            host_id=task["host_id"],
            task_id=task["task_id"],
            task_type=task["task_type"],
        )
        return True

    log.warning_event(
        "task_claim_race",
        service=SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=task["task_type"],
    )
    return False


def _promote_backlog(db: dbHandlerBKP, task: dict) -> dict:
    """
    Promote the selected discovery slice into backup work.

    Discovery already created the rows.
    This worker only changes which subset becomes backup work.
    """
    return db.update_backlog_by_filter(
        host_id=task["host_id"],
        task_filter=task["host_filter"],
        search_type=k.FILE_TASK_DISCOVERY,
        search_status=k.TASK_DONE,
        new_type=k.FILE_TASK_BACKUP_TYPE,
        new_status=k.TASK_PENDING,
    )


def _cancel_pending_backlog_promotions(
    db: dbHandlerBKP,
    *,
    host_id: int,
    now: datetime,
) -> None:
    """
    Cancel queued promotion rows before applying a rollback.

    Without this step, STOP could recreate backup work right after rollback.
    """
    db.host_task_update(
        where_dict={
            "FK_HOST": host_id,
            "NU_TYPE": k.HOST_TASK_BACKLOG_CONTROL_TYPE,
            "NU_STATUS": k.TASK_PENDING,
        },
        NU_STATUS=k.TASK_DONE,
        DT_HOST_TASK=now,
        NA_MESSAGE="Pending backlog promotion canceled by rollback request",
    )


def _rollback_backlog(db: dbHandlerBKP, task: dict) -> dict:
    """
    Roll back pending backup work into the discovery queue.

    Rollback is stronger than queued promotion for the same host.
    STOP should win before any new backup work is recreated.
    """
    _cancel_pending_backlog_promotions(
        db,
        host_id=task["host_id"],
        now=task["now"],
    )
    return db.update_backlog_by_filter(
        host_id=task["host_id"],
        task_filter=task["host_filter"],
        search_type=k.FILE_TASK_BACKUP_TYPE,
        search_status=k.TASK_PENDING,
        new_type=k.FILE_TASK_DISCOVERY,
        new_status=k.TASK_DONE,
    )


def _do_work(db: dbHandlerBKP, task: dict) -> dict:
    """
    Execute one backlog transition and return a small action summary.

    Promotion and rollback deliberately share the same DB primitive.
    The difference lives only in:
        - source type/status
        - target type/status
        - the pre-step that cancels pending promotion when STOP wins
    """
    # The entrypoint measures the full `_do_work()` duration for `task_done`.
    # This function measures only the completed domain phase.
    phase_started_at = time.monotonic()

    match task["task_type"]:
        case k.HOST_TASK_BACKLOG_CONTROL_TYPE:
            result = _promote_backlog(db, task)
            action = "promote"

        case k.HOST_TASK_BACKLOG_ROLLBACK_TYPE:
            result = _rollback_backlog(db, task)
            action = "rollback"

        case _:
            raise ValueError(f"Unsupported backlog task type: {task['task_type']}")

    # Statistics stay deferred and coarse-grained.
    # Only real row movement triggers a refresh.
    if result.get("rows_updated", 0) > 0:
        db.host_task_statistics_create(host_id=task["host_id"])

    outcome = {
        "action": action,
        "rows_updated": result.get("rows_updated", 0),
        "moved_to_backup": result.get("moved_to_backup", 0),
        "moved_to_discovery": result.get("moved_to_discovery", 0),
        "selected_total_kb": result.get("selected_total_kb", 0),
    }
    phase_elapsed_sec = time.monotonic() - phase_started_at
    log.task_phase(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=task["task_type"],
        phase="work",
        elapsed_sec=round(phase_elapsed_sec, 3),
        since_start_sec=round(phase_elapsed_sec, 3),
        action=outcome["action"],
        rows_updated=outcome["rows_updated"],
        moved_to_backup=outcome["moved_to_backup"],
        moved_to_discovery=outcome["moved_to_discovery"],
        selected_total_kb=outcome["selected_total_kb"],
    )
    return outcome


def _classify_work_failure(exc: Exception, *, task: dict | None) -> tuple[str, str]:
    """Map a raised exception to the worker error reason and stage."""
    if task is not None:
        return "Backlog management failed", k.STAGE_BACKLOG
    return "Backlog management failed", k.STAGE_MAIN


def _finalize_success(
    db: dbHandlerBKP,
    task: dict,
    result: dict,
    *,
    elapsed_sec: float,
) -> None:
    """Persist TASK_DONE for one completed backlog transition."""
    db.host_task_update(
        task_id=task["task_id"],
        NU_STATUS=k.TASK_DONE,
        NU_PID=k.HOST_UNLOCKED_PID,
        DT_HOST_TASK=task["now"],
        NA_MESSAGE=(
            "Backlog management completed successfully "
            f"(action={result['action']}, rows_updated={result['rows_updated']}, "
            f"selected_total_kb={result['selected_total_kb']})"
        ),
    )
    log.task_done(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=task["task_type"],
        elapsed_sec=round(elapsed_sec, 3),
        action=result["action"],
        rows_updated=result["rows_updated"],
        moved_to_backup=result["moved_to_backup"],
        moved_to_discovery=result["moved_to_discovery"],
        selected_total_kb=result["selected_total_kb"],
    )


def _finalize_error(
    db: dbHandlerBKP,
    task: dict | None,
    err: errors.ErrorHandler,
) -> None:
    """Persist TASK_ERROR for one failed backlog-control HOST_TASK."""
    if task is None:
        err.log_error()
        return

    err.log_error(host_id=task["host_id"], task_id=task["task_id"])
    try:
        db.host_task_update(
            task_id=task["task_id"],
            NU_STATUS=k.TASK_ERROR,
            NU_PID=k.HOST_UNLOCKED_PID,
            DT_HOST_TASK=datetime.now(),
            NA_MESSAGE=f"Backlog management error | {err.format_persisted_error()}",
        )
    except Exception as e2:
        log.error_event(
            "task_finalization_failed",
            service=SERVICE_NAME,
            host_id=task["host_id"],
            task_id=task["task_id"],
            task_type=task["task_type"],
            exception=repr(e2),
        )

    log.task_error(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["task_id"],
        task_type=task["task_type"],
        stage=err.stage,
        error=err.format_error() or "Backlog management failed",
    )


def _cleanup(task: dict | None) -> None:  # noqa: ARG001
    """Release per-iteration resources. This worker owns none."""


def _init_db() -> dbHandlerBKP:
    """Create the operational DB handler or stop the process early."""
    try:
        return dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error_event("db_init_failed", service=SERVICE_NAME, error=e)
        sys.exit(1)


def main() -> None:
    """
    Run the backlog-management worker until shutdown is requested.

    Reading guide:
        1. open the DB dependency once
        2. fetch the next queued backlog HOST_TASK
        3. claim it atomically
        4. apply the DB-only transition
        5. persist TASK_DONE or TASK_ERROR
    """
    log.service_start(SERVICE_NAME)
    db = _init_db()

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        task = None

        try:
            task = _read_next_task(db)
            if task is None:
                # Idle polls use jitter to avoid synchronized worker wakeups.
                runtime_sleep.random_jitter_sleep()
                continue

            if not _claim_task(db, task):
                # Another worker may claim the HOST_TASK first.
                runtime_sleep.random_jitter_sleep()
                continue

            # The entrypoint measures total work time.
            # `_do_work()` measures only the completed `work` phase.
            work_started_at = time.monotonic()
            result = _do_work(db, task)
            elapsed_sec = time.monotonic() - work_started_at
            _finalize_success(db, task, result, elapsed_sec=elapsed_sec)

        except Exception as e:
            if not err.triggered:
                reason, stage = _classify_work_failure(e, task=task)
                err.capture(
                    reason=reason,
                    stage=stage,
                    exc=e,
                    host_id=task["host_id"] if task else None,
                    task_id=task["task_id"] if task else None,
                )
            _finalize_error(db, task, err)

        finally:
            _cleanup(task)

        runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # The loop already handles normal task failures.
        # Reaching this block means the process itself is unstable.
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal backlog management worker crash",
            stage=k.STAGE_MAIN,
            exc=e,
        )
        err.log_error()
        raise
