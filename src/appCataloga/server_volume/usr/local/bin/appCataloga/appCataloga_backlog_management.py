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

    The other workers already use the same signal-runtime pattern, and keeping
    it here makes the service contract easy to scan during incidents.
    """


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
    on_shutdown=_shutdown_cleanup,
)


def _read_next_backlog_task(db: dbHandlerBKP) -> dict | None:
    """
    Return the next queued backlog-control HOST_TASK by fixed priority.

    Priority belongs to the worker contract, not to a random SQL ordering:
        1. rollback / STOP requests
        2. normal promotion requests
    """
    for task_type in HOST_TASK_PRIORITY:
        task = db.host_task_read(
            task_status=k.TASK_PENDING,
            task_type=task_type,
        )
        if task:
            return task
    return None


def _claim_backlog_task(db: dbHandlerBKP, task: dict) -> bool:
    """
    Atomically claim one queued backlog-control HOST_TASK.

    Backlog management is DB-only, so there is no `HOST.IS_BUSY` lock here.
    The claim boundary is the HOST_TASK row itself.
    """
    lock_result = db.host_task_update(
        task_id=task["task_id"],
        expected_status=k.TASK_PENDING,
        NU_STATUS=k.TASK_RUNNING,
        NU_PID=os.getpid(),
        DT_HOST_TASK=task["now"],
        NA_MESSAGE="Backlog management task running",
    )

    if lock_result["rows_affected"] == 1:
        return True

    log.warning(
        f"event=backlog_task_claim_race host_id={task['host_id']} task_id={task['task_id']}"
    )
    return False


def _cancel_pending_backlog_promotions(
    db: dbHandlerBKP,
    *,
    host_id: int,
    now: datetime,
) -> None:
    """
    Cancel queued promotion rows before applying a rollback.

    Without this step a pending DISCOVERY->BACKUP promotion could run right
    after STOP/rollback and recreate the queue the operator just removed.
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


def _apply_backlog_task(db: dbHandlerBKP, task: dict) -> dict:
    """
    Execute one backlog transition and return a small action summary.

    Promotion and rollback deliberately share the same DB primitive
    (`update_backlog_by_filter`). The difference lives only in:
        - source type/status
        - target type/status
        - the pre-step that cancels pending promotion when STOP wins
    """
    if task["task_type"] == k.HOST_TASK_BACKLOG_CONTROL_TYPE:
        # Normal steady-state path: discovery already wrote FILE_TASK rows,
        # now backlog management promotes the selected slice into backup.
        result = db.update_backlog_by_filter(
            host_id=task["host_id"],
            task_filter=task["host_filter"],
            search_type=k.FILE_TASK_DISCOVERY,
            search_status=k.TASK_DONE,
            new_type=k.FILE_TASK_BACKUP_TYPE,
            new_status=k.TASK_PENDING,
        )
        action = "promote"

    elif task["task_type"] == k.HOST_TASK_BACKLOG_ROLLBACK_TYPE:
        # STOP/rollback is intentionally stronger than any queued promotion for
        # the same host. We first neutralize pending promote rows, then move
        # BACKUP/PENDING back to DISCOVERY/DONE.
        _cancel_pending_backlog_promotions(
            db,
            host_id=task["host_id"],
            now=task["now"],
        )
        result = db.update_backlog_by_filter(
            host_id=task["host_id"],
            task_filter=task["host_filter"],
            search_type=k.FILE_TASK_BACKUP_TYPE,
            search_status=k.TASK_PENDING,
            new_type=k.FILE_TASK_DISCOVERY,
            new_status=k.TASK_DONE,
        )
        action = "rollback"

    else:
        raise ValueError(f"Unsupported backlog task type: {task['task_type']}")

    # Statistics stay deferred and coarse-grained, just like the rest of the
    # appCataloga workers. Only meaningful row movement triggers a refresh.
    if result.get("rows_updated", 0) > 0:
        db.host_task_statistics_create(host_id=task["host_id"])

    log.event(
        "backlog_task_applied",
        host_id=task["host_id"],
        task_id=task["task_id"],
        action=action,
        rows_updated=result.get("rows_updated", 0),
        moved_to_backup=result.get("moved_to_backup", 0),
        moved_to_discovery=result.get("moved_to_discovery", 0),
    )

    return {
        "action": action,
        "rows_updated": result.get("rows_updated", 0),
        "moved_to_backup": result.get("moved_to_backup", 0),
        "moved_to_discovery": result.get("moved_to_discovery", 0),
    }


def _finalize_backlog_success(
    db: dbHandlerBKP,
    *,
    task: dict,
    outcome: dict,
) -> None:
    """Persist TASK_DONE for one successfully applied backlog transition."""
    db.host_task_update(
        task_id=task["task_id"],
        NU_STATUS=k.TASK_DONE,
        NU_PID=k.HOST_UNLOCKED_PID,
        DT_HOST_TASK=task["now"],
        NA_MESSAGE=(
            "Backlog management completed successfully "
            f"(action={outcome['action']}, rows_updated={outcome['rows_updated']})"
        ),
    )


def _persist_backlog_error(
    db: dbHandlerBKP,
    *,
    task: dict,
    err: errors.ErrorHandler,
) -> None:
    """Persist TASK_ERROR for one failed backlog-control HOST_TASK."""
    err.log_error(host_id=task["host_id"], task_id=task["task_id"])
    db.host_task_update(
        task_id=task["task_id"],
        NU_STATUS=k.TASK_ERROR,
        NU_PID=k.HOST_UNLOCKED_PID,
        DT_HOST_TASK=datetime.now(),
        NA_MESSAGE=f"Backlog management error | {err.format_error()}",
    )


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

    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"event=db_init_failed service={SERVICE_NAME} error={e}")
        sys.exit(1)

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        task = None

        try:
            # Phase 1: backlog management consumes only explicit queue work.
            # When nothing is pending we keep the same jitter contract used by
            # the other workers instead of hot-polling the table.
            task_row = _read_next_backlog_task(db)
            if task_row is None:
                runtime_sleep.random_jitter_sleep()
                continue

            # Trim the HOST/HOST_TASK join to the fields this worker actually
            # reasons about during one backlog transition.
            task = {
                "host_id"       : task_row["HOST__ID_HOST"],
                "task_id"       : task_row["HOST_TASK__ID_HOST_TASK"],
                "task_type"     : task_row["HOST_TASK__NU_TYPE"],
                "host_filter"   : task_row.get("host_filter") or dict(k.NONE_FILTER),
                "now"           : datetime.now(),
            }

            # Phase 2: claiming the task is the only ownership boundary needed
            # here. There is no remote host session or BUSY lock involved.
            if not _claim_backlog_task(db, task):
                runtime_sleep.random_jitter_sleep()
                continue

            # Phase 3: apply one deterministic backlog transition and persist
            # the successful terminal state on the HOST_TASK itself.
            outcome = _apply_backlog_task(db, task)
            _finalize_backlog_success(db, task=task, outcome=outcome)

        except Exception as e:
            if task is None:
                # A failure before task materialization is a loop/service
                # problem, so we log it once and keep polling.
                err.capture(
                    reason="Unexpected backlog management loop failure",
                    stage="MAIN",
                    exc=e,
                )
                err.log_error()
            else:
                # Once a task exists, any failure becomes a durable TASK_ERROR
                # on that exact row for later operator inspection.
                err.capture(
                    reason="Backlog management failed",
                    stage="BACKLOG",
                    exc=e,
                    host_id=task["host_id"],
                    task_id=task["task_id"],
                )
                _persist_backlog_error(db, task=task, err=err)

        # Even after successful work we keep the same jitter so several worker
        # loops do not hammer the DB in perfect lockstep.
        runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # This is the last-resort daemon crash path. Per-task failures are
        # already normalized inside `main()`.
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal backlog management worker crash",
            stage="MAIN",
            exc=e,
        )
        err.log_error()
        raise
