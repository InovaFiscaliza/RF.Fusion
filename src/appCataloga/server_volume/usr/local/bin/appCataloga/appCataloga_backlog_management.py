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
from collections.abc import Mapping
from typing import cast

from utils.bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

from db.dbHandlerBKP import dbHandlerBKP
from db.dbHandlerBase import DbRow
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import errors, filter, logging_utils, tools
from db import constants as db_constants
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
    db_constants['tasks']['host_types']['backlog_rollback'],
    db_constants['tasks']['host_types']['backlog_control'],
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
            task_status=db_constants['tasks']['states']['pending'],
            task_type=task_type,
        )
        if task_row:
            return {
                "host_id"    : task_row["HOST__ID_HOST"],
                "task_id"    : task_row["HOST_TASK__ID_HOST_TASK"],
                "task_type"  : task_row["HOST_TASK__NU_TYPE"],
                "host_filter": task_row.get("host_filter") or dict(db_constants['tasks']['default_filter']),
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
        expected_status=db_constants['tasks']['states']['pending'],
        NU_STATUS=db_constants['tasks']['states']['running'],
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


def _update_backlog(
    db: dbHandlerBKP,
    *,
    logger: logging_utils.log,
    host_id: int,
    task_filter: Mapping[str, object],
    search_type: int,
    search_status: int | list[int],
    new_type: int,
    new_status: int,
) -> dict[str, int]:
    """Choose a backlog slice and persist its transition in one transaction.

    Args:
        db (dbHandlerBKP): Operational persistence session.
        logger (logging_utils.log): Filter diagnostics and transaction-failure logger.
        host_id (int): Host whose existing queue may be moved.
        task_filter (Mapping[str, object]): ALL/FILE/RANGE/LAST filter, optional
            extension, ordering, and max_total_gb. NONE/REDISCOVERY select nothing.
        search_type (int): Queue type required before the transition.
        search_status (int | list[int]): Required current state or allowed states.
        new_type (int): Queue type assigned to the selected rows.
        new_status (int): Queue state assigned to the selected rows.

    Returns:
        dict[str, int]: rows_updated, moved_to_backup, moved_to_discovery, and
        selected_total_kb. A bounded promotion stops at the first file that
        exceeds the budget, preserving the ordered prefix. Zero selections do
        not write or publish. Caller-owned transactions remain caller-owned.

    Raises:
        ValueError: If prepared predicates, ordering, or limits are invalid.
        psycopg.Error: If locking, persistence, or a self-owned commit fails.
    """
    result = {
        "rows_updated": 0,
        "moved_to_backup": 0,
        "moved_to_discovery": 0,
        "selected_total_kb": 0,
    }
    parsed = filter.Filter(dict(task_filter), log=logger)
    metadata = parsed.evaluate_database(host_id, search_type, search_status)
    where_conditions = cast(DbRow | None, metadata.get("where"))
    if not where_conditions:
        return result
    budget = cast(int | None, metadata.get("max_total_kb"))
    limit = cast(int | None, metadata.get("limit"))
    bounded = limit is not None or (
        new_type == db_constants['tasks']['file_types']['backup'] and budget is not None
    )
    owns_transaction = not db.in_transaction
    if owns_transaction:
        db.begin_transaction()
    try:
        selected: list[int] = []
        if bounded:
            direction = (
                "ASC"
                if parsed.data.get("sort_order")
                == filter.Filter.SORT_OLDEST_FIRST
                and parsed.data["mode"] != filter.Filter.MODE_LAST
                else "DESC"
            )
            null_order = "NULLS FIRST" if direction == "ASC" else "NULLS LAST"
            order_by = (
                ("DT_FILE_CREATED_HOST", f"{direction} {null_order}"),
                ("ID_FILE_TASK", direction),
            )
            if limit is not None and limit < 1:
                raise ValueError("Backlog limit must be positive.")
            candidates = db.file_task_list_for_backlog(
                where=where_conditions,
                order_by=order_by,
                limit=limit,
            )
            for row in candidates:
                size = max(0, cast(int, row["VL_FILE_SIZE_KB_HOST"]) or 0)
                if new_type == db_constants['tasks']['file_types']['backup'] and budget is not None:
                    if result["selected_total_kb"] + size > budget:
                        break
                    result["selected_total_kb"] += size
                selected.append(cast(int, row["ID_FILE_TASK"]))
            where_conditions = {
                **where_conditions,
                "ID_FILE_TASK__in": selected,
            }
        if not bounded or selected:
            data = {
                "NU_TYPE": new_type,
                "NU_STATUS": new_status,
                "DT_FILE_TASK": datetime.utcnow(),
                **errors.persisted_error_fields_from_handler(
                    clear_when_empty=True
                ),
            }
            prefix = tools.compose_message(
                new_type, new_status, prefix_only=True
            )
            affected = db.file_task_update_backlog(
                data=data,
                where=where_conditions,
                message_prefix=prefix,
            )
            result["rows_updated"] = affected
            if new_type == db_constants['tasks']['file_types']['backup']:
                result["moved_to_backup"] = affected
            elif new_type == db_constants['tasks']['file_types']['discovery']:
                result["moved_to_discovery"] = affected
            if affected:
                db.host_request_summary_refresh(
                    host_id=host_id, reason="backlog_transition"
                )
    except Exception:
        if owns_transaction:
            try:
                db.rollback()
            except Exception as rollback_error:
                logger.error_event(
                    "db_rollback_failed",
                    operation="backlog_transition",
                    error=type(rollback_error).__name__,
                )
        raise
    # A failed commit may already be durable; do not replay or roll it back blindly.
    if owns_transaction:
        db.commit()
    return result


def _promote_backlog(db: dbHandlerBKP, task: DbRow) -> dict[str, int]:
    """Promote the selected discovery slice into backup work.

    Discovery already created the rows.
    This worker only changes which subset becomes backup work.

    Args:
        db (dbHandlerBKP): Operational persistence session.
        task (DbRow): Worker context with host_id (int) and host_filter (mapping).

    Returns:
        dict[str, int]: rows_updated, moved_to_backup, moved_to_discovery,
        and selected_total_kb for the committed or caller-owned transition.

    Raises:
        ValueError: If prepared predicates, ordering, or limits are invalid.
        psycopg.Error: If locking, writing, or committing fails.
    """
    return _update_backlog(
        db=db,
        logger=log,
        host_id=cast(int, task["host_id"]),
        task_filter=cast(Mapping[str, object], task["host_filter"]),
        search_type=db_constants['tasks']['file_types']['discovery'],
        search_status=db_constants['tasks']['states']['done'],
        new_type=db_constants['tasks']['file_types']['backup'],
        new_status=db_constants['tasks']['states']['pending'],
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
            "NU_TYPE": db_constants['tasks']['host_types']['backlog_control'],
            "NU_STATUS": db_constants['tasks']['states']['pending'],
        },
        NU_STATUS=db_constants['tasks']['states']['done'],
        DT_HOST_TASK=now,
        NA_MESSAGE="Pending backlog promotion canceled by rollback request",
    )


def _rollback_backlog(db: dbHandlerBKP, task: DbRow) -> dict[str, int]:
    """Roll back pending backup work into the discovery queue.

    Rollback is stronger than queued promotion for the same host.
    STOP should win before any new backup work is recreated.

    Args:
        db (dbHandlerBKP): Operational persistence session.
        task (DbRow): Worker context with host_id (int), host_filter (mapping),
            and now (datetime) used when canceling pending promotion requests.

    Returns:
        dict[str, int]: rows_updated, moved_to_backup, moved_to_discovery,
        and selected_total_kb for the committed or caller-owned transition.

    Raises:
        ValueError: If prepared predicates, ordering, or limits are invalid.
        psycopg.Error: If cancellation, locking, writing, or committing fails.
    """
    _cancel_pending_backlog_promotions(
        db,
        host_id=cast(int, task["host_id"]),
        now=cast(datetime, task["now"]),
    )
    return _update_backlog(
        db=db,
        logger=log,
        host_id=cast(int, task["host_id"]),
        task_filter=cast(Mapping[str, object], task["host_filter"]),
        search_type=db_constants['tasks']['file_types']['backup'],
        search_status=db_constants['tasks']['states']['pending'],
        new_type=db_constants['tasks']['file_types']['discovery'],
        new_status=db_constants['tasks']['states']['done'],
    )


def _do_work(db: dbHandlerBKP, task: DbRow) -> DbRow:
    """
    Execute one backlog transition and return a small action summary.

    Promotion and rollback deliberately share the same DB primitive.
    The difference lives only in:
        - source type/status
        - target type/status
        - the pre-step that cancels pending promotion when STOP wins

    Args:
        db (dbHandlerBKP): Operational persistence session.
        task (DbRow): Worker context containing task_id, task_type, host_id (ints),
            host_filter (mapping), and now (datetime) for rollback cancellation.

    Returns:
        DbRow: action (promote/rollback), rows_updated, moved_to_backup,
        moved_to_discovery, and selected_total_kb (integers). Phase timing is
        logged here; main measures the duration used for finalization.

    Raises:
        ValueError: If task_type or prepared persistence arguments are invalid.
        psycopg.Error: If the requested transition fails.
    """
    # The entrypoint measures the full `_do_work()` duration for `task_done`.
    # This function measures only the completed domain phase.
    phase_started_at = time.monotonic()

    match task["task_type"]:
        case _ if task['task_type'] == db_constants['tasks']['host_types']['backlog_control']:
            result = _promote_backlog(db, task)
            action = "promote"

        case _ if task['task_type'] == db_constants['tasks']['host_types']['backlog_rollback']:
            result = _rollback_backlog(db, task)
            action = "rollback"

        case _:
            raise ValueError(
                f"Unsupported backlog task type: {task['task_type']}"
            )

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
        NU_STATUS=db_constants['tasks']['states']['done'],
        NU_PID=db_constants['hosts']['unlocked_pid'],
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
            NU_STATUS=db_constants['tasks']['states']['error'],
            NU_PID=db_constants['hosts']['unlocked_pid'],
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
        return dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log, conninfo=k.PG_CONNINFO)
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
