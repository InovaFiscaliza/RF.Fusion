#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Queued HOST_TASK worker for connectivity reconciliation and deferred statistics.

This daemon consumes only queued HOST_TASK rows and never performs background
maintenance on its own. Its responsibilities are intentionally narrow:

    - confirm host connectivity for CHECK / CHECK_CONNECTION tasks
    - promote successful CHECK tasks into PROCESSING
    - execute deferred host-statistics refresh tasks

Recurring reconciliation such as stale-lock cleanup and oldest-first ICMP
sweeps lives in `appCataloga_host_maintenance.py`, which keeps this loop
focused on queue-driven work and easier to reason about during incidents.
"""

import sys
import os
from datetime import datetime

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

# Import customized libs
from db.dbHandlerBKP import dbHandlerBKP
from host_handler import host_connectivity, host_runtime
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import (
    errors,
    logging_utils,
)
import config as k


# ============================================================
# Globals
# ============================================================
SERVICE_NAME = "appCataloga_host_check"
log = logging_utils.log()
process_status = {"running": True}
HOST_TASK_PRIORITY = (
    k.HOST_TASK_CHECK_TYPE,
    k.HOST_TASK_CHECK_CONNECTION_TYPE,
    k.HOST_TASK_UPDATE_STATISTICS_TYPE,
)


# ============================================================
# Signal handling
# ============================================================
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


def _read_next_host_task(db: dbHandlerBKP) -> dict | None:
    """
    Return the next queued HOST_TASK according to this worker's fixed priority.

    Priority is part of the worker contract, not an incidental query detail:
        1. CHECK
        2. CHECK_CONNECTION
        3. UPDATE_STATISTICS

    That ordering keeps fresh bootstrap work ahead of one-off connectivity
    reconciliation and deferred statistics refresh.
    """
    for task_type in HOST_TASK_PRIORITY:
        task = db.host_task_read(
            task_status=k.TASK_PENDING,
            task_type=task_type,
        )
        if task:
            return task
    return None


def _claim_host_task(db: dbHandlerBKP, task: dict) -> bool:
    """
    Atomically flip the queued HOST_TASK from PENDING to RUNNING.

    These supervisory tasks deliberately do not lock `HOST.IS_BUSY`; they
    supervise the host state but must not block discovery/backup data-plane
    work from using the host itself.
    """
    lock_result = db.host_task_update(
        task_id=task["task_id"],
        expected_status=k.TASK_PENDING,
        NU_STATUS=k.TASK_RUNNING,
        NU_PID=os.getpid(),
        DT_HOST_TASK=datetime.now(),
        NA_MESSAGE="Host check task running",
    )

    if lock_result["rows_affected"] == 1:
        return True

    log.warning(
        f"event=host_task_claim_race host_id={task['host_id']} task_id={task['task_id']}"
    )
    return False


def handle_degraded_connectivity_task(
    db: dbHandlerBKP,
    task_id: int,
    host_id: int,
    current_error_count: int,
    now: datetime,
) -> None:
    """
    Handle ambiguous SSH timeouts without immediately flipping host state.

    Any SSH-side supervisory failure while ICMP still responds is treated as
    degraded first. The task can eventually fail, but the host state itself is
    preserved so a short probe does not suspend the entire station incorrectly.
    """
    next_error_count = max(0, int(current_error_count or 0)) + 1
    threshold = k.HOST_CHECK_SSH_TIMEOUT_CONFIRMATIONS

    # Degraded SSH while ICMP is still alive is recorded on the HOST, but it no
    # longer forces an online -> offline transition by itself.
    db.host_update(
        host_id=host_id,
        reset=True,
        DT_LAST_CHECK=now,
        DT_LAST_FAIL=now,
        NU_HOST_CHECK_ERROR=next_error_count,
    )

    if next_error_count >= threshold:
        db.host_task_update(
            task_id=task_id,
            NU_STATUS=k.TASK_ERROR,
            NU_PID=k.HOST_UNLOCKED_PID,
            DT_HOST_TASK=now,
            NA_MESSAGE=(
                "SSH supervision degraded threshold reached while ICMP still "
                f"responds ({next_error_count}/{threshold})"
            ),
        )
        return

    # Before the threshold we keep the supervisory task alive so the next probe
    # can confirm whether the SSH side is truly degraded or just momentarily
    # overloaded.
    db.host_task_update(
        task_id=task_id,
        NU_STATUS=k.TASK_PENDING,
        NU_PID=k.HOST_UNLOCKED_PID,
        DT_HOST_TASK=now,
        NA_MESSAGE=(
            "SSH supervision degraded while ICMP still responds | "
            f"confirmation {next_error_count}/{threshold}"
        ),
    )


def check_host_connectivity(
    *,
    host_id: int,
    addr: str,
    port: int,
    user: str,
    password: str,
    event_name: str,
) -> dict:
    """
    Run the shared connectivity probe and emit one normalized worker log entry.

    Keeping this helper public gives the worker one obvious place to express
    what "a host check" means without forcing tests and callers to duplicate
    probe + log wiring.
    """
    connectivity = host_connectivity.probe_host_connectivity(
        addr=addr,
        port=port,
        user=user,
        password=password,
    )
    host_connectivity.log_connectivity_probe(
        log=log,
        event_name=event_name,
        host_id=host_id,
        addr=addr,
        port=port,
        probe=connectivity,
    )
    return connectivity


def _handle_auth_error_connectivity_task(
    db: dbHandlerBKP,
    task_id: int,
    host_id: int,
    current_error_count: int,
    now: datetime,
    detail: str,
) -> None:
    """
    Suspend host-dependent work after an explicit SSH authentication failure.

    Authentication rejection is not a transient reachability issue. Retries
    from discovery/backup would just keep failing until credentials are fixed,
    so we suspend dependent queues and wait for a later successful operational
    check to resume them.
    """
    next_error_count = max(0, int(current_error_count or 0)) + 1

    db.host_update(
        host_id=host_id,
        reset=True,
        DT_LAST_CHECK=now,
        DT_LAST_FAIL=now,
        NU_HOST_CHECK_ERROR=next_error_count,
    )

    db.host_task_suspend_by_host(host_id)
    db.file_task_suspend_by_host(host_id)
    db.file_history_suspend_by_host(host_id)

    db.host_task_update(
        task_id=task_id,
        NU_STATUS=k.TASK_ERROR,
        NU_PID=k.HOST_UNLOCKED_PID,
        DT_HOST_TASK=now,
        NA_MESSAGE=f"SSH authentication failed during connectivity confirmation | {detail}",
    )

    log.event(
        "host_auth_error_suspended",
        host_id=host_id,
        task_id=task_id,
        error_count=next_error_count,
        detail=detail,
    )


def _finalize_connectivity_task(
    db: dbHandlerBKP,
    task_id: int,
    host_id: int,
    was_offline: bool,
    online: bool,
    now: datetime,
    promote_to_processing: bool,
    host_filter: dict,
    resume_dependent_tasks: bool,
) -> None:
    """
    Apply the final result of a queued connectivity task.

    CHECK and CHECK_CONNECTION share the same failure path. They differ only on
    success:
        - CHECK finishes as DONE and queues the separate PROCESSING row
        - CHECK_CONNECTION finishes as DONE in its own reusable row
    """
    # Connectivity state is updated first so queue side effects (resume/suspend)
    # stay aligned with the final HOST_TASK result written just below.
    host_connectivity.persist_host_connectivity_state(
        db=db,
        log=log,
        host_id=host_id,
        was_offline=was_offline,
        online=online,
        now=now,
        resume_dependent_tasks=resume_dependent_tasks,
    )

    # Both connectivity task types fail the same way. They differ only on the
    # success path: CHECK opens the discovery lane, while CHECK_CONNECTION just
    # records that reconciliation succeeded. In all non-running outcomes we
    # also clear NU_PID so the row no longer looks owned by this worker.
    if not online:
        db.host_task_update(
            task_id=task_id,
            NU_STATUS=k.TASK_ERROR,
            NU_PID=k.HOST_UNLOCKED_PID,
            NA_MESSAGE="Host unreachable (connectivity check failed)",
            DT_HOST_TASK=now,
        )
        return

    if promote_to_processing:
        db.queue_host_task(
            host_id=host_id,
            task_type=k.HOST_TASK_PROCESSING_TYPE,
            task_status=k.TASK_PENDING,
            filter_dict=host_filter,
        )
        db.host_task_update(
            task_id=task_id,
            NU_STATUS=k.TASK_DONE,
            NU_PID=k.HOST_UNLOCKED_PID,
            DT_HOST_TASK=now,
            NA_MESSAGE="Host check completed successfully; separate discovery task queued",
        )
        return

    db.host_task_update(
        task_id=task_id,
        NU_STATUS=k.TASK_DONE,
        NU_PID=k.HOST_UNLOCKED_PID,
        DT_HOST_TASK=now,
        NA_MESSAGE="Host connectivity reconciliation completed successfully",
    )


def _process_connectivity_task(
    db: dbHandlerBKP,
    task: dict,
    *,
    event_name: str,
    promote_to_processing: bool,
    err: errors.ErrorHandler,
) -> None:
    """
    Execute either CHECK or CHECK_CONNECTION with one shared flow.

    The two queued task types only differ on successful completion:
        - CHECK becomes DONE and queues PROCESSING in its own row
        - CHECK_CONNECTION becomes DONE in its own row

    The connectivity state machine itself lives in `host_handler`. This worker
    owns only the queue semantics layered on top of that probe:
        - degraded SSH -> keep or fail the supervisory task
        - auth error   -> suspend dependent work until credentials are fixed
        - online/offline -> persist final host state and finish the task
    """
    try:
        connectivity = check_host_connectivity(
            host_id=task["host_id"],
            addr=task["addr"],
            port=task["port"],
            user=task["user"],
            password=task["password"],
            event_name=event_name,
        )
    except Exception as e:
        err.set("Connectivity test failed", "CONNECTIVITY", e)
        return

    if err.triggered:
        return

    state = connectivity["state"]

    try:
        if state == "degraded":
            # Ambiguous SSH degradation does not immediately suspend the host.
            # Instead, we keep the supervisory task alive until the threshold
            # says "this is persistent enough to treat as a task failure".
            handle_degraded_connectivity_task(
                db=db,
                task_id=task["task_id"],
                host_id=task["host_id"],
                current_error_count=task["host_check_error_count"],
                now=task["now"],
            )
            return

        if state == "auth_error":
            # Explicit credential rejection is operationally different from an
            # offline host: the station is reachable, but continuing to run
            # dependent work would only produce deterministic failures until
            # credentials are corrected.
            _handle_auth_error_connectivity_task(
                db=db,
                task_id=task["task_id"],
                host_id=task["host_id"],
                current_error_count=task["host_check_error_count"],
                now=task["now"],
                detail=connectivity["error"] or connectivity["reason"],
            )
            return

        # At this point the probe gave us a definitive online/offline answer,
        # so the queue can commit the final state transition in one place.
        _finalize_connectivity_task(
            db=db,
            task_id=task["task_id"],
            host_id=task["host_id"],
            was_offline=task["was_offline"],
            online=(state == "online"),
            now=task["now"],
            promote_to_processing=promote_to_processing,
            host_filter=task["host_filter"],
            resume_dependent_tasks=(task["host_check_error_count"] > 0),
        )
    except Exception as e:
        err.set("DB transaction failed", "TRANSACTION", e)


def _dispatch_claimed_task(
    db: dbHandlerBKP,
    task: dict,
    err: errors.ErrorHandler,
) -> None:
    """
    Execute one already-claimed HOST_TASK according to its type.

    Task selection priority belongs to `_read_next_host_task()`. By the time
    we reach this dispatcher, the question is no longer "which task comes
    first?" but "how should this claimed task complete?".
    """
    match task["task_type"]:
        case k.HOST_TASK_UPDATE_STATISTICS_TYPE:
            # Statistics tasks are pure DB refresh work; they do not probe
            # reachability and do not promote into any follow-up queue.
            _process_statistics_task(db, task, err)
        case k.HOST_TASK_CHECK_TYPE:
            # Fresh tasks created by `appCataloga.py` start here. On success
            # they finish the CHECK lane and queue a separate PROCESSING row
            # so discovery can take over in its own microservice slot.
            _process_connectivity_task(
                db=db,
                task=task,
                event_name="host_check",
                promote_to_processing=True,
                err=err,
            )
        case k.HOST_TASK_CHECK_CONNECTION_TYPE:
            # Ambiguous worker-side connectivity failures enqueue this lighter
            # supervisory task. On success there is no promotion; the task is
            # simply consumed as a reconciliation step.
            _process_connectivity_task(
                db=db,
                task=task,
                event_name="host_check_connection",
                promote_to_processing=False,
                err=err,
            )
        case _:
            err.set(
                f"Unsupported HOST_TASK type: {task['task_type']}",
                "TASK_TYPE",
            )


def _persist_task_error(
    db: dbHandlerBKP,
    task: dict,
    err: errors.ErrorHandler,
) -> None:
    """
    Persist the final task error once processing for this iteration failed.

    Error capture may happen deep inside helper functions, but the queue-facing
    consequence is centralized here so every failed HOST_TASK ends in the same
    durable state and message format.
    """
    err.log_error(host_id=task["host_id"], task_id=task["task_id"])

    try:
        db.host_task_update(
            task_id=task["task_id"],
            NU_STATUS=k.TASK_ERROR,
            NU_PID=k.HOST_UNLOCKED_PID,
            NA_MESSAGE=f"Host Check Error | {err.format_error()}",
            DT_HOST_TASK=datetime.now(),
        )
    except Exception as e2:
        log.error(f"event=host_task_error_persist_failed error={e2}")


def _process_statistics_task(
    db: dbHandlerBKP,
    task: dict,
    err: errors.ErrorHandler,
) -> None:
    """
    Execute the deferred host statistics refresh task.

    Statistics refresh is intentionally deferred into the same HOST_TASK queue
    so workers do not recalculate host metadata inline while handling other
    flows such as discovery.
    """
    try:
        db.host_update_statistics(host_id=task["host_id"])
        db.host_task_update(
            task_id=task["task_id"],
            NU_STATUS=k.TASK_DONE,
            NU_PID=k.HOST_UNLOCKED_PID,
            DT_HOST_TASK=task["now"],
            NA_MESSAGE=(
                f"Host statistics refreshed successfully for host {task['host_id']}"
            ),
        )
        log.event(
            "host_statistics_completed",
            host_id=task["host_id"],
            task_id=task["task_id"],
        )
    except Exception as e:
        err.set("Statistics update failed", "UPDATE_STATS", e)


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    """
    Run the queued HOST_TASK worker until shutdown is requested.

    Reading guide:
        1. open the DB dependency once
        2. read the next queued HOST_TASK by fixed priority
        3. claim it atomically
        4. dispatch the claimed task by type
        5. persist failure centrally when anything in that flow breaks
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
            # Phase 1: fetch the next queued supervisory task by the fixed
            # worker priority. When the queue is empty we deliberately jitter
            # instead of hot-polling the table.
            task_row = _read_next_host_task(db)
            if task_row is None:
                runtime_sleep.random_jitter_sleep()
                continue

            # Trim the raw HOST/HOST_TASK join to the fields this worker
            # actually reasons about during this loop iteration.
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

            try:
                # Phase 2: turn the queued row into the single task owned by
                # this process iteration. A claim race is normal when several
                # host-check workers poll together; losing the race is not an
                # error and just means another worker got there first.
                if not _claim_host_task(db, task):
                    runtime_sleep.random_jitter_sleep()
                    continue
            except Exception as e:
                err.set("Failed to lock task", "LOCK_TASK", e)

            if task is not None and not err.triggered:
                # Phase 3: the task is now ours; dispatch the claimed work by
                # type and let deeper helpers decide the exact queue outcome.
                _dispatch_claimed_task(db, task, err)

            if task is not None and err.triggered:
                # Phase 4: any failure captured during claim or execution is
                # normalized here into the durable HOST_TASK error state.
                _persist_task_error(db, task, err)

            # Even after successful work we keep the same small jitter so many
            # workers do not slam the queue again in perfect lockstep.
            runtime_sleep.random_jitter_sleep()

        except Exception as e:
            if not err.triggered:
                err.capture(
                    reason="Unexpected host check loop failure",
                    stage="MAIN",
                    exc=e,
                    host_id=task["host_id"] if task else None,
                    task_id=task["task_id"] if task else None,
                )
            if task is not None:
                _persist_task_error(db, task, err)
            else:
                err.log_error(
                    host_id=task["host_id"] if task else None,
                    task_id=task["task_id"] if task else None,
                )
            runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME)

        

# ============================================================
# Entrypoint
# ============================================================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # This is the true last-resort crash path for the daemon process. The
        # worker loop already handles per-iteration failures internally; if we
        # reach this block, the process itself is no longer trustworthy.
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal host check worker crash",
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
