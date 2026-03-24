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
import inspect
import signal
from datetime import datetime

# =================================================
# PROJECT ROOT (shared/, db/, stations/)
# =================================================
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# =================================================
# Config directory (etc/appCataloga)
# =================================================
_CFG_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga")
)
if _CFG_DIR not in sys.path and os.path.isdir(_CFG_DIR):
    sys.path.append(_CFG_DIR)

# =================================================
# DB directory
# =================================================
_DB_DIR = os.path.join(PROJECT_ROOT, "db")
if _DB_DIR not in sys.path and os.path.isdir(_DB_DIR):
    sys.path.append(_DB_DIR)

# Import customized libs
from db.dbHandlerBKP import dbHandlerBKP
from shared import errors, host_connectivity, legacy, logging_utils
import config as k


# ============================================================
# Globals
# ============================================================
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
def release_busy_hosts_on_exit() -> None:
    """
    Release all HOST records marked as BUSY by this process PID.

    This function is safe to call multiple times and should never
    interrupt the shutdown flow, even if the database is unavailable.
    """
    try:
        pid = os.getpid()
        log.event("cleanup_busy_hosts", pid=pid)

        # Create a fresh DB handler to avoid relying on partially
        # initialized or corrupted state during shutdown
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)

        # Clear BUSY flag for all HOST rows locked by this PID
        db.host_release_by_pid(pid)

    except Exception as e:
        log.error(f"event=cleanup_busy_hosts_failed error={e}")


def _signal_handler(signal_name: str) -> None:
    """
    Register shutdown intent and release BUSY resources.
    """
    current_function = inspect.currentframe().f_back.f_code.co_name
    log.signal_received(signal_name, handler=current_function)
    process_status["running"] = False
    release_busy_hosts_on_exit()


def sigterm_handler(signal=None, frame=None) -> None:
    """
    Handle SIGTERM by requesting a graceful shutdown.
    """
    _signal_handler("SIGTERM")


def sigint_handler(signal=None, frame=None) -> None:
    """
    Handle SIGINT by requesting a graceful shutdown.
    """
    _signal_handler("SIGINT")


# Register signal handlers for graceful shutdown
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


def _read_next_host_task(db: dbHandlerBKP) -> dict | None:
    """Return the next HOST_TASK according to the fixed worker priority."""
    for task_type in HOST_TASK_PRIORITY:
        task = db.host_task_read(
            task_status=k.TASK_PENDING,
            task_type=task_type,
        )
        if task:
            return task
    return None


def _build_task_context(task_row: dict) -> dict:
    """Extract the HOST/HOST_TASK fields used by this loop iteration."""
    return {
        "host_id": task_row["HOST__ID_HOST"],
        "task_id": task_row["HOST_TASK__ID_HOST_TASK"],
        "task_type": task_row["HOST_TASK__NU_TYPE"],
        "addr": task_row["HOST__NA_HOST_ADDRESS"],
        "port": task_row["HOST__NA_HOST_PORT"],
        "user": task_row["HOST__NA_HOST_USER"],
        "password": task_row["HOST__NA_HOST_PASSWORD"],
        "was_offline": bool(task_row.get("HOST__IS_OFFLINE")),
        "host_check_error_count": int(task_row.get("HOST__NU_HOST_CHECK_ERROR") or 0),
        "now": datetime.now(),
    }


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
    )

    if lock_result["rows_affected"] == 1:
        return True

    log.warning(
        f"event=host_task_claim_race host_id={task['host_id']} task_id={task['task_id']}"
    )
    return False


def check_host_connectivity(
    host_id: int,
    addr: str,
    port: int,
    user: str,
    password: str,
    event_name: str,
) -> dict:
    """
    Run the shared operational connectivity probe and emit one structured log.

    A host is considered operationally online for discovery/backup only when:
        1. ICMP responds
        2. the short SSH confirmation probe succeeds
    """
    probe = host_connectivity.probe_host_operational_connectivity(
        addr=addr,
        port=port,
        user=user,
        password=password,
    )
    online = probe["state"] == "online"

    log.event(
        event_name,
        host_id=host_id,
        address=addr,
        port=port,
        state=probe["state"],
        reason=probe["reason"],
        online=online,
        icmp_online=probe["icmp_online"],
        ssh_online=probe["ssh_online"],
        error=probe["error"],
    )
    return probe


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
        DT_LAST_CHECK=now,
        DT_LAST_FAIL=now,
        NU_HOST_CHECK_ERROR=next_error_count,
    )

    if next_error_count >= threshold:
        db.host_task_update(
            task_id=task_id,
            NU_STATUS=k.TASK_ERROR,
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
        DT_HOST_TASK=now,
        NA_MESSAGE=(
            "SSH supervision degraded while ICMP still responds | "
            f"confirmation {next_error_count}/{threshold}"
        ),
    )


def finalize_connectivity_host_task(
    db: dbHandlerBKP,
    task_id: int,
    host_id: int,
    was_offline: bool,
    online: bool,
    now: datetime,
    promote_to_processing: bool,
) -> None:
    """
    Apply the final result of a queued connectivity task.

    CHECK and CHECK_CONNECTION share the same failure path. They differ only on
    success: CHECK becomes PROCESSING, while CHECK_CONNECTION is consumed as a
    one-off reconciliation task.
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
    )

    # Both connectivity task types fail the same way. They differ only on the
    # success path: bootstrap discovery or finish a one-off reconciliation.
    if not online:
        db.host_task_update(
            task_id=task_id,
            NU_STATUS=k.TASK_ERROR,
            NA_MESSAGE="Host unreachable (connectivity check failed)",
            DT_HOST_TASK=now,
        )
        return

    if promote_to_processing:
        db.host_task_update(
            task_id=task_id,
            NU_TYPE=k.HOST_TASK_PROCESSING_TYPE,
            NU_STATUS=k.TASK_PENDING
        )
        return

    db.host_task_delete(task_id=task_id)


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
        - CHECK becomes PROCESSING
        - CHECK_CONNECTION is consumed immediately
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
            handle_degraded_connectivity_task(
                db=db,
                task_id=task["task_id"],
                host_id=task["host_id"],
                current_error_count=task["host_check_error_count"],
                now=task["now"],
            )
            return

        if state == "auth_error":
            err.set(
                "SSH authentication failed during connectivity confirmation",
                "AUTH",
                RuntimeError(connectivity["error"] or connectivity["reason"]),
            )
            return

        finalize_connectivity_host_task(
            db=db,
            task_id=task["task_id"],
            host_id=task["host_id"],
            was_offline=task["was_offline"],
            online=(state == "online"),
            now=task["now"],
            promote_to_processing=promote_to_processing,
        )
    except Exception as e:
        err.set("DB transaction failed", "TRANSACTION", e)


def _process_statistics_task(
    db: dbHandlerBKP,
    task: dict,
    err: errors.ErrorHandler,
) -> None:
    """Execute the deferred host statistics refresh task."""
    try:
        db.host_update_statistics(host_id=task["host_id"])
        db.host_task_update(
            task_id=task["task_id"],
            NU_STATUS=k.TASK_DONE,
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
def main():
    """
    Run the queued HOST_TASK worker until shutdown is requested.

    Task priority is intentional:
        1. CHECK
        2. CHECK_CONNECTION
        3. UPDATE_STATISTICS

    That ordering keeps fresh discovery bootstrap work ahead of reconciliation
    and statistics refresh.
    """
    log.service_start("appCataloga_host_check")

    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"event=db_init_failed service=appCataloga_host_check error={e}")
        sys.exit(1)

    while process_status["running"]:

        err = errors.ErrorHandler(log)
        task = None

        try:
            task_row = _read_next_host_task(db)
            if not task_row:
                legacy._random_jitter_sleep()
                continue

            task = _build_task_context(task_row)

            try:
                if not _claim_host_task(db, task):
                    continue
            except Exception as e:
                err.set("Failed to lock task", "LOCK_TASK", e)

            if not err.triggered:
                match task["task_type"]:
                    #--------------------------------------------------
                    #CASE 1: UPDATE STATISTICS
                    #--------------------------------------------------
                    case k.HOST_TASK_UPDATE_STATISTICS_TYPE:
                        _process_statistics_task(db, task, err)
                    #--------------------------------------------------
                    #CASE 2: RECEIVED FROM APPCATALOGA.PY AND SEND TO DISCOVERY
                    #--------------------------------------------------
                    case k.HOST_TASK_CHECK_TYPE:
                        _process_connectivity_task(
                            db=db,
                            task=task,
                            event_name="host_check",
                            promote_to_processing=True,
                            err=err,
                        )
                    #--------------------------------------------------
                    #CASE 3: RECEIVED FROM WORKERS TO CHECK CONNECTIVITY DUE TO CONNECTION AMBIGUITY
                    #--------------------------------------------------
                    case k.HOST_TASK_CHECK_CONNECTION_TYPE:
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

            # ====================================================
            # ERROR HANDLING (centralized)
            # ====================================================
            if err.triggered:
                err.log_error(host_id=task["host_id"], task_id=task["task_id"])

                try:
                    # Host-check failures now persist as a stable generic
                    # prefix plus the canonical ErrorHandler payload so they
                    # can be read directly and aggregated later if needed.
                    db.host_task_update(
                        task_id=task["task_id"],
                        NU_STATUS=k.TASK_ERROR,
                        NA_MESSAGE=f"Host Check Error | {err.format_error()}",
                        DT_HOST_TASK=datetime.now(),
                    )
                except Exception as e2:
                    log.error(f"event=host_task_error_persist_failed error={e2}")

                legacy._random_jitter_sleep()
                continue


            # ====================================================
            # Normal idle jitter
            # ====================================================
            legacy._random_jitter_sleep()

        except Exception as e:
            if not err.triggered:
                err.capture(
                    reason="Unexpected host check loop failure",
                    stage="MAIN",
                    exc=e,
                    host_id=task["host_id"] if task else None,
                    task_id=task["task_id"] if task else None,
                )
            err.log_error(
                host_id=task["host_id"] if task else None,
                task_id=task["task_id"] if task else None,
            )
            legacy._random_jitter_sleep()

    log.service_stop("appCataloga_host_check")

        

# ============================================================
# Entrypoint
# ============================================================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal host check worker crash",
            stage="MAIN",
            exc=e,
        )
        err.log_error()
        release_busy_hosts_on_exit()
        raise
