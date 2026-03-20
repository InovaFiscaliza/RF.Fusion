#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Host connectivity and task-state synchronizer.

This worker verifies host reachability, keeps HOST/HOST_TASK/FILE_TASK state
consistent, and also processes deferred host-statistics refresh tasks. The flow
stays intentionally explicit because connectivity failures directly affect queue
resume/suspend behavior.
"""

import sys
import os
import socket
import inspect
import signal
from datetime import datetime, timedelta

from ping3 import ping

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
from shared import errors, legacy, logging_utils
import config as k


# ============================================================
# Globals
# ============================================================
log = logging_utils.log()
process_status = {"running": True}


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


# ============================================================
# Connectivity helper
# ============================================================

def is_host_online(host_addr: str, timeout_sec=None) -> bool:
    """
    Check host reachability through ICMP without surfacing ping library errors.
    """
    timeout = k.ICMP_TIMEOUT_SEC if timeout_sec is None else timeout_sec
    try:
        return ping(host_addr, timeout=timeout) is not None
    except Exception:
        return False


def apply_host_connectivity_state(
    db: dbHandlerBKP,
    host_id: int,
    was_offline: bool,
    online: bool,
    now: datetime,
) -> None:
    """
    Persist the latest connectivity snapshot and trigger side effects only
    when the host actually changes state.
    """
    # online -> online:
    # Refresh the heartbeat only. No task churn should happen here.
    #
    # offline -> online:
    # Refresh the heartbeat and reopen work that was suspended while the
    # host was considered unreachable.
    if online:
        db.host_update(
            host_id=host_id,
            IS_OFFLINE=False,
            check_busy_timeout=True,
            DT_LAST_CHECK=now,
        )

        # Only a real offline -> online edge should reopen suspended work.
        if was_offline:
            log.event(
                "host_state_transition",
                host_id=host_id,
                previous_state="offline",
                current_state="online",
            )
            db.host_task_resume_by_host(host_id)
            db.file_task_resume_by_host(host_id)
            db.file_history_resume_by_host(host_id)

        return

    update_fields = {
        "IS_OFFLINE": True,
        "DT_LAST_CHECK": now,
    }

    # offline -> offline:
    # Keep refreshing the last failed heartbeat, but do not keep suspending
    # tasks or unlocking the host over and over again.
    #
    # online -> offline:
    # This is the important edge. Suspend host-dependent work first, then
    # release the BUSY flag so a new worker does not enter the same host in
    # the transition window.
    if not was_offline:
        log.event(
            "host_state_transition",
            host_id=host_id,
            previous_state="online",
            current_state="offline",
        )
        db.host_task_suspend_by_host(host_id)
        db.file_task_suspend_by_host(host_id)
        db.file_history_suspend_by_host(host_id)

        # Suspend host-dependent work before releasing the BUSY flag so
        # another worker does not claim fresh tasks in the transition window.
        update_fields.update(
            IS_BUSY=False,
            NU_PID=k.HOST_UNLOCKED_PID,
            NU_HOST_CHECK_ERROR=1,
            DT_LAST_FAIL=now,
        )

    db.host_update(host_id=host_id, **update_fields)


def check_host_connectivity(
    host_id: int,
    addr: str,
    port: int,
    event_name: str,
) -> bool:
    """
    Run an ICMP connectivity check and emit the corresponding structured log.
    """
    online = is_host_online(addr)
    log.event(
        event_name,
        host_id=host_id,
        address=addr,
        port=port,
        online=online,
    )
    return online


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
    Persist the connectivity result for HOST_TASK_CHECK and
    HOST_TASK_CHECK_CONNECTION tasks.
    """
    apply_host_connectivity_state(
        db=db,
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


def run_host_check_all_batch(db: dbHandlerBKP, now: datetime) -> int:
    """
    Refresh a small batch of stale host connectivity snapshots outside the
    HOST_TASK queue.
    """
    if not k.HOST_CHECK_ALL_ENABLED:
        return 0

    hosts = db.host_list_for_connectivity_check()
    if not hosts:
        return 0

    stale_after = timedelta(seconds=k.HOST_CHECK_ALL_STALE_AFTER_SEC)
    due_hosts = []

    for host in hosts:
        last_check = host.get("DT_LAST_CHECK")

        # The list is ordered from oldest to newest snapshot.
        if last_check and (now - last_check) < stale_after:
            break

        due_hosts.append(host)

        if len(due_hosts) >= k.HOST_CHECK_ALL_BATCH_SIZE:
            break

    if not due_hosts:
        return 0

    log.event(
        "host_check_all_batch",
        batch_size=len(due_hosts),
        stale_after_sec=k.HOST_CHECK_ALL_STALE_AFTER_SEC,
        timeout_sec=k.HOST_CHECK_ALL_ICMP_TIMEOUT_SEC,
    )

    checked = 0

    for host in due_hosts:
        # Shutdown should interrupt the sweep immediately.
        if not process_status["running"]:
            break

        host_id = host["ID_HOST"]
        addr = host["NA_HOST_ADDRESS"]
        host_name = host.get("NA_HOST_NAME")
        was_offline = bool(host.get("IS_OFFLINE"))

        try:
            online = is_host_online(
                addr,
                timeout_sec=k.HOST_CHECK_ALL_ICMP_TIMEOUT_SEC,
            )
            checked_at = datetime.now()

            log.event(
                "host_check_all",
                host_id=host_id,
                host=host_name,
                address=addr,
                online=online,
            )

            apply_host_connectivity_state(
                db=db,
                host_id=host_id,
                was_offline=was_offline,
                online=online,
                now=checked_at,
            )
            checked += 1

        except Exception as e:
            log.error(
                f"event=host_check_all_failed host_id={host_id} "
                f"host={host_name} address={addr} error={e}"
            )

    if checked:
        log.event("host_check_all_done", checked=checked)

    return checked


# ============================================================
# MAIN
# ============================================================
def main():
    """
    Run the host-check polling loop until shutdown is requested.
    """
    log.service_start("appCataloga_host_check")
    last_host_cleanup = datetime.min

    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"event=db_init_failed service=appCataloga_host_check error={e}")
        sys.exit(1)

    while process_status["running"]:

        err = errors.ErrorHandler(log)

        try:
            # ====================================================
            # Fetch operational HOST_TASK first.
            # ====================================================
            task = db.host_task_read(
                task_status=k.TASK_PENDING,
                task_type=[
                    k.HOST_TASK_CHECK_TYPE,
                    k.HOST_TASK_CHECK_CONNECTION_TYPE,
                ],
            )

            if not task:
                # Statistics refresh is intentionally lower priority than
                # connectivity-related work so it does not delay discovery flow.
                task = db.host_task_read(
                    task_status=k.TASK_PENDING,
                    task_type=k.HOST_TASK_UPDATE_STATISTICS_TYPE,
                )

            if not task:
                # The idle branch owns maintenance work only. Any operational
                # HOST_TASK should preempt cleanup and periodic sweeps.
                # Check for stale BUSY hosts and release them if needed
                # In this case if IS_BUSY = TRUE but there arent FILE_TASK or HOST_TASK with status RUNNING, 
                # we can assume the host is stuck and release it if DT_BUSY > 
                now = datetime.now()
                if now - last_host_cleanup > timedelta(seconds=k.HOST_CLEANUP_INTERVAL):
                    try:
                        db.host_cleanup_stale_locks(
                            threshold_seconds=k.HOST_BUSY_TIMEOUT
                        )
                    except Exception as e:
                        log.error(f"event=host_cleanup_failed error={e}")

                    last_host_cleanup = now

                try:
                    run_host_check_all_batch(db=db, now=now)
                except Exception as e:
                    log.error(f"event=host_check_all_batch_failed error={e}")

                legacy._random_jitter_sleep()
                continue
            
            # Tasks contents
            host_id   = task["HOST__ID_HOST"]
            task_id   = task["HOST_TASK__ID_HOST_TASK"]
            task_type = task["HOST_TASK__NU_TYPE"]
            addr      = task["HOST__NA_HOST_ADDRESS"]
            port      = task["HOST__NA_HOST_PORT"]
            was_offline = bool(task.get("HOST__IS_OFFLINE"))
            now       = datetime.now()

            # ====================================================
            # COMMON — Lock this task (avoid other workers)
            # ====================================================
            # HOST_CHECK / CHECK_CONNECTION / UPDATE_STATISTICS tasks are
            # claimed atomically as tasks, but they do NOT lock the host
            # itself. They are observational/maintenance work and should not
            # block discovery or backup from using the same host.
            try:
                lock_result = db.host_task_update(
                    task_id=task_id,
                    expected_status=k.TASK_PENDING,
                    NU_STATUS=k.TASK_RUNNING,
                    NU_PID=os.getpid(),
                )

                if lock_result["rows_affected"] != 1:
                    log.warning(
                        f"event=host_task_claim_race host_id={host_id} task_id={task_id}"
                    )
                    continue
            except Exception as e:
                err.set("Failed to lock task", "LOCK_TASK", e)

            if err.triggered:
                # Skip special-case cleanup: centralized handler below
                pass

            # ====================================================
            # CASE 1 — HOST_TASK_CHECK_TYPE
            # ====================================================
            if not err.triggered and task_type == k.HOST_TASK_CHECK_TYPE:

                try:
                    online = check_host_connectivity(
                        host_id=host_id,
                        addr=addr,
                        port=port,
                        event_name="host_check",
                    )
                except Exception as e:
                    err.set("Connectivity test failed", "CONNECTIVITY", e)

                if not err.triggered:
                    try:
                        # A successful CHECK task becomes PROCESSING so
                        # discovery can claim the host next.
                        finalize_connectivity_host_task(
                            db=db,
                            task_id=task_id,
                            host_id=host_id,
                            was_offline=was_offline,
                            online=online,
                            now=now,
                            promote_to_processing=True,
                        )
                    except Exception as e:
                        err.set("DB transaction failed", "TRANSACTION", e)

            # ====================================================
            # CASE 2 — HOST_TASK_UPDATE_STATISTICS_TYPE
            # ====================================================
            if not err.triggered and task_type == k.HOST_TASK_UPDATE_STATISTICS_TYPE:
                
                try:
                    # Statistics come from FILE_TASK_HISTORY; connectivity here
                    # is only a diagnostic breadcrumb in the logs.
                    online = is_host_online(addr)
                    log.event(
                        "host_check_statistics",
                        host_id=host_id,
                        address=addr,
                        port=port,
                        online=online,
                    )
                except Exception as e:
                    err.set("Connectivity test failed", "CONNECTIVITY", e)
                try:
                    
                    # Perform statistics update
                    db.host_update_statistics(host_id=host_id)

                    # Keep a singleton statistics HOST_TASK row per host and
                    # recycle it, instead of generating insert/delete churn.
                    db.host_task_update(
                        task_id=task_id,
                        NU_STATUS=k.TASK_DONE,
                        DT_HOST_TASK=now,
                        NA_MESSAGE=(
                            f"Host statistics refreshed successfully for host {host_id}"
                        ),
                    )

                except Exception as e:
                    err.set("Statistics update failed", "UPDATE_STATS", e)
            
            # ====================================================
            # CASE 3 — HOST_TASK_CHECK_CONNECTION
            # ====================================================
            if not err.triggered and task_type == k.HOST_TASK_CHECK_CONNECTION_TYPE:

                try:
                    online = check_host_connectivity(
                        host_id=host_id,
                        addr=addr,
                        port=port,
                        event_name="host_check_connection",
                    )
                except Exception as e:
                    err.set("Connectivity test failed", "CONNECTIVITY", e)

                if not err.triggered:
                    try:
                        # Connection checks stop here; they only reconcile
                        # host state after another worker observed doubt/failure.
                        finalize_connectivity_host_task(
                            db=db,
                            task_id=task_id,
                            host_id=host_id,
                            was_offline=was_offline,
                            online=online,
                            now=now,
                            promote_to_processing=False,
                        )
                    except Exception as e:
                        err.set("DB transaction failed", "TRANSACTION", e)
                

            # ====================================================
            # ERROR HANDLING (centralized)
            # ====================================================
            if err.triggered:
                err.log_error(host_id=host_id, task_id=task_id)

                try:
                    # Host-check failures now persist as a stable generic
                    # prefix plus the canonical ErrorHandler payload so they
                    # can be read directly and aggregated later if needed.
                    db.host_task_update(
                        task_id=task_id,
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
                    host_id=locals().get("host_id"),
                    task_id=locals().get("task_id"),
                )
            err.log_error(
                host_id=locals().get("host_id"),
                task_id=locals().get("task_id"),
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
