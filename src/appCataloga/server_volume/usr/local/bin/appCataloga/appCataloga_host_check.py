#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
appCataloga_host_check.py — Host Connectivity and Task State Synchronizer

This microservice verifies host connectivity and ensures HOST/HOST_TASK/FILE_TASK
consistency. It now also handles HOST_TASK_UPDATE_STATISTICS_TYPE, delegating all
statistic computation to db.host_update_statistics().
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
# Signal Handling
# ============================================================
def release_busy_hosts_on_exit() -> None:
    """
    Release all HOST records marked as BUSY by this process PID.

    This function is safe to call multiple times and should never
    interrupt the shutdown flow, even if the database is unavailable.
    """
    try:
        pid = os.getpid()
        log.entry(f"[CLEANUP] Releasing BUSY hosts for PID={pid}")

        # Create a fresh DB handler to avoid relying on partially
        # initialized or corrupted state during shutdown
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)

        # Clear BUSY flag for all HOST rows locked by this PID
        db.host_release_by_pid(pid)

    except Exception as e:
        # Cleanup must never break process termination
        log.error(f"[CLEANUP] Failed to release BUSY hosts: {e}")


def sigterm_handler(signal=None, frame=None) -> None:
    """
    Handle SIGTERM (graceful shutdown signal).

    This signal is typically sent by:
    - kill <pid>
    - pkill
    - service stop scripts
    """
    global process_status, log

    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"SIGTERM received at: {current_function}()")

    # Stop the main loop gracefully
    process_status["running"] = False

    # Release any HOST records locked by this process
    release_busy_hosts_on_exit()


def sigint_handler(signal=None, frame=None) -> None:
    """
    Handle SIGINT (interactive interrupt signal).

    This signal is typically sent by:
    - Ctrl+C in an attached terminal
    """
    global process_status, log

    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"SIGINT received at: {current_function}()")

    # Stop the main loop gracefully
    process_status["running"] = False

    # Release any HOST records locked by this process
    release_busy_hosts_on_exit()


# Register signal handlers for graceful shutdown
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


# ============================================================
# Connectivity helper
# ============================================================

def is_host_online(host_addr: str) -> bool:
    try:
        return ping(host_addr, timeout=k.ICMP_TIMEOUT_SEC) is not None
    except Exception:
        return False


# ============================================================
# MAIN
# ============================================================
def main():
    log.entry("[INIT] Host check microservice started.")
    last_host_cleanup = datetime.min

    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Failed to initialize DB: {e}")
        sys.exit(1)

    while process_status["running"]:

        err = errors.ErrorHandler(log)

        try:
            # ====================================================
            # Fetch HOST_TASK (HOST CHECK or HOST UPDATE_STATISTICS)
            # ====================================================
            task = db.host_task_read(
                task_status=k.TASK_PENDING,
                task_type=[k.HOST_TASK_CHECK_TYPE, k.HOST_TASK_UPDATE_STATISTICS_TYPE, k.HOST_TASK_CHECK_CONNECTION_TYPE],
            )

            if not task:
                # Check for stale BUSY hosts and release them if needed
                # In this case if IS_BUSY = TRUE but there arent FILE_TASK or HOST_TASK with status RUNNING, 
                # we can assume the host is stuck and release it if DT_BUSY > 
                now = datetime.now()
                if now - last_host_cleanup > timedelta(seconds=k.HOST_CLEANUP_INTERVAL):
                    try:
                        db.host_cleanup_stale_locks(threshold_seconds=k.HOST_CLEANUP_INTERVAL)
                    except Exception as e:
                        log.error(f"[HOST_CLEANUP] Failed: {e}")

                    last_host_cleanup = now
                legacy._random_jitter_sleep()
                continue
            
            # Tasks contents
            host_id   = task["HOST__ID_HOST"]
            task_id   = task["HOST_TASK__ID_HOST_TASK"]
            task_type = task["HOST_TASK__NU_TYPE"]
            addr      = task["HOST__NA_HOST_ADDRESS"]
            port      = task["HOST__NA_HOST_PORT"]
            now       = datetime.now()

            # ====================================================
            # COMMON — Lock this task (avoid other workers)
            # ====================================================
            try:
                db.host_task_update(
                    task_id=task_id,
                    NU_STATUS=k.TASK_RUNNING,
                    NU_PID=os.getpid(),
                )
            except Exception as e:
                err.set("Failed to lock task", "LOCK_TASK", e)

            if err.triggered:
                # Skip special-case cleanup: centralized handler below
                pass

            # ====================================================
            # CASE 1 — HOST_TASK_CHECK_TYPE
            # ====================================================
            if not err.triggered and task_type == k.HOST_TASK_CHECK_TYPE:

                # Connectivity test
                try:
                    online = is_host_online(addr)
                    log.entry(f"[CHECK] Host {addr}:{port} → "
                              f"{'ONLINE' if online else 'OFFLINE'}")
                except Exception as e:
                    err.set("Connectivity test failed", "CONNECTIVITY", e)

                # DB update logic
                if not err.triggered:
                    try:
                        if not online:
                            # Host unreachable: mark offline + suspend tasks
                            db.host_update(
                                host_id=host_id,
                                IS_OFFLINE=True,
                                IS_BUSY=False,
                                NU_PID=0,
                                NU_HOST_CHECK_ERROR=1,
                                DT_LAST_FAIL=now,
                                DT_LAST_CHECK=now,
                            )
                            db.host_task_suspend_by_host(host_id)
                            db.file_task_suspend_by_host(host_id)

                            # Persist error instead of deleting
                            db.host_task_update(
                                task_id=task_id,
                                NU_STATUS=k.TASK_ERROR,
                                NA_MESSAGE="Host unreachable (connectivity check failed)",
                                DT_HOST_TASK=now,
                            )

                        else:
                            # Host reachable → reset flags, resume tasks
                            db.host_update(
                                host_id=host_id,
                                IS_OFFLINE=False,
                                check_busy_timeout=True,
                                DT_LAST_CHECK=now,
                            )

                            # Resume suspended tasks
                            db.host_task_resume_by_host(host_id)
                            db.file_task_resume_by_host(host_id)
                            db.file_history_resume_by_host(host_id)

                            # Promote CHECK → PROCESSING (discovery cycle)
                            db.host_task_update(
                                task_id=task_id,
                                NU_TYPE=k.HOST_TASK_PROCESSING_TYPE,
                                NU_STATUS=k.TASK_PENDING
                            )

                    except Exception as e:
                        err.set("DB transaction failed", "TRANSACTION", e)

            # ====================================================
            # CASE 2 — HOST_TASK_UPDATE_STATISTICS_TYPE
            # ====================================================
            if not err.triggered and task_type == k.HOST_TASK_UPDATE_STATISTICS_TYPE:
                
                try:
                    online = is_host_online(addr)
                    log.entry(f"[CHECK] Host {addr}:{port} → "
                              f"{'ONLINE' if online else 'OFFLINE'}")
                except Exception as e:
                    err.set("Connectivity test failed", "CONNECTIVITY", e)
                try:
                    
                    # Perform statistics update
                    db.host_update_statistics(host_id=host_id)

                    # Delete statistics task
                    db.host_task_delete(task_id=task_id)

                except Exception as e:
                    err.set("Statistics update failed", "UPDATE_STATS", e)
            
            # ====================================================
            # CASE 3 — HOST_TASK_CHECK_CONNECTION
            # ====================================================
            if not err.triggered and task_type == k.HOST_TASK_CHECK_CONNECTION_TYPE:
                
                # Connectivity test
                try:
                    online = is_host_online(addr)
                    log.entry(f"[CHECK_CONNECTION] Host {addr}:{port} → "
                              f"{'ONLINE' if online else 'OFFLINE'}")
                except Exception as e:
                    err.set("Connectivity test failed", "CONNECTIVITY", e)

                # DB update logic
                if not err.triggered:
                    try:
                        if not online:
                            # Host unreachable: mark offline + suspend tasks
                            db.host_update(
                                host_id=host_id,
                                IS_OFFLINE=True,
                                IS_BUSY=False,
                                NU_PID=0,
                                NU_HOST_CHECK_ERROR=1,
                                DT_LAST_FAIL=now,
                                DT_LAST_CHECK=now,
                            )
                            db.host_task_suspend_by_host(host_id)
                            db.file_task_suspend_by_host(host_id)

                            # Persist error instead of deleting
                            db.host_task_update(
                                task_id=task_id,
                                NU_STATUS=k.TASK_ERROR,
                                NA_MESSAGE="Host unreachable (connectivity check failed)",
                                DT_HOST_TASK=now,
                            )

                        else:
                            # Host reachable → reset flags, resume tasks
                            db.host_update(
                                host_id=host_id,
                                IS_OFFLINE=False,
                                check_busy_timeout=True,
                                DT_LAST_CHECK=now,
                            )

                            # Resume suspended tasks
                            db.host_task_resume_by_host(host_id)
                            db.file_task_resume_by_host(host_id)
                            db.file_history_resume_by_host(host_id)

                            # Promote CHECK → PROCESSING (discovery cycle)
                            db.host_task_delete(task_id=task_id)

                    except Exception as e:
                        err.set("DB transaction failed", "TRANSACTION", e)
                

            # ====================================================
            # ERROR HANDLING (centralized)
            # ====================================================
            if err.triggered:
                err.log_error(host_id=host_id, task_id=task_id)

                try:
                    # Persist error state instead of deleting the task
                    db.host_task_update(
                        task_id=task_id,
                        NU_STATUS=k.TASK_ERROR,
                        NA_MESSAGE=err.msg,
                        DT_HOST_TASK=datetime.now(),
                    )
                except Exception as e2:
                    log.error(f"[DB-ERROR] Failed to persist HOST_TASK error: {e2}")

                legacy._random_jitter_sleep()
                continue


            # ====================================================
            # Normal idle jitter
            # ====================================================
            legacy._random_jitter_sleep()

        except Exception as e:
            log.error(f"[MAIN] Unexpected: {e}")
            legacy._random_jitter_sleep()

    log.entry("[STOP] Host check microservice stopped.")

        

# ============================================================
# Entrypoint
# ============================================================
if __name__ == "__main__":
    main()
