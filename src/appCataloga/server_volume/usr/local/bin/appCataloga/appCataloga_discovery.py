#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
appCataloga_discovery.py — Discovery microservice for appCataloga ecosystem.

This daemon scans remote hosts for candidate files to back up, creating new
FILE_TASK entries and updating the backlog accordingly. It ensures that each
host filesystem is accessed safely using HALT_FLAG control, and that tasks are
only created or moved when the host is reachable.

Key principles:
    - Direct, linear control flow (no hidden subroutines).
    - Explicit error handling and resource cleanup.
    - Full integration with the new host_update(**kwargs) format.
    - Clear, English technical comments and docstrings.
"""

from __future__ import annotations

import os
import sys
import time
import json
import signal
import traceback
import subprocess
import paramiko
import inspect
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# ----------------------------------------------------------------------
# Path setup
# ----------------------------------------------------------------------
_CFG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga"))
if _CFG_DIR not in sys.path and os.path.isdir(_CFG_DIR):
    sys.path.append(_CFG_DIR)

_DB_DIR = os.path.join(os.path.dirname(__file__), "db")
if _DB_DIR not in sys.path and os.path.isdir(_DB_DIR):
    sys.path.append(_DB_DIR)

# ----------------------------------------------------------------------
# Local imports
# ----------------------------------------------------------------------
import shared as sh
from shared import Filter
from db.dbHandlerBKP import dbHandlerBKP
import config as k


# ======================================================================
# Globals
# ======================================================================
log = sh.log()
process_status = {"running": True}
_DAEMON_REGISTRY: List[Any] = []


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


# ======================================================================
# Main daemon loop
# ======================================================================
def main() -> None:
    """Main loop for the discovery microservice."""

    log.entry("[INIT] appCataloga_discovery microservice started.")

    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Failed to initialize database: {e}")
        sys.exit(1)

    # ===============================================================
    # MAIN LOOP
    # ===============================================================
    while process_status["running"]:

        daemon = None
        sftp = None
        err = sh.ErrorHandler(log)
        task = None
        host_id = None
        task_id = None
        fatal_error = False   # FIX: global flag to ensure final cleanup always happens

        try:
            # ==========================================================
            # ACT I — Fetch next HOST_TASK (PROCESSING pending)
            # ==========================================================
            task = db.host_task_read(
                task_type=k.HOST_TASK_PROCESSING_TYPE,
                task_status=k.TASK_PENDING,
                check_host_busy=True,
            )

            if not task:
                sh._random_jitter_sleep()
                continue

            host_id = task["HOST__ID_HOST"]
            task_id = task["HOST_TASK__ID_HOST_TASK"]

            # ==========================================================
            # ACT II — Lock HOST and TASK
            # ==========================================================
            try:
                db.host_update(
                    host_id=host_id,
                    IS_BUSY=True,
                    DT_BUSY=datetime.now(),
                    NU_PID=os.getpid(),
                )
                db.host_task_update(
                    task_id=task_id,
                    NU_STATUS=k.TASK_RUNNING,
                    NU_PID=os.getpid(),
                    DT_HOST_TASK=datetime.now(),
                )

            except Exception as e:
                err.set("Failed to lock HOST or HOST_TASK", "LOCK_TASK", e)

            # FIX:
            # Never continue inside this try-block.
            # If an error is triggered, mark fatal_error and allow main try...finally to run.
            if err.triggered:
                fatal_error = True

            # ==========================================================
            # ACT III — Init SFTP + HostDaemon
            # ==========================================================
            if not err.triggered:
                try:
                    sftp, daemon = sh.init_host_context(task, log)

                except paramiko.AuthenticationException as e:
                    err.set("Authentication failed (bad credentials)", "AUTH", e)

                except paramiko.SSHException as e:
                    err.set("SSH negotiation failed", "SSH", e)

                except Exception as e:
                    err.set("Host initialization failed", "INIT", e)

            # Do not allow the pipeline to proceed after any failure
            if err.triggered:
                fatal_error = True

            # ==========================================================
            # ACT IV — Discovery
            # ==========================================================
            if not err.triggered:
                try:
                    host_filter = Filter(task["host_filter"], log=log)

                    file_metadata = daemon.get_metadata_files(
                        filter_obj=host_filter,
                        host_id=host_id,
                        callBackFileTask=db.get_all_filetask_names,
                        callBackFileTaskHistory=db.get_all_filetaskhistory_names,
                        callBackGetLastDBDate=db.get_last_discovery
                    )

                    if file_metadata:
                        # Create FILE_TASK entries
                        db.file_task_create(
                            host_id=host_id,
                            file_metadata=file_metadata,
                            task_type=k.FILE_TASK_DISCOVERY,
                            task_status=k.TASK_DONE,
                        )

                        # Create FILE_TASK_HISTORY entries
                        db.file_history_create(
                            host_id=host_id,
                            file_metadata=file_metadata,
                            task_type=k.FILE_TASK_DISCOVERY,
                            task_status=k.TASK_DONE,
                        )

                        log.entry(
                            f"[DISCOVERY] {len(file_metadata)} FILE_TASK(s) created for host {host_id}"
                        )
                    else:
                        log.entry(f"[DISCOVERY] Host {host_id}: no files found")

                except Exception as e:
                    err.set("Discovery failed", "DISCOVERY", e)

            if err.triggered:
                fatal_error = True

            # ==========================================================
            # ACT V — Promote → BACKUP queue
            # ==========================================================
            if not err.triggered:
                try:
                    n = db.update_backlog_by_filter(
                        host_id=host_id,
                        task_filter=task["host_filter"],
                        search_type=k.FILE_TASK_DISCOVERY,
                        search_status=k.TASK_DONE,
                        new_type=k.FILE_TASK_BACKUP_TYPE,
                        new_status=k.TASK_PENDING,
                        candidate_paths=file_metadata,
                    )

                    log.entry(
                        f"[DISCOVERY] Promoted {n['moved_to_backup']} FILE_TASK(s) to BACKUP"
                    )

                    db.host_task_delete(task_id=task_id)

                except Exception as e:
                    err.set("Backlog promotion failed", "BACKLOG", e)

            if err.triggered:
                fatal_error = True

        # ==============================================================
        # OUTER EXCEPTIONS
        # ==============================================================
        except Exception as e:
            log.error(f"[MAIN] Unexpected error: {e}\n{traceback.format_exc()}")
            fatal_error = True  # Ensures proper final cleanup

        # ==============================================================
        # FINALLY — UNLOCK + CLEANUP (ALWAYS EXECUTED)
        # ==============================================================
        finally:

            # Handle TASK error state
            if err.triggered and task_id:
                err.log_error(host_id=host_id, task_id=task_id)
                
                # Persist error state for observability and retry
                try:
                    db.host_task_update(
                        task_id=task_id,
                        NU_STATUS=k.TASK_ERROR,
                        NA_MESSAGE=err.msg,
                        DT_HOST_TASK=datetime.now(),
                    )
                except Exception:
                    pass

            # Always unlock host regardless of errors
            if host_id is not None:
                try:
                    db.host_update(
                        host_id=host_id,
                        IS_BUSY=False,
                        NU_PID=0,
                    )
                except Exception:
                    pass

                # Create statistics update task only when successful operations occurred
                try:
                    if (not err.triggered) and (
                        n.get("rows_updated", 0) > 0 or len(file_metadata) > 0
                    ):
                        db.host_task_statistics_create(host_id=host_id)
                except Exception as e:
                    log.warning(f"[FINALIZE] Failed to create statistics task: {e}")

            # Close SFTP safely
            try:
                if daemon and daemon.sftp_conn.is_connected():
                    sftp.close()
            except Exception as e:
                log.warning(f"[CLEANUP] Failed to cleanup host: {e}")

            # If a fatal error occurred, skip to next iteration
            if fatal_error:
                sh._random_jitter_sleep()
                continue

            sh._random_jitter_sleep()




# ======================================================================
# Entry Point
# ======================================================================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        try:
            print(f"[FATAL] appCataloga_discovery failed: {e}", file=sys.stderr)
        except Exception:
            pass
        raise
