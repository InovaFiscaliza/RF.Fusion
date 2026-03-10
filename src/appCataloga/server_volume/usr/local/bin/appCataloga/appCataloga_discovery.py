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
from shared import errors, legacy, logging_utils,filter
import config as k


# ======================================================================
# Globals
# ======================================================================
log = logging_utils.log()
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
        err = errors.ErrorHandler(log)
        task = None
        host_id = None
        task_id = None
        fatal_error = False   # FIX: global flag to ensure final cleanup always happens
        connect_busy = False

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
                legacy._random_jitter_sleep()
                continue

            host_id = task["HOST__ID_HOST"]
            task_id = task["HOST_TASK__ID_HOST_TASK"]
            hostname = task["HOST__NA_HOST_NAME"]

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
                    sftp, daemon = legacy.init_host_context(task, log)

                except paramiko.AuthenticationException as e:
                    err.set("Authentication failed (bad credentials)", stage="AUTH", exc=e)
                
                except paramiko.ssh_exception.NoValidConnectionsError as e:
                    connect_busy = True
                    log.warning(f"[Discovery] Host busy, retry later (host_id={host_id})")
                    continue

                except paramiko.SSHException as e:
                    err.set("SSH negotiation failed", stage="SSH", exc=e)

                except Exception as e:
                    err.set("SSH/SFTP initialization failed", stage="CONNECT", exc=e)
                

            # Do not allow the pipeline to proceed after any failure
            if err.triggered:
                fatal_error = True

            # ==========================================================
            # ACT IV — Discovery
            # ==========================================================
            # ------------------------------------------------------------
            # Discovery execution
            # ------------------------------------------------------------
            # Executes metadata discovery for the host only if no fatal
            # error has been previously triggered.
            #
            if not err.triggered:
                try:
                    # Build the discovery Filter from HOST_TASK definition
                    host_filter = filter.Filter(task["host_filter"], log=log)

                    # Counter used only for progress logging
                    processed = 0

                    # ----------------------------------------------------
                    # Metadata discovery pipeline
                    # ----------------------------------------------------
                    # iter_metadata_files:
                    #   • Streams remote filesystem metadata
                    #   • Yields bounded batches of FileMetadata
                    #   • Delegates deduplication via callback
                    #   • Guarantees memory bounded by batch_size
                    #
                    for batch in daemon.iter_metadata_files(
                        host_id=host_id,
                        hostname=hostname,
                        filter_obj=host_filter,
                        callBackCheckFile=db.filter_existing_file_batch,
                        callBackGetLastDBDate=db.get_last_discovery,
                        batch_size=k.DISCOVERY_BATCH_SIZE,
                    ):
                        # Persist discovered files as pipeline tasks
                        db.file_task_create(
                            host_id=host_id,
                            file_metadata=batch,
                            task_type=k.FILE_TASK_DISCOVERY,
                            task_status=k.TASK_DONE,
                        )

                        # Append discovery records to immutable history
                        db.file_history_create(
                            host_id=host_id,
                            file_metadata=batch,
                            task_type=k.FILE_TASK_DISCOVERY,
                            task_status=k.TASK_DONE,
                        )

                        # Log progress (already deduplicated files)
                        processed += len(batch)
                        log.entry(
                            f"[DISCOVERY] Host {host_id}: {processed} files processed"
                        )

                except Exception as e:
                    # Any exception here aborts discovery deterministically
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

            if connect_busy:
                log.warning(
                    f"[Discovery] Host busy, retry later (host_id={host_id})"
                )
                
                db.host_task_update(
                    task_id=task_id,
                    NU_STATUS=k.TASK_PENDING,
                    NU_PID=0,
                    DT_HOST_TASK=datetime.now(),
                    NA_MESSAGE="Host busy, retrying later",
                )
       
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
                
                # Host check tasks should be re-queued on connection 
                # errors to allow for retries after transient issues are resolved
                if err.stage == "CONNECT":
                    db.queue_host_task(
                        host_id=host_id,
                        task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE,
                        task_status=k.TASK_PENDING,
                        filter_dict=k.NONE_FILTER,
                    )

            # Always unlock host regardless of errors
            if host_id is not None and not connect_busy:
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
                        n.get("rows_updated", 0) > 0 or processed > 0
                    ):
                        db.host_update_statistics(host_id=host_id)
                except Exception as e:
                    log.warning(f"[FINALIZE] Failed to create statistics task: {e}")

            # Close SFTP safely
            try:
                if sftp:
                    try:
                        sftp.close()
                    except Exception:
                        pass
            except Exception as e:
                log.warning(f"[CLEANUP] Failed to cleanup host: {e}")

            # If a fatal error occurred, skip to next iteration
            if fatal_error:
                legacy._random_jitter_sleep()
                continue

            legacy._random_jitter_sleep()




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
