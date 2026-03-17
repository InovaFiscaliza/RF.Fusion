#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Discovery worker for the appCataloga ecosystem.

This service scans remote hosts for candidate files, writes immutable discovery
history, and promotes eligible items into the backup queue. The flow is kept
linear on purpose so that lock handling, retries, and backlog promotion stay
easy to audit in production.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import traceback
import inspect
import paramiko
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
from shared import errors, filter, legacy, logging_utils, tools
import config as k


# ======================================================================
# Globals
# ======================================================================
log = logging_utils.log()
process_status = {"running": True}
_DAEMON_REGISTRY: List[Any] = []


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


def release_locked_host(db: dbHandlerBKP, host_id: int | None) -> None:
    """
    Release the host claimed by the current discovery iteration.

    This is the normal per-task path that turns `HOST.IS_BUSY` back to
    `False` after discovery work completes, retries, or fails. Shutdown cleanup
    remains centralized in `release_busy_hosts_on_exit()`.
    """
    if host_id is None:
        return

    try:
        db.host_release_safe(
            host_id=host_id,
            current_pid=os.getpid(),
        )
    except Exception as e:
        log.warning(
            f"event=host_release_failed service=appCataloga_discovery "
            f"host_id={host_id} error={e}"
        )


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


# ======================================================================
# Main daemon loop
# ======================================================================
def main() -> None:
    """
    Run the discovery polling loop until shutdown is requested.
    """

    log.service_start("appCataloga_discovery")
    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"event=db_init_failed service=appCataloga_discovery error={e}")
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
        fatal_error = False   # FIX: global flag to ensure final cleanup always happens
        connect_busy = False
        preserve_host_busy_cooldown = False
        processed = 0
        n = {"rows_updated": 0, "moved_to_backup": 0}

        try:
            # ==========================================================
            # ACT I — Fetch next HOST_TASK (PROCESSING pending)
            # ==========================================================
            task = db.host_task_read(
                task_type=k.HOST_TASK_PROCESSING_TYPE,
                task_status=k.TASK_PENDING,
                check_host_busy=True,
                lock_host=True,
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
                lock_result = db.host_task_update(
                    where_dict={
                        "ID_HOST_TASK": task_id,
                        "NU_STATUS": k.TASK_PENDING,
                    },
                    NU_STATUS=k.TASK_RUNNING,
                    NU_PID=os.getpid(),
                    DT_HOST_TASK=datetime.now(),
                )

                if lock_result["rows_affected"] != 1:
                    log.warning(
                        f"event=host_task_claim_race host_id={host_id} task_id={task_id}"
                    )
                    continue

                log.event(
                    "discovery_started",
                    host_id=host_id,
                    task_id=task_id,
                    host=hostname,
                )

            except Exception as e:
                err.capture("Failed to lock HOST or HOST_TASK", "LOCK_TASK", e)

            if err.triggered:
                fatal_error = True

            # ==========================================================
            # ACT III — Init SFTP + HostDaemon
            # ==========================================================
            if not err.triggered:
                try:
                    sftp, daemon = legacy.init_host_context(task, log)

                except Exception as e:
                    if errors.is_transient_sftp_init_error(e):
                        connect_busy = True
                        log.warning(
                            f"event=sftp_busy_retry service=appCataloga_discovery "
                            f"host_id={host_id} task_id={task_id} error={e}"
                        )
                        continue

                    if isinstance(e, paramiko.AuthenticationException):
                        err.capture(
                            "Authentication failed (bad credentials)",
                            stage="AUTH",
                            exc=e,
                        )
                    elif isinstance(e, paramiko.SSHException):
                        err.capture("SSH negotiation failed", stage="SSH", exc=e)
                    else:
                        err.capture(
                            "SSH/SFTP initialization failed",
                            stage="CONNECT",
                            exc=e,
                        )
                

            # Do not allow the pipeline to proceed after any failure
            if err.triggered:
                fatal_error = True

            # ==========================================================
            # ACT IV — Discovery
            # ==========================================================
            # Executes metadata discovery for the host only if no fatal
            # error has been previously triggered.
            #
            if not err.triggered:
                try:
                    # Build the discovery Filter from HOST_TASK definition
                    host_filter = filter.Filter(task["host_filter"], log=log)

                    # Counter used only for progress logging
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
                        log.event(
                            "discovery_progress",
                            host_id=host_id,
                            processed_files=processed,
                        )

                except Exception as e:
                    # Any exception here aborts discovery deterministically
                    err.capture("Discovery failed", "DISCOVERY", e)


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

                    log.event(
                        "backlog_promoted",
                        host_id=host_id,
                        moved_to_backup=n["moved_to_backup"],
                    )

                    db.host_task_delete(task_id=task_id)
                    log.event(
                        "discovery_completed",
                        host_id=host_id,
                        task_id=task_id,
                        host=hostname,
                        discovered_files=processed,
                        moved_to_backup=n["moved_to_backup"],
                    )

                except Exception as e:
                    err.capture("Backlog promotion failed", "BACKLOG", e)

            if err.triggered:
                fatal_error = True

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
            )
            err.log_error(
                host_id=host_id,
                task_id=task_id,
                traceback=traceback.format_exc(),
            )
            fatal_error = True  # Ensures proper final cleanup

        # ==============================================================
        # FINALLY — UNLOCK + CLEANUP (ALWAYS EXECUTED)
        # ==============================================================
        finally:

            if connect_busy and task_id is not None and not err.triggered:
                try:
                    db.host_task_update(
                        task_id=task_id,
                        NU_STATUS=k.TASK_PENDING,
                        DT_HOST_TASK=datetime.now(),
                        NA_MESSAGE=tools.compose_message(
                            task_type=k.FILE_TASK_DISCOVERY,
                            task_status=k.TASK_PENDING,
                            detail=k.SFTP_BUSY_RETRY_DETAIL,
                        ),
                    )

                    preserve_host_busy_cooldown = db.host_start_transient_busy_cooldown(
                        host_id=host_id,
                        owner_pid=os.getpid(),
                        cooldown_seconds=k.SFTP_BUSY_COOLDOWN_SECONDS,
                    )

                    if preserve_host_busy_cooldown:
                        log.warning(
                            "event=sftp_busy_cooldown_started "
                            f"service=appCataloga_discovery host_id={host_id} "
                            f"task_id={task_id} "
                            f"cooldown_seconds={k.SFTP_BUSY_COOLDOWN_SECONDS}"
                        )
                except Exception as e:
                    log.error(
                        "event=sftp_busy_requeue_failed "
                        f"service=appCataloga_discovery host_id={host_id} "
                        f"task_id={task_id} error={e}"
                    )
       
            # Handle TASK error state
            if err.triggered and task_id:
                err.log_error(host_id=host_id, task_id=task_id)
                
                # Persist error state for observability and retry
                try:
                    db.host_task_update(
                        task_id=task_id,
                        NU_STATUS=k.TASK_ERROR,
                        NA_MESSAGE=tools.compose_message(
                            task_type=k.FILE_TASK_DISCOVERY,
                            task_status=k.TASK_ERROR,
                            detail=err.msg,
                        ),
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

                log.error_event(
                    "discovery_error",
                    host_id=host_id,
                    task_id=task_id,
                    host=hostname,
                    discovered_files=processed,
                    moved_to_backup=n.get("moved_to_backup", 0),
                    error=err.format_error() or "Discovery failed",
                )

            # Close SFTP safely
            try:
                if sftp:
                    try:
                        sftp.close()
                    except Exception:
                        pass
            except Exception as e:
                log.warning(f"event=cleanup_host_context_failed error={e}")

            # -------------------------------------------------
            # Safe release host
            # -------------------------------------------------
            if host_id is not None and not preserve_host_busy_cooldown:
                # This is the single normal-path release point for the host
                # claimed by `host_task_read(..., lock_host=True)`.
                release_locked_host(db, host_id)

                # Create statistics update task only when successful operations occurred
                try:
                    if (not err.triggered) and (
                        n.get("rows_updated", 0) > 0 or processed > 0
                    ):
                        db.host_update_statistics(host_id=host_id)
                except Exception as e:
                    log.warning(
                        f"event=statistics_update_failed host_id={host_id} error={e}"
                    )

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
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal discovery worker crash",
            stage="MAIN",
            exc=e,
        )
        err.log_error()
        release_busy_hosts_on_exit()
        raise
