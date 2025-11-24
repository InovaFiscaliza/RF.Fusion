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


# ======================================================================
# Signal handling
# ======================================================================
def _handle_sigterm(sig, frame) -> None:
    """Handle SIGTERM/SIGINT to stop the main loop gracefully."""
    process_status["running"] = False


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


# ======================================================================
# Utility helpers
# ======================================================================
def _register_daemon_for_cleanup(daemon: Any) -> None:
    """Track HostDaemon instances for later HALT_FLAG cleanup."""
    if daemon:
        _DAEMON_REGISTRY.append(daemon)


def _release_all_halt_flags() -> None:
    """Release all HALT_FLAGs created during this process runtime."""
    for daemon in _DAEMON_REGISTRY:
        try:
            if daemon and daemon.sftp_conn.is_connected():
                if getattr(daemon, "halt_flag_set_time", None):
                    daemon.release_halt_flag(service="appCataloga_discovery", force=True)
                    log.entry("[cleanup] Released HALT_FLAG from registry.")
        except Exception as e:
            log.warning(f"[cleanup] Failed to release HALT_FLAG: {e}")


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

        try:
            # ==========================================================
            # ACT I — Fetch next HOST_TASK (PROCESSING_TYPE, pending)
            # ==========================================================
            task = db.host_task_read(
                task_type=k.HOST_TASK_PROCESSING_TYPE,
                task_status=k.TASK_PENDING,
                check_host_busy=True,
            )

            if not task:
                sh._random_jitter_sleep()
                continue
            
            # Tasks contents
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
                )
            except Exception as e:
                err.set("Failed to lock HOST or HOST_TASK", "LOCK_TASK", e)

            if err.triggered:
                err.log_error(host_id=host_id, task_id=task_id)
                continue

            # ==========================================================
            # ACT III — Init SFTP + HostDaemon
            # ==========================================================
            try:
                sftp, daemon = sh.init_host_context(task, log)

            except paramiko.AuthenticationException as e:
                err.set("Authentication failed (bad credentials)", "AUTH", e)

            except paramiko.SSHException as e:
                err.set("SSH negotiation failed", "SSH", e)

            except Exception as e:
                err.set("Host initialization failed", "INIT", e)

            if err.triggered:
                err.log_error(host_id=host_id, task_id=task_id)
                continue

            # ==========================================================
            # ACT IV — Prechecks (Config + HALT_FLAG)
            # ==========================================================
            if not daemon.get_config():
                err.set("Failed to load remote configuration", "CONFIG")

            elif not daemon.get_halt_flag(
                service="appCataloga_discovery",
                use_pid=False
            ):
                err.set("Filesystem busy (halt flag set)", "HALT_FLAG")

            if err.triggered:
                err.log_error(host_id=host_id, task_id=task_id)
                continue

            # ==========================================================
            # ACT V — Discovery
            # ==========================================================
            try:
                host_filter = Filter(task["host_filter"], log=log).data

                file_metadata = daemon.get_metadata_files(
                    filter=host_filter,
                    callBackFileHistory=db.check_file_history,
                    callBackFileTaskHistory=db.check_file_task,
                )

                if file_metadata:
                    # Create FILE_TASK to keep program pipeline
                    db.file_task_create(
                        host_id=host_id,
                        file_metadata=file_metadata,
                        task_type=k.FILE_TASK_DISCOVERY,
                        task_status=k.TASK_DONE,
                    )
                    
                    # Create FILE_TASK_HISTORY entries to preserve statistics
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

            # ==========================================================
            # ACT VI — Promote → BACKUP queue
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

            # ==========================================================
            # ACT VII — Centralized Error Handling
            # ==========================================================
            if err.triggered:
                err.log_error(host_id=host_id, task_id=task_id)

                db.host_task_update(
                    task_id=task_id,
                    NU_STATUS=k.TASK_ERROR,
                    NA_MESSAGE=err.msg,
                )

                continue

        # ==============================================================
        # OUTER EXCEPTIONS
        # ==============================================================
        except Exception as e:
            log.error(f"[MAIN] Unexpected error: {e}\n{traceback.format_exc()}")

        # ==============================================================
        # FINALLY — UNLOCK + CLEANUP
        # ==============================================================
        finally:

            # Unlock host (always)
            if host_id is not None:
                try:
                    db.host_update(
                        host_id=host_id,
                        IS_BUSY=False,
                        NU_PID=0,
                    )
                except Exception:
                    pass

                # Enqueue statistics update task
                try:
                    # Only update if files were discovered
                    if n.get("rows_updated") > 0:
                        db.host_task_statistics_create(host_id=host_id)
                    
                except Exception as e:
                    log.warning(f"[FINALIZE] Failed to create statistics task: {e}")

            # Close connections
            try:
                if daemon and daemon.sftp_conn.is_connected():
                    daemon.release_halt_flag("appCataloga_discovery")
                    daemon.close_host(cleanup_due_backup=True)
                    sftp.close()
            except Exception as e:
                log.warning(f"[CLEANUP] Failed to cleanup host: {e}")

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
