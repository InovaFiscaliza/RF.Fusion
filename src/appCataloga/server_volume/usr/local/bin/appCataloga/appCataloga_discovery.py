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
    db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    log.entry("[INIT] appCataloga_discovery microservice started.")
    sh._random_jitter_sleep()

    while process_status["running"]:

        try:
            # ----------------------------------------------------------
            # 1. Fetch one pending HOST_TASK
            # ----------------------------------------------------------
            task = db.host_task_read(task_type=k.HOST_PROCESSING_TYPE,
                                     task_status=k.TASK_PENDING)
            if not task:
                sh._random_jitter_sleep()
                continue

            host_id = int(task.get("host_id"))
            task_id = int(task.get("task_id"))
            daemon = None
            sftp = None
            error = False
            n_backup = {"moved_to_backup": 0}
            
            # Change HOST_TASK to running status
            db.host_task_update(task_id=task_id,
                                NU_STATUS=k.TASK_RUNNING)

            # ----------------------------------------------------------
            # 2. Initialize host context (SFTP + daemon)
            # ----------------------------------------------------------
            try:
                sftp, daemon = sh.init_host_context(task, log)
                _register_daemon_for_cleanup(daemon)
            except Exception as e:
                log.error(f"[INIT] Failed to initialize host {host_id}: {e}")
                db.host_update(ID_HOST=host_id, HOST_CHECK_ERROR=1)
                db.host_task_update(task_id=task_id,
                                    NU_STATUS=k.TASK_ERROR,
                                    NA_MESSAGE="Error, unable to open SFTP connection")
                continue

            # ----------------------------------------------------------
            # 3. Load configuration and acquire HALT_FLAG
            # ----------------------------------------------------------
            if not daemon.get_config():
                log.warning(f"[CONFIG] Missing configuration for host {host_id}. Deleting HOST_TASK {task_id}.")
                error = True
            elif not daemon.get_halt_flag(service="appCataloga_discovery", use_pid=False):
                log.warning(f"[LOCK] Host {host_id} filesystem busy. Deleting HOST_TASK {task_id}.")
                error = True

            if error:
                db.host_update(host_id=host_id, HOST_CHECK_ERROR=1)
                db.host_task_update(task_id=task_id,
                                    NU_STATUS=k.TASK_ERROR,
                                    NA_MESSAGE="Error, unable establish daemon connection")
                continue

            # ----------------------------------------------------------
            # 4. Discover candidate files and create FILE_TASKs
            # ----------------------------------------------------------
            try:
                host_filter = Filter(task.get("host_filter"), log=log).data
                file_metadata = daemon.get_metadata_files(filter=host_filter,
                                                    callBackFileHistory=db.check_file_history,
                                                    callBackFileTaskHistory=db.check_file_task)
                if not file_metadata:
                    log.entry(f"[DISCOVERY] No files found for host {host_id}.")
                else:
                    
                    db.file_task_create(
                        host_id=host_id,
                        file_metadata=file_metadata,
                        task_type=k.FILE_TASK_DISCOVERY,
                        task_status=k.TASK_DONE,
                    )
                    log.entry(f"[DISCOVERY] Created {len(file_metadata)} FILE_TASK(s) for host {host_id}.")
            except Exception as e:
                log.error(f"[DISCOVERY] Failed to create FILE_TASKs for host {host_id}: {e}")
                error = True

            # ----------------------------------------------------------
            # 5. Promote discovered files → BACKUP queue
            # ----------------------------------------------------------
            if not error:
                try:
                    n_backup = db.update_backlog_by_filter(
                        host_id=host_id,
                        task_filter=task["host_filter"],
                        search_type=k.FILE_TASK_DISCOVERY,
                        search_status=k.TASK_DONE,
                        new_type=k.FILE_TASK_BACKUP_TYPE,
                        new_status=k.TASK_PENDING,
                        candidate_paths=file_metadata,
                    )
                    log.entry(f"[DISCOVERY] Promoted {n_backup['moved_to_backup']} FILE_TASK(s) to BACKUP.")
                except Exception as e:
                    log.error(f"[DISCOVERY] Backlog promotion failed for host {host_id}: {e}")
                    error = True

            # ----------------------------------------------------------
            # 6. Remove processed HOST_TASK
            # ----------------------------------------------------------
            db.host_task_delete(task_id=task_id)

        # ==============================================================
        # Error handling (outer)
        # ==============================================================
        except (paramiko.AuthenticationException, paramiko.SSHException) as e:
            log.error(f"[SFTP] Authentication/connection failure: {e}")
            time.sleep(5)
        except Exception as e:
            log.error(f"[MAIN] {e}\n{traceback.format_exc()}")
            time.sleep(2)
        finally:
            # ----------------------------------------------------------
            # Cleanup (HALT_FLAG + connection)
            # ----------------------------------------------------------
            try:
                if daemon:
                    daemon.release_halt_flag(service="appCataloga_discovery")
                    daemon.close_host(cleanup_due_backup=True)
            except Exception as e:
                log.warning(f"[CLEANUP] HALT_FLAG release failed: {e}")

            try:
                if sftp:
                    sftp.close()
            except Exception:
                pass

    # --------------------------------------------------------------
    # Shutdown cleanup
    # --------------------------------------------------------------
    _release_all_halt_flags()
    log.entry("[STOP] Discovery microservice terminated gracefully.")


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
