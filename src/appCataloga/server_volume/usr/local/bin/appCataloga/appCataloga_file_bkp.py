#! /usr/bin/python3
"""
File Backup Worker: transfers pending FILE_TASK records (NU_TYPE = BACKUP, NU_STATUS = PENDING)
from remote nodes to the central repository via SFTP.

Behavior:
    1. Prepare   → Initialize worker, logger, database, and signal handlers.
    2. Initialize → Detect or spawn workers and connect to the remote host.
    3. Act       → Transfer files, update statuses, and manage SFTP operations.
    4. Finalize  → Release locks, close sessions, and gracefully terminate.

Logs:
    Printed to stdout if 'target_screen=True' is set in configuration.
"""

# ======================================================================
# Imports
# ======================================================================
import sys
import os
import time
import random
import signal
import inspect
import subprocess
import paramiko
from datetime import datetime

# Load configuration and database modules
_CFG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga"))
if _CFG_DIR not in sys.path and os.path.isdir(_CFG_DIR):
    sys.path.append(_CFG_DIR)

_DB_DIR = os.path.join(os.path.dirname(__file__), "db")
if _DB_DIR not in sys.path and os.path.isdir(_DB_DIR):
    sys.path.append(_DB_DIR)

import shared as sh
from db.dbHandlerBKP import dbHandlerBKP
import config as k


# ======================================================================
# Globals
# ======================================================================
log = sh.log()
process_status = {"worker": 0, "running": True}


# ======================================================================
# Signal Handling
# ======================================================================
def _signal_handler(sig=None, frame=None):
    """Handle SIGTERM and SIGINT for graceful shutdown."""
    global process_status, log
    func = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Signal {sig} received at {func}() — stopping worker loop.")
    process_status["running"] = False
    os._exit(0)


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


# ======================================================================
# Argument Parsing
# ======================================================================
def parse_arguments() -> None:
    """Parse command-line arguments (e.g., worker=1)."""
    global process_status
    worker = 0
    for arg in sys.argv[1:]:
        if arg.startswith("worker="):
            try:
                worker = int(arg.split("=")[1])
            except ValueError:
                log.warning("Invalid worker value, defaulting to 0.")
    process_status["worker"] = worker
    log.entry(f"Worker ID set to {worker}.")


# ======================================================================
# Worker Management (systemd-free, portable)
# ======================================================================
def list_running_workers(process_filename: str) -> list[int]:
    """Detect currently running worker processes for this script."""
    workers = []
    try:
        running_pids = os.popen(f"pgrep -f {process_filename}").read().splitlines()
    except Exception as e:
        log.error(f"Error listing worker processes: {e}")
        return workers

    for pid in running_pids:
        cmdline_path = f"/proc/{pid}/cmdline"
        if not os.path.exists(cmdline_path):
            continue
        try:
            args = open(cmdline_path).read().split("\x00")
            for arg in args:
                if arg.startswith("worker="):
                    workers.append(int(arg.split("=")[1]))
                    break
        except Exception as e:
            log.warning(f"Unable to inspect PID {pid}: {e}")
            continue

    workers = sorted(set(workers))
    log.entry(f"Detected running workers: {workers}")
    return workers


def spawn_additional_worker(current_workers: list[int]) -> None:
    """Spawn a detached backup worker using subprocess.Popen()."""
    next_worker = 0
    while next_worker in current_workers:
        next_worker += 1

    if len(current_workers) >= k.BKP_TASK_MAX_WORKERS:
        log.entry("Maximum worker limit reached — not spawning new worker.")
        return

    cmd = [sys.executable, os.path.abspath(__file__), f"worker={next_worker}"]
    try:
        subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        log.entry(f"Spawned new backup worker (index={next_worker}).")
    except Exception as e:
        log.error(f"Failed to spawn worker {next_worker}: {e}")


# ======================================================================
# Task Operations
# ======================================================================
def transfer_file_task(sftp, remote_dir: str, filename: str, local_path: str) -> None:
    """
    Transfer a single file from the remote host to the local repository.

    Protected by a timeout to prevent SFTP stalls.

    Raises:
        FileNotFoundError
        TimeoutError
        RuntimeError
    """

    remote_path = f"{remote_dir}/{filename}"
    local_file = os.path.join(local_path, filename)

    # 1) Check file existence
    if not sftp.test(remote_path):
        raise FileNotFoundError(f"Remote file '{remote_path}' not found.")

    # 2) Transfer with timeout
    try:
        sh.run_with_timeout(
            lambda: sftp.transfer(remote_path, local_file),
            timeout=k.HOST_BUSY_TIMEOUT  # timeout in seconds (adjustable)
        )

    except TimeoutError as te:
        raise TimeoutError(
            f"Timeout transferring '{remote_path}' → '{local_file}': {te}"
        )

    except Exception as e:
        raise RuntimeError(
            f"Failed to transfer '{remote_path}' → '{local_file}': {e}"
        )


# ======================================================================
# Main Execution
# ======================================================================
def main():
    """Main FILE_TASK backup pipeline (one task per worker)."""

    parse_arguments()
    worker_id = process_status["worker"]
    log.entry(f"[INIT] Backup worker {worker_id} started.")

    # -------------------------------------------------------
    # DB INIT
    # -------------------------------------------------------
    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"[INIT] Failed to initialize database: {e}")
        sys.exit(1)

    # =======================================================
    # MAIN LOOP
    # =======================================================
    while process_status["running"]:

        daemon = None
        sftp_conn = None
        host = None
        err = sh.ErrorHandler(log)
        file_was_transferred = False

        task = None
        host_id = None
        file_task_id = None
        fatal_error = False  # FIX: ensures final cleanup ALWAYS runs

        try:
            # ===================================================
            # ACT I — Fetch next pending FILE_TASK (BACKUP)
            # ===================================================
            try:
                row = db.read_file_task(
                    task_status=k.TASK_PENDING,
                    task_type=k.FILE_TASK_BACKUP_TYPE,
                    check_host_busy=True,
                )
            except Exception as e:
                err.set("Failed to read FILE_TASK", stage="READ_FILE_TASK", exc=e)

            if err.triggered:
                fatal_error = True  # FIX
            else:
                if not row:
                    sh._random_jitter_sleep()
                    continue

                task, host_id, file_task_id = row

            # ===================================================
            # ACT II — Lock host + mark FILE_TASK as running
            # ===================================================
            if not err.triggered:
                try:
                    db.host_update(
                        host_id=host_id,
                        IS_BUSY=True,
                        DT_BUSY=datetime.now(),
                        NU_PID=os.getpid(),
                    )

                    db.file_task_update(
                        file_task_id,
                        NU_STATUS=k.TASK_RUNNING,
                        NU_PID=os.getpid(),
                        NA_MESSAGE=sh._compose_message(
                            task_type=k.FILE_TASK_BACKUP_TYPE,
                            task_status=k.TASK_RUNNING,
                            path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                            name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                        ),
                    )

                except Exception as e:
                    err.set("Failed locking host or marking FILE_TASK running",
                            stage="LOCK_TASK", exc=e)

            if err.triggered:
                fatal_error = True

            # ===================================================
            # ACT III — Load HOST metadata
            # ===================================================
            if not err.triggered:
                try:
                    host = db.host_read_access(host_id)
                    if not host:
                        raise RuntimeError("Host metadata missing")
                except Exception as e:
                    err.set("Failed to load HOST metadata", stage="READ_HOST", exc=e)

            if err.triggered:
                fatal_error = True

            # ===================================================
            # ACT IV — Init SFTP + HostDaemon
            # ===================================================
            if not err.triggered:
                try:
                    sftp_conn, daemon = sh.init_host_context(task, log)
                except Exception as e:
                    err.set("Failed to initialize SFTP/Daemon",
                            stage="INIT", exc=e)

            if err.triggered:
                fatal_error = True

            # ===================================================
            # ACT V — Prepare local repository folder
            # ===================================================
            if not err.triggered:
                try:
                    local_path = os.path.join(
                        k.REPO_FOLDER, k.TMP_FOLDER, host["host_uid"]
                    )
                    os.makedirs(local_path, exist_ok=True)
                except Exception as e:
                    err.set("Failed preparing local folder",
                            stage="LOCAL_PATH", exc=e)

            if err.triggered:
                fatal_error = True

            # ===================================================
            # ACT VI — Transfer file from RFeye to local storage
            # ===================================================
            if not err.triggered:
                try:
                    transfer_file_task(
                        sftp=sftp_conn,
                        remote_dir=task["FILE_TASK__NA_HOST_FILE_PATH"],
                        filename=task["FILE_TASK__NA_HOST_FILE_NAME"],
                        local_path=local_path,
                    )
                    file_was_transferred = True

                except Exception as e:
                    err.set("File transfer failed", stage="TRANSFER", exc=e)

            if err.triggered:
                fatal_error = True

            # ===================================================
            # ACT VII — Update FILE_TASK_HISTORY (backup timestamp)
            # ===================================================
            if not err.triggered:
                try:
                    db.file_history_update(
                        task_type=k.FILE_TASK_BACKUP_TYPE,
                        file_name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                        NA_SERVER_FILE_NAME=task["FILE_TASK__NA_HOST_FILE_NAME"],
                        NA_SERVER_FILE_PATH=local_path,
                    )
                except Exception as e:
                    err.set("Failed updating FILE_TASK_HISTORY",
                            stage="HISTORY", exc=e)

            if err.triggered:
                fatal_error = True

            # ===================================================
            # ACT VIII — Mark FILE_TASK as DONE
            # ===================================================
            if not err.triggered:
                try:
                    db.file_task_update(
                        task_id=file_task_id,
                        NU_STATUS=k.TASK_DONE,
                        NA_SERVER_FILE_PATH=local_path,
                        NA_SERVER_FILE_NAME=task["FILE_TASK__NA_HOST_FILE_NAME"],
                        NA_MESSAGE=sh._compose_message(
                            task_type=k.FILE_TASK_BACKUP_TYPE,
                            task_status=k.TASK_DONE,
                            path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                            name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                        ),
                    )
                except Exception as e:
                    err.set("Failed finalizing FILE_TASK", stage="FINAL", exc=e)

            if err.triggered:
                fatal_error = True

        # =======================================================
        # OUTER EXCEPTIONS (unexpected or SSH-related)
        # =======================================================
        except (paramiko.AuthenticationException, paramiko.SSHException) as e:
            log.error(f"[SSH] {e}")
            fatal_error = True
            time.sleep(5)

        except Exception as e:
            log.error(f"[UNEXPECTED] Worker {worker_id}: {e}")
            fatal_error = True
            time.sleep(3)

        # =======================================================
        # ALWAYS — FINAL CLEANUP + HOST UNLOCK
        # =======================================================
        finally:

            # -------------------------------------------------------
            # Mark FILE_TASK as ERROR if needed
            # -------------------------------------------------------
            if err.triggered and file_task_id is not None:
                try:
                    db.file_task_update(
                        task_id=file_task_id,
                        NU_STATUS=k.TASK_ERROR,
                        NA_MESSAGE=sh._compose_message(
                            task_type=k.FILE_TASK_BACKUP_TYPE,
                            task_status=k.TASK_ERROR,
                            path=task["FILE_TASK__NA_HOST_FILE_PATH"] if task else "N/A",
                            name=task["FILE_TASK__NA_HOST_FILE_NAME"] if task else "N/A",
                            extra_msg=err.msg,
                        ),
                    )
                except Exception as e:
                    log.warning(f"[CLEANUP] Failed marking FILE_TASK ERROR: {e}")

            # -------------------------------------------------------
            # Cleanup network resources
            # -------------------------------------------------------
            try:
                if daemon and daemon.sftp_conn.is_connected():
                    sftp_conn.close()
            except Exception as e:
                log.warning(f"[CLEANUP] Failed to cleanup host: {e}")

            # -------------------------------------------------------
            # ALWAYS — Unlock host
            # -------------------------------------------------------
            if host_id is not None:
                try:
                    db.host_update(
                        host_id=host_id,
                        IS_BUSY=False,
                        NU_PID=0,
                    )
                except Exception:
                    pass

                # ---------------------------------------------------
                # Enqueue stats update task only if backup succeeded
                # ---------------------------------------------------
                if file_was_transferred:
                    try:
                        db.host_task_statistics_create(host_id=host_id)
                    except Exception as e:
                        log.warning(f"[FINALIZE] Failed creating stats task: {e}")

            # -------------------------------------------------------
            # Decide if we skip next iteration due to fatal error
            # -------------------------------------------------------
            if fatal_error:
                sh._random_jitter_sleep()
                continue

            sh._random_jitter_sleep()

    log.entry(f"Backup worker {worker_id} shutting down gracefully.")




if __name__ == "__main__":
    main()
