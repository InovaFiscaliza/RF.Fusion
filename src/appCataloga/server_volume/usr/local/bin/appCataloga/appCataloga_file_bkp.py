#! /usr/bin/python3
"""
File Backup Worker: transfers pending FILE_TASK records (NU_TYPE = BACKUP, NU_STATUS = PENDING)
from remote nodes to the central repository via SFTP.

Architecture:
    • One process per worker
    • One HOST per worker (lock enforced)
    • No shared SSH sessions
    • Worker 0 acts as manager and spawns additional workers

Compatible with Debian 7 (systemd-free).
"""

# ======================================================================
# Imports
# ======================================================================
import sys
import os
import time
import signal
import inspect
import subprocess
import paramiko
from datetime import datetime

# ----------------------------------------------------------------------
# Load configuration and database modules
# ----------------------------------------------------------------------
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

        try:
            # Clear BUSY flag for all HOST rows locked by this PID
            db.host_release_by_pid(pid)
        except Exception as e:
            pass

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

    try:
        # Release any HOST records locked by this process
        release_busy_hosts_on_exit()
    except Exception as e:
        pass


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
# Argument Parsing
# ======================================================================
def parse_arguments() -> None:
    """Parse command-line arguments (e.g., worker=0)."""
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
# Worker Management (process-based)
# ======================================================================
def list_running_workers(process_filename: str) -> list:
    """Detect currently running worker processes for this script."""
    workers = []
    try:
        pids = os.popen(f"pgrep -f {process_filename}").read().splitlines()
    except Exception as e:
        log.error(f"Error listing worker processes: {e}")
        return workers

    for pid in pids:
        cmdline = f"/proc/{pid}/cmdline"
        if not os.path.exists(cmdline):
            continue
        try:
            args = open(cmdline).read().split("\x00")
            for arg in args:
                if arg.startswith("worker="):
                    workers.append(int(arg.split("=")[1]))
                    break
        except Exception:
            continue

    workers = sorted(set(workers))
    log.entry(f"Detected running workers: {workers}")
    return workers


def spawn_additional_worker(current_workers: list) -> None:
    """Spawn a detached backup worker."""
    next_worker = 0
    while next_worker in current_workers:
        next_worker += 1

    if len(current_workers) >= k.BKP_TASK_MAX_WORKERS:
        return

    cmd = [sys.executable, os.path.abspath(__file__), f"worker={next_worker}"]
    try:
        subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        log.entry(f"Spawned backup worker worker={next_worker}.")
    except Exception as e:
        log.error(f"Failed to spawn worker {next_worker}: {e}")


def ensure_worker_pool():
    """
    Ensure that up to BKP_TASK_MAX_WORKERS are running.
    Only worker=0 is allowed to spawn workers.
    """
    try:
        script_name = os.path.basename(__file__)
        current_workers = list_running_workers(script_name)

        while len(current_workers) < k.BKP_TASK_MAX_WORKERS:
            spawn_additional_worker(current_workers)
            #time.sleep(0.5)
            sh._random_jitter_sleep()
            current_workers = list_running_workers(script_name)

    except Exception as e:
        log.warning(f"[WORKER_POOL] Failed to ensure worker pool: {e}")


# ======================================================================
# File Transfer
# ======================================================================
def transfer_file_task(sftp, remote_dir: str, filename: str, local_path: str) -> None:
    """Transfer a single file with timeout protection."""
    remote_path = f"{remote_dir}/{filename}"
    local_file = os.path.join(local_path, filename)

    if not sftp.test(remote_path):
        raise FileNotFoundError(f"Remote file '{remote_path}' not found.")

    sh.run_with_timeout(
        lambda: sftp.transfer(remote_path, local_file),
        timeout=k.HOST_BUSY_TIMEOUT
    )


# ======================================================================
# Main Execution
# ======================================================================
def main():
    parse_arguments()
    worker_id = process_status["worker"]

    # -------------------------------------------------------
    # Worker 0 acts as manager
    # -------------------------------------------------------
    if worker_id == 0:
        ensure_worker_pool()

    log.entry(f"[INIT] Backup worker {worker_id} started.")

    # -------------------------------------------------------
    # DB INIT
    # -------------------------------------------------------
    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"[INIT] Database init failed: {e}")
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

        try:
            # ---------------------------------------------------
            # Fetch pending FILE_TASK
            # ---------------------------------------------------
            row = db.read_file_task(
                task_status=k.TASK_PENDING,
                task_type=k.FILE_TASK_BACKUP_TYPE,
                check_host_busy=True,
            )

            if not row:
                sh._random_jitter_sleep()
                continue

            task, host_id, file_task_id = row

            # ---------------------------------------------------
            # Lock host + mark task RUNNING
            # ---------------------------------------------------
            db.host_update(
                host_id=host_id,
                IS_BUSY=True,
                DT_BUSY=datetime.now(),
                NU_PID=os.getpid(),
            )

            db.file_task_update(
                file_task_id,
                DT_FILE_TASK = datetime.now(),
                NU_STATUS=k.TASK_RUNNING,
                NU_PID=os.getpid(),
                NA_MESSAGE=sh._compose_message(
                    task_type=k.FILE_TASK_BACKUP_TYPE,
                    task_status=k.TASK_RUNNING,
                    path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                    name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                ),
            )

            # ---------------------------------------------------
            # Init SSH/SFTP
            # ---------------------------------------------------
            host = db.host_read_access(host_id)
            sftp_conn, daemon = sh.init_host_context(task, log)

            # ---------------------------------------------------
            # Prepare local path
            # ---------------------------------------------------
            local_path = os.path.join(
                k.REPO_FOLDER, k.TMP_FOLDER, host["host_uid"]
            )
            os.makedirs(local_path, exist_ok=True)

            # ---------------------------------------------------
            # Transfer file
            # ---------------------------------------------------
            transfer_file_task(
                sftp=sftp_conn,
                remote_dir=task["FILE_TASK__NA_HOST_FILE_PATH"],
                filename=task["FILE_TASK__NA_HOST_FILE_NAME"],
                local_path=local_path,
            )
            file_was_transferred = True

            # ---------------------------------------------------
            # Update history
            # ---------------------------------------------------
            db.file_history_update(
                task_type=k.FILE_TASK_BACKUP_TYPE,
                file_name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                NA_SERVER_FILE_NAME=task["FILE_TASK__NA_HOST_FILE_NAME"],
                NA_SERVER_FILE_PATH=local_path,
                NA_MESSAGE=sh._compose_message(
                    task_type=k.FILE_TASK_BACKUP_TYPE,
                    task_status=k.TASK_DONE,
                    path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                    name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                ),
            )

            # ---------------------------------------------------
            # Mark DONE
            # ---------------------------------------------------
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
            log.error(f"[WORKER {worker_id}] {e}")
            if file_task_id:
                db.file_task_update(
                    task_id=file_task_id,
                    NU_STATUS=k.TASK_ERROR,
                    NA_MESSAGE=str(e),
                )

        finally:
            try:
                if sftp_conn:
                    sftp_conn.close()
            except Exception:
                pass

            if host_id:
                try:
                    # Check if concurrent FILE_TASK was assigned:
                    if db.host_check_free(host_id=host_id,
                                          task_type=k.FILE_TASK_BACKUP_TYPE):
                        db.host_update(
                            host_id=host_id,
                            IS_BUSY=False,
                            NU_PID=0,
                        )
                except Exception:
                    pass

                if file_was_transferred:
                    try:
                        db.host_task_statistics_create(host_id=host_id)
                    except Exception:
                        pass

            sh._random_jitter_sleep()

    log.entry(f"Backup worker {worker_id} shutting down.")


if __name__ == "__main__":
    main()
