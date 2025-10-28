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
def transfer_file_task(db: dbHandlerBKP, host: dict, sftp, task_id: int,
                       remote_dir: str, filename: str, local_path: str) -> bool:
    """
    Transfer a single FILE_TASK from remote node to local repository.

    Args:
        db (dbHandlerBKP): Database handler.
        host (dict): Host record.
        sftp: Active SFTP connection object.
        task_id (int): Task identifier.
        remote_dir (str): Remote directory on the host.
        filename (str): Filename to transfer.
        local_path (str): Local destination directory.

    Returns:
        bool: True if transfer succeeded, False otherwise.
    """
    remote_path = f"{remote_dir}/{filename}"
    local_file = os.path.join(local_path, filename)

    # Verify remote file existence
    if not sftp.test(remote_path):
        msg = f"File '{remote_path}' not found on host {host['host_addr']}."
        db.file_task_update(task_id=task_id, NU_STATUS=k.TASK_ERROR, NA_MESSAGE=msg)
        log.warning(msg)
        return False
    
    # Update current task in FILE_TASK table
    db.file_task_update(task_id=task_id, 
                                NU_STATUS=k.TASK_RUNNING)

    # Attempt transfer
    try:
        sftp.transfer(remote_path, local_file)
        db.file_task_update(
            task_id=task_id,
            NU_PID=os.getpid(),
            NA_SERVER_FILE_NAME=filename,
            NA_SERVER_FILE_PATH=local_path,
            NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
            NU_STATUS=k.TASK_PENDING,
            NA_MESSAGE=f"Transferred '{remote_path}' → '{local_file}'.",
        )
        log.entry(f"Transfer OK: {remote_path} → {local_file}")
        return True

    except Exception as e:
        msg = f"Error transferring '{remote_path}' from {host['host_addr']}: {e}"
        db.file_task_update(task_id=task_id, NU_STATUS=k.TASK_ERROR, NA_MESSAGE=msg)
        log.error(msg)
        return False


# ======================================================================
# Main Execution
# ======================================================================
def main():
    """Main backup loop divided into four logical acts."""
    parse_arguments()
    worker_id = process_status["worker"]
    log.entry(f"Backup worker {worker_id} started.")

    # Database initialization
    try:
        db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"Failed to initialize database: {e}")
        sys.exit(1)

    # ===============================================================
    # Main loop
    # ===============================================================
    while process_status["running"]:
        daemon = None
        sftp_conn = None
        host = None

        try:
            # -------------------------------------------------------
            # ACT II — Retrieve pending BACKUP tasks
            # -------------------------------------------------------
            tasks = db_bp.read_file_tasks(
                task_type=k.FILE_TASK_BACKUP_TYPE,
                task_status=k.TASK_PENDING,
                group_by_host=True,
            )

            if not tasks:
                wait_time = int((k.MAX_FILE_TASK_WAIT_TIME + k.MAX_FILE_TASK_WAIT_TIME * random.random()) / 2)
                log.entry(f"No pending backups. Sleeping {wait_time}s.")
                time.sleep(wait_time)
                continue

            host_id = tasks[0]["FK_HOST"]
            host = db_bp.host_read_access(host_id)
            sftp_conn, daemon = sh.init_host_context(host, log)

            # -------------------------------------------------------
            # ACT III — Host prechecks (configuration & lock)
            # -------------------------------------------------------
            if not daemon.get_config():
                log.warning(f"[CONFIG] Missing configuration for host {host['host_addr']}.")
                continue

            if not daemon.get_halt_flag(service="appCataloga_file_bkp", use_pid=True):
                log.warning(f"[LOCK] Host {host['host_addr']} filesystem busy.")
                continue

            running_workers = list_running_workers(os.path.basename(__file__))
            if len(running_workers) < k.BKP_TASK_MAX_WORKERS:
                spawn_additional_worker(running_workers)

            local_path = os.path.join(k.REPO_FOLDER, k.TMP_FOLDER, host["host_addr"])
            os.makedirs(local_path, exist_ok=True)

            # -------------------------------------------------------
            # ACT IV — Transfer and record updates
            # -------------------------------------------------------
            success = 0
            fail = 0

            # Iterate in each task found for host
            for curr_task in tasks:
                try:
                    check = transfer_file_task(
                        db=db_bp, host=host, sftp=sftp_conn,
                        task_id=curr_task["ID_FILE_TASK"],
                        remote_dir=curr_task["NA_HOST_FILE_PATH"],
                        filename=curr_task["NA_HOST_FILE_NAME"],
                        local_path=local_path
                    )

                    if check:
                        success += 1
                        # Check FILE_TASK_HISTORY for previous records
                        file_history = db_bp.check_file_history(
                            NA_HOST_FILE_NAME=curr_task["NA_HOST_FILE_NAME"]
                        )
                        if not file_history:
                            db_bp.file_history_create(
                                k.FILE_TASK_BACKUP_TYPE,
                                FK_HOST=curr_task["FK_HOST"],
                                NA_HOST_FILE_PATH=curr_task["NA_HOST_FILE_PATH"],
                                NA_HOST_FILE_NAME=curr_task["NA_HOST_FILE_NAME"],
                                NA_SERVER_FILE_NAME=curr_task["NA_HOST_FILE_NAME"],
                                NA_SERVER_FILE_PATH=local_path,
                                VL_FILE_SIZE_KB=curr_task["VL_FILE_SIZE_KB"]
                            )
                        else:
                            db_bp.file_history_update(
                                task_type=k.FILE_TASK_BACKUP_TYPE,
                                file_name=curr_task["NA_HOST_FILE_NAME"]
                            )
                    else:
                        fail += 1

                except Exception as e:
                    fail += 1
                    log.error(f"[Worker {worker_id}] Error processing file '{curr_task.get('NA_HOST_FILE_NAME')}': {e}")
                    continue

            # Consolidated host update
            db_bp.host_update(
                host_id=host["host_id"],
                NU_HOST_FILES=success,
                DT_LAST_BACKUP=datetime.now(),
            )

        # ---------------------------------------------------------------
        # ACT V — Exception Handling (network, ssh, etc.)
        # ---------------------------------------------------------------
        except paramiko.AuthenticationException as e:
            log.error(f"SFTP authentication failed: {e}")
            time.sleep(10)
        except paramiko.SSHException as e:
            log.error(f"SFTP connection error: {e}")
            time.sleep(10)
        except Exception as e:
            log.error(f"[Worker {worker_id}] Unexpected error: {e}")
            time.sleep(5)

        # ---------------------------------------------------------------
        # ACT VI — Finalization (release locks, cleanup)
        # ---------------------------------------------------------------
        finally:
            try:
                if daemon:
                    daemon.release_halt_flag(service="appCataloga_file_bkp", use_pid=True)
                    daemon.close_host(cleanup_due_backup=True)
                elif host:
                    log.warning(f"Skipped cleanup — daemon not initialized for host {host['host_addr']}.")
            except Exception as e:
                log.warning(f"Cleanup failed: {e}")

    log.entry(f"Backup worker {worker_id} shutting down gracefully.")


if __name__ == "__main__":
    main()
