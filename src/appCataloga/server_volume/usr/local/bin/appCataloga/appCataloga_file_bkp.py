#! /usr/bin/python3
"""
File Backup Worker.

This worker transfers pending FILE_TASK records
(NU_TYPE = BACKUP, NU_STATUS = PENDING) from remote hosts
to the central repository via SFTP.

Architecture principles:
    • One process per worker
    • One HOST per worker (BUSY lock enforced in DB)
    • No shared SSH/SFTP sessions
    • Worker 0 acts as manager and spawns additional workers

Design goals:
    • Deterministic server-side filenames
    • No filename collisions
    • Reprocessable without database
    • Compatible with RFeye proprietary software

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
import hashlib
from datetime import datetime

# ----------------------------------------------------------------------
# Load configuration and database modules
# ----------------------------------------------------------------------
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
from shared import errors, legacy, logging_utils,timeout_utils, tools
import config as k


# ======================================================================
# Globals
# ======================================================================
log = logging_utils.log()
process_status = {
    "worker": 0,
    "running": True,
}


# ======================================================================
# Server filename builder (ARCHITECTURAL CONTRACT)
# ======================================================================
def build_server_filename(host_uid: str, remote_path: str, filename: str) -> str:
    """
    Build a deterministic server-side filename.

    This function is the ONLY place where server filenames
    are defined. It must never be reimplemented elsewhere.

    Pattern:
        p-<hash>--<original_filename>

    Hash source:
        sha1(host_uid + ":" + remote_path)[:8]

    The hash:
        • Prevents filename collisions
        • Is stable across reprocessing
        • Does NOT depend on server paths

    Args:
        host_uid (str): Unique identifier of the host/station
        remote_path (str): Absolute path on the remote host
        filename (str): Original filename on the host

    Returns:
        str: Server-side filename
    """
    h = hashlib.sha1(
        f"{host_uid}:{remote_path}".encode("utf-8")
    ).hexdigest()[:8]

    return f"p-{h}--{filename}"


# ======================================================================
# Signal Handling
# ======================================================================
def release_busy_hosts_on_exit() -> None:
    """
    Release all HOST records marked as BUSY by this process PID.

    This function is safe to call multiple times and must never
    interrupt shutdown, even if the database is unavailable.
    """
    try:
        pid = os.getpid()
        log.entry(f"[CLEANUP] Releasing BUSY hosts for PID={pid}")
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
        db.host_release_by_pid(pid)
    except Exception:
        # Cleanup must never break termination
        pass


def sigterm_handler(signal=None, frame=None) -> None:
    """
    Handle SIGTERM (graceful shutdown).

    Triggered by:
        • kill <pid>
        • pkill
        • service stop scripts
    """
    process_status["running"] = False
    release_busy_hosts_on_exit()


def sigint_handler(signal=None, frame=None) -> None:
    """
    Handle SIGINT (interactive interrupt).

    Triggered by:
        • Ctrl+C in terminal
    """
    process_status["running"] = False
    release_busy_hosts_on_exit()


# Register signal handlers
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


# ======================================================================
# Argument Parsing
# ======================================================================
def parse_arguments() -> None:
    """
    Parse command-line arguments.

    Supported arguments:
        worker=<id>

    Sets:
        process_status["worker"]
    """
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
    """
    Detect currently running workers for this script.

    Args:
        process_filename (str): Script filename

    Returns:
        list[int]: Sorted list of active worker IDs
    """
    workers = []
    try:
        pids = os.popen(f"pgrep -f {process_filename}").read().splitlines()
        for pid in pids:
            cmdline = f"/proc/{pid}/cmdline"
            if not os.path.exists(cmdline):
                continue
            args = open(cmdline).read().split("\x00")
            for arg in args:
                if arg.startswith("worker="):
                    workers.append(int(arg.split("=")[1]))
                    break
    except Exception:
        pass

    workers = sorted(set(workers))
    log.entry(f"Detected running workers: {workers}")
    return workers


def spawn_additional_worker(current_workers: list) -> None:
    """
    Spawn a detached backup worker process.

    Args:
        current_workers (list): List of currently running workers
    """
    next_worker = 0
    while next_worker in current_workers:
        next_worker += 1

    if len(current_workers) >= k.BKP_TASK_MAX_WORKERS:
        return

    try:
        subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), f"worker={next_worker}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        log.entry(f"Spawned backup worker worker={next_worker}.")
    except Exception as e:
        log.error(f"Failed to spawn worker {next_worker}: {e}")


def ensure_worker_pool() -> None:
    """
    Ensure that up to BKP_TASK_MAX_WORKERS are running.

    Only worker=0 is allowed to spawn additional workers.
    """
    try:
        script_name = os.path.basename(__file__)
        current_workers = list_running_workers(script_name)

        while len(current_workers) < k.BKP_TASK_MAX_WORKERS:
            spawn_additional_worker(current_workers)
            legacy._random_jitter_sleep()
            current_workers = list_running_workers(script_name)

    except Exception as e:
        log.warning(f"[WORKER_POOL] Failed to ensure worker pool: {e}")


# ======================================================================
# File Transfer
# ======================================================================
def transfer_file_task(
    sftp,
    remote_dir: str,
    remote_filename: str,
    local_path: str,
    server_filename: str,
) -> None:
    """
    Transfer a single file from the remote host to the server.

    Args:
        sftp: Active SFTP connection
        remote_dir (str): Remote directory path
        remote_filename (str): Original filename on the host
        local_path (str): Destination directory on server
        server_filename (str): Deterministic server-side filename

    Raises:
        FileNotFoundError: If the remote file does not exist
    """
    remote_path = f"{remote_dir}/{remote_filename}"
    local_file = os.path.join(local_path, server_filename)

    if not sftp.test(remote_path):
        raise FileNotFoundError(f"Remote file '{remote_path}' not found.")

    timeout_utils.run_with_timeout(
        lambda: sftp.transfer(remote_path, local_file),
        timeout=k.HOST_BUSY_TIMEOUT,
    )


# ======================================================================
# Main Execution
# ======================================================================
def main() -> None:
    """
    Main worker execution loop.
    """
    parse_arguments()
    worker_id = process_status["worker"]

    # Worker 0 manages the pool
    if worker_id == 0:
        ensure_worker_pool()

    log.entry(f"[INIT] Backup worker {worker_id} started.")

    # Initialize database handler
    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"[INIT] Database init failed: {e}")
        sys.exit(1)

    # =======================================================
    # MAIN LOOP
    # =======================================================
    while process_status["running"]:

        sftp_conn = None
        host = None
        err = errors.ErrorHandler(log)
        file_was_transferred = False

        task = None
        host_id = None
        file_task_id = None
        server_filename = None
        server_file_path = None

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
                legacy._random_jitter_sleep()
                continue

            task, host_id, file_task_id = row

            # ---------------------------------------------------
            # Lock host and mark task RUNNING
            # ---------------------------------------------------
            db.host_update(
                host_id=host_id,
                IS_BUSY=True,
                DT_BUSY=datetime.now(),
                NU_PID=os.getpid(),
            )

            db.file_task_update(
                file_task_id,
                DT_FILE_TASK=datetime.now(),
                NU_STATUS=k.TASK_RUNNING,
                NU_PID=os.getpid(),
                NA_MESSAGE=tools.compose_message(
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
            sftp_conn, _ = legacy.init_host_context(task, log)

            # ---------------------------------------------------
            # Prepare local server path
            # ---------------------------------------------------
            server_file_path = os.path.join(
                k.REPO_FOLDER, k.TMP_FOLDER, host["host_uid"]
            )
            os.makedirs(server_file_path, exist_ok=True)

            # ---------------------------------------------------
            # Build deterministic server filename
            # ---------------------------------------------------
            # Celplan RMU has in filename specific data that doesn't could be changed
            # Also RMU has timestamp in filename and in theoric cenario never be repetead
            if "CW" in host["host_uid"]:
                server_filename = task["FILE_TASK__NA_HOST_FILE_NAME"]
            else:
                server_filename = build_server_filename(
                    host_uid=host["host_uid"],
                    remote_path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                    filename=task["FILE_TASK__NA_HOST_FILE_NAME"],
                )

            # ---------------------------------------------------
            # Transfer file
            # ---------------------------------------------------
            transfer_file_task(
                sftp=sftp_conn,
                remote_dir=task["FILE_TASK__NA_HOST_FILE_PATH"],
                remote_filename=task["FILE_TASK__NA_HOST_FILE_NAME"],
                local_path=server_file_path,
                server_filename=server_filename,
            )
            file_was_transferred = True

            # ---------------------------------------------------
            # Update FILE_TASK_HISTORY
            # ---------------------------------------------------
            db.file_history_update(
                task_type=k.FILE_TASK_BACKUP_TYPE,
                host_file_name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                NA_SERVER_FILE_NAME=server_filename,
                NA_SERVER_FILE_PATH=server_file_path,
                NU_STATUS_BACKUP=k.TASK_DONE,
                NA_MESSAGE=tools.compose_message(
                    task_type=k.FILE_TASK_BACKUP_TYPE,
                    task_status=k.TASK_DONE,
                    path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                    name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                ),
            )

            # ---------------------------------------------------
            # Promote task to PROCESS
            # ---------------------------------------------------
            db.file_task_update(
                task_id=file_task_id,
                NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
                NU_STATUS=k.TASK_PENDING,
                NA_SERVER_FILE_PATH=server_file_path,
                NA_SERVER_FILE_NAME=server_filename,
                NA_MESSAGE=tools.compose_message(
                    task_type=k.FILE_TASK_BACKUP_TYPE,
                    task_status=k.TASK_DONE,
                    path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                    name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                ),
            )

        except Exception as e:
            log.error(f"[WORKER {worker_id}] {e}")
            err.set("Backup failed", stage="BACKUP", exc=e)
            err.log_error(host_id=host_id, task_id=file_task_id)

            if file_task_id:
                NA_MESSAGE = tools.compose_message(
                    task_type=k.FILE_TASK_BACKUP_TYPE,
                    task_status=k.TASK_ERROR,
                )

                if err.triggered:
                    NA_MESSAGE = f"{NA_MESSAGE} | {err.format_error()}"

                db.file_task_update(
                    task_id=file_task_id,
                    NU_STATUS=k.TASK_ERROR,
                    NA_MESSAGE=NA_MESSAGE,
                )

                db.file_history_update(
                    task_type=k.FILE_TASK_BACKUP_TYPE,
                    host_file_name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                    NA_SERVER_FILE_NAME=server_filename,
                    NA_SERVER_FILE_PATH=server_file_path,
                    NU_STATUS_BACKUP=k.TASK_ERROR,
                    NA_MESSAGE=NA_MESSAGE,
                )

        finally:
            try:
                if sftp_conn:
                    sftp_conn.close()
            except Exception:
                pass

            if host_id:
                try:
                    if db.host_check_free(
                        host_id=host_id,
                        task_type=k.FILE_TASK_BACKUP_TYPE,
                    ):
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

            legacy._random_jitter_sleep()

    log.entry(f"Backup worker {worker_id} shutting down.")


if __name__ == "__main__":
    main()
