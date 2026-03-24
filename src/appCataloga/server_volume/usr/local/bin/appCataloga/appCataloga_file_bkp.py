#! /usr/bin/python3
"""
Backup worker for remote host payload collection.

This worker transfers pending backup FILE_TASK rows from remote hosts into the
central repository through SFTP and then promotes successful rows into the
processing queue.

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

Compatible with Debian 7 (systemd-free). The pool grows on demand instead of
starting all workers eagerly, so worker lifecycle, host ownership, and retry
rules are kept very explicit in this file.
"""

# ======================================================================
# Imports
# ======================================================================
import os
import hashlib
import inspect
import paramiko
import signal
import subprocess
import sys
import time
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
from shared import errors, legacy, logging_utils, timeout_utils, tools
import config as k


# ======================================================================
# Globals
# ======================================================================
log = logging_utils.log()
process_status = {
    "worker": 0,
    "running": True,
    "seed_recovery_last_attempt": 0.0,
    "shutdown_broadcast_sent": False,
}


# ======================================================================
# Server filename builder (ARCHITECTURAL CONTRACT)
# ======================================================================
def build_server_filename(host_uid: str, remote_path: str, filename: str) -> str:
    """
    Build a deterministic server-side filename.

    This is the single source of truth for server-side backup filenames.
    It must not be reimplemented elsewhere, otherwise reprocessing and file
    lineage become inconsistent.

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
# Signal handling
# ======================================================================
def _signal_handler(signal_name: str) -> None:
    """
    Register shutdown intent, stop sibling workers, and release BUSY resources.
    """
    process_status["running"] = False
    log.signal_received(signal_name)
    broadcast_shutdown_to_worker_pool(signal_name)
    release_busy_hosts_on_exit()


def release_busy_hosts_on_exit() -> None:
    """
    Release all HOST records marked as BUSY by this process PID.

    This function is safe to call multiple times and must never
    interrupt shutdown, even if the database is unavailable.
    """
    try:
        pid = os.getpid()
        log.event("cleanup_busy_hosts", pid=pid)
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
        db.host_release_by_pid(pid)
    except Exception:
        # Cleanup must never break termination
        pass


def release_locked_host(db: dbHandlerBKP, host_id: int | None) -> None:
    """
    Release the host claimed by the current loop iteration.

    This is the normal per-task path that turns `HOST.IS_BUSY` back to
    `False` after backup work completes, retries, or fails. Shutdown cleanup
    still uses `release_busy_hosts_on_exit()` to release all locks owned by
    the worker PID.
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
            f"event=host_release_failed service=appCataloga_file_bkp "
            f"host_id={host_id} error={e}"
        )


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
                log.warning("event=worker_arg_invalid fallback_worker=0")
    process_status["worker"] = worker
    log.event("worker_configured", worker_id=worker)


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
    workers = sorted(
        {worker_id for _, worker_id in list_running_worker_processes(process_filename)}
    )
    log.event("worker_pool_scan", active_workers=workers)
    return workers


def list_running_worker_processes(process_filename: str) -> list[tuple[int, int]]:
    """
    Detect running backup worker processes with their PID and worker ID.

    The pool is detached (`start_new_session=True`), so console-level `Ctrl+C`
    only reaches the foreground worker. This helper makes sibling shutdown
    explicit by letting the signal handler enumerate and terminate the rest of
    the pool deterministically.
    """
    processes = []
    try:
        pids = os.popen(f"pgrep -f {process_filename}").read().splitlines()
        for pid_text in pids:
            cmdline = f"/proc/{pid_text}/cmdline"
            if not os.path.exists(cmdline):
                continue

            args = open(cmdline).read().split("\x00")
            worker_id = extract_worker_id_from_cmdline(args, process_filename)

            if worker_id is None:
                continue

            try:
                pid = int(pid_text)
            except ValueError:
                continue

            processes.append((pid, worker_id))
    except Exception:
        pass

    return sorted(set(processes), key=lambda item: (item[1], item[0]))


def broadcast_shutdown_to_worker_pool(signal_name: str) -> None:
    """
    Propagate a shutdown signal to sibling backup workers.

    Because workers run in separate sessions, interactive `Ctrl+C` or a direct
    `kill` sent to one worker would otherwise leave the detached siblings alive.
    """
    if process_status.get("shutdown_broadcast_sent"):
        return

    process_status["shutdown_broadcast_sent"] = True
    current_pid = os.getpid()
    script_name = os.path.basename(__file__)
    targets = [
        (pid, worker_id)
        for pid, worker_id in list_running_worker_processes(script_name)
        if pid != current_pid
    ]

    if not targets:
        return

    log.warning(
        f"event=worker_pool_shutdown_broadcast signal={signal_name} "
        f"sender_pid={current_pid} targets={targets}"
    )

    for pid, worker_id in targets:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            continue
        except Exception as e:
            log.warning(
                f"event=worker_pool_shutdown_broadcast_failed "
                f"target_pid={pid} worker_id={worker_id} error={e}"
            )


def extract_worker_id_from_cmdline(args: list, process_filename: str):
    """
    Resolve the worker ID represented by a process command line.

    The seed process is commonly started without an explicit `worker=0`
    argument by the service wrapper or an IDE debug session. In that case,
    once we confirm the command line is running this script, we must still
    treat it as worker 0; otherwise the pool manager never sees the seed
    and on-demand scale-out stalls after the first task.
    """
    for arg in args:
        if arg.startswith("worker="):
            try:
                return int(arg.split("=")[1])
            except ValueError:
                return None

    normalized_args = [
        os.path.basename(arg)
        for arg in args
        if isinstance(arg, str) and arg
    ]

    if process_filename in normalized_args:
        return 0

    return None


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
        log.event("worker_spawned", worker_id=next_worker)
    except Exception as e:
        log.error(f"event=worker_spawn_failed worker_id={next_worker} error={e}")


def spawn_specific_worker(worker_id: int) -> bool:
    """
    Spawn a specific worker ID when a gap in the pool must be repaired.

    Normal scale-out always grows from the highest active worker. This helper is
    reserved for recovery paths, such as recreating worker 0 after an unexpected
    process death, without inflating the pool beyond the configured limit.
    """
    try:
        subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), f"worker={worker_id}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        log.event("worker_spawned", worker_id=worker_id, reason="pool_recovery")
        return True
    except Exception as e:
        log.error(
            f"event=worker_spawn_failed worker_id={worker_id} "
            f"reason=pool_recovery error={e}"
        )
        return False


def maybe_spawn_next_worker(worker_id: int) -> None:
    """
    Expand the pool only when the current worker is already busy with a task.

    The highest active worker is the only one allowed to spawn the next worker.
    This makes pool growth gradual and reduces startup races between idle
    workers competing for the same first tasks.
    """
    try:
        script_name = os.path.basename(__file__)
        current_workers = list_running_workers(script_name)

        if len(current_workers) >= k.BKP_TASK_MAX_WORKERS:
            return

        if not current_workers:
            return

        if worker_id != max(current_workers):
            return

        spawn_additional_worker(current_workers)

    except Exception as e:
        log.warning(f"event=worker_pool_scale_out_failed worker_id={worker_id} error={e}")


def ensure_seed_worker_alive(worker_id: int) -> bool:
    """
    Ensure the seed worker exists so the on-demand pool can recover itself.

    The backup pool scales gradually from worker 0. If worker 0 dies outside the
    normal try/except flow, the remaining workers must restore it, otherwise the
    pool could lose the ability to reignite after the current backlog cools down.

    Only the lowest active survivor is allowed to recreate worker 0. This keeps
    the recovery deterministic and avoids multiple workers racing to spawn the
    same replacement process.
    """
    try:
        current_workers = list_running_workers(os.path.basename(__file__))
        now = time.time()

        if 0 in current_workers:
            process_status["seed_recovery_last_attempt"] = 0.0
            return True

        if not current_workers:
            return False

        if len(current_workers) >= k.BKP_TASK_MAX_WORKERS:
            return False

        if worker_id != min(current_workers):
            return False

        last_attempt = process_status.get("seed_recovery_last_attempt", 0.0)
        if now - last_attempt < k.HOST_BUSY_RETRY:
            return False

        process_status["seed_recovery_last_attempt"] = now
        log.warning(
            f"event=worker_seed_missing worker_id={worker_id} "
            f"active_workers={current_workers}"
        )
        return spawn_specific_worker(0)

    except Exception as e:
        log.warning(
            f"event=worker_seed_guard_failed worker_id={worker_id} error={e}"
        )
        return False


def should_retire_idle_worker(worker_id: int, idle_cycles: int) -> bool:
    """
    Decide whether an extra worker should exit after repeated idle polls.

    Worker 0 stays alive permanently as the seed worker. Extra workers can
    retire once the backlog cools down, which reduces useless polling and
    lowers the chance of races around the next host acquisition.
    """
    if worker_id == 0:
        return False

    if idle_cycles < k.BKP_TASK_IDLE_EXIT_CYCLES:
        return False

    current_workers = list_running_workers(os.path.basename(__file__))

    # Extra workers should not disappear while the seed worker is missing.
    # Keeping one survivor alive buys time for the pool recovery path to
    # recreate worker 0 and prevents the dispatcher from going fully dark.
    if 0 not in current_workers:
        return False

    return len(current_workers) > 1


# ======================================================================
# File Transfer
# ======================================================================
def transfer_file_task(
    sftp,
    remote_dir: str,
    remote_filename: str,
    local_path: str,
    server_filename: str,
    discovery_size_kb: float,
) -> float:
    """
    Transfer a file from a remote host to the local repository with integrity validation.

    This function ensures that the transferred file is valid and consistent with both
    the discovery metadata and the authoritative remote file size.

    Validation rules:

        1. Remote file must exist.
        2. Remote file size must be > 0.
        3. Local file must exist after transfer.
        4. Local file size must be > 0.
        5. Local file must NOT be smaller than the remote file size.
        6. Local file must NOT be smaller than the discovery size beyond a threshold.
        7. Remote file growth during transfer is accepted.

    FILE_THRESHOLD_SIZE_KB is used to tolerate small differences caused by:

        - rounding during discovery
        - filesystem allocation differences
        - minor metadata inconsistencies

    If a valid file already exists locally and its size is within the threshold
    relative to the discovery size, the transfer is skipped.

    Returns
    -------
    float
        Final local file size in KB.

    Raises
    ------
    FileNotFoundError
        If the remote file does not exist.

    RuntimeError
        If integrity validation fails.

    TimeoutError
        If the transfer exceeds HOST_BUSY_TIMEOUT.
    """

    remote_path = f"{remote_dir}/{remote_filename}"
    final_file = os.path.join(local_path, server_filename)
    tmp_file = final_file + ".tmp"

    # ---------------------------------------------------------
    # 0) Local file pre-check (skip download if already valid)
    # ---------------------------------------------------------
    if os.path.exists(final_file):
        local_size_bytes = os.path.getsize(final_file)

        if local_size_bytes > 0:
            local_size_kb = local_size_bytes / 1024

            if local_size_kb + k.FILE_THRESHOLD_SIZE_KB >= discovery_size_kb:
                sftp.log.event(
                    "backup_transfer_skipped",
                    reason="file_already_present",
                    server_file=server_filename,
                )

                return local_size_kb
            else:
                sftp.log.warning(
                    f"event=backup_transfer_redownload reason=threshold_mismatch "
                    f"server_file={server_filename}"
                )
                try:
                    os.remove(final_file)
                except Exception:
                    pass
        else:
            try:
                os.remove(final_file)
            except Exception:
                pass

    # ---------------------------------------------------------
    # Remove leftover tmp from previous crash
    # ---------------------------------------------------------
    if os.path.exists(tmp_file):
        try:
            os.remove(tmp_file)
        except Exception:
            pass

    # ---------------------------------------------------------
    # 1) Validate remote existence
    # ---------------------------------------------------------
    if not sftp.test(remote_path):
        # This exact exception type is part of the backup contract: the caller
        # interprets TRANSFER + FileNotFoundError as terminal source drift and
        # prunes FILE_TASK / FILE_TASK_HISTORY instead of persisting TASK_ERROR.
        raise FileNotFoundError(
            f"Remote file not found: {remote_path}"
        )

    # ---------------------------------------------------------
    # 2) Read authoritative remote size
    # ---------------------------------------------------------
    remote_size_bytes = sftp.size(remote_path)

    if remote_size_bytes <= 0:
        raise RuntimeError(
            f"Remote file size invalid: {remote_size_bytes} bytes"
        )

    # ---------------------------------------------------------
    # 3) Transfer to temporary file
    # ---------------------------------------------------------
    timeout_utils.run_with_timeout(
        lambda: sftp.transfer(remote_path, tmp_file),
        timeout=k.HOST_BUSY_TIMEOUT,
    )

    # ---------------------------------------------------------
    # 4) Validate local existence
    # ---------------------------------------------------------
    if not os.path.exists(tmp_file):
        raise RuntimeError(
            "Backup failed: local file not created after transfer"
        )

    local_size_bytes = os.path.getsize(tmp_file)

    if local_size_bytes <= 0:
        raise RuntimeError(
            "Backup failed: local file size is 0 bytes"
        )

    local_size_kb = local_size_bytes / 1024

    # ---------------------------------------------------------
    # 5) Must not be smaller than remote
    # ---------------------------------------------------------
    # Remote size is the authoritative source during transfer.
    # If the local file is smaller, the transfer is considered corrupted.
    if local_size_bytes < remote_size_bytes:
        raise RuntimeError(
            f"Backup corrupted: local size ({local_size_bytes} bytes) "
            f"is smaller than remote size ({remote_size_bytes} bytes)"
        )

    # ---------------------------------------------------------
    # 6) Must not be smaller than discovery beyond threshold
    # ---------------------------------------------------------
    if local_size_kb + k.FILE_THRESHOLD_SIZE_KB < discovery_size_kb:
        raise RuntimeError(
            f"Backup invalid: local size ({local_size_kb:.2f} KB) "
            f"is smaller than discovery size ({discovery_size_kb:.2f} KB)"
        )

    # ---------------------------------------------------------
    # 7) Accept remote growth (informational only)
    # ---------------------------------------------------------
    if local_size_bytes > remote_size_bytes:
        sftp.log.warning(
            f"event=backup_remote_growth remote_size_bytes={remote_size_bytes} "
            f"local_size_bytes={local_size_bytes}"
        )

    # ---------------------------------------------------------
    # 8) Atomic rename
    # ---------------------------------------------------------
    # `os.rename()` moves only the file entry and does not prune empty source
    # directories, which keeps TMP folders stable even when the last file in a
    # batch is finalized here.
    os.rename(tmp_file, final_file)

    return local_size_kb


# ======================================================================
# Main Execution
# ======================================================================
def main() -> None:
    """
    Run one backup worker process until shutdown is requested.

    Each loop iteration tries to:
        1. select one fair backup candidate
        2. claim the FILE_TASK and its HOST
        3. bootstrap SSH/SFTP
        4. transfer and validate the payload
        5. update history and promote the row to PROCESS
    """
    parse_arguments()
    worker_id = process_status["worker"]

    log.service_start("appCataloga_file_bkp", worker_id=worker_id)
    legacy._random_jitter_sleep()

    # Initialize database handler
    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"event=db_init_failed service=appCataloga_file_bkp error={e}")
        sys.exit(1)

    # =======================================================
    # MAIN LOOP
    # =======================================================
    while process_status["running"]:

        sftp_conn = None
        host = None
        err = errors.ErrorHandler(log)
        file_was_transferred = False
        idle_cycles = process_status.get("idle_cycles", 0)

        task = None
        host_id = None
        file_task_id = None
        server_filename = None
        server_file_path = None
        updated_size_kb = None
        connect_busy = False
        connect_retry_detail = k.SFTP_BUSY_RETRY_DETAIL
        preserve_host_busy_cooldown = False
        input_filename = None

        try:
            # Keep the pool self-healing: if worker 0 disappeared unexpectedly,
            # surviving workers try to restore it before making new lifecycle
            # decisions such as retiring for idleness.
            ensure_seed_worker_alive(worker_id)

            # ==========================================================
            # ACT I — Fetch one pending backup FILE_TASK and atomically lock its host
            # ==========================================================
            row = db.read_file_task(
                task_status=k.TASK_PENDING,
                task_type=k.FILE_TASK_BACKUP_TYPE,
                check_host_busy=True,
                check_host_offline=True,
                lock_host=True,
                reserve_hosts_for_discovery=True,
                fair_by_host=True,
            )

            # The selector applies two scheduling rules before we ever touch
            # the host: reserve hosts that have pending discovery work and pick
            # backup candidates fairly across hosts instead of draining one host
            # to exhaustion while the rest of the fleet waits.
            if not row:
                idle_cycles += 1
                process_status["idle_cycles"] = idle_cycles

                if should_retire_idle_worker(worker_id, idle_cycles):
                    log.event(
                        "worker_retired_idle",
                        worker_id=worker_id,
                        idle_cycles=idle_cycles,
                    )
                    break

                legacy._random_jitter_sleep()
                continue

            task, host_id, file_task_id = row
            idle_cycles = 0
            process_status["idle_cycles"] = 0
            input_filename = os.path.join(
                task["FILE_TASK__NA_HOST_FILE_PATH"],
                task["FILE_TASK__NA_HOST_FILE_NAME"],
            )

            # ==========================================================
            # ACT II — Mark the selected FILE_TASK as RUNNING
            # ==========================================================
            try:
                # The worker claims the FILE_TASK before touching SSH/SFTP so
                # ownership stays deterministic. Doing a separate connectivity
                # probe first would duplicate network traffic, reopen a race
                # with sibling workers, and still require the same claim step
                # afterwards. Transient bootstrap failures are therefore
                # handled by requeueing this same row back to PENDING later.
                # The expected-status guard prevents multiple workers from
                # converting the same pending FILE_TASK into RUNNING.
                result = db.file_task_update(
                        task_id=file_task_id,
                        expected_status=k.TASK_PENDING,
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
                log.event(
                    "backup_started",
                    worker_id=worker_id,
                    host_id=host_id,
                    task_id=file_task_id,
                    file=input_filename,
                )
                if result["rows_affected"] != 1:
                    log.warning(
                        f"event=file_task_claim_race worker_id={worker_id} "
                        f"host_id={host_id} task_id={file_task_id}"
                    )
                    continue

                # A worker only asks for a new sibling after it has already
                # secured work for a concrete host.
                maybe_spawn_next_worker(worker_id)
            except Exception as e:
                err.capture("Failed to lock HOST or FILE_TASK", "LOCK", e)
            
            # ==========================================================
            # ACT III — Bootstrap the remote SSH/SFTP session
            # ==========================================================
            if not err.triggered:
                host = db.host_read_access(host_id)
                if not host:
                    err.capture("Host not found in database", "HOST_READ")
                
                try:
                    sftp_conn, _ = legacy.init_host_context(task, log)
                except Exception as e:
                    if errors.is_transient_sftp_init_error(e):
                        connect_busy = True
                        connect_retry_detail = errors.get_transient_sftp_retry_detail(e)

                        # Transient bootstrap failures do not consume the
                        # FILE_TASK. The row goes back to PENDING in `finally`
                        # so another worker can retry after cooldown.

                        # A transient init failure still returns the current
                        # FILE_TASK to PENDING. Only the subset that smells
                        # like real connectivity trouble asks the queued host
                        # worker for explicit reconciliation.
                        if errors.should_queue_connection_check_for_sftp_init_error(e):
                            try:
                                db.queue_host_task(
                                    host_id=host_id,
                                    task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE,
                                    task_status=k.TASK_PENDING,
                                    filter_dict=k.NONE_FILTER,
                                )
                            except Exception as e_queue:
                                log.error(
                                    "event=queue_host_check_failed "
                                    f"service=appCataloga_file_bkp worker_id={worker_id} "
                                    f"host_id={host_id} task_id={file_task_id} "
                                    f"error={e_queue}"
                                )

                        log.warning_event(
                            "sftp_init_retry",
                            service="appCataloga_file_bkp",
                            worker_id=worker_id,
                            host_id=host_id,
                            task_id=file_task_id,
                            timeout_like=errors.is_timeout_like_sftp_init_error(e),
                            retry_detail=connect_retry_detail,
                            error=e,
                        )
                        continue

                    if isinstance(e, paramiko.AuthenticationException):
                        err.capture(
                            "SSH authentication failed",
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
            
            # ==========================================================
            # ACT IV — Prepare the local repository destination
            # ==========================================================
            if not err.triggered:
                server_file_path = os.path.join(
                    k.REPO_FOLDER, k.TMP_FOLDER, host["host_uid"]
                )
                os.makedirs(server_file_path, exist_ok=True)

            # ---------------------------------------------------
            # Build deterministic server filename
            # ---------------------------------------------------
            # CelPlan payloads keep their original filename because the station
            # naming is already unique enough and downstream tooling expects it.
            # Other hosts use the deterministic hashed naming contract above.
            if not err.triggered:
                if "CW" in host["host_uid"]:
                    server_filename = task["FILE_TASK__NA_HOST_FILE_NAME"]
                else:
                    server_filename = build_server_filename(
                        host_uid=host["host_uid"],
                        remote_path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                        filename=task["FILE_TASK__NA_HOST_FILE_NAME"],
                    )

            # ==========================================================
            # ACT V — Transfer and validate the payload
            # ==========================================================
            if not err.triggered:
                try:
                    updated_size_kb = transfer_file_task(
                        sftp=sftp_conn,
                        remote_dir=task["FILE_TASK__NA_HOST_FILE_PATH"],
                        remote_filename=task["FILE_TASK__NA_HOST_FILE_NAME"],
                        local_path=server_file_path,
                        server_filename=server_filename,
                        discovery_size_kb=task["FILE_TASK__VL_FILE_SIZE_KB"],
                    )
                except Exception as e:
                    # Keep the stage-level reason stable here. The persisted
                    # message is enriched later by ErrorHandler.format_error(),
                    # which now extracts actionable detail from the exception
                    # itself (SSH banner errors, timeouts, permission issues,
                    # etc.) without fragmenting dashboards by raw text.
                    err.capture("File transfer failed", "TRANSFER", e)
            
            if not err.triggered:
                file_was_transferred = True

            # ---------------------------------------------------
            # Update FILE_TASK_HISTORY and promote the pipeline row
            # ---------------------------------------------------
            # `(FK_HOST, NA_HOST_FILE_PATH, NA_HOST_FILE_NAME)` is the logical
            # identity of the source file, so the history row can be updated in
            # place after the transfer succeeds.
            if not err.triggered:
                try:
                    backup_completed_at = datetime.now()
                    db.file_history_update(
                        task_type=k.FILE_TASK_BACKUP_TYPE,
                        host_id=host_id,
                        host_file_path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                        host_file_name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                        DT_BACKUP=backup_completed_at,
                        NA_SERVER_FILE_NAME=server_filename,
                        NA_SERVER_FILE_PATH=server_file_path,
                        NU_STATUS_BACKUP=k.TASK_DONE,
                        VL_FILE_SIZE_KB=updated_size_kb,
                        NA_MESSAGE=tools.compose_message(
                            task_type=k.FILE_TASK_BACKUP_TYPE,
                            task_status=k.TASK_DONE,
                            path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                            name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                        ),
                    )

                    # The same FILE_TASK row continues into PROCESS instead of
                    # creating a second queue record for the same payload.
                    # That preserves one mutable queue row per source artifact
                    # while FILE_TASK_HISTORY remains the immutable audit trail.
                    db.file_task_update(
                        task_id=file_task_id,
                        NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
                        DT_FILE_TASK=backup_completed_at,
                        NU_STATUS=k.TASK_PENDING,
                        NA_SERVER_FILE_PATH=server_file_path,
                        NA_SERVER_FILE_NAME=server_filename,
                        VL_FILE_SIZE_KB=updated_size_kb,
                        NA_MESSAGE=tools.compose_message(
                            task_type=k.FILE_TASK_BACKUP_TYPE,
                            task_status=k.TASK_DONE,
                            path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                            name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                        ),
                    )
                    log.event(
                        "backup_completed",
                        worker_id=worker_id,
                        host_id=host_id,
                        task_id=file_task_id,
                        file=input_filename,
                        final_file=os.path.join(server_file_path, server_filename),
                    )
                except Exception as e:
                    err.capture("Post-transfer update failed", "FINALIZE", e)


        # -------------------------------------------------
        # Unexpected error handling (catch-all)
        # -------------------------------------------------
        except Exception as e:
            if not err.triggered:
                err.capture(
                    reason="Unexpected backup worker failure",
                    stage="BACKUP",
                    exc=e,
                    worker_id=worker_id,
                    host_id=host_id,
                    task_id=file_task_id,
                )
            err.log_error(
                worker_id=worker_id,
                host_id=host_id,
                task_id=file_task_id,
            )

        
        # ==============================================================
        # FINALLY — UNLOCK + CLEANUP (ALWAYS EXECUTED)
        # ==============================================================
        finally:
            # -------------------------------------------------
            # Transient SSH/SFTP bootstrap failures keep the FILE_TASK pending
            # so another worker can retry it later.
            # -------------------------------------------------
            if connect_busy and file_task_id is not None and not err.triggered:
                try:
                    retry_at = datetime.now()
                    db.file_task_update(
                        task_id=file_task_id,
                        DT_FILE_TASK=retry_at,
                        NU_STATUS=k.TASK_PENDING,
                        NA_MESSAGE=tools.compose_message(
                            task_type=k.FILE_TASK_BACKUP_TYPE,
                            task_status=k.TASK_PENDING,
                            path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                            name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                            detail=connect_retry_detail,
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
                            f"service=appCataloga_file_bkp worker_id={worker_id} "
                            f"host_id={host_id} task_id={file_task_id} "
                            f"cooldown_seconds={k.SFTP_BUSY_COOLDOWN_SECONDS}"
                        )
                except Exception as e:
                    log.error(
                        "event=sftp_busy_requeue_failed "
                        f"service=appCataloga_file_bkp worker_id={worker_id} "
                        f"host_id={host_id} task_id={file_task_id} error={e}"
                    )
            # -------------------------------------------------
            # Persist task failure after any non-transient error.
            # -------------------------------------------------
            if err.triggered and file_task_id is not None:
                # Only one transfer failure is treated as terminal drift:
                # discovery saw the file earlier, but the source host no
                # longer exposes it at backup time.
                remote_file_missing = (
                    err.stage == "TRANSFER"
                    and isinstance(err.exc, FileNotFoundError)
                    and task is not None
                )

                # A file that disappeared on the remote host after discovery is
                # treated as terminal drift, not as a retryable backup failure.
                # We prune both the live task and its history snapshot so host
                # recovery logic does not resurrect an artifact that no longer
                # exists on the source station.
                if remote_file_missing:
                    deleted_file_task = None
                    deleted_history = None

                    # Cleanup is best-effort per table. A partial failure should
                    # still remove whatever can be safely pruned instead of
                    # collapsing back into the generic retryable error path.
                    try:
                        deleted_file_task = db.file_task_delete(file_task_id)
                    except Exception as e_db:
                        log.error(
                            "event=backup_missing_remote_file_task_delete_failed "
                            f"worker_id={worker_id} host_id={host_id} "
                            f"task_id={file_task_id} file={input_filename} "
                            f"error={e_db}"
                        )

                    try:
                        deleted_history = db.file_history_delete(
                            host_id=host_id,
                            host_file_path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                            host_file_name=task["FILE_TASK__NA_HOST_FILE_NAME"],
                        )
                    except Exception as e_db:
                        log.error(
                            "event=backup_missing_remote_history_delete_failed "
                            f"worker_id={worker_id} host_id={host_id} "
                            f"task_id={file_task_id} file={input_filename} "
                            f"error={e_db}"
                        )

                    log.warning_event(
                        "backup_remote_file_missing_pruned",
                        worker_id=worker_id,
                        host_id=host_id,
                        task_id=file_task_id,
                        file=input_filename,
                        deleted_file_task=deleted_file_task,
                        deleted_history=deleted_history,
                        reason=str(err.exc) if err.exc else None,
                    )

                if not remote_file_missing:
                    err.log_error(host_id=host_id, task_id=file_task_id)

                    # Build Message with error details for both FILE_TASK and FILE_HISTORY
                    NA_MESSAGE = tools.compose_message(
                        task_type=k.FILE_TASK_BACKUP_TYPE,
                        task_status=k.TASK_ERROR,
                        path=task["FILE_TASK__NA_HOST_FILE_PATH"] if task else None,
                        name=task["FILE_TASK__NA_HOST_FILE_NAME"] if task else None,
                        error=err.format_error(),
                    )
                    error_at = datetime.now()

                    try:
                        db.file_task_update(
                            task_id=file_task_id,
                            NU_TYPE=k.FILE_TASK_BACKUP_TYPE,
                            DT_FILE_TASK=error_at,
                            NU_STATUS=k.TASK_ERROR,
                            NA_MESSAGE=NA_MESSAGE,
                        )

                        db.file_history_update(
                            task_type=k.FILE_TASK_BACKUP_TYPE,
                            host_id=host_id,
                            host_file_path=task["FILE_TASK__NA_HOST_FILE_PATH"] if task else None,
                            host_file_name=task["FILE_TASK__NA_HOST_FILE_NAME"] if task else None,
                            DT_BACKUP=error_at,
                            NA_SERVER_FILE_NAME=server_filename,
                            NA_SERVER_FILE_PATH=server_file_path,
                            NU_STATUS_BACKUP=k.TASK_ERROR,
                            NA_MESSAGE=NA_MESSAGE,
                        )

                        # Host check tasks should be re-queued on connection
                        # errors to allow for retries after transient issues are resolved
                        if err.stage in {"CONNECT", "SSH"}:
                            # The queued host worker reconciles host state out
                            # of band so this backup worker can close its task
                            # deterministically and move on.
                            db.queue_host_task(
                                host_id=host_id,
                                task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE,
                                task_status=k.TASK_PENDING,
                                filter_dict=k.NONE_FILTER,
                            )

                    except Exception as e_db:
                        log.error(f"event=finalize_error_persist_failed error={e_db}")

                    log.error_event(
                        "backup_error",
                        worker_id=worker_id,
                        host_id=host_id,
                        task_id=file_task_id,
                        file=input_filename,
                        final_file=(
                            os.path.join(server_file_path, server_filename)
                            if server_file_path and server_filename else None
                        ),
                        error=err.format_error() or "Backup failed",
                    )

            # -------------------------------------------------
            # Close transport objects defensively. Cleanup must not hide the
            # result of the transfer attempt.
            # -------------------------------------------------
            if sftp_conn:
                try:
                    sftp_conn.close()
                    time.sleep(0.3)  # Ensure proper closure before next connection
                except Exception:
                    pass

            # -------------------------------------------------
            # Release the host unless a short transient cooldown is now
            # intentionally holding the BUSY flag.
            # -------------------------------------------------
            if host_id is not None and not preserve_host_busy_cooldown:
                # This is the single normal-path release point for the host
                # claimed by `read_file_task(..., lock_host=True)`.
                release_locked_host(db, host_id)

                # Successful backup activity schedules deferred statistics
                # refresh for the host through the queued HOST_TASK path.
                if not err.triggered and file_was_transferred:
                    try:
                        # Stats are deferred on purpose so repository I/O is
                        # done before any host-level aggregation work begins.
                        db.host_task_statistics_create(host_id=host_id)
                    except Exception:
                        pass

            legacy._random_jitter_sleep()

    log.service_stop("appCataloga_file_bkp", worker_id=worker_id)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        worker_id = process_status.get("worker", 0)
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal backup worker crash",
            stage="MAIN",
            exc=e,
            worker_id=worker_id,
        )
        err.log_error(worker_id=worker_id)
        release_busy_hosts_on_exit()
        raise
