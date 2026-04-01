#! /usr/bin/python3
"""
Backup worker for remote host payload collection.

This worker transfers pending backup FILE_TASK rows from remote hosts into the
central repository through SFTP and then promotes successful rows into the
processing queue.

In the larger pipeline, backup is the bridge between:
    - one FILE_TASK row in BACKUP state for a remote source artifact
    - the same FILE_TASK row promoted to PROCESS after a successful transfer

So unlike discovery, which fans out one host pass into many file tasks, backup
walks a single file artifact through transfer, validation, and promotion while
still honoring host-level BUSY ownership.

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
import signal
import sys
import time
import traceback
from datetime import datetime

from bootstrap_paths import bootstrap_app_paths

# Keep the entrypoint bootstrap tiny: one helper prepares the app root,
# config directory, and local DB package for every worker.
PROJECT_ROOT = bootstrap_app_paths(__file__)

# Import customized libs
from db.dbHandlerBKP import dbHandlerBKP
from host_handler import bootstrap_flow, host_runtime
from server_handler import signal_runtime, sleep as runtime_sleep, worker_pool
from shared import (
    errors,
    file_metadata,
    logging_utils,
    tools,
)
import config as k


# ======================================================================
# Globals
# ======================================================================
SERVICE_NAME = "appCataloga_file_bkp"
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
def _shutdown_cleanup(signal_name: str) -> None:
    """
    Stop sibling workers and release BUSY resources during shutdown.

    Backup is the only main worker here that owns a small process pool, so its
    shutdown cleanup has two responsibilities:
        1. tell sibling workers to stop
        2. release any HOST rows still marked BUSY by this PID
    """
    worker_pool.broadcast_shutdown_to_worker_pool(
        signal_name,
        process_status=process_status,
        logger=log,
        script_path=__file__,
    )
    host_runtime.release_busy_hosts_for_current_pid(
        db_factory=dbHandlerBKP,
        database_name=k.BKP_DATABASE_NAME,
        logger=log,
    )


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
    on_shutdown=_shutdown_cleanup,
)


def _requeue_transient_bootstrap_failure(
    db: dbHandlerBKP,
    *,
    worker_id: int,
    host_id: int,
    file_task_id: int,
    task: dict,
    exc: Exception,
) -> bool:
    """
    Return the same FILE_TASK to PENDING after a transient SSH/SFTP failure.

    Backup owns a slightly different retry contract from discovery because the
    queue row being recycled here is a FILE_TASK, not a HOST_TASK. The shared
    bootstrap helper delegates to this function so worker-specific persistence
    stays explicit in the entrypoint.

    "Transient" here means the bootstrap failure is weak evidence. We should
    retry the same file later instead of turning it into TASK_ERROR right away.
    """
    retry_detail = errors.get_transient_sftp_retry_detail(exc)
    input_path = task["FILE_TASK__NA_HOST_FILE_PATH"]
    input_name = task["FILE_TASK__NA_HOST_FILE_NAME"]

    # Some retryable bootstrap failures still look suspicious enough that the
    # queued host worker should reconcile the host out of band.
    if errors.should_queue_host_check(exc):
        try:
            db.queue_host_task(
                host_id=host_id,
                task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE,
                task_status=k.TASK_PENDING,
                filter_dict=k.NONE_FILTER,
            )
        except Exception as queue_exc:
            log.error(
                "event=queue_host_check_failed "
                f"service=appCataloga_file_bkp worker_id={worker_id} "
                f"host_id={host_id} task_id={file_task_id} "
                f"error={queue_exc}"
            )

    # The live FILE_TASK goes back to PENDING so another worker can retry the
    # same artifact later without creating a second queue row for it.
    db.file_task_update(
        task_id=file_task_id,
        DT_FILE_TASK=datetime.now(),
        NU_STATUS=k.TASK_PENDING,
        NA_MESSAGE=tools.compose_message(
            task_type=k.FILE_TASK_BACKUP_TYPE,
            task_status=k.TASK_PENDING,
            path=input_path,
            name=input_name,
            detail=retry_detail,
        ),
    )

    preserve_host_busy_cooldown = db.host_start_transient_busy_cooldown(
        host_id=host_id,
        owner_pid=os.getpid(),
        cooldown_seconds=k.SFTP_BUSY_COOLDOWN_SECONDS,
    )

    log.warning_event(
        "sftp_init_retry",
        service="appCataloga_file_bkp",
        worker_id=worker_id,
        host_id=host_id,
        task_id=file_task_id,
        timeout_like=errors.is_timeout_like_sftp_init_error(exc),
        retry_detail=retry_detail,
        error=exc,
    )

    if preserve_host_busy_cooldown:
        log.warning(
            "event=sftp_busy_cooldown_started "
            f"service=appCataloga_file_bkp worker_id={worker_id} "
            f"host_id={host_id} task_id={file_task_id} "
            f"cooldown_seconds={k.SFTP_BUSY_COOLDOWN_SECONDS}"
        )

    return preserve_host_busy_cooldown


def _claim_backup_task(
    db: dbHandlerBKP,
    *,
    worker_id: int,
    host_id: int,
    file_task_id: int,
    task: dict,
    input_filename: str,
) -> bool:
    """
    Atomically convert one backup FILE_TASK from PENDING to RUNNING.

    This helper owns two related policies:
        1. deterministic claim of the queue row before opening SSH/SFTP
        2. opportunistic pool growth only after a worker has secured real work

    Returns:
        bool: False when another worker already claimed the row and the caller
        should simply fetch another candidate.

    Claim and pool-growth live together here because they are one policy
    bundle: backup only scales out after a worker has secured concrete work.
    """
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

    if result["rows_affected"] != 1:
        log.warning(
            f"event=file_task_claim_race worker_id={worker_id} "
            f"host_id={host_id} task_id={file_task_id}"
        )
        return False

    log.event(
        "backup_started",
        worker_id=worker_id,
        host_id=host_id,
        task_id=file_task_id,
        file=input_filename,
    )

    # Only a worker that already owns concrete work is allowed to ask for the
    # next sibling. This keeps pool growth demand-driven instead of speculative.
    worker_pool.maybe_spawn_next_worker(
        worker_id,
        script_path=__file__,
        max_workers=k.BKP_TASK_MAX_WORKERS,
        logger=log,
    )

    return True


def _has_discovery_metadata_drift(
    task: dict,
    remote_metadata: file_metadata.FileMetadata,
) -> bool:
    """
    Return True when the transfer-time remote snapshot differs from discovery.

    Discovery may run long before backup, so a remote file can be recreated in
    place with the same pathname but different size or timestamps. Backup
    treats that as legitimate source drift and refreshes the stored metadata
    instead of rejecting the transfer as corrupted.
    """
    discovery_created = task.get("FILE_TASK__DT_FILE_CREATED")
    if isinstance(discovery_created, datetime):
        discovery_created = discovery_created.replace(microsecond=0)
    else:
        discovery_created = None

    discovery_modified = task.get("FILE_TASK__DT_FILE_MODIFIED")
    if isinstance(discovery_modified, datetime):
        discovery_modified = discovery_modified.replace(microsecond=0)
    else:
        discovery_modified = None

    remote_created = remote_metadata.DT_FILE_CREATED
    if isinstance(remote_created, datetime):
        remote_created = remote_created.replace(microsecond=0)

    remote_modified = remote_metadata.DT_FILE_MODIFIED
    if isinstance(remote_modified, datetime):
        remote_modified = remote_modified.replace(microsecond=0)

    return any((
        task.get("FILE_TASK__NA_EXTENSION") != remote_metadata.NA_EXTENSION,
        task.get("FILE_TASK__VL_FILE_SIZE_KB") != remote_metadata.VL_FILE_SIZE_KB,
        discovery_created != remote_created,
        discovery_modified != remote_modified,
    ))


def _finalize_successful_backup(
    db: dbHandlerBKP,
    *,
    worker_id: int,
    host_id: int,
    file_task_id: int,
    task: dict,
    input_filename: str,
    server_filename: str,
    server_file_path: str,
    refreshed_metadata: file_metadata.FileMetadata,
    updated_size_kb: float,
) -> None:
    """
    Persist the successful BACKUP stage and promote the live row to PROCESS.

    FILE_TASK_HISTORY records the immutable audit of the completed backup.
    The live FILE_TASK row then moves forward to PROCESS so the pipeline keeps
    one mutable queue row per source artifact.

    That split is intentional:
        - history answers "what happened?"
        - the live task answers "what should happen next?"

    Discovery-time metadata may already be stale when backup finally runs, so
    this step persists the refreshed remote snapshot gathered during transfer.
    """
    backup_completed_at = datetime.now()

    # First write the immutable history snapshot of the completed transfer.
    db.file_history_update(
        task_type=k.FILE_TASK_BACKUP_TYPE,
        host_id=host_id,
        host_file_path=task["FILE_TASK__NA_HOST_FILE_PATH"],
        host_file_name=task["FILE_TASK__NA_HOST_FILE_NAME"],
        DT_BACKUP=backup_completed_at,
        NA_SERVER_FILE_NAME=server_filename,
        NA_SERVER_FILE_PATH=server_file_path,
        NU_STATUS_BACKUP=k.TASK_DONE,
        NA_EXTENSION=refreshed_metadata.NA_EXTENSION,
        VL_FILE_SIZE_KB=updated_size_kb,
        DT_FILE_CREATED=refreshed_metadata.DT_FILE_CREATED,
        DT_FILE_MODIFIED=refreshed_metadata.DT_FILE_MODIFIED,
        NA_MESSAGE=tools.compose_message(
            task_type=k.FILE_TASK_BACKUP_TYPE,
            task_status=k.TASK_DONE,
            path=task["FILE_TASK__NA_HOST_FILE_PATH"],
            name=task["FILE_TASK__NA_HOST_FILE_NAME"],
        ),
    )

    # Then move the live queue row forward to PROCESS using the same file
    # identity instead of creating a second task row for the same artifact.
    db.file_task_update(
        task_id=file_task_id,
        NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
        DT_FILE_TASK=backup_completed_at,
        NU_STATUS=k.TASK_PENDING,
        NA_SERVER_FILE_PATH=server_file_path,
        NA_SERVER_FILE_NAME=server_filename,
        NA_EXTENSION=refreshed_metadata.NA_EXTENSION,
        VL_FILE_SIZE_KB=updated_size_kb,
        DT_FILE_CREATED=refreshed_metadata.DT_FILE_CREATED,
        DT_FILE_MODIFIED=refreshed_metadata.DT_FILE_MODIFIED,
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


def _persist_backup_error(
    db: dbHandlerBKP,
    err: errors.ErrorHandler,
    *,
    worker_id: int,
    host_id: int | None,
    file_task_id: int,
    task: dict | None,
    input_filename: str | None,
    server_filename: str | None,
    server_file_path: str | None,
) -> None:
    """
    Persist a terminal backup failure or prune source drift when the file vanished.

    Backup has one special terminal case that discovery does not have: the file
    may legitimately disappear between discovery and backup. That case is
    treated as source drift and pruned, not persisted as TASK_ERROR.

    This helper therefore has two finalization modes:
        1. prune source drift when the remote file vanished after discovery
        2. persist FILE_TASK / FILE_TASK_HISTORY error state for everything else
    """
    remote_file_missing = (
        err.stage == "TRANSFER"
        and isinstance(err.exc, FileNotFoundError)
        and task is not None
    )

    if remote_file_missing:
        deleted_file_task = None
        deleted_history = None

        # Best-effort prune per table. A partial cleanup is still better than
        # leaving both rows around to be resurrected later.
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
        return

    # For ordinary terminal failures, log once with the structured root cause
    # before persisting the queue/history error state.
    err.log_error(
        worker_id=worker_id,
        host_id=host_id,
        task_id=file_task_id,
        file=input_filename,
    )

    na_message = tools.compose_message(
        task_type=k.FILE_TASK_BACKUP_TYPE,
        task_status=k.TASK_ERROR,
        path=task["FILE_TASK__NA_HOST_FILE_PATH"] if task else None,
        name=task["FILE_TASK__NA_HOST_FILE_NAME"] if task else None,
        error=err.format_error(),
    )
    error_at = datetime.now()

    try:
        # The live FILE_TASK returns to BACKUP/ERROR because this worker owns
        # the failed transfer attempt. It should no longer advertise a worker
        # PID once it leaves RUNNING.
        db.file_task_update(
            task_id=file_task_id,
            NU_TYPE=k.FILE_TASK_BACKUP_TYPE,
            DT_FILE_TASK=error_at,
            NU_STATUS=k.TASK_ERROR,
            NU_PID=None,
            NA_MESSAGE=na_message,
        )

        # FILE_TASK_HISTORY mirrors the same backup-stage error so audit
        # queries and operational queue state do not diverge.
        db.file_history_update(
            task_type=k.FILE_TASK_BACKUP_TYPE,
            host_id=host_id,
            host_file_path=task["FILE_TASK__NA_HOST_FILE_PATH"] if task else None,
            host_file_name=task["FILE_TASK__NA_HOST_FILE_NAME"] if task else None,
            DT_BACKUP=error_at,
            NA_SERVER_FILE_NAME=server_filename,
            NA_SERVER_FILE_PATH=server_file_path,
            NU_STATUS_BACKUP=k.TASK_ERROR,
            NA_MESSAGE=na_message,
        )

        # Fatal bootstrap failures ask host_check to reconcile host state out of
        # band so this worker can close the FILE_TASK deterministically.
        if err.stage in {"AUTH", "CONNECT", "SSH"}:
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
# File Transfer
# ======================================================================
def transfer_file_task(
    sftp,
    remote_dir: str,
    remote_filename: str,
    local_path: str,
    server_filename: str,
    task: dict,
) -> tuple[float, file_metadata.FileMetadata]:
    """
    Transfer a file from a remote host to the local repository with integrity validation.

    Backup re-checks the remote file metadata immediately before transfer. That
    fresh snapshot becomes the source of truth for this stage, because the file
    may have changed since discovery originally queued the FILE_TASK row.

    Validation rules:

        1. Remote file must exist.
        2. Remote metadata is refreshed before any skip decision is made.
        3. Local file must exist after transfer.
        4. Local file size must be > 0.
        5. Local file must NOT be smaller than the authoritative remote size.
        6. Remote file growth during transfer is accepted.

    The discovery snapshot is still useful, but only as drift detection. If
    the file was recreated in place weeks later, backup should refresh the
    metadata instead of failing because the old discovery size no longer fits.

    `FILE_THRESHOLD_SIZE_KB` is now used only for the "already present"
    shortcut. We skip a download only when:
        - the existing local payload still matches the current remote size, and
        - the current remote metadata still matches the old discovery snapshot

    Returns
    -------
    tuple[float, FileMetadata]
        Final local file size in KB and the refreshed remote metadata snapshot.

    Raises
    ------
    FileNotFoundError
        If the remote file does not exist.

    RuntimeError
        If integrity validation fails.

    TimeoutError
        If the transfer stalls or exceeds the configured watchdog limits.
    """

    remote_path = f"{remote_dir}/{remote_filename}"
    final_file = os.path.join(local_path, server_filename)
    tmp_file = final_file + ".tmp"
    remote_metadata = sftp.read_file_metadata(remote_path)
    remote_size_bytes = sftp.size(remote_path)

    if remote_size_bytes <= 0:
        raise RuntimeError(
            f"Remote file size invalid: {remote_size_bytes} bytes"
        )

    metadata_drift = _has_discovery_metadata_drift(task, remote_metadata)
    if metadata_drift:
        sftp.log.warning(
            "event=backup_metadata_refreshed "
            f"server_file={server_filename} remote_file={remote_path} "
            f"old_size_kb={task.get('FILE_TASK__VL_FILE_SIZE_KB')} "
            f"new_size_kb={remote_metadata.VL_FILE_SIZE_KB}"
        )

    # ---------------------------------------------------------
    # 0) Local file pre-check (skip download if already valid)
    # ---------------------------------------------------------
    if os.path.exists(final_file):
        local_size_bytes = os.path.getsize(final_file)

        if local_size_bytes > 0:
            local_size_kb = local_size_bytes / 1024

            if (
                not metadata_drift
                and abs(local_size_kb - remote_metadata.VL_FILE_SIZE_KB)
                <= k.FILE_THRESHOLD_SIZE_KB
            ):
                sftp.log.event(
                    "backup_transfer_skipped",
                    reason="file_already_present",
                    server_file=server_filename,
                )

                return local_size_kb, remote_metadata
            else:
                sftp.log.warning(
                    f"event=backup_transfer_redownload "
                    f"reason={'metadata_drift' if metadata_drift else 'remote_size_mismatch'} "
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
    # 1) Transfer to temporary file
    # ---------------------------------------------------------
    sftp.transfer(
        remote_path,
        tmp_file,
        max_seconds=k.BACKUP_TRANSFER_MAX_SECONDS,
        stall_timeout_seconds=k.BACKUP_TRANSFER_STALL_TIMEOUT_SECONDS,
        progress_poll_seconds=k.BACKUP_TRANSFER_PROGRESS_POLL_SECONDS,
        heartbeat_seconds=k.BACKUP_TRANSFER_HEARTBEAT_SECONDS,
    )

    # ---------------------------------------------------------
    # 2) Validate local existence
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
    # 3) Must not be smaller than remote
    # ---------------------------------------------------------
    # Remote size is the authoritative source during transfer.
    # If the local file is smaller, the transfer is considered corrupted.
    if local_size_bytes < remote_size_bytes:
        raise RuntimeError(
            f"Backup corrupted: local size ({local_size_bytes} bytes) "
            f"is smaller than remote size ({remote_size_bytes} bytes)"
        )

    # ---------------------------------------------------------
    # 4) Accept remote growth (informational only)
    # ---------------------------------------------------------
    if local_size_bytes > remote_size_bytes:
        sftp.log.warning(
            f"event=backup_remote_growth remote_size_bytes={remote_size_bytes} "
            f"local_size_bytes={local_size_bytes}"
        )

    # ---------------------------------------------------------
    # 5) Atomic rename
    # ---------------------------------------------------------
    # `os.rename()` moves only the file entry and does not prune empty source
    # directories, which keeps TMP folders stable even when the last file in a
    # batch is finalized here.
    os.rename(tmp_file, final_file)

    return local_size_kb, remote_metadata


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

    The worker keeps those phases visible in this file on purpose. The helpers
    extracted above remove repeated local policy, but the full lifecycle of one
    backup pass should still read top-to-bottom in the entrypoint.
    """
    parse_arguments()
    worker_id = process_status["worker"]

    log.service_start(SERVICE_NAME, worker_id=worker_id)
    runtime_sleep.random_jitter_sleep()

    # Initialize database handler
    try:
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"event=db_init_failed service={SERVICE_NAME} error={e}")
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
        refreshed_metadata = None
        updated_size_kb = None
        preserve_host_busy_cooldown = False
        input_filename = None

        try:
            # Keep the pool self-healing: if worker 0 disappeared unexpectedly,
            # surviving workers try to restore it before making new lifecycle
            # decisions such as retiring for idleness.
            worker_pool.ensure_seed_worker_alive(
                worker_id,
                process_status=process_status,
                script_path=__file__,
                max_workers=k.BKP_TASK_MAX_WORKERS,
                retry_seconds=k.MAX_FILE_TASK_WAIT_TIME,
                logger=log,
            )

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

                if worker_pool.should_retire_idle_worker(
                    worker_id,
                    idle_cycles,
                    script_path=__file__,
                    idle_exit_cycles=k.BKP_TASK_IDLE_EXIT_CYCLES,
                    logger=log,
                ):
                    log.event(
                        "worker_retired_idle",
                        worker_id=worker_id,
                        idle_cycles=idle_cycles,
                    )
                    break

                # Idle workers just drift back to the common finally/jitter
                # path instead of spinning hot when the queue is empty.
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
                if not _claim_backup_task(
                    db,
                    worker_id=worker_id,
                    host_id=host_id,
                    file_task_id=file_task_id,
                    task=task,
                    input_filename=input_filename,
                ):
                    continue
            except Exception as e:
                err.capture(
                    "Failed to lock HOST or FILE_TASK",
                    "LOCK",
                    e,
                    worker_id=worker_id,
                    host_id=host_id,
                    task_id=file_task_id,
                )
                continue
            
            # ==========================================================
            # ACT III — Bootstrap the remote SSH/SFTP session
            # ==========================================================
            # Read the authoritative host view after the task claim. Backup
            # needs host metadata both for the SSH session and for local file
            # naming/layout decisions.
            host = db.host_read_access(host_id)
            if not host:
                err.capture(
                    "Host not found in database",
                    "HOST_READ",
                    worker_id=worker_id,
                    host_id=host_id,
                    task_id=file_task_id,
                )
                continue

            sftp_conn, _, preserve_host_busy_cooldown = (
                bootstrap_flow.init_host_context_with_retry(
                    task=task,
                    log=log,
                    err=err,
                    host_id=host_id,
                    task_id=file_task_id,
                    transient_retry_handler=_requeue_transient_bootstrap_failure,
                    retry_handler_kwargs={
                        "db": db,
                        "worker_id": worker_id,
                        "host_id": host_id,
                        "file_task_id": file_task_id,
                        "task": task,
                    },
                    retry_failure_reason="Failed to requeue transient backup task",
                )
            )
            if sftp_conn is None:
                # The shared bootstrap flow already decided whether this was:
                #   - a transient retryable failure, or
                #   - a fatal AUTH/CONNECT/SSH error stored in `err`
                continue
            
            # ==========================================================
            # ACT IV — Prepare the local repository destination
            # ==========================================================
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
            try:
                updated_size_kb, refreshed_metadata = transfer_file_task(
                    sftp=sftp_conn,
                    remote_dir=task["FILE_TASK__NA_HOST_FILE_PATH"],
                    remote_filename=task["FILE_TASK__NA_HOST_FILE_NAME"],
                    local_path=server_file_path,
                    server_filename=server_filename,
                    task=task,
                )
            except Exception as e:
                # Keep the stage-level reason stable here. The persisted
                # message is enriched later by ErrorHandler.format_error(),
                # which now extracts actionable detail from the exception
                # itself without fragmenting dashboards by raw text.
                err.capture(
                    "File transfer failed",
                    "TRANSFER",
                    e,
                    worker_id=worker_id,
                    host_id=host_id,
                    task_id=file_task_id,
                )
                continue

            file_was_transferred = True

            # ---------------------------------------------------
            # Update FILE_TASK_HISTORY and promote the pipeline row
            # ---------------------------------------------------
            # `(FK_HOST, NA_HOST_FILE_PATH, NA_HOST_FILE_NAME)` is the logical
            # identity of the source file, so the history row can be updated in
            # place after the transfer succeeds.
            try:
                _finalize_successful_backup(
                    db,
                    worker_id=worker_id,
                    host_id=host_id,
                    file_task_id=file_task_id,
                    task=task,
                    input_filename=input_filename,
                    server_filename=server_filename,
                    server_file_path=server_file_path,
                    refreshed_metadata=refreshed_metadata,
                    updated_size_kb=updated_size_kb,
                )
            except Exception as e:
                err.capture(
                    "Post-transfer update failed",
                    "FINALIZE",
                    e,
                    worker_id=worker_id,
                    host_id=host_id,
                    task_id=file_task_id,
                )
                continue


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
                    traceback=traceback.format_exc(),
                )

        
        # ==============================================================
        # FINALLY — UNLOCK + CLEANUP (ALWAYS EXECUTED)
        # ==============================================================
        finally:
            # Phase 1 — Persist the final task outcome if this loop iteration
            # crossed from retryable uncertainty into a stable terminal error.
            if err.triggered:
                if file_task_id is not None:
                    _persist_backup_error(
                        db,
                        err,
                        worker_id=worker_id,
                        host_id=host_id,
                        file_task_id=file_task_id,
                        task=task,
                        input_filename=input_filename,
                        server_filename=server_filename,
                        server_file_path=server_file_path,
                    )
                else:
                    err.log_error(
                        worker_id=worker_id,
                        host_id=host_id,
                        task_id=file_task_id,
                    )

            # Phase 2 — Close transport objects defensively. Cleanup must never
            # overwrite the actual transfer result with a secondary close issue.
            if sftp_conn:
                try:
                    sftp_conn.close()
                    time.sleep(0.3)  # Ensure proper closure before next connection
                except Exception:
                    pass

            # Phase 3 — Release the host unless transient bootstrap retry is
            # intentionally preserving BUSY for its short cooldown window.
            if host_id is not None and not preserve_host_busy_cooldown:
                # This is the single normal-path release point for the host
                # claimed by `read_file_task(..., lock_host=True)`.
                host_runtime.release_locked_host(
                    db,
                    host_id,
                    logger=log,
                    service_name=SERVICE_NAME,
                )

                # Phase 4 — Successful backup activity schedules deferred
                # statistics refresh only after repository I/O and host release
                # are already complete.
                if not err.triggered and file_was_transferred:
                    try:
                        # Stats are deferred on purpose so repository I/O is
                        # done before any host-level aggregation work begins.
                        db.host_task_statistics_create(host_id=host_id)
                    except Exception:
                        pass

            # Phase 5 — Every iteration ends with the same jitter contract so
            # the worker does not spin too aggressively on hot success/error paths.
            runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME, worker_id=worker_id)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # This outer boundary is for daemon-level failure, not one file pass.
        # If we get here the worker process itself is crashing, so we log once,
        # release BUSY hosts owned by this PID, and let the exception terminate.
        worker_id = process_status.get("worker", 0)
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal backup worker crash",
            stage="MAIN",
            exc=e,
            worker_id=worker_id,
        )
        err.log_error(worker_id=worker_id)
        host_runtime.release_busy_hosts_for_current_pid(
            db_factory=dbHandlerBKP,
            database_name=k.BKP_DATABASE_NAME,
            logger=log,
        )
        raise
