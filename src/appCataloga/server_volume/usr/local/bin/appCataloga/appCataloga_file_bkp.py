#! /usr/bin/python3
"""
Backup worker: transfers FILE_TASK rows from remote hosts to the central repository.

Consumes FILE_TASK rows of type BACKUP and locks HOST.IS_BUSY during each transfer.
Worker 0 manages a small on-demand pool; siblings are spawned after each successful claim.
"""

import os
import sys
import time
import traceback
from datetime import datetime

from bootstrap_paths import bootstrap_app_paths

# Keep the entrypoint bootstrap tiny: one helper prepares the app root,
# config directory, and local DB package for every worker.
PROJECT_ROOT = bootstrap_app_paths(__file__)

from db.dbHandlerBKP import dbHandlerBKP
from host_handler import host_context, host_runtime
from host_handler.host_ssh_utils import sftpConnection
from server_handler import signal_runtime, sleep as runtime_sleep, worker_pool
from shared import (
    errors,
    file_utils,
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
# Signal handling
# ======================================================================
def _shutdown_cleanup(signal_name: str) -> None:
    """
    Stop sibling workers and release BUSY resources during shutdown.

    Backup is the only main worker here that owns a small process pool, so its
    shutdown cleanup has two responsibilities:
        1. stop sibling workers
        2. release HOST rows still owned by this PID
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


def _claim_task(db: dbHandlerBKP, task: dict) -> bool:
    """
    Atomically move one backup FILE_TASK from PENDING to RUNNING.

    Backup grows its worker pool only after a real claim succeeds.
    This keeps scale-out tied to actual queue pressure.
    """
    worker_id = process_status["worker"]
    message = tools.compose_message(
        task_type=k.FILE_TASK_BACKUP_TYPE,
        task_status=k.TASK_RUNNING,
        path=task["FILE_TASK__NA_HOST_FILE_PATH"],
        name=task["FILE_TASK__NA_HOST_FILE_NAME"],
    )

    result = db.file_task_update(
        task_id=task["file_task_id"],
        expected_status=k.TASK_PENDING,
        DT_FILE_TASK=datetime.now(),
        NU_STATUS=k.TASK_RUNNING,
        NU_PID=os.getpid(),
        NA_MESSAGE=message,
        **errors.persisted_error_fields_from_handler(message=message),
    )

    if result["rows_affected"] != 1:
        log.warning_event(
            "task_claim_race",
            service=SERVICE_NAME,
            worker_id=worker_id,
            host_id=task["host_id"],
            task_id=task["file_task_id"],
            task_type=k.FILE_TASK_BACKUP_TYPE,
        )
        return False

    log.task_claimed(
        SERVICE_NAME,
        worker_id=worker_id,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=k.FILE_TASK_BACKUP_TYPE,
        file=task["input_filename"],
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



def _finalize_success(
    db: dbHandlerBKP,
    task: dict,
    result: dict,
    *,
    elapsed_sec: float,
) -> None:
    """
    Persist BACKUP DONE, promote the row to PROCESS, and log completion.

    `(FK_HOST, NA_HOST_FILE_PATH, NA_HOST_FILE_NAME)` is the logical identity
    of the source file, so the FILE_TASK row is updated in place rather than
    creating a second row for the same artifact.
    """
    worker_id = process_status["worker"]
    host_id = task["host_id"]
    file_task_id = task["file_task_id"]
    input_filename = task["input_filename"]
    server_filename = task["server_filename"]
    server_file_path = task["server_file_path"]
    refreshed_metadata = result["refreshed_metadata"]
    updated_size_kb = result["updated_size_kb"]
    backup_completed_at = datetime.now()
    history_message = tools.compose_message(
        task_type=k.FILE_TASK_BACKUP_TYPE,
        task_status=k.TASK_DONE,
        path=task["FILE_TASK__NA_HOST_FILE_PATH"],
        name=task["FILE_TASK__NA_HOST_FILE_NAME"],
    )

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
        NA_EXTENSION_HOST=refreshed_metadata.NA_EXTENSION,
        VL_FILE_SIZE_KB_HOST=updated_size_kb,
        DT_FILE_CREATED_HOST=refreshed_metadata.DT_FILE_CREATED,
        DT_FILE_MODIFIED_HOST=refreshed_metadata.DT_FILE_MODIFIED,
        NA_EXTENSION_SERVER=refreshed_metadata.NA_EXTENSION,
        VL_FILE_SIZE_KB_SERVER=updated_size_kb,
        DT_FILE_CREATED_SERVER=refreshed_metadata.DT_FILE_CREATED,
        DT_FILE_MODIFIED_SERVER=refreshed_metadata.DT_FILE_MODIFIED,
        NA_MESSAGE=history_message,
        **errors.persisted_error_fields_from_handler(message=history_message),
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
        NA_EXTENSION_HOST=refreshed_metadata.NA_EXTENSION,
        VL_FILE_SIZE_KB_HOST=updated_size_kb,
        DT_FILE_CREATED_HOST=refreshed_metadata.DT_FILE_CREATED,
        DT_FILE_MODIFIED_HOST=refreshed_metadata.DT_FILE_MODIFIED,
        NA_EXTENSION_SERVER=refreshed_metadata.NA_EXTENSION,
        VL_FILE_SIZE_KB_SERVER=updated_size_kb,
        DT_FILE_CREATED_SERVER=refreshed_metadata.DT_FILE_CREATED,
        DT_FILE_MODIFIED_SERVER=refreshed_metadata.DT_FILE_MODIFIED,
        NA_MESSAGE=history_message,
        **errors.persisted_error_fields_from_handler(message=history_message),
    )

    log.task_done(
        SERVICE_NAME,
        worker_id=worker_id,
        host_id=host_id,
        task_id=file_task_id,
        task_type=k.FILE_TASK_BACKUP_TYPE,
        file=input_filename,
        final_file=os.path.join(server_file_path, server_filename),
        elapsed_sec=round(elapsed_sec, 3),
    )


def _finalize_error(
    db: dbHandlerBKP,
    task: dict | None,
    err: errors.ErrorHandler,
) -> None:
    """Persist backup failure or prune source drift when the file vanished."""
    worker_id = process_status["worker"]

    if task is None:
        err.log_error(worker_id=worker_id)
        return

    host_id = task.get("host_id")
    file_task_id = task.get("file_task_id")
    input_filename = task.get("input_filename")
    server_filename = task.get("server_filename")
    server_file_path = task.get("server_file_path")

    if file_task_id is None:
        err.log_error(worker_id=worker_id, host_id=host_id)
        return

    # Source drift: the remote file vanished between discovery and backup.
    # Prune both queue rows instead of persisting a misleading TASK_ERROR.
    if err.stage == k.STAGE_TRANSFER and isinstance(err.exc, FileNotFoundError):
        deleted_file_task = None
        deleted_history = None

        try:
            deleted_file_task = db.file_task_delete(file_task_id)
        except Exception as e_db:
            log.error_event(
                "backup_missing_remote_file_task_delete_failed",
                service=SERVICE_NAME,
                worker_id=worker_id,
                host_id=host_id,
                task_id=file_task_id,
                task_type=k.FILE_TASK_BACKUP_TYPE,
                file=input_filename,
                error=e_db,
            )

        try:
            deleted_history = db.file_history_delete(
                host_id=host_id,
                host_file_path=task["FILE_TASK__NA_HOST_FILE_PATH"],
                host_file_name=task["FILE_TASK__NA_HOST_FILE_NAME"],
            )
        except Exception as e_db:
            log.error_event(
                "backup_missing_remote_history_delete_failed",
                service=SERVICE_NAME,
                worker_id=worker_id,
                host_id=host_id,
                task_id=file_task_id,
                task_type=k.FILE_TASK_BACKUP_TYPE,
                file=input_filename,
                error=e_db,
            )

        log.warning_event(
            "backup_remote_file_missing_pruned",
            service=SERVICE_NAME,
            worker_id=worker_id,
            host_id=host_id,
            task_id=file_task_id,
            file=input_filename,
            deleted_file_task=deleted_file_task,
            deleted_history=deleted_history,
            reason=str(err.exc) if err.exc else None,
        )
        return

    # Ordinary terminal failure: log once then persist the error state.
    err.log_error(
        worker_id=worker_id,
        host_id=host_id,
        task_id=file_task_id,
        file=input_filename,
    )

    na_message = tools.compose_message(
        task_type=k.FILE_TASK_BACKUP_TYPE,
        task_status=k.TASK_ERROR,
        path=task["FILE_TASK__NA_HOST_FILE_PATH"],
        name=task["FILE_TASK__NA_HOST_FILE_NAME"],
        error=err.format_persisted_error(),
    )
    error_at = datetime.now()
    structured_error_fields = errors.persisted_error_fields_from_handler(
        err,
        message=na_message,
    )

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
            **structured_error_fields,
        )

        # FILE_TASK_HISTORY mirrors the same backup-stage error so audit
        # queries and operational queue state do not diverge.
        db.file_history_update(
            task_type=k.FILE_TASK_BACKUP_TYPE,
            host_id=host_id,
            host_file_path=task["FILE_TASK__NA_HOST_FILE_PATH"],
            host_file_name=task["FILE_TASK__NA_HOST_FILE_NAME"],
            DT_BACKUP=error_at,
            NA_SERVER_FILE_NAME=server_filename,
            NA_SERVER_FILE_PATH=server_file_path,
            NU_STATUS_BACKUP=k.TASK_ERROR,
            NA_MESSAGE=na_message,
            **structured_error_fields,
        )

        # Fatal bootstrap failures ask host_check to reconcile host state out
        # of band so this worker can close the FILE_TASK deterministically.
        if err.stage in {k.STAGE_AUTH, k.STAGE_CONNECT, k.STAGE_SSH}:
            db.queue_host_task(
                host_id=host_id,
                task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE,
                task_status=k.TASK_PENDING,
                filter_dict=k.NONE_FILTER,
            )

    except Exception as e_db:
        log.error_event(
            "task_finalization_failed",
            service=SERVICE_NAME,
            worker_id=worker_id,
            host_id=host_id,
            task_id=file_task_id,
            task_type=k.FILE_TASK_BACKUP_TYPE,
            exception=repr(e_db),
        )

    log.task_error(
        SERVICE_NAME,
        worker_id=worker_id,
        host_id=host_id,
        task_id=file_task_id,
        task_type=k.FILE_TASK_BACKUP_TYPE,
        file=input_filename,
        stage=err.stage,
        final_file=(
            os.path.join(server_file_path, server_filename)
            if server_file_path and server_filename else None
        ),
        error=err.format_error() or "Backup failed",
    )


def _cleanup(
    sftp: sftpConnection | None,
    db: dbHandlerBKP,
    host_id: int | None,
    err: errors.ErrorHandler,
    file_was_transferred: bool,
) -> None:
    """Close SFTP, release the host lock, and run deferred statistics."""
    if sftp:
        try:
            sftp.close()
            time.sleep(0.3)  # Allow the remote side to close the session cleanly.
        except Exception:
            pass

    if host_id is None:
        return

    # Single release point for the host claimed by read_file_task(..., lock_host=True).
    host_runtime.release_locked_host(
        db,
        host_id,
        logger=log,
        service_name=SERVICE_NAME,
    )
    # Stats are deferred so repository I/O finishes before
    # any host-level aggregation work begins.
    if not err.triggered and file_was_transferred:
        try:
            db.host_task_statistics_create(host_id=host_id)
        except Exception:
            pass


# ======================================================================
# Argument Parsing
# ======================================================================
def parse_arguments() -> None:
    """
    Parse the optional worker id used by the backup pool.
    """
    worker = 0
    for arg in sys.argv[1:]:
        if arg.startswith("worker="):
            try:
                worker = int(arg.split("=")[1])
            except ValueError:
                log.warning_event(
                    "worker_arg_invalid",
                    service=SERVICE_NAME,
                    fallback_worker=0,
                )
    process_status["worker"] = worker


def _do_work(sftp: sftpConnection, task: dict) -> dict:
    """
    Transfer one remote file to the repository and return backup artifacts.

    The entrypoint measures total `_do_work()` duration for `task_done`.
    This function emits only the completed `transfer` phase.
    """
    transfer_started_at = time.monotonic()
    transfer_result = sftp.transfer_file_task(
        remote_dir=task["host_file_path"],
        remote_filename=task["host_file_name"],
        local_path=task["server_file_path"],
        server_filename=task["server_filename"],
        discovery_snapshot=task["discovery_snapshot"],
    )
    transfer_elapsed_sec = round(time.monotonic() - transfer_started_at, 3)

    log.task_phase(
        SERVICE_NAME,
        worker_id=process_status["worker"],
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=k.FILE_TASK_BACKUP_TYPE,
        file=task["input_filename"],
        phase="transfer",
        elapsed_sec=transfer_elapsed_sec,
        since_start_sec=transfer_elapsed_sec,
        final_file=os.path.join(
            task["server_file_path"],
            task["server_filename"],
        ),
    )

    return {
        "refreshed_metadata": transfer_result["refreshed_metadata"],
        "updated_size_kb": transfer_result["updated_size_kb"],
        "file_was_transferred": transfer_result["file_was_transferred"],
    }


def _classify_work_failure(
    exc: Exception,
    *,
    task: dict | None,
    sftp_conn: sftpConnection | None,
    result: dict,
) -> tuple[str, str]:
    """Map a raised exception to the worker error reason and stage.

    SSH bootstrap failures reuse the shared classifier because the same
    transport errors appear in discovery and host-check too.
    """
    if task is not None and sftp_conn is None:
        ssh_failure = errors.classify_ssh_connect_failure(exc)
        if ssh_failure is not None:
            return ssh_failure

    if task is not None and not result["file_was_transferred"]:
        return "Backup worker failure", k.STAGE_TRANSFER

    return "Backup worker failure", k.STAGE_MAIN



def _init_db() -> dbHandlerBKP:
    """Create the operational DB handler or stop the process early."""
    try:
        return dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error_event("db_init_failed", service=SERVICE_NAME, error=e)
        sys.exit(1)
        
def _read_next_task(db: dbHandlerBKP) -> dict | None:
    """Read the next pending backup FILE_TASK and normalize worker context."""
    row = db.read_file_task(
        task_status=k.TASK_PENDING,
        task_type=k.FILE_TASK_BACKUP_TYPE,
        check_host_busy=True,
        check_host_offline=True,
        lock_host=True,
        reserve_hosts_for_discovery=True,
        fair_by_host=True,
    )
    if not row:
        return None
    task_row, host_id, file_task_id = row
    host_file_path = task_row["FILE_TASK__NA_HOST_FILE_PATH"]
    host_file_name = task_row["FILE_TASK__NA_HOST_FILE_NAME"]
    host_uid = task_row["HOST__NA_HOST_NAME"]
    return {
        **task_row,
        "host_id"      : host_id,
        "file_task_id" : file_task_id,
        "host_uid"     : host_uid,
        "host_file_path": host_file_path,
        "host_file_name": host_file_name,
        "input_filename": os.path.join(
            host_file_path,
            host_file_name,
        ),
        "server_file_path": file_utils.build_server_filepath(host_uid),
        "server_filename": file_utils.build_server_filename(
            host_uid=host_uid,
            remote_path=host_file_path,
            filename=host_file_name,
        ),
        "discovery_snapshot": {
            "extension": task_row["FILE_TASK__NA_EXTENSION_HOST"],
            "size_kb": task_row["FILE_TASK__VL_FILE_SIZE_KB_HOST"],
            "dt_created": task_row["FILE_TASK__DT_FILE_CREATED_HOST"],
            "dt_modified": task_row["FILE_TASK__DT_FILE_MODIFIED_HOST"],
        },
    }


def main() -> None:
    """
    Run one backup worker process until shutdown is requested.

    Each loop iteration tries to:
        1. select one fair backup candidate
        2. claim the FILE_TASK and its HOST
        3. bootstrap SSH/SFTP
        4. transfer and validate the payload
        5. update history and promote the row to PROCESS

    The entrypoint keeps this lifecycle visible on purpose.
    Helpers remove repeated policy, but the file flow should still read
    top-to-bottom in one place.
    """
    parse_arguments()
    worker_id = process_status["worker"]

    log.service_start(SERVICE_NAME, worker_id=worker_id)
    runtime_sleep.random_jitter_sleep()
    db = _init_db()

    # =======================================================
    # MAIN LOOP
    # =======================================================
    while process_status["running"]:

        err = errors.ErrorHandler(log)
        task = None
        sftp_conn = None
        result = {"file_was_transferred": False}

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

            # --- read ---
            task = _read_next_task(db)
            if task is None:
                idle_cycles = process_status.get("idle_cycles", 0) + 1
                process_status["idle_cycles"] = idle_cycles

                # Idle siblings retire so the pool shrinks with demand.
                if worker_pool.should_retire_idle_worker(
                    worker_id,
                    idle_cycles,
                    script_path=__file__,
                    idle_exit_cycles=k.BKP_TASK_IDLE_EXIT_CYCLES,
                    logger=log,
                ):
                    log.warning_event(
                        "worker_retired_idle",
                        service=SERVICE_NAME,
                        worker_id=worker_id,
                        idle_cycles=idle_cycles,
                    )
                    break

                runtime_sleep.random_jitter_sleep()
                continue

            process_status["idle_cycles"] = 0

            # --- claim ---
            if not _claim_task(db, task):
                # Another worker got this FILE_TASK first.
                runtime_sleep.random_jitter_sleep()
                continue

            # --- SSH bootstrap ---
            # Open the remote session only after queue ownership is stable.
            sftp_conn = host_context.init_host_context(task, log)

            # --- transfer ---
            # The entrypoint measures total work time.
            # `_do_work()` measures only the completed transfer phase.
            work_started_at = time.monotonic()
            result = _do_work(sftp_conn, task)
            elapsed_sec = time.monotonic() - work_started_at

            # --- finalize success ---
            _finalize_success(db, task, result, elapsed_sec=elapsed_sec)


        except Exception as e:
            if not err.triggered:
                reason, stage = _classify_work_failure(
                    e,
                    task=task,
                    sftp_conn=sftp_conn,
                    result=result,
                )
                err.capture(
                    reason=reason,
                    stage=stage,
                    exc=e,
                    worker_id=worker_id,
                    host_id=task["host_id"] if task else None,
                    task_id=task["file_task_id"] if task else None,
                    traceback=traceback.format_exc(),
                )
            _finalize_error(db, task, err)

        finally:
            _cleanup(
                sftp_conn,
                db,
                task["host_id"] if task else None,
                err,
                result["file_was_transferred"],
            )

        runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME, worker_id=worker_id)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # The loop already handles normal task failures.
        # Reaching this block means the process itself is unstable.
        worker_id = process_status.get("worker", 0)
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal backup worker crash",
            stage=k.STAGE_MAIN,
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
