#!/usr/bin/python3
"""appAnalise-backed FILE_TASK processing worker.

Owns the PROCESS queue row lifecycle: claims one pending FILE_TASK,
delegates spectral parsing to the external appAnalise service, persists
the result to RFDATA, and resolves the queue row to DONE or ERROR.

Transient failures (service unreachable, filesystem busy) freeze the task
for manual review instead of writing a definitive ERROR.
"""

import os
import sys
import time
from datetime import datetime

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

# --- internal imports ---
import config as k
from appAnalise import processing_bin
from appAnalise.appAnalise_connection import AppAnaliseConnection
from host_handler import host_runtime
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import errors, file_utils, logging_utils, tools

from db.dbHandlerBKP import dbHandlerBKP
from db.dbHandlerRFM import dbHandlerRFM


# --- globals ---

SERVICE_NAME = "appCataloga_file_bin_process_appAnalise"
log = logging_utils.log(target_screen=False)
process_status = {"running": True}


def _error_fields(err, message: str) -> dict:
    """Structured error fields for FILE_TASK_HISTORY writes."""
    return errors.persisted_error_fields_from_handler(
        err,
        message=message,
        clear_when_empty=True,
    )


# --- signal handling ---

def _shutdown_cleanup(signal_name: str) -> None:
    """Release BUSY host locks on process shutdown."""
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


# --- loop helpers ---

def _read_next_task(db_bp: dbHandlerBKP) -> dict | None:
    """Return the next pending PROCESS FILE_TASK, or None if queue is empty."""
    result = db_bp.read_file_task(
        task_type=k.FILE_TASK_PROCESS_TYPE,
        task_status=k.TASK_PENDING,
        check_host_busy=False,
    )
    if not result:
        return None

    row, host_id, _ = result
    server_path     = row["FILE_TASK__NA_SERVER_FILE_PATH"]
    server_name     = row["FILE_TASK__NA_SERVER_FILE_NAME"]
    hostname_db     = row["HOST__NA_HOST_NAME"]
    extension       = row["FILE_TASK__NA_EXTENSION"]
    dt_created      = row["FILE_TASK__DT_FILE_CREATED"]
    dt_modified     = row["FILE_TASK__DT_FILE_MODIFIED"]
    vl_file_size_kb = row["FILE_TASK__VL_FILE_SIZE_KB"]

    return {
        "file_task_id"    : row["FILE_TASK__ID_FILE_TASK"],
        "server_path"     : server_path,
        "server_name"     : server_name,
        "host_path"       : row["FILE_TASK__NA_HOST_FILE_PATH"],
        "host_file_name"  : row["FILE_TASK__NA_HOST_FILE_NAME"],
        "hostname_db"     : hostname_db,
        "host_id"         : host_id,
        "extension"       : extension,
        "dt_created"      : dt_created,
        "dt_modified"     : dt_modified,
        "vl_file_size_kb" : vl_file_size_kb,
        "filename"        : f"{server_path}/{server_name}",
        "export"          : processing_bin.should_export(hostname_db),
        "source_file_meta": {
            "file_path"  : server_path,
            "file_name"  : server_name,
            "extension"  : extension,
            "size_kb"    : vl_file_size_kb,
            "dt_created" : dt_created,
            "dt_modified": dt_modified,
            "full_path"  : os.path.join(server_path, server_name),
        },
    }


def _claim_task(db_bp: dbHandlerBKP, task: dict) -> bool:
    """Atomically mark FILE_TASK RUNNING. Return False if another worker claimed it first."""
    claim_message = tools.compose_message(
        task_type=k.FILE_TASK_PROCESS_TYPE,
        task_status=k.TASK_RUNNING,
        path=task["server_path"],
        name=task["server_name"],
        detail=k.APP_ANALISE_WORKER_DETAIL,
    )

    claim_result = db_bp.file_task_update(
        task_id=task["file_task_id"],
        expected_status=k.TASK_PENDING,
        NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
        DT_FILE_TASK=datetime.now(),
        NU_STATUS=k.TASK_RUNNING,
        NU_PID=os.getpid(),
        NA_MESSAGE=claim_message,
        **errors.persisted_error_fields_from_handler(message=claim_message),
    )

    rows_affected = 1
    if isinstance(claim_result, dict):
        rows_affected = claim_result.get("rows_affected", 0)

    if rows_affected != 1:
        log.warning_event(
            "task_claim_race",
            service=SERVICE_NAME,
            host_id=task["host_id"],
            task_id=task["file_task_id"],
            task_type=k.FILE_TASK_PROCESS_TYPE,
            rows_affected=rows_affected,
        )
        return False

    log.task_claimed(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=k.FILE_TASK_PROCESS_TYPE,
        file=task["filename"],
        export=task["export"],
    )
    return True


# --- work ---

def _classify_work_failure(exc: Exception) -> tuple[str, str]:
    """Return (reason, stage) for err.capture based on the exception type."""
    
    # When appAnalise returns explict timeout error during processing phase
    if isinstance(exc, errors.AppAnaliseReadTimeoutError):
        return "APP_ANALISE read timeout during processing", k.STAGE_PROCESS
    
    # When appANalise does not found  the file in filesystem
    if isinstance(exc, errors.AppAnaliseFileUnavailableError):
        return "APP_ANALISE file unavailable during processing", k.STAGE_PROCESS
    
    # When appAnalise started to process binary file and does not responded within expected time
    # Maybe connection was loose or appAnalise service was under heavy load, but the failure is likely transient
    if isinstance(exc, errors.ExternalServiceTransientError):
        return "Transient appAnalise processing failure", k.STAGE_PROCESS
    
    # When binary is not validated in payload_parser.py
    if isinstance(exc, errors.BinValidationError):
        return "Payload validation failed during processing", k.STAGE_PROCESS
    
    # When binary is not found in expected location
    if processing_bin.is_transient_filesystem_error(exc):
        return "Transient filesystem finalization failure", k.STAGE_FS
    return "Unexpected processing loop failure", k.STAGE_MAIN


def _do_work(
    db_rfm: dbHandlerRFM,
    task: dict,
    app_analise: AppAnaliseConnection,
) -> dict:
    """Execute the full processing pipeline for one FILE_TASK. Raises on any failure."""
    # The entrypoint measures the full `_do_work()` duration for `task_done`.
    # This function measures only completed domain phases.
    work_started_at = time.monotonic()

    phase_started_at = time.monotonic()
    bin_data, file_meta = app_analise.process(
        file_path=task["server_path"],
        file_name=task["server_name"],
        export=task["export"],
    )
    process_elapsed_sec = round(time.monotonic() - phase_started_at, 3)
    log.task_phase(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=k.FILE_TASK_PROCESS_TYPE,
        phase="process",
        elapsed_sec=process_elapsed_sec,
        since_start_sec=round(time.monotonic() - work_started_at, 3),
        file=task["filename"],
        export=task["export"],
    )

    # --- site resolution ---
    # Stays outside the RFDATA transaction to keep geocoding latency out of
    # the DB critical section.
    phase_started_at = time.monotonic()
    resolved_site_ids = processing_bin.resolve_spectrum_sites(db_rfm, bin_data, logger=log)
    site_elapsed_sec = round(time.monotonic() - phase_started_at, 3)
    log.task_phase(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=k.FILE_TASK_PROCESS_TYPE,
        phase="site",
        elapsed_sec=site_elapsed_sec,
        since_start_sec=round(time.monotonic() - work_started_at, 3),
        file=task["filename"],
        resolved_sites=len(set(resolved_site_ids)) if resolved_site_ids else 0,
    )

    # --- db persist ---
    phase_started_at = time.monotonic()
    new_path = processing_bin.build_repository_destination_path(db_rfm, bin_data, task["hostname_db"])
    db_rfm.begin_transaction()
    spectrum_ids = processing_bin.insert_spectra_batch(
        db_rfm=db_rfm,
        bin_data=bin_data,
        hostname_db=task["hostname_db"],
        host_path=task["host_path"],
        host_file_name=task["host_file_name"],
        extension=task["extension"],
        vl_file_size_kb=task["vl_file_size_kb"],
        dt_created=task["dt_created"],
        dt_modified=task["dt_modified"],
    )
    db_elapsed_sec = round(time.monotonic() - phase_started_at, 3)
    log.task_phase(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=k.FILE_TASK_PROCESS_TYPE,
        phase="db",
        elapsed_sec=db_elapsed_sec,
        since_start_sec=round(time.monotonic() - work_started_at, 3),
        file=task["filename"],
        spectra=len(spectrum_ids),
    )

    # --- filesystem: promote artifact ---
    # Pure filesystem step: move the canonical artifact to the repository and
    # retire any superseded source payload.  DB registration of the server-side
    # file follows once the move is confirmed.
    phase_started_at = time.monotonic()
    file_meta = file_utils.promote_final_artifact(
        new_path=new_path,
        file_meta=file_meta,
        source_file_meta=task["source_file_meta"],
        export=task["export"],
        filename=task["filename"],
        logger=log,
    )
    # Register the moved artifact in RFDATA.  These two writes span two tables
    # and must be inside a transaction (ARCHITECTURE §3.4).  They happen after
    # the file is in its final location so the recorded path is always valid.
    #db_rfm.begin_transaction()
    server_file_id = db_rfm.insert_file(
        hostname=task["hostname_db"],
        NA_VOLUME=k.REPO_VOLUME_NAME,
        NA_PATH=new_path,
        NA_FILE=file_meta["file_name"],
        NA_EXTENSION=file_meta["extension"],
        VL_FILE_SIZE_KB=file_meta["size_kb"],
        DT_FILE_CREATED=file_meta["dt_created"],
        DT_FILE_MODIFIED=file_meta["dt_modified"],
        log_success=False,
    )
    db_rfm.insert_bridge_spectrum_file(spectrum_ids, [server_file_id])
    db_rfm.commit()
    finalize_elapsed_sec = round(time.monotonic() - phase_started_at, 3)
    log.task_phase(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=k.FILE_TASK_PROCESS_TYPE,
        phase="finalize",
        elapsed_sec=finalize_elapsed_sec,
        since_start_sec=round(time.monotonic() - work_started_at, 3),
        file=task["filename"],
        persisted_spectra=len(spectrum_ids),
        final_file=os.path.join(new_path, file_meta["file_name"]),
    )

    return {
        "file_meta"        : file_meta,
        "new_path"         : new_path,
        "bin_data"         : bin_data,
        "resolved_site_ids": resolved_site_ids,
        "spectrum_ids"     : spectrum_ids,
    }


# --- finalization ---

def _finalize_success(
    db_bp: dbHandlerBKP,
    task: dict,
    result: dict,
    *,
    elapsed_sec: float,
) -> None:
    """Write DONE to queue and emit timing. Never raises."""
    try:
        file_meta = result["file_meta"]
        new_path = result["new_path"]
        message = tools.compose_message(
            task_type=k.FILE_TASK_PROCESS_TYPE,
            task_status=k.TASK_DONE,
            path=new_path,
            name=file_meta["file_name"],
        )
        processed_at = datetime.now()
        db_bp.begin_transaction()
        try:
            history_result = db_bp.file_history_update(
                host_id=task["host_id"],
                task_type=k.FILE_TASK_PROCESS_TYPE,
                host_file_name=task["host_file_name"],
                host_file_path=task["host_path"],
                DT_PROCESSED=processed_at,
                NA_SERVER_FILE_NAME=file_meta["file_name"],
                NA_SERVER_FILE_PATH=new_path,
                NA_EXTENSION=file_meta["extension"],
                VL_FILE_SIZE_KB=file_meta["size_kb"],
                DT_FILE_CREATED=file_meta["dt_created"],
                DT_FILE_MODIFIED=file_meta["dt_modified"],
                NU_STATUS_PROCESSING=k.TASK_DONE,
                NA_MESSAGE=message,
                **_error_fields(None, message),
            )
            if history_result.get("rows_affected") != 1:
                raise RuntimeError(
                    "FILE_TASK_HISTORY finalization affected "
                    f"{history_result.get('rows_affected')} rows "
                    f"(expected 1 for host={task['host_id']}, "
                    f"path={task['host_path']}, name={task['host_file_name']})"
                )
            deleted_rows = db_bp.file_task_delete(task_id=task["file_task_id"])
            if deleted_rows != 1:
                raise RuntimeError(
                    f"FILE_TASK delete affected {deleted_rows} rows "
                    f"(expected 1 for task_id={task['file_task_id']})"
                )
            db_bp.commit()
        except Exception:
            db_bp.rollback()
            raise
        db_bp.host_task_statistics_create(host_id=task["host_id"], log_if_active=False)
        log.task_done(
            SERVICE_NAME,
            host_id=task["host_id"],
            task_id=task["file_task_id"],
            task_type=k.FILE_TASK_PROCESS_TYPE,
            file=task["filename"],
            elapsed_sec=round(elapsed_sec, 3),
            final_file=os.path.join(new_path, file_meta["file_name"]),
        )
    except Exception as e:
        log.error_event(
            "task_finalization_failed",
            task_id=task["file_task_id"],
            host_id=task["host_id"],
            error_type=type(e).__name__,
            exception=repr(e),
        )


def _finalize_freeze(
    db_bp: dbHandlerBKP,
    task: dict,
    err: errors.ErrorHandler,
) -> None:
    """Write FROZEN to queue. Raises on DB failure — caught by _finalize_error."""
    detail = errors.freeze_processing_detail(err.exc)
    message = tools.compose_message(
        task_type=k.FILE_TASK_PROCESS_TYPE,
        task_status=k.TASK_FROZEN,
        detail=detail,
        error=err.format_persisted_error(),
    )
    structured = _error_fields(err, message)
    db_bp.begin_transaction()
    try:
        db_bp.file_task_update(
            task_id=task["file_task_id"],
            NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
            NU_STATUS=k.TASK_FROZEN,
            NU_PID=None,
            DT_FILE_TASK=datetime.now(),
            NA_MESSAGE=message,
            **structured,
        )
        db_bp.file_history_update(
            task_type=k.FILE_TASK_PROCESS_TYPE,
            host_id=task["host_id"],
            host_file_path=task["host_path"],
            host_file_name=task["host_file_name"],
            NU_STATUS_PROCESSING=k.TASK_FROZEN,
            NA_MESSAGE=message,
            **structured,
        )
        db_bp.commit()
    except Exception:
        db_bp.rollback()
        raise
    db_bp.host_task_statistics_create(host_id=task["host_id"], log_if_active=False)
    log.task_frozen(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=k.FILE_TASK_PROCESS_TYPE,
        file=task["filename"],
        stage=err.stage,
        error=err.format_error() or detail,
    )


def _write_task_error(
    db_bp: dbHandlerBKP,
    task: dict,
    err: errors.ErrorHandler,
) -> None:
    """Quarantine the error artifact and write ERROR. Raises on failure."""
    file_meta = getattr(err.exc, "file_meta", None)
    server_path, history_meta_override = file_utils.quarantine_error_artifact(
        file_meta=file_meta,
        source_file_meta=task["source_file_meta"],
        export=task.get("export"),
        logger=log,
    )
    if history_meta_override:
        history_name = history_meta_override["name"]
        history_extension = history_meta_override["extension"]
        history_size_kb = history_meta_override["size_kb"]
        history_dt_created = history_meta_override["dt_created"]
        history_dt_modified = history_meta_override["dt_modified"]
    else:
        history_name = task["server_name"]
        history_extension = task["extension"]
        history_size_kb = task["vl_file_size_kb"]
        history_dt_created = task["dt_created"]
        history_dt_modified = task["dt_modified"]
    message = tools.compose_message(
        task_type=k.FILE_TASK_PROCESS_TYPE,
        task_status=k.TASK_ERROR,
        error=err.format_persisted_error(),
    )
    error_at = datetime.now()
    structured = _error_fields(err, message)
    db_bp.begin_transaction()
    try:
        history_result = db_bp.file_history_update(
            host_id=task["host_id"],
            task_type=k.FILE_TASK_PROCESS_TYPE,
            host_file_name=task["host_file_name"],
            host_file_path=task["host_path"],
            DT_PROCESSED=error_at,
            NA_SERVER_FILE_NAME=history_name,
            NA_SERVER_FILE_PATH=server_path,
            NA_EXTENSION=history_extension,
            VL_FILE_SIZE_KB=history_size_kb,
            DT_FILE_CREATED=history_dt_created,
            DT_FILE_MODIFIED=history_dt_modified,
            NU_STATUS_PROCESSING=k.TASK_ERROR,
            NA_MESSAGE=message,
            **structured,
        )
        if history_result.get("rows_affected") != 1:
            raise RuntimeError(
                "FILE_TASK_HISTORY finalization affected "
                f"{history_result.get('rows_affected')} rows "
                f"(expected 1 for host={task['host_id']}, "
                f"path={task['host_path']}, name={task['host_file_name']})"
            )
        deleted_rows = db_bp.file_task_delete(task_id=task["file_task_id"])
        if deleted_rows != 1:
            raise RuntimeError(
                f"FILE_TASK delete affected {deleted_rows} rows "
                f"(expected 1 for task_id={task['file_task_id']})"
            )
        db_bp.commit()
    except Exception:
        db_bp.rollback()
        raise
    db_bp.host_task_statistics_create(host_id=task["host_id"], log_if_active=False)
    final_file = (
        os.path.join(server_path, history_name) if server_path else None
    )
    log.task_error(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=k.FILE_TASK_PROCESS_TYPE,
        file=task["filename"],
        stage=err.stage,
        error=err.format_error() or "Processing failed",
        export=task.get("export"),
        final_file=final_file,
    )


def _finalize_error(
    db_bp: dbHandlerBKP,
    task: dict | None,
    err: errors.ErrorHandler,
) -> None:
    """Route to freeze or error finalization. Never raises."""
    if task is None:
        return

    try:
        if errors.should_freeze_processing_task(err.exc):
            _finalize_freeze(db_bp, task, err)
        else:
            _write_task_error(db_bp, task, err)
    except Exception as finalize_err:
        log.error_event(
            "task_finalization_failed",
            task_id=task["file_task_id"],
            host_id=task["host_id"],
            error_type=type(finalize_err).__name__,
            exception=repr(finalize_err),
        )


def _cleanup(db_rfm: dbHandlerRFM, task: dict | None) -> None:  # noqa: ARG001
    """Rollback any open RFDATA transaction. Never raises."""
    try:
        if db_rfm.in_transaction:
            db_rfm.rollback()
    except Exception:
        pass


# --- main ---
def _init_db() -> tuple[dbHandlerBKP, dbHandlerRFM]:
    """Connect to the operational database. Exits the process on failure."""
    try:
        return dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log), dbHandlerRFM(database=k.RFM_DATABASE_NAME, log=log)
    except Exception as e:
        log.error_event("db_init_failed", service=SERVICE_NAME, error=e)
        sys.exit(1)
        
def main() -> None:
    """Run the appAnalise processing worker until shutdown is requested."""
    
    # Initialize logging, DB connections, and appAnalise client outside the loop to
    log.service_start(SERVICE_NAME)
    db_bp, db_rfm = _init_db()
    app_analise = AppAnaliseConnection()

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        task = None

        try:
            # --- preflight ---
            # Do not claim any FILE_TASK while appAnalise is unreachable.
            if not app_analise.check_connection_with_log(log):
                runtime_sleep.random_jitter_sleep()
                continue

            # --- read ---
            task = _read_next_task(db_bp)
            if task is None:
                runtime_sleep.random_jitter_sleep(interval=10)
                continue

            # --- claim ---
            if not _claim_task(db_bp, task):
                runtime_sleep.random_jitter_sleep()
                continue

            # --- work ---
            work_started_at = time.monotonic()
            result = _do_work(db_rfm, task, app_analise)
            elapsed_sec = time.monotonic() - work_started_at

            # --- finalize ---
            _finalize_success(db_bp, task, result, elapsed_sec=elapsed_sec)

        except Exception as e:
            # If appAnalise produced a partial export artifact, attach it to
            # the exception so _write_task_error can quarantine it.
            if getattr(app_analise, "last_output_meta", None) and not hasattr(e, "file_meta"):
                e.file_meta = app_analise.last_output_meta
            if not err.triggered:
                reason, stage = _classify_work_failure(e)
                err.capture(reason=reason, stage=stage, exc=e)
            host_id = task["host_id"] if task else None
            task_id = task["file_task_id"] if task else None
            err.log_error(host_id=host_id, task_id=task_id)
            _finalize_error(db_bp, task, err)

        finally:
            _cleanup(db_rfm, task)

        runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal appAnalise processing worker crash",
            stage=k.STAGE_MAIN,
            exc=e,
        )
        err.log_error()
        host_runtime.release_busy_hosts_for_current_pid(
            db_factory=dbHandlerBKP,
            database_name=k.BKP_DATABASE_NAME,
            logger=log,
        )
        raise
