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
from collections.abc import Callable
from datetime import datetime

from utils.bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

# --- internal imports ---
from db import constants as db_constants
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
PROCESSING_TYPE = db_constants["tasks"]["file_types"]["processing"]
TASK_STATES = db_constants["tasks"]["states"]
log = logging_utils.log(target_screen=False)
process_status = {"running": True}


def _error_fields(err, message: str) -> dict:
    """Build structured error fields for queue and history writes."""
    return errors.persisted_error_fields_from_handler(
        err,
        message=message,
        clear_when_empty=True,
    )


def _is_current_month_host_file(task: dict, observed_at: datetime) -> bool:
    """Return whether host file dates can describe the current GPS state."""
    host_dates = (
        task.get("host_dt_created"),
        task.get("host_dt_modified"),
    )
    return any(
        isinstance(value, datetime)
        and value.year == observed_at.year
        and value.month == observed_at.month
        for value in host_dates
    )


def _record_gps_available_if_needed(task: dict, *, observed_at: datetime) -> None:
    """Mark GPS as available when the processed file is from the current month."""
    if _is_current_month_host_file(task, observed_at):
        host_runtime.record_gps_gnss_available(
            task["host_id"],
            evaluated_at=observed_at,
            logger=log,
        )


def _record_gps_failure_if_needed(
    task: dict,
    *,
    observed_at: datetime,
    structured: dict,
) -> None:
    """Persist GPS state only when processing classified the GPS condition."""
    if structured.get("NA_ERROR_CODE") != k.ERROR_CODE_GPS_GNSS_UNAVAILABLE:
        return
    if not _is_current_month_host_file(task, observed_at):
        return

    host_runtime.record_gps_gnss_unavailable(
        task["host_id"],
        evaluated_at=observed_at,
        description=(
            structured.get("NA_ERROR_SUMMARY")
            or k.ERROR_CODE_GPS_GNSS_UNAVAILABLE
        ),
        host_file_name=task["host_file_name"],
        logger=log,
    )


# --- signal handling ---

def _shutdown_cleanup(signal_name: str) -> None:
    """Release BUSY host marks owned by this PID during shutdown."""
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
    """Read the newest pending source file and normalize processing context.

    Args:
        db_bp (dbHandlerBKP): Operational queue handler.

    Returns:
        dict | None: Task IDs, host/server paths and names, timestamps, extension,
            size, export flag, and source_file_meta for processing;
            None when no pending task exists. source_file_meta contains file_path,
            file_name, extension, size_kb, dt_created, dt_modified, and full_path.

    Raises:
        Exception: Propagates selection or context construction failures.
    """
    result = db_bp.file_task_read(
        task_type=PROCESSING_TYPE,
        task_status=TASK_STATES["pending"],
        check_host_busy=False,
        newest_first=True,
    )
    if not result:
        return None

    row, host_id, _ = result
    task = {
        "file_task_id"    : row["FILE_TASK__ID_FILE_TASK"],
        "host_id"         : host_id,
        "hostname_db"     : row["HOST__NA_HOST_NAME"],
        "server_path"     : row["FILE_TASK__NA_SERVER_FILE_PATH"],
        "server_name"     : row["FILE_TASK__NA_SERVER_FILE_NAME"],
        "host_path"       : row["FILE_TASK__NA_HOST_FILE_PATH"],
        "host_file_name"  : row["FILE_TASK__NA_HOST_FILE_NAME"],
        "extension"       : row["FILE_TASK__NA_EXTENSION_SERVER"],
        "vl_file_size_kb" : row["FILE_TASK__VL_FILE_SIZE_KB_SERVER"],
        "dt_created"      : row["FILE_TASK__DT_FILE_CREATED_SERVER"],
        "dt_modified"     : row["FILE_TASK__DT_FILE_MODIFIED_SERVER"],
        "host_dt_created" : row.get("FILE_TASK__DT_FILE_CREATED_HOST"),
        "host_dt_modified": row.get("FILE_TASK__DT_FILE_MODIFIED_HOST"),
    }
    full_path = os.path.join(task["server_path"], task["server_name"])
    task["filename"] = f'{task["server_path"]}/{task["server_name"]}'
    task["export"] = processing_bin.should_export(task["hostname_db"])
    task["source_file_meta"] = {
        "file_path"  : task["server_path"],
        "file_name"  : task["server_name"],
        "extension"  : task["extension"],
        "size_kb"    : task["vl_file_size_kb"],
        "dt_created" : task["dt_created"],
        "dt_modified": task["dt_modified"],
        "full_path"  : full_path,
    }
    return task


def _claim_task(db_bp: dbHandlerBKP, task: dict) -> bool:
    """Atomically move the FILE_TASK from PENDING to RUNNING.

    Another worker may win the race between read and claim.
    That case is normal and should not be treated as a task failure.
    """
    claim_message = tools.compose_message(
        task_type=PROCESSING_TYPE,
        task_status=TASK_STATES["running"],
        path=task["server_path"],
        name=task["server_name"],
        detail=k.APP_ANALISE_WORKER_DETAIL,
    )

    claim_result = db_bp.file_task_update(
        task_id=task["file_task_id"],
        expected_status=TASK_STATES["pending"],
        DT_FILE_TASK=datetime.now(),
        NU_STATUS=TASK_STATES["running"],
        NU_PID=os.getpid(),
        NA_MESSAGE=claim_message,
        **errors.persisted_error_fields_from_handler(message=claim_message),
    )

    rows_affected = claim_result["rows_affected"]
    if rows_affected != 1:
        log.warning_event(
            "task_claim_race",
            service=SERVICE_NAME,
            host_id=task["host_id"],
            task_id=task["file_task_id"],
            task_type=PROCESSING_TYPE,
            rows_affected=rows_affected,
        )
        return False

    log.task_claimed(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=PROCESSING_TYPE,
        file=task["filename"],
        export=task["export"],
    )
    return True


# --- work ---

WORK_FAILURE_REASONS = (
    (errors.AppAnaliseReadTimeoutError, "APP_ANALISE read timeout during processing"),
    (errors.AppAnaliseFileUnavailableError, "APP_ANALISE file unavailable during processing"),
    (errors.AppAnaliseServiceResponseError, "APP_ANALISE service returned processing error"),
    (errors.AppAnaliseInvalidSuccessPayloadError, "APP_ANALISE returned invalid success payload"),
    (errors.ExternalServiceTransientError, "Transient appAnalise processing failure"),
    (errors.BinValidationError, "Payload validation failed during processing"),
)


def _classify_work_failure(exc: Exception) -> tuple[str, str]:
    """Map a raised exception to the worker error reason and stage."""
    for error_type, reason in WORK_FAILURE_REASONS:
        if isinstance(exc, error_type):
            return reason, k.STAGE_PROCESS
    if file_utils.is_transient_filesystem_error(exc):
        return "Transient filesystem finalization failure", k.STAGE_FS
    return "Unexpected processing loop failure", k.STAGE_MAIN


def _do_work(
    db_rfm: dbHandlerRFM,
    task: dict,
    app_analise: AppAnaliseConnection,
) -> dict:
    """
    Delegate the processing pipeline to the appAnalise domain flow.

    The worker keeps queue lifecycle ownership. The domain flow owns the
    processing stages and emits the intermediate `task_phase` events.
    """
    return processing_bin.run_processing_flow(
        db_rfm,
        task,
        app_analise,
        logger=log,
        service_name=SERVICE_NAME,
    )


# --- finalization ---
# DONE and ERROR remove the FILE_TASK row; FROZEN keeps it for manual review.
# History and queue row change in one transaction; GPS and summary refresh
# afterwards are best-effort.

def _finalize_success(
    db_bp: dbHandlerBKP,
    task: dict,
    result: dict,
    *,
    elapsed_sec: float,
) -> None:
    """Record DONE and remove the queue row; failures are only logged.

    Spectra are already committed in RFDATA, so the task never becomes ERROR here.
    """
    file_meta = result["file_meta"]
    new_path = result["new_path"]
    message = tools.compose_message(
        task_type=PROCESSING_TYPE,
        task_status=TASK_STATES["done"],
        path=new_path,
        name=file_meta["file_name"],
    )
    processed_at = datetime.now()
    server_fields = {
        "NA_SERVER_FILE_NAME": file_meta["file_name"],
        "NA_SERVER_FILE_PATH": new_path,
        "NA_EXTENSION_SERVER": file_meta["extension"],
        "VL_FILE_SIZE_KB_SERVER": file_meta["size_kb"],
        "DT_FILE_CREATED_SERVER": file_meta["dt_created"],
        "DT_FILE_MODIFIED_SERVER": file_meta["dt_modified"],
    }
    try:
        _resolve_queue_row(
            db_bp,
            task,
            status=TASK_STATES["done"],
            message=message,
            structured=_error_fields(None, message),
            resolved_at=processed_at,
            server_fields=server_fields,
            keep_in_queue=False,
        )
    except Exception as exc:
        _log_finalization_failure(task, exc)
        return

    _after_resolution(
        db_bp,
        task,
        reason="processing_completed",
        update_gps=lambda: _record_gps_available_if_needed(task, observed_at=processed_at),
    )
    log.task_done(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=PROCESSING_TYPE,
        file=task["filename"],
        elapsed_sec=round(elapsed_sec, 3),
        final_file=os.path.join(new_path, file_meta["file_name"]),
    )


def _finalize_error(
    db_bp: dbHandlerBKP,
    task: dict | None,
    err: errors.ErrorHandler,
) -> None:
    """Keep the queue row FROZEN for review, or quarantine and remove it as ERROR."""
    if task is None:
        return

    frozen = errors.should_freeze_processing_task(err.exc)
    status = TASK_STATES["frozen"] if frozen else TASK_STATES["error"]
    try:
        if frozen:
            detail = errors.freeze_processing_detail(err.exc)
            server_path, server_fields = None, {}
        else:
            detail = None
            server_path, server_fields = _quarantine_failed_artifact(task, err)
        message = tools.compose_message(
            task_type=PROCESSING_TYPE,
            task_status=status,
            detail=detail,
            error=err.format_persisted_error(),
        )
        structured = _error_fields(err, message)
        failed_at = datetime.now()
        _resolve_queue_row(
            db_bp,
            task,
            status=status,
            message=message,
            structured=structured,
            resolved_at=failed_at,
            server_fields=server_fields,
            keep_in_queue=frozen,
        )
    except Exception as exc:
        _log_finalization_failure(task, exc)
        return

    _after_resolution(
        db_bp,
        task,
        reason="processing_frozen" if frozen else "processing_error",
        update_gps=lambda: _record_gps_failure_if_needed(
            task, observed_at=failed_at, structured=structured
        ),
    )
    if frozen:
        log.task_frozen(
            SERVICE_NAME,
            host_id=task["host_id"],
            task_id=task["file_task_id"],
            task_type=PROCESSING_TYPE,
            file=task["filename"],
            stage=err.stage,
            error=err.format_error() or detail,
        )
        return

    server_name = server_fields["NA_SERVER_FILE_NAME"]
    log.task_error(
        SERVICE_NAME,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=PROCESSING_TYPE,
        file=task["filename"],
        stage=err.stage,
        error=err.format_error() or "Processing failed",
        export=task.get("export"),
        final_file=os.path.join(server_path, server_name) if server_path else None,
    )


def _resolve_queue_row(
    db_bp: dbHandlerBKP,
    task: dict,
    *,
    status: int,
    message: str,
    structured: dict,
    resolved_at: datetime,
    server_fields: dict,
    keep_in_queue: bool,
) -> None:
    """Update history and delete (or freeze) the queue row in one transaction."""
    db_bp.begin_transaction()
    try:
        history = db_bp.file_task_history_update(
            host_id=task["host_id"],
            task_type=PROCESSING_TYPE,
            host_file_name=task["host_file_name"],
            host_file_path=task["host_path"],
            DT_PROCESSED=resolved_at,
            NU_STATUS_PROCESSING=status,
            NA_MESSAGE=message,
            **server_fields,
            **structured,
        )
        _expect_one_row("FILE_TASK_HISTORY update", history.get("rows_affected"), task)
        if keep_in_queue:
            queue = db_bp.file_task_update(
                task_id=task["file_task_id"],
                NU_STATUS=status,
                NU_PID=None,
                DT_FILE_TASK=resolved_at,
                NA_MESSAGE=message,
                **structured,
            )
            _expect_one_row("FILE_TASK update", queue.get("rows_affected"), task)
        else:
            deleted = db_bp.file_task_delete(task_id=task["file_task_id"])
            _expect_one_row("FILE_TASK delete", deleted, task)
        db_bp.commit()
    except Exception:
        db_bp.rollback()
        raise


def _expect_one_row(operation: str, rows: object, task: dict) -> None:
    """Abort the finalization when a write did not hit exactly one row."""
    if rows != 1:
        raise RuntimeError(
            f"{operation} affected {rows} rows (expected 1, task_id={task['file_task_id']})"
        )


def _quarantine_failed_artifact(
    task: dict,
    err: errors.ErrorHandler,
) -> tuple[str | None, dict]:
    """Quarantine the failed artifact; return its directory and history fields."""
    server_path, moved = file_utils.quarantine_error_artifact(
        file_meta=getattr(err.exc, "file_meta", None),
        source_file_meta=task["source_file_meta"],
        export=task.get("export"),
        logger=log,
    )
    meta = moved or {
        "name": task["server_name"],
        "extension": task["extension"],
        "size_kb": task["vl_file_size_kb"],
        "dt_created": task["dt_created"],
        "dt_modified": task["dt_modified"],
    }
    return server_path, {
        "NA_SERVER_FILE_NAME": meta["name"],
        "NA_SERVER_FILE_PATH": server_path,
        "NA_EXTENSION_SERVER": meta["extension"],
        "VL_FILE_SIZE_KB_SERVER": meta["size_kb"],
        "DT_FILE_CREATED_SERVER": meta["dt_created"],
        "DT_FILE_MODIFIED_SERVER": meta["dt_modified"],
    }


def _after_resolution(
    db_bp: dbHandlerBKP,
    task: dict,
    *,
    reason: str,
    update_gps: Callable[[], None],
) -> None:
    """Refresh GPS and summary state; failures here never reopen the task."""
    try:
        update_gps()
    except Exception as exc:
        log.warning_event(
            "gps_metric_update_failed",
            service=SERVICE_NAME,
            host_id=task["host_id"],
            task_id=task["file_task_id"],
            error=exc,
        )
    try:
        db_bp.host_request_summary_refresh(host_id=task["host_id"], reason=reason)
    except Exception as exc:
        log.warning_event(
            "host_statistics_refresh_failed",
            service=SERVICE_NAME,
            host_id=task["host_id"],
            task_id=task["file_task_id"],
            error_type=type(exc).__name__,
            exception=repr(exc),
        )


def _log_finalization_failure(task: dict, exc: Exception) -> None:
    """Report a finalization that could not be persisted."""
    log.error_event(
        "task_finalization_failed",
        task_id=task["file_task_id"],
        host_id=task["host_id"],
        error_type=type(exc).__name__,
        exception=repr(exc),
    )


def _cleanup(db_rfm: dbHandlerRFM, task: dict | None) -> None:  # noqa: ARG001
    """Roll back any open RFDATA transaction left by a failed iteration."""
    try:
        if db_rfm.in_transaction:
            db_rfm.rollback()
    except Exception:
        pass


# --- main ---
def _init_db() -> tuple[dbHandlerBKP, dbHandlerRFM]:
    """Create the operational and analytical DB handlers or stop early."""
    try:
        return (
            dbHandlerBKP(
                database=k.BKP_DATABASE_NAME, log=log, conninfo=k.PG_CONNINFO
            ),
            dbHandlerRFM(
                database=k.RFM_DATABASE_NAME, log=log, conninfo=k.PG_CONNINFO
            ),
        )
    except Exception as e:
        log.error_event("db_init_failed", service=SERVICE_NAME, error=e)
        sys.exit(1)
        
def main() -> None:
    """
    Run the appAnalise processing worker until shutdown is requested.

    The worker keeps queue ownership in the entrypoint.
    The appAnalise domain flow owns the processing pipeline itself.
    """

    # These dependencies are long-lived and should not be rebuilt per task.
    log.service_start(SERVICE_NAME)
    db_bp, db_rfm = _init_db()
    app_analise = AppAnaliseConnection()

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        task = None

        try:
            # --- read ---
            task = _read_next_task(db_bp)
            if task is None:
                # Processing is heavier than the other queues, so idle polls wait longer.
                runtime_sleep.random_jitter_sleep(interval=10)
                continue
            
            # --- preflight ---
            # Do not claim work while the external processing service is down.
            if not app_analise.check_connection_with_log(log):
                runtime_sleep.random_jitter_sleep()
                continue

            # --- claim ---
            if not _claim_task(db_bp, task):
                # Another worker may claim the same FILE_TASK first.
                runtime_sleep.random_jitter_sleep()
                continue

            # --- work ---
            # The entrypoint measures total work time.
            # The domain flow emits its own completed phases.
            work_started_at = time.monotonic()
            result = _do_work(db_rfm, task, app_analise)
            elapsed_sec = time.monotonic() - work_started_at

            # --- finalize ---
            _finalize_success(db_bp, task, result, elapsed_sec=elapsed_sec)

        except Exception as e:
            # Partial export artifacts must travel with the exception so the
            # finalizer can quarantine them consistently.
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
            try:
                if db_bp.in_transaction:
                    db_bp.rollback()
            except Exception:
                pass
            _cleanup(db_rfm, task)

        runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # The loop already handles normal task failures.
        # Reaching this block means the process itself is unstable.
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
