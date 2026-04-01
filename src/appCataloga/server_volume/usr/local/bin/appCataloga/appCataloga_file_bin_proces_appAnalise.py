#!/usr/bin/python3
"""
appAnalise-backed processing worker for heterogeneous station files.

This worker delegates spectral parsing to the external MATLAB-based
`appAnalise` service and keeps the rest of the lifecycle explicit: semantic
validation first, persistence second, and final file resolution last. The flow
stays linear so transient service failures can be distinguished cleanly from
definitive payload failures.
"""

import os
import sys
from datetime import datetime

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

# ---------------------------------------------------------------
# Internal modules
# ---------------------------------------------------------------
import config as k
from appAnalise import task_flow
from appAnalise.appAnalise_connection import AppAnaliseConnection
from host_handler import host_runtime
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import (
    errors,
    logging_utils,
    tools,
)

from db.dbHandlerBKP import dbHandlerBKP
from db.dbHandlerRFM import dbHandlerRFM


# ===============================================================
# GLOBAL STATE
# ===============================================================

SERVICE_NAME = "appCataloga_file_bin_proces_appAnalise"
log = logging_utils.log(target_screen=False)
process_status = {"running": True}


# ===============================================================
# Signal handling
# ===============================================================
def _shutdown_cleanup(signal_name: str) -> None:
    """
    Release BUSY host locks during process shutdown.
    """
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


def preflight_app_analise_connection(app_analise) -> bool:
    """
    Check appAnalise availability before claiming a FILE_TASK.

    When the external service is unavailable we must not even pick a task from
    the queue, otherwise the `finally` block may resolve it with no captured
    error context and persist a generic "Processing Error".
    """
    try:
        app_analise.check_connection()
        return True
    except errors.ExternalServiceTransientError as e:
        log.warning(f"event=appanalise_unavailable_retry error={e}")
        return False


# ===============================================================
# MAIN LOOP
# ===============================================================

def main():
    """
    Run the production worker loop backed by appAnalise.

    The worker only commits side effects after the external processing
    result has been normalized and validated. Transient dependency
    failures are requeued for retry; definitive validation failures
    follow the normal trash/history finalization path.
    """
    log.service_start(SERVICE_NAME)

    db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    db_rfm = dbHandlerRFM(database=k.RFM_DATABASE_NAME, log=log)
    app_analise = AppAnaliseConnection()

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        file_task_id = None
        file_was_processed = False
        new_path = None
        host_id = None
        file_meta = None
        retry_later = False
        export = None
        source_file_meta = None
        resolved_site_ids = None

        try:
            # ===================================================
            # ACT I — Confirm appAnalise is reachable before claiming work
            # ===================================================
            if not preflight_app_analise_connection(app_analise):
                # Preflight failure means the external dependency is down, not
                # that a FILE_TASK is bad. Sleep before polling again so the
                # worker does not hot-loop on a known-outage condition.
                runtime_sleep.random_jitter_sleep()
                continue

            # ===================================================
            # ACT II — Fetch one pending PROCESS FILE_TASK
            # ===================================================
            result = db_bp.read_file_task(
                task_type=k.FILE_TASK_PROCESS_TYPE,
                task_status=k.TASK_PENDING,
                check_host_busy=False,
            )

            if not result:
                # This worker does not own a pool, so an empty queue simply
                # yields back to the normal jitter contract before polling again.
                runtime_sleep.random_jitter_sleep()
                continue

            # From this point on, the loop is working on one concrete payload.
            # Everything below must either requeue this row explicitly or
            # finalize it through the single resolution helper in `finally`.
            row, host_id, _ = result
            file_task_id    = row["FILE_TASK__ID_FILE_TASK"]
            server_path     = row["FILE_TASK__NA_SERVER_FILE_PATH"]
            server_name     = row["FILE_TASK__NA_SERVER_FILE_NAME"]
            host_path       = row["FILE_TASK__NA_HOST_FILE_PATH"]
            host_file_name  = row["FILE_TASK__NA_HOST_FILE_NAME"]
            hostname_db     = row["HOST__NA_HOST_NAME"]
            extension       = row["FILE_TASK__NA_EXTENSION"]
            dt_created      = row["FILE_TASK__DT_FILE_CREATED"]
            dt_modified     = row["FILE_TASK__DT_FILE_MODIFIED"]
            vl_file_size_kb = row["FILE_TASK__VL_FILE_SIZE_KB"]
            filename        = f"{server_path}/{server_name}"
            source_file_meta = {
                "file_path": server_path,
                "file_name": server_name,
                "extension": extension,
                "size_kb": vl_file_size_kb,
                "dt_created": dt_created,
                "dt_modified": dt_modified,
                "full_path": os.path.join(server_path, server_name),
            }

            # ===================================================
            # ACT III — Mark the FILE_TASK as RUNNING
            # ===================================================
            try:
                db_bp.file_task_update(
                    task_id=file_task_id,
                    NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
                    DT_FILE_TASK=datetime.now(),
                    NU_STATUS=k.TASK_RUNNING,
                    NU_PID=os.getpid(),
                    NA_MESSAGE=tools.compose_message(
                        task_type=k.FILE_TASK_PROCESS_TYPE,
                        task_status=k.TASK_RUNNING,
                        path=server_path,
                        name=server_name,
                        detail=k.APP_ANALISE_WORKER_DETAIL,
                    ),
                )
            except Exception as e:
                err.capture(
                    reason="Failed to claim processing FILE_TASK",
                    stage="CLAIM",
                    exc=e,
                    host_id=host_id,
                    task_id=file_task_id,
                )
                raise

            # ===================================================
            # ACT IV — Delegate parsing to appAnalise and validate the result
            # ===================================================
            try:
                export = task_flow.should_export(hostname_db)
                log.event(
                    "processing_started",
                    file=filename,
                    export=export,
                )

                bin_data, file_meta = app_analise.process(
                    file_path=server_path,
                    file_name=server_name,
                    export=export,
                )

            except errors.ExternalServiceTransientError as e:
                # appAnalise availability problems are retryable. They are the
                # only processing failures that should requeue the FILE_TASK
                # instead of resolving it as a definitive payload error.
                # Keeping this branch explicit prevents generic "Processing
                # Error" rows when the external service is merely unavailable.
                retry_later = True
                err.capture(
                    reason="Transient appAnalise processing failure",
                    stage="PROCESS",
                    exc=e,
                    host_id=host_id,
                    task_id=file_task_id,
                )
                raise
            except errors.BinValidationError as e:
                # Validation errors are definitive payload problems. They go to
                # normal finalization instead of retry because the same input
                # would fail again with the same semantic defect.
                #
                # appAnalise may already have materialized an exported artifact
                # and `process()` may already have resolved its metadata before
                # the later semantic validation step rejects the payload. Keep
                # that metadata so finalization can quarantine the orphaned
                # export instead of leaving it behind in the live inbox.
                if file_meta is None:
                    file_meta = app_analise.last_output_meta

                err.capture(
                    reason="Payload validation failed during processing",
                    stage="PROCESS",
                    exc=e,
                    host_id=host_id,
                    task_id=file_task_id,
                )
                raise
            except Exception as e:
                err.capture(
                    reason="Unexpected appAnalise processing failure",
                    stage="PROCESS",
                    exc=e,
                    host_id=host_id,
                    task_id=file_task_id,
                )
                raise

            # ===================================================
            # ACT V — Resolve or create every spectrum SITE outside the DB transaction
            # ===================================================
            try:
                # SITE resolution stays outside the RFDATA transaction, matching
                # the existing BIN worker behavior and keeping geocoding latency
                # out of the DB critical section. The ownership is now per
                # spectrum, not per file, so mixed-location payloads can be
                # persisted without forcing a single synthetic `site_id`.
                resolved_site_ids = task_flow.resolve_spectrum_sites(
                    db_rfm,
                    bin_data,
                    logger=log,
                )
            except Exception as e:
                err.capture(
                    reason="Failed to resolve SITE ownership for processed spectra",
                    stage="SITE",
                    exc=e,
                    host_id=host_id,
                    task_id=file_task_id,
                )
                raise

            # ===================================================
            # ACT VI — Begin the RFDATA transaction
            # ===================================================
            try:
                db_rfm.begin_transaction()
            except Exception as e:
                err.capture(
                    reason="Failed to open RFDATA transaction",
                    stage="DB",
                    exc=e,
                    host_id=host_id,
                    task_id=file_task_id,
                )
                raise

            # ===================================================
            # ACT VII — Insert spectra and related metadata
            # ===================================================
            try:
                # The host-side source file and all derived spectra must be
                # committed as one unit for consistent lineage.
                spectrum_ids = task_flow.insert_spectra_batch(
                    db_rfm=db_rfm,
                    bin_data=bin_data,
                    hostname_db=hostname_db,
                    host_path=host_path,
                    host_file_name=host_file_name,
                    extension=extension,
                    vl_file_size_kb=vl_file_size_kb,
                    dt_created=dt_created,
                    dt_modified=dt_modified,
                )
                # After this commit, the payload is already part of RFDATA.
                # Filesystem finalization below must therefore preserve a
                # canonical artifact path instead of retrying DB writes.
                db_rfm.commit()
            except Exception as e:
                err.capture(
                    reason="Failed to persist processed spectra batch",
                    stage="DB",
                    exc=e,
                    host_id=host_id,
                    task_id=file_task_id,
                )
                raise

            # ===================================================
            # ACT VIII — Finalize files on disk and register the canonical output
            # ===================================================
            try:
                # Success resolution decides which artifact becomes canonical
                # (`.mat` export or original file) and retires superseded input.
                new_path, file_meta = task_flow.finalize_successful_processing(
                    db_rfm=db_rfm,
                    spectrum_ids=spectrum_ids,
                    bin_data=bin_data,
                    hostname_db=hostname_db,
                    file_meta=file_meta,
                    source_file_meta=source_file_meta,
                    export=export,
                    filename=filename,
                    logger=log,
                )
                file_was_processed = True
            except Exception as e:
                if task_flow.is_transient_filesystem_error(e):
                    # Shared-storage hiccups (EBUSY, stale handles, etc.) do
                    # not mean the payload is bad. Requeue the FILE_TASK so the
                    # worker retries later instead of resolving it as a fatal
                    # processing error.
                    retry_later = True
                    err.capture(
                        reason="Transient filesystem finalization failure",
                        stage="FS",
                        exc=e,
                        host_id=host_id,
                        task_id=file_task_id,
                    )
                else:
                    err.capture(
                        reason="Failed to finalize processed artifacts on disk",
                        stage="FS",
                        exc=e,
                        host_id=host_id,
                        task_id=file_task_id,
                    )
                raise

        except Exception as e:
            if not err.triggered:
                err.capture(
                    reason="Unexpected processing loop failure",
                    stage="PROCESS",
                    exc=e,
                    host_id=host_id,
                    task_id=file_task_id,
                )
            err.log_error(host_id=host_id, task_id=file_task_id)
            if db_rfm.in_transaction:
                db_rfm.rollback()

        finally:
            if not file_task_id:
                continue

            # ---------------------------------------------------
            # Phase 1 — Resolve the queue row through one exit point
            # ---------------------------------------------------
            if retry_later:
                try:
                    # Transient dependency failures keep the queue row alive.
                    task_flow.return_task_to_pending(db_bp, file_task_id, err)
                except Exception as update_err:
                    log.error(
                        f"event=retry_requeue_failed error={update_err}"
                    )

                # Even retryable outages deserve a short pause so a sick
                # dependency does not cause this worker to churn through the
                # same queue rows at maximum speed.
                runtime_sleep.random_jitter_sleep()
                continue

            # Definitive outcomes (success or fatal payload error) are closed
            # here so task deletion, trash handling, and history stay aligned.
            # Having one exit point avoids splitting queue state, history state,
            # and filesystem cleanup across many error branches above.
            try:
                resolution = task_flow.finalize_task_resolution(
                    db_bp,
                    file_task_id=file_task_id,
                    host_id=host_id,
                    host_file_name=host_file_name,
                    host_path=host_path,
                    server_name=server_name,
                    extension=extension,
                    vl_file_size_kb=vl_file_size_kb,
                    dt_created=dt_created,
                    dt_modified=dt_modified,
                    file_was_processed=file_was_processed,
                    new_path=new_path,
                    file_meta=file_meta,
                    source_file_meta=source_file_meta,
                    export=export,
                    err=err,
                )
            except Exception as finalize_err:
                # One FILE_TASK cleanup failure must not kill the daemon. When
                # final resolution fails, keep the row for later recovery
                # (requeue or stale-task sweep) and continue serving the queue.
                if hasattr(log, "error_event"):
                    log.error_event(
                        "task_finalization_failed",
                        host_id=host_id,
                        task_id=file_task_id,
                        error_type=type(finalize_err).__name__,
                        exception=repr(finalize_err),
                    )
                else:
                    log.error(
                        "event=task_finalization_failed "
                        f"host_id={host_id} task_id={file_task_id} "
                        f"error={finalize_err!r}"
                    )
                runtime_sleep.random_jitter_sleep()
                continue

            if resolution["status"] == k.TASK_ERROR:
                log.error_event(
                    "processing_error",
                    file=filename,
                    export=export,
                    final_file=resolution["final_file"],
                    error=err.format_error() or "Processing failed",
                )

            # Phase 2 — End the iteration with the same jitter contract used by
            # the other workers so success and fatal payload paths do not spin
            # more aggressively than idle or retry paths.
            runtime_sleep.random_jitter_sleep()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err = errors.ErrorHandler(log)
        # This outer boundary is for daemon-level crashes, not one FILE_TASK.
        # The worker loop above already owns normal retry/error resolution.
        err.capture(
            reason="Fatal appAnalise processing worker crash",
            stage="MAIN",
            exc=e,
        )
        err.log_error()
        host_runtime.release_busy_hosts_for_current_pid(
            db_factory=dbHandlerBKP,
            database_name=k.BKP_DATABASE_NAME,
            logger=log,
        )
        raise
