#!/usr/bin/python3
"""
Repository garbage-collection worker.

Worker responsible for cleaning retired repository artifacts after quarantine.

The worker owns two distinct cleanup channels:
    1. The operator-facing artifact still referenced by `FILE_TASK_HISTORY`
       and quarantined in the main `trash` area
    2. Superseded source/export leftovers quarantined in `trash/resolved_files`

That distinction matters because only the first channel updates
`IS_PAYLOAD_DELETED/DT_PAYLOAD_DELETED`. Files in `resolved_files` are no
longer tracked by `FILE_TASK_HISTORY`; they are cleaned purely by filesystem
retention.

The worker is deliberately small because it is the last step in the file
lifecycle and must stay easy to reason about during production cleanup
incidents.
"""

import os
import sys
from datetime import datetime

from utils.bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)


# ---------------------------------------------------------------
# Internal modules
# ---------------------------------------------------------------
import config as k
from gc_handler import gc_maintenance
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import errors, logging_utils
from db.dbHandlerBKP import dbHandlerBKP


# ===============================================================
# GLOBAL STATE
# ===============================================================
SERVICE_NAME = "appCataloga_garbage_collector"
log = logging_utils.log(target_screen=False)
process_status = {"running": True}


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
)


def _init_db() -> dbHandlerBKP:
    """Create the operational DB handler or stop the process early."""
    try:
        return dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    except Exception as e:
        log.error_event(
            "db_init_failed",
            service=SERVICE_NAME,
            component="garbage_collector_daemon",
            operation="init_db",
            error=e,
        )
        sys.exit(1)


def _read_next_cycle(*, now: datetime, trash_root: str, resolved_root: str) -> dict:
    """Build the minimal GC cycle context for one loop iteration."""
    return {
        "now": now,
        "trash_root": trash_root,
        "resolved_root": resolved_root,
    }


def _do_maintenance(db_bp: dbHandlerBKP, cycle: dict) -> dict:
    """Run one GC pass and return a compact batch summary."""
    history_rows, resolved_rows = gc_maintenance.collect_gc_candidates(db_bp, logger=log)

    if not history_rows and not resolved_rows:
        return {
            "had_candidates": False,
            "deleted": 0,
            "deleted_history_payloads": 0,
            "deleted_resolved_files": 0,
        }

    deleted_history_payloads = gc_maintenance.delete_history_artifacts(
        db_bp,
        history_rows,
        trash_root=cycle["trash_root"],
        resolved_root=cycle["resolved_root"],
        logger=log,
    )
    deleted_resolved_files = gc_maintenance.delete_resolved_files_artifacts(
        resolved_rows,
        resolved_root=cycle["resolved_root"],
        logger=log,
    )

    return {
        "had_candidates": True,
        "deleted": deleted_history_payloads + deleted_resolved_files,
        "deleted_history_payloads": deleted_history_payloads,
        "deleted_resolved_files": deleted_resolved_files,
    }


def _finalize_success(result: dict) -> None:
    """Emit the final batch log for one successful GC cycle."""
    if not result["had_candidates"]:
        log.event(
            "garbage_candidates_empty",
            service=SERVICE_NAME,
            component="garbage_collector_daemon",
            operation="finalize_success",
        )
        return

    log.event(
        "garbage_batch_processed",
        service=SERVICE_NAME,
        component="garbage_collector_daemon",
        operation="finalize_success",
        deleted=result["deleted"],
        deleted_history_payloads=result["deleted_history_payloads"],
        deleted_resolved_files=result["deleted_resolved_files"],
    )


def _classify_cycle_failure(exc: Exception) -> tuple[str, str]:
    """Map a cycle-level exception to the canonical error fields."""
    return "Garbage collector cycle failed", k.STAGE_MAIN


def _finalize_error(err: errors.ErrorHandler) -> None:
    """Emit the final GC cycle failure log."""
    err.log_error()


def main() -> None:
    """Run the garbage-collection daemon until shutdown is requested."""
    log.service_start(SERVICE_NAME)

    db_bp = _init_db()
    trash_root = os.path.join(k.REPO_FOLDER, k.TRASH_FOLDER)
    resolved_root = gc_maintenance.build_resolved_files_trash_path()
    gc_maintenance.log_gc_configuration(
        logger=log,
        trash_root=trash_root,
        resolved_root=resolved_root,
    )

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        sleep_interval = k.GC_LOOP_SLEEP

        try:
            # Build one small cycle context so cadence stays explicit.
            cycle = _read_next_cycle(
                now=datetime.now(),
                trash_root=trash_root,
                resolved_root=resolved_root,
            )

            # Run one GC pass and keep only a compact batch summary in the loop.
            result = _do_maintenance(db_bp, cycle)
            _finalize_success(result)
            if not result["had_candidates"]:
                sleep_interval = k.GC_IDLE_SLEEP
        except Exception as e:
            if not err.triggered:
                reason, stage = _classify_cycle_failure(e)
                err.capture(reason=reason, stage=stage, exc=e)
            _finalize_error(err)

        # Empty cycles back off longer than active cleanup cycles.
        runtime_sleep.random_jitter_sleep(interval=sleep_interval)

    log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal garbage-collector worker crash",
            stage=k.STAGE_MAIN,
            exc=e,
        )
        err.log_error()
        raise
