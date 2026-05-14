#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Incremental Python worker that keeps RFFUSION_SUMMARY updated from SUMMARY_OUTBOX.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

from db.dbHandlerSummary import dbHandlerSummary
from server_handler import signal_runtime
from shared import logging_utils
from summary_handler.engine import SummaryRefreshEngine
import config as k


SERVICE_NAME = "appCataloga_rffusion_summary_worker"
WORKER_LOCK_NAME = "RFFUSION_SUMMARY_PY_WORKER"

log = logging_utils.log()
process_status = {"running": True}


def _shutdown_cleanup(signal_name: str) -> None:
    """Handle shutdown by letting the polling loop exit cleanly."""


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
    on_shutdown=_shutdown_cleanup,
)


def _acquire_db_lock(db: dbHandlerSummary) -> bool:
    """Claim the singleton DB lock using a dedicated long-lived connection."""
    db._connect()
    rows = db._select_raw(
        "SELECT GET_LOCK(%s, 0) AS LOCK_ACQUIRED",
        (WORKER_LOCK_NAME,),
    )
    return bool(rows and rows[0].get("LOCK_ACQUIRED"))


def _release_db_lock(db: dbHandlerSummary) -> None:
    """Release the singleton DB lock and close the lock-owning connection."""
    try:
        db._select_raw("SELECT RELEASE_LOCK(%s) AS LOCK_RELEASED", (WORKER_LOCK_NAME,))
    finally:
        db._disconnect(force=True)


def main() -> None:
    """Run the incremental summary worker until the service is stopped."""
    log.service_start(SERVICE_NAME)

    try:
        db = dbHandlerSummary(
            database=k.SUMMARY_DATABASE_NAME,
            log=log,
            reuse_connection=True,
        )
        lock_db = dbHandlerSummary(
            database=k.SUMMARY_DATABASE_NAME,
            log=log,
            reuse_connection=True,
        )
    except Exception as exc:
        log.error_event(
            "summary_worker_init_failed",
            service=SERVICE_NAME,
            error=repr(exc),
        )
        sys.exit(1)

    if not _acquire_db_lock(lock_db):
        log.warning_event(
            "summary_worker_lock_busy",
            service=SERVICE_NAME,
        )
        sys.exit(0)

    engine = SummaryRefreshEngine(db=db, logger=log)
    last_full_reconcile = datetime.min
    last_prune_at = datetime.min

    try:
        db.configure_worker_session()

        if getattr(k, "SUMMARY_WORKER_DISABLE_SQL_EVENT_ON_START", False):
            try:
                db.disable_sql_event(k.SUMMARY_WORKER_SQL_EVENT_NAME)
                log.event(
                    "summary_sql_event_disabled",
                    event_name=k.SUMMARY_WORKER_SQL_EVENT_NAME,
                )
            except Exception as exc:
                log.warning_event(
                    "summary_sql_event_disable_failed",
                    event_name=k.SUMMARY_WORKER_SQL_EVENT_NAME,
                    error=repr(exc),
                )

        while process_status["running"]:
            now = datetime.utcnow()

            try:
                # A daily full pass caps drift if any publisher event is missed.
                if (
                    last_full_reconcile == datetime.min
                    or now - last_full_reconcile
                    >= timedelta(seconds=k.SUMMARY_WORKER_RECONCILE_INTERVAL_SEC)
                ):
                    db.mark_worker_start(k.SUMMARY_WORKER_CONSUMER_NAME)
                    engine.refresh_all(reason="scheduled_full_reconcile")
                    state = db.read_worker_state(k.SUMMARY_WORKER_CONSUMER_NAME)
                    db.mark_worker_success(
                        k.SUMMARY_WORKER_CONSUMER_NAME,
                        last_outbox_id=int(state.get("ID_LAST_OUTBOX") or 0),
                        batch_size=0,
                        event_count=0,
                    )
                    last_full_reconcile = now

                batch = db.read_outbox_batch(
                    k.SUMMARY_WORKER_CONSUMER_NAME,
                    batch_size=k.SUMMARY_WORKER_BATCH_SIZE,
                )

                if not batch:
                    if (
                        now - last_prune_at
                        >= timedelta(hours=1)
                    ):
                        # Prune only rows already behind the durable checkpoint.
                        pruned = db.prune_processed_outbox(
                            k.SUMMARY_WORKER_CONSUMER_NAME,
                            keep_days=k.SUMMARY_WORKER_OUTBOX_PRUNE_DAYS,
                        )
                        if pruned:
                            log.event(
                                "summary_outbox_pruned",
                                rows=pruned,
                            )
                        last_prune_at = now

                    time.sleep(max(1, int(k.SUMMARY_WORKER_IDLE_SLEEP_SEC)))
                    continue

                db.mark_worker_start(k.SUMMARY_WORKER_CONSUMER_NAME)
                engine.refresh_for_events(batch)
                db.mark_worker_success(
                    k.SUMMARY_WORKER_CONSUMER_NAME,
                    last_outbox_id=int(batch[-1]["ID_OUTBOX"]),
                    batch_size=k.SUMMARY_WORKER_BATCH_SIZE,
                    event_count=len(batch),
                )

            except Exception as exc:
                db.mark_worker_failure(
                    k.SUMMARY_WORKER_CONSUMER_NAME,
                    error_message=repr(exc),
                )
                log.error_event(
                    "summary_worker_loop_failed",
                    error=repr(exc),
                )
                time.sleep(max(1, int(k.SUMMARY_WORKER_IDLE_SLEEP_SEC)))

    finally:
        _release_db_lock(lock_db)
        db._disconnect(force=True)
        log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    main()
