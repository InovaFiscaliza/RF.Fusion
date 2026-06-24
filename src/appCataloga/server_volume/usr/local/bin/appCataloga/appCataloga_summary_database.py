#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Maintain the RFFUSION_SUMMARY read model from the Python worker.

This daemon owns two refresh paths:

1. Full reconcile on startup when the last successful run is stale and then
   daily at 02:00 BRT.
2. Incremental refresh driven by ``SUMMARY_OUTBOX`` scope rows between full
   reconciles.

The worker keeps one DB connection for normal summary work and one dedicated
lock connection so the MariaDB user-lock is not released accidentally during
routine reconnects.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone

from utils.bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

import config as k
from db.dbHandlerSummary import dbHandlerSummary
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import logging_utils
from summary_handler.refresh_engine import SummaryRefreshEngine


SERVICE_NAME = "appCataloga_summary_database"
WORKER_LOCK_NAME = "RFFUSION_SUMMARY_PY_WORKER"
_BRT = timezone(timedelta(hours=-3))

log = logging_utils.log()
process_status = {"running": True}


def _shutdown_cleanup(signal_name: str) -> None:
    """Hook required by the shared signal installer.

    The summary worker has no extra resource to release here because the DB
    lock and normal connection are closed in ``main()``'s ``finally`` block.
    """
    pass


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
    on_shutdown=_shutdown_cleanup,
)


def _init_db() -> tuple[dbHandlerSummary, dbHandlerSummary]:
    """Create the working DB handler and the dedicated lock handler.

    Returns:
        Tuple ``(db, lock_db)`` using the same schema and logger.

    Exits:
        Terminates the process when the DB handlers cannot be initialized.
    """
    try:
        return (
            dbHandlerSummary(
                database=k.SUMMARY_DATABASE_NAME,
                log=log,
                reuse_connection=True,
            ),
            dbHandlerSummary(
                database=k.SUMMARY_DATABASE_NAME,
                log=log,
                reuse_connection=True,
            ),
        )
    except Exception as exc:
        log.error_event("summary_worker_init_failed", service=SERVICE_NAME, error=repr(exc))
        sys.exit(1)


def _next_2am_brt(after_utc: datetime) -> datetime:
    """Return the next daily full-reconcile slot expressed in UTC.

    Args:
        after_utc: Naive UTC datetime used as the scheduling reference.

    Returns:
        Naive UTC datetime corresponding to the next 02:00 BRT boundary.
    """
    aware_brt = after_utc.replace(tzinfo=timezone.utc).astimezone(_BRT)
    target_brt = aware_brt.replace(hour=2, minute=0, second=0, microsecond=0)
    if aware_brt >= target_brt:
        target_brt += timedelta(days=1)
    return target_brt.astimezone(timezone.utc).replace(tzinfo=None)


def _needs_startup_reconcile(
    db: dbHandlerSummary,
    consumer_name: str,
    stale_threshold: timedelta,
) -> bool:
    """Decide whether startup must force a full reconcile.

    The worker trusts the last successful checkpoint stored in
    ``SUMMARY_WORKER_STATE``. If that state is missing, too old, or cannot be
    read, the safe choice is to rebuild the full read model before starting
    incremental consumption.
    """
    try:
        state = db.read_worker_state(consumer_name)
        last_success = state.get("DT_LAST_SUCCESS")
        return last_success is None or datetime.utcnow() - last_success > stale_threshold
    except Exception as exc:
        log.warning_event("summary_startup_state_read_failed", error=repr(exc))
        return True


def _schedule_startup_reconcile(db: dbHandlerSummary) -> datetime:
    """Choose the first full-reconcile deadline for this process lifetime.

    Returns:
        ``datetime.min`` when a reconcile must run immediately, otherwise the
        next scheduled 02:00 BRT slot in UTC.
    """
    stale_threshold = timedelta(
        seconds=getattr(k, "SUMMARY_WORKER_STALE_THRESHOLD_SEC", 3600)
    )
    if _needs_startup_reconcile(db, k.SUMMARY_WORKER_CONSUMER_NAME, stale_threshold):
        log.event("summary_startup_reconcile_required")
        return datetime.min

    next_reconcile_at = _next_2am_brt(datetime.utcnow())
    log.event(
        "summary_startup_reconcile_skipped",
        next_reconcile_utc=next_reconcile_at.isoformat(),
    )
    return next_reconcile_at


def _run_full_reconcile(
    db: dbHandlerSummary,
    engine: SummaryRefreshEngine,
    now: datetime,
) -> datetime:
    """Run the full refresh chain and schedule the next daily rebuild.

    A full reconcile rebuilds every public summary table and then clears the
    incremental queue because all pending outbox scopes are now represented by
    the freshly rebuilt read model.
    """
    db.mark_worker_start(k.SUMMARY_WORKER_CONSUMER_NAME)
    engine.refresh_all(reason="scheduled_full_reconcile")
    db.reset_after_reconcile(k.SUMMARY_WORKER_CONSUMER_NAME)
    next_reconcile_at = _next_2am_brt(now)
    log.event(
        "summary_next_reconcile_scheduled",
        next_reconcile_utc=next_reconcile_at.isoformat(),
    )
    return next_reconcile_at


def _run_incremental_batch(db: dbHandlerSummary, engine: SummaryRefreshEngine) -> bool:
    """Process at most one incremental outbox batch.

    Returns:
        ``True`` when at least one outbox row was processed, otherwise
        ``False`` so the caller can sleep before polling again.
    """
    batch = db.read_outbox_batch(
        k.SUMMARY_WORKER_CONSUMER_NAME,
        batch_size=k.SUMMARY_WORKER_BATCH_SIZE,
    )
    if not batch:
        return False

    # Success is checkpointed only after the refresh finishes. Then the
    # consumed rows are drained so the outbox behaves like a real queue.
    db.mark_worker_start(k.SUMMARY_WORKER_CONSUMER_NAME)
    engine.refresh_for_events(batch)
    db.mark_worker_success(
        k.SUMMARY_WORKER_CONSUMER_NAME,
        last_outbox_id=int(batch[-1]["ID_OUTBOX"]),
        batch_size=k.SUMMARY_WORKER_BATCH_SIZE,
        event_count=len(batch),
    )
    db.drain_consumed_outbox(k.SUMMARY_WORKER_CONSUMER_NAME)
    return True


def _finalize_error(db: dbHandlerSummary, exc: Exception) -> None:
    """Persist worker failure state and emit the structured error log."""
    error_message = repr(exc)
    db.mark_worker_failure(
        k.SUMMARY_WORKER_CONSUMER_NAME,
        error_message=error_message,
    )
    log.error_event(
        "summary_worker_loop_failed",
        service=SERVICE_NAME,
        error=error_message,
    )


def main() -> None:
    """Run the summary maintenance loop until shutdown is requested."""
    log.service_start(SERVICE_NAME)
    db, lock_db = _init_db()

    if not lock_db.acquire_worker_lock(WORKER_LOCK_NAME):
        log.warning_event("summary_worker_lock_busy", service=SERVICE_NAME)
        sys.exit(0)

    engine = SummaryRefreshEngine(db=db, logger=log)
    next_reconcile_at = _schedule_startup_reconcile(db)

    try:
        try:
            # Apply the low-contention session settings once per process.
            db.configure_worker_session()
        except Exception as exc:
            _finalize_error(db, exc)
            sys.exit(1)

        while process_status["running"]:
            try:
                now = datetime.utcnow()
                # Full reconcile always runs before any incremental batch due
                # on the same cycle so the rebuilt snapshot becomes ground truth.
                if now >= next_reconcile_at:
                    next_reconcile_at = _run_full_reconcile(db, engine, now)

                batch_processed = _run_incremental_batch(db, engine)
                if batch_processed:
                    continue
                # No work pending. Sleep with jitter to avoid synchronized wakeups.
                runtime_sleep.random_jitter_sleep()
            except Exception as exc:
                _finalize_error(db, exc)
                runtime_sleep.random_jitter_sleep()
    finally:
        lock_db.release_worker_lock(WORKER_LOCK_NAME)
        db.close()
        log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    main()
