#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Worker service that keeps the RFFUSION_SUMMARY read-only database up to date.

Architecture overview
---------------------
RFFUSION_SUMMARY is a denormalized, read-optimised database used by dashboards,
maps and the web interface.  It is never written to directly by other services;
instead it is maintained exclusively by this worker via
:class:`~summary_handler.engine.SummaryRefreshEngine`.

Two distinct update strategies coexist:

**Full reconcile**
    Rebuilds all summary tables from scratch by reading the authoritative source
    tables in BPDATA and RFDATA.  Reconcile is heavy (full table scans, many
    JOINs) but guarantees correctness — it is the safety net that catches any
    drift between the summary and the live DB no matter what caused it
    (missed events, bugs, schema migrations, manual fixes).

    Triggered:
    - Automatically on startup, so the summary is always consistent before
      incremental processing begins.
    - Nightly at 02:00 BRT (UTC-3) to correct any drift accumulated during
      the day.

    After a reconcile, both queue tables are fully purged
    (``SUMMARY_OUTBOX`` + ``SUMMARY_WORKER_STATE``) because all accumulated
    events are already reflected in the rebuilt summary — they no longer carry
    any information.

**Incremental update**
    Reads the next batch of ``SUMMARY_OUTBOX`` rows that arrived since the
    last processed position (the *checkpoint*), determines which summary
    objects are affected (the *dirty scope*), and refreshes only those objects.
    This runs continuously between reconciles so that dashboards see new
    measurements within a few seconds of ingestion rather than waiting until
    the nightly rebuild.

    The checkpoint (``ID_LAST_OUTBOX`` in ``SUMMARY_WORKER_STATE``) is the
    durable high-water mark.  It advances only on success, so a failed batch
    is retried automatically on the next poll without losing position.

    After each successful batch the consumed outbox rows are deleted immediately
    (``drain_consumed_outbox``), keeping ``SUMMARY_OUTBOX`` small regardless
    of volume.

Queue tables
------------
``BPDATA.SUMMARY_OUTBOX``
    Append-only event log published by ``dbHandlerBKP`` and ``dbHandlerRFM``
    whenever a measurement or task changes.  Each row holds the FK_HOST and
    event type that the engine needs to scope a targeted refresh.

``BPDATA.SUMMARY_WORKER_STATE``
    Single-row consumer checkpoint table.  Stores ``ID_LAST_OUTBOX`` (durable
    outbox position) and health fields (last start/success/failure timestamps,
    status, error message).

Singleton guard
---------------
Only one instance of this worker may run at a time.  A MariaDB named lock
(``RFFUSION_SUMMARY_PY_WORKER``) is acquired at startup and held until the
process exits.  A second invocation will detect the lock is busy and exit
immediately via ``sys.exit(0)``, leaving the running instance undisturbed.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta, timezone

from bootstrap_paths import bootstrap_app_paths


PROJECT_ROOT = bootstrap_app_paths(__file__)

from db.dbHandlerSummary import dbHandlerSummary
from server_handler import signal_runtime, sleep as runtime_sleep
from shared import logging_utils
from summary_handler.engine import SummaryRefreshEngine
import config as k


SERVICE_NAME = "appCataloga_rffusion_summary_worker"
WORKER_LOCK_NAME = "RFFUSION_SUMMARY_PY_WORKER"

log = logging_utils.log()
process_status = {"running": True}


def _shutdown_cleanup(signal_name: str) -> None:
    """Callback invoked by the signal handler immediately before the loop exits.

    The signal handler (installed below) sets ``process_status['running'] = False``
    so the ``while`` loop exits after the current iteration completes.  Any
    cleanup that must happen after the last iteration (before ``finally``
    releases the DB lock) can be added here.

    Args:
        signal_name: Name of the received signal (e.g. ``'SIGTERM'``).
    """


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
    on_shutdown=_shutdown_cleanup,
)


def _acquire_db_lock(db: dbHandlerSummary) -> bool:
    """Claim the MariaDB named lock that prevents duplicate worker instances.

    Uses ``GET_LOCK(<name>, 0)`` (zero timeout) so the call returns immediately
    rather than blocking.  The lock is held for the lifetime of the *connection*
    owned by ``db``; releasing the connection releases the lock automatically.
    A dedicated ``lock_db`` instance is used in ``main`` so that the lock is
    never accidentally dropped when the main ``db`` handle reconnects.

    Args:
        db: An open (or auto-connecting) ``dbHandlerSummary`` instance whose
            underlying connection will own the lock.

    Returns:
        ``True`` if the lock was acquired, ``False`` if another process already
        holds it.
    """
    db._connect()
    rows = db._select_raw(
        "SELECT GET_LOCK(%s, 0) AS LOCK_ACQUIRED",
        (WORKER_LOCK_NAME,),
    )
    return bool(rows and rows[0].get("LOCK_ACQUIRED"))


def _release_db_lock(db: dbHandlerSummary) -> None:
    """Release the named lock and close the dedicated lock-owning connection.

    Called inside the ``finally`` block in ``main`` so the lock is freed even
    when the worker exits due to an unhandled exception.  After release any
    waiting or newly started instance can immediately claim the lock.

    Args:
        db: The same ``lock_db`` instance passed to :func:`_acquire_db_lock`.
    """
    try:
        db._select_raw("SELECT RELEASE_LOCK(%s) AS LOCK_RELEASED", (WORKER_LOCK_NAME,))
    finally:
        db._disconnect(force=True)


_BRT = timezone(timedelta(hours=-3))  # Brazil Standard Time (UTC-3, no DST)


def _next_2am_brt(after_utc: datetime) -> datetime:
    """Return the next 02:00 BRT as a naive UTC datetime.

    The daily full reconcile is scheduled at 02:00 BRT (low-traffic window)
    rather than running on a fixed interval from the last pass.  This keeps
    the heavy rebuild away from peak hours even if the worker restarts.

    Args:
        after_utc: Reference point (naive UTC).  The returned datetime is
                   always strictly after this value.

    Returns:
        Naive UTC datetime of the next 02:00 BRT.
    """
    aware = after_utc.replace(tzinfo=timezone.utc).astimezone(_BRT)
    target = aware.replace(hour=2, minute=0, second=0, microsecond=0)
    if aware >= target:            # 02:00 already passed today — aim for tomorrow
        target += timedelta(days=1)
    return target.astimezone(timezone.utc).replace(tzinfo=None)


def _needs_startup_reconcile(
    db: dbHandlerSummary,
    consumer_name: str,
    stale_threshold: timedelta,
) -> bool:
    """Return True if a full reconcile should run before incremental processing.

    A reconcile is mandatory when:

    * The consumer state row does not exist or has no ``DT_LAST_SUCCESS``
      (first ever run, or state was manually cleared).
    * The worker was offline longer than ``stale_threshold`` — enough time for
      the summary to have drifted beyond what the outbox can safely correct.

    A reconcile is skipped when:

    * ``DT_LAST_SUCCESS`` is within ``stale_threshold`` seconds — the summary
      is fresh and the outbox has everything needed to catch up incrementally.

    Falls back to ``True`` (reconcile) if the state row cannot be read.

    Args:
        db:              Open DB handler (session already configured).
        consumer_name:   Consumer identifier in ``SUMMARY_WORKER_STATE``.
        stale_threshold: Maximum acceptable offline duration before reconcile
                         is considered mandatory.

    Returns:
        ``True`` if a startup reconcile should run, ``False`` to skip it.
    """
    try:
        state = db.read_worker_state(consumer_name)
        last_success = state.get("DT_LAST_SUCCESS")
        if last_success is None:
            return True  # no previous success recorded — must reconcile
        offline_duration = datetime.utcnow() - last_success
        return offline_duration > stale_threshold
    except Exception as exc:
        log.warning_event("summary_startup_state_read_failed", error=repr(exc))
        return True  # cannot determine freshness — reconcile for safety


def main() -> None:
    """Run the summary worker until a shutdown signal is received.

    Execution flow
    --------------
    1. Instantiate two DB handles: ``db`` for all operational queries and
       ``lock_db`` for the singleton guard lock.
    2. Acquire the MariaDB named lock; exit immediately if already held.
    3. Configure the session (e.g. ``wait_timeout``, ``max_execution_time``).
    4. Optionally disable the legacy SQL Event that predates this worker.
    5. Enter the main polling loop:

       a. **Full reconcile** — if ``now >= next_reconcile_at`` (which is
          ``datetime.min`` on first entry, guaranteeing an immediate startup
          reconcile).  Rebuilds all summary tables, then purges both queue
          tables and schedules the next reconcile for 02:00 BRT.

       b. **Incremental batch** — read the next ``BATCH_SIZE`` outbox rows
          after the stored checkpoint.  If none exist, sleep and loop.  If
          rows exist, refresh only the affected summary objects, advance the
          checkpoint, and drain (delete) the consumed rows immediately.

       c. **Error handling** — any exception inside the loop is caught, the
          failure is recorded in ``SUMMARY_WORKER_STATE`` (checkpoint is NOT
          advanced so the same batch is retried), and the loop sleeps before
          the next attempt.

    6. On exit (signal or unhandled exception): release the lock and close
       all DB connections.
    """
    log.service_start(SERVICE_NAME)

    try:
        # Two separate DB handles are required:
        #   db       — used for all operational queries (outbox reads, engine
        #              writes, state updates).  reuse_connection=True so it
        #              keeps a single persistent connection across loop iterations.
        #   lock_db  — owns the named lock exclusively.  Keeping it separate
        #              ensures the lock is never dropped mid-loop when db
        #              reconnects after a transient network error.
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
        # Another instance is running.  Exit silently so process managers
        # (systemd, supervisord) do not treat this as a failure — it is
        # expected behaviour when the service is restarted while the previous
        # instance is still shutting down.
        log.warning_event(
            "summary_worker_lock_busy",
            service=SERVICE_NAME,
        )
        sys.exit(0)

    engine = SummaryRefreshEngine(db=db, logger=log)

    stale_threshold = timedelta(
        seconds=getattr(k, "SUMMARY_WORKER_STALE_THRESHOLD_SEC", 3600)
    )
    
    # Check DB state before entering the loop so configure_worker_session() is
    # already active when read_worker_state is called inside the helper.
    # next_reconcile_at = datetime.min  → fires on the very first iteration.
    # next_reconcile_at = future time   → skips startup reconcile entirely.
    if _needs_startup_reconcile(db, k.SUMMARY_WORKER_CONSUMER_NAME, stale_threshold):
        next_reconcile_at: datetime = datetime.min
        log.event("summary_startup_reconcile_required")
    else:
        next_reconcile_at = _next_2am_brt(datetime.utcnow())
        log.event(
            "summary_startup_reconcile_skipped",
            next_reconcile_utc=next_reconcile_at.isoformat(),
        )

    try:
        db.configure_worker_session()

        if getattr(k, "SUMMARY_WORKER_DISABLE_SQL_EVENT_ON_START", False):
            # The legacy MariaDB Event (EVT_REFRESH_ALL_RFFUSION_SUMMARY_10MIN)
            # predates this Python worker.  If it was never manually disabled in
            # the DB, this flag lets the worker disable it automatically at
            # startup so the two mechanisms do not run concurrently and produce
            # inconsistent intermediate states.
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
                # ── FULL RECONCILE ────────────────────────────────────────────
                # Decision: run a full reconcile when the scheduled wall-clock
                # time has been reached.  On the very first iteration
                # next_reconcile_at == datetime.min so this always fires at
                # startup.
                #
                # A full reconcile is safe at any time: it reads the current
                # state of the source tables and writes a consistent snapshot
                # to RFFUSION_SUMMARY.  It is slower than incremental but
                # tolerates gaps, duplicates, and any other inconsistencies
                # that may have accumulated in the outbox.
                if now >= next_reconcile_at:
                    db.mark_worker_start(k.SUMMARY_WORKER_CONSUMER_NAME)
                    engine.refresh_all(reason="scheduled_full_reconcile")

                    # After a full reconcile the summary tables are the single
                    # source of truth.  All outbox rows that arrived before
                    # (or during) the reconcile are now irrelevant — they do not
                    # hold any information that is not already in the summary.
                    # Purging both queue tables here achieves two goals:
                    #   1. SUMMARY_OUTBOX stays empty (no unbounded growth).
                    #   2. SUMMARY_WORKER_STATE is reset so incremental picks
                    #      up from scratch (ID_LAST_OUTBOX = 0), which is
                    #      correct because there are no more outbox rows to read.                     db.reset_after_reconcile(k.SUMMARY_WORKER_CONSUMER_NAME)

                    # Pin the next reconcile to a fixed wall-clock time rather
                    # than a relative interval.  This prevents the reconcile
                    # from drifting later and later each day if the worker
                    # restarts frequently, and keeps the heavy rebuild away
                    # from peak traffic hours.
                    next_reconcile_at = _next_2am_brt(now)
                    log.event(
                        "summary_next_reconcile_scheduled",
                        next_reconcile_utc=next_reconcile_at.isoformat(),
                    )

                # ── INCREMENTAL UPDATE ────────────────────────────────────────
                # Read the next batch of SUMMARY_OUTBOX rows whose ID_OUTBOX
                # is strictly greater than ID_LAST_OUTBOX (the durable
                # checkpoint stored in SUMMARY_WORKER_STATE).
                # Rows are ordered by ID_OUTBOX ASC so events are always
                # processed in publish order.
                batch = db.read_outbox_batch(
                    k.SUMMARY_WORKER_CONSUMER_NAME,
                    batch_size=k.SUMMARY_WORKER_BATCH_SIZE,
                )

                if not batch:
                    # Outbox is empty — the summary is up to date.  Sleep
                    # before polling again to avoid busy-waiting on the DB.
                    runtime_sleep.random_jitter_sleep()
                    continue

                # ── PROCESS BATCH ─────────────────────────────────────────────
                # Mark start before any write so monitoring can detect a worker
                # that started a batch but never finished (stall detection).
                db.mark_worker_start(k.SUMMARY_WORKER_CONSUMER_NAME)

                # The engine inspects the batch to build a DirtyScope (the set
                # of affected FK_HOST values and event types) and refreshes
                # only the summary sub-tables that depend on those hosts.
                # This is much faster than a full reconcile when only a few
                # hosts were active.
                engine.refresh_for_events(batch)

                # Advance the durable checkpoint to the last row in the batch.
                # Only called after a successful refresh — if refresh_for_events
                # raises, we fall through to the except block which calls
                # mark_worker_failure (no checkpoint advancement), so the same
                # batch will be retried on the next poll cycle.
                db.mark_worker_success(
                    k.SUMMARY_WORKER_CONSUMER_NAME,
                    last_outbox_id=int(batch[-1]["ID_OUTBOX"]),
                    batch_size=k.SUMMARY_WORKER_BATCH_SIZE,
                    event_count=len(batch),
                )

                # True queue semantics: delete the rows that were just checkpointed.
                # This is safe because ID_LAST_OUTBOX was already committed above;
                # even if drain_consumed_outbox fails the checkpoint is safe and
                # the rows will simply be deleted on the next successful drain.
                db.drain_consumed_outbox(k.SUMMARY_WORKER_CONSUMER_NAME)

            except Exception as exc:
                # Record the failure without advancing the checkpoint so the
                # same batch (or reconcile) is retried.  Sleep before retrying
                # to avoid hammering the DB after a transient error.
                db.mark_worker_failure(
                    k.SUMMARY_WORKER_CONSUMER_NAME,
                    error_message=repr(exc),
                )
                log.error_event(
                    "summary_worker_loop_failed",
                    error=repr(exc),
                )
                runtime_sleep.random_jitter_sleep()

    finally:
        # Always release the named lock regardless of how the worker exits
        # (clean shutdown, unhandled exception, or SIGKILL recovery on restart).
        _release_db_lock(lock_db)
        db._disconnect(force=True)
        log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    main()
