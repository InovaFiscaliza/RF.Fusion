#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Summary-domain database handler for the incremental RFFUSION_SUMMARY worker.

This module provides :class:`dbHandlerSummary`, a thin specialization of
:class:`DBHandlerBase` that adds all the database operations required by the
``appCataloga_rffusion_summary_worker`` daemon.  It covers four areas:

1. **Worker lifecycle state** — reading/writing ``BPDATA.SUMMARY_WORKER_STATE``
   so the daemon can persist its outbox checkpoint across restarts and expose
   health status to monitoring.

2. **Outbox consumption** — reading ``BPDATA.SUMMARY_OUTBOX`` rows in order,
   pruning rows that are already behind the durable checkpoint.

3. **Refresh audit trail** — writing start/success/failure events to
   ``SUMMARY_REFRESH_STATE`` and ``SUMMARY_REFRESH_LOG`` for every summary
   object rebuild attempted by the engine.

4. **Write operations for summary tables** — ``replace_table_rows`` (truncate
   + bulk-insert for full-snapshot rebuilds) and ``upsert_rows`` /
   ``execute_delete`` (incremental scope-targeted writes).

Connection strategy
-------------------
The handler is instantiated with ``reuse_connection=True`` so the summary
worker shares one long-lived connection.  Every public method explicitly calls
``_connect()`` at entry and ``_disconnect()`` in a ``finally`` block so the
connection lifecycle is visible and consistent across methods.  The session
isolation level and lock-wait timeout are configured once at startup via
:meth:`configure_worker_session`.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

import config as k
from .dbHandlerBase import DBHandlerBase


class dbHandlerSummary(DBHandlerBase):
    """Database handler for all RFFUSION_SUMMARY and outbox operations.

    Extends :class:`DBHandlerBase` with high-level methods that encapsulate
    the SQL statements needed by the ``appCataloga_rffusion_summary_worker``
    service.  The :class:`SummaryRefreshEngine` calls these methods; it never
    touches ``_connect`` / ``_disconnect`` or raw SQL directly.

    Attributes:
        in_transaction (bool): True while a managed explicit transaction opened
            by :meth:`begin_transaction` is active.  Most methods ignore this
            flag; the flag is used only by the optional managed-transaction API
            (begin / commit / rollback) which are available for callers that
            need to group several writes atomically outside the ``autocommit``
            default.
    """

    def __init__(
        self,
        database: str,
        log: Any,
        reuse_connection: bool = True,
    ) -> None:
        """Initialize the handler bound to a specific summary database.

        Args:
            database: Name of the target schema (typically
                      ``k.SUMMARY_DATABASE_NAME``, e.g. ``'RFFUSION_SUMMARY'``).
            log:      Application logger instance.
            reuse_connection: When ``True`` (the default), the underlying
                      :class:`DBHandlerBase` keeps the same TCP connection alive
                      between ``_connect`` / ``_disconnect`` calls.  Should be
                      ``True`` for the long-lived worker process.
        """
        super().__init__(
            database=database,
            log=log,
            reuse_connection=reuse_connection,
        )
        self.log.entry(f"[dbHandlerSummary] Initialized for DB '{database}'")
        self.in_transaction: bool = False

    def begin_transaction(self) -> None:
        """Open a managed explicit transaction, disabling autocommit.

        After this call, writes through the base-class helper methods
        (``_insert_row``, ``_update_row``, etc.) are not committed until
        :meth:`commit` is called.  Use :meth:`rollback` to abort.

        Note:
            Most engine methods use per-call ``commit=True`` on the base
            helpers instead of this API.  The managed-transaction API is
            provided for future callers that need to group multiple writes
            atomically.
        """
        self.in_transaction = True
        self._connect()
        self.db_connection.autocommit = False

    def commit(self) -> None:
        """Commit the active managed transaction and restore autocommit.

        A no-op if :meth:`begin_transaction` has not been called.
        Restores ``autocommit=True`` and clears ``in_transaction`` regardless
        of whether the commit itself succeeds.
        """
        if not self.in_transaction:
            return

        try:
            self.db_connection.commit()
        finally:
            self.db_connection.autocommit = True
            self.in_transaction = False

    def rollback(self) -> None:
        """Rollback the active managed transaction and restore autocommit.

        A no-op if :meth:`begin_transaction` has not been called.
        Restores ``autocommit=True`` and clears ``in_transaction`` regardless
        of whether the rollback itself succeeds.
        """
        if not self.in_transaction:
            return

        try:
            self.db_connection.rollback()
        finally:
            self.db_connection.autocommit = True
            self.in_transaction = False

    def configure_worker_session(self) -> None:
        """Apply the low-contention session settings for the summary worker.

        Two settings are applied once at startup:

        * ``READ COMMITTED`` isolation level — avoids gap-lock escalation by
          not acquiring next-key locks on index scans.  The summary tables are
          read-model writes (not transactional data), so phantom-read protection
          is unnecessary and the stricter default ``REPEATABLE READ`` would
          increase lock contention against BPDATA writers.

        * ``innodb_lock_wait_timeout = 5`` — caps the per-statement lock wait
          at 5 seconds so that a slow summary write never holds back the
          operational workers for more than a few seconds.  The worker loop
          catches the resulting ``OperationalError`` and retries on the next
          poll cycle.
        """
        self._connect()
        try:
            # READ COMMITTED avoids next-key locks on scans; summary writes are
            # not transactional data, so phantom-read protection is unnecessary.
            self.cursor.execute(
                "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"
            )
            # Cap lock-wait at 5 s so a slow summary write never blocks operational workers.
            self.cursor.execute("SET SESSION innodb_lock_wait_timeout = 5")
        finally:
            self._disconnect()

    def acquire_worker_lock(self, lock_name: str) -> bool:
        """Attempt to claim a named MariaDB user-lock (non-blocking).

        Uses ``GET_LOCK(name, 0)`` with zero timeout so the call returns
        immediately if another session holds the lock.  This prevents two
        summary worker processes from running concurrently.

        Note:
            In the current worker implementation, lock acquisition is handled
            directly by the worker's ``lock_db`` connection using
            :meth:`DBHandlerBase._select_raw` without going through this method.
            This method is retained for use by alternative callers or testing.

        Args:
            lock_name: The MariaDB user-lock name (e.g. ``'RFFUSION_SUMMARY_PY_WORKER'``).

        Returns:
            ``True`` if the lock was acquired, ``False`` if already held.
        """
        self._connect()
        try:
            rows = self._select_raw(
                "SELECT GET_LOCK(%s, 0) AS LOCK_ACQUIRED",
                (lock_name,),
            )
            if not rows:
                return False
            return bool(rows[0].get("LOCK_ACQUIRED"))
        finally:
            self._disconnect()

    def release_worker_lock(self, lock_name: str) -> None:
        """Release a held named MariaDB user-lock.

        Uses ``RELEASE_LOCK(name)``.  A no-op at the DB level if the lock is
        not held by this session (MariaDB returns 0, which is silently ignored).

        Note:
            As with :meth:`acquire_worker_lock`, the worker currently releases
            the lock directly on the ``lock_db`` connection in its ``finally``
            block rather than using this method.

        Args:
            lock_name: The MariaDB user-lock name passed to
                       :meth:`acquire_worker_lock`.
        """
        self._connect()
        try:
            self._select_raw("SELECT RELEASE_LOCK(%s) AS LOCK_RELEASED", (lock_name,))
        finally:
            self._disconnect()

    def disable_sql_event(self, event_name: str) -> None:
        """Disable the legacy MariaDB event-scheduler refresh path.

        The original RFFUSION_SUMMARY refresh was driven by a scheduled
        MariaDB event that ran stored procedures.  When the Python worker is
        deployed, that event should be disabled to avoid double-writes and the
        deadlocks that motivated the migration.

        Whether this method is called at startup is controlled by the
        ``SUMMARY_WORKER_DISABLE_SQL_EVENT_ON_START`` config flag.  When the
        event has already been removed from the schema, the ``ALTER EVENT``
        statement fails; the worker logs a warning and continues rather than
        aborting.

        Args:
            event_name: The MariaDB event name as stored in ``information_schema``
                        (e.g. ``k.SUMMARY_WORKER_SQL_EVENT_NAME``).
        """
        self._connect()
        try:
            self._execute_custom(
                f"ALTER EVENT `{event_name}` DISABLE",
                commit=True,
            )
        finally:
            self._disconnect()

    def read_worker_state(self, consumer_name: str) -> Dict[str, Any]:  # noqa: D401
        """Return the checkpoint row for one consumer, creating it if absent.

        The state row stores the durable outbox position (``ID_LAST_OUTBOX``)
        and simple health flags.  If the row does not exist yet (first run after
        schema initialization), it is inserted with ``ID_LAST_OUTBOX = 0`` so
        the worker starts reading from the beginning of the outbox.

        Args:
            consumer_name: The consumer identifier string stored in
                           ``NA_CONSUMER`` (e.g. ``k.SUMMARY_WORKER_CONSUMER_NAME``).

        Returns:
            A dict with at minimum ``{'NA_CONSUMER': str, 'ID_LAST_OUTBOX': int,
            'NA_STATUS': str}``.  May include additional columns if the schema
            has been extended.
        """
        self._connect()
        try:
            rows = self._select_rows(
                table="BPDATA.SUMMARY_WORKER_STATE",
                where={"NA_CONSUMER": consumer_name},
                limit=1,
            )
            if rows:
                return rows[0]

            # Row not found — first run after schema init; self-bootstrap keeps
            # deployment simple (no manual INSERT required after migration).
            payload = {
                "NA_CONSUMER": consumer_name,
                "ID_LAST_OUTBOX": 0,
                "NA_STATUS": "idle",
            }
            self._insert_row(
                table="BPDATA.SUMMARY_WORKER_STATE",
                data=payload,
                commit=True,
                log_success=False,
            )
            return payload
        finally:
            self._disconnect()

    def mark_worker_start(self, consumer_name: str) -> None:
        """Record that one refresh pass has started.

        Updates the state row to ``NA_STATUS = 'running'`` and stamps
        ``DT_LAST_START``.  Called before both full-reconcile and incremental
        batch passes so the monitoring dashboard can detect stalled workers.

        Args:
            consumer_name: Consumer identifier (see :meth:`read_worker_state`).
        """
        self._connect()
        try:
            self._update_row(
                table="BPDATA.SUMMARY_WORKER_STATE",
                data={
                    "DT_LAST_START": datetime.utcnow(),
                    "NA_STATUS": "running",
                    "NA_ERROR_MESSAGE": None,
                },
                where={"NA_CONSUMER": consumer_name},
                commit=True,
            )
        finally:
            self._disconnect()

    def mark_worker_success(
        self,
        consumer_name: str,
        *,
        last_outbox_id: int,
        batch_size: int,
        event_count: int,
    ) -> None:
        """Advance the durable outbox checkpoint after a successful refresh pass.

        Updates ``ID_LAST_OUTBOX`` to ``last_outbox_id`` so that the next
        :meth:`read_outbox_batch` call returns only rows after this position.
        Also stamps ``DT_LAST_SUCCESS``, resets ``NA_STATUS`` to ``'idle'``,
        and records the batch size and event count for diagnostics.

        Args:
            consumer_name:  Consumer identifier.
            last_outbox_id: The ``ID_OUTBOX`` of the last row processed in this
                            pass.  Pass ``0`` for full-reconcile passes that did
                            not consume any outbox rows.
            batch_size:     The configured maximum batch size (for audit logs).
            event_count:    The actual number of outbox events processed.
        """
        self._connect()
        try:
            now = datetime.utcnow()
            self._update_row(
                table="BPDATA.SUMMARY_WORKER_STATE",
                data={
                    "ID_LAST_OUTBOX": int(last_outbox_id),
                    "DT_LAST_END": now,
                    "DT_LAST_SUCCESS": now,
                    "NU_LAST_BATCH_SIZE": int(batch_size),
                    "NU_LAST_EVENT_COUNT": int(event_count),
                    "NA_STATUS": "idle",
                    "NA_ERROR_MESSAGE": None,
                },
                where={"NA_CONSUMER": consumer_name},
                commit=True,
            )
        finally:
            self._disconnect()

    def mark_worker_failure(
        self,
        consumer_name: str,
        *,
        error_message: str,
    ) -> None:
        """Record a refresh failure without advancing the outbox checkpoint.

        Sets ``NA_STATUS = 'error'``, stamps ``DT_LAST_FAILURE``, and writes
        the error message for operator visibility.  ``ID_LAST_OUTBOX`` is NOT
        updated so the same batch will be retried on the next poll cycle.

        Args:
            consumer_name: Consumer identifier.
            error_message: ``repr(exc)`` string from the caught exception.
        """
        self._connect()
        try:
            now = datetime.utcnow()
            self._update_row(
                table="BPDATA.SUMMARY_WORKER_STATE",
                data={
                    "DT_LAST_END": now,
                    "DT_LAST_FAILURE": now,
                    "NA_STATUS": "error",
                    "NA_ERROR_MESSAGE": error_message,
                },
                where={"NA_CONSUMER": consumer_name},
                commit=True,
            )
        finally:
            self._disconnect()

    def read_outbox_batch(
        self,
        consumer_name: str,
        *,
        batch_size: int,
    ) -> List[Dict[str, Any]]:
        """Return the next N append-only outbox rows after the stored checkpoint.

        Reads ``BPDATA.SUMMARY_OUTBOX`` using the ``ID_LAST_OUTBOX`` position
        stored in the consumer's state row.  Rows are returned in ascending
        ``ID_OUTBOX`` order so the engine always processes events in the order
        they were published.

        The ``JS_PAYLOAD`` column is stored as opaque JSON text (or bytes on
        some MySQL driver versions).  This method decodes it and returns a
        Python dict in the ``'JS_PAYLOAD'`` key of each row so callers never
        need to call ``json.loads`` themselves.  JSON parse failures silently
        produce an empty dict rather than raising.

        Args:
            consumer_name: Consumer identifier used to look up the checkpoint.
            batch_size:    Maximum number of rows to return (``LIMIT`` clause).

        Returns:
            List of row dicts.  Each dict has: ``ID_OUTBOX`` (int),
            ``NA_EVENT_TYPE``, ``NA_SOURCE_HANDLER``, ``JS_PAYLOAD`` (dict),
            ``DT_CREATED_AT``.
            Returns an empty list when no new rows are available.
        """
        state = self.read_worker_state(consumer_name)
        last_outbox_id = int(state.get("ID_LAST_OUTBOX") or 0)

        self._connect()
        try:
            rows = self._select_raw(
                """
                SELECT
                    ID_OUTBOX,
                    NA_EVENT_TYPE,
                    NA_SOURCE_HANDLER,
                    JS_PAYLOAD,
                    DT_CREATED_AT
                FROM BPDATA.SUMMARY_OUTBOX
                WHERE ID_OUTBOX > %s
                ORDER BY ID_OUTBOX ASC
                LIMIT %s
                """,
                (last_outbox_id, int(batch_size)),
            )
        finally:
            self._disconnect()

        # The table stores opaque JSON so producers stay decoupled from SQL shape.
        # Some MySQL/MariaDB driver versions return BLOB columns as bytes rather than str.
        parsed_rows: List[Dict[str, Any]] = []
        for row in rows:
            payload = row.get("JS_PAYLOAD") or "{}"  # NULL column → treat as empty object
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8", errors="replace")  # driver returned BLOB bytes
            try:
                payload_dict = json.loads(payload)
            except Exception:
                payload_dict = {}  # malformed JSON → silently skip rather than crash

            parsed = dict(row)
            parsed["JS_PAYLOAD"] = payload_dict
            parsed_rows.append(parsed)

        return parsed_rows

    def prune_processed_outbox(
        self,
        consumer_name: str,
        *,
        keep_days: int,
    ) -> int:
        """Delete outbox rows that are both behind the checkpoint and old.

        The deletion is bounded by **two** conditions:

        1. ``ID_OUTBOX <= ID_LAST_OUTBOX`` — only rows already durably checkpointed
           are eligible.  This guarantees the worker can always recover from
           ``ID_LAST_OUTBOX = 0`` by reading from the beginning of the surviving
           rows.
        2. ``DT_CREATED_AT < utcnow() - keep_days`` — keeps a rolling window of
           recent rows even if they are behind the checkpoint, for debugging.

        This method is called approximately once per hour when the outbox is
        idle (no new events).

        Args:
            consumer_name: Consumer identifier used to look up the checkpoint.
            keep_days:     Minimum age (in days) of rows to prune.  Values less
                           than 1 are treated as 1 to prevent runaway deletion.

        Returns:
            Number of rows deleted, or ``0`` if the checkpoint is at position 0.
        """
        state = self.read_worker_state(consumer_name)
        last_outbox_id = int(state.get("ID_LAST_OUTBOX") or 0)
        if last_outbox_id <= 0:  # nothing has been checkpointed yet — no-op
            return 0

        # max(1, ...) guards against a config value of 0 which would delete everything.
        cutoff = datetime.utcnow() - timedelta(days=max(1, int(keep_days)))

        self._connect()
        try:
            return self._execute_custom(
                """
                DELETE FROM BPDATA.SUMMARY_OUTBOX
                WHERE ID_OUTBOX <= %s
                  AND DT_CREATED_AT < %s
                """,
                (last_outbox_id, cutoff),
                commit=True,
            )
        finally:
            self._disconnect()

    def drain_consumed_outbox(self, consumer_name: str) -> int:
        """Delete all outbox rows that have already been durably checkpointed.

        Implements true queue semantics: once a batch is committed via
        :meth:`mark_worker_success`, the consumed rows are removed immediately
        so ``BPDATA.SUMMARY_OUTBOX`` does not grow indefinitely.

        Only rows with ``ID_OUTBOX <= ID_LAST_OUTBOX`` are deleted; rows ahead
        of the checkpoint (not yet processed) are never touched.

        Args:
            consumer_name: Consumer identifier used to look up the checkpoint.

        Returns:
            Number of rows deleted, or ``0`` if the checkpoint is still at 0.
        """
        state = self.read_worker_state(consumer_name)
        last_outbox_id = int(state.get("ID_LAST_OUTBOX") or 0)
        if last_outbox_id <= 0:  # nothing processed yet — nothing to delete
            return 0

        self._connect()
        try:
            return self._execute_custom(
                "DELETE FROM BPDATA.SUMMARY_OUTBOX WHERE ID_OUTBOX <= %s",
                (last_outbox_id,),
                commit=True,
            )
        finally:
            self._disconnect()

    def reset_after_reconcile(self, consumer_name: str) -> None:
        """Clean up both queue tables after a successful full reconcile.

        After :meth:`~summary_handler.engine.SummaryRefreshEngine.refresh_all`
        completes, the summary database is ground truth.  Accumulated
        ``SUMMARY_OUTBOX`` rows are irrelevant (already reflected by the
        reconcile) and the outbox position must be reset to 0 so the next
        incremental cycle starts from the beginning of any new events.

        What this method does:

        * ``BPDATA.SUMMARY_OUTBOX`` — **all** rows deleted.
        * ``BPDATA.SUMMARY_WORKER_STATE`` — ``ID_LAST_OUTBOX`` reset to 0,
          ``DT_LAST_SUCCESS`` stamped to now.  The row is **kept** (not
          deleted) so the startup heuristic can read ``DT_LAST_SUCCESS`` on
          the next restart and decide whether a new reconcile is necessary.

        Args:
            consumer_name: Consumer identifier whose state row to update.
        """
        self._connect()
        try:
            # Remove every outbox row — the full reconcile already covered them.
            self._execute_custom(
                "DELETE FROM BPDATA.SUMMARY_OUTBOX",
                (),
                commit=False,
            )
            # Reset the checkpoint to 0 and stamp DT_LAST_SUCCESS so the
            # startup heuristic can determine how fresh the summary is.
            # The row is upserted rather than deleted so DT_LAST_SUCCESS
            # survives across restarts.
            now = datetime.utcnow()
            self._execute_custom(
                """
                INSERT INTO BPDATA.SUMMARY_WORKER_STATE
                    (NA_CONSUMER, ID_LAST_OUTBOX, DT_LAST_SUCCESS, NA_STATUS)
                VALUES (%s, 0, %s, 'idle')
                ON DUPLICATE KEY UPDATE
                    ID_LAST_OUTBOX  = 0,
                    DT_LAST_SUCCESS = VALUES(DT_LAST_SUCCESS),
                    NA_STATUS       = 'idle'
                """,
                (consumer_name, now),
                commit=True,
            )
        finally:
            self._disconnect()

    def summary_refresh_start(self, object_name: str) -> datetime:
        """Record that one summary-object rebuild has been initiated.

        Upserts a row in ``SUMMARY_REFRESH_STATE`` setting ``IS_SUCCESS = 0``
        and a fresh ``DT_LAST_START``.  If the engine subsequently calls
        :meth:`summary_refresh_success`, ``IS_SUCCESS`` will be updated to 1.
        If it calls :meth:`summary_refresh_failure`, ``IS_SUCCESS`` stays 0 and
        the error message is stored.  The state table always reflects the latest
        attempt for each object.

        Args:
            object_name: Summary table name used as the state row key
                         (e.g. ``'HOST_CURRENT_SNAPSHOT'``).

        Returns:
            The UTC datetime captured at the start of this call (passed back to
            :meth:`summary_refresh_success` / :meth:`summary_refresh_failure`
            to compute elapsed time).
        """
        started_at = datetime.utcnow()
        self._connect()
        try:
            # Upsert with IS_SUCCESS=0 and cleared timestamps so a stalled
            # refresh (no matching success/failure call) is visible in the state table.
            self._upsert_row(
                table="SUMMARY_REFRESH_STATE",
                data={
                    "NA_OBJECT_NAME": object_name,
                    "DT_LAST_START": started_at,
                    "DT_LAST_END": None,
                    "IS_SUCCESS": 0,  # will be set to 1 only on explicit success call
                    "NU_LAST_ROW_COUNT": None,
                    "NA_SOURCE_HIGH_WATERMARK": None,
                    "NA_ERROR_MESSAGE": None,
                },
                unique_keys=["NA_OBJECT_NAME"],
                commit=True,
                log_each=False,
            )
        finally:
            self._disconnect()
        return started_at

    def summary_refresh_success(
        self,
        object_name: str,
        *,
        started_at: datetime,
        row_count: int,
        high_watermark: Optional[str] = None,
    ) -> None:
        """Record a successful summary-object rebuild.

        Performs two writes in sequence:

        1. Updates ``SUMMARY_REFRESH_STATE`` with ``IS_SUCCESS = 1``, the
           finished timestamp, row count, and the optional high-watermark tag
           (a short diagnostic string describing the scope, e.g. ``'rows=42'``).
           This write uses ``commit=False`` because the state row and the log
           entry should land in the same commit.
        2. Inserts one row in ``SUMMARY_REFRESH_LOG`` with full timing and
           metadata for a permanent audit trail, then commits.

        Args:
            object_name:     Summary table name (key in SUMMARY_REFRESH_STATE).
            started_at:      Datetime returned by :meth:`summary_refresh_start`.
            row_count:       Number of rows written by the refresh step.
            high_watermark:  Optional short diagnostic string stored for
                             monitoring (e.g. ``'hosts=12;month=2026-05'``).
        """
        finished_at = datetime.utcnow()
        self._connect()
        try:
            # commit=False on state update so both writes land in the same transaction.
            self._update_row(
                table="SUMMARY_REFRESH_STATE",
                data={
                    "DT_LAST_END": finished_at,
                    "IS_SUCCESS": 1,
                    "NU_LAST_ROW_COUNT": int(row_count),
                    "NA_SOURCE_HIGH_WATERMARK": high_watermark,
                    "NA_ERROR_MESSAGE": None,
                },
                where={"NA_OBJECT_NAME": object_name},
                commit=False,  # defer commit until log row is also ready
            )
            self._insert_row(
                table="SUMMARY_REFRESH_LOG",
                data={
                    "NA_OBJECT_NAME": object_name,
                    "DT_STARTED_AT": started_at,
                    "DT_FINISHED_AT": finished_at,
                    "IS_SUCCESS": 1,
                    "NU_ROW_COUNT": int(row_count),
                    "NA_SOURCE_HIGH_WATERMARK": high_watermark,
                    "NA_ERROR_MESSAGE": None,
                },
                commit=True,  # commits both rows atomically
                log_success=False,
            )
        finally:
            self._disconnect()

    def summary_refresh_failure(
        self,
        object_name: str,
        *,
        started_at: datetime,
        error_message: str,
    ) -> None:
        """Record a failed summary-object rebuild.

        Updates ``SUMMARY_REFRESH_STATE`` with ``IS_SUCCESS = 0`` and the
        error message, then inserts one row in ``SUMMARY_REFRESH_LOG`` so the
        failure is permanently auditable.  Mirrors the structure of
        :meth:`summary_refresh_success` so monitoring queries can use the same
        columns against both outcomes.

        Args:
            object_name:   Summary table name (key in SUMMARY_REFRESH_STATE).
            started_at:    Datetime returned by :meth:`summary_refresh_start`.
            error_message: String representation of the caught exception.
        """
        finished_at = datetime.utcnow()
        self._connect()
        try:
            self._update_row(
                table="SUMMARY_REFRESH_STATE",
                data={
                    "DT_LAST_END": finished_at,
                    "IS_SUCCESS": 0,
                    "NA_ERROR_MESSAGE": error_message,
                },
                where={"NA_OBJECT_NAME": object_name},
                commit=False,  # defer commit until log row is also ready
            )
            self._insert_row(
                table="SUMMARY_REFRESH_LOG",
                data={
                    "NA_OBJECT_NAME": object_name,
                    "DT_STARTED_AT": started_at,
                    "DT_FINISHED_AT": finished_at,
                    "IS_SUCCESS": 0,
                    "NU_ROW_COUNT": 0,
                    "NA_SOURCE_HIGH_WATERMARK": None,
                    "NA_ERROR_MESSAGE": error_message,
                },
                commit=True,  # commits both rows atomically
                log_success=False,
            )
        finally:
            self._disconnect()

    def replace_table_rows(self, table: str, rows: List[Dict[str, Any]]) -> int:
        """Atomically replace all rows in a summary table using a shadow-table swap.

        Writes new rows into a ``{table}_shadow`` staging table, then issues a
        single ``RENAME TABLE`` statement to atomically promote the shadow as the
        new live table.  Readers observe either the previous complete snapshot or
        the new one — there is no instant where the table appears empty or
        partially written.

        This replaces the naïve TRUNCATE+INSERT approach, which left the live
        table empty during the bulk-insert phase and caused read failures in
        MATLAB and webfusion during long rebuild passes.

        The ``{table}_shadow`` table is created automatically on the first call
        (``CREATE TABLE IF NOT EXISTS ... LIKE {table}``) and reused on every
        subsequent call, so no manual schema migration is needed.

        Swap cycle for each call::

            TRUNCATE {table}_shadow           (DDL → implicit commit; staging is empty)
            INSERT INTO {table}_shadow ...    (bulk-insert new rows, then commit)
            RENAME TABLE {table}        → {table}_old,  ← atomic swap point
                         {table}_shadow → {table},
                         {table}_old    → {table}_shadow  ← ready for next cycle

        After the rename: ``{table}`` holds the new rows; ``{table}_shadow`` holds
        the previous snapshot and will be truncated at the start of the next call.

        Note:
            Column names are inferred from the first row's keys.  Every row in
            ``rows`` must have the exact same set of keys.  An empty ``rows``
            list produces an empty live table (the swap still happens).

        Args:
            table: Unqualified table name within RFFUSION_SUMMARY
                   (e.g. ``'HOST_CURRENT_SNAPSHOT'``).
            rows:  New rows to publish.

        Returns:
            Number of rows inserted (``len(rows)``).
        """
        shadow = f"{table}_shadow"
        self._connect()
        try:
            # Create the shadow table once; IF NOT EXISTS makes subsequent calls no-ops.
            # Using LIKE preserves indexes and column definitions automatically.
            self._execute_custom(
                f"CREATE TABLE IF NOT EXISTS {shadow} LIKE {table}",
                commit=True,
            )

            # TRUNCATE is DDL → implicit commit; shadow is guaranteed empty from here.
            # Any partial state from a previous crashed cycle is cleared.
            self._execute_custom(f"TRUNCATE TABLE {shadow}", commit=True)

            if rows:
                # Build and execute the bulk INSERT into the shadow (invisible to readers).
                columns = list(rows[0].keys())
                values = [tuple(row.get(col) for col in columns) for row in rows]
                placeholders = ", ".join(["%s"] * len(columns))
                sql = (
                    f"INSERT INTO {shadow} ({', '.join(columns)}) "
                    f"VALUES ({placeholders})"
                )
                self._execute_many_custom(sql, values, commit=True)

            # Single DDL statement atomically promotes shadow → live.
            # RENAME TABLE acquires a metadata lock for only the instant of the swap;
            # no reader ever sees an empty table or a partial write.
            # {table}_old is transient (exists only for the duration of this statement).
            self._execute_custom(
                f"RENAME TABLE {table} TO {table}_old, "
                f"{shadow} TO {table}, "
                f"{table}_old TO {shadow}",
                commit=True,
            )
            return len(rows)
        finally:
            self._disconnect()

    def upsert_rows(
        self,
        *,
        table: str,
        rows: List[Dict[str, Any]],
        unique_keys: List[str],
    ) -> int:
        """Idempotently insert-or-update rows into one summary table.

        Delegates to :meth:`DBHandlerBase._upsert_batch` with a fixed batch
        size of 500 rows.  Suitable for the incremental-scope write path where
        only a subset of a summary table needs to be updated.

        Callers are expected to call :meth:`execute_delete` (via
        :meth:`SummaryRefreshEngine._delete_with_scope`) before this method to
        remove the stale rows for the dirty scope, then call this method to
        re-insert the freshly computed replacements.

        Args:
            table:       Target RFFUSION_SUMMARY table name.
            rows:        List of dicts to upsert.  Returns ``0`` immediately for
                         an empty list (no DB round-trip).
            unique_keys: Column names that form the conflict-detection key for
                         the ``ON DUPLICATE KEY UPDATE`` clause.

        Returns:
            Number of rows submitted to ``executemany`` (not rows changed;
            unchanged rows count as 1 each in MySQL row-counts).
        """
        if not rows:
            return 0

        self._connect()
        try:
            return self._upsert_batch(
                table=table,
                rows=rows,
                unique_keys=unique_keys,
                batch_size=500,
                commit=True,
            )
        finally:
            self._disconnect()

    def execute_delete(self, sql: str, params: Iterable[Any]) -> int:
        """Execute one targeted DELETE statement against summary tables.

        This is a thin pass-through that adds the ``_connect`` / ``_disconnect``
        lifecycle around :meth:`DBHandlerBase._execute_custom`.  It exists so
        the engine can issue the scoped DELETE SQL generated by
        :meth:`SummaryRefreshEngine._delete_with_scope` without knowing about
        the connection lifecycle.

        Args:
            sql:    Parameterized DELETE SQL with ``%s`` placeholders.
            params: Iterable of positional parameter values.

        Returns:
            Number of rows deleted (``cursor.rowcount``).
        """
        self._connect()
        try:
            return self._execute_custom(sql, tuple(params), commit=True)
        finally:
            self._disconnect()
