#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Summary-domain database handler for the incremental RFFUSION_SUMMARY worker.

This module provides :class:`dbHandlerSummary`, a thin specialization of
:class:`DBHandlerBase` that adds all the database operations required by the
``appCataloga_summary_database`` daemon. It covers four areas:

1. **Worker lifecycle state** — reading/writing ``RFFUSION_SUMMARY.SUMMARY_WORKER_STATE``
   so the daemon can persist its outbox checkpoint across restarts and expose
   health status to monitoring.

2. **Outbox consumption** — reading ``RFFUSION_SUMMARY.SUMMARY_OUTBOX`` rows in order,
   advancing the durable checkpoint, and deleting rows that are already behind
   that checkpoint.

3. **Refresh audit trail** — writing one bounded rolling audit row to
   ``SUMMARY_REFRESH_LOG`` for every summary object rebuild attempted by the
   engine.

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

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import config as k
from .dbHandlerBase import DBHandlerBase

SUMMARY_OUTBOX_TABLE = f"{k.SUMMARY_DATABASE_NAME}.SUMMARY_OUTBOX"
SUMMARY_WORKER_STATE_TABLE = f"{k.SUMMARY_DATABASE_NAME}.SUMMARY_WORKER_STATE"


class dbHandlerSummary(DBHandlerBase):
    """Database handler for all RFFUSION_SUMMARY and outbox operations.

    Extends :class:`DBHandlerBase` with high-level methods that encapsulate
    the SQL statements needed by the ``appCataloga_summary_database``
    service.  The :class:`SummaryRefreshEngine` calls these methods; it never
    touches ``_connect`` / ``_disconnect`` or raw SQL directly.
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
        """Attempt to claim a named MariaDB user-lock without blocking."""
        self._connect()
        try:
            rows = self._select_raw(
                "SELECT GET_LOCK(%s, 0) AS LOCK_ACQUIRED",
                (lock_name,),
            )
            lock_acquired = bool(rows and rows[0].get("LOCK_ACQUIRED"))
            if lock_acquired:
                return True
        except Exception:
            self._disconnect(force=True)
            raise

        self._disconnect()
        return False

    def release_worker_lock(self, lock_name: str) -> None:
        """Release the named user-lock and close the lock-owning connection."""
        try:
            self._select_raw("SELECT RELEASE_LOCK(%s) AS LOCK_RELEASED", (lock_name,))
        finally:
            self._disconnect(force=True)

    def close(self) -> None:
        """Close the current connection, if any."""
        self._disconnect(force=True)

    def read_worker_state(self, consumer_name: str) -> Dict[str, Any]:  # noqa: D401
        """Return the state row for one consumer, creating it if absent.

        The state row stores the last consumed outbox id (``ID_LAST_OUTBOX``)
        and simple health flags. If the row does not exist yet (first run
        after schema initialization), it is inserted with
        ``ID_LAST_OUTBOX = 0`` so the worker starts from the oldest pending
        outbox row.

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
                table=SUMMARY_WORKER_STATE_TABLE,
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
                table=SUMMARY_WORKER_STATE_TABLE,
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
                table=SUMMARY_WORKER_STATE_TABLE,
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
        """Record one successful refresh pass in the worker state row.

        Updates ``ID_LAST_OUTBOX`` to the last consumed outbox id from the
        batch. Coalesced scope rows that are re-dirtied during processing are
        republished with a fresh ``ID_OUTBOX`` via `REPLACE INTO`, so the
        monotonic watermark remains safe. Also stamps ``DT_LAST_SUCCESS``,
        resets ``NA_STATUS`` to ``'idle'``, and records the batch size and
        event count for diagnostics.

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
                table=SUMMARY_WORKER_STATE_TABLE,
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
                table=SUMMARY_WORKER_STATE_TABLE,
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
        """Return the next N outbox rows after the stored watermark.

        Reads ``RFFUSION_SUMMARY.SUMMARY_OUTBOX`` using the ``ID_LAST_OUTBOX``
        position stored in the consumer's state row. Rows are returned in
        ascending ``ID_OUTBOX`` order so the engine always processes the
        oldest pending dirty scopes first.

        Args:
            consumer_name: Consumer identifier used to look up the watermark.
            batch_size:    Maximum number of rows to return (``LIMIT`` clause).

        Returns:
            List of row dicts. Each dict has at minimum:
            ``ID_OUTBOX`` (int), ``NA_SCOPE_TYPE``, ``NA_SCOPE_VALUE``,
            ``NA_SOURCE_HANDLER``, ``NA_REASON``, and ``DT_CREATED_AT``.
            Returns an empty list when no new rows are available.
        """
        state = self.read_worker_state(consumer_name)
        last_outbox_id = int(state.get("ID_LAST_OUTBOX") or 0)

        self._connect()
        try:
            self._assert_summary_outbox_schema()
            rows = self._select_raw(
                f"""
                SELECT
                    ID_OUTBOX,
                    NA_SCOPE_TYPE,
                    NA_SCOPE_VALUE,
                    NA_SOURCE_HANDLER,
                    NA_REASON,
                    DT_CREATED_AT
                FROM {SUMMARY_OUTBOX_TABLE}
                WHERE ID_OUTBOX > %s
                ORDER BY ID_OUTBOX ASC
                LIMIT %s
                """,
                (last_outbox_id, int(batch_size)),
            )
        finally:
            self._disconnect()

        return rows

    def drain_consumed_outbox(self, consumer_name: str) -> int:
        """Delete all outbox rows that have already been durably checkpointed.

        Implements true queue semantics: once a batch is committed via
        :meth:`mark_worker_success`, the consumed rows are removed immediately
        so ``RFFUSION_SUMMARY.SUMMARY_OUTBOX`` does not grow indefinitely.

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
                f"DELETE FROM {SUMMARY_OUTBOX_TABLE} WHERE ID_OUTBOX <= %s",
                (last_outbox_id,),
                commit=True,
            )
        finally:
            self._disconnect()

    def reset_after_reconcile(self, consumer_name: str) -> None:
        """Clean up both queue tables after a successful full reconcile.

        After :meth:`~summary_handler.refresh_engine.SummaryRefreshEngine.refresh_all`
        completes, the summary database is ground truth.  Accumulated
        ``SUMMARY_OUTBOX`` rows are irrelevant (already reflected by the
        reconcile) and the outbox position must be reset to 0 so the next
        incremental cycle starts from the beginning of any new events.

        What this method does:

        * ``RFFUSION_SUMMARY.SUMMARY_OUTBOX`` — **all** rows deleted.
        * ``RFFUSION_SUMMARY.SUMMARY_WORKER_STATE`` — ``ID_LAST_OUTBOX`` reset to 0,
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
                f"DELETE FROM {SUMMARY_OUTBOX_TABLE}",
                (),
                commit=False,
            )
            # Reset the checkpoint to 0 and stamp DT_LAST_SUCCESS so the
            # startup heuristic can determine how fresh the summary is.
            # The row is upserted rather than deleted so DT_LAST_SUCCESS
            # survives across restarts.
            now = datetime.utcnow()
            self._execute_custom(
                f"""
                INSERT INTO {SUMMARY_WORKER_STATE_TABLE}
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

    def _prune_summary_refresh_log(self, *, max_rows: int) -> int:
        """Keep only the newest refresh-log rows within one rolling window."""
        keep_rows = max(1, int(max_rows))

        self._connect()
        try:
            cutoff_rows = self._select_raw(
                """
                SELECT ID_REFRESH_LOG
                FROM SUMMARY_REFRESH_LOG
                ORDER BY ID_REFRESH_LOG DESC
                LIMIT 1 OFFSET %s
                """,
                (keep_rows - 1,),
            )
            if not cutoff_rows:
                return 0

            cutoff_id = int(cutoff_rows[0]["ID_REFRESH_LOG"])
            deleted_rows = self._execute_custom(
                "DELETE FROM SUMMARY_REFRESH_LOG WHERE ID_REFRESH_LOG < %s",
                (cutoff_id,),
                commit=True,
            )
            return int(deleted_rows or 0)
        finally:
            self._disconnect()

    def summary_refresh_success(
        self,
        object_name: str,
        *,
        started_at: datetime,
        row_count: int,
        high_watermark: Optional[str] = None,
    ) -> None:
        """Record a successful summary-object rebuild.

        Inserts one row in ``SUMMARY_REFRESH_LOG`` with full timing and
        metadata for a bounded rolling audit trail, then prunes older rows
        beyond the configured rolling window.

        Args:
            object_name:     Summary table name being refreshed.
            started_at:      UTC timestamp captured by the refresh engine.
            row_count:       Number of rows written by the refresh step.
            high_watermark:  Optional short diagnostic string stored for
                             monitoring (e.g. ``'hosts=12;month=2026-05'``).
        """
        finished_at = datetime.utcnow()
        self._connect()
        try:
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
                commit=True,
                log_success=False,
            )
            try:
                self._prune_summary_refresh_log(
                    max_rows=k.SUMMARY_REFRESH_LOG_MAX_ROWS
                )
            except Exception as exc:
                self._log_db_warning(
                    "summary_refresh_log_prune_failed",
                    operation="summary_refresh_success",
                    max_rows=k.SUMMARY_REFRESH_LOG_MAX_ROWS,
                    error=repr(exc),
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

        Inserts one row in ``SUMMARY_REFRESH_LOG`` so the failure stays in the
        rolling audit window. Mirrors the structure of
        :meth:`summary_refresh_success` so monitoring queries can use the same
        columns against both outcomes.

        Args:
            object_name:   Summary table name being refreshed.
            started_at:    UTC timestamp captured by the refresh engine.
            error_message: String representation of the caught exception.
        """
        finished_at = datetime.utcnow()
        self._connect()
        try:
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
                commit=True,
                log_success=False,
            )
            try:
                self._prune_summary_refresh_log(
                    max_rows=k.SUMMARY_REFRESH_LOG_MAX_ROWS
                )
            except Exception as exc:
                self._log_db_warning(
                    "summary_refresh_log_prune_failed",
                    operation="summary_refresh_failure",
                    max_rows=k.SUMMARY_REFRESH_LOG_MAX_ROWS,
                    error=repr(exc),
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
        subsequent call.  However, later schema changes to the live table do
        not automatically propagate to an already-existing shadow table.  That
        drift is treated as a deployment/configuration error and this method
        fails fast with a diagnostic message rather than attempting to repair
        the shadow schema at runtime.

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

            # Validate that the staging table still matches the live table.
            # ALTER TABLE on `{table}` does not propagate to an existing
            # `{table}_shadow`. If they drift apart, that is a rollout/migration
            # error and must fail loudly instead of being auto-healed here.
            live_cols = self._select_raw(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (self.database, table),
            )
            shadow_cols = self._select_raw(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (self.database, shadow),
            )

            live_col_names = [row["COLUMN_NAME"] for row in live_cols]
            shadow_col_names = [row["COLUMN_NAME"] for row in shadow_cols]

            if live_col_names != shadow_col_names:
                missing_in_shadow = [
                    col for col in live_col_names if col not in shadow_col_names
                ]
                extra_in_shadow = [
                    col for col in shadow_col_names if col not in live_col_names
                ]
                message = (
                    f"Shadow table schema drift detected: {shadow} != {table}. "
                    f"missing_in_shadow={missing_in_shadow} "
                    f"extra_in_shadow={extra_in_shadow} "
                    f"live_columns={live_col_names} "
                    f"shadow_columns={shadow_col_names}"
                )
                self._log_db_error(
                    "db_schema_drift_detected",
                    operation="replace_table_rows",
                    table=table,
                    shadow_table=shadow,
                    error=message,
                )
                raise RuntimeError(message)

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
