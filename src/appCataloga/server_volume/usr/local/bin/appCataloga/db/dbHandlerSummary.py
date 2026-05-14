#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Summary-domain database handler for the incremental RFFUSION_SUMMARY worker.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

import config as k
from .dbHandlerBase import DBHandlerBase


class dbHandlerSummary(DBHandlerBase):
    """DB handler for the Python-owned `RFFUSION_SUMMARY` refresh path."""

    def __init__(
        self,
        database: str,
        log: Any,
        reuse_connection: bool = True,
    ) -> None:
        """Bind one reusable connection wrapper for summary worker duties."""
        super().__init__(
            database=database,
            log=log,
            reuse_connection=reuse_connection,
        )
        self.log.entry(f"[dbHandlerSummary] Initialized for DB '{database}'")
        self.in_transaction: bool = False

    def begin_transaction(self) -> None:
        """Open one managed transaction for grouped summary writes."""
        self.in_transaction = True
        self._connect()
        self.db_connection.autocommit = False

    def commit(self) -> None:
        """Commit the active managed transaction, if one exists."""
        if not self.in_transaction:
            return

        try:
            self.db_connection.commit()
        finally:
            self.db_connection.autocommit = True
            self.in_transaction = False

    def rollback(self) -> None:
        """Rollback the active managed transaction, if one exists."""
        if not self.in_transaction:
            return

        try:
            self.db_connection.rollback()
        finally:
            self.db_connection.autocommit = True
            self.in_transaction = False

    def configure_worker_session(self) -> None:
        """Apply the low-lock session policy used by the summary worker."""
        self._connect()
        try:
            self.cursor.execute(
                "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED"
            )
            self.cursor.execute("SET SESSION innodb_lock_wait_timeout = 5")
        finally:
            self._disconnect()

    def acquire_worker_lock(self, lock_name: str) -> bool:
        """Claim the singleton DB lock that protects the summary worker."""
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
        """Release the singleton DB lock used by the summary worker."""
        self._connect()
        try:
            self._select_raw("SELECT RELEASE_LOCK(%s) AS LOCK_RELEASED", (lock_name,))
        finally:
            self._disconnect()

    def disable_sql_event(self, event_name: str) -> None:
        """Disable the legacy MariaDB event scheduler refresh path."""
        self._connect()
        try:
            self._execute_custom(
                f"ALTER EVENT `{event_name}` DISABLE",
                commit=True,
            )
        finally:
            self._disconnect()

    def read_worker_state(self, consumer_name: str) -> Dict[str, Any]:
        """Read or lazily create the checkpoint row for one consumer."""
        self._connect()
        try:
            rows = self._select_rows(
                table="BPDATA.SUMMARY_WORKER_STATE",
                where={"NA_CONSUMER": consumer_name},
                limit=1,
            )
            if rows:
                return rows[0]

            # The worker self-bootstrap keeps deployment simple after schema init.
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
        """Persist the start heartbeat for one polling pass."""
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
        """Advance the outbox checkpoint after a successful refresh pass."""
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
        """Persist one failure heartbeat without advancing the checkpoint."""
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
        """Read the next append-only outbox batch after the stored checkpoint."""
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
        parsed_rows: List[Dict[str, Any]] = []
        for row in rows:
            payload = row.get("JS_PAYLOAD") or "{}"
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8", errors="replace")
            try:
                payload_dict = json.loads(payload)
            except Exception:
                payload_dict = {}

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
        """Delete old outbox rows that are already behind the checkpoint."""
        state = self.read_worker_state(consumer_name)
        last_outbox_id = int(state.get("ID_LAST_OUTBOX") or 0)
        if last_outbox_id <= 0:
            return 0

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

    def summary_refresh_start(self, object_name: str) -> datetime:
        """Mark one public summary object refresh as started."""
        started_at = datetime.utcnow()
        self._connect()
        try:
            self._upsert_row(
                table="SUMMARY_REFRESH_STATE",
                data={
                    "NA_OBJECT_NAME": object_name,
                    "DT_LAST_START": started_at,
                    "DT_LAST_END": None,
                    "IS_SUCCESS": 0,
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
        """Persist one successful summary refresh record."""
        finished_at = datetime.utcnow()
        self._connect()
        try:
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
                commit=False,
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
                commit=True,
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
        """Persist one failed summary refresh record."""
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
                commit=False,
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
                commit=True,
                log_success=False,
            )
        finally:
            self._disconnect()

    def replace_table_rows(self, table: str, rows: List[Dict[str, Any]]) -> int:
        """Replace one full-snapshot summary table with caller-provided rows."""
        self._connect()
        try:
            # Some read models are still easier to rebuild as whole snapshots.
            self._execute_custom(f"TRUNCATE TABLE {table}", commit=False)
            if not rows:
                self.db_connection.commit()
                return 0

            columns = list(rows[0].keys())
            values = [
                tuple(row.get(column) for column in columns)
                for row in rows
            ]
            placeholders = ", ".join(["%s"] * len(columns))
            sql = (
                f"INSERT INTO {table} ({', '.join(columns)}) "
                f"VALUES ({placeholders})"
            )
            self._execute_many_custom(sql, values, commit=False)
            self.db_connection.commit()
            return len(rows)
        except Exception:
            self.db_connection.rollback()
            raise
        finally:
            self._disconnect()

    def upsert_rows(
        self,
        *,
        table: str,
        rows: List[Dict[str, Any]],
        unique_keys: List[str],
    ) -> int:
        """UPSERT caller-provided rows into one summary table."""
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
        """Execute one targeted `DELETE` against summary tables."""
        self._connect()
        try:
            return self._execute_custom(sql, tuple(params), commit=True)
        finally:
            self._disconnect()
