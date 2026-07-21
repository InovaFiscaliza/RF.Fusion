"""
Validation tests for `dbHandlerSummary.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/db/test_dbhandler_summary.py -q

What is covered here:
    - `SUMMARY_REFRESH_LOG` pruning keeps only the newest rolling window
    - pruning is skipped when the log is already below the configured limit
    - refresh success does not fail when post-commit log pruning raises
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import DB_ROOT, bind_real_package, bind_real_shared_package, ensure_app_paths, import_package_module


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        db_base_module = import_package_module("db", DB_ROOT, "dbHandlerBase")
        db_summary_module = import_package_module("db", DB_ROOT, "dbHandlerSummary")


class FakeLog:
    """Collect structured log output emitted by the summary handler."""

    def __init__(self) -> None:
        self.entries: list[tuple[str, dict]] = []
        self.warnings: list[tuple[str, dict]] = []
        self.errors: list[tuple[str, dict]] = []

    def event(self, event: str, **fields) -> None:
        self.entries.append((event, fields))

    def warning_event(self, event: str, **fields) -> None:
        self.warnings.append((event, fields))

    def error_event(self, event: str, **fields) -> None:
        self.errors.append((event, fields))

    def entry(self, message: str) -> None:
        self.entries.append(("entry", {"message": message}))

    def warning(self, message: str) -> None:
        self.warnings.append(("warning", {"message": message}))

    def error(self, message: str) -> None:
        self.errors.append(("error", {"message": message}))


class SummaryRefreshLogRetentionTests(unittest.TestCase):
    """Validate bounded retention for `SUMMARY_REFRESH_LOG`."""

    def make_handler(self):
        handler = object.__new__(db_summary_module.dbHandlerSummary)
        handler.log = FakeLog()
        handler.database = "RFFUSION_SUMMARY_TEST"
        handler.reuse_connection = True
        handler.in_transaction = False
        handler.db_connection = None
        handler.cursor = None
        handler._connect = lambda: None
        handler._disconnect = lambda *args, **kwargs: None
        return handler

    def test_prune_summary_refresh_log_deletes_rows_older_than_cutoff(self) -> None:
        """Prune should delete rows older than the 100th newest entry."""

        handler = self.make_handler()
        executed = []

        handler._select_raw = lambda sql, params: [{"ID_REFRESH_LOG": 901}]
        handler._execute_custom = lambda sql, params, commit=True: executed.append(
            (sql.strip(), params, commit)
        ) or 800

        deleted_rows = handler._prune_summary_refresh_log(max_rows=100)

        self.assertEqual(deleted_rows, 800)
        self.assertEqual(
            executed,
            [
                (
                    "DELETE FROM SUMMARY_REFRESH_LOG WHERE ID_REFRESH_LOG < %s",
                    (901,),
                    True,
                )
            ],
        )

    def test_prune_summary_refresh_log_skips_delete_when_log_is_small(self) -> None:
        """Prune should no-op when fewer than `max_rows` entries exist."""

        handler = self.make_handler()
        execute_calls = []

        handler._select_raw = lambda sql, params: []
        handler._execute_custom = lambda *args, **kwargs: execute_calls.append((args, kwargs))

        deleted_rows = handler._prune_summary_refresh_log(max_rows=100)

        self.assertEqual(deleted_rows, 0)
        self.assertEqual(execute_calls, [])

    def test_summary_refresh_success_logs_warning_when_prune_fails(self) -> None:
        """Post-commit prune failure must not turn a refresh success into failure."""

        handler = self.make_handler()
        log_inserts = []

        handler._insert_row = lambda **kwargs: log_inserts.append(kwargs) or 1

        def raise_prune(*, max_rows: int) -> int:
            raise RuntimeError(f"prune failed for {max_rows}")

        handler._prune_summary_refresh_log = raise_prune

        handler.summary_refresh_success(
            "HOST_CURRENT_SNAPSHOT",
            started_at=datetime(2026, 6, 11, 0, 0, 0),
            row_count=12,
            high_watermark="hosts=12",
        )

        self.assertEqual(len(log_inserts), 1)
        self.assertEqual(len(handler.log.warnings), 1)
        event, fields = handler.log.warnings[0]
        self.assertEqual(event, "summary_refresh_log_prune_failed")
        self.assertEqual(fields["operation"], "summary_refresh_success")
        self.assertEqual(fields["max_rows"], 100)


class SummaryOutboxShapeTests(unittest.TestCase):
    """Validate the coalesced dirty-scope outbox contract."""

    def make_base_handler(self):
        handler = object.__new__(db_base_module.DBHandlerBase)
        handler.log = FakeLog()
        handler.database = "BPDATA"
        handler.reuse_connection = True
        handler.db_connection = None
        handler.cursor = None
        handler.in_transaction = False
        handler._connect = lambda: None
        handler._disconnect = lambda *args, **kwargs: None
        return handler

    def make_summary_handler(self):
        handler = object.__new__(db_summary_module.dbHandlerSummary)
        handler.log = FakeLog()
        handler.database = "RFFUSION_SUMMARY"
        handler.reuse_connection = True
        handler.db_connection = None
        handler.cursor = None
        handler.in_transaction = False
        handler._connect = lambda: None
        handler._disconnect = lambda *args, **kwargs: None
        return handler

    def test_summary_enqueue_refresh_replaces_one_row_per_dirty_scope(self) -> None:
        """Scope mode should publish one coalesced row per dirty key."""

        handler = self.make_base_handler()
        executed = {}
        handler._assert_summary_outbox_schema = lambda: None

        handler._execute_many_custom = lambda sql, values, commit=True: executed.update(
            {
                "sql": sql.strip(),
                "values": values,
                "commit": commit,
            }
        ) or len(values)

        published = handler.summary_enqueue_refresh(
            host_ids=[7, 7],
            reference_months=["2026-06-11"],
            reason="file_history_update",
            source_handler="dbHandlerBKP",
        )

        self.assertEqual(published, 2)
        self.assertIn("REPLACE INTO", executed["sql"])
        self.assertEqual(
            executed["values"],
            [
                (
                    db_base_module.k.SUMMARY_SCOPE_HOST,
                    "7",
                    "dbHandlerBKP",
                    "file_history_update",
                ),
                (
                    db_base_module.k.SUMMARY_SCOPE_REFERENCE_MONTH,
                    "2026-06-01",
                    "dbHandlerBKP",
                    "file_history_update",
                ),
            ],
        )
        self.assertTrue(executed["commit"])

    def test_read_outbox_batch_returns_canonical_scope_rows(self) -> None:
        """Outbox reads should return the canonical scope-row contract."""

        handler = self.make_summary_handler()
        handler._assert_summary_outbox_schema = lambda: None
        rows_result = [
            {
                "ID_OUTBOX": 51,
                "NA_SCOPE_TYPE": db_summary_module.k.SUMMARY_SCOPE_HOST,
                "NA_SCOPE_VALUE": "17",
                "NA_SOURCE_HANDLER": "dbHandlerBKP",
                "NA_REASON": "host_update",
                "DT_CREATED_AT": datetime(2026, 6, 11, 12, 0, 0),
            },
            {
                "ID_OUTBOX": 52,
                "NA_SCOPE_TYPE": db_summary_module.k.SUMMARY_SCOPE_FULL_RECONCILE,
                "NA_SCOPE_VALUE": db_summary_module.k.SUMMARY_SCOPE_FULL_RECONCILE_KEY,
                "NA_SOURCE_HANDLER": "dbHandlerRFM",
                "NA_REASON": "bulk_import",
                "DT_CREATED_AT": datetime(2026, 6, 11, 12, 1, 0),
            },
        ]

        handler.read_worker_state = lambda consumer_name: {"ID_LAST_OUTBOX": 50}
        handler._select_raw = lambda sql, params: rows_result

        rows = handler.read_outbox_batch("summary_consumer", batch_size=10)

        self.assertEqual(len(rows), 2)
        self.assertEqual(
            rows[0],
            {
                "ID_OUTBOX": 51,
                "NA_SCOPE_TYPE": db_summary_module.k.SUMMARY_SCOPE_HOST,
                "NA_SCOPE_VALUE": "17",
                "NA_SOURCE_HANDLER": "dbHandlerBKP",
                "NA_REASON": "host_update",
                "DT_CREATED_AT": datetime(2026, 6, 11, 12, 0, 0),
            },
        )
        self.assertEqual(
            rows[1],
            {
                "ID_OUTBOX": 52,
                "NA_SCOPE_TYPE": db_summary_module.k.SUMMARY_SCOPE_FULL_RECONCILE,
                "NA_SCOPE_VALUE": db_summary_module.k.SUMMARY_SCOPE_FULL_RECONCILE_KEY,
                "NA_SOURCE_HANDLER": "dbHandlerRFM",
                "NA_REASON": "bulk_import",
                "DT_CREATED_AT": datetime(2026, 6, 11, 12, 1, 0),
            },
        )

    def test_validate_summary_outbox_schema_raises_on_legacy_shape(self) -> None:
        """Schema validation should point operators to the required migration."""

        handler = self.make_summary_handler()
        query_results = [
            [
                {"COLUMN_NAME": "ID_OUTBOX"},
                {"COLUMN_NAME": "NA_SOURCE_HANDLER"},
                {"COLUMN_NAME": "DT_CREATED_AT"},
            ],
            [],
        ]

        handler._select_raw = lambda sql, params: query_results.pop(0)

        with self.assertRaisesRegex(
            RuntimeError,
            "createFusionSummaryDB.sql",
        ):
            handler._assert_summary_outbox_schema()


class BaseCustomExecutionTransactionTests(unittest.TestCase):
    """Validate rollback ownership in shared custom SQL helpers."""

    def make_base_handler(self, *, in_transaction: bool):
        handler = object.__new__(db_base_module.DBHandlerBase)
        handler.log = FakeLog()
        handler.database = "BPDATA"
        handler.reuse_connection = True
        handler.in_transaction = in_transaction
        handler._connect = lambda: None
        handler._disconnect = lambda *args, **kwargs: None

        class FakeConnection:
            def __init__(self) -> None:
                self.rollback_calls = 0
                self.commit_calls = 0

            def rollback(self) -> None:
                self.rollback_calls += 1

            def commit(self) -> None:
                self.commit_calls += 1

        class FailingCursor:
            def execute(self, *_args, **_kwargs) -> None:
                raise RuntimeError("statement failed")

            def executemany(self, *_args, **_kwargs) -> None:
                raise RuntimeError("statement failed")

        handler.db_connection = FakeConnection()
        handler.cursor = FailingCursor()
        return handler

    def test_execute_custom_skips_rollback_inside_managed_transaction(self) -> None:
        """Statement failure must not roll back an outer managed transaction."""

        handler = self.make_base_handler(in_transaction=True)

        with self.assertRaises(RuntimeError):
            handler._execute_custom(
                "UPDATE SUMMARY_OUTBOX SET NA_REASON = %s",
                ("host_update",),
                commit=False,
            )

        self.assertEqual(handler.db_connection.rollback_calls, 0)
        self.assertEqual(handler.log.errors[0][0], "db_execute_failed")

    def test_execute_many_custom_skips_rollback_inside_managed_transaction(self) -> None:
        """Batch failure must not roll back an outer managed transaction."""

        handler = self.make_base_handler(in_transaction=True)

        with self.assertRaises(RuntimeError):
            handler._execute_many_custom(
                "REPLACE INTO SUMMARY_OUTBOX VALUES (%s, %s, %s, %s)",
                [("host", "7", "dbHandlerBKP", "file_task_delete")],
                commit=False,
            )

        self.assertEqual(handler.db_connection.rollback_calls, 0)
        self.assertEqual(handler.log.errors[0][0], "db_executemany_failed")


if __name__ == "__main__":
    unittest.main()
