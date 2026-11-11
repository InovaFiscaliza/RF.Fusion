"""Verify PostgreSQL summary contracts, binding and atomic publication."""

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import re
import sys
import unittest

import psycopg
from psycopg._queries import PostgresQuery
from psycopg.adapt import Transformer
from psycopg.pq import TransactionStatus

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _support import APP_ROOT, load_test_config

sys.path.insert(0, str(APP_ROOT))
with patch.dict(sys.modules, {"config": load_test_config()}):
    from db import dbHandlerSummary as summary_module

dbHandlerSummary = summary_module.dbHandlerSummary


from db import constants as db_constants


class PostgresSummaryTests(unittest.TestCase):
    """Protect checkpoints, session locks and rollback-sensitive writes."""

    def setUp(self) -> None:
        """Configure a handler and a driver-shaped session.

        Args:
            None.
        Returns:
            None.
        """
        self.handler = dbHandlerSummary(
            'Summary " Test', MagicMock(), conninfo="dbname=test"
        )
        self.k = summary_module.k
        self.connection = MagicMock(spec=psycopg.Connection)
        self.connection.closed = False
        self.connection.broken = False
        self.connection.info = SimpleNamespace(
            transaction_status=TransactionStatus.IDLE
        )
        self.cursor = MagicMock(spec=psycopg.Cursor)
        self.cursor.connection = self.connection
        self.connection.cursor.return_value.__enter__.return_value = self.cursor
        self.cursor.rowcount = 1
        self.cursor.fetchall.return_value = [{"acquired": True, "released": True}]
        self.driver = patch.object(psycopg.Connection, "connect", return_value=self.connection)
        self.driver.start()
        self.addCleanup(self.driver.stop)
        self.addCleanup(self.handler.close)

    def test_schema_has_only_canonical_aggregates_and_restorable_usage(self) -> None:
        """Reject regressions that recreate copied cadastro or global snapshots.

        Args:
            None.
        Returns:
            None.
        """
        ddl_path = (
            Path(__file__).resolve().parents[3]
            / "src/postgres/scripts/createFusionSummaryDB.sql"
        )
        tables = dict(
            re.findall(r'CREATE TABLE "(\w+)" \((.*?)\n\);', ddl_path.read_text(), re.S)
        )
        self.assertEqual(
            set(tables),
            {
                "HOST_EQUIPMENT_LINK",
                "SITE_EQUIPMENT_METRIC",
                "HOST_LOCATION_VISIT",
                "HOST_MONTHLY_METRIC",
                "HOST_METRIC",
                "HOST_ERROR_METRIC",
                "SUMMARY_OUTBOX",
                "SUMMARY_WORKER_STATE",
                "SUMMARY_REFRESH_LOG",
                "WEBFUSION_USAGE_LOG_CHECKPOINT",
                "WEBFUSION_USAGE_METRIC_MONTHLY",
            },
        )
        for name, body in tables.items():
            if not name.startswith("WEBFUSION_USAGE"):
                self.assertNotRegex(
                    body,
                    r'"(?:NA_HOST_NAME|NA_EQUIPMENT|VL_LATITUDE|VL_LONGITUDE|IS_BUSY|IS_OFFLINE|IS_SSH_FAILURE)"',
                )
        self.assertNotIn("CREATE VIEW", ddl_path.read_text())

    def test_snapshot_failure_rolls_back_and_preserves_external_transaction(
        self,
    ) -> None:
        """A failed bulk insert must not expose a partially replaced table.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.executemany.side_effect = psycopg.DataError("bad row")
        with self.assertRaises(psycopg.DataError):
            self.handler.site_equipment_replace(
                [{"FK_SITE": 1, "FK_EQUIPMENT": 2, "NU_SPECTRUM_COUNT": 1}]
            )
        self.connection.rollback.assert_called_once()
        self.connection.commit.assert_not_called()
        self.connection.rollback.reset_mock()
        self.handler.begin_transaction()
        with self.assertRaises(psycopg.DataError):
            self.handler.site_equipment_replace(
                [{"FK_SITE": 1, "FK_EQUIPMENT": 2, "NU_SPECTRUM_COUNT": 1}]
            )
        self.connection.rollback.assert_not_called()
        self.assertTrue(self.handler.in_transaction)

    def test_entity_api_checks_scope_and_propagates_sql_column_errors(self) -> None:
        """Only entity operations can write their exact canonical scope.

        Args:
            None.
        Returns:
            None.
        """
        for obsolete in (
            "replace_table_rows",
            "upsert_rows",
            "delete_rows",
            "execute_delete",
            "update_host_current_snapshot_signals",
        ):
            self.assertFalse(hasattr(self.handler, obsolete))
        with self.assertRaises(ValueError):
            self.handler.host_visit_replace(7, [{"FK_HOST": 8}])
        with self.assertRaises(ValueError):
            self.handler.site_equipment_replace(
                [{"FK_EQUIPMENT": 8}], equipment_ids=[7]
            )
        error = psycopg.errors.UndefinedColumn("source-only column")
        self.cursor.executemany.side_effect = error
        with self.assertRaises(psycopg.errors.UndefinedColumn) as raised:
            self.handler.site_equipment_replace(
                [{"FK_EQUIPMENT": 7, "NA_EQUIPMENT": "copied"}], equipment_ids=[7]
            )
        self.assertIs(raised.exception, error)

    def test_session_lock_contention_and_release_use_the_same_key(self) -> None:
        """Contention returns False and release never changes the lock identity.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [{"acquired": False}]
        self.assertFalse(self.handler.worker_lock_acquire("singleton"))
        key = self.cursor.execute.call_args.args[1]
        self.cursor.fetchall.return_value = [{"released": True}]
        self.handler.worker_lock_release("singleton")
        self.assertEqual(key, self.cursor.execute.call_args.args[1])
        self.connection.close.assert_called_once()
        self.handler.reuse_connection = False
        with self.assertRaises(RuntimeError):
            self.handler.worker_lock_acquire("singleton")

    def test_empty_scope_cannot_delete_or_publish_global_rows(self) -> None:
        """Empty equipment selection remains an explicit false predicate.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.site_equipment_replace([], equipment_ids=[])
        queries = [
            call.args[0].as_string() for call in self.cursor.execute.call_args_list
        ]
        deletion = next(query for query in queries if query.startswith("DELETE"))
        self.assertIn("WHERE FALSE", deletion)
        self.cursor.executemany.assert_not_called()

    def test_checkpoint_failure_does_not_advance_or_delete_events(self) -> None:
        """Failure metadata excludes the checkpoint and successful drain is bounded.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.worker_state_failure("worker", error_message="sanitized")
        query, params = self.cursor.execute.call_args.args
        self.assertNotIn('"ID_LAST_OUTBOX"', query.as_string())
        self.assertIn("sanitized", params)
        with patch.object(
            self.handler, "worker_state_read", return_value={"ID_LAST_OUTBOX": 18}
        ):
            self.handler.outbox_delete_consumed("worker")
        query, params = self.cursor.execute.call_args.args
        self.assertIn('"ID_OUTBOX" <= %s', query.as_string())
        self.assertEqual(params, (18,))

    def _domain_calls(self) -> list[tuple[str, object]]:
        """Return executed statements, skipping the savepoint opener.

        Args:
            None.
        Returns:
            list[tuple[str, object]]: Rendered SQL and its bound parameters.
        """
        return [
            (call.args[0].as_string(), call.args[1] if len(call.args) > 1 else ())
            for call in self.cursor.execute.call_args_list
            if call.args[0].as_string() != "SELECT 1"
        ]

    def test_error_sources_read_each_table_through_crud_and_map_in_python(
        self,
    ) -> None:
        """Map phase, held state and timestamp fallback without a SQL UNION.

        Args:
            None.
        Returns:
            None.
        """
        k = self.k
        created = datetime(2025, 1, 2)
        self.cursor.fetchall.side_effect = [
            [],
            [],
            [],
            [
                {
                    "ID_FILE_TASK": 4,
                    "FK_HOST": 8,
                    "NU_TYPE": db_constants['tasks']['file_types']['processing'],
                    "NU_STATUS": db_constants['tasks']['states']['frozen'],
                    "DT_FILE_TASK": None,
                    "DT_FILE_CREATED_HOST": created,
                    "DT_FILE_MODIFIED_HOST": None,
                    "NA_ERROR_CODE": "E1",
                    "NA_MESSAGE": "msg",
                    "NA_ERROR_DETAIL": None,
                }
            ],
            [
                {
                    "ID_HOST_TASK": 5,
                    "FK_HOST": 8,
                    "NU_STATUS": db_constants['tasks']['states']['error'],
                    "DT_HOST_TASK": None,
                    "NA_MESSAGE": "down",
                }
            ],
        ]
        queue, host = self.handler.host_error_list_sources(host_ids=[8])
        self.assertEqual(queue["NA_ERROR_SCOPE"], "PROCESSING_QUEUE")
        self.assertEqual(queue["NA_TASK_STATE"], db_constants['summary']['task_states']['frozen'])
        self.assertEqual(queue["DT_EVENT_AT"], created)
        self.assertEqual(queue["NA_RAW_MESSAGE"], "msg")
        self.assertEqual(host["NA_ERROR_SCOPE"], "HOST_TASK")
        self.assertIsNone(host["NA_ERROR_CODE"])
        statements = [text for text, _ in self._domain_calls()]
        self.assertEqual(len(statements), 5)
        self.assertTrue(all("UNION" not in text for text in statements))
        self.assertIn('"BPDATA"."HOST_TASK"', statements[-1])
        self.assertNotIn('"NU_TYPE"', statements[-1])

    def test_source_tables_use_their_owning_schema(self) -> None:
        """Qualify source reads with their BPDATA/RFDATA owners.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = []
        sources = self.handler.host_equipment_list_sources()
        self.assertEqual(sources, {"hosts": [], "equipment": []})
        statements = [text for text, _ in self._domain_calls()]
        self.assertEqual(len(statements), 2)
        self.assertIn('FROM "BPDATA"."HOST"', statements[0])
        self.assertIn('FROM "RFDATA"."DIM_SPECTRUM_EQUIPMENT"', statements[1])

    def test_affected_hosts_and_equipment_use_distinct_crud_reads(self) -> None:
        """Expand dirty sites through equipment links without SQL unions.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.side_effect = [
            [{"FK_EQUIPMENT": 3}],
            [{"FK_EQUIPMENT": 4}],
        ]
        self.assertEqual(
            self.handler.site_equipment_list_affected(site_ids=[1], equipment_ids=[9]),
            [3, 4, 9],
        )
        statements = [text for text, _ in self._domain_calls()]
        self.assertTrue(all(text.startswith("SELECT DISTINCT") for text in statements))
        self.assertIn('"RFDATA"."FACT_SPECTRUM"', statements[0])
        self.cursor.execute.reset_mock()
        self.cursor.fetchall.side_effect = [
            [{"FK_EQUIPMENT": 3}],
            [{"FK_HOST": 7}],
            [{"FK_HOST": 6}],
        ]
        self.assertEqual(
            self.handler.host_visit_list_hosts(host_ids={2}, site_ids={1}),
            [2, 6, 7],
        )
        _, link_params = self._domain_calls()[-1]
        self.assertEqual(link_params, (3,))

    def test_full_rebuild_and_watermark_and_prune_use_crud(self) -> None:
        """Rebuild without a filter only through the explicit full-delete path.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.host_equipment_replace([])
        statements = [text for text, _ in self._domain_calls()]
        self.assertIn('DELETE FROM "Summary "" Test"."HOST_EQUIPMENT_LINK"', statements)
        self.cursor.execute.reset_mock()
        self.cursor.fetchall.side_effect = None
        self.cursor.fetchall.return_value = []
        self.assertEqual(self.handler.outbox_read_watermark(), 0)
        self.cursor.fetchall.return_value = [{"ID_REFRESH_LOG": 30}, {"ID_REFRESH_LOG": 29}]
        self.assertEqual(self.handler._refresh_log_prune(max_rows=3), 0)
        self.handler._refresh_log_prune(max_rows=2)
        query, params = self.cursor.execute.call_args.args
        self.assertIn('"ID_REFRESH_LOG" < %s', query.as_string())
        self.assertEqual(params, (29,))

    def test_host_metrics_aggregate_queue_and_history_then_merge(self) -> None:
        """Filter each count/sum in one scan per table and default absent hosts.

        Args:
            None.
        Returns:
            None.
        """
        k = self.k
        self.cursor.fetchall.side_effect = [
            [{"FK_HOST": 8, "NU_BACKUP_QUEUE_FILES_TOTAL": 2, "KB_BACKUP_QUEUE_GB_TOTAL": Decimal(1572864)}],
            [{"FK_HOST": 8, "NU_PAYLOAD_DELETED_FILES_TOTAL": 2, "KB_PAYLOAD_SERVER": Decimal(1048576), "KB_PAYLOAD_HOST": None}],
            [{"ID_HOST": 8}, {"ID_HOST": 9}],
        ]
        rows = self.handler.host_metric_list_sources(
            current_month_start=datetime(2026, 11, 1),
            next_month_start=datetime(2026, 12, 1),
            host_ids=[8, 9],
        )
        first, second = rows
        self.assertEqual(first["NU_BACKUP_QUEUE_FILES_TOTAL"], 2)
        self.assertEqual(first["VL_BACKUP_QUEUE_GB_TOTAL"], Decimal("1.50"))
        self.assertEqual(first["VL_PAYLOAD_DELETED_GB_TOTAL"], Decimal("1.00"))
        self.assertEqual(first["DT_REFERENCE_MONTH"], date(2026, 11, 1))
        self.assertEqual(second["NU_DISCOVERED_FILES_TOTAL"], 0)
        self.assertEqual(second["VL_DISCOVERED_GB_TOTAL"], Decimal("0.00"))
        self.assertEqual(set(first) - {"FK_HOST", "DT_REFERENCE_MONTH"}, set(self.handler._host_metric_value_fields(*self.handler._host_metric_aggregates(datetime(2025, 2, 1), datetime(2025, 3, 1)))))
        queue, history, hosts = self.cursor.execute.call_args_list
        query, params = queue.args
        PostgresQuery(Transformer()).convert(query, params)
        rendered = query.as_string()
        self.assertIn('COUNT(*) FILTER (WHERE TRUE AND "NU_TYPE" = %s AND "NU_STATUS" = %s)', rendered)
        self.assertIn('GROUP BY "FK_HOST"', rendered)
        self.assertEqual(params[-2:], (8, 9))
        query, params = history.args
        PostgresQuery(Transformer()).convert(query, params)
        self.assertIn('"DT_BACKUP" >= %s AND "DT_BACKUP" < %s', query.as_string())
        self.assertIn(datetime(2026, 12, 1), params)
        self.assertIn('"VL_FILE_SIZE_KB_SERVER" IS NULL', query.as_string())
        self.assertIn('"BPDATA"."FILE_TASK_HISTORY"', query.as_string())
        self.assertIn(db_constants['tasks']['states']['done'], params)

    def test_monthly_sources_group_by_month_inside_one_index_range(self) -> None:
        """Bucket creation months and drop unrequested months inside the range.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [
            {"FK_HOST": 8, "DT_REFERENCE_MONTH": date(2025, 11, 1), "NU_DISCOVERED_FILES": 1, "KB_DISCOVERED_GB": 0},
            {"FK_HOST": 8, "DT_REFERENCE_MONTH": date(2025, 12, 1), "NU_DISCOVERED_FILES": 3, "KB_DISCOVERED_GB": Decimal(524288)},
        ]
        rows = self.handler.host_monthly_list_sources(
            host_ids=[8], reference_months=[date(2025, 10, 1), date(2025, 12, 1)]
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["VL_DISCOVERED_GB"], Decimal("0.50"))
        self.assertNotIn("KB_DISCOVERED_GB", rows[0])
        query, params = self.cursor.execute.call_args.args
        PostgresQuery(Transformer()).convert(query, params)
        rendered = query.as_string()
        self.assertIn("date_trunc('month', \"DT_FILE_CREATED_HOST\")::date AS \"DT_REFERENCE_MONTH\"", rendered)
        self.assertIn('"DT_FILE_CREATED_HOST" IS NOT NULL', rendered)
        self.assertEqual(params[-2:], (date(2025, 10, 1), date(2026, 1, 1)))
        self.cursor.execute.reset_mock()
        self.assertEqual(self.handler.host_monthly_list_sources(host_ids=[]), [])
        self.cursor.execute.assert_not_called()

    def test_operational_read_uses_crud_and_decodes_current_site(self) -> None:
        """Combine host, metric and current site without projecting credentials.

        Args:
            None.
        Returns:
            None.
        """
        point = "0101000020E6100000000000000000F03F0000000000000040"
        self.cursor.fetchall.side_effect = [
            [{"ID_HOST": 8, "NA_HOST_NAME": "rfeye"}],
            [{"DT_REFERENCE_MONTH": date(2000, 1, 1), "NU_BACKUP_DONE_THIS_MONTH": 4, "NU_DISCOVERED_FILES_TOTAL": 7, "DT_REFRESHED_AT": None}],
            [
                {"FK_SITE": 1, "NU_SPECTRUM_COUNT": 2, "DT_FIRST_SEEN_AT": None, "DT_LAST_SEEN_AT": None},
                {"FK_SITE": 2, "NU_SPECTRUM_COUNT": 3, "DT_FIRST_SEEN_AT": datetime(2025, 1, 1), "DT_LAST_SEEN_AT": None},
            ],
            [{"NA_SITE": "Site", "FK_STATE": 33, "FK_COUNTY": 3304557, "FK_DISTRICT": None, "GEO_POINT": point}],
            [{"NA_COUNTY": "Rio"}],
            [{"LC_STATE": "RJ"}],
        ]
        result = self.handler.host_read_operational(host_id=8)
        self.assertEqual(result["NU_BACKUP_DONE_THIS_MONTH"], 0)
        self.assertEqual(result["NU_DISCOVERED_FILES_TOTAL"], 7)
        self.assertEqual(result["NU_FACT_SPECTRUM_TOTAL"], 5)
        self.assertEqual(result["NA_CURRENT_LOCALITY_LABEL"], "Rio")
        self.assertEqual(result["NA_CURRENT_STATE_CODE"], "RJ")
        self.assertEqual((result["VL_CURRENT_LONGITUDE"], result["VL_CURRENT_LATITUDE"]), (1.0, 2.0))
        statements = [call.args[0].as_string() for call in self.cursor.execute.call_args_list]
        self.assertTrue(all("PASSWORD" not in text for text in statements))
        self.assertEqual(statements[3].count('"ID_SITE" = %s'), 1)
        self.assertEqual(self.cursor.execute.call_args_list[3].args[1], (2,))
        self.cursor.fetchall.side_effect = [[]]
        self.assertEqual(self.handler.host_read_operational(host_id=8), {})

    def test_visits_split_runs_of_the_same_site_under_a_local_timeout(self) -> None:
        """Group ordered spectra into visits and restore the caller timeout.

        Args:
            None.
        Returns:
            None.
        """
        t = datetime(2025, 1, 1)
        self.cursor.fetchall.side_effect = [
            [{"value": "2min"}],
            [{}],
            [
                {"ID_SPECTRUM": 1, "FK_SITE": 5, "DT_TIME_START": t, "DT_TIME_END": None},
                {"ID_SPECTRUM": 2, "FK_SITE": 5, "DT_TIME_START": t.replace(hour=1), "DT_TIME_END": t.replace(hour=3)},
                {"ID_SPECTRUM": 3, "FK_SITE": 6, "DT_TIME_START": t.replace(hour=2), "DT_TIME_END": t},
                {"ID_SPECTRUM": 4, "FK_SITE": 5, "DT_TIME_START": t.replace(hour=4), "DT_TIME_END": None},
            ],
            [{}],
        ]
        visits = self.handler.host_visit_list_sources(9)
        self.assertEqual([v["FK_SITE"] for v in visits], [5, 6, 5])
        self.assertEqual([v["NU_VISIT"] for v in visits], [1, 2, 3])
        self.assertEqual(visits[0]["NU_SPECTRUM_COUNT"], 2)
        self.assertEqual(visits[0]["DT_LAST_SEEN_AT"], t.replace(hour=3))
        self.assertEqual(visits[1]["DT_LAST_SEEN_AT"], t.replace(hour=2))
        calls = self.cursor.execute.call_args_list
        self.assertEqual(calls[1].args[1], ("statement_timeout", "30s", True))
        query, params = calls[2].args
        rendered = query.as_string()
        self.assertIn('JOIN "Summary "" Test"."HOST_EQUIPMENT_LINK" AS "joined"', rendered)
        self.assertIn('FROM "RFDATA"."FACT_SPECTRUM"', rendered)
        self.assertTrue(rendered.endswith('ORDER BY "base"."DT_TIME_START" ASC, "base"."FK_SITE" ASC, "base"."ID_SPECTRUM" ASC'))
        self.assertEqual(params, (9, 1))
        self.assertEqual(calls[3].args[1], ("statement_timeout", "2min", True))

    def test_prune_failure_is_sanitized_and_keeps_audit(self) -> None:
        """Retention failure is isolated and never logged with its message.

        Args:
            None.
        Returns:
            None.
        """
        with patch.object(
            self.handler,
            "_refresh_log_prune",
            side_effect=psycopg.OperationalError("private"),
        ):
            self.handler.refresh_log_record(
                "SITE_EQUIPMENT_METRIC",
                success=True,
                started_at=datetime(2026, 11, 1),
                row_count=1,
            )
        self.connection.commit.assert_called_once()
        self.assertNotIn("private", str(self.handler.log.warning_event.call_args))
        self.handler.log.warning_event.assert_called_once()
