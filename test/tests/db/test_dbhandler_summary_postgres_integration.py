"""Verify summary visibility and advisory exclusion in a disposable database."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4
import os
import sys
import unittest

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _support import APP_ROOT, load_test_config

sys.path.insert(0, str(APP_ROOT))
with patch.dict(sys.modules, {"config": load_test_config()}):
    from db.dbHandlerSummary import dbHandlerSummary


class PostgresSummaryIntegrationTests(unittest.TestCase):
    """Use real transactions, constraints and two independent sessions."""

    @classmethod
    def setUpClass(cls) -> None:
        """Install the repository's summary tables in a random isolated schema.

        Args:
            None.
        Returns:
            None.
        Raises:
            unittest.SkipTest: No disposable database was configured.
            ValueError: The configured database name is not explicitly disposable.
        """
        cls.conninfo = os.environ.get("RFFUSION_TEST_PG_CONNINFO", "")
        if not cls.conninfo:
            raise unittest.SkipTest("RFFUSION_TEST_PG_CONNINFO is not configured.")
        if (
            not conninfo_to_dict(cls.conninfo)
            .get("dbname", "")
            .startswith("rffusion_test_")
        ):
            raise ValueError(
                "Summary integration requires a disposable rffusion_test_* database."
            )
        cls.schema = "TEST_SUMMARY_" + uuid4().hex
        cls.handler = dbHandlerSummary(cls.schema, MagicMock(), conninfo=cls.conninfo)
        cls.observer = dbHandlerSummary(cls.schema, MagicMock(), conninfo=cls.conninfo)
        cls.addClassCleanup(cls._cleanup)
        ddl_path = (
            Path(__file__).resolve().parents[3]
            / "src/postgres/scripts/createFusionSummaryDB.sql"
        )
        # Views reference operational schemas. Only install the actual summary
        # table definitions here; shared operational schemas are never touched.
        ddl = ddl_path.read_text()
        ddl = ddl.replace('"RFFUSION_SUMMARY"', sql.Identifier(cls.schema).as_string())
        with psycopg.connect(cls.conninfo, autocommit=True) as connection:
            connection.execute(ddl)

    @classmethod
    def _cleanup(cls) -> None:
        """Close owned sessions and remove only this test's random schema.

        Args:
            None.
        Returns:
            None.
        """
        cls.handler.close()
        cls.observer.close()
        with psycopg.connect(cls.conninfo, autocommit=True) as connection:
            connection.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(cls.schema)
                )
            )

    def tearDown(self) -> None:
        """Release locks and aborted transactions after every scenario.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.close()
        self.observer.close()

    def test_replacement_is_invisible_until_commit_and_failure_restores_rows(
        self,
    ) -> None:
        """Readers retain the complete old snapshot until the atomic commit.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.site_equipment_replace(
            [{"FK_SITE": 1, "FK_EQUIPMENT": 1, "NU_SPECTRUM_COUNT": 2}]
        )
        self.handler.begin_transaction()
        self.handler.site_equipment_replace(
            [{"FK_SITE": 2, "FK_EQUIPMENT": 1, "NU_SPECTRUM_COUNT": 3}]
        )
        query = sql.SQL('SELECT "FK_SITE" FROM {}').format(
            sql.Identifier(self.schema, "SITE_EQUIPMENT_METRIC")
        )
        self.assertEqual(
            self.observer.connect().execute(query).fetchall(), [{"FK_SITE": 1}]
        )
        self.handler.commit()
        self.assertEqual(
            self.observer.connect().execute(query).fetchall(), [{"FK_SITE": 2}]
        )
        with self.assertRaises(psycopg.errors.NotNullViolation):
            self.handler.site_equipment_replace([{"FK_SITE": 3}])
        self.assertEqual(
            self.observer.connect().execute(query).fetchall(), [{"FK_SITE": 2}]
        )

    def test_session_lock_excludes_another_connection_and_close_releases_it(
        self,
    ) -> None:
        """Only one session owns a shared singleton at a time.

        Args:
            None.
        Returns:
            None.
        """
        name = self.schema + "_singleton"
        self.assertTrue(self.handler.worker_lock_acquire(name))
        self.assertFalse(self.observer.worker_lock_acquire(name))
        self.handler.close()
        self.assertTrue(self.observer.worker_lock_acquire(name))
        self.observer.worker_lock_release(name)

    def test_rebuild_operations_preserve_usage_counters_and_log_offsets(self) -> None:
        """Derived-table publication cannot erase or reset restored usage data.

        Args:
            None.
        Returns:
            None.
        """
        with psycopg.connect(self.conninfo, autocommit=True) as connection:
            connection.execute(
                sql.SQL(
                    'INSERT INTO {} ("NA_METRIC_NAME", "DT_REFERENCE_MONTH", "NU_VALUE") VALUES (%s, %s, %s)'
                ).format(sql.Identifier(self.schema, "WEBFUSION_USAGE_METRIC_MONTHLY")),
                ("test_usage", "2026-11-01", 23),
            )
            connection.execute(
                sql.SQL(
                    'INSERT INTO {} ("NA_SOURCE_NAME", "NA_LOG_PATH", "NU_LAST_OFFSET") VALUES (%s, %s, %s)'
                ).format(sql.Identifier(self.schema, "WEBFUSION_USAGE_LOG_CHECKPOINT")),
                ("test_source", "/test/log", 71),
            )
        self.handler.host_equipment_replace([])
        self.handler.site_equipment_replace([])
        self.handler.host_metric_replace([])
        self.handler.host_monthly_replace([])
        self.handler.host_error_replace([])
        self.handler.host_visit_replace(10, [])
        connection = self.observer.connect()
        row = connection.execute(
            sql.SQL('SELECT "NU_VALUE" FROM {} WHERE "NA_METRIC_NAME" = %s').format(
                sql.Identifier(self.schema, "WEBFUSION_USAGE_METRIC_MONTHLY")
            ),
            ("test_usage",),
        ).fetchone()
        self.assertEqual(row["NU_VALUE"], 23)
        row = connection.execute(
            sql.SQL(
                'SELECT "NU_LAST_OFFSET" FROM {} WHERE "NA_SOURCE_NAME" = %s'
            ).format(sql.Identifier(self.schema, "WEBFUSION_USAGE_LOG_CHECKPOINT")),
            ("test_source",),
        ).fetchone()
        self.assertEqual(row["NU_LAST_OFFSET"], 71)

    def test_outbox_checkpoint_retains_newer_events_and_failure_retries(self) -> None:
        """Failed refreshes preserve events while successful drains stay bounded.

        Args:
            None.
        Returns:
            None.
        """
        consumer = "consumer_" + uuid4().hex
        self.handler.worker_state_read(consumer)
        target = sql.Identifier(self.schema, "SUMMARY_OUTBOX")
        query = sql.SQL(
            'INSERT INTO {} ("NA_SCOPE_TYPE", "NA_SCOPE_VALUE") VALUES (%s, %s) RETURNING "ID_OUTBOX"'
        ).format(target)
        connection = self.handler.connect()
        first = connection.execute(query, ("host", uuid4().hex)).fetchone()["ID_OUTBOX"]
        second = connection.execute(query, ("host", uuid4().hex)).fetchone()[
            "ID_OUTBOX"
        ]
        self.handler.worker_state_failure(consumer, error_message="sanitized")
        self.assertEqual(self.handler.worker_state_read(consumer)["ID_LAST_OUTBOX"], 0)
        self.handler.worker_state_success(
            consumer, last_outbox_id=first, batch_size=1, event_count=1
        )
        self.handler.outbox_delete_consumed(consumer)
        pending = self.handler.outbox_list_pending(consumer, batch_size=10)
        self.assertIn(second, [row["ID_OUTBOX"] for row in pending])
        self.assertNotIn(first, [row["ID_OUTBOX"] for row in pending])

    def test_retention_failure_uses_savepoint_and_preserves_outer_transaction(
        self,
    ) -> None:
        """A real failed retention statement must not abort an audit transaction.

        Args:
            None.
        Returns:
            None.
        """

        def fail_retention(*, max_rows: int) -> int:
            """Raise a real PostgreSQL error inside the retention savepoint.

            Args:
                max_rows (int): Ignored audit window.
            Returns:
                int: Never returns.
            Raises:
                psycopg.errors.DivisionByZero: Deliberate SQL failure.
            """
            self.handler.connect().execute("SELECT 1 / 0")
            return 0

        self.handler.begin_transaction()
        with patch.object(
            self.handler, "_refresh_log_prune", side_effect=fail_retention
        ):
            self.handler.refresh_log_record(
                "audit_test",
                success=True,
                started_at=datetime(2026, 11, 1),
                row_count=1,
            )
        self.assertTrue(self.handler.in_transaction)
        self.handler.commit()
        row = (
            self.observer.connect()
            .execute(
                sql.SQL(
                    'SELECT "NA_OBJECT_NAME" FROM {} WHERE "NA_OBJECT_NAME" = %s'
                ).format(sql.Identifier(self.schema, "SUMMARY_REFRESH_LOG")),
                ("audit_test",),
            )
            .fetchone()
        )
        self.assertEqual(row["NA_OBJECT_NAME"], "audit_test")
