"""Verify BKP domain contracts with Psycopg-shaped sessions and real SQL binding."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
import sys
import unittest
from unittest.mock import MagicMock, patch
import os

import psycopg
from psycopg import sql
from psycopg.adapt import Transformer
from psycopg._queries import PostgresQuery
from psycopg.pq import TransactionStatus


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _support import APP_ROOT, bind_test_config, load_package_from_dir

sys.path.insert(0, str(APP_ROOT))
with bind_test_config():
    from db import dbHandlerBKP as bkp_module
    from shared import constants, logging_utils
    from shared.file_metadata import FileMetadata


from db import constants as db_constants


class PostgresBkpTests(unittest.TestCase):
    """Exercise queue and history policies without starting workers or a database."""

    def setUp(self) -> None:
        """Build a real handler, typed session doubles, and a separate outbox boundary.

        Args:
            None.
        Returns:
            None.
        """
        self.k = bkp_module.k
        self.log = MagicMock(spec=logging_utils.log)
        self.log.pid = 123
        self.handler = bkp_module.dbHandlerBKP(
            "BKP_TEST", self.log, conninfo="dbname=unit_test"
        )
        self.connection = MagicMock(spec=psycopg.Connection)
        self.connection.closed = False
        self.connection.broken = False
        self.connection.info = SimpleNamespace(
            transaction_status=TransactionStatus.IDLE
        )
        self.cursor = MagicMock(spec=psycopg.Cursor)
        self.cursor.connection = self.connection
        self.cursor.rowcount = 1
        self.cursor.fetchall.return_value = []
        self.cursor.fetchone.return_value = {"ID_HOST_TASK": 7}
        self.cursor.execute.side_effect = self._bind_query
        self.cursor.executemany.side_effect = self._bind_batch
        self.connection.cursor.return_value.__enter__.return_value = self.cursor
        self.connection.transaction.return_value.__exit__.return_value = False
        connect = patch.object(psycopg.Connection, "connect", return_value=self.connection)
        self.connect = connect.start()
        self.addCleanup(connect.stop)
        publisher = patch.object(self.handler, "_summary_publish_host_scope")
        self.publisher = publisher.start()
        self.addCleanup(publisher.stop)
        self.addCleanup(self.handler.close)
        self.created = datetime(2025, 2, 3, 4, 5)
        self.file = FileMetadata(
            "/source/a.zip",
            "/source",
            "a.zip",
            ".zip",
            20,
            self.created,
            self.created,
            None,
            "owner",
            "group",
            "rw",
        )

    @staticmethod
    def _bind_query(query: sql.Composable, params: tuple[object, ...] = ()) -> None:
        """Use the installed driver's binding parser to reject broken placeholders.

        Args:
            query (sql.Composable): Actual statement composed by the handler.
            params (tuple[object, ...]): Values supplied to cursor.execute.
        Returns:
            None. This parses/adapts values locally; PostgreSQL execution is separate.
        Raises:
            psycopg.Error: If placeholders or parameter adaptation are invalid.
        """
        PostgresQuery(Transformer()).convert(query, params)

    @classmethod
    def _bind_batch(
        cls, query: sql.Composable, values: list[tuple[object, ...]]
    ) -> None:
        """Validate each executemany parameter set through the installed driver.

        Args:
            query (sql.Composable): Shared statement.
            values (list[tuple[object, ...]]): Bound rows in statement order.
        Returns:
            None.
        Raises:
            psycopg.Error: If any binding is invalid.
        """
        for params in values:
            cls._bind_query(query, params)

    def test_constructor_is_lazy_and_requires_explicit_postgres_destination(
        self,
    ) -> None:
        """Keep the established schema argument without silently using MariaDB settings.

        Args:
            None.
        Returns:
            None.
        """
        self.assertEqual(self.handler.schema, "BKP_TEST")
        self.connect.assert_not_called()
        with self.assertRaises(TypeError):
            bkp_module.dbHandlerBKP("BKP_TEST", self.log)

    def test_host_flags_are_smallints_and_credentials_stay_out_of_sql(self) -> None:
        """Normalize booleans for the unchanged schema and bind access credentials.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.host_upsert(
            ID_HOST=1, IS_BUSY=True, IS_OFFLINE=False, NA_HOST_PASSWORD="private"
        )
        query, rows = self.cursor.executemany.call_args.args
        self.assertIn('"BKP_TEST"."HOST"', query.as_string())
        self.assertEqual(rows, [(1, 1, 0, "private")])
        self.assertNotIn("private", query.as_string())
        self.connection.commit.assert_called_once_with()
        for value in ("1", 2, -1):
            with self.assertRaises(ValueError):
                self.handler.host_upsert(ID_HOST=1, IS_BUSY=value)

    def test_host_status_uses_utc_epoch_and_missing_host_marker(self) -> None:
        """Keep status and timestamp response keys used by workers and the gateway.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [
            {"ID_HOST": 1, "DT_LAST_CHECK": datetime(1970, 1, 1)}
        ]
        self.assertEqual(self.handler.host_read_status(1)["DT_LAST_CHECK"], 0)
        self.cursor.fetchall.return_value = []
        self.assertEqual(self.handler.host_read_status(99), {"status": 0})

    def test_host_update_keeps_deltas_and_assignment_flags_bound(self) -> None:
        """Preserve counter deltas while preventing booleans from becoming arithmetic.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.host_update(1, IS_BUSY=True, NU_HOST_CHECK_ERROR=("INC", 2))
        query, params = self.cursor.execute.call_args.args
        self.assertIn('"IS_BUSY" = %s', query.as_string())
        self.assertIn(
            '"NU_HOST_CHECK_ERROR" = "NU_HOST_CHECK_ERROR" + %s', query.as_string()
        )
        self.assertEqual(params, (2, 1, 1))
        self.handler.host_update(1, NU_HOST_CHECK_ERROR=("DEC", 3))
        self.assertEqual(self.cursor.execute.call_args.args[1], (-3, 1))
        self.handler.host_update(1, reset=True, NU_HOST_CHECK_ERROR=3)
        self.assertNotIn(" + ", self.cursor.execute.call_args.args[0].as_string())

    def test_connectivity_snapshot_keeps_address_and_null_ordering_rules(self) -> None:
        """Exclude missing/blank addresses while retaining busy and offline hosts.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.host_list_for_connectivity_check()
        query, params = self.cursor.execute.call_args.args
        text = query.as_string()
        self.assertIn('"NA_HOST_ADDRESS" IS NOT NULL', text)
        self.assertIn('TRIM("NA_HOST_ADDRESS") <> %s', text)
        self.assertIn(
            'ORDER BY "IS_OFFLINE" ASC NULLS FIRST, "DT_LAST_CHECK" ASC NULLS FIRST, "ID_HOST" ASC',
            text,
        )
        self.assertEqual(params, ("",))
        self.assertNotIn('"IS_BUSY" =', text)
        self.assertNotIn('"IS_OFFLINE" =', text)

    def test_shutdown_snapshots_use_integer_flags_and_running_states(self) -> None:
        """Read cleanup ownership without filtering out unaddressable hosts.

        Args:
            None.
        Returns:
            None.
        """
        for method, table, field, value in (
            (self.handler.host_list_busy, "HOST", "IS_BUSY", 1),
            (self.handler.host_list_offline, "HOST", "IS_OFFLINE", 1),
            (self.handler.host_task_list_running, "HOST_TASK", "NU_STATUS", db_constants['tasks']['states']['running']),
            (self.handler.file_task_list_running, "FILE_TASK", "NU_STATUS", db_constants['tasks']['states']['running']),
        ):
            with self.subTest(table=table, field=field):
                self.cursor.fetchall.return_value = []
                self.assertEqual(method(), [])
                query, params = self.cursor.execute.call_args.args
                text = query.as_string()
                self.assertIn(f'FROM "BKP_TEST"."{table}"', text)
                self.assertIn(f'"{field}" = %s', text)
                self.assertEqual(params, (value,))
                self.assertNotIn('"NA_HOST_ADDRESS"', text)

    def test_host_task_creation_returns_id_and_adapts_json_objects(self) -> None:
        """Use RETURNING instead of deriving the identity or passing a raw dict.

        Args:
            None.
        Returns:
            None.
        """
        task_id = self.handler.host_task_create(FK_HOST=1, FILTER={"mode": "ALL"})
        query, params = self.cursor.execute.call_args.args
        self.assertEqual(task_id, 7)
        self.assertIn('RETURNING "ID_HOST_TASK"', query.as_string())
        self.assertEqual(params[1].obj, {"mode": "ALL"})
        for value in ("invalid json", "[]", 1):
            with self.assertRaises(ValueError):
                self.handler.host_task_create(FK_HOST=1, FILTER=value)

    def test_host_task_claim_copies_filters_and_preserves_external_transaction(
        self,
    ) -> None:
        """Bind an expected status without mutating the caller's selection or committing it.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.begin_transaction()
        self.cursor.rowcount = 0
        where = {"ID_HOST_TASK": 7}
        result = self.handler.host_task_update(
            where_dict=where,
            expected_status=db_constants['tasks']['states']['pending'],
            NU_STATUS=db_constants['tasks']['states']['running'],
        )
        self.assertEqual(where, {"ID_HOST_TASK": 7})
        self.assertEqual(result["rows_affected"], 0)
        self.assertEqual(
            self.cursor.execute.call_args.args[1][-2:], (7, db_constants['tasks']['states']['pending'])
        )
        self.assertTrue(self.handler.in_transaction)
        self.connection.commit.assert_not_called()
        self.publisher.assert_not_called()

    def test_host_task_read_handles_native_json_and_prefixed_projection(self) -> None:
        """Decode both native PostgreSQL JSON and legacy JSON text for worker filters.

        Args:
            None.
        Returns:
            None.
        """
        for value in ({"mode": "ALL"}, '{"mode":"ALL"}'):
            self.cursor.fetchall.return_value = [
                {"HOST__ID_HOST": 1, "HOST_TASK__FILTER": value}
            ]
            row = self.handler.host_task_read(task_type=[1, 2])
            self.assertEqual(row["host_filter"], {"mode": "ALL"})
            query = self.cursor.execute.call_args.args[0].as_string()
            self.assertIn('SELECT "base".*, "joined".*', query)
            self.assertIsNotNone(self.cursor.row_factory)
            self.assertIn('"base"."NU_TYPE" IN (%s, %s)', query)

    def test_file_upsert_targets_hash_expressions_and_preserves_supplied_dates(
        self,
    ) -> None:
        """Infer the real expression index while keeping source identities out of SQL.

        Args:
            None.
        Returns:
            None.
        """
        payload = {
            "FK_HOST": 1,
            "NA_HOST_FILE_PATH": "/a'quoted",
            "NA_HOST_FILE_NAME": "a.zip",
            "DT_FILE_TASK": self.created,
        }
        self.assertEqual(self.handler._file_upsert_batch("FILE_TASK", [payload]), 1)
        query, values = self.cursor.executemany.call_args.args
        text = query.as_string()
        self.assertIn(
            'ON CONFLICT ("FK_HOST", "BKP_TEST"."file_identity_sha256"("NA_HOST_FILE_PATH")',
            text,
        )
        self.assertIn('"DT_FILE_TASK" = EXCLUDED."DT_FILE_TASK"', text)
        self.assertNotIn("CURRENT_TIMESTAMP", text)
        self.assertNotIn("/a'quoted", text)
        self.assertEqual(values[0][-1], self.created)

    def test_file_creation_reuses_metadata_mapping_and_standalone_transaction(
        self,
    ) -> None:
        """Keep source timestamps unchanged while making every input chunk atomic.

        Args:
            None.
        Returns:
            None.
        """
        result = self.handler.file_task_create(
            1, db_constants['tasks']['file_types']['backup'], db_constants['tasks']['states']['pending'], [self.file]
        )
        self.assertEqual(result, 1)
        values = self.cursor.executemany.call_args.args[1][0]
        self.assertIn(self.created, values)
        self.connection.commit.assert_called_once_with()
        self.assertFalse(self.handler.in_transaction)

    def test_file_update_preserves_timestamp_ownership_and_expected_status(
        self,
    ) -> None:
        """Do not synthesize queue dates when an entrypoint claims or finalizes a task.

        Args:
            None.
        Returns:
            None.
        """
        result = self.handler.file_task_update(
            task_id=7,
            expected_status=db_constants['tasks']['states']['pending'],
            NU_STATUS=db_constants['tasks']['states']['running'],
        )
        self.assertNotIn("DT_FILE_TASK", result["updated_fields"])
        self.assertEqual(result["updated_fields"]["NU_PID"], self.log.pid)
        self.assertEqual(
            result["where_used"], {"ID_FILE_TASK": 7, "NU_STATUS": db_constants['tasks']['states']['pending']}
        )

    def test_history_none_and_set_null_have_distinct_meanings(self) -> None:
        """Retain the established history API for ignored fields and explicit SQL NULL.

        Args:
            None.
        Returns:
            None.
        """
        result = self.handler.file_task_history_update(history_id=4, DT_BACKUP=None)
        self.assertEqual(result["updated_fields"], {})
        self.connect.assert_not_called()
        result = self.handler.file_task_history_update(
            history_id=4, DT_BACKUP=constants.SET_NULL, IS_PAYLOAD_DELETED=True
        )
        self.assertIsNone(result["updated_fields"]["DT_BACKUP"])
        self.assertEqual(result["updated_fields"]["IS_PAYLOAD_DELETED"], 1)
        self.assertNotIn("DT_PROCESSED", result["updated_fields"])

    def test_history_markers_are_monotonic_and_publish_old_and_new_months(self) -> None:
        """Update the host marker with history and invalidate both sides of a month move.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [
            {"FK_HOST": 1, "DT_FILE_CREATED_HOST": self.created}
        ]
        moved = datetime(2025, 3, 1)
        self.handler.file_task_history_update(
            history_id=4,
            NU_STATUS_BACKUP=db_constants['tasks']['states']['done'],
            DT_BACKUP=moved,
            DT_FILE_CREATED_HOST=moved,
        )
        query, params = self.cursor.execute.call_args.args
        self.assertIn(
            '"DT_LAST_BACKUP" = GREATEST("DT_LAST_BACKUP", %s)', query.as_string()
        )
        self.assertEqual(params, (moved, 1))
        self.assertEqual(
            self.publisher.call_args.kwargs["reference_months"], [self.created, moved]
        )
        self.connection.commit.assert_called_once_with()

    def test_compound_writes_roll_back_on_failure_but_never_rollback_external_work(
        self,
    ) -> None:
        """Retain transaction ownership when the database rejects an operation.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.execute.side_effect = psycopg.errors.CheckViolation(
            "private detail"
        )
        with self.assertRaises(psycopg.errors.CheckViolation):
            self.handler.file_task_update(task_id=7, NU_STATUS=db_constants['tasks']['states']['done'])
        self.connection.rollback.assert_called_once_with()
        self.connection.commit.assert_not_called()
        self.assertNotIn("private detail", str(self.log.error_event.call_args_list))
        self.connection.rollback.reset_mock()
        self.handler.begin_transaction()
        with self.assertRaises(psycopg.errors.CheckViolation):
            self.handler.file_task_update(task_id=7, NU_STATUS=db_constants['tasks']['states']['done'])
        self.assertTrue(self.handler.in_transaction)
        self.connection.rollback.assert_not_called()

    def test_uncertain_commit_is_not_rolled_back_or_replayed(self) -> None:
        """Keep the base's uncertain-commit contract for compound domain operations.

        Args:
            None.
        Returns:
            None.
        """
        self.connection.commit.side_effect = psycopg.OperationalError(
            "private connection"
        )
        with self.assertRaises(psycopg.OperationalError):
            self.handler.file_task_create(1, 1, 1, [self.file])
        self.connection.commit.assert_called_once_with()
        self.connection.rollback.assert_not_called()
        self.assertTrue(self.handler.in_transaction)
        self.connect.assert_called_once()

    def test_selection_preserves_fairness_discovery_reservation_and_null_order(
        self,
    ) -> None:
        """Compose PostgreSQL scheduling predicates without MySQL date syntax.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [
            {"HOST__ID_HOST": 1, "FILE_TASK__ID_FILE_TASK": 7}
        ]
        row = self.handler.file_task_read(
            task_type=db_constants['tasks']['file_types']['backup'],
            check_host_busy=False,
            reserve_hosts_for_discovery=True,
            fair_by_host=True,
            prefer_current_year=True,
            newest_first=True,
        )
        query, params = self.cursor.execute.call_args.args
        text = query.as_string()
        self.assertEqual(row[1:], (1, 7))
        self.assertIn("EXTRACT(YEAR", text)
        self.assertIn('"candidate"."DT_FILE_CREATED_HOST" DESC NULLS LAST', text)
        self.assertIn('h."DT_LAST_BACKUP" ASC NULLS FIRST', text)
        self.assertIn("NOT EXISTS", text)
        self.assertIn('reservation."NU_TYPE" = %s', text)
        self.assertIn(self.k.DISCOVERY_RESERVATION_TTL_SEC, params)
        self.assertNotIn("DATE_SUB", text)

    def test_explicit_task_selection_bypasses_queue_priorities(self) -> None:
        """Read a requested identity without applying global queue eligibility.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.file_task_read(
            task_id=7,
            check_host_busy=False,
            task_type=1,
            fair_by_host=True,
            newest_first=True,
            prefer_current_year=True,
        )
        query, params = self.cursor.execute.call_args.args
        self.assertEqual(params, (7, 1))
        self.assertNotIn("EXTRACT", query.as_string())
        self.assertNotIn('"base"."NU_TYPE" = ', query.as_string())

    def test_file_task_selection_binds_hostile_extension_in_each_queue_mode(
        self,
    ) -> None:
        """Keep SQL syntax inside extension values from changing the selection statement.

        Args:
            None.
        Returns:
            None.
        """
        extension = ".zip' OR TRUE; DROP TABLE FILE_TASK; -- %s"
        for fair, reserve in ((False, False), (False, True), (True, True)):
            with self.subTest(fair_by_host=fair, reserve_discovery=reserve):
                self.handler.file_task_read(
                    task_type=db_constants['tasks']['file_types']['backup'],
                    check_host_busy=False,
                    extension=extension,
                    fair_by_host=fair,
                    reserve_hosts_for_discovery=reserve,
                    prefer_current_year=True,
                    newest_first=True,
                )
                query, params = self.cursor.execute.call_args.args
                self.assertNotIn(extension, query.as_string())
                self.assertEqual(params.count(extension), 1)
                bound = PostgresQuery(Transformer())
                bound.convert(query, params)
                self.assertNotIn(extension.encode(), bound.query)
                if fair:
                    self.assertIn(
                        'candidate."NA_EXTENSION_HOST" = t."NA_EXTENSION_HOST"',
                        query.as_string(),
                    )

    def test_fair_candidate_uses_only_the_requested_status_and_extension(self) -> None:
        """Match optional filters in both the outer task and its per-host candidate.

        Args:
            None.
        Returns:
            None.
        """
        for status in (None, db_constants['tasks']['states']['pending']):
            for extension in (None, ".zip"):
                with self.subTest(status=status, extension=extension):
                    self.handler.file_task_read(
                        task_type=db_constants['tasks']['file_types']['backup'],
                        task_status=status,
                        extension=extension,
                        fair_by_host=True,
                        check_host_busy=False,
                    )
                    query = self.cursor.execute.call_args.args[0].as_string()
                    self.assertEqual(
                        'candidate."NU_STATUS" = t."NU_STATUS"' in query,
                        status is not None,
                    )
                    self.assertEqual(
                        'candidate."NA_EXTENSION_HOST" = t."NA_EXTENSION_HOST"'
                        in query,
                        extension is not None,
                    )

    def _read_backup_with_host(self, host: dict, *occupancy: list) -> object:
        """Run one locked backup read with a candidate and the host lock result.

        Args:
            host (dict): Row returned by the FOR UPDATE host read.
            *occupancy (list): Rows returned by the HOST_TASK/FILE_TASK checks.
        Returns:
            object: The file_task_read result.
        """
        self.cursor.fetchall.side_effect = [
            [{"HOST__ID_HOST": 1, "FILE_TASK__ID_FILE_TASK": 7}],
            [host],
            *occupancy,
        ]
        return self.handler.file_task_read(
            task_type=1, lock_host=True, check_host_offline=True
        )

    def _host_writes(self) -> list[str]:
        """Return reservation UPDATEs, excluding the cooldown sweep.

        Args:
            None.
        Returns:
            list[str]: SQL text of host reservation writes.
        """
        return [
            text
            for call in self.cursor.execute.call_args_list
            if (text := call.args[0].as_string()).startswith('UPDATE "BKP_TEST"."HOST"')
            and "DT_BUSY\" < %s" not in text
            and '"DT_BUSY" < %s' not in text
        ]

    def test_failed_host_reservation_returns_none_without_claiming_task(self) -> None:
        """Treat a host held by another owner as an expected selection race.

        Args:
            None.
        Returns:
            None.
        """
        host = {"IS_BUSY": 1, "IS_OFFLINE": 0, "NU_PID": os.getpid() + 1}
        self.assertIsNone(self._read_backup_with_host(host))
        query = self.cursor.execute.call_args.args[0].as_string()
        self.assertTrue(query.endswith("FOR UPDATE"))
        self.assertEqual(self._host_writes(), [])
        self.connection.commit.assert_called_once_with()

    def test_live_host_task_blocks_reservation_but_dead_owner_does_not(self) -> None:
        """Occupy a host only when no other live worker runs host-dependent work.

        Args:
            None.
        Returns:
            None.
        """
        host = {"IS_BUSY": 0, "IS_OFFLINE": 0, "NU_PID": 0}
        with patch.object(bkp_module.tools, "pid_exists", return_value=True):
            self.assertIsNone(self._read_backup_with_host(host, [{"NU_PID": 4242}]))
        self.assertEqual(self._host_writes(), [])
        occupancy = self.cursor.execute.call_args.args
        self.assertIn('"NU_PID" <> %s', occupancy[0].as_string())
        self.assertIn(os.getpid(), occupancy[1])
        self.assertEqual(
            occupancy[1], (1, db_constants['tasks']['states']['running'], db_constants['tasks']['host_types']['processing'], os.getpid())
        )
        self.cursor.execute.reset_mock()
        with patch.object(bkp_module.tools, "pid_exists", return_value=False):
            result = self._read_backup_with_host(host, [{"NU_PID": 4242}], [])
        self.assertEqual(result[1:], (1, 7))
        self.assertEqual(len(self._host_writes()), 1)

    def test_reservation_is_idempotent_for_the_current_owner(self) -> None:
        """Let a retry by the same PID keep its host instead of losing it.

        Args:
            None.
        Returns:
            None.
        """
        host = {"IS_BUSY": 1, "IS_OFFLINE": 0, "NU_PID": os.getpid()}
        self.assertIsNotNone(self._read_backup_with_host(host, [], []))
        (write,) = self._host_writes()
        self.assertIn('"NU_PID" = %s', write)
        self.assertNotIn('"IS_BUSY" = %s AND', write)

    def test_rediscovery_upsert_never_resets_running_rows(self) -> None:
        """Skip conflict updates while queue or history work is RUNNING.

        Args:
            None.
        Returns:
            None.
        """
        running = db_constants['tasks']['states']['running']
        self.handler.file_task_create(1, 1, db_constants['tasks']['states']['pending'], [self.file])
        self.handler.file_task_history_create(1, 1, db_constants['tasks']['states']['done'], [self.file])
        queue, history = [
            call.args[0].as_string()
            for call in self.cursor.executemany.call_args_list
        ]
        self.assertIn(f'WHERE "FILE_TASK"."NU_STATUS" IS DISTINCT FROM {running}', queue)
        self.assertIn(
            f'"FILE_TASK_HISTORY"."NU_STATUS_BACKUP" IS DISTINCT FROM {running} AND '
            f'"FILE_TASK_HISTORY"."NU_STATUS_PROCESSING" IS DISTINCT FROM {running}',
            history,
        )

    def test_busy_timeout_release_compares_the_observed_reservation(self) -> None:
        """Never clear a reservation taken after the timeout read.

        Args:
            None.
        Returns:
            None.
        """
        busy_since = datetime.utcnow() - timedelta(days=1)
        self.cursor.fetchall.return_value = [
            {"IS_BUSY": 1, "DT_BUSY": busy_since, "NU_PID": 77}
        ]
        self.handler.host_update(
            1, check_busy_timeout=True, IS_BUSY=1, DT_LAST_CHECK=busy_since
        )
        release, update = [
            call.args
            for call in self.cursor.execute.call_args_list
            if call.args[0].as_string().startswith("UPDATE")
        ]
        self.assertIn('"NU_PID" = %s AND "DT_BUSY" = %s', release[0].as_string())
        self.assertEqual(release[1][-2:], (77, busy_since))
        self.assertNotIn('"IS_BUSY"', update[0].as_string())
        self.assertEqual(update[1], (busy_since, 1))

    def test_cooldown_expiry_updates_only_the_sentinel(self) -> None:
        """Keep PID ownership checks and integer storage for transient reservations.

        Args:
            None.
        Returns:
            None.
        """
        self.handler._host_release_expired_cooldowns()
        query, params = self.cursor.execute.call_args.args
        self.assertIn('"DT_BUSY" < %s', query.as_string())
        self.assertIn(db_constants['hosts']['transient_busy_pid'], params)

    def test_safe_release_preserves_cooldown_and_checks_the_observed_reservation(
        self,
    ) -> None:
        """Keep a stale inspection from releasing a reservation acquired afterward.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [
            {
                "IS_BUSY": 1,
                "NU_PID": db_constants['hosts']['transient_busy_pid'],
                "DT_BUSY": self.created,
            }
        ]
        self.handler.host_release_safe(1, 123)
        self.assertEqual(self.cursor.execute.call_count, 1)
        self.cursor.fetchall.return_value = [
            {"IS_BUSY": 1, "NU_PID": 123, "DT_BUSY": self.created}
        ]
        self.cursor.rowcount = 0
        self.handler.host_release_safe(1, 123)
        query, params = self.cursor.execute.call_args.args
        self.assertIn('"DT_BUSY" = %s', query.as_string())
        self.assertEqual(params[-4:], (1, 1, 123, self.created))
        self.publisher.assert_not_called()

    def test_shared_compatibility_imports_are_lazy_and_keep_existing_aliases(
        self,
    ) -> None:
        """Keep importing shared utilities independent from unmigrated domain packages.

        Args:
            None.
        Returns:
            None.
        """
        with patch.dict(sys.modules):
            shared = load_package_from_dir("test_bkp_shared_lazy", APP_ROOT / "shared")
            module = ModuleType("server_handler.sleep")
            with patch.object(shared, "import_module", return_value=module) as loader:
                loader.assert_not_called()
                self.assertIs(shared.sleep, module)
                self.assertIs(shared.sleep, module)
                loader.assert_called_once_with("server_handler.sleep")
                with self.assertRaises(AttributeError):
                    shared.unsupported_alias

    def test_maintenance_snapshots_bind_requested_types_and_states(self) -> None:
        """Read host/task snapshots without executing janitor decisions or writes.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.side_effect = [
            [{"ID_HOST": 1, "DT_BUSY": None, "NU_PID": 9}, {"ID_HOST": 2}],
            [{"FK_HOST": 2}],
            [{"FK_HOST": 1}, {"FK_HOST": 1}],
        ]
        rows = self.handler.host_list_busy_with_task_state(
            task_status=2, host_task_type=3
        )
        self.assertEqual(
            [(r["FILE_RUNNING"], r["HOST_PROCESSING_RUNNING"]) for r in rows],
            [(False, True), (True, False)],
        )
        self.assertEqual(
            [call.args[1] for call in self.cursor.execute.call_args_list],
            [(1,), (2, 1, 2), (2, 3, 1, 2)],
        )
        self.cursor.fetchall.side_effect = None
        self.cursor.execute.reset_mock()
        self.handler.host_task_list_with_host_state(
            task_types=(1, 2, 3, 6, 7, 8), task_statuses=(1, 2)
        )
        query, params = self.cursor.execute.call_args.args
        self.assertEqual(params, (1, 2, 3, 6, 7, 8, 1, 2))
        self.assertIn('"base"."NU_TYPE" IN (%s, %s, %s, %s, %s, %s)', query.as_string())
        self.assertTrue(
            all(
                call.args[0].as_string().lstrip().startswith("SELECT")
                for call in self.cursor.execute.call_args_list
            )
        )

    def test_backlog_primitives_lock_and_bind_prepared_values(self) -> None:
        """Keep candidate locks and row-specific messages inside parameterized SQL.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.begin_transaction()
        self.handler.file_task_list_for_backlog(
            where={"FK_HOST": 1},
            order_by=(("ID_FILE_TASK", "DESC"),),
            limit=1,
        )
        query, params = self.cursor.execute.call_args.args
        self.assertTrue(query.as_string().endswith("LIMIT %s FOR UPDATE"))
        self.assertEqual(params, (1, 1))
        prefix = "'; DROP TABLE FILE_TASK; --"
        self.assertEqual(
            self.handler.file_task_update_backlog(
                data={"NU_TYPE": 1},
                where={"FK_HOST": 1, "ID_FILE_TASK__in": [4]},
                message_prefix=prefix,
            ),
            1,
        )
        query, params = self.cursor.execute.call_args.args
        self.assertNotIn(prefix, query.as_string())
        self.assertIn('"NA_MESSAGE" = CONCAT(%s', query.as_string())
        self.assertEqual(params, (1, prefix, 1, 4))
        self.connection.commit.assert_not_called()
        self.connection.rollback.assert_not_called()
        self.publisher.assert_not_called()

    def test_backlog_primitives_reject_missing_scope_and_message_assignment(
        self,
    ) -> None:
        """Reject broad writes and overriding the composed message before connecting.

        Args:
            None.
        Returns:
            None.
        """
        for data, where in (
            ({}, {"FK_HOST": 1}),
            ({"NU_TYPE": 1}, {}),
            ({"NA_MESSAGE": "x"}, {"FK_HOST": 1}),
        ):
            with self.subTest(data=data, where=where):
                with self.assertRaises(ValueError):
                    self.handler.file_task_update_backlog(
                        data=data, where=where, message_prefix="prefix"
                    )
        with self.assertRaises(RuntimeError):
            self.handler.file_task_list_for_backlog(where={"FK_HOST": 1}, order_by=())
        self.connect.assert_not_called()

    def test_connectivity_recovery_preserves_frozen_and_statistics_task_rules(
        self,
    ) -> None:
        """Keep automatic host recovery away from manually frozen work.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.host_task_resume_by_host(1)
        updates = [
            call
            for call in self.cursor.execute.call_args_list
            if call.args[0].as_string().startswith("UPDATE")
        ]
        self.assertEqual(len(updates), 2)
        for call in updates:
            params = call.args[1]
            self.assertNotIn(db_constants['tasks']['states']['frozen'], params)
            self.assertEqual(params[0], db_constants['tasks']['states']['pending'])
            self.assertIsNone(params[1])
            text = call.args[0].as_string()
            self.assertIn('"NU_TYPE" IN', text)

    def test_file_recovery_restricts_error_types_without_restricting_suspended_tasks(
        self,
    ) -> None:
        """Keep file-queue eligibility separate from host-task connectivity types.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [
            {"ID_FILE_TASK": 5, "NU_PID": 111},
            {"ID_FILE_TASK": 6, "NU_PID": 222},
        ]
        with patch.object(
            bkp_module.tools, "pid_exists", side_effect=lambda pid: pid == 222
        ):
            self.handler.file_task_resume_by_host(1)
        candidates, suspended, errored, stale = self.cursor.execute.call_args_list
        self.assertTrue(candidates.args[0].as_string().startswith("SELECT"))
        self.assertIn('"ID_FILE_TASK" IN (%s)', stale.args[0].as_string())
        self.assertEqual(stale.args[1][-1], 5)
        self.assertNotIn('"NU_TYPE" IN', suspended.args[0].as_string())
        self.assertIn('"NU_TYPE" IN', errored.args[0].as_string())
        self.assertEqual(
            errored.args[1][-2:],
            (db_constants['tasks']['file_types']['discovery'], db_constants['tasks']['file_types']['backup']),
        )
        self.assertIn('"DT_FILE_TASK" < %s', stale.args[0].as_string())
        self.assertTrue(
            all(
                db_constants['tasks']['states']['frozen'] not in call.args[1]
                for call in self.cursor.execute.call_args_list
            )
        )

    def test_dedup_uses_size_and_preserves_converted_zip_path_identity(self) -> None:
        """Retain larger source revisions and equal filenames from different paths.

        Args:
            None.
        Returns:
            None.
        """
        files = [
            self.file,
            replace(self.file, VL_FILE_SIZE_KB=30),
            replace(self.file, NA_PATH="/different"),
        ]
        history = {
            "NA_HOST_FILE_PATH": "/source",
            "NA_HOST_FILE_NAME": "a.zip",
            "VL_FILE_SIZE_KB_HOST": 20,
            "NA_EXTENSION_SERVER": None,
            "NU_STATUS_PROCESSING": db_constants['tasks']['states']['pending'],
        }
        self.cursor.fetchall.side_effect = [[history]]
        self.assertEqual(
            self.handler.file_task_history_filter_existing_batch(
                1, files, batch_size=3
            ),
            files[1:],
        )
        converted = {
            **history,
            "VL_FILE_SIZE_KB_HOST": 99,
            "NA_EXTENSION_SERVER": ".MAT",
            "NU_STATUS_PROCESSING": db_constants['tasks']['states']['done'],
        }
        self.cursor.fetchall.side_effect = [[converted]]
        self.assertEqual(
            self.handler.file_task_history_filter_existing_batch(
                1, files, batch_size=3
            ),
            files[2:],
        )

    def test_queue_host_task_preserves_running_singleton(self) -> None:
        """Keep an active host task unchanged while returning the current host status.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.side_effect = [
            [{"ID_HOST_TASK": 7, "NU_STATUS": db_constants['tasks']['states']['running']}],
            [{"ID_HOST": 1}],
        ]
        result = self.handler.host_task_enqueue(
            1, db_constants['tasks']['host_types']['check'], db_constants['tasks']['states']['pending'], {"mode": "ALL"}
        )
        self.assertEqual(result["status"], 1)
        self.assertTrue(
            all(
                call.args[0].as_string().startswith("SELECT")
                for call in self.cursor.execute.call_args_list
            )
        )
        self.log.warning_event.assert_called_once()
        self.publisher.assert_not_called()
        self.connection.commit.assert_called_once_with()

    def test_enqueue_reuses_pending_task_before_running_or_terminal_matches(
        self,
    ) -> None:
        """Reuse the preferred existing identity while keeping singleton diagnostics.

        Args:
            None.
        Returns:
            None.
        """
        tasks = [
            {"HOST_TASK__ID_HOST_TASK": 9, "HOST_TASK__NU_STATUS": db_constants['tasks']['states']['done']},
            {"HOST_TASK__ID_HOST_TASK": 8, "HOST_TASK__NU_STATUS": db_constants['tasks']['states']['running']},
            {"HOST_TASK__ID_HOST_TASK": 7, "HOST_TASK__NU_STATUS": db_constants['tasks']['states']['pending']},
        ]
        with patch.object(
            self.handler, "host_task_list", return_value=tasks
        ), patch.object(self.handler, "host_task_update") as update, patch.object(
            self.handler, "host_task_create"
        ) as create, patch.object(
            self.handler, "host_read_status", return_value={"status": 1}
        ):
            self.handler.host_task_enqueue(
                1, db_constants['tasks']['host_types']['check'], db_constants['tasks']['states']['pending'], {"mode": "ALL"}
            )
        self.assertEqual(update.call_args.kwargs["task_id"], 7)
        create.assert_not_called()

    def test_gc_binds_quarantine_and_requires_processed_anchor(self) -> None:
        """Keep retention tied to terminal processing time and undeleted payloads.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.file_task_history_list_gc_candidates(
            10,
            7,
            trash_root="/repo/trash/",
            resolved_root="/repo/trash/resolved_files",
        )
        query, params = self.cursor.execute.call_args.args
        text = query.as_string()
        self.assertIn('"DT_PROCESSED" < %s', text)
        self.assertIn(
            ' AND ("NA_SERVER_FILE_PATH" = %s OR "NA_SERVER_FILE_PATH" LIKE %s)'
            ' AND NOT ("NA_SERVER_FILE_PATH" = %s OR "NA_SERVER_FILE_PATH" LIKE %s)',
            text,
        )
        self.assertTrue(text.endswith('ORDER BY "DT_PROCESSED" ASC, "ID_HISTORY" ASC LIMIT %s'))
        self.assertEqual(params[:2], (db_constants['tasks']['states']['error'], 0))
        self.assertAlmostEqual(
            params[2],
            datetime.utcnow() - timedelta(days=7),
            delta=timedelta(seconds=5),
        )
        self.assertEqual(
            params[3:],
            (
                "/repo/trash",
                "/repo/trash/%",
                "/repo/trash/resolved_files",
                "/repo/trash/resolved\\_files/%",
                10,
            ),
        )

    def test_workers_reuse_one_session_across_compound_operations(self) -> None:
        """Avoid reconnecting between iterations or after a successful managed commit.

        Args:
            None.
        Returns:
            None.
        """
        for identifier in range(3):
            self.handler.host_upsert(ID_HOST=identifier)
        self.connect.assert_called_once()
        self.connection.close.assert_not_called()
        self.assertEqual(self.connection.commit.call_count, 3)

    def test_publication_refreshes_identity_and_isolates_failures(self) -> None:
        """Publish inside a savepoint and report only sanitized failure classification.

        Args:
            None.
        Returns:
            None.
        """
        publish = bkp_module.dbHandlerBKP._summary_publish_host_scope
        self.handler.begin_transaction()
        publish(
            self.handler,
            1,
            reference_months=[self.created, "2025-02", "invalid"],
            reason="test",
        )
        query, rows = self.cursor.executemany.call_args.args
        self.assertIn('"ID_OUTBOX" = EXCLUDED."ID_OUTBOX"', query.as_string())
        self.assertIn('"DT_CREATED_AT" = EXCLUDED."DT_CREATED_AT"', query.as_string())
        self.assertEqual(
            {(row[0], row[1]) for row in rows},
            {("host", "1"), ("reference_month", "2025-02-01")},
        )
        self.connection.transaction.assert_called_once_with()
        self.assertTrue(self.handler.in_transaction)
        self.connection.commit.assert_not_called()
        error = psycopg.errors.UndefinedTable("private SQL or credentials")
        self.cursor.executemany.side_effect = error
        publish(self.handler, 1, reason="test")
        self.assertTrue(self.handler.in_transaction)
        self.connection.rollback.assert_not_called()
        self.assertIs(
            self.connection.transaction.return_value.__exit__.call_args.args[1], error
        )
        self.assertNotIn("private", str(self.log.warning_event.call_args_list))

    def test_operational_signals_update_only_the_source_host(self) -> None:
        """Immediate evidence must not write a derived snapshot or invent recovery.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.host_update_signals(8, {"IS_SSH_FAILURE": False, "DT_LAST_SSH_EVALUATED_AT": self.created})
        query, params = self.cursor.execute.call_args.args
        rendered = query.as_string()
        self.assertIn('UPDATE "BKP_TEST"."HOST"', rendered)
        self.assertIn(0, params)
        self.assertNotIn('"NA_LAST_SSH_FAILURE_DESCRIPTION"', rendered)
        self.cursor.executemany.assert_not_called()
        with self.assertRaises(ValueError):
            self.handler.host_update_signals(8, {"IS_BUSY": 0})
        with self.assertRaises(ValueError):
            self.handler.host_update_signals(8, {})


if __name__ == "__main__":
    unittest.main()
