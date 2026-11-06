"""Verify PostgreSQL base contracts without importing the MariaDB runtime."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import sys
import unittest

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus


APP_ROOT = (
    Path(__file__).resolve().parents[3]
    / "src/appCataloga/server_volume/usr/local/bin/appCataloga"
)
sys.path.insert(0, str(APP_ROOT))
sys.path.insert(1, str(Path(__file__).resolve().parents[1]))

from _support import SHARED_ROOT, load_module_from_path, load_test_config

with patch.dict(sys.modules, {"config": load_test_config()}):
    from db.dbHandlerBase import DbHandler

    logging_utils = load_module_from_path(
        "test_logging_utils", str(SHARED_ROOT / "logging_utils.py")
    )


class ExampleHandler(DbHandler):
    """Declare one test table independently from production schemas."""

    VALID_FIELDS_SAMPLE = {"ID", "NAME", "UPDATED", "ACTIVE"}
    PRIMARY_KEY_SAMPLE = ("ID",)


class PostgresBaseTests(unittest.TestCase):
    """Exercise real SQL composition with driver-shaped connection doubles."""

    def setUp(self) -> None:
        """Build a disconnected handler and one controlled session.

        Args:
            None.
        Returns:
            None.
        """
        self.log = MagicMock(spec=logging_utils.log)
        self.handler = ExampleHandler("TEST", self.log, conninfo="dbname=unit_test")
        self.connection = MagicMock(spec=psycopg.Connection)
        self.connection.closed = False
        self.connection.broken = False
        self.connection.info = SimpleNamespace(
            transaction_status=TransactionStatus.IDLE
        )
        self.cursor = MagicMock(spec=psycopg.Cursor)
        self.cursor.connection = self.connection
        self.cursor.rowcount = 1
        self.connection.cursor.return_value.__enter__.return_value = self.cursor
        self.connect_patch = patch.object(
            psycopg, "connect", return_value=self.connection
        )
        self.connect = self.connect_patch.start()
        self.addCleanup(self.connect_patch.stop)
        self.addCleanup(self.handler.close)

    def test_construction_is_lazy_and_requires_explicit_database(self) -> None:
        """Require a destination without connecting during construction.

        Args:
            None.
        Returns:
            None.
        """
        self.connect.assert_not_called()
        with self.assertRaises(ValueError):
            ExampleHandler("", self.log, conninfo="dbname=unit_test")
        for conninfo in ("", "host=localhost", "password='invalid"):
            with self.subTest(conninfo_kind=bool(conninfo)):
                with self.assertRaises(ValueError) as raised:
                    ExampleHandler("TEST", self.log, conninfo=conninfo)
                self.assertNotIn("invalid'", str(raised.exception))

    def test_public_connect_reuses_the_owned_session(self) -> None:
        """Allow explicit startup connection without replacing an existing session.

        Args:
            None.
        Returns:
            None.
        """
        self.assertIs(self.handler.connect(), self.connection)
        self.assertIs(self.handler.connect(), self.connection)
        self.connect.assert_called_once()
        self.cursor.execute.assert_not_called()
        self.assertFalse(self.handler.in_transaction)

    def test_loop_operations_reuse_the_session_until_explicit_close(self) -> None:
        """Retain one idle session across repeated reads and autocommit writes.

        Args:
            None.
        Returns:
            None.
        """
        for identifier in range(3):
            self.handler._select_batch("SAMPLE", {"ID": identifier})
            self.handler._insert_row("SAMPLE", {"ID": identifier})
        self.connect.assert_called_once()
        self.connection.close.assert_not_called()
        self.assertTrue(self.connection.autocommit)
        self.assertFalse(self.handler.in_transaction)
        self.connection.commit.assert_not_called()
        self.connection.transaction.assert_not_called()
        self.handler.close()
        self.connection.close.assert_called_once_with()

    def test_constructor_checks_setting_types_at_the_public_boundary(self) -> None:
        """Reject malformed external settings before any driver operation.

        Args:
            None.
        Returns:
            None.
        """
        for schema, conninfo in ((None, "dbname=unit_test"), ("TEST", None)):
            with self.subTest(schema=schema, conninfo=conninfo):
                with self.assertRaises(TypeError):
                    ExampleHandler(schema, self.log, conninfo=conninfo)
        self.connect.assert_not_called()

    def test_public_connect_preserves_the_callers_transaction(self) -> None:
        """Validate session reuse without changing transaction ownership or mode.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.begin_transaction()
        self.assertIs(self.handler.connect(), self.connection)
        self.assertTrue(self.handler.in_transaction)
        self.assertFalse(self.connection.autocommit)
        self.connection.commit.assert_not_called()
        self.connection.rollback.assert_not_called()
        self.connect.assert_called_once()

    def test_public_connect_propagates_failure_without_logging_credentials(
        self,
    ) -> None:
        """Expose startup failures while keeping sensitive driver details out of logs.

        Args:
            None.
        Returns:
            None.
        """
        error = psycopg.OperationalError("private connection details")
        self.connect.side_effect = error
        with self.assertRaises(psycopg.OperationalError) as raised:
            self.handler.connect()
        self.assertIs(raised.exception, error)
        self.assertIsNone(self.handler.db_connection)
        self.log.error_event.assert_called_once_with(
            "db_connect_failed",
            db_handler="ExampleHandler",
            operation="connect",
            error="OperationalError",
            sqlstate=None,
        )

    def test_select_keeps_names_quoted_and_values_separate(self) -> None:
        """Bind hostile values without inserting them into generated SQL.

        Args:
            None.
        Returns:
            None.
        """
        value = "x'; DROP TABLE SAMPLE; --"
        expected = [{"ID": 3, "NAME": value}]
        self.cursor.fetchall.return_value = expected
        actual = self.handler._select_batch(
            "SAMPLE",
            {"NAME": value},
            columns=("NAME", "ID"),
            order_by=(("ID", "desc"),),
            limit=0,
        )
        query, params = self.cursor.execute.call_args.args
        self.assertEqual(
            query.as_string(),
            'SELECT "NAME", "ID" FROM "TEST"."SAMPLE" WHERE "NAME" = %s ORDER BY "ID" DESC LIMIT %s',
        )
        self.assertEqual(params, (value, 0))
        self.assertEqual(actual, expected)
        self.assertNotIn(value, query.as_string())

    def test_unknown_names_and_sql_fragments_are_rejected_before_connection(
        self,
    ) -> None:
        """Reject undeclared names and the legacy raw-filter escape hatch.

        Args:
            None.
        Returns:
            None.
        """
        operations = (
            lambda: self.handler._select_batch("OTHER"),
            lambda: self.handler._insert_row("OTHER", {"ID": 1}),
            lambda: self.handler._insert_batch("OTHER", [{"ID": 1}]),
            lambda: self.handler._upsert_batch(
                "OTHER", [{"ID": 1}], conflict_columns=("ID",)
            ),
            lambda: self.handler._insert_row("SAMPLE", {}),
            lambda: self.handler._insert_batch("SAMPLE", [{}]),
            lambda: self.handler._upsert_batch(
                "SAMPLE", [{}], conflict_columns=("ID",)
            ),
            lambda: self.handler._insert_row("SAMPLE", {"UNKNOWN": 1}),
            lambda: self.handler._insert_batch("SAMPLE", [{"UNKNOWN": 1}]),
            lambda: self.handler._upsert_batch(
                "SAMPLE", [{"ID": 1, "UNKNOWN": 1}], conflict_columns=("ID",)
            ),
            lambda: self.handler._insert_row(
                "SAMPLE", {"ID": 1}, returning=("UNKNOWN",)
            ),
            lambda: self.handler._select_batch("TEST.SAMPLE"),
            lambda: self.handler._select_batch("SAMPLE", {"#CUSTOM#x": "TRUE"}),
            lambda: self.handler._select_batch(
                "SAMPLE", columns=("ID; DROP TABLE SAMPLE",)
            ),
            lambda: self.handler._select_batch(
                "SAMPLE", order_by=(("ID", "DESC; DELETE"),)
            ),
            lambda: self.handler._select_batch(
                "SAMPLE", order_by=(("ID", "ASC NULLS FIRST; DELETE"),)
            ),
            lambda: self.handler._update_row(
                "SAMPLE", {"NAME__expr": "NOW()"}, {"ID": 1}
            ),
            lambda: self.handler._select_batch("SAMPLE", limit=-1),
            lambda: self.handler._select_batch("SAMPLE", columns=()),
            lambda: self.handler._select_batch("SAMPLE", columns=("ID", "ID")),
        )
        for operation in operations:
            with self.subTest(operation=operation):
                with self.assertRaises(ValueError):
                    operation()
        self.connect.assert_not_called()

    def test_text_filters_keep_trim_and_case_insensitive_values_bound(self) -> None:
        """Keep text filter values separate from SQL, including hostile operands.

        Args:
            None.
        Returns:
            None.
        """
        value = "'; DROP TABLE SAMPLE; --"
        self.handler._select_batch(
            table="SAMPLE",
            where={"NAME__trim_ne": value, "NAME__ilike": "%.MAT"},
        )
        query, params = self.cursor.execute.call_args.args
        self.assertIn('TRIM("NAME") <> %s AND "NAME" ILIKE %s', query.as_string())
        self.assertEqual(params, (value, "%.MAT"))
        self.assertNotIn(value, query.as_string())

    def test_locking_select_requires_and_preserves_a_caller_transaction(self) -> None:
        """Reject ineffective autocommit locks and leave transaction completion explicit.

        Args:
            None.
        Returns:
            None.
        """
        with self.assertRaises(RuntimeError):
            self.handler._select_batch(table="SAMPLE", for_update=True)
        self.connect.assert_not_called()
        self.handler.begin_transaction()
        self.handler._select_batch(
            table="SAMPLE", where={"ID": 1}, limit=1, for_update=True
        )
        query, params = self.cursor.execute.call_args.args
        self.assertTrue(query.as_string().endswith("LIMIT %s FOR UPDATE"))
        self.assertEqual(params, (1, 1))
        self.assertTrue(self.handler.in_transaction)
        self.connection.commit.assert_not_called()
        self.connection.rollback.assert_not_called()

    def test_ordering_accepts_only_supported_null_placement(self) -> None:
        """Compose the finite ordering vocabulary without accepting arbitrary SQL.

        Args:
            None.
        Returns:
            None.
        """
        for direction in (
            "ASC",
            "DESC",
            "ASC NULLS FIRST",
            "ASC NULLS LAST",
            "DESC NULLS FIRST",
            "DESC NULLS LAST",
        ):
            with self.subTest(direction=direction):
                self.handler._select_batch(
                    table="SAMPLE", order_by=(("NAME", direction.lower()),)
                )
                query = self.cursor.execute.call_args.args[0].as_string()
                self.assertTrue(query.endswith(f'ORDER BY "NAME" {direction}'))

    def test_select_row_returns_one_record_or_none_by_primary_key(self) -> None:
        """Read a primary-key lookup without exposing a list to the subclass.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchall.return_value = [{"ID": 7, "NAME": "one"}]
        self.assertEqual(
            self.handler._select_row("SAMPLE", {"ID": 7}, columns=("ID", "NAME")),
            {"ID": 7, "NAME": "one"},
        )
        query, params = self.cursor.execute.call_args.args
        self.assertEqual(
            query.as_string(),
            'SELECT "ID", "NAME" FROM "TEST"."SAMPLE" WHERE "ID" = %s',
        )
        self.assertEqual(params, (7,))
        self.cursor.fetchall.return_value = []
        self.assertIsNone(self.handler._select_row("SAMPLE", {"ID": 8}))

    def test_row_operations_reject_broad_or_incomplete_keys_before_sql(self) -> None:
        """Prevent row APIs from accepting predicates that can affect many records.

        Args:
            None.
        Returns:
            None.
        """
        for key in (
            {},
            {"NAME": "one"},
            {"ID__in": [1, 2]},
            {"ID": None},
            {"ID": 1, "ACTIVE": True},
        ):
            for operation in (
                lambda: self.handler._select_row("SAMPLE", key),
                lambda: self.handler._update_row("SAMPLE", {"NAME": "new"}, key),
                lambda: self.handler._delete_row("SAMPLE", key),
            ):
                with self.subTest(key=key, operation=operation):
                    with self.assertRaises(ValueError):
                        operation()
        self.connect.assert_not_called()

    def test_composite_primary_keys_require_all_fields_and_keep_declared_order(
        self,
    ) -> None:
        """Use the whole composite key even when input mapping order differs.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.PRIMARY_KEY_SAMPLE = ("ID", "NAME")
        with self.assertRaises(ValueError):
            self.handler._delete_row("SAMPLE", {"ID": 1})
        self.handler._delete_row("SAMPLE", {"NAME": "one", "ID": 1})
        query, params = self.cursor.execute.call_args.args
        self.assertEqual(
            query.as_string(),
            'DELETE FROM "TEST"."SAMPLE" WHERE "ID" = %s AND "NAME" = %s',
        )
        self.assertEqual(params, (1, "one"))

    def test_invalid_primary_key_declarations_are_rejected_before_sql(self) -> None:
        """Keep identity validation and column authorization before any writes.

        Args:
            None.
        Returns:
            None.
        """
        for declaration in ((), ("ID", "ID"), ("UNKNOWN",), ("ID__gt",)):
            self.handler.PRIMARY_KEY_SAMPLE = declaration
            key = {name: 1 for name in declaration}
            for operation in (
                lambda: self.handler._select_row("SAMPLE", key),
                lambda: self.handler._update_row("SAMPLE", {"NAME": "new"}, key),
                lambda: self.handler._delete_row("SAMPLE", key),
            ):
                with self.subTest(declaration=declaration, operation=operation):
                    with self.assertRaises(ValueError):
                        operation()
        self.connect.assert_not_called()

    def test_batch_update_and_delete_allow_multiple_matching_rows(self) -> None:
        """Apply one filtered set operation instead of pretending it is single-row.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.rowcount = 5
        self.assertEqual(
            self.handler._update_batch(
                "SAMPLE", {"ACTIVE": False}, {"ID__in": [1, 2, 3, 4, 5]}
            ),
            5,
        )
        self.assertEqual(self.handler._delete_batch("SAMPLE", {"ACTIVE": False}), 5)
        query, params = self.cursor.execute.call_args.args
        self.assertEqual(
            query.as_string(), 'DELETE FROM "TEST"."SAMPLE" WHERE "ACTIVE" = %s'
        )
        self.assertEqual(params, (False,))
        with self.assertRaises(ValueError):
            self.handler._update_batch("SAMPLE", {"ACTIVE": False}, {})
        with self.assertRaises(ValueError):
            self.handler._delete_batch("SAMPLE", {})

    def test_insert_batch_aligns_values_and_shares_one_atomic_executor(self) -> None:
        """Reuse INSERT construction and keep every chunk in the same transaction.

        Args:
            None.
        Returns:
            None.
        """
        affected = self.handler._insert_batch(
            "SAMPLE",
            [
                {"ID": 1, "NAME": "one"},
                {"NAME": "two", "ID": 2},
            ],
            skip_duplicates=True,
            batch_size=1,
        )
        self.assertEqual(affected, 2)
        first, second = self.cursor.executemany.call_args_list
        self.assertEqual(
            first.args[0].as_string(),
            'INSERT INTO "TEST"."SAMPLE" ("ID", "NAME") VALUES (%s, %s) ON CONFLICT DO NOTHING',
        )
        self.assertEqual(first.args[1], [(1, "one")])
        self.assertEqual(second.args[1], [(2, "two")])
        self.connection.transaction.assert_called_once()

    def test_empty_batches_do_not_connect_and_insert_rejects_heterogeneous_rows(
        self,
    ) -> None:
        """Skip empty work and reject inconsistent INSERT rows before writes.

        Args:
            None.
        Returns:
            None.
        """
        self.assertEqual(self.handler._insert_batch(table="SAMPLE", data=[]), 0)
        self.assertEqual(
            self.handler._upsert_batch(
                table="SAMPLE", data=[], conflict_columns=("ID",)
            ),
            0,
        )
        with self.assertRaises(ValueError):
            self.handler._insert_batch(
                table="SAMPLE", data=[{"ID": 1}, {"ID": 2, "NAME": "two"}]
            )
        self.connect.assert_not_called()

    def test_declared_identifiers_are_quoted_without_custom_name_rules(self) -> None:
        """Keep special characters inside literal identifiers, not executable SQL.

        Args:
            None.
        Returns:
            None.
        """
        field = 'NOTE"; DROP TABLE SAMPLE; --'
        self.handler.VALID_FIELDS_SAMPLE = {"ID", field}
        self.handler.schema = 'TEST"SCHEMA'
        self.handler._select_batch("SAMPLE", columns=(field,))
        query, params = self.cursor.execute.call_args.args
        self.assertEqual(
            query.as_string(),
            'SELECT "NOTE""; DROP TABLE SAMPLE; --" FROM "TEST""SCHEMA"."SAMPLE"',
        )
        self.assertEqual(params, ())

    def test_updated_field_declarations_are_used_without_a_schema_cache(self) -> None:
        """Use an updated handler contract without requiring frozen collections.

        Args:
            None.
        Returns:
            None.
        """
        with self.assertRaises(ValueError):
            self.handler._select_batch("SAMPLE", columns=("NOTE",))
        self.handler.VALID_FIELDS_SAMPLE = {"ID", "NAME", "UPDATED", "ACTIVE", "NOTE"}
        self.handler._select_batch("SAMPLE", columns=("NOTE",))
        query = self.cursor.execute.call_args.args[0]
        self.assertEqual(query.as_string(), 'SELECT "NOTE" FROM "TEST"."SAMPLE"')

    def test_null_ranges_and_empty_membership_share_one_filter_builder(self) -> None:
        """Preserve NULL and empty IN semantics in every CRUD operation.

        Args:
            None.
        Returns:
            None.
        """
        fragment, values = self.handler._build_where_clause(
            {
                "NAME": None,
                "UPDATED__ne": None,
                "ID__between": (2, 5),
                "ACTIVE__in": [],
                "ID__not_in": [],
            },
            ExampleHandler.VALID_FIELDS_SAMPLE,
        )
        self.assertEqual(
            fragment.as_string(),
            ' WHERE "NAME" IS NULL AND "UPDATED" IS NOT NULL AND "ID" BETWEEN %s AND %s AND FALSE AND TRUE',
        )
        self.assertEqual(values, (2, 5))
        self.handler._delete_batch("SAMPLE", {"NAME": None})
        query, params = self.cursor.execute.call_args.args
        self.assertIn('"NAME" IS NULL', query.as_string())
        self.assertEqual(params, ())

    def test_comparison_and_membership_values_keep_placeholder_order(self) -> None:
        """Cover the supported operators without interpolating operands.

        Args:
            None.
        Returns:
            None.
        """
        cases = (
            ("eq", "="),
            ("ne", "<>"),
            ("lt", "<"),
            ("gt", ">"),
            ("lte", "<="),
            ("gte", ">="),
            ("like", "LIKE"),
        )
        for operator, symbol in cases:
            with self.subTest(operator=operator):
                query, params = self.handler._build_where_clause(
                    {f"ID__{operator}": 4}, ExampleHandler.VALID_FIELDS_SAMPLE
                )
                self.assertEqual(query.as_string(), f' WHERE "ID" {symbol} %s')
                self.assertEqual(params, (4,))
        for operator, symbol in (("in", "IN"), ("not_in", "NOT IN")):
            query, params = self.handler._build_where_clause(
                {f"ID__{operator}": [2, 8]}, ExampleHandler.VALID_FIELDS_SAMPLE
            )
            self.assertEqual(query.as_string(), f' WHERE "ID" {symbol} (%s, %s)')
            self.assertEqual(params, (2, 8))

    def test_invalid_filter_operands_are_not_silently_ignored(self) -> None:
        """Reject malformed filter shapes before executing anything.

        Args:
            None.
        Returns:
            None.
        """
        for filters in (
            {"ID__between": [1]},
            {"ID__between": [1, None]},
            {"ID__in": "1,2"},
            {"ID__gt": None},
            {"ID__other": 2},
            {"NAME__trim_ne": None},
            {"NAME__trim_ne": 1},
            {"NAME__trim_ne": sql.SQL("CURRENT_USER")},
        ):
            with self.subTest(filters=filters):
                with self.assertRaises(ValueError):
                    self.handler._select_batch("SAMPLE", filters)
        self.connect.assert_not_called()

    def test_insert_returns_explicit_generated_columns(self) -> None:
        """Use RETURNING instead of MySQL lastrowid or sequence arithmetic.

        Args:
            None.
        Returns:
            None.
        """
        self.cursor.fetchone.return_value = {"ID": 17}
        result = self.handler._insert_row(
            "SAMPLE", {"NAME": "test"}, returning=("ID",), skip_duplicates=True
        )
        query, params = self.cursor.execute.call_args.args
        self.assertEqual(result, {"ID": 17})
        self.assertIn('ON CONFLICT DO NOTHING RETURNING "ID"', query.as_string())
        self.assertEqual(params, ("test",))
        self.connection.commit.assert_not_called()
        self.cursor.fetchone.return_value = None
        self.assertIsNone(
            self.handler._insert_row(
                "SAMPLE", {"NAME": "test"}, returning=("ID",), skip_duplicates=True
            )
        )

    def test_update_aligns_set_and_filter_values_and_requires_filters(self) -> None:
        """Protect full-table writes and keep bound values in SQL order.

        Args:
            None.
        Returns:
            None.
        """
        self.assertEqual(
            self.handler._update_row(
                "SAMPLE", {"NAME": "new"}, {"ID": 4}, touch_field="UPDATED"
            ),
            1,
        )
        query, params = self.cursor.execute.call_args.args
        self.assertIn(
            'SET "NAME" = %s, "UPDATED" = CURRENT_TIMESTAMP WHERE "ID" = %s',
            query.as_string(),
        )
        self.assertEqual(params, ("new", 4))
        for operation in (
            lambda: self.handler._update_row("SAMPLE", {"NAME": "new"}, {}),
            lambda: self.handler._delete_row("SAMPLE", {}),
            lambda: self.handler._insert_row("SAMPLE", {}),
        ):
            with self.assertRaises(ValueError):
                operation()

    def test_upsert_keeps_conflict_keys_and_handles_key_only_rows(self) -> None:
        """Require explicit conflict keys and support DO NOTHING for key-only rows.

        Args:
            None.
        Returns:
            None.
        """
        self.handler._upsert_row(
            "SAMPLE", {"ID": 4, "NAME": "new"}, conflict_columns=("ID",)
        )
        query = self.cursor.executemany.call_args.args[0].as_string()
        self.assertIn(
            'ON CONFLICT ("ID") DO UPDATE SET "NAME" = EXCLUDED."NAME"', query
        )
        self.handler._upsert_row("SAMPLE", {"ID": 4}, conflict_columns=("ID",))
        self.assertTrue(
            self.cursor.executemany.call_args.args[0].as_string().endswith("DO NOTHING")
        )
        for keys in ((), ("NAME",)):
            with self.assertRaises(ValueError):
                self.handler._upsert_row("SAMPLE", {"ID": 4}, conflict_columns=keys)

    def test_single_upsert_preserves_caller_transaction_on_success_and_failure(
        self,
    ) -> None:
        """Keep transaction ownership when single-row UPSERT uses batch execution.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.begin_transaction()
        self.assertEqual(
            self.handler._upsert_row("SAMPLE", {"ID": 1}, conflict_columns=("ID",)), 1
        )
        self.cursor.executemany.side_effect = psycopg.errors.CheckViolation(
            "private row"
        )
        with self.assertRaises(psycopg.errors.CheckViolation):
            self.handler._upsert_row("SAMPLE", {"ID": 2}, conflict_columns=("ID",))
        self.assertTrue(self.handler.in_transaction)
        self.connection.transaction.assert_not_called()
        self.connection.commit.assert_not_called()
        self.connection.rollback.assert_not_called()

    def test_batch_aligns_mapping_order_and_uses_one_transaction(self) -> None:
        """Keep all chunks atomic and bind values by the first row's columns.

        Args:
            None.
        Returns:
            None.
        """
        result = self.handler._upsert_batch(
            "SAMPLE",
            [
                {"ID": 1, "NAME": "one"},
                {"NAME": "two", "ID": 2},
            ],
            conflict_columns=("ID",),
            batch_size=1,
        )
        self.assertEqual(result, 2)
        self.assertEqual(
            [call.args[1] for call in self.cursor.executemany.call_args_list],
            [[(1, "one")], [(2, "two")]],
        )
        self.connection.transaction.assert_called_once_with()
        self.connection.commit.assert_not_called()

    def test_failed_batch_exits_its_transaction_with_the_original_error(self) -> None:
        """Let Psycopg roll back the entire standalone batch on failure.

        Args:
            None.
        Returns:
            None.
        """
        error = psycopg.errors.UniqueViolation("sensitive row detail")
        self.cursor.executemany.side_effect = [None, error]
        with self.assertRaises(psycopg.errors.UniqueViolation):
            self.handler._upsert_batch(
                table="SAMPLE",
                data=[{"ID": 1}, {"ID": 2}],
                conflict_columns=("ID",),
                batch_size=1,
            )
        exit_call = self.connection.transaction.return_value.__exit__.call_args.args
        self.assertIs(exit_call[1], error)
        self.assertNotIn("sensitive", str(self.log.error_event.call_args))

    def test_batch_validates_all_rows_before_writing(self) -> None:
        """Reject heterogeneous rows and invalid chunk sizes before SQL.

        Args:
            None.
        Returns:
            None.
        """
        with self.assertRaises(ValueError):
            self.handler._upsert_batch(
                table="SAMPLE",
                data=[{"ID": 1}, {"NAME": "x"}],
                conflict_columns=("ID",),
            )
        for size in (0, -1):
            with self.assertRaises(ValueError):
                self.handler._execute_batch(
                    sql.SQL("SELECT %s"), [(1,)], batch_size=size
                )
        self.connect.assert_not_called()

    def test_explicit_transaction_stays_owned_by_caller_across_helpers(self) -> None:
        """Keep writes and the session owned until the caller commits.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.reuse_connection = False
        self.handler.begin_transaction()
        self.handler._update_row("SAMPLE", {"NAME": "test"}, {"ID": 1})
        self.handler._upsert_batch(
            table="SAMPLE", data=[{"ID": 2}], conflict_columns=("ID",)
        )
        self.assertTrue(self.handler.in_transaction)
        self.connection.commit.assert_not_called()
        self.connection.rollback.assert_not_called()
        self.connection.close.assert_not_called()
        self.connection.transaction.assert_not_called()
        self.handler.commit()
        self.connection.commit.assert_called_once_with()
        self.connection.close.assert_called_once_with()
        self.assertFalse(self.handler.in_transaction)

    def test_failed_explicit_transaction_requires_caller_rollback(self) -> None:
        """Retain transaction state after SQL errors and reject aborted commits.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.begin_transaction()
        self.cursor.execute.side_effect = psycopg.errors.CheckViolation(
            "private detail"
        )
        with self.assertRaises(psycopg.errors.CheckViolation):
            self.handler._delete_row("SAMPLE", {"ID": 1})
        self.connection.rollback.assert_not_called()
        self.connection.info.transaction_status = TransactionStatus.INERROR
        with self.assertRaises(RuntimeError):
            self.handler.commit()
        self.assertTrue(self.handler.in_transaction)
        self.handler.rollback()
        self.assertFalse(self.handler.in_transaction)
        self.assertTrue(self.connection.autocommit)

    def test_transaction_misuse_is_explicit(self) -> None:
        """Reject nested transactions and completion without an owned transaction.

        Args:
            None.
        Returns:
            None.
        """
        for operation in (self.handler.commit, self.handler.rollback):
            with self.assertRaises(RuntimeError):
                operation()
        self.handler.begin_transaction()
        with self.assertRaises(RuntimeError):
            self.handler.begin_transaction()

    def test_lost_session_is_not_reconnected_or_replayed(self) -> None:
        """Require explicit cleanup when a session and its locks may be lost.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.begin_transaction()
        self.connection.closed = True
        with self.assertRaises(psycopg.OperationalError):
            self.handler._select_batch("SAMPLE")
        self.assertTrue(self.handler.in_transaction)
        self.connect.assert_called_once()
        self.cursor.execute.assert_not_called()
        self.handler.close()
        self.assertFalse(self.handler.in_transaction)

    def test_failed_commit_retains_state_without_retry(self) -> None:
        """Expose an uncertain commit rather than reopening or replaying it.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.begin_transaction()
        self.connection.commit.side_effect = psycopg.OperationalError("connection lost")
        with self.assertRaises(psycopg.OperationalError):
            self.handler.commit()
        self.assertTrue(self.handler.in_transaction)
        self.connection.commit.assert_called_once()
        self.connection.rollback.assert_not_called()
        self.connect.assert_called_once()

    def test_nonreused_sessions_close_after_reads_and_errors(self) -> None:
        """Release cursors and idle sessions on both successful and failed reads.

        Args:
            None.
        Returns:
            None.
        """
        self.handler.reuse_connection = False
        self.handler._select_batch("SAMPLE")
        self.connection.close.assert_called_once()
        self.assertIsNone(self.handler.db_connection)
        self.cursor.execute.side_effect = psycopg.errors.SyntaxError("private SQL")
        with self.assertRaises(psycopg.errors.SyntaxError):
            self.handler._select_batch("SAMPLE")
        self.assertEqual(self.connection.close.call_count, 2)
        self.assertIsNone(self.handler.db_connection)

    def test_trusted_queries_keep_values_separate_from_sql(self) -> None:
        """Bind parameters through protected query operations with typed callers.

        Args:
            None.
        Returns:
            None.
        """
        query = sql.SQL('SELECT "ID" FROM "TEST"."SAMPLE" WHERE "NAME" = %s')
        value = "private value with ' quote"
        self.cursor.fetchall.return_value = [{"ID": 1}]
        self.assertEqual(self.handler._select_raw(query, (value,)), [{"ID": 1}])
        self.cursor.execute.assert_called_once_with(query, (value,))
        self.assertNotIn(value, query.as_string())


if __name__ == "__main__":
    unittest.main()
