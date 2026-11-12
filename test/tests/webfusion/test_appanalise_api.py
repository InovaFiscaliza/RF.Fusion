"""Validate the MATLAB compatibility contract inside the WebFusion container."""

from datetime import datetime
from decimal import Decimal
import importlib.util
from pathlib import Path
import sys
import sqlite3
import types
import unittest
from unittest.mock import MagicMock, patch

from flask import Flask


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = ROOT / "src" / "webfusion" / "api" / "appAnalise_api"


def load_api() -> tuple:
    """Import an isolated API package without production credentials or connections.

    Args:
        None.

    Returns:
        Routes, DB handler, filters and service modules (tuple).
    """
    stub = types.ModuleType("db.connection")
    stub.get_connection_bpdata = MagicMock()
    stub.get_connection_rfdata = MagicMock()
    stub.get_connection_summary = MagicMock()
    name = "appanalise_contract_test"
    spec = importlib.util.spec_from_file_location(
        name, PACKAGE / "__init__.py", submodule_search_locations=[str(PACKAGE)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    with patch.dict(sys.modules, {"db.connection": stub}):
        spec.loader.exec_module(module)
        loaded = (sys.modules[f"{name}.routes"], sys.modules["db.appanalise"],
                  sys.modules[f"{name}.filters"], sys.modules[f"{name}.service"])
    return loaded


routes, database, filters_module, service = load_api()
Filters = filters_module.Filters
TableResult = database.TableResult
PREFIX = routes.k.API_PREFIX


class FilterTests(unittest.TestCase):
    """Check untrusted HTTP inputs and temporal semantics."""

    def test_dates_cover_whole_days_without_timezone_shift(self) -> None:
        """Verify dates cover whole days without timezone shift.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        filters = filters_module.parse_filters({
            "startDate": "2026-09-01T17:32:00", "endDate": "2026-09-11",
            "districtId": "1,2", "equipmentId": "12",
        })
        self.assertEqual(filters.start_date, datetime(2026, 9, 1))
        self.assertEqual(filters.end_date, datetime(2026, 9, 11, 23, 59, 59))
        self.assertEqual(filters.district_ids, (1, 2))

    def test_invalid_values_are_rejected_before_querying(self) -> None:
        """Verify invalid values are rejected before querying.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        for value in (
            {"sql": 'SELECT * FROM "HOST"'}, {"equipmentId": "1 OR 1=1"},
            {"equipmentId": True}, {"siteId": 1.5}, {"page": 0},
            {"pageSize": 1001}, {"districtId": [1, None]},
            {"districtId": "1,,2"}, {"districtId": list(range(1, 1002))},
            {"freqStart": "NaN"}, {"freqEnd": float("inf")},
            {"startDate": "2026-02-30"}, {"endDate": "2026-09-11T01:00:00Z"},
            {"description": {}}, {"stateCode": ["SP"]},
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                filters_module.parse_filters(value)

    def test_ids_do_not_pass_through_float(self) -> None:
        """Verify ids do not pass through float.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.assertEqual(filters_module.parse_filters({"siteId": "9007199254740993"}).site_id,
                         9007199254740993)


class QueryTests(unittest.TestCase):
    """Check legacy query branches, binding and cleanup without live data."""

    def setUp(self) -> None:
        """Initialize isolated test fixtures.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.db = database.AppAnaliseDB()
        self.fetch = patch.object(self.db, "_fetch").start()
        self.addCleanup(patch.stopall)
        self.fetch.return_value = TableResult([], [])

    def test_file_metadata_uses_all_spectra_for_selected_ids(self) -> None:
        """Preserve full-file aggregates independently of the original filters.

        Args:
            None.

        Returns:
            None. Checks bound IDs, complete joins and all public aggregates.
        """
        self.fetch.return_value = TableResult(["ID_FILE"], [{"ID_FILE": 20}, {"ID_FILE": 10}])
        result = self.db.get_spectrum_files_by_ids((10, 20))
        schema, sql, params = self.fetch.call_args.args
        self.assertEqual(schema, "spectrum")
        self.assertEqual(params, [10, 20])
        self.assertIn('WHERE repos."ID_FILE" IN (%s, %s)', sql)
        self.assertNotIn("LIMIT", sql)
        for aggregate in (
            'MIN(f."DT_TIME_START")', 'MAX(f."DT_TIME_END")',
            'MIN(f."NU_FREQ_START")', 'MAX(f."NU_FREQ_END")',
            'COUNT(DISTINCT f."ID_SPECTRUM")', 'COUNT(DISTINCT s."ID_SITE")',
            'AS "LOCALITY_LABELS"', 'AS "EQUIPMENT_LABELS"',
        ):
            self.assertIn(aggregate, sql)
        self.assertEqual([row["ID_FILE"] for row in result.rows], [10, 20])

    def test_direct_file_page_preserves_offset_and_lookahead(self) -> None:
        """Slice the ordered index with the existing extra-row contract.

        Args:
            None.

        Returns:
            None. Checks arbitrary page sizes, offsets and empty pages.
        """
        for page, size, expected in ((1, 2, (0, 1, 2)), (2, 2, (2, 3, 4)),
                                     (3, 2, (4,)), (4, 2, ())):
            with patch.object(self.db, "get_spectrum_file_index", return_value=tuple(range(5))), \
                    patch.object(self.db, "get_spectrum_files_by_ids") as metadata:
                self.db.get_spectrum_file_data(Filters(page=page, page_size=size))
                metadata.assert_called_once_with(expected)

    def test_empty_file_page_retains_columns_without_query(self) -> None:
        """Return the public empty-table schema without SQL for empty IDs.

        Args:
            None.

        Returns:
            None. Checks empty-page compatibility and absence of database work.
        """
        result = self.db.get_spectrum_files_by_ids(())
        self.assertEqual(result.rows, [])
        self.assertEqual(result.columns, [
            "ID_FILE", "NA_PATH", "NA_FILE", "NA_EXTENSION", "VL_FILE_SIZE_KB",
            "DT_TIME_START", "DT_TIME_END", "NU_FREQ_START", "NU_FREQ_END",
            "NU_SPECTRA", "LOCALITY_COUNT", "LOCALITY_LABELS", "EQUIPMENT_LABELS",
        ])
        self.fetch.assert_not_called()

    def test_description_wildcards_and_negative_frequency(self) -> None:
        """Verify description wildcards and negative frequency.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        for description, expected in (("abc", "%abc%"), ("a_c", "a_c"), ("a%", "a%")):
            sql, params = database._spectrum_conditions(Filters(description=description, freq_start=-1))
            self.assertNotIn('f."NU_FREQ_START" >=', sql)
            self.assertEqual(params[-1], expected)

    def test_equipment_fallback_preserves_legacy_district_exception(self) -> None:
        """Verify equipment fallback preserves legacy district exception.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.fetch.side_effect = [TableResult([], []), TableResult(["ID_EQUIPMENT"], [{"ID_EQUIPMENT": 1}])]
        self.db.get_spectrum_equipments(Filters(state_code="SP", site_id=8, district_ids=(2, 3)))
        summary, fact = self.fetch.call_args_list
        self.assertIn('"FK_DISTRICT" IN (%s, %s)', summary.args[1])
        self.assertEqual(summary.args[2], ["SP", 8, 2, 3])
        self.assertNotIn("DISTRICT", fact.args[1])
        self.assertEqual(fact.args[2], ["SP", 8])

    def test_populated_summary_does_not_run_fallback(self) -> None:
        """Verify populated summary does not run fallback.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.fetch.return_value = TableResult(["value"], [{"value": 1}])
        for method in (self.db.get_spectrum_equipments, self.db.get_spectrum_states,
                       self.db.get_spectrum_localities):
            self.fetch.reset_mock()
            method(Filters(equipment_id=12))
            self.fetch.assert_called_once()
            self.assertEqual(self.fetch.call_args.args[0], "summary")

    def test_summary_failure_is_not_treated_as_empty(self) -> None:
        """Verify summary failure is not treated as empty.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.fetch.side_effect = RuntimeError("offline")
        with self.assertRaises(RuntimeError):
            self.db.get_spectrum_states(Filters())
        self.fetch.assert_called_once()

    def test_state_fallback_scope(self) -> None:
        """Verify state fallback scope.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.db.get_spectrum_states(Filters(equipment_id=12, site_id=8, state_code="SP", district_ids=(4,)))
        self.assertEqual(self.fetch.call_count, 2)
        for call in self.fetch.call_args_list:
            self.assertEqual(call.args[2], [12, 8])
        self.assertIn('st."LC_STATE" IS NOT NULL', self.fetch.call_args.args[1])

    def test_unscoped_localities_do_not_open_database(self) -> None:
        """Verify unscoped localities do not open database.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        result = self.db.get_spectrum_localities(Filters(site_id=8, district_ids=(2,)))
        self.assertEqual(result.rows, [])
        self.assertEqual(result.columns, database.k.LOCALITY_COLUMNS)
        self.fetch.assert_not_called()

    def test_locality_summary_and_fact_have_different_scopes(self) -> None:
        """Verify locality summary and fact have different scopes.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.db.get_spectrum_localities(Filters(equipment_id=12, site_id=8, district_ids=(4,)))
        summary, fact = self.fetch.call_args_list
        self.assertEqual(summary.args[2], [12])
        self.assertEqual(fact.args[2], [12, 4])
        self.assertNotIn('"f"."FK_SITE" = %s', fact.args[1])
        self.assertIn('COUNT(*) AS "SPECTRUM_COUNT"', fact.args[1])
        self.assertIn('SUM(sm."NU_SPECTRUM_COUNT")', summary.args[1])

    def test_spectrum_filter_selects_fact_even_for_negative_bound(self) -> None:
        """Verify spectrum filter selects fact even for negative bound.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        for value in ({"freq_start": -1}, {"freq_end": 100}, {"description": "abc"},
                      {"start_date": datetime(2026, 9, 1)}, {"end_date": datetime(2026, 9, 1)}):
            self.fetch.reset_mock()
            self.db.get_spectrum_localities(Filters(equipment_id=12, **value))
            self.fetch.assert_called_once()
            self.assertEqual(self.fetch.call_args.args[0], "spectrum")

    def test_file_count_preserves_conditional_state_join(self) -> None:
        """Verify file count preserves conditional state join.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.fetch.return_value = TableResult(["TOTAL_COUNT"], [{"TOTAL_COUNT": 9}])
        for state in (None, "SP"):
            self.assertEqual(self.db.get_spectrum_file_data_count(Filters(state_code=state)), 9)
            _, sql, params = self.fetch.call_args.args
            self.assertEqual('JOIN "RFDATA"."DIM_SITE_STATE"' in sql, state is not None)
            self.assertNotIn("DIM_SPECTRUM_EQUIPMENT", sql)
            self.assertNotIn("LIMIT", sql)
            self.assertEqual(params, ["reposfi", state] if state else ["reposfi"])

    def test_file_details_are_complete_and_restricted_to_reposfi(self) -> None:
        """Verify file details are complete and restricted to reposfi.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.db.get_spectra_by_file_id(77)
        _, sql, params = self.fetch.call_args.args
        self.assertEqual(params, ["reposfi", 77])
        self.assertIn('f."FK_SITE" AS "ID_SITE"', sql)
        self.assertIn('st."LC_STATE" AS "STATE_CODE"', sql)
        self.assertIn('ORDER BY f."DT_TIME_START" DESC NULLS LAST, f."ID_SPECTRUM" DESC', sql)
        self.assertEqual(sql.count("%s"), 2)
        self.assertNotIn('f."DT_TIME_END" >=', sql)
        self.assertNotIn('f."DT_TIME_START" <=', sql)
        self.assertNotIn("LIMIT", sql)

    def test_fixed_queries_preserve_contracts(self) -> None:
        """Verify fixed queries preserve contracts.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        for method, table, fields in (
            (self.db.get_station_summary, '"BPDATA"."HOST"', ['"IS_OFFLINE_SNAPSHOT" ASC', '"VL_LATITUDE" IS NOT NULL']),
        ):
            method()
            sql = self.fetch.call_args.args[1]
            self.assertIn(table, sql)
            for field in fields:
                self.assertIn(field, sql)
        self.db.get_host_stats(12)
        self.assertEqual(self.fetch.call_args.args[0], "host")
        self.assertEqual(self.fetch.call_args.args[2], [0, 1, 0, 0, 1, 12, 12])
        self.assertNotIn("PASSWORD", self.fetch.call_args.args[1])

    def test_host_counters_retain_history_predicates_and_kb_precision(self) -> None:
        """Verify the removed counters use their original history definitions.

        Args:
            None.

        Returns:
            None. Assertions detect changed status predicates or rounded volume.
        """
        self.db.get_host_stats(12)
        _, sql, params = self.fetch.call_args.args
        self.assertIn('SUM(CASE WHEN "NU_STATUS_DISCOVERY" = %s THEN 1 ELSE 0 END)::numeric AS "NU_HOST_FILES"', sql)
        self.assertIn('SUM(CASE WHEN "NU_STATUS_BACKUP" = %s THEN 1 ELSE 0 END)::numeric AS "NU_PENDING_FILE_BACKUP_TASKS"', sql)
        self.assertIn('SUM(CASE WHEN "NU_STATUS_BACKUP" = %s THEN 1 ELSE 0 END)::numeric AS "NU_DONE_FILE_BACKUP_TASKS"', sql)
        self.assertIn('"NU_STATUS_DISCOVERY" = %s AND "NU_STATUS_BACKUP" = %s', sql)
        self.assertIn('THEN "VL_FILE_SIZE_KB_HOST" ELSE 0 END', sql)
        self.assertIn('"BPDATA"."FILE_TASK_HISTORY" WHERE "FK_HOST" = %s', sql)
        self.assertIn('COALESCE(stats."NU_HOST_FILES", 0)', sql)
        self.assertNotIn("ROUND", sql)
        self.assertEqual(params, [0, 1, 0, 0, 1, 12, 12])


class ConnectionTests(unittest.TestCase):
    """Check resource release on query failures and empty table metadata."""

    def test_cleanup_and_empty_columns(self) -> None:
        """Verify cleanup and empty columns.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        for error in (None, RuntimeError("query failed")):
            connection = MagicMock()
            cursor = connection.cursor.return_value.__enter__.return_value
            cursor.description = [("ID_SITE",), ("SITE_LABEL",)]
            cursor.fetchall.return_value = []
            cursor.execute.side_effect = error
            with patch.object(database, "get_connection_summary", return_value=connection):
                if error:
                    with self.assertRaises(RuntimeError):
                        database.AppAnaliseDB().get_station_summary()
                else:
                    result = database.AppAnaliseDB().get_station_summary()
                    self.assertEqual(result.columns, ["ID_SITE", "SITE_LABEL"])
                    self.assertEqual(result.rows, [])
            connection.close.assert_called_once()
            connection.cursor.return_value.__exit__.assert_called_once()

    def test_catalog_and_count_with_shared_files_and_missing_dimensions(self) -> None:
        """Verify optimized membership and count queries on relational fixtures.

        Args:
            None.

        Returns:
            None. Checks duplicates, repository scope and optional geography.
        """
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.row_factory = sqlite3.Row
        connection.executescript('\n            ATTACH DATABASE \':memory:\' AS "RFDATA";\n            CREATE TABLE "RFDATA"."DIM_SPECTRUM_EQUIPMENT" ("ID_EQUIPMENT" INTEGER PRIMARY KEY, "NA_EQUIPMENT" TEXT);\n            INSERT INTO "RFDATA"."DIM_SPECTRUM_EQUIPMENT" VALUES (1, \'A\'), (2, \'A\'), (3, NULL), (4, \'Unused\');\n            CREATE TABLE "RFDATA"."DIM_SITE_STATE" ("ID_STATE" INTEGER PRIMARY KEY, "LC_STATE" TEXT);\n            INSERT INTO "RFDATA"."DIM_SITE_STATE" VALUES (10, \'SP\'), (20, \'RJ\');\n            CREATE TABLE "RFDATA"."DIM_SPECTRUM_SITE" ("ID_SITE" INTEGER PRIMARY KEY, "FK_STATE" INTEGER, "FK_DISTRICT" INTEGER);\n            INSERT INTO "RFDATA"."DIM_SPECTRUM_SITE" VALUES (1, 10, 100), (2, 20, 200), (3, NULL, 100);\n            CREATE TABLE "RFDATA"."FACT_SPECTRUM" ("ID_SPECTRUM" INTEGER PRIMARY KEY, "FK_EQUIPMENT" INTEGER, "FK_SITE" INTEGER);\n            INSERT INTO "RFDATA"."FACT_SPECTRUM" VALUES (1, 1, 1), (2, 1, 1), (3, 2, 2), (4, 3, 3), (5, 1, 99);\n            CREATE TABLE "RFDATA"."DIM_SPECTRUM_FILE" ("ID_FILE" INTEGER PRIMARY KEY, "NA_VOLUME" TEXT);\n            INSERT INTO "RFDATA"."DIM_SPECTRUM_FILE" VALUES (11, \'reposfi\'), (12, \'reposfi\'), (13, \'other\'), (14, \'reposfi\');\n            CREATE TABLE "RFDATA"."BRIDGE_SPECTRUM_FILE" ("FK_SPECTRUM" INTEGER, "FK_FILE" INTEGER);\n            INSERT INTO "RFDATA"."BRIDGE_SPECTRUM_FILE" VALUES (1, 11), (2, 11), (3, 11), (4, 12), (1, 13), (5, 14);\n        ')
        db = database.AppAnaliseDB()
        for query_filters, expected_count, expected_equipment in (
            (Filters(), 3, {1, 2, 3}),
            (Filters(state_code="SP"), 1, {1}),
            (Filters(state_code="RJ"), 1, {2}),
            (Filters(site_id=99), 1, {1}),
            (Filters(district_ids=(100,)), 2, {1, 2, 3}),
            (Filters(district_ids=(200,)), 1, {1, 2, 3}),
            (Filters(district_ids=(100,), state_code="RJ"), 0, {2}),
        ):
            with self.subTest(filters=query_filters):
                with patch.object(db, "_fetch", return_value=TableResult(["TOTAL_COUNT"], [{"TOTAL_COUNT": 0}])) as fetch:
                    db.get_spectrum_file_data_count(query_filters)
                _, sql, params = fetch.call_args.args
                row = connection.execute(sql.replace("%s", "?"), params).fetchone()
                self.assertEqual(row["TOTAL_COUNT"], expected_count)
                with patch.object(db, "_fetch", return_value=TableResult([], [])) as fetch:
                    db.get_spectrum_equipments(query_filters)
                _, sql, params = fetch.call_args.args
                rows = connection.execute(sql.replace("%s", "?"), params).fetchall()
                self.assertEqual({row["ID_EQUIPMENT"] for row in rows}, expected_equipment)
                self.assertEqual(len(rows), len(expected_equipment))


class SerializationTests(unittest.TestCase):
    """Cover the table serializer independently of the shared map API."""

    def test_table_serialization_preserves_dates_numbers_and_nulls(self) -> None:
        """Verify native query values retain their documented JSON meaning.

        Args:
            None.

        Returns:
            None. Assertions cover ISO dates, decimals, nulls and integer precision.
        """
        columns = ["DATE", "VALUE", "MISSING", "ID", "FLAG"]
        rows = [{"DATE": datetime(2026, 9, 11), "VALUE": Decimal("1.25"),
                 "MISSING": None, "ID": 9007199254740993, "FLAG": True}]
        payload = service.table_payload(TableResult(columns, rows))
        self.assertEqual(payload, {"columns": columns, "rows": [{
            "DATE": "2026-09-11T00:00:00", "VALUE": 1.25,
            "MISSING": None, "ID": 9007199254740993, "FLAG": True,
        }]})
        self.assertEqual(service.json_value((float("nan"), float("inf"))), [None, None])
        with self.assertRaises(TypeError):
            service.json_value(object())


class FileIndexTests(unittest.TestCase):
    """Protect filter materialization, full-file ordering and resource cleanup."""

    def test_index_materializes_filters_and_streams_complete_order(self) -> None:
        """Check bound filters and complete dates. Args: None. Returns: None."""
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.__iter__.return_value = iter([{"ID_FILE": 30}, {"ID_FILE": 10}])
        query = Filters(equipment_id=12, site_id=8, district_ids=(1, 2), state_code="SP",
                        start_date=datetime(2026, 9, 1), end_date=datetime(2026, 9, 30),
                        freq_start=100, freq_end=200, description="O'Brien", page=9, page_size=1000)
        with patch.object(database, "get_connection_rfdata", return_value=connection):
            self.assertEqual(database.AppAnaliseDB().get_spectrum_file_index(query), (30, 10))
        materialize, order = cursor.execute.call_args_list
        filter_sql, values = materialize.args
        order_sql, volume = order.args
        self.assertIn("CREATE TEMPORARY TABLE", filter_sql)
        self.assertNotIn("O'Brien", filter_sql)
        self.assertEqual(values, [12, 8, "SP", 1, 2, query.start_date, query.end_date,
                                  100, 200, "%O'Brien%"])
        self.assertEqual(volume, ["reposfi"])
        for predicate in ('"f"."FK_EQUIPMENT" = %s', '"f"."FK_SITE" = %s', '"st"."LC_STATE" = %s',
                          '"s"."FK_DISTRICT" IN (%s, %s)', 'f."DT_TIME_END" >= %s',
                          'f."DT_TIME_START" <= %s', 'f."NU_FREQ_START" >= %s',
                          'f."NU_FREQ_END" <= %s', "LIKE UPPER(%s)"):
            self.assertIn(predicate, filter_sql)
            self.assertNotIn(predicate, order_sql)
        self.assertIn('b."FK_FILE" = matched_files."ID_FILE"', order_sql)
        self.assertIn('f."ID_SPECTRUM" = b."FK_SPECTRUM"', order_sql)
        self.assertIn('e."ID_EQUIPMENT" = f."FK_EQUIPMENT"', order_sql)
        self.assertIn('s."ID_SITE" = f."FK_SITE"', order_sql)
        self.assertIn('ORDER BY MIN(f."DT_TIME_START") DESC NULLS LAST, matched_files."ID_FILE" DESC', order_sql)
        self.assertNotIn("LIMIT", order_sql)
        cursor.fetchall.assert_not_called()
        connection.close.assert_called_once()

    def test_index_releases_connection_after_each_failure_stage(self) -> None:
        """Release temporary resources on failures. Args: None. Returns: None."""
        for stage in ("materialize", "order", "stream"):
            with self.subTest(stage=stage):
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                failure = RuntimeError(stage)
                if stage == "stream":
                    cursor.__iter__.side_effect = failure
                else:
                    cursor.execute.side_effect = [failure] if stage == "materialize" else [None, failure]
                with patch.object(database, "get_connection_rfdata", return_value=connection):
                    with self.assertRaisesRegex(RuntimeError, stage):
                        database.AppAnaliseDB().get_spectrum_file_index(Filters())
                connection.close.assert_called_once()
                self.assertEqual(connection.cursor.return_value.__exit__.call_count, 1 if stage == "materialize" else 2)


class FileServiceTests(unittest.TestCase):
    """Check cross-page reuse without merging count and listing contracts."""

    def setUp(self) -> None:
        """Isolate process-cache state. Args: None. Returns: None."""
        self.db = MagicMock(spec=database.AppAnaliseDB)
        cache_patch = patch.object(service, "_FILE_QUERY_CACHE", service.FileQueryCache())
        cache_patch.start()
        self.addCleanup(cache_patch.stop)
        self.db.get_spectrum_files_by_ids.side_effect = lambda ids: TableResult(
            ["ID_FILE"], [{"ID_FILE": value} for value in ids],
        )

    def test_pages_and_sizes_share_index_but_filters_do_not(self) -> None:
        """Reuse normalized filters across pages. Args: None. Returns: None."""
        self.db.get_spectrum_file_index.return_value = (30, 20, 10, 5, 1)
        first = service.get_file_page(Filters(district_ids=(2, 1, 2), page_size=2), self.db)
        second = service.get_file_page(Filters(district_ids=(1, 2), page=2, page_size=2), self.db)
        other_size = service.get_file_page(Filters(district_ids=(2, 1), page=2, page_size=3), self.db)
        self.assertEqual([r["ID_FILE"] for r in first.rows], [30, 20, 10])
        self.assertEqual([r["ID_FILE"] for r in second.rows], [10, 5, 1])
        self.assertEqual([r["ID_FILE"] for r in other_size.rows], [5, 1])
        self.db.get_spectrum_file_index.assert_called_once_with(
            Filters(district_ids=(1, 2), page=1, page_size=1),
        )
        service.get_file_page(Filters(state_code="SP", page_size=2), self.db)
        self.assertEqual(self.db.get_spectrum_file_index.call_count, 2)

    def test_count_uses_independent_join_scope_and_cache(self) -> None:
        """Preserve legitimate count/list differences. Args: None. Returns: None."""
        self.db.get_spectrum_file_data_count.return_value = 9
        self.db.get_spectrum_file_index.return_value = (2, 1)
        service.get_file_page(Filters(), self.db)
        self.assertEqual(service.get_file_count(Filters(page=2, page_size=1000), self.db), 9)
        self.assertEqual(service.get_file_count(Filters(page_size=1), self.db), 9)
        self.db.get_spectrum_file_data_count.assert_called_once_with(Filters(page_size=1))

    def test_metadata_failure_does_not_become_empty_or_poison_index(self) -> None:
        """Retry failed metadata with the valid index. Args: None. Returns: None."""
        self.db.get_spectrum_file_index.return_value = (2, 1)
        self.db.get_spectrum_files_by_ids.side_effect = [RuntimeError("metadata"), TableResult([], [])]
        with self.assertRaisesRegex(RuntimeError, "metadata"):
            service.get_file_page(Filters(), self.db)
        service.get_file_page(Filters(), self.db)
        self.assertEqual(self.db.get_spectrum_file_index.call_count, 1)
        self.assertEqual(self.db.get_spectrum_files_by_ids.call_count, 2)

    def test_reused_index_does_not_freeze_page_metadata(self) -> None:
        """Fetch current metadata on repeated pages. Args: None. Returns: None."""
        self.db.get_spectrum_file_index.return_value = (2,)
        self.db.get_spectrum_files_by_ids.side_effect = [
            TableResult(["ID_FILE", "NU_SPECTRA"], [{"ID_FILE": 2, "NU_SPECTRA": 1}]),
            TableResult(["ID_FILE", "NU_SPECTRA"], [{"ID_FILE": 2, "NU_SPECTRA": 3}]),
        ]
        first = service.get_file_page(Filters(), self.db)
        second = service.get_file_page(Filters(), self.db)
        self.assertEqual(first.rows[0]["NU_SPECTRA"], 1)
        self.assertEqual(second.rows[0]["NU_SPECTRA"], 3)
        self.db.get_spectrum_file_index.assert_called_once()


class FileCacheTests(unittest.TestCase):
    """Exercise expiry, memory limits and concurrent success/failure recovery."""

    def test_expiry_starts_when_load_finishes_and_empty_results_are_cached(self) -> None:
        """Expire fixed lifetimes after loading. Args: None. Returns: None."""
        cache = service.FileQueryCache()
        with patch("time.monotonic", return_value=10) as clock:
            def load():
                """Advance a fake long build. Args: None. Returns: tuple[int, ...]."""
                clock.return_value = 1000
                return ()
            loader = MagicMock(side_effect=load)
            key = ("index", Filters())
            self.assertEqual(cache.get_or_load(key, loader), ())
            clock.return_value = 1000 + routes.k.FILE_QUERY_CACHE_TTL_SECONDS - 1
            self.assertEqual(cache.get_or_load(key, loader), ())
            loader.assert_called_once()
            clock.return_value += 1
            cache.get_or_load(key, loader)
            self.assertEqual(loader.call_count, 2)

    def test_entry_limit_evicts_least_recently_used(self) -> None:
        """Retain recently accessed entries. Args: None. Returns: None."""
        cache = service.FileQueryCache()
        loaders = [MagicMock(return_value=i) for i in range(3)]
        keys = [("count", Filters(equipment_id=i + 1)) for i in range(3)]
        with patch.object(routes.k, "FILE_QUERY_CACHE_MAX_ENTRIES", 2):
            for i in (0, 1, 0, 2, 0, 1):
                cache.get_or_load(keys[i], loaders[i])
        self.assertEqual([loader.call_count for loader in loaders], [1, 2, 1])

    def test_byte_limit_evicts_and_oversized_values_are_never_truncated(self) -> None:
        """Bound retained values independently of entries. Args: None. Returns: None."""
        cache = service.FileQueryCache()
        loaders = [MagicMock(return_value=i) for i in range(3)]
        keys = [("count", Filters(equipment_id=i + 1)) for i in range(3)]
        with patch.object(routes.k, "FILE_QUERY_CACHE_MAX_BYTES", 2 * sys.getsizeof(0)):
            for i in (0, 1, 2, 0):
                cache.get_or_load(keys[i], loaders[i])
            self.assertLessEqual(cache._bytes, 2 * sys.getsizeof(0))
        self.assertEqual(loaders[0].call_count, 2)
        full = tuple(range(100))
        large = MagicMock(return_value=full)
        with patch.object(routes.k, "FILE_QUERY_CACHE_MAX_BYTES", 1):
            for _ in range(2):
                self.assertEqual(cache.get_or_load(("index", Filters()), large), full)
        self.assertEqual(large.call_count, 2)

    def test_same_key_shares_result_or_failure_and_retries(self) -> None:
        """Release all waiters after success or failure. Args: None. Returns: None."""
        from concurrent.futures import ThreadPoolExecutor
        from threading import Event, Lock

        for fail in (False, True):
            with self.subTest(fail=fail):
                cache = service.FileQueryCache()
                key = ("index", Filters())
                entered, release, joined = Event(), Event(), Event()
                error = RuntimeError("database unavailable")
                counter_lock = Lock()
                waiting = [0]
                def load():
                    """Block one build for its waiters. Args: None. Returns: tuple[int, ...]."""
                    entered.set()
                    if not release.wait(3):
                        raise TimeoutError("test did not release build")
                    if fail:
                        raise error
                    return (3, 2, 1)
                loader = MagicMock(side_effect=load)
                with ThreadPoolExecutor(max_workers=4) as pool:
                    owner = pool.submit(cache.get_or_load, key, loader)
                    try:
                        self.assertTrue(entered.wait(3))
                        pending = cache._pending[key]
                        result = pending.result
                        def wait_for_owner():
                            """Track joining the same build. Args: None. Returns: tuple[int, ...]."""
                            with counter_lock:
                                waiting[0] += 1
                                if waiting[0] == 3:
                                    joined.set()
                            return result(timeout=3)
                        with patch.object(pending, "result", side_effect=wait_for_owner):
                            waiters = [pool.submit(cache.get_or_load, key, loader) for _ in range(3)]
                            self.assertTrue(joined.wait(3))
                            release.set()
                            for future in [owner, *waiters]:
                                if fail:
                                    with self.assertRaises(RuntimeError) as raised:
                                        future.result(timeout=3)
                                    self.assertIs(raised.exception, error)
                                else:
                                    self.assertEqual(future.result(timeout=3), (3, 2, 1))
                    finally:
                        release.set()
                loader.assert_called_once()
                self.assertEqual(cache.get_or_load(key, lambda: (3, 2, 1)), (3, 2, 1))

    def test_ready_hits_are_available_while_another_key_builds(self) -> None:
        """Do not block cache hits on database work. Args: None. Returns: None."""
        from concurrent.futures import ThreadPoolExecutor
        from threading import Event

        cache = service.FileQueryCache()
        ready = ("count", Filters())
        cache.get_or_load(ready, lambda: 10)
        entered, release = Event(), Event()
        def load():
            """Block an unrelated build. Args: None. Returns: tuple[int, ...]."""
            entered.set()
            if not release.wait(3):
                raise TimeoutError("test did not release build")
            return (1,)
        with ThreadPoolExecutor(max_workers=2) as pool:
            building = pool.submit(cache.get_or_load, ("index", Filters()), load)
            try:
                self.assertTrue(entered.wait(3))
                hit = pool.submit(cache.get_or_load, ready, lambda: 20)
                self.assertEqual(hit.result(timeout=1), 10)
            finally:
                release.set()
            self.assertEqual(building.result(timeout=3), (1,))


class RouteTests(unittest.TestCase):
    """Check the public REST mapping, envelopes and error boundaries."""

    def setUp(self) -> None:
        """Initialize isolated test fixtures.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        patcher = patch.object(service, "_FILE_QUERY_CACHE", service.FileQueryCache())
        patcher.start()
        self.addCleanup(patcher.stop)
        app = Flask(__name__)
        app.register_blueprint(routes.appanalise_api_bp)
        app.logger.disabled = True
        self.client = app.test_client()
        self.db = MagicMock(spec=database.AppAnaliseDB)
        self.patch = patch.object(routes, "AppAnaliseDB", return_value=self.db)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_table_routes_map_to_expected_operations(self) -> None:
        """Verify table routes map to expected operations.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        for path, method in (
            ("/stations/summary", "get_station_summary"),
            ("/hosts/12/stats", "get_host_stats"),
            ("/equipments", "get_spectrum_equipments"),
            ("/states", "get_spectrum_states"),
            ("/localities", "get_spectrum_localities"),
            ("/files/77/spectra", "get_spectra_by_file_id"),
        ):
            with self.subTest(path=path):
                self.db.reset_mock()
                getattr(self.db, method).return_value = TableResult(["ID"], [])
                response = self.client.get(PREFIX + path)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json, {"columns": ["ID"], "rows": []})
                getattr(self.db, method).assert_called_once()
        self.db.get_spectra_by_file_id.assert_called_once_with(77)

    def test_get_and_post_filters_are_equivalent(self) -> None:
        """Verify get and post filters are equivalent.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        for path, method in (("/equipments", "get_spectrum_equipments"),
                             ("/states", "get_spectrum_states"),
                             ("/localities", "get_spectrum_localities"),
                             ("/files", "get_spectrum_file_index"),
                             ("/files/count", "get_spectrum_file_data_count")):
            mocked = getattr(self.db, method)
            mocked.return_value = 0 if path.endswith("count") else (() if path == "/files" else TableResult([], []))
            self.db.get_spectrum_files_by_ids.return_value = TableResult([], [])
            get_response = self.client.get(PREFIX + path + "?equipmentId=12&districtId=1&districtId=2")
            first = mocked.call_args
            post_response = self.client.post(PREFIX + path, json={"equipmentId": 12, "districtId": [1, 2]})
            self.assertEqual(get_response.status_code, 200)
            self.assertEqual(post_response.status_code, 200)
            self.assertEqual(first, mocked.call_args)

    def test_files_retains_lookahead_and_exposes_pagination(self) -> None:
        """Verify files retains lookahead and exposes pagination.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.db.get_spectrum_file_index.return_value = (1, 2)
        self.db.get_spectrum_files_by_ids.return_value = TableResult(["ID_FILE"], [{"ID_FILE": 1}, {"ID_FILE": 2}])
        response = self.client.get(PREFIX + "/files?pageSize=1")
        self.assertEqual(len(response.json["rows"]), 2)
        self.assertEqual(response.json["pagination"], {"page": 1, "page_size": 1, "has_more": True})

    def test_count_envelope(self) -> None:
        """Verify the file count envelope.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.db.get_spectrum_file_data_count.return_value = 7
        self.assertEqual(self.client.get(PREFIX + "/files/count").json, {"count": 7})

    def test_failed_index_returns_json_and_next_request_can_retry(self) -> None:
        """Propagate a failed build through HTTP. Args: None. Returns: None."""
        self.db.get_spectrum_file_index.side_effect = [RuntimeError("private SQL details"), (1,)]
        self.db.get_spectrum_files_by_ids.return_value = TableResult(["ID_FILE"], [{"ID_FILE": 1}])
        failed = self.client.post(PREFIX + "/files", json={"stateCode": "SP"})
        self.assertEqual(failed.status_code, 500)
        self.assertEqual(failed.json["error"]["code"], "query_failed")
        self.assertNotIn("private SQL", failed.get_data(as_text=True))
        retried = self.client.post(PREFIX + "/files", json={"stateCode": "SP"})
        self.assertEqual(retried.status_code, 200)
        self.assertEqual(retried.json["rows"], [{"ID_FILE": 1}])

    def test_bad_inputs_never_query(self) -> None:
        """Verify bad inputs never query.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        cases = [
            ("post", "/files", {"json": {"sql": "SELECT 1"}}, 400),
            ("post", "/files", {"json": []}, 400),
            ("post", "/files", {"data": "SELECT 1"}, 415),
            ("post", "/files", {"data": "{bad", "content_type": "application/json"}, 400),
            ("post", "/files?page=1", {"json": {}}, 400),
            ("post", "/files", {"json": {"description": "x" * 65536}}, 413),
            ("get", "/files?equipmentId=1&equipmentId=2", {}, 400),
            ("get", "/files?pageSize=0", {}, 400),
            ("get", "/files", {"json": {}}, 400),
            ("get", "/stations/summary?stateCode=SP", {}, 400),
        ]
        for verb, path, kwargs, expected in cases:
            with self.subTest(path=path, verb=verb, expected=expected):
                response = getattr(self.client, verb)(PREFIX + path, **kwargs)
                self.assertEqual(response.status_code, expected)
                self.assertIn("error", response.json)
        self.assertEqual(self.db.mock_calls, [])

    def test_database_failure_is_not_an_empty_success_or_sql_leak(self) -> None:
        """Verify database failure is not an empty success or sql leak.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.db.get_spectrum_states.side_effect = RuntimeError("SELECT password FROM secrets")
        response = self.client.get(PREFIX + "/states")
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json["error"]["code"], "query_failed")
        self.assertNotIn("password", response.get_data(as_text=True))

    def test_unsupported_methods_and_ids(self) -> None:
        """Verify unsupported methods and ids.

        Args:
            None.

        Returns:
            None. Assertions fail when the contract changes.
        """
        self.assertEqual(self.client.delete(PREFIX + "/files").status_code, 405)
        self.assertEqual(self.client.get(PREFIX + "/hosts/0/stats").status_code, 404)
        self.assertEqual(self.client.get(PREFIX + "/files/no/spectra").status_code, 404)
        self.assertEqual(self.db.mock_calls, [])


if __name__ == "__main__":
    unittest.main()
