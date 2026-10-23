"""
Validation tests for `webfusion.modules.spectrum.service`.

How to run:
    /usr/local/bin/python /RF.Fusion/test/tests/webfusion/test_spectrum_service.py -v

What is covered here:
    - frequency filters keep spectra contained within the requested interval
    - paginated file search ranks file ids before expanding page details
    - locality options follow the live filtered catalog
    - expanded file details expose `IS_MATCH` for highlighted rows
"""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from contextlib import ExitStack
from unittest.mock import MagicMock, patch
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[3] / "src/webfusion/modules/spectrum/service.py"


def load_spectrum_service():
    """Import the spectrum service with lightweight DB stubs only."""
    stub_db = types.ModuleType("db")
    stub_db.get_connection_rfdata = lambda: None
    stub_db.get_connection_summary = lambda: None

    previous_db = sys.modules.get("db")
    sys.modules["db"] = stub_db

    try:
        spec = importlib.util.spec_from_file_location(
            "webfusion_spectrum_service_test",
            MODULE_PATH,
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Unable to load module from {MODULE_PATH}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if previous_db is not None:
            sys.modules["db"] = previous_db
        else:
            sys.modules.pop("db", None)


class TestSpectrumService(unittest.TestCase):
    """Validate the unified spectrum/file search helpers."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_spectrum_service()

    def test_selection_snapshot_keeps_hierarchy_and_exact_detail_filters(self) -> None:
        """Keep broad menus and narrowly scoped availability in one snapshot.

        Returns:
            None.
        """
        rows = [
            {"ID_EQUIPMENT": 88, "NA_EQUIPMENT": "A", "ID_DISTRICT": 10, "DISTRICT_NAME": "Palmas"},
            {"ID_EQUIPMENT": 99, "NA_EQUIPMENT": "B", "ID_DISTRICT": 11, "DISTRICT_NAME": "Araguaína"},
        ]
        with ExitStack() as stack:
            stack.enter_context(patch.object(self.module, "_get_cached_query", return_value=None))
            stack.enter_context(patch.object(self.module, "_set_cached_query"))
            global_options = stack.enter_context(patch.object(self.module, "get_spectrum_filter_options", return_value={
                "states": [{"ID_STATE": 17, "OPTION_LABEL": "TO"}, {"ID_STATE": 35, "OPTION_LABEL": "SP"}],
            }))
            matches = stack.enter_context(patch.object(self.module, "_load_selection_matches", return_value=rows))
            summary = stack.enter_context(patch.object(self.module, "_load_summary_filter_availability_row"))
            availability = stack.enter_context(patch.object(self.module, "_load_fact_filter_availability_row", return_value={
                "DATE_START": "2025-01-01", "DATE_END": "2025-12-31",
            }))
            result = self.module.get_spectrum_selection_options(
                equipment_id="88", state_id="17", district_id="10", site_id="42",
                start_date="2025-02-01", end_date="2025-10-01", freq_start=70, freq_end=110,
                description="PMRD",
            )
        self.assertEqual(result["equipments"], [{"ID_EQUIPMENT": 88, "OPTION_LABEL": "A"}])
        self.assertEqual([row["ID_DISTRICT"] for row in result["districts"]], [10, 11])
        self.assertEqual(len(result["states"]), 2)
        global_options.assert_called_once_with(include_districts=False, include_availability=False)
        matches.assert_called_once_with(state_id="17", start_date="2025-02-01", end_date="2025-10-01",
                                        freq_start=70, freq_end=110, description="PMRD")
        summary.assert_not_called()
        availability.assert_called_once_with(equipment_id="88", state_id="17", district_id="10",
                                             site_id="42", freq_start=70, freq_end=110, description="PMRD")
        self.assertNotIn("SPECTRUM_COUNT", result["availability"])

    def test_selection_probe_stops_at_first_exact_match_and_closes_connection(self) -> None:
        """Verify bound predicates and cleanup on success and query failure.

        Returns:
            None.
        """
        connection = MagicMock()
        cursor = connection.cursor.return_value
        cursor.fetchall.return_value = []
        with patch.object(self.module, "get_connection", return_value=connection):
            self.assertEqual(self.module._load_selection_matches(
                state_id="35", start_date="2025-01-01", end_date="2025-12-31",
                freq_start=70, freq_end=110, description="PMRD",
            ), [])
        sql, params = cursor.execute.call_args.args
        self.assertIn("LIMIT 1) IS NOT NULL", sql)
        self.assertNotIn("COUNT(", sql)
        self.assertIn("f.NU_FREQ_START >= %s", sql)
        self.assertIn("f.NU_FREQ_END <= %s", sql)
        self.assertEqual(params, ["35", "2025-01-01", "2025-12-31 23:59:59", 70, 110, "%PMRD%"])
        connection.close.assert_called_once()
        connection.reset_mock()
        cursor.execute.side_effect = RuntimeError("Unavailable")
        with patch.object(self.module, "get_connection", return_value=connection), self.assertRaises(RuntimeError):
            self.module._load_selection_matches(state_id="35")
        connection.close.assert_called_once()

    def test_selection_reuses_only_data_unaffected_by_the_edited_field(self) -> None:
        """Reuse matches across equipment edits and availability across date edits.

        Returns:
            None.
        """
        self.module._SPECTRUM_QUERY_CACHE.clear()
        try:
            with patch.object(self.module, "get_spectrum_filter_options", return_value={"states": []}), \
                    patch.object(self.module, "_load_selection_matches", return_value=[]) as matches, \
                    patch.object(self.module, "_load_selection_availability", return_value=None) as availability:
                self.module.get_spectrum_selection_options(state_id="17", start_date="2025-01-01")
                self.module.get_spectrum_selection_options(state_id="17", start_date="2025-01-01", equipment_id="88")
                self.assertEqual(matches.call_count, 1)
                self.assertEqual(availability.call_count, 2)
                self.module.get_spectrum_selection_options(state_id="17", start_date="2024-01-01", equipment_id="88")
                self.assertEqual(matches.call_count, 2)
                self.assertEqual(availability.call_count, 2)
        finally:
            self.module._SPECTRUM_QUERY_CACHE.clear()

    def test_detail_filters_bypass_geography_only_summary_options(self) -> None:
        """Verify that detail predicates reach every fact facet.

        Returns:
            None.
        """
        for filters in ({"start_date": "2025-01-01"}, {"end_date": "2025-12-31"},
                        {"freq_start": 100.0}, {"freq_end": 200.0}, {"description": "PMRD"}):
            with self.subTest(filters=filters), ExitStack() as stack:
                stack.enter_context(patch.object(self.module, "_get_cached_query", return_value=None))
                stack.enter_context(patch.object(self.module, "_set_cached_query"))
                for name in ("_finalize_equipment_options", "_finalize_state_options", "_finalize_district_options", "_finalize_availability_row"):
                    stack.enter_context(patch.object(self.module, name, side_effect=lambda rows: rows))
                summaries = {}
                facts = {}
                for suffix in ("equipment_filter_rows", "state_rows", "district_rows", "filter_availability_row"):
                    value = {"DATE_START": "2025-01-01", "DATE_END": "2025-12-31"} if suffix == "filter_availability_row" else [{"OPTION_LABEL": "match"}]
                    summaries[suffix] = stack.enter_context(patch.object(self.module, "_load_summary_" + suffix, return_value=value))
                    facts[suffix] = stack.enter_context(patch.object(self.module, "_load_fact_" + suffix, return_value=value))
                self.module.get_spectrum_filter_options(equipment_id="88", state_id="29", **filters)
                for suffix in ("equipment_filter_rows", "state_rows", "district_rows"):
                    summaries[suffix].assert_not_called()
                    facts[suffix].assert_called_once()
                    for key, value in filters.items():
                        self.assertEqual(facts[suffix].call_args.kwargs[key], value)
                if "freq_start" in filters or "freq_end" in filters or "description" in filters:
                    summaries["filter_availability_row"].assert_not_called()
                    facts["filter_availability_row"].assert_called_once()
                    self.assertNotIn("start_date", facts["filter_availability_row"].call_args.kwargs)
                else:
                    summaries["filter_availability_row"].assert_called_once()
                    facts["filter_availability_row"].assert_not_called()

    def test_geography_only_filters_keep_using_summary(self) -> None:
        """Keep the inexpensive summary path for geography-only changes.

        Returns:
            None.
        """
        with ExitStack() as stack:
            stack.enter_context(patch.object(self.module, "_get_cached_query", return_value=None))
            stack.enter_context(patch.object(self.module, "_set_cached_query"))
            for name in ("_finalize_equipment_options", "_finalize_state_options", "_finalize_district_options", "_finalize_availability_row"):
                stack.enter_context(patch.object(self.module, name, side_effect=lambda rows: rows))
            summaries = []
            for suffix in ("equipment_filter_rows", "state_rows", "district_rows", "filter_availability_row"):
                value = {"DATE_START": "2025-01-01", "DATE_END": "2025-12-31"} if suffix == "filter_availability_row" else [{"OPTION_LABEL": "match"}]
                summaries.append(stack.enter_context(patch.object(self.module, "_load_summary_" + suffix, return_value=value)))
                stack.enter_context(patch.object(self.module, "_load_fact_" + suffix, side_effect=AssertionError("Unexpected fact scan")))
            self.module.get_spectrum_filter_options(equipment_id="88", state_id="29")
            for summary in summaries:
                summary.assert_called_once()

    def test_build_fact_filters_uses_containment_for_full_frequency_interval(self):
        where_clauses, params = self.module._build_fact_filters(
            freq_start=100.0,
            freq_end=200.0,
            fact_alias="f",
        )

        self.assertEqual(
            where_clauses,
            [
                "f.NU_FREQ_START >= %s",
                "f.NU_FREQ_END <= %s",
            ],
        )
        self.assertEqual(params, [100.0, 200.0])

    def test_build_fact_filters_uses_containment_with_single_frequency_limit(self):
        lower_where, lower_params = self.module._build_fact_filters(
            freq_start=100.0,
            fact_alias="f",
        )
        upper_where, upper_params = self.module._build_fact_filters(
            freq_end=200.0,
            fact_alias="f",
        )

        self.assertEqual(lower_where, ["f.NU_FREQ_START >= %s"])
        self.assertEqual(lower_params, [100.0])
        self.assertEqual(upper_where, ["f.NU_FREQ_END <= %s"])
        self.assertEqual(upper_params, [200.0])

    def test_append_summary_geo_filters_uses_materialized_district_id(self):
        where_clauses = []
        params = []

        self.module._append_summary_geo_filters(
            where_clauses,
            params,
            equipment_id=133,
            state_id=32,
            district_id=181,
            site_id=237,
            summary_alias="sm",
            site_alias="unused_alias",
        )

        self.assertEqual(
            where_clauses,
            [
                "sm.FK_EQUIPMENT = %s",
                "sm.ID_STATE = %s",
                "sm.FK_DISTRICT = %s",
                "sm.FK_SITE = %s",
            ],
        )
        self.assertEqual(params, [133, 32, 181, 237])

    def test_get_spectrum_data_uses_summary_for_geography_and_labels(self):
        class FakeCursor:
            def __init__(self):
                self.executed = []
                self.fetchall_responses = [[
                    {
                        "ID_SPECTRUM": 4174724,
                        "NA_DESCRIPTION": "PMRD",
                        "NU_FREQ_START": 70.0,
                        "NU_FREQ_END": 110.0,
                        "DT_TIME_START": "2026-05-06 01:50:00",
                        "DT_TIME_END": "2026-05-07 05:55:00",
                        "NU_TRACE_COUNT": 338,
                        "NU_TRACE_LENGTH": 1024,
                        "NU_RBW": 73828.0,
                        "NU_VBW": 73828.0,
                        "NU_ATT_GAIN": 10.0,
                        "NA_EQUIPMENT": "rfeye002129",
                        "ID_SITE": 237,
                        "LOCALITY_LABEL": "Enseada do Sua (Vitoria/ES)",
                        "COUNTY_NAME": "Vitoria",
                        "STATE_CODE": "ES",
                    }
                ]]

            def execute(self, query, params):
                self.executed.append((query, params))

            def fetchall(self):
                return self.fetchall_responses.pop(0)

            def fetchone(self):
                return {"total": 1}

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor
                self.closed = False

            def cursor(self):
                return self._cursor

            def close(self):
                self.closed = True

        fake_cursor = FakeCursor()
        fake_connection = FakeConnection(fake_cursor)
        original_fetch_latest = self.module._fetch_latest_repo_files_for_spectra
        self.module.get_connection = lambda: fake_connection
        self.module._fetch_latest_repo_files_for_spectra = lambda cur, spectrum_ids: {}
        self.module._SPECTRUM_QUERY_CACHE.clear()

        try:
            rows, total = self.module.get_spectrum_data(
                equipment_id=338,
                state_id=32,
                page=1,
                page_size=50,
            )
        finally:
            self.module._fetch_latest_repo_files_for_spectra = original_fetch_latest

        self.assertEqual(total, 1)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ID_SPECTRUM"], 4174724)
        self.assertIn("RFFUSION_SUMMARY.SITE_EQUIPMENT_OBS_SUMMARY", fake_cursor.executed[0][0])
        self.assertNotIn("JOIN DIM_SPECTRUM_SITE", fake_cursor.executed[0][0])
        self.assertNotIn("JOIN DIM_SPECTRUM_EQUIPMENT", fake_cursor.executed[0][0])
        self.assertIn("RFFUSION_SUMMARY.SITE_EQUIPMENT_OBS_SUMMARY", fake_cursor.executed[1][0])
        self.assertEqual(fake_cursor.executed[0][1], [338, 32, 50, 0])
        self.assertEqual(fake_cursor.executed[1][1], [338, 32])
        self.assertTrue(fake_connection.closed)

    def test_file_metadata_aggregates_all_spectra_after_filtering_files(self) -> None:
        """Keep file metadata independent from the spectra search interval.

        Returns:
            None.
        """
        cursor = MagicMock()
        cursor.fetchall.return_value = [{
            "ID_FILE": 626575,
            "DT_TIME_START": "2026-09-30 17:44:35",
            "DT_TIME_END": "2026-10-05 05:44:26",
            "NU_FREQ_START": 404.99,
            "NU_FREQ_END": 410.0,
            "NU_SPECTRA": 9,
        }]
        connection = MagicMock()
        connection.cursor.return_value = cursor
        with ExitStack() as stack:
            stack.enter_context(patch.object(self.module, "get_connection", return_value=connection))
            stack.enter_context(patch.object(self.module, "_get_cached_query", return_value=None))
            stack.enter_context(patch.object(self.module, "_set_cached_query"))
            stack.enter_context(patch.object(self.module, "_set_cached_query_with_ttl"))
            rows, total = self.module.get_spectrum_file_data(
                start_date="2026-09-30", end_date="2026-10-01",
                freq_start=400.0, freq_end=407.0, sort_by="spectrum_count",
            )

        filter_query, filter_params = cursor.execute.call_args_list[0].args
        metadata_query, metadata_params = cursor.execute.call_args_list[1].args
        self.assertIn("fs.DT_TIME_START <= %s", filter_query)
        self.assertIn("2026-10-01 23:59:59", filter_params)
        self.assertEqual(metadata_params, ())
        # Only file selection reads the filtered set. Aggregates must use the
        # unfiltered fact rows reached through every link of a matching file.
        self.assertEqual(metadata_query.count(self.module.FILE_FILTER_TEMP_TABLE), 1)
        self.assertIn("ON b.FK_FILE = repos.ID_FILE", metadata_query)
        self.assertIn("JOIN FACT_SPECTRUM f", metadata_query)
        self.assertIn("ON f.ID_SPECTRUM = b.FK_SPECTRUM", metadata_query)
        self.assertIn("MIN(f.DT_TIME_START)", metadata_query)
        self.assertIn("MAX(f.DT_TIME_END)", metadata_query)
        self.assertIn("MAX(f.NU_FREQ_END)", metadata_query)
        self.assertIn("COUNT(DISTINCT b.FK_SPECTRUM) AS NU_SPECTRA", metadata_query)
        self.assertIn("ORDER BY ctx.NU_SPECTRA DESC", metadata_query)
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["DT_TIME_END"], "05-10-2026 05:44:26")
        self.assertEqual(rows[0]["NU_FREQ_END"], 410.0)
        connection.close.assert_called_once()

    def test_get_spectrum_file_data_caches_full_search_results_before_paging(self):
        class FakeCursor:
            def __init__(self):
                self.executed = []
                self.fetchall_responses = [[
                    {
                        "ID_FILE": 200,
                        "NA_PATH": "/mnt/reposfi/2026",
                        "NA_FILE": "b.mat",
                        "NA_EXTENSION": ".mat",
                        "VL_FILE_SIZE_KB": 20,
                        "DT_TIME_START": "2026-05-06 01:48:12",
                        "DT_TIME_END": "2026-05-07 05:58:00",
                        "NU_FREQ_START": 70.0,
                        "NU_FREQ_END": 5460.0,
                        "NU_SPECTRA": 18,
                        "LOCALITY_COUNT": 1,
                        "LOCALITY_LABELS": None,
                    },
                    {
                        "ID_FILE": 150,
                        "NA_PATH": "/mnt/reposfi/2026",
                        "NA_FILE": "a.mat",
                        "NA_EXTENSION": ".mat",
                        "VL_FILE_SIZE_KB": 15,
                        "DT_TIME_START": "2026-05-05 01:48:12",
                        "DT_TIME_END": "2026-05-06 05:58:00",
                        "NU_FREQ_START": 70.0,
                        "NU_FREQ_END": 3800.0,
                        "NU_SPECTRA": 10,
                        "LOCALITY_COUNT": 1,
                        "LOCALITY_LABELS": None,
                    },
                ]]
                self.fetchone_responses = []

            def execute(self, query, params):
                self.executed.append((query, params))

            def fetchall(self):
                return self.fetchall_responses.pop(0)

            def fetchone(self):
                return self.fetchone_responses.pop(0)

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor
                self.closed = False

            def cursor(self):
                return self._cursor

            def close(self):
                self.closed = True

        fake_cursor = FakeCursor()
        fake_connection = FakeConnection(fake_cursor)
        self.module.get_connection = lambda: fake_connection
        self.module._SPECTRUM_QUERY_CACHE.clear()

        rows, total = self.module.get_spectrum_file_data(
            equipment_id=338,
            freq_start=50.0,
            freq_end=120.0,
            page=2,
            page_size=1,
        )

        self.assertEqual(total, 2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ID_FILE"], 150)
        self.assertEqual(rows[0]["LOCALITY_DISPLAY"], "—")
        self.assertEqual(len(fake_cursor.executed), 2)
        self.assertIn("CREATE TEMPORARY TABLE", fake_cursor.executed[0][0])
        self.assertIn("tmp_webfusion_filtered_spectra", fake_cursor.executed[0][0])
        self.assertIn("RFFUSION_SUMMARY.SITE_EQUIPMENT_OBS_SUMMARY", fake_cursor.executed[0][0])
        self.assertNotIn("JOIN DIM_SPECTRUM_SITE", fake_cursor.executed[0][0])
        self.assertEqual(
            fake_cursor.executed[0][1],
            [338, 50.0, 120.0],
        )
        self.assertNotIn("LIMIT %s OFFSET %s", fake_cursor.executed[1][0])
        self.assertNotIn("FACT_SPECTRUM fs", fake_cursor.executed[1][0])
        self.assertEqual(fake_cursor.executed[1][1], ())

        cached_rows, cached_total = self.module.get_spectrum_file_data(
            equipment_id=338,
            freq_start=50.0,
            freq_end=120.0,
            page=1,
            page_size=1,
        )

        self.assertEqual(cached_total, 2)
        self.assertEqual(len(cached_rows), 1)
        self.assertEqual(cached_rows[0]["ID_FILE"], 200)
        self.assertEqual(len(fake_cursor.executed), 2)
        self.assertTrue(fake_connection.closed)

    def test_get_spectrum_locality_options_uses_summary_scoped_live_rows(self):
        class FakeCursor:
            def __init__(self):
                self.executed = []

            def execute(self, query, params):
                self.executed.append((query, params))

            def fetchall(self):
                return [
                    {
                        "ID_SITE": 12,
                        "LOCALITY_LABEL": "Brasilia",
                        "COUNTY_NAME": "Brasilia",
                        "STATE_CODE": "DF",
                        "DATE_START": "2024-01-01 00:00:00",
                        "DATE_END": "2024-01-31 23:59:59",
                        "SPECTRUM_COUNT": 25,
                    },
                    {
                        "ID_SITE": 13,
                        "LOCALITY_LABEL": "Brasilia",
                        "COUNTY_NAME": "Brasilia",
                        "STATE_CODE": "DF",
                        "DATE_START": "2024-02-01 00:00:00",
                        "DATE_END": "2024-02-29 23:59:59",
                        "SPECTRUM_COUNT": 10,
                    },
                ]

            def fetchone(self):
                return None

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor
                self.closed = False

            def cursor(self):
                return self._cursor

            def close(self):
                self.closed = True

        fake_cursor = FakeCursor()
        fake_connection = FakeConnection(fake_cursor)
        self.module.get_connection = lambda: fake_connection
        self.module._SPECTRUM_QUERY_CACHE.clear()

        rows = self.module.get_spectrum_locality_options(
            equipment_id=99,
            start_date="2024-01-01",
            freq_start=70.0,
            freq_end=120.0,
            description="PMEC",
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["ID_SITE"], 12)
        self.assertEqual(rows[0]["OPTION_LABEL"], "Brasilia (site 12)")
        self.assertEqual(rows[1]["OPTION_LABEL"], "Brasilia (site 13)")
        self.assertIn("FACT_SPECTRUM", fake_cursor.executed[0][0])
        self.assertIn("RFFUSION_SUMMARY.SITE_EQUIPMENT_OBS_SUMMARY", fake_cursor.executed[0][0])
        self.assertNotIn("JOIN DIM_SPECTRUM_SITE", fake_cursor.executed[0][0])
        self.assertEqual(
            fake_cursor.executed[0][1],
            [99, "2024-01-01", 70.0, 120.0, "%PMEC%"],
        )
        self.assertTrue(fake_connection.closed)

    def test_get_spectrum_site_option_reads_map_site_summary(self):
        class FakeCursor:
            def __init__(self):
                self.executed = []

            def execute(self, query, params):
                self.executed.append((query, params))

            def fetchone(self):
                return {
                    "ID_SITE": 237,
                    "ID_DISTRICT": 181,
                    "DISTRICT_NAME": "Enseada do Sua",
                    "ID_STATE": 32,
                    "STATE_NAME": "Espirito Santo",
                    "LOCALITY_LABEL": "Enseada do Sua (Vitoria/ES)",
                    "COUNTY_NAME": "Vitoria",
                    "STATE_CODE": "ES",
                }

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor
                self.closed = False

            def cursor(self):
                return self._cursor

            def close(self):
                self.closed = True

        fake_cursor = FakeCursor()
        fake_connection = FakeConnection(fake_cursor)
        self.module.get_connection_summary = lambda: fake_connection

        row = self.module.get_spectrum_site_option(237)

        self.assertEqual(row["ID_SITE"], 237)
        self.assertEqual(row["ID_DISTRICT"], 181)
        self.assertEqual(row["ID_STATE"], 32)
        self.assertEqual(row["STATE_CODE"], "ES")
        self.assertIn("RFFUSION_SUMMARY.MAP_SITE_SUMMARY", fake_cursor.executed[0][0])
        self.assertEqual(fake_cursor.executed[0][1], (237,))
        self.assertTrue(fake_connection.closed)

    def test_get_spectra_by_file_id_marks_rows_that_match_active_search(self):
        class FakeCursor:
            def __init__(self):
                self.executed = []

            def execute(self, query, params):
                self.executed.append((query, params))

            def fetchall(self):
                return [
                    {
                        "ID_SPECTRUM": 4174724,
                        "NA_DESCRIPTION": "PMRD (Faixa 2 de 4).",
                        "NU_FREQ_START": 70.0,
                        "NU_FREQ_END": 110.0,
                        "DT_TIME_START": "2026-05-06 01:50:00",
                        "DT_TIME_END": "2026-05-07 05:55:00",
                        "NU_RBW": 73828.0,
                        "NU_TRACE_COUNT": 338,
                        "NA_EQUIPMENT": "rfeye002129",
                        "IS_MATCH": 1,
                    }
                ]

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor

            def cursor(self):
                return self._cursor

            def close(self):
                pass

        fake_cursor = FakeCursor()
        self.module.get_connection = lambda: FakeConnection(fake_cursor)
        self.module._SPECTRUM_QUERY_CACHE.clear()

        rows = self.module.get_spectra_by_file_id(
            436239,
            equipment_id=338,
            freq_start=50.0,
            freq_end=120.0,
            description="PMRD",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["IS_MATCH"], 1)
        self.assertIn("CASE", fake_cursor.executed[0][0])
        self.assertIn("IS_MATCH", fake_cursor.executed[0][0])
        self.assertIn("RFFUSION_SUMMARY.SITE_EQUIPMENT_OBS_SUMMARY", fake_cursor.executed[0][0])
        self.assertNotIn("JOIN DIM_SPECTRUM_SITE", fake_cursor.executed[0][0])
        self.assertEqual(
            fake_cursor.executed[0][1],
            [338, 50.0, 120.0, "%PMRD%", 436239],
        )

    def test_get_spectrum_site_availability_range_uses_summary_rows(self):
        class FakeCursor:
            def __init__(self):
                self.executed = []

            def execute(self, query, params):
                self.executed.append((query, params))

            def fetchone(self):
                return {
                    "ID_SITE": 44,
                    "LOCALITY_LABEL": "Manaus/AM",
                    "DATE_START": "2024-03-01 00:00:00",
                    "DATE_END": "2024-03-15 23:59:59",
                    "SPECTRUM_COUNT": 7,
                }

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor

            def cursor(self):
                return self._cursor

            def close(self):
                pass

        fake_cursor = FakeCursor()
        self.module.get_connection_summary = lambda: FakeConnection(fake_cursor)
        self.module._SPECTRUM_QUERY_CACHE.clear()

        row = self.module.get_spectrum_site_availability_range(equipment_id=7, site_id=44)

        self.assertEqual(row["ID_SITE"], 44)
        self.assertEqual(row["LOCALITY_LABEL"], "Manaus/AM")
        self.assertEqual(row["SPECTRUM_COUNT"], 7)
        self.assertIn("SITE_EQUIPMENT_OBS_SUMMARY", fake_cursor.executed[0][0])
        self.assertEqual(fake_cursor.executed[0][1], (7, 44))


if __name__ == "__main__":
    unittest.main()
