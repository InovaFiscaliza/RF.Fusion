"""
Validation tests for `webfusion.modules.spectrum.service`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_spectrum_service.py -q

What is covered here:
    - frequency filters use overlap semantics for unified file search
    - paginated file search ranks file ids before expanding page details
    - locality options follow the live filtered catalog
    - expanded file details expose `IS_MATCH` for highlighted rows
"""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


MODULE_PATH = Path("/RFFusion/src/webfusion/modules/spectrum/service.py")


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

    def test_build_fact_filters_uses_overlap_for_full_frequency_interval(self):
        where_clauses, params = self.module._build_fact_filters(
            freq_start=100.0,
            freq_end=200.0,
            fact_alias="f",
        )

        self.assertEqual(
            where_clauses,
            [
                "f.NU_FREQ_END >= %s",
                "f.NU_FREQ_START <= %s",
            ],
        )
        self.assertEqual(params, [100.0, 200.0])

    def test_build_fact_filters_uses_overlap_with_single_frequency_limit(self):
        lower_where, lower_params = self.module._build_fact_filters(
            freq_start=100.0,
            fact_alias="f",
        )
        upper_where, upper_params = self.module._build_fact_filters(
            freq_end=200.0,
            fact_alias="f",
        )

        self.assertEqual(lower_where, ["f.NU_FREQ_END >= %s"])
        self.assertEqual(lower_params, [100.0])
        self.assertEqual(upper_where, ["f.NU_FREQ_START <= %s"])
        self.assertEqual(upper_params, [200.0])

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
        self.assertEqual(len(fake_cursor.executed), 1)
        self.assertNotIn("LIMIT %s OFFSET %s", fake_cursor.executed[0][0])
        self.assertIn("EXISTS", fake_cursor.executed[0][0])
        self.assertEqual(fake_cursor.executed[0][1], [338, 50.0, 120.0])

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
        self.assertEqual(len(fake_cursor.executed), 1)
        self.assertTrue(fake_connection.closed)

    def test_get_spectrum_locality_options_uses_live_filtered_rows(self):
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
        self.assertEqual(
            fake_cursor.executed[0][1],
            [99, "2024-01-01", 70.0, 120.0, "%PMEC%"],
        )
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
