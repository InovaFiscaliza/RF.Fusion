"""
Validation tests for `webfusion.modules.spectrum.service`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_spectrum_service.py -q

What is covered here:
    - reduction of repository-file rows to the newest file per spectrum
    - merge of paginated spectrum rows with repository download metadata
    - frequency filters keep spectrum bands contained within the requested range
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
    """Validate page-local repository file resolution for spectrum rows."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_spectrum_service()

    def test_reduce_latest_repo_file_rows_keeps_highest_file_id_per_spectrum(self):
        reduced = self.module._reduce_latest_repo_file_rows(
            [
                {
                    "ID_SPECTRUM": 10,
                    "ID_FILE": 100,
                    "NA_PATH": "/mnt/reposfi/2026",
                    "NA_FILE": "older.mat",
                    "NA_EXTENSION": ".mat",
                    "VL_FILE_SIZE_KB": 10,
                },
                {
                    "ID_SPECTRUM": 10,
                    "ID_FILE": 120,
                    "NA_PATH": "/mnt/reposfi/2026",
                    "NA_FILE": "newer.mat",
                    "NA_EXTENSION": ".mat",
                    "VL_FILE_SIZE_KB": 12,
                },
                {
                    "ID_SPECTRUM": 11,
                    "ID_FILE": 130,
                    "NA_PATH": "/mnt/reposfi/2026",
                    "NA_FILE": "single.mat",
                    "NA_EXTENSION": ".mat",
                    "VL_FILE_SIZE_KB": 13,
                },
            ]
        )

        self.assertEqual(reduced[10]["ID_FILE"], 120)
        self.assertEqual(reduced[10]["NA_FILE"], "newer.mat")
        self.assertEqual(reduced[11]["ID_FILE"], 130)

    def test_attach_repository_file_metadata_leaves_missing_rows_empty(self):
        rows = [
            {"ID_SPECTRUM": 10, "NA_DESCRIPTION": "A"},
            {"ID_SPECTRUM": 99, "NA_DESCRIPTION": "B"},
        ]
        latest_repo_files = {
            10: {
                "NA_PATH": "/mnt/reposfi/2026",
                "NA_FILE": "resolved.mat",
                "NA_EXTENSION": ".mat",
                "VL_FILE_SIZE_KB": 50,
            }
        }

        enriched = self.module._attach_repository_file_metadata(rows, latest_repo_files)

        self.assertEqual(enriched[0]["NA_FILE"], "resolved.mat")
        self.assertEqual(enriched[0]["NA_EXTENSION"], ".mat")
        self.assertEqual(enriched[0]["VL_FILE_SIZE_KB"], 50)
        self.assertIsNone(enriched[1]["NA_PATH"])
        self.assertIsNone(enriched[1]["NA_FILE"])
        self.assertIsNone(enriched[1]["NA_EXTENSION"])
        self.assertIsNone(enriched[1]["VL_FILE_SIZE_KB"])

    def test_build_fact_filters_requires_band_within_full_frequency_interval(self):
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

    def test_build_fact_filters_uses_same_bound_when_only_one_frequency_limit_exists(self):
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

    def test_get_spectrum_locality_options_uses_summary_rows(self):
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
        self.module.get_connection_summary = lambda: fake_connection
        self.module._SPECTRUM_QUERY_CACHE.clear()

        rows = self.module.get_spectrum_locality_options(equipment_id=99, query_mode="spectrum")

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["ID_SITE"], 12)
        self.assertEqual(rows[0]["OPTION_LABEL"], "Brasilia (site 12)")
        self.assertEqual(rows[1]["OPTION_LABEL"], "Brasilia (site 13)")
        self.assertIn("SITE_EQUIPMENT_OBS_SUMMARY", fake_cursor.executed[0][0])
        self.assertEqual(fake_cursor.executed[0][1], (99,))
        self.assertTrue(fake_connection.closed)

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
