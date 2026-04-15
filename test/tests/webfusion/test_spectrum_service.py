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


if __name__ == "__main__":
    unittest.main()
