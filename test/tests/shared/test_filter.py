"""
Validation tests for `shared.filter`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/shared/test_filter.py -q

What is covered here:
    - backward compatibility with legacy payloads that still send `agent`
    - canonical filter payload no longer exposing the retired `agent` field
"""

from __future__ import annotations

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import ensure_app_paths, import_package_module, SHARED_ROOT


ensure_app_paths()

filter_module = import_package_module("app_shared", SHARED_ROOT, "filter")
Filter = filter_module.Filter


class FilterContractTests(unittest.TestCase):
    def test_last_mode_alias_is_normalized(self) -> None:
        parsed = Filter(
            {
                "mode": "LAST_N",
                "last_n_files": "12",
                "file_path": "/mnt/internal",
            }
        ).data

        self.assertEqual(parsed["mode"], "LAST")
        self.assertEqual(parsed["last_n_files"], 12)

    def test_legacy_agent_field_is_ignored(self) -> None:
        parsed = Filter(
            {
                "mode": "NONE",
                "extension": ".zip",
                "file_path": "/mnt/internal",
                "agent": "remote",
            }
        ).data

        self.assertEqual(parsed["mode"], "NONE")
        self.assertEqual(parsed["extension"], ".zip")
        self.assertEqual(parsed["file_path"], "/mnt/internal")
        self.assertNotIn("agent", parsed)

    def test_default_filter_no_longer_exposes_agent(self) -> None:
        parsed = Filter().data

        self.assertEqual(parsed["mode"], "NONE")
        self.assertIn("file_path", parsed)
        self.assertNotIn("agent", parsed)

    def test_budget_fields_are_normalized(self) -> None:
        parsed = Filter(
            {
                "mode": "RANGE",
                "start_date": "2025-01-01",
                "max_total_gb": "30.5",
                "sort_order": "oldest_first",
                "file_path": "/mnt/internal",
            }
        ).data

        self.assertEqual(parsed["mode"], "RANGE")
        self.assertEqual(parsed["max_total_gb"], 30.5)
        self.assertEqual(parsed["sort_order"], "oldest_first")

    def test_none_mode_discards_budget_fields_and_db_promotion(self) -> None:
        filter_obj = Filter(
            {
                "mode": "NONE",
                "extension": ".zip",
                "file_path": "/mnt/internal",
                "max_total_gb": "30",
                "sort_order": "newest_first",
            }
        )

        parsed = filter_obj.data
        self.assertEqual(parsed["mode"], "NONE")
        self.assertIsNone(parsed["max_total_gb"])
        self.assertIsNone(parsed["sort_order"])

        db_eval = filter_obj.evaluate_database(host_id=77, search_type=1, search_status=0)
        self.assertIsNone(db_eval["where"])
        self.assertIsNone(db_eval["msg_prefix"])


if __name__ == "__main__":
    unittest.main()
