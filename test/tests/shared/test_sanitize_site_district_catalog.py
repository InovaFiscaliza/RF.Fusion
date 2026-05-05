"""
Validation tests for `sanitize_site_district_catalog.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/shared/test_sanitize_site_district_catalog.py -q

What is covered here:
    - fill-name planning when the county has no named duplicate yet
    - repoint planning when a county-name district already exists
    - conflict planning when more than one duplicate target exists
"""

from __future__ import annotations

import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import APP_ROOT, ensure_app_paths, load_module_from_path


ensure_app_paths()

module = load_module_from_path(
    "test_sanitize_site_district_catalog_module",
    str(APP_ROOT / "utils" / "sanitize_site_district_catalog.py"),
)


class BuildSanitationPlanTests(unittest.TestCase):
    def test_build_sanitation_plan_marks_fill_name_when_county_name_is_missing(self) -> None:
        plan = module.build_sanitation_plan(
            [
                {
                    "ID_DISTRICT": 54,
                    "FK_COUNTY": 2100204,
                    "NA_COUNTY": "Alcântara",
                    "NA_STATE": "Maranhão",
                    "REFERENCED_SITES": 1,
                    "SITE_NAMES": "54:Alcântara",
                }
            ],
            [],
        )

        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0]["action"], "fill_name")
        self.assertEqual(plan[0]["new_name"], "Alcântara")

    def test_build_sanitation_plan_marks_repoint_when_named_target_exists(self) -> None:
        plan = module.build_sanitation_plan(
            [
                {
                    "ID_DISTRICT": 53,
                    "FK_COUNTY": 1506807,
                    "NA_COUNTY": "Santarém",
                    "NA_STATE": "Pará",
                    "REFERENCED_SITES": 1,
                    "SITE_NAMES": "53:Santarém",
                }
            ],
            [
                {
                    "ID_DISTRICT": 248,
                    "FK_COUNTY": 1506807,
                    "NA_DISTRICT": "Santarém",
                }
            ],
        )

        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0]["action"], "repoint")
        self.assertEqual(plan[0]["target_district_id"], 248)

    def test_build_sanitation_plan_marks_conflict_when_more_than_one_target_exists(self) -> None:
        plan = module.build_sanitation_plan(
            [
                {
                    "ID_DISTRICT": 31,
                    "FK_COUNTY": 1721000,
                    "NA_COUNTY": "Palmas",
                    "NA_STATE": "Tocantins",
                    "REFERENCED_SITES": 1,
                    "SITE_NAMES": "31:Palmas",
                }
            ],
            [
                {
                    "ID_DISTRICT": 255,
                    "FK_COUNTY": 1721000,
                    "NA_DISTRICT": "Palmas",
                },
                {
                    "ID_DISTRICT": 300,
                    "FK_COUNTY": 1721000,
                    "NA_DISTRICT": "Palmas",
                },
            ],
        )

        self.assertEqual(len(plan), 1)
        self.assertEqual(plan[0]["action"], "conflict")
        self.assertEqual(plan[0]["conflicting_target_ids"], [255, 300])


if __name__ == "__main__":
    unittest.main()
