"""
Validation tests for `webfusion.modules.task.routes`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_task_routes.py -q

What is covered here:
    - family-profile rows are built with prefilled defaults for known station
      groups such as RFEye and CelPlan
    - collective mixed-family requests split into per-family batches when the
      builder is still using auto-suggested defaults
    - explicit per-family overrides drive batch-specific path/extension values
    - explicit custom path/extension keeps a shared collective request intact
"""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


def load_task_routes():
    """Reload the task routes so helper tests observe current module constants."""
    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

    fake_flask = ModuleType("flask")

    class FakeBlueprint:
        def __init__(self, *args, **kwargs):
            pass

        def before_request(self, func):
            return func

        def route(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

    fake_flask.Blueprint = FakeBlueprint
    fake_flask.Response = lambda *args, **kwargs: None
    fake_flask.redirect = lambda *args, **kwargs: None
    fake_flask.render_template = lambda *args, **kwargs: None
    fake_flask.request = SimpleNamespace(
        authorization=None,
        args={},
        form={},
        method="GET",
    )
    fake_flask.url_for = lambda *args, **kwargs: ""

    fake_db = ModuleType("db")
    fake_db.get_connection_bpdata = lambda: None

    sys.modules["flask"] = fake_flask
    sys.modules["db"] = fake_db
    sys.modules.pop("modules.task.routes", None)
    return importlib.import_module("modules.task.routes")


class TestTaskRoutes(unittest.TestCase):
    """Protect the collective builder heuristics for mixed station families."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_task_routes()

    def test_station_profile_rows_prefill_known_families(self):
        rows = self.module._build_station_profile_rows(
            [
                {"PREFIX": "RFEye", "HOSTS": 5},
                {"PREFIX": "CWSM", "HOSTS": 7},
            ]
        )

        indexed = {row["prefix"].upper(): row for row in rows}

        self.assertEqual(
            indexed["RFEYE"]["file_path"],
            self.module.DEFAULT_LINUX_FILE_PATH,
        )
        self.assertEqual(
            indexed["RFEYE"]["extension"],
            self.module.DEFAULT_LINUX_EXTENSION,
        )
        self.assertEqual(
            indexed["CWSM"]["file_path"],
            self.module.DEFAULT_CWSM_FILE_PATH,
        )
        self.assertEqual(
            indexed["CWSM"]["extension"],
            self.module.DEFAULT_CWSM_EXTENSION,
        )

    def test_collective_auto_defaults_split_mixed_station_families(self):
        batches = self.module._build_collective_task_batches(
            host_rows=[
                {"ID_HOST": 11, "NA_HOST_NAME": "RFEye002264"},
                {"ID_HOST": 12, "NA_HOST_NAME": "CWSM211006"},
            ],
            filter_data={
                "mode": "NONE",
                "start_date": None,
                "end_date": None,
                "last_n_files": None,
                "extension": ".bin",
                "file_path": "/mnt/internal/data",
                "file_name": None,
            },
        )

        self.assertEqual(len(batches), 2)

        flattened = {
            tuple(batch["hosts"]): (
                batch["filter_data"]["file_path"],
                batch["filter_data"]["extension"],
            )
            for batch in batches
        }

        self.assertEqual(
            flattened[(11,)],
            (self.module.DEFAULT_LINUX_FILE_PATH, self.module.DEFAULT_LINUX_EXTENSION),
        )
        self.assertEqual(
            flattened[(12,)],
            (self.module.DEFAULT_CWSM_FILE_PATH, self.module.DEFAULT_CWSM_EXTENSION),
        )

    def test_collective_profile_overrides_drive_family_specific_batches(self):
        batches = self.module._build_collective_task_batches(
            host_rows=[
                {"ID_HOST": 31, "NA_HOST_NAME": "RFEye002264"},
                {"ID_HOST": 32, "NA_HOST_NAME": "CWSM211006"},
            ],
            filter_data={
                "mode": "NONE",
                "start_date": None,
                "end_date": None,
                "last_n_files": None,
                "extension": None,
                "file_path": None,
                "file_name": None,
            },
            profile_overrides={
                "RFEYE": {
                    "file_path": "/mnt/internal/custom",
                    "extension": ".bin",
                },
                "CWSM": {
                    "file_path": "C:/CelPlan/Custom",
                    "extension": ".zip",
                },
            },
        )

        flattened = {
            tuple(batch["hosts"]): (
                batch["filter_data"]["file_path"],
                batch["filter_data"]["extension"],
            )
            for batch in batches
        }

        self.assertEqual(flattened[(31,)], ("/mnt/internal/custom", ".bin"))
        self.assertEqual(flattened[(32,)], ("C:/CelPlan/Custom", ".zip"))

    def test_collective_explicit_filter_stays_shared(self):
        batches = self.module._build_collective_task_batches(
            host_rows=[
                {"ID_HOST": 21, "NA_HOST_NAME": "RFEye002264"},
                {"ID_HOST": 22, "NA_HOST_NAME": "CWSM211006"},
            ],
            filter_data={
                "mode": "RANGE",
                "start_date": "2025-01-01",
                "end_date": None,
                "last_n_files": None,
                "extension": ".zip",
                "file_path": "/custom/shared/path",
                "file_name": None,
            },
        )

        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["hosts"], [21, 22])
        self.assertEqual(batches[0]["filter_data"]["file_path"], "/custom/shared/path")
        self.assertEqual(batches[0]["filter_data"]["extension"], ".zip")


if __name__ == "__main__":
    unittest.main()
