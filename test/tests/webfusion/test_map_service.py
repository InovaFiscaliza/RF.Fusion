"""
Validation tests for `webfusion.modules.map.service`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_map_service.py -q

What is covered here:
    - normalization of Celplan/CWSM receiver naming
    - map-side host reconciliation against `BPDATA`
"""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


MODULE_PATH = Path("/RFFusion/src/webfusion/modules/map/service.py")


def load_map_service():
    """Import the map service with lightweight DB stubs only."""
    stub_db = types.ModuleType("db")
    stub_db.get_connection_bpdata = lambda: None
    stub_db.get_connection_rfdata = lambda: None

    previous_db = sys.modules.get("db")
    sys.modules["db"] = stub_db

    try:
        spec = importlib.util.spec_from_file_location(
            "webfusion_map_service_test",
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


class TestMapService(unittest.TestCase):
    """Validate the receiver-to-host matching rules used by the map popup."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_map_service()

    def test_build_cwsm_signature_handles_short_host_name(self):
        self.assertEqual(
            self.module._build_cwsm_signature("cwsm211001"),
            "cwsm211001",
        )

    def test_build_cwsm_signature_handles_long_receiver_name(self):
        self.assertEqual(
            self.module._build_cwsm_signature("cwsm21100001"),
            "cwsm211001",
        )
        self.assertEqual(
            self.module._build_cwsm_signature("cwsm21120037"),
            "cwsm212037",
        )
        self.assertEqual(
            self.module._build_cwsm_signature("cwsm22010007"),
            "cwsm211007",
        )
        self.assertEqual(
            self.module._build_cwsm_signature("cwsm22010040"),
            "cwsm220040",
        )

    def test_find_host_for_equipment_matches_celplan_receiver_variants(self):
        host_index = {
            "raw": {
                "cwsm211001": {
                    "host_id": 101,
                    "host_name": "CWSM211001",
                    "is_offline": False,
                },
                "cwsm220040": {
                    "host_id": 202,
                    "host_name": "CWSM220040",
                    "is_offline": False,
                },
                "cwsm212037": {
                    "host_id": 303,
                    "host_name": "CWSM212037",
                    "is_offline": False,
                },
                "cwsm211007": {
                    "host_id": 404,
                    "host_name": "CWSM211007",
                    "is_offline": False,
                },
            },
            "normalized": {
                "cwsm211001": [
                    {
                        "host_id": 101,
                        "host_name": "CWSM211001",
                        "is_offline": False,
                    }
                ],
                "cwsm220040": [
                    {
                        "host_id": 202,
                        "host_name": "CWSM220040",
                        "is_offline": False,
                    }
                ],
                "cwsm212037": [
                    {
                        "host_id": 303,
                        "host_name": "CWSM212037",
                        "is_offline": False,
                    }
                ],
                "cwsm211007": [
                    {
                        "host_id": 404,
                        "host_name": "CWSM211007",
                        "is_offline": False,
                    }
                ],
            },
        }

        host = self.module._find_host_for_equipment(host_index, "cwsm21100001")
        self.assertIsNotNone(host)
        self.assertEqual(host["host_id"], 101)

        host = self.module._find_host_for_equipment(host_index, "cwsm22010040")
        self.assertIsNotNone(host)
        self.assertEqual(host["host_id"], 202)

        host = self.module._find_host_for_equipment(host_index, "cwsm21120037")
        self.assertIsNotNone(host)
        self.assertEqual(host["host_id"], 303)

        host = self.module._find_host_for_equipment(host_index, "cwsm22010007")
        self.assertIsNotNone(host)
        self.assertEqual(host["host_id"], 404)


if __name__ == "__main__":
    unittest.main()
