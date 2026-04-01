"""
Validation tests for `webfusion.modules.host.service`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_host_service.py -q

What is covered here:
    - normalization of CelPlan/CWSM receiver naming on the host page
    - boolean host/equipment reconciliation for fixed monitoring stations
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path("/RFFusion/src/webfusion/modules/host/service.py")


def load_host_service():
    """Import the host service with lightweight DB stubs only."""
    stub_db = types.ModuleType("db")
    stub_db.get_connection_bpdata = lambda: None
    stub_db.get_connection_rfdata = lambda: None

    previous_db = sys.modules.get("db")
    sys.modules["db"] = stub_db

    try:
        spec = importlib.util.spec_from_file_location(
            "webfusion_host_service_test",
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


class TestHostService(unittest.TestCase):
    """Validate host-page reconciliation of fixed-station receiver names."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_host_service()

    def test_build_cwsm_signature_handles_known_receiver_families(self):
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

    def test_equipment_matches_host_handles_cwsm_2112_family(self):
        self.assertTrue(
            self.module._equipment_matches_host("CWSM212037", "cwsm21120037")
        )
        self.assertFalse(
            self.module._equipment_matches_host("CWSM211037", "cwsm21120037")
        )
        self.assertTrue(
            self.module._equipment_matches_host("CWSM211007", "cwsm22010007")
        )

    def test_check_appanalise_status_uses_ping_successfully(self):
        with patch.object(
            self.module,
            "_load_appanalise_settings",
            return_value={"host": "appanalise.local", "port": 8910, "timeout": 2.0},
        ):
            with patch.object(
                self.module.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(
                    args=["ping"],
                    returncode=0,
                    stdout="64 bytes from appanalise.local: icmp_seq=1 ttl=64 time=12.4 ms\n",
                    stderr="",
                ),
            ) as mocked_run:
                status = self.module._check_appanalise_status()

        self.assertTrue(status["online"])
        self.assertEqual(status["latency_ms"], 12.4)
        self.assertIsNone(status["error"])
        mocked_run.assert_called_once()

    def test_check_appanalise_status_reports_ping_failure(self):
        with patch.object(
            self.module,
            "_load_appanalise_settings",
            return_value={"host": "appanalise.local", "port": 8910, "timeout": 2.0},
        ):
            with patch.object(
                self.module.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(
                    args=["ping"],
                    returncode=1,
                    stdout="",
                    stderr="Destination Host Unreachable",
                ),
            ):
                status = self.module._check_appanalise_status()

        self.assertFalse(status["online"])
        self.assertIsNone(status["latency_ms"])
        self.assertEqual(status["error"], "Destination Host Unreachable")


if __name__ == "__main__":
    unittest.main()
