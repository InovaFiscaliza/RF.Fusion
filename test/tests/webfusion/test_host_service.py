"""
Validation tests for `webfusion.modules.host.service`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_host_service.py -q

What is covered here:
    - normalization of CelPlan/CWSM receiver naming on the host page
    - boolean host/equipment reconciliation for fixed monitoring stations
    - live FILE_TASK queue metrics on the host detail page
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

    def test_canonicalize_processing_error_message_groups_unclassified_and_bin_validation_noise(self):
        self.assertEqual(
            self.module._canonicalize_processing_error_message("Processing Error"),
            (
                "Processing Error | [ERROR] [stage=PROCESS] "
                "[code=UNCLASSIFIED] Processing failed without structured detail"
            ),
        )

        self.assertEqual(
            self.module._canonicalize_processing_error_message(
                "Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] "
                "APP_ANALISE returned invalid Answer.Spectra type: {'Receiver': 'CWSM21100001'}"
            ),
            (
                "Processing Error | [ERROR] [stage=PROCESS] "
                "[code=APP_ANALISE_INVALID_SPECTRA_TYPE] "
                "APP_ANALISE returned invalid Answer.Spectra type"
            ),
        )

        self.assertEqual(
            self.module._canonicalize_processing_error_message(
                "Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] "
                "Payload validation failed during processing [host_id=10364] [task_id=12]"
            ),
            (
                "Processing Error | [ERROR] [stage=PROCESS] "
                "[code=BIN_PAYLOAD_VALIDATION_FAILED] "
                "Payload validation failed during processing"
            ),
        )

    def test_merge_grouped_processing_errors_collapses_bin_validation_variants(self):
        rows = [
            {"ERROR_MESSAGE": "Processing Error", "ERROR_COUNT": 4},
            {
                "ERROR_MESSAGE": (
                    "Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] "
                    "APP_ANALISE returned invalid Answer.Spectra type: {'Receiver': 'CWSM21100001'}"
                ),
                "ERROR_COUNT": 2,
            },
            {
                "ERROR_MESSAGE": (
                    "Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] "
                    "APP_ANALISE returned invalid Answer.Spectra type: {'Receiver': 'RFeye002239'}"
                ),
                "ERROR_COUNT": 3,
            },
            {
                "ERROR_MESSAGE": (
                    "Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] "
                    "Payload validation failed during processing [host_id=10364] [task_id=12]"
                ),
                "ERROR_COUNT": 5,
            },
            {
                "ERROR_MESSAGE": (
                    "Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] "
                    "Payload validation failed during processing [host_id=10378] [task_id=34019]"
                ),
                "ERROR_COUNT": 7,
            },
        ]

        merged = self.module._merge_grouped_processing_errors(rows)

        self.assertEqual(
            merged,
            [
                {
                    "ERROR_MESSAGE": (
                        "Processing Error | [ERROR] [stage=PROCESS] "
                        "[code=BIN_PAYLOAD_VALIDATION_FAILED] "
                        "Payload validation failed during processing"
                    ),
                    "ERROR_COUNT": 12,
                },
                {
                    "ERROR_MESSAGE": (
                        "Processing Error | [ERROR] [stage=PROCESS] "
                        "[code=APP_ANALISE_INVALID_SPECTRA_TYPE] "
                        "APP_ANALISE returned invalid Answer.Spectra type"
                    ),
                    "ERROR_COUNT": 5,
                },
                {
                    "ERROR_MESSAGE": (
                        "Processing Error | [ERROR] [stage=PROCESS] "
                        "[code=UNCLASSIFIED] Processing failed without structured detail"
                    ),
                    "ERROR_COUNT": 4,
                },
            ],
        )

    def test_get_server_summary_metrics_keeps_totals_and_adds_live_queue_metrics(self):
        class FakeCursor:
            def __init__(self, rows):
                self.rows = list(rows)
                self.executed = []

            def execute(self, query, params=None):
                self.executed.append((query, params))

            def fetchone(self):
                if not self.rows:
                    return {}
                return self.rows.pop(0)

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor
                self.closed = False

            def cursor(self):
                return self._cursor

            def close(self):
                self.closed = True

        bp_cursor = FakeCursor(
            [
                {
                    "BACKUP_DONE_THIS_MONTH": 12,
                    "BACKUP_DONE_GB_THIS_MONTH": 3.75,
                },
                {
                    "BACKUP_QUEUE_FILES_TOTAL": 7,
                    "BACKUP_QUEUE_GB_TOTAL": 15.5,
                    "PROCESSING_QUEUE_FILES_TOTAL": 4,
                    "PROCESSING_QUEUE_GB_TOTAL": 8.25,
                },
                {
                    "DISCOVERED_FILES_TOTAL": 120,
                    "BACKUP_PENDING_FILES_TOTAL": 19,
                    "BACKUP_PENDING_GB_TOTAL": 22.5,
                    "BACKUP_ERROR_FILES_TOTAL": 3,
                    "PROCESSING_PENDING_FILES_TOTAL": 11,
                    "PROCESSING_DONE_FILES_TOTAL": 44,
                    "PROCESSING_ERROR_FILES_TOTAL": 2,
                },
            ]
        )
        rf_cursor = FakeCursor(
            [
                {
                    "FACT_SPECTRUM_TOTAL": 987,
                }
            ]
        )

        bp_connection = FakeConnection(bp_cursor)
        rf_connection = FakeConnection(rf_cursor)

        self.module._SERVER_SUMMARY_CACHE["payload"] = None
        self.module._SERVER_SUMMARY_CACHE["expires_at"] = 0.0

        with patch.object(self.module.time, "monotonic", return_value=100.0):
            with patch.object(
                self.module,
                "get_connection",
                return_value=bp_connection,
            ):
                with patch.object(
                    self.module,
                    "get_connection_rfdata",
                    return_value=rf_connection,
                ):
                    summary = self.module.get_server_summary_metrics()

        self.assertEqual(summary["BACKUP_DONE_THIS_MONTH"], 12)
        self.assertEqual(summary["BACKUP_DONE_GB_THIS_MONTH"], 3.75)
        self.assertEqual(summary["DISCOVERED_FILES_TOTAL"], 120)
        self.assertEqual(summary["BACKUP_PENDING_FILES_TOTAL"], 19)
        self.assertEqual(summary["BACKUP_PENDING_GB_TOTAL"], 22.5)
        self.assertEqual(summary["PROCESSING_DONE_FILES_TOTAL"], 44)
        self.assertEqual(summary["PROCESSING_PENDING_FILES_TOTAL"], 11)
        self.assertEqual(summary["PROCESSING_ERROR_FILES_TOTAL"], 2)
        self.assertEqual(summary["BACKUP_QUEUE_FILES_TOTAL"], 7)
        self.assertEqual(summary["BACKUP_QUEUE_GB_TOTAL"], 15.5)
        self.assertEqual(summary["PROCESSING_QUEUE_FILES_TOTAL"], 4)
        self.assertEqual(summary["PROCESSING_QUEUE_GB_TOTAL"], 8.25)
        self.assertEqual(summary["FACT_SPECTRUM_TOTAL"], 987)
        self.assertTrue(bp_connection.closed)
        self.assertTrue(rf_connection.closed)
        self.assertEqual(len(bp_cursor.executed), 3)
        self.assertIn("FROM FILE_TASK", bp_cursor.executed[1][0])
        self.assertIn("NU_TYPE = 2", bp_cursor.executed[1][0])

    def test_get_host_statistics_keeps_totals_and_adds_host_queue_metrics(self):
        class FakeCursor:
            def __init__(self, rows):
                self.rows = list(rows)
                self.executed = []

            def execute(self, query, params=None):
                self.executed.append((query, params))

            def fetchone(self):
                if not self.rows:
                    return {}
                return self.rows.pop(0)

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor
                self.closed = False

            def cursor(self):
                return self._cursor

            def close(self):
                self.closed = True

        bp_cursor = FakeCursor(
            [
                {
                    "ID_HOST": 42,
                    "NA_HOST_NAME": "rfeye002274",
                    "NA_HOST_ADDRESS": "10.0.0.42",
                    "NA_HOST_PORT": 22,
                    "IS_OFFLINE": 0,
                    "IS_BUSY": 0,
                    "NU_PID": None,
                    "DT_BUSY": None,
                    "DT_LAST_FAIL": None,
                    "DT_LAST_CHECK": "2026-04-08 12:00:00",
                    "NU_HOST_CHECK_ERROR": 0,
                    "DT_LAST_DISCOVERY": "2026-04-08 11:00:00",
                    "NU_DONE_FILE_DISCOVERY_TASKS": 100,
                    "NU_ERROR_FILE_DISCOVERY_TASKS": 2,
                    "DT_LAST_BACKUP": "2026-04-08 10:00:00",
                    "NU_PENDING_FILE_BACKUP_TASKS": 5,
                    "NU_DONE_FILE_BACKUP_TASKS": 80,
                    "NU_ERROR_FILE_BACKUP_TASKS": 1,
                    "VL_PENDING_BACKUP_KB": 2048,
                    "VL_DONE_BACKUP_KB": 8192,
                    "DT_LAST_PROCESSING": "2026-04-08 09:00:00",
                    "NU_PENDING_FILE_PROCESS_TASKS": 4,
                    "NU_DONE_FILE_PROCESS_TASKS": 70,
                    "NU_ERROR_FILE_PROCESS_TASKS": 3,
                    "NU_HOST_FILES": 123,
                },
                {
                    "BACKUP_QUEUE_FILES_TOTAL": 3,
                    "BACKUP_QUEUE_GB_TOTAL": 1.5,
                    "PROCESSING_QUEUE_FILES_TOTAL": 2,
                    "PROCESSING_QUEUE_GB_TOTAL": 0.75,
                },
                {
                    "FAILURE_AT": "2026-04-08 08:00:00",
                    "FAILURE_REASON": "Processing Error",
                },
            ]
        )

        bp_connection = FakeConnection(bp_cursor)

        self.module._HOST_STATISTICS_CACHE.clear()

        with patch.object(self.module.time, "monotonic", return_value=100.0):
            with patch.object(
                self.module,
                "get_connection",
                return_value=bp_connection,
            ):
                with patch.object(
                    self.module,
                    "_get_history_summary_for_host",
                    return_value={
                        "BACKUP_DONE_THIS_MONTH": 8,
                        "BACKUP_DONE_GB_THIS_MONTH": 2.25,
                        "DISCOVERED_FILES_TOTAL": 120,
                        "DISCOVERED_GB_TOTAL": 10.5,
                        "BACKUP_DONE_FILES_TOTAL": 80,
                        "BACKUP_DONE_GB_TOTAL": 8.0,
                        "BACKUP_PENDING_FILES_TOTAL": 5,
                        "BACKUP_PENDING_GB_TOTAL": 2.0,
                        "BACKUP_ERROR_FILES_TOTAL": 1,
                        "BACKUP_ERROR_GB_TOTAL": 0.25,
                        "PROCESSING_DONE_FILES_TOTAL": 70,
                        "PROCESSING_DONE_GB_TOTAL": 6.5,
                        "PROCESSING_PENDING_FILES_TOTAL": 4,
                        "PROCESSING_PENDING_GB_TOTAL": 1.75,
                        "PROCESSING_ERROR_FILES_TOTAL": 3,
                        "PROCESSING_ERROR_GB_TOTAL": 0.5,
                    },
                ):
                    with patch.object(
                        self.module,
                        "_get_yearly_status_breakdown_for_host",
                        return_value=[],
                    ):
                        with patch.object(
                            self.module,
                            "_get_host_fact_spectrum_total",
                            return_value=321,
                        ):
                            stats = self.module.get_host_statistics(42)

        self.assertEqual(stats["BACKUP_QUEUE_FILES_TOTAL"], 3)
        self.assertEqual(stats["BACKUP_QUEUE_GB_TOTAL"], 1.5)
        self.assertEqual(stats["PROCESSING_QUEUE_FILES_TOTAL"], 2)
        self.assertEqual(stats["PROCESSING_QUEUE_GB_TOTAL"], 0.75)
        self.assertEqual(stats["BACKUP_PENDING_FILES_TOTAL"], 5)
        self.assertEqual(stats["PROCESSING_PENDING_FILES_TOTAL"], 4)
        self.assertEqual(stats["FACT_SPECTRUM_TOTAL"], 321)
        self.assertTrue(bp_connection.closed)
        self.assertEqual(len(bp_cursor.executed), 3)
        self.assertIn("FROM FILE_TASK", bp_cursor.executed[1][0])
        self.assertEqual(bp_cursor.executed[1][1], (42,))


if __name__ == "__main__":
    unittest.main()
