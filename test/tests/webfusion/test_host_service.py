"""Validation tests for `webfusion.modules.host.service`."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path(__file__).resolve().parents[3] / "src/webfusion/modules/host/service.py"


def load_host_service():
    """Import the host service with lightweight DB stubs only."""
    stub_db = types.ModuleType("db")
    stub_db.get_connection_bpdata = lambda: None
    stub_db.get_connection_rfdata = lambda: None
    stub_db.get_connection_summary = lambda: None

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
    """Validate summary-backed host and server service helpers."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_host_service()

    def test_build_host_list_cache_key_normalizes_inputs(self):
        self.assertEqual(
            self.module._build_host_list_cache_key(
                online_only=True,
                search="  RFeye002274  ",
            ),
            (True, "rfeye002274"),
        )
        self.assertEqual(
            self.module._build_host_list_cache_key(),
            (False, ""),
        )

    def test_operational_metrics_snapshot_keeps_only_appcataloga_contract(self):
        with patch.object(
            self.module,
            "_get_host_current_snapshot_row",
            return_value={
                "ID_HOST": 73,
                "NA_HOST_NAME": "RFEye000073",
                "NU_FACT_SPECTRUM_TOTAL": 4900933,
                "NA_HOST_ADDRESS": "172.16.18.73",
                "DT_LAST_DISCOVERY": "2026-08-25 12:00:00",
            },
        ):
            payload = self.module.get_host_operational_metrics_snapshot(73)

        self.assertEqual(payload["ID_HOST"], 73)
        self.assertEqual(payload["NA_HOST_NAME"], "RFEye000073")
        self.assertEqual(payload["NU_FACT_SPECTRUM_TOTAL"], 4900933)
        self.assertNotIn("NA_HOST_ADDRESS", payload)
        self.assertNotIn("DT_LAST_DISCOVERY", payload)
        self.assertIn("VL_BACKUP_DONE_GB_TOTAL", payload)
        self.assertIsNone(payload["VL_BACKUP_DONE_GB_TOTAL"])

    def test_current_activity_prefers_running_host_task(self):
        class FakeCursor:
            def __init__(self):
                self.executions = []

            def execute(self, query, params):
                self.executions.append((query, params))

            def fetchone(self):
                return {
                    "TASK_ID": 19,
                    "HOST_ID": 73,
                    "TASK_TYPE": 2,
                    "TASK_STATUS": 2,
                    "MESSAGE": "Discovery: listando arquivos remotos.",
                    "UPDATED_AT": None,
                }

        class FakeConnection:
            def __init__(self):
                self.cursor_instance = FakeCursor()
                self.closed = False

            def cursor(self):
                return self.cursor_instance

            def close(self):
                self.closed = True

        connection = FakeConnection()
        with patch.object(self.module, "get_connection_bpdata", return_value=connection):
            activity = self.module.get_host_current_activity(73)

        self.assertEqual(activity["source"], "host-task")
        self.assertEqual(activity["task_id"], 19)
        self.assertEqual(activity["task_type"], 2)
        self.assertEqual(len(connection.cursor_instance.executions), 1)
        self.assertTrue(connection.closed)

    def test_current_activity_falls_back_to_next_backup_file(self):
        class FakeCursor:
            def __init__(self):
                self.executions = []
                self.rows = [
                    None,
                    {
                        "TASK_ID": 55,
                        "HOST_ID": 73,
                        "TASK_TYPE": 1,
                        "TASK_STATUS": 1,
                        "FILE_NAME": "capture_20260826.zip",
                        "FILE_PATH": "/captures",
                        "FILE_SIZE_KB": 2048,
                        "MESSAGE": "Aguardando worker de backup.",
                        "UPDATED_AT": None,
                    },
                    None,
                ]

            def execute(self, query, params):
                self.executions.append((query, params))

            def fetchone(self):
                return self.rows.pop(0)

        class FakeConnection:
            def __init__(self):
                self.cursor_instance = FakeCursor()

            def cursor(self):
                return self.cursor_instance

            def close(self):
                pass

        with patch.object(
            self.module,
            "get_connection_bpdata",
            return_value=FakeConnection(),
        ):
            activity = self.module.get_host_current_activity(73)

        self.assertEqual(activity["source"], "backup-file")
        self.assertEqual(activity["file_name"], "capture_20260826.zip")
        self.assertEqual(activity["file_size_kb"], 2048)
        self.assertEqual(activity["status"], 1)

    def test_current_activity_keeps_running_file_stage_visible(self):
        class FakeCursor:
            def __init__(self):
                self.rows = [
                    None,
                    None,
                    {
                        "TASK_ID": 81,
                        "HOST_ID": 73,
                        "TASK_TYPE": 2,
                        "TASK_STATUS": 2,
                        "FILE_NAME": "capture_20260826.zip",
                        "FILE_PATH": "/captures",
                        "MESSAGE": "Processamento do arquivo em execução.",
                        "UPDATED_AT": None,
                    },
                ]

            def execute(self, query, params):
                pass

            def fetchone(self):
                return self.rows.pop(0)

        class FakeConnection:
            def __init__(self):
                self.cursor_instance = FakeCursor()

            def cursor(self):
                return self.cursor_instance

            def close(self):
                pass

        with patch.object(
            self.module,
            "get_connection_bpdata",
            return_value=FakeConnection(),
        ):
            activity = self.module.get_host_current_activity(73)

        self.assertEqual(activity["source"], "file-task")
        self.assertEqual(activity["task_type"], 2)
        self.assertEqual(activity["status"], 2)

    def test_running_backup_wins_over_waiting_host_task(self):
        class FakeCursor:
            def __init__(self):
                self.rows = [
                    {
                        "TASK_ID": 19,
                        "HOST_ID": 73,
                        "TASK_TYPE": 2,
                        "TASK_STATUS": 1,
                        "MESSAGE": "Discovery aguardando worker.",
                        "UPDATED_AT": None,
                    },
                    {
                        "TASK_ID": 55,
                        "HOST_ID": 73,
                        "TASK_TYPE": 1,
                        "TASK_STATUS": 2,
                        "FILE_NAME": "capture_20260826.zip",
                        "FILE_PATH": "/captures",
                        "MESSAGE": "Transferência iniciada.",
                        "UPDATED_AT": None,
                    },
                ]

            def execute(self, query, params):
                pass

            def fetchone(self):
                return self.rows.pop(0)

        class FakeConnection:
            def __init__(self):
                self.cursor_instance = FakeCursor()

            def cursor(self):
                return self.cursor_instance

            def close(self):
                pass

        with patch.object(
            self.module,
            "get_connection_bpdata",
            return_value=FakeConnection(),
        ):
            activity = self.module.get_host_current_activity(73)

        self.assertEqual(activity["source"], "backup-file")
        self.assertEqual(activity["status"], 2)

    def test_backup_activity_detail_survives_promotion_to_processing(self):
        class FakeCursor:
            def __init__(self):
                self.executions = []

            def execute(self, query, params):
                self.executions.append((query, params))

            def fetchone(self):
                return {
                    "TASK_ID": 55,
                    "HOST_ID": 73,
                    "TASK_TYPE": 2,
                    "TASK_STATUS": 1,
                    "FILE_NAME": "capture_20260826.zip",
                    "FILE_PATH": "/captures",
                    "MESSAGE": "Backup concluído para capture_20260826.zip.",
                    "UPDATED_AT": None,
                }

        class FakeConnection:
            def __init__(self):
                self.cursor_instance = FakeCursor()

            def cursor(self):
                return self.cursor_instance

            def close(self):
                pass

        connection = FakeConnection()
        with patch.object(self.module, "get_connection_bpdata", return_value=connection):
            activity = self.module.get_host_activity_detail(73, "backup-file", 55)

        self.assertEqual(activity["task_type"], 2)
        self.assertEqual(
            activity["message"],
            "Backup concluído para capture_20260826.zip.",
        )
        self.assertEqual(connection.cursor_instance.execution[1], (55, 73))

    def test_file_activity_detail_reads_processing_result_from_history_after_delete(self):
        class FakeCursor:
            def __init__(self):
                self.rows = [
                    None,
                    {
                        "HOST_ID": 73,
                        "FILE_PATH": "/captures",
                        "FILE_NAME": "capture_20260826.zip",
                        "TASK_STATUS": 0,
                        "MESSAGE": "Processing Done | file=capture_20260826.zip",
                        "UPDATED_AT": None,
                    },
                ]
                self.executions = []

            def execute(self, query, params):
                self.executions.append((query, params))

            def fetchone(self):
                return self.rows.pop(0)

        class FakeConnection:
            def __init__(self):
                self.cursor_instance = FakeCursor()

            def cursor(self):
                return self.cursor_instance

            def close(self):
                pass

        connection = FakeConnection()
        with patch.object(self.module, "get_connection_bpdata", return_value=connection):
            activity = self.module.get_host_activity_detail(
                73,
                "file-task",
                55,
                "/captures",
                "capture_20260826.zip",
            )

        self.assertEqual(activity["status"], 0)
        self.assertEqual(activity["task_type"], 2)
        self.assertEqual(activity["message"], "Processing Done | file=capture_20260826.zip")
        self.assertEqual(
            connection.cursor_instance.executions[1][1],
            (73, "/captures", "capture_20260826.zip"),
        )

    def test_processed_file_spectrum_metadata_uses_the_repository_file_name(self):
        class FakeCursor:
            def __init__(self):
                self.execution = None

            def execute(self, query, params):
                self.execution = (query, params)

            def fetchone(self):
                return {"ID_FILE": 41, "SPECTRUM_COUNT": 15}

            def fetchall(self):
                return [{"ID_SPECTRUM": 9}]

        class FakeConnection:
            def __init__(self):
                self.cursor_instance = FakeCursor()

            def cursor(self):
                return self.cursor_instance

            def close(self):
                pass

        connection = FakeConnection()
        with patch.object(self.module, "get_connection_rfdata", return_value=connection):
            metadata = self.module.get_processed_file_spectrum_metadata("p-123--capture.bin")

        self.assertEqual(metadata["ID_FILE"], 41)
        self.assertEqual(metadata["SPECTRA"], [{"ID_SPECTRUM": 9}])
        self.assertEqual(
            connection.cursor_instance.executions[0][1],
            ("reposfi", "p-123--capture.bin"),
        )
        self.assertEqual(
            connection.cursor_instance.executions[1][1],
            (41, self.module.PROCESSED_FILE_SPECTRUM_METADATA_LIMIT),
        )

    def test_format_structured_error_bucket_renders_code_and_summary(self):
        self.assertEqual(
            self.module._format_structured_error_bucket(
                {
                    "ERROR_CODE": "NO_VALID_SPECTRA",
                    "ERROR_SUMMARY": "BIN discarded: no valid spectra after validation",
                },
                default_label="Processing Error",
            ),
            (
                "Processing Error | [ERROR] [code=NO_VALID_SPECTRA] "
                "BIN discarded: no valid spectra after validation"
            ),
        )
        self.assertIsNone(
            self.module._format_structured_error_bucket(
                {"ERROR_CODE": "", "ERROR_SUMMARY": ""},
                default_label="Processing Error",
            )
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
            "[code=UNCLASSIFIED] Processing failed without structured detail",
        )

        self.assertEqual(
            self.module._canonicalize_processing_error_message(
                "Processing Error | [ERROR] [stage=PROCESS] [type=BinValidationError] "
                "APP_ANALISE returned invalid Answer.Spectra type: {'Receiver': 'CWSM21100001'}"
            ),
            (
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
                    "TASK_STATE": "ERROR",
                    "ERROR_MESSAGE": (
                        "[code=BIN_PAYLOAD_VALIDATION_FAILED] "
                        "Payload validation failed during processing"
                    ),
                    "ERROR_COUNT": 12,
                },
                {
                    "TASK_STATE": "ERROR",
                    "ERROR_MESSAGE": (
                        "[code=APP_ANALISE_INVALID_SPECTRA_TYPE] "
                        "APP_ANALISE returned invalid Answer.Spectra type"
                    ),
                    "ERROR_COUNT": 5,
                },
                {
                    "TASK_STATE": "ERROR",
                    "ERROR_MESSAGE": (
                        "[code=UNCLASSIFIED] Processing failed without structured detail"
                    ),
                    "ERROR_COUNT": 4,
                },
            ],
        )

    def test_merge_grouped_processing_errors_collapses_code_variants_with_volatile_detail(self):
        rows = [
            {
                "ERROR_CODE": "APP_ANALISE_FILE_UNAVAILABLE",
                "ERROR_SUMMARY": (
                    "APP_ANALISE source file unavailable before request _ _4_DONE.zip]"
                ),
                "ERROR_COUNT": 4,
            },
            {
                "ERROR_CODE": "APP_ANALISE_FILE_UNAVAILABLE",
                "ERROR_SUMMARY": "APP_ANALISE file unavailable during processing",
                "ERROR_COUNT": 3,
            },
            {
                "ERROR_CODE": "APP_ANALISE_TRANSIENT_SERVICE_FAILURE",
                "ERROR_SUMMARY": "Transient appAnalise processing failure Connection refused]",
                "ERROR_COUNT": 5,
            },
            {
                "ERROR_CODE": "APP_ANALISE_TRANSIENT_SERVICE_FAILURE",
                "ERROR_SUMMARY": "Transient appAnalise processing failure",
                "ERROR_COUNT": 13,
            },
            {
                "ERROR_CODE": "UNCLASSIFIED",
                "ERROR_SUMMARY": "Unexpected processing loop failure",
                "ERROR_COUNT": 19,
            },
            {
                "ERROR_CODE": "UNCLASSIFIED",
                "ERROR_SUMMARY": "Failed to persist processed spectra batch",
                "ERROR_COUNT": 1,
            },
        ]

        merged = self.module._merge_grouped_processing_errors(rows)

        self.assertEqual(
            merged,
            [
                {
                    "TASK_STATE": "ERROR",
                    "ERROR_MESSAGE": "[code=UNCLASSIFIED] Unexpected processing loop failure",
                    "ERROR_COUNT": 19,
                },
                {
                    "TASK_STATE": "ERROR",
                    "ERROR_MESSAGE": (
                        "[code=APP_ANALISE_TRANSIENT_SERVICE_FAILURE] "
                        "Transient appAnalise processing failure"
                    ),
                    "ERROR_COUNT": 18,
                },
                {
                    "TASK_STATE": "ERROR",
                    "ERROR_MESSAGE": "[code=APP_ANALISE_FILE_UNAVAILABLE] APP_ANALISE file unavailable",
                    "ERROR_COUNT": 7,
                },
                {
                    "TASK_STATE": "ERROR",
                    "ERROR_MESSAGE": "[code=UNCLASSIFIED] Failed to persist processed spectra batch",
                    "ERROR_COUNT": 1,
                },
            ],
        )

    def test_get_server_summary_metrics_reads_materialized_server_summary(self):
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

        summary_cursor = FakeCursor(
            [
                {
                    "ID_SUMMARY": 1,
                    "NA_CURRENT_MONTH_LABEL": "2026-04",
                    "NU_BACKUP_DONE_THIS_MONTH": 12,
                    "VL_BACKUP_DONE_GB_THIS_MONTH": 3.75,
                    "NU_DISCOVERED_FILES_TOTAL": 120,
                    "VL_DISCOVERED_GB_TOTAL": 55.5,
                    "NU_BACKUP_DONE_FILES_TOTAL": 88,
                    "VL_BACKUP_DONE_GB_TOTAL": 44.25,
                    "NU_BACKUP_PENDING_FILES_TOTAL": 19,
                    "VL_BACKUP_PENDING_GB_TOTAL": 22.5,
                    "NU_BACKUP_ERROR_FILES_TOTAL": 3,
                    "VL_BACKUP_ERROR_GB_TOTAL": 1.75,
                    "NU_BACKUP_QUEUE_FILES_TOTAL": 7,
                    "VL_BACKUP_QUEUE_GB_TOTAL": 15.5,
                    "NU_PROCESSING_PENDING_FILES_TOTAL": 11,
                    "VL_PROCESSING_PENDING_GB_TOTAL": 9.5,
                    "NU_PROCESSING_DONE_FILES_TOTAL": 44,
                    "VL_PROCESSING_DONE_GB_TOTAL": 28.0,
                    "NU_PROCESSING_ERROR_FILES_TOTAL": 2,
                    "VL_PROCESSING_ERROR_GB_TOTAL": 0.5,
                    "NU_PROCESSING_QUEUE_FILES_TOTAL": 4,
                    "VL_PROCESSING_QUEUE_GB_TOTAL": 8.25,
                    "NU_FACT_SPECTRUM_TOTAL": 987,
                }
            ]
        )
        summary_connection = FakeConnection(summary_cursor)

        self.module._SERVER_SUMMARY_CACHE["payload"] = None
        self.module._SERVER_SUMMARY_CACHE["expires_at"] = 0.0

        with patch.object(self.module.time, "monotonic", return_value=100.0):
            with patch.object(
                self.module,
                "get_connection_summary",
                return_value=summary_connection,
            ):
                summary = self.module.get_server_summary_metrics()

        self.assertEqual(summary["CURRENT_MONTH_LABEL"], "2026-04")
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
        self.assertTrue(summary_connection.closed)
        self.assertEqual(len(summary_cursor.executed), 1)
        self.assertIn("FROM SERVER_CURRENT_SUMMARY", summary_cursor.executed[0][0])

    def test_get_server_processing_error_overview_reads_summary_only(self):
        class FakeCursor:
            def __init__(self, rows):
                self.rows = list(rows)
                self.executed = []

            def execute(self, query, params=None):
                self.executed.append((query, params))

            def fetchall(self):
                if not self.rows:
                    return []
                return self.rows.pop(0)

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor
                self.closed = False

            def cursor(self):
                return self._cursor

            def close(self):
                self.closed = True

        summary_cursor = FakeCursor(
            [[
                {
                    "ERROR_DOMAIN": "PROCESSING",
                    "TASK_STATE": "FROZEN",
                    "ERROR_CODE": "NO_VALID_SPECTRA",
                    "ERROR_SUMMARY": "BIN discarded: no valid spectra after validation",
                    "ERROR_COUNT": 5,
                }
            ]]
        )
        summary_connection = FakeConnection(summary_cursor)

        self.module._GROUPED_PROCESSING_ERRORS_CACHE["payload"] = None
        self.module._GROUPED_PROCESSING_ERRORS_CACHE["expires_at"] = 0.0

        with patch.object(self.module.time, "monotonic", return_value=100.0):
            with patch.object(
                self.module,
                "get_connection_summary",
                return_value=summary_connection,
            ):
                payload = self.module.get_server_processing_error_overview()

        self.assertEqual(payload["error_group_count"], 1)
        self.assertEqual(payload["error_total_occurrences"], 5)
        self.assertEqual(payload["rows"][0]["ERROR_COUNT"], 5)
        self.assertEqual(payload["rows"][0]["TASK_STATE"], "FROZEN")
        self.assertIn("NO_VALID_SPECTRA", payload["rows"][0]["ERROR_MESSAGE"])
        self.assertIn("NA_TASK_STATE AS TASK_STATE", summary_cursor.executed[0][0])
        self.assertTrue(summary_connection.closed)

    def test_get_server_overview_reads_materialized_host_totals(self):
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

        summary_cursor = FakeCursor(
            [
                {
                    "NA_CURRENT_MONTH_LABEL": "2026-04",
                    "NU_TOTAL_HOSTS": 20,
                    "NU_ONLINE_HOSTS": 13,
                    "NU_OFFLINE_HOSTS": 7,
                    "NU_BUSY_HOSTS": 2,
                }
            ]
        )
        summary_connection = FakeConnection(summary_cursor)

        self.module._SERVER_OVERVIEW_CACHE["payload"] = None
        self.module._SERVER_OVERVIEW_CACHE["expires_at"] = 0.0

        with patch.object(self.module.time, "monotonic", return_value=100.0):
            with patch.object(
                self.module,
                "get_connection_summary",
                return_value=summary_connection,
            ):
                with patch.object(
                    self.module,
                    "_get_runtime_overview",
                    return_value={
                        "memory": {"used_human": "1 GB", "total_human": "2 GB", "available_human": "1 GB", "use_percent": 50},
                        "reposfi": {"mounted": True, "used_human": "1 GB", "total_human": "4 GB", "free_human": "3 GB", "use_percent": 25, "path": "/mnt/reposfi"},
                        "appanalise": {"online": True, "host": "appanalise.local", "latency_ms": 10.0, "error": None},
                    },
                ):
                    overview = self.module.get_server_overview()

        self.assertEqual(overview["TOTAL_HOSTS"], 20)
        self.assertEqual(overview["ONLINE_HOSTS"], 13)
        self.assertEqual(overview["OFFLINE_HOSTS"], 7)
        self.assertEqual(overview["BUSY_HOSTS"], 2)
        self.assertTrue(summary_connection.closed)

    def test_get_host_statistics_reads_summary_snapshot_current_month_backup_metrics(self):
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

            def fetchall(self):
                if not self.rows:
                    return []
                return self.rows.pop(0)

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor
                self.closed = False

            def cursor(self):
                return self._cursor

            def close(self):
                self.closed = True

        snapshot_cursor = FakeCursor(
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
                    "DT_LAST_DISCOVERY_COMPLETED_AT": "2026-04-08 11:05:00",
                    "NU_LAST_DISCOVERY_FILE_COUNT": 6,
                    "VL_LAST_DISCOVERY_KB": 1280,
                    "DT_LAST_DISCOVERY_WITH_FILES": "2026-04-08 11:05:00",
                    "DT_LAST_BACKUP": "2026-04-08 10:00:00",
                    "NU_BACKUP_DONE_THIS_MONTH": 8,
                    "VL_BACKUP_DONE_GB_THIS_MONTH": 2.25,
                    "DT_LAST_PROCESSING": "2026-04-08 09:00:00",
                    "NU_PROCESSING_DONE_THIS_MONTH": 7,
                    "VL_PROCESSING_DONE_GB_THIS_MONTH": 1.75,
                    "NU_DISCOVERED_FILES_TOTAL": 123,
                    "VL_DISCOVERED_GB_TOTAL": 10.5,
                    "NU_BACKUP_DONE_FILES_TOTAL": 80,
                    "VL_BACKUP_DONE_GB_TOTAL": 8.0,
                    "NU_PROCESSING_DONE_FILES_TOTAL": 70,
                    "VL_PROCESSING_DONE_GB_TOTAL": 6.5,
                    "NU_BACKUP_PENDING_FILES_CURRENT": 5,
                    "VL_BACKUP_PENDING_GB_CURRENT": 2.0,
                    "NU_BACKUP_ERROR_FILES_CURRENT": 1,
                    "VL_BACKUP_ERROR_GB_CURRENT": 0.25,
                    "NU_BACKUP_SUSPENDED_FILES_CURRENT": 2,
                    "VL_BACKUP_SUSPENDED_GB_CURRENT": 0.5,
                    "NU_PROCESSING_PENDING_FILES_CURRENT": 4,
                    "VL_PROCESSING_PENDING_GB_CURRENT": 1.75,
                    "NU_PROCESSING_ERROR_FILES_CURRENT": 3,
                    "VL_PROCESSING_ERROR_GB_CURRENT": 0.5,
                    "NU_PROCESSING_FROZEN_FILES_CURRENT": 1,
                    "VL_PROCESSING_FROZEN_GB_CURRENT": 0.25,
                    "NU_BACKUP_QUEUE_FILES_TOTAL": 3,
                    "VL_BACKUP_QUEUE_GB_TOTAL": 1.5,
                    "NU_BACKUP_QUEUE_RUNNING_FILES_TOTAL": 1,
                    "VL_BACKUP_QUEUE_RUNNING_GB_TOTAL": 0.75,
                    "NU_BACKUP_QUEUE_SUSPENDED_FILES_TOTAL": 2,
                    "VL_BACKUP_QUEUE_SUSPENDED_GB_TOTAL": 0.5,
                    "NU_PROCESSING_QUEUE_FILES_TOTAL": 2,
                    "VL_PROCESSING_QUEUE_GB_TOTAL": 0.75,
                    "NU_PROCESSING_QUEUE_RUNNING_FILES_TOTAL": 1,
                    "VL_PROCESSING_QUEUE_RUNNING_GB_TOTAL": 0.5,
                    "NU_PROCESSING_QUEUE_FROZEN_FILES_TOTAL": 1,
                    "VL_PROCESSING_QUEUE_FROZEN_GB_TOTAL": 0.25,
                    "NU_FACT_SPECTRUM_TOTAL": 321,
                    "NU_PAYLOAD_DELETED_FILES_TOTAL": 9,
                    "VL_PAYLOAD_DELETED_GB_TOTAL": 1.25,
                    "IS_SSH_FAILURE": 1,
                    "DT_LAST_SSH_EVALUATED_AT": "2026-04-08 12:00:00",
                    "DT_LAST_SSH_FAILURE_AT": "2026-04-08 11:58:00",
                    "NA_LAST_SSH_FAILURE_CODE": "AUTHENTICATION",
                    "NA_LAST_SSH_FAILURE_DESCRIPTION": "Authentication failed",
                    "IS_GPS_GNSS_UNAVAILABLE": 1,
                    "DT_LAST_GPS_GNSS_EVALUATED_AT": "2026-04-08 11:30:00",
                    "DT_LAST_GPS_GNSS_UNAVAILABLE_AT": "2026-04-08 11:30:00",
                    "NA_LAST_GPS_GNSS_UNAVAILABLE_DESCRIPTION": "GPS unavailable",
                    "NA_LAST_GPS_GNSS_UNAVAILABLE_HOST_FILE_NAME": "sample.bin",
                    "DT_LAST_ERROR_AT": "2026-04-08 08:00:00",
                    "NA_LAST_ERROR_SUMMARY": "Processing Error",
                },
            ]
        )
        snapshot_connection = FakeConnection(snapshot_cursor)

        monthly_cursor = FakeCursor(
            [
                [
                    {
                        "DT_REFERENCE_MONTH": self.module.datetime(2026, 4, 1),
                        "NU_DISCOVERED_FILES": 120,
                        "VL_DISCOVERED_GB": 10.5,
                        "NU_BACKUP_DONE_FILES": 80,
                        "VL_BACKUP_DONE_GB": 8.0,
                        "NU_BACKUP_PENDING_FILES": 5,
                        "VL_BACKUP_PENDING_GB": 2.0,
                        "NU_BACKUP_ERROR_FILES": 1,
                        "VL_BACKUP_ERROR_GB": 0.25,
                        "NU_PROCESSING_DONE_FILES": 70,
                        "VL_PROCESSING_DONE_GB": 6.5,
                        "NU_PROCESSING_PENDING_FILES": 4,
                        "VL_PROCESSING_PENDING_GB": 1.75,
                        "NU_PROCESSING_ERROR_FILES": 3,
                        "VL_PROCESSING_ERROR_GB": 0.5,
                    }
                ]
            ]
        )
        monthly_connection = FakeConnection(monthly_cursor)

        self.module._HOST_STATISTICS_CACHE.clear()

        with patch.object(self.module.time, "monotonic", return_value=100.0):
            with patch.object(
                self.module,
                "get_connection_summary",
                side_effect=[snapshot_connection, monthly_connection],
            ):
                stats = self.module.get_host_statistics(42)

        self.assertEqual(stats["BACKUP_DONE_THIS_MONTH"], 8)
        self.assertEqual(stats["BACKUP_DONE_GB_THIS_MONTH"], 2.25)
        self.assertEqual(stats["LAST_DISCOVERY_VOLUME_LABEL"], "1.2 MB")
        self.assertEqual(stats["BACKUP_QUEUE_FILES_TOTAL"], 3)
        self.assertEqual(stats["BACKUP_QUEUE_GB_TOTAL"], 1.5)
        self.assertEqual(stats["BACKUP_QUEUE_RUNNING_FILES_TOTAL"], 1)
        self.assertEqual(stats["BACKUP_QUEUE_SUSPENDED_FILES_TOTAL"], 2)
        self.assertEqual(stats["PROCESSING_QUEUE_FILES_TOTAL"], 2)
        self.assertEqual(stats["PROCESSING_QUEUE_GB_TOTAL"], 0.75)
        self.assertEqual(stats["PROCESSING_QUEUE_RUNNING_FILES_TOTAL"], 1)
        self.assertEqual(stats["PROCESSING_QUEUE_FROZEN_FILES_TOTAL"], 1)
        self.assertEqual(stats["BACKUP_PENDING_FILES_TOTAL"], 5)
        self.assertEqual(stats["BACKUP_SUSPENDED_FILES_TOTAL"], 2)
        self.assertEqual(stats["PROCESSING_PENDING_FILES_TOTAL"], 4)
        self.assertEqual(stats["PROCESSING_FROZEN_FILES_TOTAL"], 1)
        self.assertEqual(stats["PAYLOAD_DELETED_FILES_TOTAL"], 9)
        self.assertEqual(stats["PAYLOAD_DELETED_GB_TOTAL"], 1.25)
        self.assertEqual(stats["FACT_SPECTRUM_TOTAL"], 321)
        self.assertNotIn("PENDING_GB", stats)
        self.assertNotIn("DONE_GB", stats)
        self.assertNotIn("GROUPED_PROCESSING_ERRORS", stats)
        self.assertNotIn("MATCHED_RFDATA_EQUIPMENTS", stats)
        self.assertNotIn("LOCATION_HISTORY", stats)
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(monthly_connection.closed)
        self.assertEqual(len(snapshot_cursor.executed), 1)
        self.assertIn("FROM HOST_CURRENT_SNAPSHOT", snapshot_cursor.executed[0][0])
        self.assertIn("IS_SSH_FAILURE", snapshot_cursor.executed[0][0])
        self.assertIn("VL_LAST_DISCOVERY_KB", snapshot_cursor.executed[0][0])
        self.assertIn("NU_PROCESSING_QUEUE_FROZEN_FILES_TOTAL", snapshot_cursor.executed[0][0])
        self.assertNotIn("DT_LAST_DISCOVERY,", snapshot_cursor.executed[0][0])
        self.assertEqual(snapshot_cursor.executed[0][1], (42,))
        self.assertEqual(len(monthly_cursor.executed), 1)
        self.assertIn("FROM HOST_MONTHLY_METRIC", monthly_cursor.executed[0][0])
        self.assertEqual(monthly_cursor.executed[0][1], (42,))

    def test_get_hosts_reads_summary_snapshot_rows(self):
        class FakeCursor:
            def __init__(self, rows):
                self.rows = list(rows)
                self.executed = []

            def execute(self, query, params=None):
                self.executed.append((query, params))

            def fetchall(self):
                if not self.rows:
                    return []
                return self.rows.pop(0)

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor
                self.closed = False

            def cursor(self):
                return self._cursor

            def close(self):
                self.closed = True

        summary_cursor = FakeCursor(
            [[
                {
                    "ID_HOST": 42,
                    "NA_HOST_NAME": "rfeye002274",
                    "NA_HOST_ADDRESS": "10.0.0.42",
                    "NA_HOST_PORT": 22,
                    "IS_OFFLINE": 0,
                    "IS_BUSY": 1,
                    "DT_LAST_CHECK": "2026-04-08 12:00:00",
                    "DT_LAST_DISCOVERY": "2026-04-08 11:00:00",
                    "DT_LAST_BACKUP": "2026-04-08 10:00:00",
                    "DT_LAST_PROCESSING": "2026-04-08 09:00:00",
                    "NU_PENDING_FILE_BACKUP_TASKS": 5,
                    "NU_ERROR_FILE_BACKUP_TASKS": 1,
                    "NU_PENDING_FILE_PROCESS_TASKS": 4,
                    "NU_ERROR_FILE_PROCESS_TASKS": 3,
                    "PENDING_BACKUP_GB": 1.5,
                }
            ]]
        )
        summary_connection = FakeConnection(summary_cursor)

        self.module._SERVER_HOST_ROWS_CACHE.clear()

        with patch.object(self.module.time, "monotonic", return_value=100.0):
            with patch.object(
                self.module,
                "get_connection_summary",
                return_value=summary_connection,
            ):
                rows = self.module.get_hosts(search="rfeye", online_only=True)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["PENDING_BACKUP_GB"], 1.5)
        self.assertEqual(rows[0]["PENDING_BACKUP_MB"], 1536.0)
        self.assertEqual(rows[0]["STATUS_LABEL"], "Online")
        self.assertEqual(rows[0]["BUSY_LABEL"], "Busy")
        self.assertTrue(summary_connection.closed)
        self.assertEqual(len(summary_cursor.executed), 1)
        self.assertIn("FROM HOST_CURRENT_SNAPSHOT", summary_cursor.executed[0][0])
        self.assertEqual(summary_cursor.executed[0][1], ["%rfeye%"])

    def test_get_host_location_history_exposes_county_and_district_ids(self):
        class FakeCursor:
            def __init__(self):
                self.executed = []
                self.responses = [
                    [
                        {
                            "ID_EQUIPMENT": 133,
                            "NA_EQUIPMENT": "ermxes03",
                            "MATCH_TYPE": "exact_normalized",
                            "MATCH_CONFIDENCE": 1.0,
                        }
                    ],
                    [
                        {
                            "ID_SITE": 237,
                            "ID_COUNTY": 3205309,
                            "ID_DISTRICT": 181,
                            "LOCALITY_LABEL": "Enseada do Sua · Vitoria/ES",
                            "COUNTY_NAME": "Vitoria",
                            "STATE_NAME": "Espirito Santo",
                            "STATE_CODE": "ES",
                            "FIRST_SEEN_AT": "2025-06-28 12:22:53",
                            "LAST_SEEN_AT": "2026-01-10 09:01:12",
                            "SPECTRUM_COUNT": 1925,
                        }
                    ],
                ]

            def execute(self, query, params=None):
                self.executed.append((query, params))

            def fetchall(self):
                if not self.responses:
                    return []
                return self.responses.pop(0)

        class FakeConnection:
            def __init__(self, cursor):
                self._cursor = cursor
                self.closed = False

            def cursor(self):
                return self._cursor

            def close(self):
                self.closed = True

        summary_cursor = FakeCursor()
        summary_cursor.responses[1][0]["NU_VISIT"] = 3
        summary_cursor.responses[1][0]["IS_OVERLAPPING"] = 1
        earlier_visit = dict(summary_cursor.responses[1][0])
        earlier_visit.update(NU_VISIT=1, IS_OVERLAPPING=0,
                             FIRST_SEEN_AT="01/01/2024 10:00:00",
                             LAST_SEEN_AT="02/01/2024 10:00:00")
        summary_cursor.responses[1].append(earlier_visit)
        summary_connection = FakeConnection(summary_cursor)

        self.module._HOST_LOCATION_HISTORY_CACHE.clear()

        with patch.object(self.module.time, "monotonic", return_value=100.0):
            with patch.object(
                self.module,
                "get_connection_summary",
                return_value=summary_connection,
            ):
                payload = self.module._get_host_location_history(10845)

        self.assertEqual(len(payload["equipment_matches"]), 1)
        self.assertEqual(payload["equipment_matches"][0]["ID_EQUIPMENT"], 133)
        self.assertEqual(len(payload["location_history"]), 2)
        self.assertEqual([row["NU_VISIT"] for row in payload["location_history"]], [3, 1])
        self.assertEqual([row["IS_OVERLAPPING"] for row in payload["location_history"]], [True, False])
        self.assertEqual(payload["location_history"][0]["ID_SITE"], 237)
        self.assertEqual(payload["location_history"][0]["ID_COUNTY"], 3205309)
        self.assertEqual(payload["location_history"][0]["ID_DISTRICT"], 181)
        self.assertEqual(payload["location_history"][0]["SPECTRUM_COUNT"], 1925)
        self.assertTrue(summary_connection.closed)
        self.assertEqual(len(summary_cursor.executed), 2)
        self.assertIn("FROM HOST_LOCATION_TIMELINE_SUMMARY", summary_cursor.executed[1][0])
        self.assertIn("ORDER BY visit.DT_FIRST_SEEN_AT DESC", summary_cursor.executed[1][0])


if __name__ == "__main__":
    unittest.main()
