"""Validation tests for the connectivity-test polling payload."""

from __future__ import annotations

import importlib
import sys
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch


WEBFUSION_ROOT = Path(__file__).resolve().parents[3] / "src/webfusion"


def load_host_routes():
    """Reload host routes with only the dependencies needed by pure helpers."""
    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

    fake_flask = ModuleType("flask")

    class FakeBlueprint:
        def __init__(self, *args, **kwargs):
            pass

        def route(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

    fake_flask.Blueprint = FakeBlueprint
    fake_flask.Response = lambda *args, **kwargs: None
    fake_flask.current_app = SimpleNamespace(logger=SimpleNamespace(exception=lambda *args, **kwargs: None))
    class JsonResponse(dict):
        def __init__(self, payload):
            super().__init__(payload)
            self.headers = {}

    fake_flask.jsonify = JsonResponse
    fake_flask.render_template = lambda *args, **kwargs: None
    fake_flask.request = SimpleNamespace(authorization=None, args={})

    fake_db = ModuleType("db")
    fake_db.get_connection_bpdata = lambda: None

    fake_service = ModuleType("modules.host.service")
    fake_service.FILE_TASK_DISCOVERY_TYPE = 3
    fake_service.FILE_TASK_PROCESS_TYPE = 2
    fake_service.get_all_hosts = lambda **kwargs: []
    fake_service.get_host_activity_detail = lambda host_id, source, task_id, file_path=None, file_name=None: None
    fake_service.get_host_backup_error_overview = lambda host_id: {}
    fake_service.get_host_current_activity = lambda host_id: None
    fake_service.get_host_current_flags = lambda host_id: None
    fake_service.get_host_location_history_overview = lambda host_id: {}
    fake_service.get_host_operational_metrics_snapshot = lambda host_id: {}
    fake_service.get_host_processing_error_overview = lambda host_id: {}
    fake_service.get_processed_file_spectrum_metadata = lambda server_file_name: None
    fake_service.get_host_statistics = lambda host_id: {}

    fake_usage_metrics = ModuleType("modules.server.usage_metrics")
    fake_usage_metrics.record_page_view = lambda: None

    fake_task_service = ModuleType("modules.tasks.station_service")
    fake_task_service.HOST_TASK_BACKLOG_CONTROL_TYPE = 5
    fake_task_service.HOST_TASK_BACKLOG_ROLLBACK_TYPE = 6
    fake_task_service.HOST_TASK_CHECK_CONNECTION_TYPE = 4
    fake_task_service.HOST_TASK_CHECK_TYPE = 1
    fake_task_service.HOST_TASK_INTERACTIVE_CHECK_TYPE = 99
    fake_task_service.HOST_TASK_PROCESSING_TYPE = 2
    fake_task_service.HOST_TASK_UPDATE_STATISTICS_TYPE = 3
    fake_task_service.TASK_DONE = 0
    fake_task_service.TASK_ERROR = -1
    fake_task_service.TASK_PENDING = 1
    fake_task_service.TASK_RUNNING = 2
    fake_task_service.queue_interactive_connectivity_test = lambda connection, host_id: {}

    sys.modules["flask"] = fake_flask
    sys.modules["db"] = fake_db
    sys.modules["modules.host.service"] = fake_service
    sys.modules["modules.server.usage_metrics"] = fake_usage_metrics
    sys.modules["modules.tasks.station_service"] = fake_task_service
    sys.modules.pop("modules.host.routes", None)
    return importlib.import_module("modules.host.routes")


class TestHostConnectivityRoutePayload(unittest.TestCase):
    """Keep per-step reasons available to the station-test dialog."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_host_routes()

    def test_ssh_failure_keeps_icmp_success_and_ssh_reason_separate(self):
        payload = self.module._serialize_connectivity_test_row(
            {
                "ID_HOST_TASK": 41,
                "FK_HOST": 18,
                "NU_STATUS": -1,
                "NA_HOST_NAME": "UMSRS01",
                "NA_MESSAGE": (
                    "Teste concluído com falha. "
                    "ICMP: a estação respondeu. "
                    "SSH: A conexão SSH não respondeu em até 20 segundos."
                ),
                "DT_HOST_TASK": None,
            }
        )

        self.assertEqual(payload["stage"], "ssh")
        self.assertEqual(payload["step_details"]["icmp"], "a estação respondeu.")
        self.assertEqual(
            payload["step_details"]["ssh"],
            "A conexão SSH não respondeu em até 20 segundos.",
        )

    def test_icmp_failure_marks_ssh_as_not_executed(self):
        payload = self.module._serialize_connectivity_test_row(
            {
                "ID_HOST_TASK": 42,
                "FK_HOST": 19,
                "NU_STATUS": -1,
                "NA_HOST_NAME": "RFEye002125",
                "NA_MESSAGE": (
                    "Teste concluído com falha. "
                    "ICMP: A estação não respondeu em até 10 segundos. "
                    "SSH: não executado porque o ICMP não respondeu."
                ),
                "DT_HOST_TASK": None,
            }
        )

        self.assertEqual(payload["stage"], "icmp")
        self.assertEqual(
            payload["step_details"]["icmp"],
            "A estação não respondeu em até 10 segundos.",
        )
        self.assertEqual(
            payload["step_details"]["ssh"],
            "não executado porque o ICMP não respondeu.",
        )


class TestHostActivityRoutePayload(unittest.TestCase):
    """Keep live station work understandable without exposing queue internals."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_host_routes()

    def test_activity_poll_reports_released_lock_even_with_a_waiting_task(self):
        activity = {
            "source": "host-task", "task_id": 41, "host_id": 18,
            "task_type": 2, "status": 1, "updated_at": None,
        }
        flags = {
            "IS_BUSY": False, "IS_OFFLINE": False,
            "IS_SSH_FAILURE": False, "IS_GPS_GNSS_UNAVAILABLE": True,
        }
        with patch.object(self.module, "get_host_current_activity", return_value=activity), patch.object(
            self.module, "get_host_current_flags", return_value=flags,
        ):
            response = self.module.host_current_activity(18)

        self.assertEqual(response["activity"]["status"], 1)
        self.assertEqual(response["flags"], flags)
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_activity_poll_keeps_lock_state_without_a_visible_task(self):
        with patch.object(self.module, "get_host_current_activity", return_value=None), patch.object(
            self.module, "get_host_current_flags", return_value={"IS_BUSY": True},
        ):
            response = self.module.host_current_activity(18)

        self.assertIsNone(response["activity"])
        self.assertTrue(response["flags"]["IS_BUSY"])

    def test_flags_read_failure_is_not_returned_as_success(self):
        with patch.object(self.module, "get_host_current_activity", return_value=None), patch.object(
            self.module, "get_host_current_flags", side_effect=RuntimeError("read failed"),
        ):
            response, status = self.module.host_current_activity(18)

        self.assertEqual(status, 503)
        self.assertIn("error", response)
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_discovery_activity_keeps_worker_result_and_timestamp(self):
        payload = self.module._serialize_host_activity(
            {
                "source": "host-task",
                "task_id": 41,
                "host_id": 18,
                "task_type": 2,
                "status": 2,
                "message": "Discovery: listando arquivos remotos.",
                "updated_at": datetime(2026, 8, 26, 10, 11, 12),
            }
        )

        self.assertEqual(payload["title"], "Discovery de arquivos")
        self.assertEqual(payload["status_label"], "Em execução")
        self.assertEqual(payload["message"], "Discovery: listando arquivos remotos.")
        self.assertEqual(payload["updated_at"], "2026-08-26 10:11:12")
        self.assertFalse(payload["is_terminal"])

    def test_running_backup_exposes_the_downloaded_file(self):
        payload = self.module._serialize_host_activity(
            {
                "source": "backup-file",
                "task_id": 62,
                "host_id": 18,
                "status": 2,
                "file_name": "capture_20260826.zip",
                "message": "Transferência iniciada.",
                "updated_at": None,
            }
        )

        self.assertEqual(payload["title"], "Arquivo em download")
        self.assertEqual(payload["file_name"], "capture_20260826.zip")
        self.assertFalse(payload["is_terminal"])

    def test_running_backup_exposes_transfer_bytes_for_the_progress_bar(self):
        payload = self.module._serialize_host_activity(
            {
                "source": "backup-file",
                "task_id": 62,
                "host_id": 18,
                "status": 2,
                "file_name": "capture_20260826.zip",
                "file_size_kb": 2048,
                "message": (
                    "Backup Running | file=/captures/capture_20260826.zip | "
                    "transfer=524288/2097152 bytes"
                ),
                "updated_at": None,
            }
        )

        self.assertEqual(
            payload["transfer_progress"],
            {
                "transferred_bytes": 524288,
                "total_bytes": 2097152,
                "percentage": 25.0,
                "is_determinate": True,
            },
        )

    def test_running_backup_without_byte_progress_marks_the_bar_indeterminate(self):
        payload = self.module._serialize_host_activity(
            {
                "source": "backup-file",
                "task_id": 62,
                "host_id": 18,
                "status": 2,
                "file_name": "capture_20260826.zip",
                "file_size_kb": 2048,
                "message": "Backup iniciado.",
                "updated_at": None,
            }
        )

        self.assertEqual(
            payload["transfer_progress"],
            {
                "transferred_bytes": 0,
                "total_bytes": 2097152,
                "percentage": 0.0,
                "is_determinate": False,
            },
        )

    def test_running_file_stage_remains_available_for_monitoring(self):
        payload = self.module._serialize_host_activity(
            {
                "source": "file-task",
                "task_id": 63,
                "host_id": 18,
                "task_type": 2,
                "status": 2,
                "file_name": "capture_20260826.zip",
                "message": "Processamento do arquivo em execução.",
                "updated_at": None,
            }
        )

        self.assertEqual(payload["title"], "Processamento de arquivo")
        self.assertEqual(payload["status_label"], "Em execução")
        self.assertEqual(payload["file_name"], "capture_20260826.zip")
        self.assertTrue(payload["is_processing"])
        self.assertIsNone(payload["transfer_progress"])
        self.assertFalse(payload["is_terminal"])

    def test_completed_processing_exposes_the_repository_file_for_spectral_metadata(self):
        payload = self.module._serialize_host_activity(
            {
                "source": "file-task",
                "task_id": 63,
                "host_id": 18,
                "task_type": 2,
                "status": 0,
                "file_name": "capture_20260826.zip",
                "server_file_name": "p-123--capture_20260826.zip",
                "message": "Processing Done | file=p-123--capture_20260826.zip",
                "updated_at": None,
            }
        )

        self.assertTrue(payload["is_processing"])
        self.assertTrue(payload["is_terminal"])
        self.assertEqual(payload["server_file_name"], "p-123--capture_20260826.zip")

    def test_processed_spectrum_metadata_serializes_per_spectrum_details(self):
        payload = self.module._serialize_processed_spectrum_metadata(
            {
                "ID_FILE": 41,
                "NA_FILE": "p-123--capture.bin",
                "SPECTRUM_COUNT": 1,
                "SITE_COUNT": 1,
                "EQUIPMENT_COUNT": 1,
                "SPECTRA": [
                    {
                        "ID_SPECTRUM": 9,
                        "FREQUENCY_START": 88.5,
                        "FREQUENCY_END": 108.0,
                        "DESCRIPTION": "FM band",
                        "LOCALITY": "Brasília/DF",
                        "EQUIPMENT": "RFEye",
                    }
                ],
            }
        )

        self.assertFalse(payload["spectra_truncated"])
        self.assertEqual(payload["spectra"][0]["description"], "FM band")
        self.assertEqual(payload["spectra"][0]["locality"], "Brasília/DF")
        self.assertEqual(payload["spectra"][0]["equipment"], "RFEye")

    def test_promoted_backup_keeps_its_terminal_message_visible(self):
        payload = self.module._serialize_host_activity(
            {
                "source": "backup-file",
                "task_id": 62,
                "host_id": 18,
                "task_type": 2,
                "status": 2,
                "file_name": "capture_20260826.zip",
                "message": "Backup concluído para capture_20260826.zip.",
                "updated_at": datetime(2026, 8, 26, 10, 12, 13),
            }
        )

        self.assertEqual(payload["title"], "Processamento de arquivo")
        self.assertEqual(payload["status"], 2)
        self.assertEqual(payload["status_label"], "Em execução")
        self.assertFalse(payload["is_terminal"])
        self.assertIsNone(payload["transfer_progress"])
        self.assertEqual(
            payload["message"],
            "Backup concluído para capture_20260826.zip.",
        )


class TestHostStatisticsRoutes(unittest.TestCase):
    """Protect current statistics and diagnostic failure responses."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_host_routes()

    def test_statistics_preserve_display_timestamps_and_numeric_volumes(self):
        with patch.object(self.module, "get_host_statistics", return_value={
            "DT_LAST_BACKUP": datetime(2026, 10, 26, 12, 30),
            "VL_BACKUP_DONE_GB_TOTAL": Decimal("1.25"),
            "BACKUP_YEARLY_BREAKDOWN": [{"REFERENCE_YEAR": 2026, "BACKUP_DONE_FILES": 12}],
        }):
            response = self.module.host_statistics(73)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response["statistics"]["DT_LAST_BACKUP"], "2026-10-26 12:30:00")
        self.assertEqual(response["statistics"]["VL_BACKUP_DONE_GB_TOTAL"], 1.25)
        self.assertEqual(response["statistics"]["BACKUP_YEARLY_BREAKDOWN"][0]["BACKUP_DONE_FILES"], 12)

    def test_statistics_distinguish_missing_snapshot_and_failed_read(self):
        with patch.object(self.module, "get_host_statistics", return_value=None):
            response, code = self.module.host_statistics(73)
        self.assertEqual(code, 404)
        self.assertIn("error", response)
        with patch.object(self.module, "get_host_statistics", side_effect=RuntimeError("read failed")):
            response, code = self.module.host_statistics(73)
        self.assertEqual(code, 503)
        self.assertIn("error", response)
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_diagnostic_read_failures_do_not_claim_zero_errors(self):
        for phase in ("backup", "processing"):
            with self.subTest(phase=phase), patch.object(
                self.module, f"get_host_{phase}_error_overview", side_effect=RuntimeError("read failed"),
            ):
                response, code = getattr(self.module, f"host_{phase}_errors")(73)
                self.assertEqual(code, 503)
                self.assertIn("error", response)
                self.assertNotIn("error_total_occurrences", response)
                self.assertEqual(response.headers["Cache-Control"], "no-store")


class TestHostZabbixMetricsRoute(unittest.TestCase):
    """Keep the pilot response compatible with the appCataloga collector."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_host_routes()

    def test_metrics_endpoint_serializes_appcataloga_scalars(self):
        self.module.get_host_operational_metrics_snapshot = lambda host_id: {
            "ID_HOST": host_id,
            "DT_LAST_BACKUP": datetime(2026, 8, 25, 12, 30, 45),
            "VL_BACKUP_DONE_GB_TOTAL": Decimal("13.01"),
        }

        payload = self.module.host_zabbix_metrics(73)

        self.assertEqual(payload["status"], 1)
        self.assertEqual(payload["message"], "Host operational metrics read successfully")
        self.assertEqual(
            payload["metrics"],
            {
                "ID_HOST": 73,
                "DT_LAST_BACKUP": 1787661045,
                "VL_BACKUP_DONE_GB_TOTAL": 13.01,
            },
        )

    def test_metric_value_serializer_keeps_date_compatibility(self):
        self.assertEqual(
            self.module._serialize_host_metric_value(date(2026, 8, 1)),
            1785542400,
        )

    def test_metrics_endpoint_reports_missing_host_with_empty_metrics(self):
        self.module.get_host_operational_metrics_snapshot = lambda host_id: {}

        payload, status_code = self.module.host_zabbix_metrics(73)

        self.assertEqual(status_code, 404)
        self.assertEqual(payload["status"], 0)
        self.assertEqual(payload["message"], "Host operational metrics are not available")
        self.assertEqual(payload["metrics"], {})


if __name__ == "__main__":
    unittest.main()
