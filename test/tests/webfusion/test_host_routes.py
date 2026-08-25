"""Validation tests for the connectivity-test polling payload."""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


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
    fake_flask.jsonify = lambda payload: payload
    fake_flask.render_template = lambda *args, **kwargs: None
    fake_flask.request = SimpleNamespace(authorization=None, args={})

    fake_db = ModuleType("db")
    fake_db.get_connection_bpdata = lambda: None

    fake_service = ModuleType("modules.host.service")
    fake_service.get_all_hosts = lambda **kwargs: []
    fake_service.get_host_backup_error_overview = lambda host_id: {}
    fake_service.get_host_location_history_overview = lambda host_id: {}
    fake_service.get_host_processing_error_overview = lambda host_id: {}
    fake_service.get_host_statistics = lambda host_id: {}

    fake_usage_metrics = ModuleType("modules.server.usage_metrics")
    fake_usage_metrics.record_page_view = lambda: None

    fake_task_service = ModuleType("modules.task.service")
    fake_task_service.HOST_TASK_INTERACTIVE_CHECK_TYPE = 99
    fake_task_service.TASK_DONE = 0
    fake_task_service.TASK_ERROR = -1
    fake_task_service.TASK_PENDING = 1
    fake_task_service.TASK_RUNNING = 2
    fake_task_service.queue_interactive_connectivity_test = lambda connection, host_id: {}

    sys.modules["flask"] = fake_flask
    sys.modules["db"] = fake_db
    sys.modules["modules.host.service"] = fake_service
    sys.modules["modules.server.usage_metrics"] = fake_usage_metrics
    sys.modules["modules.task.service"] = fake_task_service
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


if __name__ == "__main__":
    unittest.main()
