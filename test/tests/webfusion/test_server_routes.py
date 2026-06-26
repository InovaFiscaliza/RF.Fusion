"""
Validation tests for `webfusion.modules.server`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_server_routes.py -q

What is covered here:
    - `/server` injects the in-memory usage metrics banner payload
    - download-action telemetry increments the lightweight counter
    - the shared usage-metric helpers keep independent counters
"""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


def load_server_routes():
    """Reload server routes with framework and service stubs."""

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
    fake_flask.current_app = SimpleNamespace(
        logger=SimpleNamespace(
            exception=lambda *args, **kwargs: None,
        )
    )
    fake_flask.jsonify = lambda payload: payload
    fake_flask.render_template = lambda template, **context: {
        "template": template,
        "context": context,
    }
    fake_flask.request = SimpleNamespace(args={}, method="GET")

    fake_host_service = ModuleType("modules.host.service")
    fake_host_service.get_hosts = lambda *args, **kwargs: []
    fake_host_service.get_server_backup_error_overview = lambda: {"rows": []}
    fake_host_service.get_server_overview = lambda *args, **kwargs: {
        "TOTAL_HOSTS": 10,
        "ONLINE_HOSTS": 7,
        "OFFLINE_HOSTS": 3,
        "BUSY_HOSTS": 2,
        "SERVER_MEMORY": {
            "used_human": "1 GB",
            "total_human": "2 GB",
            "available_human": "1 GB",
            "use_percent": 50,
        },
        "REPOSFI_USAGE": {
            "mounted": True,
            "used_human": "1 TB",
            "total_human": "2 TB",
            "free_human": "1 TB",
            "use_percent": 50,
            "path": "/mnt/reposfi",
        },
        "APP_ANALISE_STATUS": {
            "online": True,
            "host": "appanalise.local",
            "latency_ms": 2.1,
            "error": None,
        },
    }
    fake_host_service.get_server_processing_error_overview = lambda: {"rows": []}
    fake_host_service.get_server_summary_metrics = lambda: {
        "CURRENT_MONTH_LABEL": "2026-06",
    }

    sys.modules["flask"] = fake_flask
    sys.modules["modules.host.service"] = fake_host_service
    sys.modules.pop("modules.server.routes", None)
    return importlib.import_module("modules.server.routes")


class TestServerUsageMetrics(unittest.TestCase):
    """Protect the lightweight usage counters exposed on `/server`."""

    @classmethod
    def setUpClass(cls):
        root = str(WEBFUSION_ROOT)
        if root not in sys.path:
            sys.path.insert(0, root)

        cls.usage_metrics = importlib.import_module("modules.server.usage_metrics")
        cls.routes = load_server_routes()

    def setUp(self):
        self.usage_metrics.reset_usage_metrics()
        self.routes.request.args = {}

    def test_server_route_includes_banner_metrics_snapshot(self):
        payload = self.routes.server()

        self.assertEqual(payload["template"], "server/server.html")
        self.assertEqual(payload["context"]["usage_metrics"]["page_view_count"], 1)
        self.assertEqual(payload["context"]["usage_metrics"]["spectrum_query_count"], 0)
        self.assertEqual(payload["context"]["usage_metrics"]["download_action_count"], 0)

    def test_download_action_endpoint_counts_ui_clicks(self):
        payload, status_code = self.routes.server_download_action_metric()

        self.assertEqual(status_code, 202)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["download_action_count"], 1)

    def test_usage_metric_helpers_keep_independent_counters(self):
        self.usage_metrics.record_page_view()
        self.usage_metrics.record_page_view()
        self.usage_metrics.record_spectrum_query()
        self.usage_metrics.record_download_action()

        snapshot = self.usage_metrics.get_usage_metrics_snapshot()

        self.assertEqual(
            snapshot,
            {
                "page_view_count": 2,
                "spectrum_query_count": 1,
                "download_action_count": 1,
            },
        )


if __name__ == "__main__":
    unittest.main()
