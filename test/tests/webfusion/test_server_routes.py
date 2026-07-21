"""
Validation tests for `webfusion.modules.server`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_server_routes.py -q

What is covered here:
    - `/server` injects the aggregated usage metrics payload
    - download-action telemetry increments the lightweight counter
    - the shared usage-metric helpers keep independent monthly counters
"""

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")
os.environ["WEBFUSION_USAGE_METRICS_BACKEND"] = "memory"


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
        usage_metrics = payload["context"]["usage_metrics"]

        self.assertEqual(payload["template"], "server/server.html")
        self.assertEqual(usage_metrics["totals"]["page_view_count"], 1)
        self.assertEqual(usage_metrics["totals"]["spectrum_query_count"], 0)
        self.assertEqual(usage_metrics["totals"]["download_action_count"], 0)
        self.assertEqual(usage_metrics["totals"]["nginx_download_count"], 0)
        self.assertTrue(usage_metrics["current_year_label"])
        self.assertTrue(usage_metrics["current_month_label"])

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
            snapshot["totals"],
            {
                "page_view_count": 2,
                "spectrum_query_count": 1,
                "download_action_count": 1,
                "nginx_download_count": 0,
            },
        )
        self.assertEqual(len(snapshot["annual_breakdown"]), 1)
        self.assertEqual(len(snapshot["monthly_breakdown"]), 1)

    def test_usage_metrics_ingest_nginx_downloads_from_log_once(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "access.log"
            log_path.write_text(
                (
                    '10.88.0.34 - - [06/Jul/2026:18:06:19 +0000] '
                    '"GET /downloads/sample-a.mat HTTP/1.1" 200 100 "-" "ua"\n'
                    '10.88.0.34 - - [06/Jul/2026:18:06:20 +0000] '
                    '"GET /downloads/sample-b.mat HTTP/1.1" 206 100 "-" "ua"\n'
                    '10.88.0.34 - - [06/Jul/2026:18:06:21 +0000] '
                    '"GET /static/js/base_page.js HTTP/1.1" 200 100 "-" "ua"\n'
                ),
                encoding="utf-8",
            )
            previous_path = os.environ.get("WEBFUSION_NGINX_DOWNLOAD_LOG_PATH")
            os.environ["WEBFUSION_NGINX_DOWNLOAD_LOG_PATH"] = str(log_path)
            try:
                first_snapshot = self.usage_metrics.get_usage_metrics_snapshot()
                second_snapshot = self.usage_metrics.get_usage_metrics_snapshot()
            finally:
                if previous_path is None:
                    os.environ.pop("WEBFUSION_NGINX_DOWNLOAD_LOG_PATH", None)
                else:
                    os.environ["WEBFUSION_NGINX_DOWNLOAD_LOG_PATH"] = previous_path

        self.assertEqual(first_snapshot["totals"]["nginx_download_count"], 2)
        self.assertEqual(second_snapshot["totals"]["nginx_download_count"], 2)

    def test_usage_metrics_keep_monthly_nginx_total_after_log_truncation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "access.log"
            previous_path = os.environ.get("WEBFUSION_NGINX_DOWNLOAD_LOG_PATH")
            os.environ["WEBFUSION_NGINX_DOWNLOAD_LOG_PATH"] = str(log_path)
            try:
                log_path.write_text(
                    '10.88.0.34 - - [06/Jul/2026:18:06:19 +0000] '
                    '"GET /downloads/sample-a.mat HTTP/1.1" 200 100 "-" "ua"\n',
                    encoding="utf-8",
                )
                first_snapshot = self.usage_metrics.get_usage_metrics_snapshot()

                log_path.write_text(
                    '10.88.0.34 - - [06/Jul/2026:18:16:19 +0000] '
                    '"GET /downloads/sample-b.mat HTTP/1.1" 200 100 "-" "ua"\n',
                    encoding="utf-8",
                )
                second_snapshot = self.usage_metrics.get_usage_metrics_snapshot()
            finally:
                if previous_path is None:
                    os.environ.pop("WEBFUSION_NGINX_DOWNLOAD_LOG_PATH", None)
                else:
                    os.environ["WEBFUSION_NGINX_DOWNLOAD_LOG_PATH"] = previous_path

        self.assertEqual(first_snapshot["totals"]["nginx_download_count"], 1)
        self.assertEqual(second_snapshot["totals"]["nginx_download_count"], 2)


if __name__ == "__main__":
    unittest.main()
