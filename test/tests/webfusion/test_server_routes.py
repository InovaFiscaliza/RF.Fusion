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
from unittest.mock import patch


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
            "total_bytes": 2_147_483_648,
            "used_bytes": 1_073_741_824,
            "available_bytes": 1_073_741_824,
            "use_percent": 50,
            "used_human": "1 GB",
            "total_human": "2 GB",
            "available_human": "1 GB",
        },
        "REPOSFI_USAGE": {
            "mounted": True,
            "total_bytes": 2_199_023_255_552,
            "used_bytes": 1_099_511_627_776,
            "free_bytes": 1_099_511_627_776,
            "use_percent": 50,
            "used_human": "1 TB",
            "total_human": "2 TB",
            "free_human": "1 TB",
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
        "DISCOVERED_FILES_TOTAL": 99,
        "BACKUP_DONE_GB_THIS_MONTH": 1.25,
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

        # Other route tests install a lightweight telemetry stub in sys.modules.
        # This suite exercises the real counters, so it must start with a clean
        # module entry regardless of the order selected by pytest.
        sys.modules.pop("modules.server.usage_metrics", None)
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

    def test_zabbix_metrics_route_returns_flat_server_payload(self):
        payload = self.routes.server_zabbix_metrics()

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["reference_month"], "2026-06")
        self.assertEqual(payload["host_total"], 10)
        self.assertEqual(payload["host_online"], 7)
        self.assertEqual(payload["memory_available_bytes"], 1_073_741_824)
        self.assertEqual(payload["reposfi_mounted"], 1)
        self.assertEqual(payload["appanalise_online"], 1)
        self.assertEqual(payload["discovered_files_total"], 99)
        self.assertEqual(payload["backup_done_bytes_this_month"], 1_342_177_280)
        self.assertNotIn("backup_done_gb_this_month", payload)
        self.assertEqual(payload["webfusion_page_view_count_total"], 0)
        self.assertEqual(payload["webfusion_nginx_download_count_current_month"], 0)

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

    def test_nginx_checkpoint_uses_final_log_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "access.log"
            log_path.write_text(
                '10.88.0.34 - - [06/Jul/2026:18:06:19 +0000] '
                '"GET /downloads/sample-a.mat HTTP/1.1" 200 100 "-" "ua"\n',
                encoding="utf-8",
            )
            initial_stat = log_path.stat()
            final_stat = SimpleNamespace(
                st_dev=initial_stat.st_dev,
                st_ino=initial_stat.st_ino,
                st_size=initial_stat.st_size,
                st_mtime_ns=initial_stat.st_mtime_ns + 1,
            )

            with patch.object(
                self.usage_metrics.os,
                "stat",
                side_effect=(initial_stat, final_stat),
            ), patch.object(
                self.usage_metrics.os,
                "fstat",
                return_value=final_stat,
            ):
                first_counts, checkpoint = self.usage_metrics._read_nginx_download_counts(
                    str(log_path),
                    None,
                )
                second_counts, _ = self.usage_metrics._read_nginx_download_counts(
                    str(log_path),
                    checkpoint,
                )

        self.assertEqual(sum(first_counts.values()), 1)
        self.assertFalse(second_counts)

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

    def test_reconcile_nginx_download_metrics_replaces_duplicate_month_total(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "access.log"
            log_path.write_text(
                (
                    '10.88.0.34 - - [06/Jul/2026:18:06:19 +0000] '
                    '"GET /downloads/sample-a.mat HTTP/1.1" 200 100 "-" "ua"\n'
                    '10.88.0.34 - - [06/Jul/2026:18:06:20 +0000] '
                    '"GET /downloads/sample-b.mat HTTP/1.1" 206 100 "-" "ua"\n'
                ),
                encoding="utf-8",
            )
            previous_path = os.environ.get("WEBFUSION_NGINX_DOWNLOAD_LOG_PATH")
            os.environ["WEBFUSION_NGINX_DOWNLOAD_LOG_PATH"] = str(log_path)
            try:
                self.usage_metrics.get_usage_metrics_snapshot()
                self.usage_metrics._increment_counter_by_month_in_memory(
                    counter_name="nginx_download_count",
                    reference_month=self.usage_metrics._normalize_reference_month("2026-07-01"),
                    amount=4,
                )
                reconciled = self.usage_metrics.reconcile_nginx_download_metrics()
                snapshot = self.usage_metrics.get_usage_metrics_snapshot()
            finally:
                if previous_path is None:
                    os.environ.pop("WEBFUSION_NGINX_DOWNLOAD_LOG_PATH", None)
                else:
                    os.environ["WEBFUSION_NGINX_DOWNLOAD_LOG_PATH"] = previous_path

        self.assertEqual(reconciled, {"2026-07": 2})
        self.assertEqual(snapshot["totals"]["nginx_download_count"], 2)


if __name__ == "__main__":
    unittest.main()
