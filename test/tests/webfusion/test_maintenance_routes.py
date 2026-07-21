"""
Validation tests for `webfusion.modules.maintenance.routes`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_maintenance_routes.py -q

What is covered here:
    - basic-auth protection for the maintenance page
    - GET rendering with normalized filters and current table payload
    - route wiring for history-driven FILE_TASK recreation
"""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


class FakeBlueprint:
    """Tiny blueprint double that keeps route decorators importable in tests."""

    def __init__(self, *args, **kwargs):
        self.before_request_handler = None

    def before_request(self, func):
        self.before_request_handler = func
        return func

    def route(self, *args, **kwargs):
        def decorator(func):
            return func

        return decorator


class FakeResponse:
    """Small response double exposing only the fields asserted in tests."""

    def __init__(self, body, status, headers):
        self.body = body
        self.status_code = status
        self.headers = headers


class FakeDB:
    """Very small connection double used by the route tests."""

    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


def load_maintenance_routes():
    """Reload routes with framework and dependency stubs."""
    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

    fake_flask = ModuleType("flask")
    fake_flask.Blueprint = FakeBlueprint
    fake_flask.Response = FakeResponse
    fake_flask.render_template = lambda template, **context: {
        "template": template,
        "context": context,
    }
    fake_flask.request = SimpleNamespace(
        authorization=None,
        args={},
        form={},
        method="GET",
    )

    fake_db = ModuleType("db")
    fake_db.get_connection_bpdata = lambda: FakeDB()

    fake_service = ModuleType("modules.maintenance.service")
    fake_service.ACTION_OPTIONS = {"restart": "Reiniciar", "suspend": "Suspender"}
    fake_service.ACTION_RECREATE_BACKUP = "recreate_backup"
    fake_service.ACTION_RECREATE_PROCESS = "recreate_process"
    fake_service.FILE_TASK_TYPE_LABELS = {1: "Backup", 2: "Processamento"}
    fake_service.HISTORY_PHASE_OPTIONS = {
        "backup": "Recriar backup",
        "process": "Recriar processamento",
    }
    fake_service.HOST_TASK_TYPE_LABELS = {1: "Solicitar backup", 2: "Descoberta"}
    fake_service.QUEUE_FILE_TASK = "file"
    fake_service.QUEUE_HOST_TASK = "host"
    fake_service.TASK_STATUS_LABELS = {-2: "Suspensa", 1: "Pendente"}
    fake_service.apply_bulk_action = lambda db, queue_kind, task_ids, action: {
        "queue_kind": queue_kind,
        "queue_label": "HOST_TASK",
        "action": action,
        "action_label": "Reiniciar",
        "selected_count": len(task_ids),
        "updated_count": len(task_ids),
        "blocked_count": 0,
        "missing_count": 0,
        "blocked_rows": [],
        "missing_ids": [],
    }
    fake_service.build_filters = lambda source: {
        "queue_kind": (source.get("queue_kind") or "host"),
        "task_type": None,
        "status": None,
        "search": str(source.get("search") or ""),
        "limit": 50,
    }
    fake_service.build_history_filters = lambda source: {
        "phase": (source.get("history_phase") or "process"),
        "host_name": str(source.get("history_host_name") or ""),
        "host_file_name": str(source.get("history_host_file_name") or ""),
        "server_file_name": str(source.get("history_server_file_name") or ""),
        "message": str(source.get("history_message") or ""),
        "date_field": str(source.get("history_date_field") or ""),
        "date_from": str(source.get("history_date_from") or ""),
        "date_to": str(source.get("history_date_to") or ""),
        "limit": 50,
    }
    fake_service.format_block_reason = lambda reason: reason
    fake_service.history_filters_are_actionable = lambda filters: any(
        [
            filters.get("host_name"),
            filters.get("host_file_name"),
            filters.get("server_file_name"),
            filters.get("message"),
            filters.get("date_from"),
            filters.get("date_to"),
        ]
    )
    fake_service.list_file_history_recreate_candidates = lambda db, filters: [
        {
            "ID_HISTORY": 70,
            "NA_HOST_NAME": "host-01",
            "IS_OFFLINE": 0,
            "NA_HOST_FILE_NAME": "host.zip",
            "NA_SERVER_FILE_NAME": "server.zip",
            "NA_MESSAGE": "error",
            "NU_STATUS_BACKUP": -1,
            "NU_STATUS_PROCESSING": -1,
            "BACKUP_STATUS_LABEL": "Erro",
            "PROCESS_STATUS_LABEL": "Erro",
        }
    ]
    fake_service.list_tasks = lambda db, filters: [
        {
            "ID_HOST_TASK": 10,
            "NA_HOST_NAME": "host-01",
            "TYPE_LABEL": "Solicitar backup",
            "STATUS_LABEL": "Pendente",
            "NU_STATUS": 1,
            "IS_OFFLINE": 0,
            "DT_HOST_TASK": "2026-07-09 12:00:00",
            "NA_MESSAGE": "queued",
        }
    ]
    fake_service.parse_selected_ids = lambda form_data: [10]
    fake_service.parse_selected_history_ids = lambda form_data: [70]
    fake_service.apply_history_recreate_action = lambda db, history_ids, action: {
        "action": action,
        "action_label": "Recriar processamento",
        "selected_count": len(history_ids),
        "updated_count": len(history_ids),
        "blocked_count": 0,
        "missing_count": 0,
        "blocked_rows": [],
        "missing_ids": [],
    }

    fake_usage_metrics = ModuleType("modules.server.usage_metrics")
    fake_usage_metrics.record_page_view = lambda: None

    sys.modules["flask"] = fake_flask
    sys.modules["db"] = fake_db
    sys.modules["modules.maintenance.service"] = fake_service
    sys.modules["modules.server.usage_metrics"] = fake_usage_metrics
    sys.modules.pop("modules.maintenance.routes", None)
    return importlib.import_module("modules.maintenance.routes")


class TestMaintenanceRoutes(unittest.TestCase):
    """Protect authentication and response wiring for the maintenance UI."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_maintenance_routes()

    def setUp(self):
        self.module.request.authorization = None
        self.module.request.args = {}
        self.module.request.form = {}
        self.module.request.method = "GET"

    def test_require_maintenance_auth_rejects_missing_credentials(self):
        response = self.module.require_maintenance_auth()

        self.assertEqual(response.status_code, 401)
        self.assertIn("WWW-Authenticate", response.headers)

    def test_dashboard_renders_current_rows_when_authenticated(self):
        self.module.request.authorization = SimpleNamespace(
            username="admin",
            password="admin",
        )

        payload = self.module.maintenance_dashboard()

        self.assertEqual(payload["template"], "maintenance/maintenance.html")
        self.assertEqual(payload["context"]["filters"]["queue_kind"], "host")
        self.assertEqual(len(payload["context"]["rows"]), 1)
        self.assertIsNone(payload["context"]["action_summary"])
        self.assertEqual(len(payload["context"]["history_rows"]), 0)
        self.assertFalse(payload["context"]["history_loaded"])
        self.assertIsNone(payload["context"]["history_action_summary"])

    def test_dashboard_keeps_history_unloaded_when_request_has_no_anchor_filter(self):
        self.module.request.authorization = SimpleNamespace(
            username="admin",
            password="admin",
        )
        self.module.request.args = {
            "history_load": "1",
            "history_phase": "process",
        }

        payload = self.module.maintenance_dashboard()

        self.assertTrue(payload["context"]["history_loaded"])
        self.assertEqual(payload["context"]["history_rows"], [])
        self.assertIn("Informe ao menos um filtro", payload["context"]["history_query_message"])


if __name__ == "__main__":
    unittest.main()
