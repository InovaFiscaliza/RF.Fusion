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
    fake_flask.jsonify = lambda payload: payload
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
    fake_service.FILE_TASK_ACTION_OPTIONS = {
        "restart": "Reiniciar etapa atual",
        "suspend": "Suspender",
        "move_to_backup": "Mover para backup",
        "redo_backup": "Refazer backup",
        "reprocess": "Reprocessar",
    }
    fake_service.FILE_TASK_TYPE_LABELS = {1: "Backup", 2: "Processamento"}
    fake_service.HISTORY_TARGET_STAGE_OPTIONS = {
        "backup": "Backup",
        "process": "Processamento",
    }
    fake_service.HISTORY_TARGET_STATUS_OPTIONS = {
        1: "Aguardando",
        -2: "Suspensa",
        -3: "Congelada",
    }
    fake_service.HOST_TASK_TYPE_LABELS = {1: "Solicitar backup", 2: "Descoberta"}
    fake_service.QUEUE_FILE_TASK = "file"
    fake_service.QUEUE_HOST_TASK = "host"
    fake_service.TASK_STATUS_LABELS = {-3: "Congelada", -2: "Suspensa", 0: "Concluída", 1: "Pendente"}
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
    fake_service.apply_file_task_target_action = lambda db, task_ids, *, target_stage, target_status: {
        "queue_kind": "file",
        "queue_label": "Fila de Arquivos",
        "target_stage": target_stage,
        "target_status": target_status,
        "action_label": "Backup: Aguardando",
        "selected_count": len(task_ids),
        "updated_count": len(task_ids),
        "blocked_count": 0,
        "missing_count": 0,
        "blocked_rows": [],
        "missing_ids": [],
    }
    fake_service.build_filters = lambda source: {
        "queue_kind": (source.get("queue_kind") or "host"),
        "host_id": int(source["host_id"]) if source.get("host_id") else None,
        "task_type": None,
        "status": None,
        "search": str(source.get("search") or ""),
        "limit": 50,
    }
    fake_service.build_file_task_filters = lambda source: {
        "queue_kind": "file",
        "host_id": int(source["host_id"]) if source.get("host_id") else None,
        "task_type": None,
        "status": None,
        "search": str(source.get("search") or ""),
        "host_file_name": str(source.get("host_file_name") or ""),
        "date_field": str(source.get("date_field") or ""),
        "date_from": str(source.get("date_from") or ""),
        "date_to": str(source.get("date_to") or ""),
        "limit": 50,
    }
    fake_service.build_history_filters = lambda source: {
        "host_id": int(source["history_host_id"]) if source.get("history_host_id") else None,
        "host_file_name": str(source.get("history_host_file_name") or ""),
        "server_file_name": str(source.get("history_server_file_name") or ""),
        "message": str(source.get("history_message") or ""),
        "date_field": str(source.get("history_date_field") or ""),
        "date_from": str(source.get("history_date_from") or ""),
        "date_to": str(source.get("history_date_to") or ""),
        "discovery_status": int(source["history_discovery_status"])
        if source.get("history_discovery_status") not in (None, "", "all")
        else None,
        "backup_status": int(source["history_backup_status"])
        if source.get("history_backup_status") not in (None, "", "all")
        else None,
        "processing_status": int(source["history_processing_status"])
        if source.get("history_processing_status") not in (None, "", "all")
        else None,
        "limit": 50,
    }
    fake_service.format_block_reason = lambda reason: reason
    def validate_history_filters(filters):
        has_identity_filter = any(
            [
                filters.get("host_id"),
                filters.get("host_file_name"),
                filters.get("server_file_name"),
            ]
        )
        if not has_identity_filter:
            raise ValueError(
                "Selecione um host ou informe o nome completo de um arquivo para consultar o histórico. "
                "Data e mensagem apenas refinam esses filtros."
            )

    fake_service.validate_history_filters = validate_history_filters
    fake_service.validate_file_task_filters = lambda filters: None
    fake_service.list_maintenance_hosts = lambda db: [
        {
            "ID_HOST": 101,
            "NA_HOST_NAME": "host-01",
            "IS_OFFLINE": 0,
        }
    ]
    fake_service.list_file_task_hosts = lambda db: [
        {
            "ID_HOST": 101,
            "NA_HOST_NAME": "host-01",
            "IS_OFFLINE": 0,
        }
    ]
    fake_service.list_file_history = lambda db, filters: [
        {
            "ID_HISTORY": 70,
            "NA_HOST_NAME": "host-01",
            "IS_OFFLINE": 0,
            "NA_HOST_FILE_NAME": "host.zip",
            "NA_SERVER_FILE_PATH": "/repository",
            "NA_SERVER_FILE_NAME": "server.zip",
            "NA_MESSAGE": "error",
            "NU_STATUS_DISCOVERY": 0,
            "NU_STATUS_BACKUP": -1,
            "NU_STATUS_PROCESSING": -1,
            "ID_FILE_TASK": None,
            "ACTIVE_TASK_LABEL": "Nenhuma",
            "DISCOVERY_STATUS_LABEL": "Concluída",
            "BACKUP_STATUS_LABEL": "Erro",
            "PROCESS_STATUS_LABEL": "Erro",
        }
    ]
    fake_service.list_host_tasks = lambda db, filters: [
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
    fake_service.list_file_tasks = lambda db, filters: [
        {
            "ID_FILE_TASK": 20,
            "NA_HOST_NAME": "host-01",
            "TYPE_LABEL": "Backup",
            "STATUS_LABEL": "Pendente",
            "NU_STATUS": 1,
            "IS_OFFLINE": 0,
            "NA_HOST_FILE_NAME": "host.zip",
            "NA_SERVER_FILE_NAME": None,
            "DT_FILE_TASK": "2026-07-09 12:00:00",
            "NA_MESSAGE": "queued",
        }
    ]
    fake_service.parse_selected_ids = lambda form_data: [10]
    fake_service.parse_selected_history_ids = lambda form_data: [70]
    fake_service.apply_history_action = lambda db, history_ids, *, target_stage, target_status: {
        "target_stage": target_stage,
        "target_status": target_status,
        "action_label": "Processamento: Aguardando",
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

    def test_dashboard_keeps_task_panels_unloaded_when_authenticated(self):
        self.module.request.authorization = SimpleNamespace(
            username="admin",
            password="admin",
        )

        payload = self.module.maintenance_dashboard()

        self.assertEqual(payload["template"], "maintenance/maintenance.html")
        self.assertEqual(payload["context"]["host_task_filters"]["queue_kind"], "host")
        self.assertEqual(payload["context"]["file_task_filters"]["queue_kind"], "file")
        self.assertEqual(payload["context"]["host_task_rows"], [])
        self.assertEqual(payload["context"]["file_task_rows"], [])
        self.assertFalse(payload["context"]["host_task_loaded"])
        self.assertFalse(payload["context"]["file_task_loaded"])
        self.assertEqual(len(payload["context"]["hosts"]), 1)
        self.assertIsNone(payload["context"]["host_task_action_summary"])
        self.assertIsNone(payload["context"]["file_task_action_summary"])
        self.assertEqual(len(payload["context"]["history_rows"]), 0)
        self.assertFalse(payload["context"]["history_loaded"])
        self.assertIsNone(payload["context"]["history_action_summary"])

    def test_dashboard_loads_host_tasks_only_when_requested(self):
        self.module.request.authorization = SimpleNamespace(
            username="admin",
            password="admin",
        )
        self.module.request.args = {
            "host_task_load": "1",
            "host_task_host_id": "101",
        }

        payload = self.module.maintenance_dashboard()

        self.assertTrue(payload["context"]["host_task_loaded"])
        self.assertEqual(payload["context"]["host_task_filters"]["host_id"], 101)
        self.assertEqual(len(payload["context"]["host_task_rows"]), 1)
        self.assertFalse(payload["context"]["file_task_loaded"])

    def test_dashboard_loads_file_tasks_only_when_requested(self):
        self.module.request.authorization = SimpleNamespace(
            username="admin",
            password="admin",
        )
        self.module.request.args = {
            "file_task_load": "1",
            "file_task_host_id": "101",
            "file_task_host_file_name": "sample.zip",
        }

        payload = self.module.maintenance_dashboard()

        self.assertTrue(payload["context"]["file_task_loaded"])
        self.assertEqual(payload["context"]["file_task_filters"]["host_id"], 101)
        self.assertEqual(payload["context"]["file_task_filters"]["host_file_name"], "sample.zip")
        self.assertEqual(len(payload["context"]["file_task_rows"]), 1)
        self.assertFalse(payload["context"]["host_task_loaded"])

    def test_dashboard_keeps_history_unloaded_when_request_has_no_anchor_filter(self):
        self.module.request.authorization = SimpleNamespace(
            username="admin",
            password="admin",
        )
        calls = []
        original_history_loader = self.module.list_file_history
        self.module.list_file_history = lambda db, filters: calls.append(
            filters
        ) or self.fail("The history loader must not run without an anchor filter.")
        self.module.request.args = {
            "history_load": "1",
        }

        try:
            payload = self.module.maintenance_dashboard()
        finally:
            self.module.list_file_history = original_history_loader

        self.assertFalse(payload["context"]["history_loaded"])
        self.assertEqual(payload["context"]["history_rows"], [])
        self.assertIn("nome completo de um arquivo", payload["context"]["history_query_message"])
        self.assertEqual(calls, [])

    def test_dashboard_rejects_invalid_history_action_without_mutating_tasks(self):
        self.module.request.authorization = SimpleNamespace(
            username="admin",
            password="admin",
        )
        self.module.request.method = "POST"
        self.module.request.form = {
            "maintenance_form": "history_actions",
            "history_target_stage": "unexpected_target",
            "history_target_status": "1",
            "selected_history_ids": [70],
        }

        payload = self.module.maintenance_dashboard()

        self.assertFalse(payload["context"]["history_loaded"])
        self.assertIn("etapa de destino", payload["context"]["history_query_message"])
        self.assertIsNone(payload["context"]["history_action_summary"])

    def test_dashboard_reprocesses_validated_history_from_history_scope(self):
        self.module.request.authorization = SimpleNamespace(
            username="admin",
            password="admin",
        )
        calls = []
        original_history_action = self.module.apply_history_action
        self.module.apply_history_action = lambda db, history_ids, *, target_stage, target_status: calls.append(
            (history_ids, target_stage, target_status)
        ) or {
            "target_stage": target_stage,
            "target_status": target_status,
            "action_label": "Processamento: Aguardando",
            "selected_count": len(history_ids),
            "updated_count": len(history_ids),
            "blocked_count": 0,
            "missing_count": 0,
            "blocked_rows": [],
            "missing_ids": [],
        }
        self.module.request.method = "POST"
        self.module.request.form = {
            "maintenance_form": "history_actions",
            "history_target_stage": "process",
            "history_target_status": "1",
            "selected_history_ids": [70],
        }
        try:
            payload = self.module.maintenance_dashboard()
        finally:
            self.module.apply_history_action = original_history_action

        self.assertEqual(calls, [([70], "process", 1)])
        self.assertEqual(
            payload["context"]["history_action_summary"]["action_label"],
            "Processamento: Aguardando",
        )

    def test_dashboard_rejects_invalid_host_action_without_mutating_tasks(self):
        self.module.request.authorization = SimpleNamespace(
            username="admin",
            password="admin",
        )
        self.module.request.method = "POST"
        self.module.request.form = {
            "maintenance_form": "host_task_actions",
            "action": "unexpected_action",
            "selected_ids": [10],
        }

        payload = self.module.maintenance_dashboard()

        self.assertIn("Ação inválida para tarefas do host", payload["context"]["host_task_action_error"])
        self.assertIsNone(payload["context"]["host_task_action_summary"])

    def test_dashboard_file_target_ignores_tampered_queue_kind(self):
        self.module.request.authorization = SimpleNamespace(
            username="admin",
            password="admin",
        )
        calls = []
        original_file_action = self.module.apply_file_task_target_action
        self.module.apply_file_task_target_action = lambda db, task_ids, *, target_stage, target_status: calls.append(
            (task_ids, target_stage, target_status)
        ) or {
            "queue_kind": "file",
            "queue_label": "Fila de Arquivos",
            "target_stage": target_stage,
            "target_status": target_status,
            "action_label": "Processamento: Aguardando",
            "selected_count": len(task_ids),
            "updated_count": len(task_ids),
            "blocked_count": 0,
            "missing_count": 0,
            "blocked_rows": [],
            "missing_ids": [],
        }
        self.module.request.method = "POST"
        self.module.request.form = {
            "maintenance_form": "file_task_targets",
            "queue_kind": "host",
            "file_task_target_stage": "process",
            "file_task_target_status": "1",
            "selected_ids": [10],
        }
        try:
            payload = self.module.maintenance_dashboard()
        finally:
            self.module.apply_file_task_target_action = original_file_action

        self.assertEqual(calls, [([10], "process", 1)])
        self.assertEqual(payload["context"]["file_task_action_summary"]["updated_count"], 1)

    def test_file_task_hosts_returns_only_the_optional_queue_subset(self):
        payload = self.module.file_task_hosts()

        self.assertEqual(payload["hosts"][0]["ID_HOST"], 101)


if __name__ == "__main__":
    unittest.main()
