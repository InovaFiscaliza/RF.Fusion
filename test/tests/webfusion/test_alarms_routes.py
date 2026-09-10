"""Validation tests for the WebFusion alarms route registration."""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


class FakeBlueprint:
    """Minimal Flask blueprint double for module import tests."""

    def __init__(self, *args, **kwargs):
        self.routes = {}

    def route(self, *args, **kwargs):
        def decorator(func):
            self.routes[func.__name__] = {"path": args[0], "methods": kwargs.get("methods")}
            return func

        return decorator


def load_alarms_routes():
    """Reload alarm routes with service and Flask dependencies stubbed."""
    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

    fake_flask = ModuleType("flask")
    fake_flask.Blueprint = FakeBlueprint
    fake_flask.current_app = SimpleNamespace(logger=SimpleNamespace(exception=lambda *args: None))
    fake_flask.render_template = lambda template, **context: {"template": template, "context": context}
    fake_alarms_service = ModuleType("modules.alarms.service")
    fake_alarms_service.get_zabbix_problems_url = (
        lambda: "https://zabbix.example/zabbix/zabbix.php?action=problem.view"
    )
    fake_alarms_service.list_alarms = lambda: []
    fake_usage_metrics = ModuleType("modules.server.usage_metrics")
    fake_usage_metrics.record_page_view = lambda: None
    fake_zabbix_service = ModuleType("modules.configuration.service")
    fake_zabbix_service.ZabbixApiError = RuntimeError

    sys.modules["flask"] = fake_flask
    sys.modules["modules.alarms.service"] = fake_alarms_service
    sys.modules["modules.server.usage_metrics"] = fake_usage_metrics
    sys.modules["modules.configuration.service"] = fake_zabbix_service
    sys.modules.pop("modules.alarms.routes", None)
    return importlib.import_module("modules.alarms.routes")


class TestAlarmsRoutes(unittest.TestCase):
    """Keep the authenticated alarms dashboard and its context stable."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_alarms_routes()

    def test_alarms_dashboard_is_registered_at_the_expected_path(self):
        self.assertEqual(self.module.alarms_bp.routes["alarms_dashboard"]["path"], "/")
        self.assertEqual(self.module.alarms_bp.routes["alarms_dashboard"]["methods"], ["GET"])

    def test_alarms_dashboard_exposes_the_general_zabbix_problems_url(self):
        result = self.module.alarms_dashboard()

        self.assertEqual(
            result["context"]["zabbix_problems_url"],
            "https://zabbix.example/zabbix/zabbix.php?action=problem.view",
        )


if __name__ == "__main__":
    unittest.main()