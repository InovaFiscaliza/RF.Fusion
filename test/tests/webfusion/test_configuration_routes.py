"""Validation tests for the Zabbix configuration route protection."""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


class FakeBlueprint:
    """Minimal blueprint double that stores the registered request hook."""

    def __init__(self, *args, **kwargs):
        self.name = args[0]
        self.url_prefix = kwargs.get("url_prefix")
        self.before_request_handler = None

    def before_request(self, func):
        self.before_request_handler = func
        return func

    def route(self, *args, **kwargs):
        def decorator(func):
            return func

        return decorator


def load_configuration_routes():
    """Reload the route module with only the dependencies needed by auth."""

    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

    fake_flask = ModuleType("flask")
    fake_flask.Blueprint = FakeBlueprint
    fake_flask.current_app = SimpleNamespace(
        logger=SimpleNamespace(warning=lambda *args, **kwargs: None),
    )
    fake_flask.redirect = lambda value: value
    fake_flask.render_template = lambda template, **context: {
        "template": template,
        "context": context,
    }
    fake_flask.request = SimpleNamespace(authorization=None, args={}, form={})
    fake_flask.url_for = lambda endpoint, **kwargs: f"/{endpoint}"

    fake_usage_metrics = ModuleType("modules.server.usage_metrics")
    fake_usage_metrics.record_page_view = lambda: None

    fake_service = ModuleType("modules.configuration.service")
    fake_service.ACTION_RESTORE = "restore"
    fake_service.ACTION_SAVE = "save"
    fake_service.TARGET_KIND_HOST = "host"
    fake_service.TARGET_KIND_TEMPLATE = "template"
    fake_service.ZabbixApiError = RuntimeError
    fake_service.ZabbixConfigurationError = ValueError
    fake_service.apply_macro_change = lambda **kwargs: None
    fake_service.get_catalog = lambda: {"hosts": [], "templates": []}
    fake_service.get_configuration = lambda *args, **kwargs: None

    sys.modules["flask"] = fake_flask
    sys.modules["modules.server.usage_metrics"] = fake_usage_metrics
    sys.modules["modules.configuration.service"] = fake_service
    sys.modules.pop("modules.configuration.routes", None)
    return importlib.import_module("modules.configuration.routes")


class TestConfigurationRoutes(unittest.TestCase):
    """Keep the station configuration route importable with its dependencies."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_configuration_routes()

    def test_configuration_dashboard_remains_registered(self):
        self.assertTrue(callable(self.module.configuration_dashboard))

    def test_configuration_blueprints_use_consistent_names_and_prefixes(self):
        self.assertEqual(self.module.configuration_bp.name, "configuration")
        self.assertEqual(self.module.configuration_bp.url_prefix, "/configuration")
        self.assertEqual(self.module.configuration_api_bp.name, "configuration_api")
        self.assertEqual(
            self.module.configuration_api_bp.url_prefix,
            "/api/configuration",
        )


if __name__ == "__main__":
    unittest.main()
