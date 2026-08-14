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
        self.before_request_handler = None

    def before_request(self, func):
        self.before_request_handler = func
        return func

    def route(self, *args, **kwargs):
        def decorator(func):
            return func

        return decorator


class FakeResponse:
    """Small response double exposing the values asserted by these tests."""

    def __init__(self, body, status, headers):
        self.body = body
        self.status_code = status
        self.headers = headers


def load_zabbix_configuration_routes():
    """Reload the route module with only the dependencies needed by auth."""

    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

    fake_flask = ModuleType("flask")
    fake_flask.Blueprint = FakeBlueprint
    fake_flask.Response = FakeResponse
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

    fake_service = ModuleType("modules.zabbix_configuration.service")
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
    sys.modules["modules.zabbix_configuration.service"] = fake_service
    sys.modules.pop("modules.zabbix_configuration.routes", None)
    return importlib.import_module("modules.zabbix_configuration.routes")


class TestZabbixConfigurationRoutes(unittest.TestCase):
    """Keep the station configuration console behind the module auth gate."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_zabbix_configuration_routes()

    def setUp(self):
        self.module.request.authorization = None

    def test_require_zabbix_configuration_auth_rejects_missing_credentials(self):
        response = self.module.require_zabbix_configuration_auth()

        self.assertEqual(response.status_code, 401)
        self.assertIn("WWW-Authenticate", response.headers)

    def test_require_zabbix_configuration_auth_accepts_module_credentials(self):
        self.module.request.authorization = SimpleNamespace(
            username="admin",
            password="admin",
        )

        self.assertIsNone(self.module.require_zabbix_configuration_auth())


if __name__ == "__main__":
    unittest.main()
