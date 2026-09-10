"""Focused route tests for WebFusion user-directory actions."""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


class FakeBlueprint:
    """Minimal blueprint double for registering user-directory routes."""

    def __init__(self, *args, **kwargs):
        pass

    def route(self, *args, **kwargs):
        def decorator(func):
            return func

        return decorator


def load_users_routes():
    """Reload user routes with deterministic Flask and service doubles."""
    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

    fake_flask = ModuleType("flask")
    fake_flask.Blueprint = FakeBlueprint
    fake_flask.Response = Mock
    fake_flask.current_app = SimpleNamespace(
        logger=SimpleNamespace(exception=lambda *args, **kwargs: None),
    )
    fake_flask.g = SimpleNamespace(webfusion_user={"email": None})
    fake_flask.jsonify = lambda value: value
    fake_flask.redirect = lambda value: value
    fake_flask.render_template = lambda template, **context: {
        "template": template,
        "context": context,
    }
    fake_flask.request = SimpleNamespace(args={}, form={})
    fake_flask.url_for = lambda endpoint, **kwargs: f"/{endpoint}"

    importlib.import_module("auth")
    fake_db_users = ModuleType("auth.db_users")
    fake_db_users.create_webfusion_user = lambda **kwargs: None
    fake_db_users.delete_webfusion_user = lambda **kwargs: None
    fake_users_service = ModuleType("modules.users.service")
    fake_users_service.ROLE_FILTERS = (("all", "Todos os papéis"),)
    fake_users_service.get_notice_error_message = lambda value: None
    fake_users_service.get_notice_success_message = lambda value: None
    fake_users_service.get_directory_data = lambda values: {
        "users": [],
        "filters": {},
        "filter_options": {},
        "role_filters": fake_users_service.ROLE_FILTERS,
    }
    fake_users_service.create_directory_user = lambda form: None
    fake_users_service.update_directory_user_privileges = lambda form: None
    fake_users_service.delete_directory_user = lambda form: None
    fake_db_users.update_webfusion_user_privileges = lambda **kwargs: None

    fake_usage_metrics = ModuleType("modules.server.usage_metrics")
    fake_usage_metrics.record_page_view = lambda: None

    sys.modules["flask"] = fake_flask
    sys.modules["auth.db_users"] = fake_db_users
    sys.modules["modules.users.service"] = fake_users_service
    sys.modules["modules.server.usage_metrics"] = fake_usage_metrics
    sys.modules.pop("modules.users.routes", None)
    return importlib.import_module("modules.users.routes")


class TestUsersRoutes(unittest.TestCase):
    """Verify the user-directory routes delegate normalized input correctly."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_users_routes()

    def setUp(self):
        self.module.request.args = {}
        self.module.request.form = {}

    def test_directory_passes_query_filters_to_service(self):
        self.module.request.args = {"email": "maria@example.org", "role": "admin"}
        self.module.service.get_directory_data = Mock(return_value={
            "users": [],
            "filters": {"email": "maria@example.org", "role": "admin"},
            "filter_options": {},
            "role_filters": (),
        })

        response = self.module.user_directory()

        self.assertEqual(response["template"], "users/users.html")
        self.assertEqual(response["context"]["filters"]["email"], "maria@example.org")
        self.module.service.get_directory_data.assert_called_once_with(self.module.request.args)

    def test_create_user_normalizes_email_and_privileges(self):
        self.module.request.form = {
            "user_name": "Maria Silva",
            "user_email": "MARIA.SILVA@example.org",
            "job_title": "Analista",
            "department": "Fiscalização",
            "location": "Brasília",
            "is_admin": "1",
        }
        self.module.service.create_directory_user = Mock()

        self.module.create_user()

        self.module.service.create_directory_user.assert_called_once_with(self.module.request.form)

    def test_privilege_update_uses_email_as_the_identity_key(self):
        self.module.request.form = {
            "user_email": "MARIA.SILVA@example.org",
            "is_developer": "1",
        }
        self.module.service.update_directory_user_privileges = Mock()

        self.module.update_user_privileges()

        self.module.service.update_directory_user_privileges.assert_called_once_with(self.module.request.form)

    def test_delete_user_uses_email_as_the_identity_key(self):
        self.module.request.form = {"user_email": "MARIA.SILVA@example.org"}
        self.module.service.delete_directory_user = Mock()

        self.module.delete_user()

        self.module.service.delete_directory_user.assert_called_once_with(self.module.request.form)


if __name__ == "__main__":
    unittest.main()
