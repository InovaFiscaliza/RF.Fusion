"""Validation tests for the proxy-backed current-user API."""

import importlib
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


class TestCurrentUserApi(unittest.TestCase):
    """Verify the login probe and current-user API contracts."""

    @classmethod
    def setUpClass(cls):
        root = str(WEBFUSION_ROOT)
        if root not in sys.path:
            sys.path.insert(0, root)

        module = importlib.import_module("app")
        module.app.config.update(TESTING=True)
        cls.module = module
        cls.client = module.app.test_client()

    def setUp(self):
        self.module.AUTH_SERVICE.observed_user_profiles.clear()
        self.register_observed_user = patch.object(
            importlib.import_module("auth.service"),
            "register_observed_user",
        )
        self.register_user_mock = self.register_observed_user.start()
        self.addCleanup(self.register_observed_user.stop)

    def tearDown(self):
        self.module.AUTH_SERVICE.observed_user_profiles.clear()

    def test_login_probe_returns_empty_html_body(self):
        response = self.client.get("/api/users/login")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_data(as_text=True), "<body></body>")

    def test_user_headers_returns_empty_204_with_proxy_identity(self):
        profile = {"NA_ROLE": "developer"}
        with patch(
            "modules.users.routes.service.get_current_user_profile",
            return_value=profile,
        ):
            response = self.client.get(
                "/api/users",
                headers={
                    "X-User-Name": "Maria Silva",
                    "X-User-Email": "maria.silva@example.org",
                    "X-User-Job-Title": "Analista",
                    "X-User-Department": "Fiscalização",
                    "X-User-Location": "Brasília",
                    "X-User-Avatar-Url": "https://login.example.org/avatar/maria",
                },
            )

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.get_data(), b"")
        self.assertEqual(response.headers["X-User-Name"], "Maria Silva")
        self.assertEqual(response.headers["X-User-Email"], "maria.silva@example.org")
        self.assertEqual(response.headers["X-User-Job-Title"], "Analista")
        self.assertEqual(response.headers["X-User-Department"], "Fiscalização")
        self.assertEqual(response.headers["X-User-Location"], "Brasília")
        self.assertEqual(response.headers["X-User-Roles"], "dev")
        self.assertEqual(
            response.headers["X-User-Avatar-Url"],
            "https://login.example.org/avatar/maria",
        )
        self.assertEqual(response.headers["Cache-Control"], "no-store")

    def test_user_headers_uses_user_role_without_special_privilege(self):
        with patch(
            "modules.users.routes.service.get_current_user_profile",
            return_value=None,
        ):
            response = self.client.get(
                "/api/users",
                headers={"X-User-Email": "visitor@example.org"},
            )

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.headers["X-User-Roles"], "user")

    def test_user_admin_api_remains_restricted(self):
        response = self.client.post("/api/users/")

        self.assertEqual(response.status_code, 403)

    def test_current_user_returns_database_profile_without_cache(self):
        profile = {
            "ID_USER": 17,
            "NA_USER_NAME": "Maria Silva",
            "NA_USER_EMAIL": "maria.silva@example.org",
            "NA_JOB_TITLE": "Analista",
            "NA_DEPARTMENT": "Fiscalização",
            "NA_LOCATION": "Brasília",
            "DT_CREATED_AT": "2026-09-01 10:00:00",
            "DT_UPDATED_AT": "2026-09-10 12:00:00",
            "NA_ROLE": "developer",
            "IS_ADMIN": 0,
            "IS_DEVELOPER": 1,
        }
        with patch(
            "modules.users.routes.service.get_current_user_profile",
            return_value=profile,
        ):
            response = self.client.get(
                "/api/users/me",
                headers={"X-User-Email": "MARIA.SILVA@example.org"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.get_json(), profile)

    def test_current_user_rejects_request_without_proxy_identity(self):
        response = self.client.get("/api/users/me")

        self.assertEqual(response.status_code, 401)

    def test_debug_headers_route_is_removed(self):
        response = self.client.get("/debug/headers")

        self.assertEqual(response.status_code, 404)

    def test_health_route_is_removed(self):
        response = self.client.get("/health")

        self.assertEqual(response.status_code, 404)

    def test_json_and_admin_routes_use_api_prefix(self):
        rules = {
            (rule.rule, method)
            for rule in self.module.app.url_map.iter_rules()
            for method in rule.methods
        }
        expected_rules = {
            ("/api/server/runtime-health", "GET"),
            ("/api/server/zabbix_metrics", "GET"),
            ("/api/maintenance/file-task-hosts", "GET"),
            ("/api/task/host/<int:host_id>/backup-defaults", "GET"),
            ("/api/task/hosts/backup-defaults", "GET"),
            ("/api/users/", "POST"),
            ("/api/users/privileges", "POST"),
            ("/api/users/delete", "POST"),
            ("/api/host-configuration/macro", "POST"),
        }
        removed_rules = {
            ("/server/runtime-health", "GET"),
            ("/server/zabbix_metrics", "GET"),
            ("/maintenance/file-task-hosts", "GET"),
            ("/task/api/host/<int:host_id>/backup-defaults", "GET"),
            ("/task/api/hosts/backup-defaults", "GET"),
            ("/users/", "POST"),
            ("/users/privileges", "POST"),
            ("/users/delete", "POST"),
            ("/host-configuration/macro", "POST"),
        }

        self.assertTrue(expected_rules.issubset(rules))
        self.assertTrue(removed_rules.isdisjoint(rules))

    def test_normalizes_utf8_identity_header_decoded_as_latin1(self):
        with self.module.app.test_request_context(
            "/",
            headers={"X-User-Name": "Augusto CÃ©sar Federici Peterle"},
        ):
            self.assertEqual(
                self.module.AUTH_SERVICE.identity_header_value(
                    self.module.request,
                    "X-User-Name",
                ),
                "Augusto César Federici Peterle",
            )

    def test_removes_email_repeated_in_f5_display_name(self):
        headers = {
            "X-User-Name": "Augusto César Federici Peterleaugusto.cesar@example.org",
            "X-User-Email": "augusto.cesar@example.org",
        }

        with self.module.app.test_request_context("/", headers=headers):
            self.module.load_request_identity()
            self.assertEqual(
                self.module.g.webfusion_user["name"],
                "Augusto César Federici Peterle",
            )

    def test_does_not_register_malformed_f5_email(self):
        headers = {
            "X-User-Name": "Identidade inválida",
            "X-User-Email": "Identidade inválida@",
        }

        with self.module.app.test_request_context("/", headers=headers):
            self.module.load_request_identity()
            self.module.inject_request_identity()

        self.register_user_mock.assert_not_called()

    def test_registers_page_identity_once_per_process(self):
        headers = {
            "X-User-Name": "Maria Silva",
            "X-User-Email": "MARIA.SILVA@example.org",
            "X-User-Job-Title": "Analista",
            "X-User-Department": "Fiscalização",
            "X-User-Location": "Brasília",
        }

        with self.module.app.test_request_context("/", headers=headers):
            self.module.load_request_identity()
            self.module.inject_request_identity()
        with self.module.app.test_request_context("/", headers=headers):
            self.module.load_request_identity()
            self.module.inject_request_identity()

        self.register_user_mock.assert_called_once_with(
            user_name="Maria Silva",
            user_email="maria.silva@example.org",
            job_title="Analista",
            department="Fiscalização",
            location="Brasília",
        )

    def test_does_not_register_identity_for_api_request(self):
        with self.module.app.test_request_context(
            "/api/map/stations",
            headers={
                "Accept": "application/json",
                "X-User-Email": "maria.silva@example.org",
            },
        ):
            self.module.load_request_identity()

        self.register_user_mock.assert_not_called()

    def test_refreshes_identity_when_an_f5_attribute_changes(self):
        headers = {
            "X-User-Name": "Maria Silva",
            "X-User-Email": "maria.silva@example.org",
            "X-User-Department": "Fiscalização",
        }
        with self.module.app.test_request_context("/", headers=headers):
            self.module.load_request_identity()
            self.module.inject_request_identity()

        headers["X-User-Department"] = "Supervisão"
        with self.module.app.test_request_context("/", headers=headers):
            self.module.load_request_identity()
            self.module.inject_request_identity()

        self.assertEqual(self.register_user_mock.call_count, 2)

    def test_restricted_module_rejects_anonymous_user(self):
        response = self.client.get("/task/")

        self.assertEqual(response.status_code, 403)

    def test_restricted_module_rejects_user_without_access_role(self):
        module = importlib.import_module("app")
        with patch("auth.service.get_access_role", return_value=None):
            response = self.client.get(
                "/task/",
                headers={"X-User-Email": "visitor@example.org"},
            )

        self.assertEqual(response.status_code, 403)

    def test_template_context_queries_access_role_for_navigation(self):
        module = importlib.import_module("app")
        with patch("auth.service.get_access_role", return_value="developer"):
            with module.app.test_request_context(
                "/",
                headers={
                    "X-User-Email": "maria.silva@example.org",
                },
            ):
                module.load_request_identity()
                context = module.inject_request_identity()
                self.assertEqual(context["current_user"]["role"], "developer")

    def test_api_routes_do_not_query_access_role(self):
        module = importlib.import_module("app")
        with patch(
            "auth.service.get_access_role",
            side_effect=AssertionError("API routes must not query the access-control database."),
        ):
            with module.app.test_request_context(
                "/api/map/stations",
                headers={
                    "Accept": "application/json",
                    "X-User-Email": "maria.silva@example.org",
                },
            ):
                module.load_request_identity()
                self.assertIsNone(module.g.webfusion_user["role"])

    def test_restricted_module_accepts_configured_user_role(self):
        module = importlib.import_module("app")
        with patch("auth.service.get_access_role", return_value="developer"):
            with module.app.test_request_context(
                "/task/",
                headers={
                    "X-User-Name": "Maria Silva",
                    "X-User-Email": "maria.silva@example.org",
                },
            ):
                module.load_request_identity()
                self.assertIsNone(module.require_restricted_access())
                self.assertEqual(module.g.webfusion_user["role"], "developer")
                self.assertEqual(module.g.webfusion_user["label"], "Maria Silva")


if __name__ == "__main__":
    unittest.main()
