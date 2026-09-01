"""Validation tests for the F5 identity-header diagnostic endpoint."""

import importlib
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


class TestIdentityHeaderDebugRoute(unittest.TestCase):
    """Verify that the diagnostic route returns only the expected F5 fields."""

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
        self.module.OBSERVED_USER_PROFILES.clear()
        self.register_observed_user = patch.object(
            self.module,
            "register_observed_user",
        )
        self.register_user_mock = self.register_observed_user.start()
        self.addCleanup(self.register_observed_user.stop)

    def tearDown(self):
        self.module.OBSERVED_USER_PROFILES.clear()

    def test_returns_forwarded_identity_headers_without_cache(self):
        response = self.client.get(
            "/debug/headers",
            headers={
                "X-User-Name": "Maria Silva",
                "X-User-Email": "maria.silva@example.org",
                "X-User-Job-Title": "Analista",
                "X-User-Department": "Fiscalização",
                "X-User-Location": "Brasília",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(
            response.get_json(),
            {
                "X-User-Name": "Maria Silva",
                "X-User-Email": "maria.silva@example.org",
                "X-User-Job-Title": "Analista",
                "X-User-Department": "Fiscalização",
                "X-User-Location": "Brasília",
            },
        )

    def test_returns_null_when_f5_does_not_forward_a_header(self):
        response = self.client.get("/debug/headers")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "X-User-Name": None,
                "X-User-Email": None,
                "X-User-Job-Title": None,
                "X-User-Department": None,
                "X-User-Location": None,
            },
        )
        self.register_user_mock.assert_not_called()

    def test_normalizes_utf8_identity_header_decoded_as_latin1(self):
        response = self.client.get(
            "/debug/headers",
            headers={"X-User-Name": "Augusto CÃ©sar Federici Peterle"},
        )

        self.assertEqual(
            response.get_json()["X-User-Name"],
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
        original_get_access_role = module.get_access_role
        module.get_access_role = lambda user_email: None
        try:
            response = self.client.get(
                "/task/",
                headers={"X-User-Email": "visitor@example.org"},
            )
        finally:
            module.get_access_role = original_get_access_role

        self.assertEqual(response.status_code, 403)

    def test_template_context_queries_access_role_for_navigation(self):
        module = importlib.import_module("app")
        original_get_access_role = module.get_access_role
        module.get_access_role = lambda user_email: "developer"
        try:
            with module.app.test_request_context(
                "/",
                headers={
                    "X-User-Email": "maria.silva@example.org",
                },
            ):
                module.load_request_identity()
                context = module.inject_request_identity()
                self.assertEqual(context["current_user"]["role"], "developer")
        finally:
            module.get_access_role = original_get_access_role

    def test_api_routes_do_not_query_access_role(self):
        module = importlib.import_module("app")
        original_get_access_role = module.get_access_role
        module.get_access_role = lambda user_email: self.fail(
            "API routes must not query the access-control database."
        )
        try:
            with module.app.test_request_context(
                "/api/map/stations",
                headers={
                    "Accept": "application/json",
                    "X-User-Email": "maria.silva@example.org",
                },
            ):
                module.load_request_identity()
                self.assertIsNone(module.g.webfusion_user["role"])
        finally:
            module.get_access_role = original_get_access_role

    def test_restricted_module_accepts_configured_user_role(self):
        module = importlib.import_module("app")
        original_get_access_role = module.get_access_role
        module.get_access_role = lambda user_email: "developer"
        try:
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
        finally:
            module.get_access_role = original_get_access_role


if __name__ == "__main__":
    unittest.main()
