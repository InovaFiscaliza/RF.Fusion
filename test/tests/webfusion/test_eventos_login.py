"""Validate browser form navigation to appEventos without external writes."""

from __future__ import annotations

from html.parser import HTMLParser
import importlib
from pathlib import Path
import sys
import unittest
from unittest.mock import patch


WEBFUSION_ROOT = Path(__file__).resolve().parents[3] / "src/webfusion"


class EventosFormParser(HTMLParser):
    """Collect the appEventos form and hidden fields from rendered HTML.

    Attributes:
        form: Target form attributes as dict[str, str | None], initially empty.
        fields: Hidden field values as dict[str, str | None], initially empty.
        inside_form: Whether the parser is inside the target form, initially False.
    """

    def __init__(self):
        """Initialize an empty form capture."""
        super().__init__()
        self.form = {}
        self.fields = {}
        self.inside_form = False

    def handle_starttag(self, tag, attrs):
        """Capture the destination and decoded input values of the target form."""
        attributes = dict(attrs)
        if tag == "form" and attributes.get("id") == "eventos-login-form":
            self.form = attributes
            self.inside_form = True
        if self.inside_form and tag == "input":
            self.fields[attributes["name"]] = attributes.get("value")

    def handle_endtag(self, tag):
        """Stop input capture when the target form closes."""
        if tag == "form":
            self.inside_form = False


class TestEventosLogin(unittest.TestCase):
    """Check browser navigation, selected fields, escaping, and identity guards."""

    @classmethod
    def setUpClass(cls):
        """Load the real Flask application in the authorized webserver runtime."""
        sys.path.insert(0, str(WEBFUSION_ROOT))
        cls.module = importlib.import_module("app")
        cls.service = importlib.import_module("modules.users.service")
        cls.module.app.config.update(TESTING=True)
        cls.client = cls.module.app.test_client()

    def setUp(self):
        """Replace storage and profile enrichment with isolated doubles."""
        self.module.AUTH_SERVICE.observed_user_profiles.clear()
        self.addCleanup(self.module.AUTH_SERVICE.observed_user_profiles.clear)
        self.profile = {
            "ID_USER": 987,
            "NA_USER_EMAIL": "fiscal@example.test",
            "NA_USER_NAME": "Fiscal Ação",
            "NA_DEPARTMENT": None,
            "NA_JOB_TITLE": None,
            "NA_LOCATION": "Brasília",
            "NA_ROLE": "admin",
        }
        self.headers = {"X-User-Email": "fiscal@example.test"}
        patches = {
            "registration": patch("auth.service.register_observed_user"),
            "profile_lookup": patch.object(self.service, "get_current_user_profile", return_value=self.profile),
            "photo_lookup": patch.object(self.module.AUTH_SERVICE, "load_profile_image"),
            "role_lookup": patch.object(self.module.AUTH_SERVICE, "load_access_role"),
        }
        for name, patcher in patches.items():
            setattr(self, name, patcher.start())
            self.addCleanup(patcher.stop)

    def test_both_routes_render_a_post_in_the_same_tab(self):
        """Navigate with only profile form fields and the fixed destination."""
        for path in ("/api/users/login_eventos", "/api/users/login_evento"):
            with self.subTest(path=path):
                response = self.client.get(path, headers=self.headers)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.mimetype, "text/html")
                self.assertEqual(response.headers["Cache-Control"], "no-store")
                self.assertEqual(response.headers["Referrer-Policy"], "no-referrer")
                parser = EventosFormParser()
                parser.feed(response.get_data(as_text=True))
                self.assertEqual(parser.form["method"], "post")
                self.assertEqual(parser.form["target"], "_self")
                self.assertEqual(parser.form["enctype"], "application/x-www-form-urlencoded")
                self.assertEqual(parser.form["action"], "http://rhfisnsdsin01:9088/api/usuarios/login-evento")
                self.assertEqual(parser.fields, {
                    "NA_USER_EMAIL": "fiscal@example.test",
                    "NA_USER_NAME": "Fiscal Ação",
                    "NA_DEPARTMENT": "",
                    "NA_JOB_TITLE": "",
                    "NA_LOCATION": "Brasília",
                })
                self.assertNotIn("X-User-Profile", response.headers)
                self.assertNotIn("Set-Cookie", response.headers)
                script = self.client.get("/static/js/login_eventos.js")
                self.assertEqual(script.status_code, 200)
                self.assertIn(b'.requestSubmit()', script.data)
                script.close()

    def test_client_cannot_replace_profile_or_destination(self):
        """Use persisted fields even when the client submits forged values."""
        response = self.client.get(
            "/api/users/login_eventos?next=https://untrusted.example/&NA_USER_EMAIL=other@example.test",
            headers={**self.headers, "Authorization": "Bearer client-token", "X-User-Profile": "forged-profile"},
        )
        parser = EventosFormParser()
        html = response.get_data(as_text=True)
        parser.feed(html)
        self.assertEqual(parser.fields["NA_USER_EMAIL"], "fiscal@example.test")
        self.assertNotIn("untrusted.example", html)
        self.assertNotIn("client-token", html)
        self.assertNotIn("forged-profile", html)
        self.profile_lookup.assert_called_once_with("fiscal@example.test")

    def test_profile_values_are_escaped_without_losing_accents(self):
        """Keep quotes and markup inside input values rather than executable HTML."""
        name = 'Fiscal Ação "><script>alert(1)</script>'
        self.profile["NA_USER_NAME"] = name
        response = self.client.get("/api/users/login_eventos", headers=self.headers)
        html = response.get_data(as_text=True)
        parser = EventosFormParser()
        parser.feed(html)
        self.assertEqual(parser.fields["NA_USER_NAME"], name)
        self.assertNotIn("<script>alert(1)</script>", html)

    def test_no_identity_does_not_query_profile_or_render_form(self):
        """Reject anonymous requests before preparing any profile transfer."""
        response = self.client.get("/api/users/login_eventos")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.json, {"error": "proxy_identity_required"})
        self.profile_lookup.assert_not_called()

    def test_no_profile_does_not_render_form(self):
        """Reject an unregistered user instead of sending an empty identity."""
        self.profile_lookup.return_value = None
        response = self.client.get("/api/users/login_eventos", headers=self.headers)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.json, {"error": "user_not_found"})

    def test_database_failure_does_not_render_form(self):
        """Keep local lookup failures visible without exposing database details."""
        self.profile_lookup.side_effect = RuntimeError("database unavailable")
        with self.assertLogs(self.module.app.logger, level="ERROR"):
            response = self.client.get("/api/users/login_eventos", headers=self.headers)
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(response.json, {"error": "current_user_lookup_failed"})


if __name__ == "__main__":
    unittest.main()
