"""Validation tests for the proxy-backed current-user API."""

import importlib
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


WEBFUSION_ROOT = Path(__file__).resolve().parents[3] / "src/webfusion"


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
        self.module.AUTH_SERVICE.photo_refresh_after.clear()
        from api.microsoft_api import MicrosoftNotConfigured
        self.graph_lookup = patch("api.microsoft_api.get_profile_image_url", side_effect=MicrosoftNotConfigured())
        self.graph_mock = self.graph_lookup.start()
        self.addCleanup(self.graph_lookup.stop)
        self.register_observed_user = patch.object(
            importlib.import_module("auth.service"),
            "register_observed_user",
        )
        self.register_user_mock = self.register_observed_user.start()
        self.addCleanup(self.register_observed_user.stop)
        self.profile_image_lookup = patch("auth.service.get_profile_image_url", return_value=None)
        self.profile_image_mock = self.profile_image_lookup.start()
        self.addCleanup(self.profile_image_lookup.stop)
        self.role_lookup = patch("auth.service.get_access_role", return_value=None)
        self.role_lookup.start()
        self.addCleanup(self.role_lookup.stop)

    def tearDown(self):
        self.module.AUTH_SERVICE.observed_user_profiles.clear()

    def test_login_returns_current_user_profile_in_header_with_empty_body(self):
        profile = {
            "ID_USER": 17,
            "NA_USER_NAME": "Maria Silva",
            "NA_USER_EMAIL": "maria.silva@example.org",
            "NA_JOB_TITLE": "Analista",
            "NA_DEPARTMENT": "Fiscalização",
            "NA_LOCATION": "Brasília",
            "NA_URL_PROFILE_IMG": "https://photos.example.org/maria.jpg",
            "DT_CREATED_AT": "2026-09-01 10:00:00",
            "DT_UPDATED_AT": "2026-09-10 12:00:00",
            "NA_ROLE": "admin",
            "IS_ADMIN": 1,
            "IS_DEVELOPER": 0,
        }
        with patch(
            "modules.users.routes.service.get_current_user_profile",
            return_value=profile,
        ):
            headers = {"X-User-Email": "maria.silva@example.org"}
            login_response = self.client.get("/api/users/login", headers=headers)
            current_user_response = self.client.get("/api/users/me", headers=headers)

        self.assertEqual(login_response.status_code, 200)
        self.assertEqual(login_response.get_data(), b"")
        self.assertEqual(
            self.module.app.json.loads(login_response.headers["X-User-Profile"]),
            current_user_response.get_json(),
        )
        self.assertEqual(login_response.headers["Cache-Control"], "no-store")

    def test_login_without_proxy_identity_returns_empty_profile_header(self):
        response = self.client.get("/api/users/login")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_data(), b"")
        self.assertEqual(response.headers["X-User-Profile"], "{}")

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
                "/api/users/",
                headers={"X-User-Email": "visitor@example.org"},
            )

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.headers["X-User-Roles"], "user")

    def test_user_headers_accepts_path_without_trailing_slash(self):
        response = self.client.get("/api/users")

        self.assertEqual(response.status_code, 204)

    def test_user_admin_api_remains_restricted(self):
        response = self.client.post("/api/users/")

        self.assertEqual(response.status_code, 403)

    def test_task_workspace_requires_role_and_renders_for_authorized_user(self):
        self.assertEqual(self.client.get("/tasks/").status_code, 403)

        with patch("auth.service.get_access_role", return_value="developer"):
            response = self.client.get(
                "/tasks/",
                headers={"X-User-Email": "operator@example.org"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("Tarefas", response.get_data(as_text=True))
        self.assertIn("Gerenciar tarefas de estação", response.get_data(as_text=True))
        self.assertIn("Gerenciar fila de arquivos", response.get_data(as_text=True))
        self.assertIn("Recuperar a partir do histórico", response.get_data(as_text=True))

    def test_home_exposes_task_actions_only_to_privileged_roles(self):
        scenarios = (
            (None, {}, "false"),
            (None, {"X-User-Email": "visitor@example.org"}, "false"),
            ("admin", {"X-User-Email": "admin@example.org"}, "true"),
            ("developer", {"X-User-Email": "developer@example.org"}, "true"),
        )

        for role, headers, expected in scenarios:
            with self.subTest(role=role, headers=headers):
                with (
                    patch("app.get_station_map_dataset", return_value={"points": [], "site_details": []}),
                    patch("auth.service.get_access_role", return_value=role),
                ):
                    response = self.client.get("/", headers=headers)

                self.assertEqual(response.status_code, 200)
                self.assertIn(
                    f'data-can-manage-tasks="{expected}"',
                    response.get_data(as_text=True),
                )

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
            ("/api/tasks/file-task-hosts", "GET"),
            ("/api/tasks/host/<int:host_id>/backup-defaults", "GET"),
            ("/api/tasks/hosts/backup-defaults", "GET"),
            ("/tasks/stations", "GET"),
            ("/tasks/files", "GET"),
            ("/tasks/history", "GET"),
            ("/configuration/", "GET"),
            ("/api/users/", "POST"),
            ("/api/users/privileges", "POST"),
            ("/api/users/delete", "POST"),
            ("/api/configuration/macro", "POST"),
        }
        removed_rules = {
            ("/server/runtime-health", "GET"),
            ("/server/zabbix_metrics", "GET"),
            ("/maintenance/file-task-hosts", "GET"),
            ("/api/maintenance/file-task-hosts", "GET"),
            ("/api/task/host/<int:host_id>/backup-defaults", "GET"),
            ("/api/task/hosts/backup-defaults", "GET"),
            ("/task/api/host/<int:host_id>/backup-defaults", "GET"),
            ("/task/api/hosts/backup-defaults", "GET"),
            ("/tasks/queues", "GET"),
            ("/host-configuration/", "GET"),
            ("/users/", "POST"),
            ("/users/privileges", "POST"),
            ("/users/delete", "POST"),
            ("/configuration/macro", "POST"),
        }

        self.assertTrue(expected_rules.issubset(rules))
        self.assertTrue(removed_rules.isdisjoint(rules))

    def test_all_json_handlers_are_registered_below_api_root(self):
        json_endpoints = {
            "map_api.map_stations",
            "map_api.map_station_detail",
            "host.host_zabbix_metrics",
            "host.host_processing_errors",
            "host.host_backup_errors",
            "host.host_locations",
            "host.host_current_activity",
            "host.host_activity_detail",
            "host.processed_file_spectrum_metadata",
            "host.start_connectivity_test",
            "host.connectivity_test_status",
            "server.server_zabbix_metrics",
            "server.server_processing_errors",
            "server.server_backup_errors",
            "server.server_summary_metrics",
            "server.server_usage_metrics",
            "server.server_runtime_health",
            "server.server_download_action_metric",
            "server.server_hosts",
            "spectrum.spectrum_filters",
            "spectrum.spectrum_localities",
            "spectrum.spectrum_file_spectra",
            "tasks_api.file_task_hosts",
            "tasks_api.task_zabbix_backup_defaults",
            "tasks_api.task_zabbix_collective_backup_defaults",
            "users_api.current_user",
        }
        rules_by_endpoint = {
            rule.endpoint: rule.rule
            for rule in self.module.app.url_map.iter_rules()
            if rule.endpoint in json_endpoints
        }

        self.assertEqual(set(rules_by_endpoint), json_endpoints)
        self.assertTrue(
            all(rule.startswith("/api/") for rule in rules_by_endpoint.values()),
            rules_by_endpoint,
        )

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
            profile_image_url=None,
        )

    def test_proxy_photo_is_persisted_and_rendered_next_to_user_name(self):
        photo = "https://photos.example.org/maria.jpg"
        with self.module.app.test_request_context("/", headers={
            "X-User-Email": "maria@example.org",
            "X-User-Name": "Maria",
            "X-User-Avatar-Url": photo,
        }):
            self.module.load_request_identity()
            html = self.module.render_template("base.html")

        self.assertIn(f'src="{photo}"', html)
        self.assertIn('<span>Maria</span>', html)
        self.assertEqual(self.register_user_mock.call_args.kwargs["profile_image_url"], photo)
        self.profile_image_mock.assert_not_called()

    def test_missing_proxy_photo_uses_stored_photo_without_erasing_it(self):
        photo = "/rffusion/static/photos/maria.jpg"
        self.profile_image_mock.return_value = photo
        with self.module.app.test_request_context("/", headers={"X-User-Email": "maria@example.org"}):
            self.module.load_request_identity()
            html = self.module.render_template("base.html")
        self.assertIn(f'src="{photo}"', html)
        self.assertIsNone(self.register_user_mock.call_args.kwargs["profile_image_url"])

    def test_photo_url_rejects_credentials_and_unsafe_schemes(self):
        for value in (
            "javascript:alert(1)", "data:image/svg+xml,anything", "//outside.example/photo",
            "http://photos.example/photo", "https://user:secret@photos.example/photo",
            "https://photos.example/photo?access_token=secret", "/photo?token=secret",
            "/\\outside.example/photo", "https://[invalid/photo",
        ):
            with self.subTest(value=value):
                self.assertIsNone(self.module.AUTH_SERVICE.normalize_profile_image_url(value))

    def test_photo_change_refreshes_observed_profile(self):
        for photo in ("https://photos.example/one.jpg", "https://photos.example/two.jpg"):
            with self.module.app.test_request_context("/", headers={
                "X-User-Email": "maria@example.org", "X-User-Avatar-Url": photo,
            }):
                self.module.load_request_identity()
                self.module.inject_request_identity()
        self.assertEqual(self.register_user_mock.call_count, 2)

    def test_fallback_svg_uses_name_initial_and_is_not_cached(self):
        from xml.etree import ElementTree
        response = self.client.get("/api/users/avatar.svg", headers={
            "X-User-Name": "Ágata Silva", "X-User-Email": "agata@example.org",
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "image/svg+xml")
        self.assertEqual(response.headers["Cache-Control"], "private, no-store")
        root = ElementTree.fromstring(response.data)
        self.assertEqual(root.find(".//{http://www.w3.org/2000/svg}text").text, "Á")
        self.graph_mock.assert_not_called()

    def test_fallback_svg_escapes_markup_in_initial(self):
        from xml.etree import ElementTree
        response = self.client.get("/api/users/avatar.svg", headers={"X-User-Name": "<script>"})
        root = ElementTree.fromstring(response.data)
        self.assertEqual(root.find(".//{http://www.w3.org/2000/svg}text").text, "<")
        self.assertNotIn(b"<script>", response.data)

    def test_fallback_svg_uses_email_initial_without_name(self):
        from xml.etree import ElementTree
        response = self.client.get("/api/users/avatar.svg", headers={"X-User-Email": "maria@example.org"})
        root = ElementTree.fromstring(response.data)
        self.assertEqual(root.find(".//{http://www.w3.org/2000/svg}text").text, "M")

    def test_anonymous_header_uses_profile_out(self):
        with self.module.app.test_request_context("/"):
            self.module.load_request_identity()
            html = self.module.render_template("base.html")
        self.assertIn('/static/img/profile_out.svg', html)
        self.assertNotIn('/api/users/avatar.svg', html)
        self.graph_mock.assert_not_called()

    def test_graph_photo_change_updates_database_and_current_header(self):
        self.profile_image_mock.return_value = "/rffusion/static/img/profiles/old.jpg"
        self.graph_mock.side_effect = None
        photo = "/rffusion/static/img/profiles/new.jpg"
        self.graph_mock.return_value = photo
        with patch("auth.service.update_profile_image_url") as update:
            with self.module.app.test_request_context("/", headers={"X-User-Email": "maria@example.org"}):
                self.module.load_request_identity()
                context = self.module.inject_request_identity()
                self.assertEqual(context["current_user"]["avatar_url"], photo)
            update.assert_called_once_with("maria@example.org", photo)
            self.profile_image_mock.return_value = photo
            with self.module.app.test_request_context("/", headers={"X-User-Email": "maria@example.org"}):
                self.module.load_request_identity()
                self.module.inject_request_identity()
        self.graph_mock.assert_called_once()

    def test_unchanged_graph_photo_does_not_write_to_database(self):
        photo = "/rffusion/static/img/profiles/unchanged.jpg"
        self.profile_image_mock.return_value = photo
        self.graph_mock.side_effect = None
        self.graph_mock.return_value = photo
        with patch("auth.service.update_profile_image_url") as update:
            with self.module.app.test_request_context("/", headers={"X-User-Email": "maria@example.org"}):
                self.module.load_request_identity()
                self.module.inject_request_identity()
            update.assert_not_called()

    def test_graph_failure_preserves_existing_photo(self):
        from api.microsoft_api import MicrosoftPhotoError, MicrosoftPhotoNotFound
        photo = "https://photos.example/existing.jpg"
        for failure in (MicrosoftPhotoError("Unavailable"), MicrosoftPhotoNotFound()):
            with self.subTest(failure=type(failure).__name__):
                self.module.AUTH_SERVICE.photo_refresh_after.clear()
                self.profile_image_mock.return_value = photo
                self.graph_mock.side_effect = failure
                with patch("auth.service.update_profile_image_url") as update:
                    with self.module.app.test_request_context("/", headers={"X-User-Email": "maria@example.org"}):
                        self.module.load_request_identity()
                        context = self.module.inject_request_identity()
                        self.assertEqual(context["current_user"]["avatar_url"], photo)
                    update.assert_not_called()

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
        response = self.client.get("/tasks/")

        self.assertEqual(response.status_code, 403)

    def test_restricted_module_rejects_user_without_access_role(self):
        module = importlib.import_module("app")
        with patch("auth.service.get_access_role", return_value=None):
            response = self.client.get(
                "/tasks/",
                headers={"X-User-Email": "visitor@example.org"},
            )

        self.assertEqual(response.status_code, 403)

    def test_alarms_is_available_without_a_privileged_role(self):
        scenarios = ({}, {"X-User-Email": "visitor@example.org"})

        for headers in scenarios:
            with self.subTest(headers=headers):
                with (
                    patch("modules.alarms.routes.list_alarms", return_value=[]),
                    patch(
                        "modules.alarms.routes.get_zabbix_problems_url",
                        return_value="https://zabbix.example/zabbix/zabbix.php?action=problem.view",
                    ),
                ):
                    response = self.client.get("/alarms/", headers=headers)

                self.assertEqual(response.status_code, 200)
                self.assertIn("Alarmes", response.get_data(as_text=True))

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
                "/tasks/",
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
