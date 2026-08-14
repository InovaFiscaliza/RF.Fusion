"""
Validation tests for `webfusion.modules.task.routes`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_task_routes.py -q

What is covered here:
    - family-profile rows are built with prefilled defaults for known station
      groups such as RFEye and CelPlan
    - collective mixed-family requests split into per-family batches when the
      builder is still using auto-suggested defaults
    - explicit per-family overrides drive batch-specific path/extension values
    - explicit custom path/extension keeps a shared collective request intact
    - stop/rollback tasks do not expose backlog-budget semantics
    - stop/rollback tasks normalize unsupported filter modes to ALL
"""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")
SOURCE_ROOT = Path("/RFFusion/src")


def load_task_routes():
    """Reload the task routes so helper tests observe current module constants."""
    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)
    source_root = str(SOURCE_ROOT)
    if source_root not in sys.path:
        sys.path.insert(0, source_root)

    fake_flask = ModuleType("flask")

    class FakeBlueprint:
        def __init__(self, *args, **kwargs):
            pass

        def before_request(self, func):
            return func

        def route(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

    fake_flask.Blueprint = FakeBlueprint
    fake_flask.Response = lambda *args, **kwargs: None
    fake_flask.current_app = SimpleNamespace(logger=SimpleNamespace(warning=lambda *args: None))
    fake_flask.jsonify = lambda payload: payload
    fake_flask.redirect = lambda *args, **kwargs: None
    fake_flask.render_template = lambda *args, **kwargs: None
    fake_flask.request = SimpleNamespace(
        authorization=None,
        args={},
        form={},
        method="GET",
    )
    fake_flask.url_for = lambda *args, **kwargs: ""

    fake_db = ModuleType("db")
    fake_db.get_connection_bpdata = lambda: None

    sys.modules["flask"] = fake_flask
    sys.modules["db"] = fake_db
    sys.modules.pop("modules.task.routes", None)
    return importlib.import_module("modules.task.routes")


class TestTaskRoutes(unittest.TestCase):
    """Protect the collective builder heuristics for mixed station families."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_task_routes()

    def test_station_profile_rows_prefill_known_families(self):
        rows = self.module._build_station_profile_rows(
            [
                {"PREFIX": "RFEye", "HOSTS": 5},
                {"PREFIX": "CWSM", "HOSTS": 7},
            ]
        )

        indexed = {row["prefix"].upper(): row for row in rows}

        self.assertEqual(
            indexed["RFEYE"]["file_path"],
            self.module.DEFAULT_LINUX_FILE_PATH,
        )
        self.assertEqual(
            indexed["RFEYE"]["extension"],
            self.module.DEFAULT_LINUX_EXTENSION,
        )
        self.assertEqual(
            indexed["CWSM"]["file_path"],
            self.module.DEFAULT_CWSM_FILE_PATH,
        )
        self.assertEqual(
            indexed["CWSM"]["extension"],
            self.module.DEFAULT_CWSM_EXTENSION,
        )

    def test_collective_auto_defaults_split_mixed_station_families(self):
        batches = self.module._build_collective_task_batches(
            host_rows=[
                {"ID_HOST": 11, "NA_HOST_NAME": "RFEye002264"},
                {"ID_HOST": 12, "NA_HOST_NAME": "CWSM211006"},
            ],
            filter_data={
                "mode": "NONE",
                "start_date": None,
                "end_date": None,
                "last_n_files": None,
                "extension": ".bin",
                "file_path": "/mnt/internal/data",
                "file_name": None,
            },
        )

        self.assertEqual(len(batches), 2)

        flattened = {
            tuple(batch["hosts"]): (
                batch["filter_data"]["file_path"],
                batch["filter_data"]["extension"],
            )
            for batch in batches
        }

        self.assertEqual(
            flattened[(11,)],
            (self.module.DEFAULT_LINUX_FILE_PATH, self.module.DEFAULT_LINUX_EXTENSION),
        )
        self.assertEqual(
            flattened[(12,)],
            (self.module.DEFAULT_CWSM_FILE_PATH, self.module.DEFAULT_CWSM_EXTENSION),
        )

    def test_collective_profile_overrides_drive_family_specific_batches(self):
        batches = self.module._build_collective_task_batches(
            host_rows=[
                {"ID_HOST": 31, "NA_HOST_NAME": "RFEye002264"},
                {"ID_HOST": 32, "NA_HOST_NAME": "CWSM211006"},
            ],
            filter_data={
                "mode": "NONE",
                "start_date": None,
                "end_date": None,
                "last_n_files": None,
                "extension": None,
                "file_path": None,
                "file_name": None,
            },
            profile_overrides={
                "RFEYE": {
                    "file_path": "/mnt/internal/custom",
                    "extension": ".bin",
                },
                "CWSM": {
                    "file_path": "C:/CelPlan/Custom",
                    "extension": ".zip",
                },
            },
        )

        flattened = {
            tuple(batch["hosts"]): (
                batch["filter_data"]["file_path"],
                batch["filter_data"]["extension"],
            )
            for batch in batches
        }

        self.assertEqual(flattened[(31,)], ("/mnt/internal/custom", ".bin"))
        self.assertEqual(flattened[(32,)], ("C:/CelPlan/Custom", ".zip"))

    def test_ermx_hosts_share_one_station_family(self):
        self.assertEqual(self.module._extract_host_prefix("ERMxES02"), "ERMX")
        self.assertEqual(self.module._extract_host_prefix("ERMxBA01"), "ERMX")

        batches = self.module._build_collective_task_batches(
            host_rows=[
                {"ID_HOST": 35, "NA_HOST_NAME": "ERMxES02"},
                {"ID_HOST": 36, "NA_HOST_NAME": "ERMxBA01"},
            ],
            filter_data={
                "mode": "NONE",
                "start_date": None,
                "end_date": None,
                "last_n_files": None,
                "extension": None,
                "file_path": None,
                "file_name": None,
            },
            profile_overrides={
                "ERMX": {
                    "file_path": "/data/ermx",
                    "extension": ".bin",
                }
            },
        )

        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["hosts"], [35, 36])
        self.assertEqual(batches[0]["filter_data"]["file_path"], "/data/ermx")

    def test_collective_zabbix_defaults_split_batches_by_effective_values(self):
        batches = self.module._build_collective_task_batches(
            host_rows=[
                {"ID_HOST": 41, "NA_HOST_NAME": "ERMxBA01"},
                {"ID_HOST": 42, "NA_HOST_NAME": "ERMxBA02"},
                {"ID_HOST": 43, "NA_HOST_NAME": "ERMxBA03"},
            ],
            filter_data={
                "mode": "NONE",
                "start_date": None,
                "end_date": None,
                "last_n_files": None,
                "extension": ".bin",
                "file_path": "/mnt/internal/data",
                "file_name": None,
            },
            zabbix_defaults_by_host={
                "41": {"file_path": "/data/ermx", "extension": ".zip"},
                "42": {"file_path": "/data/ermx", "extension": ".zip"},
                "43": {"file_path": "/data/ermx-legacy", "extension": ".bin"},
            },
        )

        flattened = {
            tuple(batch["hosts"]): (
                batch["filter_data"]["file_path"],
                batch["filter_data"]["extension"],
            )
            for batch in batches
        }

        self.assertEqual(flattened[(41, 42)], ("/data/ermx", ".zip"))
        self.assertEqual(flattened[(43,)], ("/data/ermx-legacy", ".bin"))

    def test_collective_zabbix_defaults_ignore_unselected_host_payload(self):
        defaults = self.module._parse_collective_zabbix_defaults(
            '{"41":{"file_path":"/data/ermx","extension":".zip"},'
            '"999":{"file_path":"/untrusted","extension":".raw"}}',
            [41],
        )

        self.assertEqual(
            defaults,
            {"41": {"file_path": "/data/ermx", "extension": ".zip"}},
        )

    def test_collective_backup_defaults_endpoint_deduplicates_host_ids(self):
        self.module.request.args = SimpleNamespace(
            getlist=lambda name: ["41", "invalid", "41", "42"]
        )

        with patch.object(
            self.module,
            "get_hosts_backup_defaults",
            return_value={
                "41": {"file_path": "/data/ermx", "extension": ".zip"},
                "42": {"file_path": "/data/ermx", "extension": ".zip"},
            },
        ) as get_hosts_backup_defaults:
            response = self.module.task_zabbix_collective_backup_defaults()

        self.assertEqual(response["source"], "zabbix")
        self.assertEqual(response["defaults"]["41"]["file_path"], "/data/ermx")
        get_hosts_backup_defaults.assert_called_once_with(["41", "42"])

    def test_collective_explicit_filter_stays_shared(self):
        batches = self.module._build_collective_task_batches(
            host_rows=[
                {"ID_HOST": 21, "NA_HOST_NAME": "RFEye002264"},
                {"ID_HOST": 22, "NA_HOST_NAME": "CWSM211006"},
            ],
            filter_data={
                "mode": "RANGE",
                "start_date": "2025-01-01",
                "end_date": None,
                "last_n_files": None,
                "extension": ".zip",
                "file_path": "/custom/shared/path",
                "file_name": None,
            },
        )

        self.assertEqual(len(batches), 1)
        self.assertEqual(batches[0]["hosts"], [21, 22])
        self.assertEqual(batches[0]["filter_data"]["file_path"], "/custom/shared/path")
        self.assertEqual(batches[0]["filter_data"]["extension"], ".zip")

    def test_task_type_supports_backlog_budget_only_for_backup_requests(self):
        self.assertTrue(
            self.module._task_type_supports_backlog_budget(
                self.module.HOST_TASK_CHECK_TYPE
            )
        )
        self.assertFalse(
            self.module._task_type_supports_backlog_budget(
                self.module.HOST_TASK_BACKLOG_ROLLBACK_TYPE
            )
        )

    def test_filter_mode_supports_backlog_budget_hides_budget_for_none_and_rediscovery(self):
        self.assertFalse(self.module._filter_mode_supports_backlog_budget("NONE"))
        self.assertFalse(self.module._filter_mode_supports_backlog_budget("REDISCOVERY"))
        self.assertTrue(self.module._filter_mode_supports_backlog_budget("ALL"))
        self.assertTrue(self.module._filter_mode_supports_backlog_budget("RANGE"))

    def test_selection_supports_backlog_budget_requires_supported_task_and_mode(self):
        self.assertFalse(
            self.module._selection_supports_backlog_budget(
                self.module.HOST_TASK_CHECK_TYPE,
                "NONE",
            )
        )
        self.assertFalse(
            self.module._selection_supports_backlog_budget(
                self.module.HOST_TASK_CHECK_TYPE,
                "REDISCOVERY",
            )
        )
        self.assertFalse(
            self.module._selection_supports_backlog_budget(
                self.module.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
                "RANGE",
            )
        )
        self.assertTrue(
            self.module._selection_supports_backlog_budget(
                self.module.HOST_TASK_CHECK_TYPE,
                "LAST",
            )
        )

    def test_normalize_filter_mode_for_stop_replaces_none_and_rediscovery(self):
        self.assertEqual(
            self.module._normalize_filter_mode_for_task_type(
                "NONE",
                self.module.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
            ),
            "ALL",
        )
        self.assertEqual(
            self.module._normalize_filter_mode_for_task_type(
                "REDISCOVERY",
                self.module.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
            ),
            "ALL",
        )
        self.assertEqual(
            self.module._normalize_filter_mode_for_task_type(
                "RANGE",
                self.module.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
            ),
            "RANGE",
        )

    def test_action_selection_fixes_discovery_and_rediscovery_modes(self):
        discover_action = self.module._resolve_task_action(
            self.module.TASK_ACTION_DISCOVER
        )
        rediscover_action = self.module._resolve_task_action(
            self.module.TASK_ACTION_REDISCOVER
        )

        self.assertEqual(
            self.module._resolve_action_mode(discover_action, "ALL"),
            "NONE",
        )
        self.assertEqual(
            self.module._resolve_action_mode(rediscover_action, "ALL"),
            "REDISCOVERY",
        )

    def test_connectivity_action_uses_the_interactive_queue_type(self):
        action = self.module._resolve_task_action(
            self.module.TASK_ACTION_CONNECTIVITY_TEST
        )

        self.assertEqual(
            action["task_type"],
            self.module.HOST_TASK_INTERACTIVE_CHECK_TYPE,
        )

    def test_generic_backup_rejects_discovery_and_rediscovery_modes(self):
        backup_action = self.module._resolve_task_action(
            self.module.TASK_ACTION_BACKUP
        )

        self.assertEqual(
            self.module._resolve_action_mode(backup_action, "NONE"),
            "ALL",
        )
        self.assertEqual(
            self.module._resolve_action_mode(backup_action, "REDISCOVERY"),
            "ALL",
        )

    def test_legacy_task_links_map_to_the_matching_visible_action(self):
        self.assertEqual(
            self.module._action_from_legacy_selection(
                self.module.HOST_TASK_CHECK_TYPE,
                "NONE",
            ),
            self.module.TASK_ACTION_DISCOVER,
        )
        self.assertEqual(
            self.module._action_from_legacy_selection(
                self.module.HOST_TASK_CHECK_TYPE,
                "REDISCOVERY",
            ),
            self.module.TASK_ACTION_REDISCOVER,
        )
        self.assertEqual(
            self.module._action_from_legacy_selection(
                self.module.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
                "ALL",
            ),
            self.module.TASK_ACTION_BACKLOG_ROLLBACK,
        )

    def test_extract_zabbix_backup_defaults_uses_only_plain_text_macros(self):
        defaults = self.module._extract_zabbix_backup_defaults(
            {
                "macros": [
                    {
                        "name": "{$BACKUP_PATH}",
                        "type": "0",
                        "accepts_value": True,
                        "editable_value": "/mnt/rfeye/backup",
                    },
                    {
                        "name": "{$BACKUP_EXTENSION}",
                        "type": "0",
                        "accepts_value": True,
                        "editable_value": ".raw",
                    },
                    {
                        "name": "{$SSH_PASSWD}",
                        "type": "1",
                        "accepts_value": True,
                        "editable_value": "",
                    },
                ]
            }
        )

        self.assertEqual(defaults["file_path"], "/mnt/rfeye/backup")
        self.assertEqual(defaults["extension"], ".raw")

    def test_extract_zabbix_backup_defaults_ignores_secret_macro_values(self):
        defaults = self.module._extract_zabbix_backup_defaults(
            {
                "macros": [
                    {
                        "name": "{$BACKUP_PATH}",
                        "type": "1",
                        "accepts_value": True,
                        "editable_value": "",
                        "display_value": "/should-not-be-read",
                    },
                    {
                        "name": "{$BACKUP_EXTENSION}",
                        "type": "2",
                        "accepts_value": False,
                        "editable_value": ".should-not-be-read",
                    },
                ]
            }
        )

        self.assertEqual(defaults, {"file_path": None, "extension": None})

    def test_task_zabbix_backup_defaults_returns_effective_plain_text_values(self):
        with patch.object(
            self.module,
            "get_configuration",
            return_value={
                "macros": [
                    {
                        "name": "{$BACKUP_PATH}",
                        "type": "0",
                        "accepts_value": True,
                        "editable_value": "/mnt/rfeye/backup",
                    },
                    {
                        "name": "{$BACKUP_EXTENSION}",
                        "type": "0",
                        "accepts_value": True,
                        "editable_value": ".raw",
                    },
                ]
            },
        ) as get_configuration:
            response = self.module.task_zabbix_backup_defaults(10482)

        self.assertEqual(
            response,
            {
                "file_path": "/mnt/rfeye/backup",
                "extension": ".raw",
                "source": "zabbix",
            },
        )
        get_configuration.assert_called_once_with("host", "10482")


if __name__ == "__main__":
    unittest.main()
