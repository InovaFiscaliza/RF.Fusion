"""Unit tests for the WebFusion Zabbix configuration service."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SRC_ROOT = Path("/RFFusion/src")
WEBFUSION_ROOT = SRC_ROOT / "webfusion"
for root in (str(SRC_ROOT), str(WEBFUSION_ROOT)):
    if root not in sys.path:
        sys.path.insert(0, root)

from modules.zabbix_configuration import service  # noqa: E402


class FakeZabbixClient:
    """Record writes while returning one managed host and inherited macro."""

    def __init__(self):
        self.created = []
        self.updated = []
        self.deleted = []

    def list_catalog(self):
        return {
            "hosts": [{"hostid": "501", "name": "RFEye000501"}],
            "templates": [{"templateid": "100", "name": "RFEye Nodes"}],
        }

    def get_host_configuration(self, target_id):
        return {
            "kind": "host",
            "target_id": target_id,
            "macros": [
                {
                    "name": "{$BACKUP_PATH}",
                    "type": "0",
                    "is_direct_on_target": False,
                    "source_macro_id": "44",
                    "accepts_value": True,
                },
                {
                    "name": "{$SSH_PORT}",
                    "type": "0",
                    "is_direct_on_target": True,
                    "source_macro_id": "45",
                    "accepts_value": True,
                },
                {
                    "name": "{$HOST_ID}",
                    "type": "0",
                    "is_direct_on_target": False,
                    "source_macro_id": "46",
                    "accepts_value": True,
                    "editable_value": "10482",
                },
            ],
        }

    def get_template_configuration(self, target_id):
        raise AssertionError(f"Unexpected template lookup: {target_id}")

    def get_host_configurations(self, target_ids):
        return {
            str(target_id): {
                "macros": [
                    {
                        "name": "{$BACKUP_PATH}",
                        "type": "0",
                        "accepts_value": True,
                        "editable_value": "/mnt/collective",
                    },
                    {
                        "name": "{$BACKUP_EXTENSION}",
                        "type": "0",
                        "accepts_value": True,
                        "editable_value": ".bin",
                    },
                ]
            }
            for target_id in target_ids
        }

    def create_macro(self, **kwargs):
        self.created.append(kwargs)

    def update_macro(self, **kwargs):
        self.updated.append(kwargs)

    def delete_macro(self, **kwargs):
        self.deleted.append(kwargs)


class TestZabbixConfigurationService(unittest.TestCase):
    """Keep writes constrained to the effective macro selected by the user."""

    def setUp(self):
        self.client = FakeZabbixClient()
        service._clear_catalog_cache()
        self.client_patch = patch.object(service, "_build_client", return_value=self.client)
        self.client_patch.start()

    def tearDown(self):
        self.client_patch.stop()
        service._clear_catalog_cache()

    def test_saving_inherited_macro_creates_direct_host_override(self):
        service.apply_macro_change(
            target_kind="host",
            target_id="501",
            macro_name="{$BACKUP_PATH}",
            action=service.ACTION_SAVE,
            submitted_value="/mnt/custom",
        )

        self.assertEqual(
            self.client.created,
            [
                {
                    "owner_id": "501",
                    "macro_name": "{$BACKUP_PATH}",
                    "macro_type": "0",
                    "value": "/mnt/custom",
                }
            ],
        )
        self.assertEqual(self.client.updated, [])

    def test_restoring_direct_macro_deletes_only_direct_override(self):
        service.apply_macro_change(
            target_kind="host",
            target_id="501",
            macro_name="{$SSH_PORT}",
            action=service.ACTION_RESTORE,
        )

        self.assertEqual(self.client.deleted, [{"macro_id": "45"}])

    def test_rejects_restore_when_macro_is_already_inherited(self):
        with self.assertRaises(service.ZabbixConfigurationError):
            service.apply_macro_change(
                target_kind="host",
                target_id="501",
                macro_name="{$BACKUP_PATH}",
                action=service.ACTION_RESTORE,
            )

    def test_rejects_host_identifier_change_even_when_submitted_directly(self):
        with self.assertRaises(service.ZabbixConfigurationError):
            service.apply_macro_change(
                target_kind="host",
                target_id="501",
                macro_name="{$HOST_ID}",
                action=service.ACTION_SAVE,
                submitted_value="999",
            )

        self.assertEqual(self.client.created, [])
        self.assertEqual(self.client.updated, [])
        self.assertEqual(self.client.deleted, [])

    def test_rejects_protected_station_parameter_change(self):
        self.client.get_host_configuration = lambda target_id: {
            "kind": "host",
            "target_id": target_id,
            "macros": [
                {
                    "name": "{$SNMP_COMMUNITY}",
                    "type": "0",
                    "is_direct_on_target": True,
                    "source_macro_id": "47",
                    "accepts_value": True,
                }
            ],
        }

        with self.assertRaises(service.ZabbixConfigurationError):
            service.apply_macro_change(
                target_kind="host",
                target_id="501",
                macro_name="{$SNMP_COMMUNITY}",
                action=service.ACTION_SAVE,
                submitted_value="new-community",
            )

        self.assertEqual(self.client.updated, [])

    def test_local_secret_file_is_used_when_environment_is_absent(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            secret_file = Path(temporary_directory) / "zabbix_api.env"
            secret_file.write_text(
                "ZABBIX_API_URL=http://zabbix.example/api_jsonrpc.php\n"
                "ZABBIX_API_TOKEN=local-token\n"
                "ZABBIX_API_TIMEOUT_SECONDS=7\n",
                encoding="utf-8",
            )
            with patch.object(service, "ZABBIX_SECRET_FILE", secret_file), patch.dict(
                service.os.environ,
                {
                    "ZABBIX_API_URL": "",
                    "ZABBIX_API_TOKEN": "",
                    "ZABBIX_API_TIMEOUT_SECONDS": "",
                },
                clear=False,
            ):
                settings = service._get_zabbix_settings()

        self.assertEqual(settings["ZABBIX_API_URL"], "http://zabbix.example/api_jsonrpc.php")
        self.assertEqual(settings["ZABBIX_API_TOKEN"], "local-token")
        self.assertEqual(settings["ZABBIX_API_TIMEOUT_SECONDS"], "7")

    def test_environment_settings_take_priority_over_local_secret_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            secret_file = Path(temporary_directory) / "zabbix_api.env"
            secret_file.write_text(
                "ZABBIX_API_URL=http://zabbix.example/api_jsonrpc.php\n"
                "ZABBIX_API_TOKEN=local-token\n",
                encoding="utf-8",
            )
            with patch.object(service, "ZABBIX_SECRET_FILE", secret_file), patch.dict(
                service.os.environ,
                {
                    "ZABBIX_API_URL": "http://override.example/api_jsonrpc.php",
                    "ZABBIX_API_TOKEN": "environment-token",
                },
                clear=False,
            ):
                settings = service._get_zabbix_settings()

        self.assertEqual(settings["ZABBIX_API_URL"], "http://override.example/api_jsonrpc.php")
        self.assertEqual(settings["ZABBIX_API_TOKEN"], "environment-token")

    def test_collective_backup_defaults_include_only_managed_hosts(self):
        defaults = service.get_hosts_backup_defaults(["501", "999"])

        self.assertEqual(
            defaults,
            {
                "501": {
                    "file_path": "/mnt/collective",
                    "extension": ".bin",
                }
            },
        )

    def test_host_configuration_exposes_operational_host_id(self):
        configuration = service.get_configuration("host", "501")

        self.assertEqual(configuration["operational_host_id"], 10482)


if __name__ == "__main__":
    unittest.main()
