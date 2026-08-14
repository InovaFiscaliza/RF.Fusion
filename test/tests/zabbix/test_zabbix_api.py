"""Unit tests for the dependency-free Zabbix configuration client."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


SRC_ROOT = Path("/RFFusion/src")
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from zabbix.zabbix_api import (  # noqa: E402
    API_METHOD_HOST_GET,
    API_METHOD_TEMPLATE_GET,
    API_METHOD_USER_MACRO_CREATE,
    API_METHOD_USER_MACRO_DELETE,
    API_METHOD_USER_MACRO_GET,
    API_METHOD_USER_MACRO_UPDATE,
    MACRO_TYPE_SECRET,
    MACRO_TYPE_TEXT,
    ZabbixApiClient,
)


class FixtureZabbixClient(ZabbixApiClient):
    """Provide a small Zabbix hierarchy without network access."""

    def __init__(self):
        super().__init__("http://zabbix.test/api_jsonrpc.php", "test-token")
        self.calls = []

    def _call(self, method, params):
        self.calls.append((method, params))
        if method == API_METHOD_HOST_GET:
            return [
                {
                    "hostid": "501",
                    "host": "RFEye000501",
                    "name": "RFEye000501",
                    "parentTemplates": [
                        {"templateid": "100", "host": "RFEye_Nodes", "name": "RFEye Nodes"}
                    ],
                }
            ]
        if method == API_METHOD_TEMPLATE_GET:
            requested = set(params["templateids"])
            rows = []
            if "100" in requested:
                rows.append(
                    {
                        "templateid": "100",
                        "host": "RFEye_Nodes",
                        "name": "RFEye Nodes",
                        "parentTemplates": [
                            {"templateid": "90", "host": "appCataloga", "name": "appCataloga"}
                        ],
                    }
                )
            if "90" in requested:
                rows.append(
                    {
                        "templateid": "90",
                        "host": "appCataloga",
                        "name": "appCataloga",
                        "parentTemplates": [],
                    }
                )
            return rows
        if method == API_METHOD_USER_MACRO_GET:
            if "filter" in params:
                return [
                    {"hostmacroid": "1", "value": "2828"},
                    {"hostmacroid": "2", "value": "22"},
                    {"hostmacroid": "4", "value": "/mnt/internal"},
                    {"hostmacroid": "5", "value": "501"},
                ]
            return [
                {
                    "hostmacroid": "1",
                    "hostid": "501",
                    "macro": "{$SSH_PORT}",
                    "type": MACRO_TYPE_TEXT,
                    "description": "Host override",
                },
                {
                    "hostmacroid": "2",
                    "hostid": "100",
                    "macro": "{$SSH_PORT}",
                    "type": MACRO_TYPE_TEXT,
                    "description": "Inherited port",
                },
                {
                    "hostmacroid": "3",
                    "hostid": "100",
                    "macro": "{$SSH_PASSWD}",
                    "type": MACRO_TYPE_SECRET,
                    "description": "Protected password",
                },
                {
                    "hostmacroid": "4",
                    "hostid": "90",
                    "macro": "{$BACKUP_PATH}",
                    "type": MACRO_TYPE_TEXT,
                    "description": "Default path",
                },
                {
                    "hostmacroid": "5",
                    "hostid": "90",
                    "macro": "{$HOST_ID}",
                    "type": MACRO_TYPE_TEXT,
                    "description": "Zabbix host identifier",
                },
            ]
        raise AssertionError(f"Unexpected method: {method}")


class TestZabbixApiClient(unittest.TestCase):
    """Protect macro precedence and secret-value boundaries."""

    def test_host_configuration_prefers_direct_override_and_hides_secret(self):
        client = FixtureZabbixClient()

        configuration = client.get_host_configuration("501")
        macros = {macro["name"]: macro for macro in configuration["macros"]}

        self.assertEqual(configuration["title"], "RFEye000501")
        self.assertEqual(macros["{$SSH_PORT}"]["display_value"], "2828")
        self.assertTrue(macros["{$SSH_PORT}"]["is_direct_on_target"])
        self.assertEqual(macros["{$BACKUP_PATH}"]["display_value"], "/mnt/internal")
        self.assertFalse(macros["{$BACKUP_PATH}"]["is_direct_on_target"])
        self.assertEqual(macros["{$SSH_PASSWD}"]["display_value"], "Valor protegido")
        self.assertEqual(macros["{$SSH_PASSWD}"]["editable_value"], "")
        self.assertTrue(macros["{$HOST_ID}"]["is_protected"])
        self.assertEqual(
            [macro["name"] for macro in configuration["direct_macros"]],
            ["{$SSH_PORT}"],
        )
        self.assertEqual(
            [group["template_name"] for group in configuration["inherited_macro_groups"]],
            ["RFEye Nodes", "appCataloga"],
        )
        self.assertTrue(configuration["inherited_macro_groups"][0]["is_direct_link"])
        self.assertFalse(configuration["inherited_macro_groups"][1]["is_direct_link"])

        text_value_calls = [
            params
            for method, params in client.calls
            if method == API_METHOD_USER_MACRO_GET and "filter" in params
        ]
        self.assertEqual(len(text_value_calls), 1)
        self.assertEqual(text_value_calls[0]["filter"], {"type": MACRO_TYPE_TEXT})

    def test_collective_host_configurations_share_macro_resolution(self):
        client = FixtureZabbixClient()

        configurations = client.get_host_configurations(["501"])
        macros = {
            macro["name"]: macro
            for macro in configurations["501"]["macros"]
        }

        self.assertEqual(configurations["501"]["title"], "RFEye000501")
        self.assertEqual(macros["{$BACKUP_PATH}"]["editable_value"], "/mnt/internal")
        self.assertTrue(macros["{$SSH_PORT}"]["is_direct_on_target"])

        metadata_calls = [
            params
            for method, params in client.calls
            if method == API_METHOD_USER_MACRO_GET and "filter" not in params
        ]
        self.assertEqual(len(metadata_calls), 1)

    def test_write_operations_use_only_single_macro_methods(self):
        client = FixtureZabbixClient()
        recorded_calls = []

        def record_call(method, params):
            recorded_calls.append((method, params))
            return {"hostmacroids": ["99"]}

        client._call = record_call
        client.create_macro(
            owner_id="501",
            macro_name="{$SSH_PORT}",
            macro_type=MACRO_TYPE_TEXT,
            value="2222",
        )
        client.update_macro(macro_id="99", value="2223")
        client.delete_macro(macro_id="99")

        self.assertEqual(recorded_calls[0][0], API_METHOD_USER_MACRO_CREATE)
        self.assertEqual(recorded_calls[0][1]["hostid"], "501")
        self.assertEqual(recorded_calls[1], (API_METHOD_USER_MACRO_UPDATE, {"hostmacroid": "99", "value": "2223"}))
        self.assertEqual(recorded_calls[2], (API_METHOD_USER_MACRO_DELETE, ["99"]))


if __name__ == "__main__":
    unittest.main()
