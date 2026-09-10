"""Unit tests for the read-only appCataloga alarms service."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


SRC_ROOT = Path("/RFFusion/src")
WEBFUSION_ROOT = SRC_ROOT / "webfusion"
for root in (str(SRC_ROOT), str(WEBFUSION_ROOT)):
    if root not in sys.path:
        sys.path.insert(0, root)

from modules.alarms import service  # noqa: E402
from zabbix_api.client import ZabbixApiClient  # noqa: E402


class TestAlarmsService(unittest.TestCase):
    """Keep the operator alarm view bounded to the appCataloga context."""

    def test_list_alarms_formats_problem_and_zabbix_link(self):
        problems = [
            {
                "eventid": "12001",
                "objectid": "8001",
                "name": "Estação sem descobrir novos arquivos há mais de 2d",
                "clock": "1780272981",
                "severity": "3",
                "acknowledged": "0",
                "suppressed": "0",
                "hosts": [{"name": "UMSSP04", "host": "UMSSP04"}],
                "tags": [
                    {"tag": "Contex", "value": "appCataloga"},
                    {"tag": "Application", "value": "Spectrum"},
                ],
            }
        ]
        with patch.object(service, "get_zabbix_api_url", return_value="https://zabbix.example/zabbix/api_jsonrpc.php"), patch.object(service, "get_appcataloga_problems", return_value=problems):
            alarms = service.list_alarms()

        self.assertEqual(alarms[0]["severity_label"], "Média")
        self.assertEqual(alarms[0]["hosts"], "UMSSP04")
        self.assertFalse(alarms[0]["acknowledged"])
        self.assertEqual(
            alarms[0]["zabbix_url"],
            "https://zabbix.example/zabbix/tr_events.php?triggerid=8001&eventid=12001",
        )

    def test_get_zabbix_problems_url_opens_monitoring_problems(self):
        with patch.object(
            service,
            "get_zabbix_api_url",
            return_value="https://zabbix.example/zabbix/api_jsonrpc.php",
        ):
            result = service.get_zabbix_problems_url()

        self.assertEqual(
            result,
            "https://zabbix.example/zabbix/zabbix.php?action=problem.view",
        )

    def test_client_uses_context_contains_filter_for_appcataloga(self):
        client = object.__new__(ZabbixApiClient)
        with patch.object(client, "_call", return_value=[]) as call:
            client.list_appcataloga_problems()

        method, params = call.call_args.args
        self.assertEqual(method, "problem.get")
        self.assertEqual(
            params["tags"],
            [{"tag": "Contex", "value": "appCataloga", "operator": 0}],
        )
        self.assertEqual(params["selectHosts"], ["hostid", "host", "name"])
        self.assertEqual(params["limit"], 100)

    def test_client_resolves_problem_host_with_permitted_host_get(self):
        client = object.__new__(ZabbixApiClient)
        problems = [{"eventid": "12001", "objectid": "8001"}]
        hosts = [{"hostid": "501", "host": "UMSSP04", "name": "UMSSP04"}]
        with patch.object(client, "_call", side_effect=[problems, hosts]) as call:
            result = client.list_appcataloga_problems()

        self.assertEqual(result[0]["hosts"], hosts)
        self.assertEqual(call.call_args_list[1].args[0], "host.get")
        self.assertEqual(call.call_args_list[1].args[1]["triggerids"], ["8001"])



if __name__ == "__main__":
    unittest.main()