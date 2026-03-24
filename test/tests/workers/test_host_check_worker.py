"""
Validation tests for host task and maintenance workers.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_host_check_worker.py -q

What is covered here:
    - operational connectivity returns tri-state details instead of a bool-only answer
    - ping-only hosts become degraded, not fully online
    - ICMP-only background sweep does not resurrect offline hosts by itself
"""

from __future__ import annotations

import socket
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
import sys

import paramiko

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import APP_ROOT, DB_ROOT, bind_real_package, bind_real_shared_package, ensure_app_paths, load_module_from_path


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        host_check_worker = load_module_from_path(
            "test_host_check_worker_module",
            str(APP_ROOT / "appCataloga_host_check.py"),
        )
        host_maintenance_worker = load_module_from_path(
            "test_host_maintenance_worker_module",
            str(APP_ROOT / "appCataloga_host_maintenance.py"),
        )


class FakeLog:
    def __init__(self) -> None:
        self.events = []

    def event(self, event_name: str, **fields) -> None:
        self.events.append((event_name, fields))

    def error(self, message: str) -> None:
        self.events.append(("error", {"message": message}))


class FakeSSHClient:
    def __init__(self, connect_side_effect=None) -> None:
        self.connect_side_effect = connect_side_effect
        self.connect_calls = []
        self.closed = False

    def set_missing_host_key_policy(self, _policy) -> None:
        pass

    def connect(self, **kwargs) -> None:
        self.connect_calls.append(kwargs)
        if self.connect_side_effect is not None:
            raise self.connect_side_effect

    def close(self) -> None:
        self.closed = True


class FakeDB:
    def __init__(self, hosts) -> None:
        self._hosts = hosts
        self.host_updates = []
        self.host_task_updates = []
        self.queued_tasks = []
        self.resumed_hosts = []
        self.suspended_hosts = []

    def host_list_for_connectivity_check(self):
        return list(self._hosts)

    def host_update(self, **kwargs):
        self.host_updates.append(kwargs)

    def host_task_update(self, **kwargs):
        self.host_task_updates.append(kwargs)

    def queue_host_task(self, **kwargs):
        self.queued_tasks.append(kwargs)

    def host_task_resume_by_host(self, host_id):
        self.resumed_hosts.append(("host_task", host_id))

    def file_task_resume_by_host(self, host_id):
        self.resumed_hosts.append(("file_task", host_id))

    def file_history_resume_by_host(self, host_id):
        self.resumed_hosts.append(("file_history", host_id))

    def host_task_suspend_by_host(self, host_id):
        self.suspended_hosts.append(("host_task", host_id))

    def file_task_suspend_by_host(self, host_id):
        self.suspended_hosts.append(("file_task", host_id))

    def file_history_suspend_by_host(self, host_id):
        self.suspended_hosts.append(("file_history", host_id))


class HostConnectivityTests(unittest.TestCase):
    def test_check_host_connectivity_returns_online_when_icmp_and_ssh_succeed(self) -> None:
        fake_log = FakeLog()
        ssh_client = FakeSSHClient()

        with patch.object(host_check_worker, "log", fake_log):
            with patch.object(
                host_check_worker.host_connectivity,
                "resolve_host_addresses",
                return_value=["172.24.1.11"],
            ):
                with patch.object(
                    host_check_worker.host_connectivity,
                    "_ping_address",
                    return_value=True,
                ):
                    with patch.object(
                        host_check_worker.host_connectivity.paramiko,
                        "SSHClient",
                        return_value=ssh_client,
                    ):
                        connectivity = host_check_worker.check_host_connectivity(
                            host_id=101,
                            addr="172.24.1.11",
                            port=22,
                            user="root",
                            password="secret",
                            event_name="host_check_connection",
                        )

        self.assertEqual(connectivity["state"], "online")
        self.assertTrue(connectivity["icmp_online"])
        self.assertTrue(connectivity["ssh_online"])
        self.assertIsNone(connectivity["error"])
        self.assertEqual(len(ssh_client.connect_calls), 1)
        self.assertEqual(len(fake_log.events), 1)
        _, payload = fake_log.events[0]
        self.assertTrue(payload["online"])

    def test_check_host_connectivity_marks_ping_only_timeout_as_degraded(self) -> None:
        fake_log = FakeLog()
        ssh_client = FakeSSHClient(connect_side_effect=socket.timeout("timed out"))

        with patch.object(host_check_worker, "log", fake_log):
            with patch.object(
                host_check_worker.host_connectivity,
                "resolve_host_addresses",
                return_value=["172.24.1.12"],
            ):
                with patch.object(
                    host_check_worker.host_connectivity,
                    "_ping_address",
                    return_value=True,
                ):
                    with patch.object(
                        host_check_worker.host_connectivity.paramiko,
                        "SSHClient",
                        return_value=ssh_client,
                    ):
                        connectivity = host_check_worker.check_host_connectivity(
                            host_id=202,
                            addr="172.24.1.12",
                            port=22,
                            user="root",
                            password="secret",
                            event_name="host_check_connection",
                        )

        self.assertEqual(connectivity["state"], "degraded")
        self.assertTrue(connectivity["icmp_online"])
        self.assertFalse(connectivity["ssh_online"])
        self.assertEqual(len(ssh_client.connect_calls), 1)
        self.assertEqual(len(fake_log.events), 1)
        _, payload = fake_log.events[0]
        self.assertFalse(payload["online"])
        self.assertEqual(payload["state"], "degraded")

    def test_check_host_connectivity_marks_connection_refused_as_degraded(self) -> None:
        fake_log = FakeLog()
        exc = paramiko.ssh_exception.NoValidConnectionsError(
            {("172.24.1.12", 22): ConnectionRefusedError(111, "refused")}
        )
        ssh_client = FakeSSHClient(connect_side_effect=exc)

        with patch.object(host_check_worker, "log", fake_log):
            with patch.object(
                host_check_worker.host_connectivity,
                "resolve_host_addresses",
                return_value=["172.24.1.12"],
            ):
                with patch.object(
                    host_check_worker.host_connectivity,
                    "_ping_address",
                    return_value=True,
                ):
                    with patch.object(
                        host_check_worker.host_connectivity.paramiko,
                        "SSHClient",
                        return_value=ssh_client,
                    ):
                        connectivity = host_check_worker.check_host_connectivity(
                            host_id=203,
                            addr="172.24.1.12",
                            port=22,
                            user="root",
                            password="secret",
                            event_name="host_check_connection",
                        )

        self.assertEqual(connectivity["state"], "degraded")
        self.assertTrue(connectivity["icmp_online"])
        self.assertFalse(connectivity["ssh_online"])

    def test_check_host_connectivity_marks_auth_timeout_as_degraded(self) -> None:
        fake_log = FakeLog()
        ssh_client = FakeSSHClient(
            connect_side_effect=paramiko.AuthenticationException("Authentication timeout.")
        )

        with patch.object(host_check_worker, "log", fake_log):
            with patch.object(
                host_check_worker.host_connectivity,
                "resolve_host_addresses",
                return_value=["172.24.1.14"],
            ):
                with patch.object(
                    host_check_worker.host_connectivity,
                    "_ping_address",
                    return_value=True,
                ):
                    with patch.object(
                        host_check_worker.host_connectivity.paramiko,
                        "SSHClient",
                        return_value=ssh_client,
                    ):
                        connectivity = host_check_worker.check_host_connectivity(
                            host_id=204,
                            addr="172.24.1.14",
                            port=22,
                            user="root",
                            password="secret",
                            event_name="host_check_connection",
                        )

        self.assertEqual(connectivity["state"], "degraded")
        self.assertEqual(connectivity["reason"], "ssh_auth_timeout")
        self.assertTrue(connectivity["icmp_online"])
        self.assertTrue(connectivity["ssh_online"])

    def test_handle_degraded_connectivity_task_preserves_host_state(self) -> None:
        now = datetime(2026, 3, 23, 12, 0, 0)
        db = FakeDB(hosts=[])

        host_check_worker.handle_degraded_connectivity_task(
            db=db,
            task_id=17,
            host_id=203,
            current_error_count=2,
            now=now,
        )

        self.assertEqual(
            db.host_updates,
            [
                {
                    "host_id": 203,
                    "DT_LAST_CHECK": now,
                    "DT_LAST_FAIL": now,
                    "NU_HOST_CHECK_ERROR": 3,
                }
            ],
        )
        self.assertEqual(
            db.host_task_updates,
            [
                {
                    "task_id": 17,
                    "NU_STATUS": host_check_worker.k.TASK_ERROR,
                    "DT_HOST_TASK": now,
                    "NA_MESSAGE": (
                        "SSH supervision degraded threshold reached while ICMP still "
                        "responds (3/3)"
                    ),
                }
            ],
        )

    def test_check_host_connectivity_skips_ssh_probe_when_ping_is_down(self) -> None:
        fake_log = FakeLog()
        ssh_client = FakeSSHClient()

        with patch.object(host_check_worker, "log", fake_log):
            with patch.object(
                host_check_worker.host_connectivity,
                "resolve_host_addresses",
                return_value=["172.24.1.13"],
            ):
                with patch.object(
                    host_check_worker.host_connectivity,
                    "_ping_address",
                    return_value=False,
                ):
                    with patch.object(
                        host_check_worker.host_connectivity.paramiko,
                        "SSHClient",
                        return_value=ssh_client,
                    ):
                        connectivity = host_check_worker.check_host_connectivity(
                            host_id=303,
                            addr="172.24.1.13",
                            port=22,
                            user="root",
                            password="secret",
                            event_name="host_check",
                        )

        self.assertEqual(connectivity["state"], "offline")
        self.assertFalse(connectivity["icmp_online"])
        self.assertFalse(connectivity["ssh_online"])
        self.assertEqual(ssh_client.connect_calls, [])
        self.assertEqual(len(fake_log.events), 1)
        _, payload = fake_log.events[0]
        self.assertFalse(payload["online"])

    def test_check_host_connectivity_uses_second_resolved_ip_when_first_one_is_down(self) -> None:
        fake_log = FakeLog()
        ssh_client = FakeSSHClient()

        with patch.object(host_check_worker, "log", fake_log):
            with patch.object(
                host_check_worker.host_connectivity,
                "resolve_host_addresses",
                return_value=["172.24.1.147"],
            ):
                with patch.object(
                    host_check_worker.host_connectivity,
                    "_ping_address",
                    return_value=True,
                ):
                    with patch.object(
                        host_check_worker.host_connectivity.paramiko,
                        "SSHClient",
                        return_value=ssh_client,
                    ):
                        connectivity = host_check_worker.check_host_connectivity(
                            host_id=304,
                            addr="rfeye002158.anatel.gov.br",
                            port=2828,
                            user="root",
                            password="secret",
                            event_name="host_check_connection",
                        )

        self.assertEqual(connectivity["state"], "online")
        self.assertEqual(connectivity["resolved_addr"], "172.24.1.147")
        self.assertEqual(
            connectivity["resolved_candidates"],
            ["172.24.1.147"],
        )
        self.assertEqual(len(ssh_client.connect_calls), 1)
        self.assertEqual(ssh_client.connect_calls[0]["hostname"], "172.24.1.147")

    def test_resolve_host_addresses_prefers_172_network_when_available(self) -> None:
        with patch.object(
            host_check_worker.host_connectivity.socket,
            "getaddrinfo",
            return_value=[
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.82.10.123", 0)),
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("172.24.1.147", 0)),
            ],
        ):
            addresses = host_check_worker.host_connectivity.resolve_host_addresses(
                "rfeye002158.anatel.gov.br"
            )

        self.assertEqual(addresses, ["172.24.1.147"])

class HostMaintenanceTests(unittest.TestCase):
    def test_run_host_check_all_batch_recovers_offline_host_only_after_ssh_probe(self) -> None:
        now = datetime(2026, 3, 23, 12, 0, 0)
        db = FakeDB(
            hosts=[
                {
                    "ID_HOST": 77,
                    "NA_HOST_NAME": "station-a",
                    "NA_HOST_ADDRESS": "172.24.1.77",
                    "NA_HOST_PORT": 22,
                    "NA_HOST_USER": "root",
                    "NA_HOST_PASSWORD": "secret",
                    "IS_OFFLINE": True,
                    "DT_LAST_CHECK": now - timedelta(hours=1),
                }
            ]
        )

        with patch.object(
            host_maintenance_worker.host_connectivity,
            "is_host_online",
            return_value=True,
        ):
            with patch.object(
                host_maintenance_worker.host_connectivity,
                "probe_host_operational_connectivity",
                return_value={
                    "state": "online",
                    "reason": "ssh_connect_ok",
                    "icmp_online": True,
                    "ssh_online": True,
                    "error": None,
                },
            ) as probe_host_operational_connectivity:
                checked = host_maintenance_worker.run_host_check_all_batch(db=db, now=now)

        self.assertEqual(checked, 1)
        self.assertEqual(
            db.host_updates,
            [{
                "host_id": 77,
                "IS_OFFLINE": False,
                "check_busy_timeout": True,
                "DT_LAST_CHECK": now,
                "NU_HOST_CHECK_ERROR": 0,
            }],
        )
        self.assertEqual(db.queued_tasks, [])
        self.assertEqual(
            db.resumed_hosts,
            [
                ("host_task", 77),
                ("file_task", 77),
                ("file_history", 77),
            ],
        )
        probe_host_operational_connectivity.assert_called_once()

    def test_run_host_check_all_batch_keeps_offline_host_suspended_when_ssh_is_still_degraded(self) -> None:
        now = datetime(2026, 3, 23, 12, 0, 0)
        db = FakeDB(
            hosts=[
                {
                    "ID_HOST": 78,
                    "NA_HOST_NAME": "station-a-degraded",
                    "NA_HOST_ADDRESS": "172.24.1.78",
                    "NA_HOST_PORT": 22,
                    "NA_HOST_USER": "root",
                    "NA_HOST_PASSWORD": "secret",
                    "IS_OFFLINE": True,
                    "DT_LAST_CHECK": now - timedelta(hours=1),
                }
            ]
        )

        with patch.object(
            host_maintenance_worker.host_connectivity,
            "is_host_online",
            return_value=True,
        ):
            with patch.object(
                host_maintenance_worker.host_connectivity,
                "probe_host_operational_connectivity",
                return_value={
                    "state": "degraded",
                    "reason": "ssh_timeout",
                    "icmp_online": True,
                    "ssh_online": False,
                    "error": "timed out",
                },
            ) as probe_host_operational_connectivity:
                checked = host_maintenance_worker.run_host_check_all_batch(db=db, now=now)

        self.assertEqual(checked, 1)
        self.assertEqual(
            db.host_updates,
            [{
                "host_id": 78,
                "IS_OFFLINE": True,
                "DT_LAST_CHECK": now,
                "DT_LAST_FAIL": now,
            }],
        )
        self.assertEqual(db.resumed_hosts, [])
        probe_host_operational_connectivity.assert_called_once()

    def test_run_host_check_all_batch_does_not_clear_timeout_counter_on_ping_only_success(self) -> None:
        now = datetime(2026, 3, 23, 12, 0, 0)
        db = FakeDB(
            hosts=[
                {
                    "ID_HOST": 88,
                    "NA_HOST_NAME": "station-b",
                    "NA_HOST_ADDRESS": "172.24.1.88",
                    "NA_HOST_PORT": 22,
                    "NA_HOST_USER": "root",
                    "NA_HOST_PASSWORD": "secret",
                    "IS_BUSY": False,
                    "IS_OFFLINE": False,
                    "DT_LAST_CHECK": now - timedelta(hours=1),
                }
            ]
        )

        with patch.object(
            host_maintenance_worker.host_connectivity,
            "is_host_online",
            return_value=True,
        ):
            checked = host_maintenance_worker.run_host_check_all_batch(db=db, now=now)

        self.assertEqual(checked, 1)
        self.assertEqual(
            db.host_updates,
            [{"host_id": 88, "DT_LAST_CHECK": now}],
        )
        self.assertEqual(db.queued_tasks, [])

    def test_run_host_check_all_batch_skips_busy_host(self) -> None:
        now = datetime(2026, 3, 23, 12, 0, 0)
        db = FakeDB(
            hosts=[
                {
                    "ID_HOST": 99,
                    "NA_HOST_NAME": "station-c",
                    "NA_HOST_ADDRESS": "172.24.1.99",
                    "NA_HOST_PORT": 22,
                    "NA_HOST_USER": "root",
                    "NA_HOST_PASSWORD": "secret",
                    "IS_BUSY": True,
                    "IS_OFFLINE": False,
                    "DT_LAST_CHECK": now - timedelta(hours=1),
                }
            ]
        )

        with patch.object(
            host_maintenance_worker.host_connectivity,
            "is_host_online",
        ) as is_host_online:
            checked = host_maintenance_worker.run_host_check_all_batch(db=db, now=now)

        self.assertEqual(checked, 0)
        is_host_online.assert_not_called()
        self.assertEqual(db.host_updates, [])
        self.assertEqual(db.queued_tasks, [])


if __name__ == "__main__":
    unittest.main()
