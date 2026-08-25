"""Unit tests for the database-independent container health collector."""

import importlib
import sys
import unittest
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from unittest.mock import Mock, patch


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


class TestRuntimeHealth(unittest.TestCase):
    """Keep live health checks bounded and independent from database services."""

    @classmethod
    def setUpClass(cls):
        root = str(WEBFUSION_ROOT)
        if root not in sys.path:
            sys.path.insert(0, root)

        sys.modules.pop("modules.server.runtime_health", None)
        cls.health = importlib.import_module("modules.server.runtime_health")

    def setUp(self):
        with self.health._SNAPSHOT_CACHE_LOCK:
            self.health._SNAPSHOT_CACHE["payload"] = None
            self.health._SNAPSHOT_CACHE["expires_at"] = 0.0

    def test_remote_component_uses_key_only_strict_ssh(self):
        completed = SimpleNamespace(
            returncode=0,
            stdout='{"status":"healthy","checks":[]}',
            stderr="",
        )

        with patch.object(self.health.os.path, "isfile", return_value=True), patch.object(
            self.health.subprocess,
            "run",
            return_value=completed,
        ) as run:
            payload = self.health._run_remote_component(
                component="appcataloga",
                label="appCataloga",
                host="10.88.0.2",
                port=22,
                remote_script="/RFFusion/health.sh",
                key_path="/run/secrets/key",
                known_hosts_path="/run/secrets/known_hosts",
                timeout_seconds=3,
            )

        command = run.call_args.args[0]
        self.assertEqual(payload["status"], "healthy")
        self.assertIn("BatchMode=yes", command)
        self.assertIn("PasswordAuthentication=no", command)
        self.assertIn("StrictHostKeyChecking=yes", command)
        self.assertIn("root@10.88.0.2", command)

    def test_missing_ssh_key_does_not_start_remote_process(self):
        with patch.object(self.health.os.path, "isfile", return_value=False), patch.object(
            self.health.subprocess,
            "run",
        ) as run:
            payload = self.health._run_remote_component(
                component="mariadb",
                label="MariaDB",
                host="10.88.0.33",
                port=2828,
                remote_script="/RFFusion/health.sh",
                key_path="/run/secrets/key",
                known_hosts_path="/run/secrets/known_hosts",
                timeout_seconds=3,
            )

        self.assertEqual(payload["status"], "unconfigured")
        self.assertIn("Chave", payload["message"])
        run.assert_not_called()

    def test_overall_status_does_not_reuse_previous_state(self):
        status = self.health._overall_status(
            [
                {"status": "healthy"},
                {"status": "unavailable"},
            ]
        )

        self.assertEqual(status, "degraded")

    def test_snapshot_cache_reuses_one_collector_result(self):
        snapshot = {
            "checked_at": "2026-08-20T12:00:00Z",
            "status": "healthy",
            "components": [],
        }

        with patch.object(
            self.health,
            "_collect_runtime_health_snapshot",
            return_value=snapshot,
        ) as collect:
            first_payload = self.health.get_runtime_health_snapshot()
            second_payload = self.health.get_runtime_health_snapshot()

        self.assertEqual(collect.call_count, 1)
        self.assertEqual(first_payload, snapshot)
        self.assertEqual(second_payload, snapshot)
        self.assertIsNot(first_payload, second_payload)

    def test_concurrent_requests_share_one_health_collection(self):
        collection_started = Event()
        release_collection = Event()
        snapshot = {
            "checked_at": "2026-08-20T12:00:00Z",
            "status": "healthy",
            "components": [],
        }

        def collect_snapshot():
            collection_started.set()
            release_collection.wait(timeout=1)
            return snapshot

        collector = Mock(side_effect=collect_snapshot)
        payloads = []

        with patch.object(
            self.health,
            "_collect_runtime_health_snapshot",
            collector,
        ):
            first_request = Thread(
                target=lambda: payloads.append(
                    self.health.get_runtime_health_snapshot()
                )
            )
            second_request = Thread(
                target=lambda: payloads.append(
                    self.health.get_runtime_health_snapshot()
                )
            )
            first_request.start()
            self.assertTrue(collection_started.wait(timeout=1))
            second_request.start()
            release_collection.set()
            first_request.join(timeout=1)
            second_request.join(timeout=1)

        self.assertFalse(first_request.is_alive())
        self.assertFalse(second_request.is_alive())
        self.assertEqual(collector.call_count, 1)
        self.assertEqual(payloads, [snapshot, snapshot])

    def test_refresh_in_progress_returns_the_previous_snapshot(self):
        stale_snapshot = {
            "checked_at": "2026-08-20T11:59:00Z",
            "status": "degraded",
            "components": [],
        }
        refreshed_snapshot = {
            "checked_at": "2026-08-20T12:00:00Z",
            "status": "healthy",
            "components": [],
        }
        collection_started = Event()
        release_collection = Event()

        def collect_snapshot():
            collection_started.set()
            release_collection.wait(timeout=1)
            return refreshed_snapshot

        with self.health._SNAPSHOT_CACHE_LOCK:
            self.health._SNAPSHOT_CACHE["payload"] = stale_snapshot

        with patch.object(
            self.health,
            "_collect_runtime_health_snapshot",
            side_effect=collect_snapshot,
        ) as collect:
            refresh_request = Thread(target=self.health.get_runtime_health_snapshot)
            refresh_request.start()
            self.assertTrue(collection_started.wait(timeout=1))

            payload = self.health.get_runtime_health_snapshot()
            release_collection.set()
            refresh_request.join(timeout=1)

        self.assertFalse(refresh_request.is_alive())
        self.assertEqual(collect.call_count, 1)
        self.assertEqual(payload, stale_snapshot)
