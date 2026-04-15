"""
Validation tests for `appCataloga.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_appcataloga_entrypoint.py -q
"""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import APP_ROOT, DB_ROOT, bind_real_package, bind_real_shared_package, ensure_app_paths, load_module_from_path


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        appcataloga = load_module_from_path(
            "test_appcataloga_entrypoint_module",
            str(APP_ROOT / "appCataloga.py"),
        )


class FakeLog:
    """Minimal logger double for entrypoint tests."""

    def __init__(self) -> None:
        self.events = []

    def event(self, event_name: str, **fields) -> None:
        self.events.append((event_name, fields))


class FakeDB:
    """Minimal DB double for entrypoint routing tests."""

    def __init__(self) -> None:
        self.host_upserts = []
        self.queued_tasks = []
        self.host_updates = []
        self.host_status = {"status": 0}

    def host_read_status(self, host_id: int):
        return dict(self.host_status)

    def host_upsert(self, **kwargs):
        self.host_upserts.append(kwargs)

    def queue_host_task(self, **kwargs):
        self.queued_tasks.append(kwargs)
        return {
            "HOST_TASK__ID_HOST_TASK": 123,
            "HOST_TASK__NU_TYPE": kwargs["task_type"],
        }

    def host_update(self, **kwargs):
        self.host_updates.append(kwargs)


class AppCatalogaEntrypointTests(unittest.TestCase):
    """Protect query-tag routing without changing the external FILTER contract."""

    def _build_host_payload(self, command: str) -> dict:
        return {
            "command": command,
            "host_id": 77,
            "host_uid": "CWSM211004",
            "host_addr": "10.0.0.10",
            "host_port": 22,
            "user": "celplan",
            "password": "secret",
            "filter": {"mode": "ALL"},
        }

    def test_backup_query_queues_host_check_task(self) -> None:
        fake_db = FakeDB()
        fake_log = FakeLog()
        err = appcataloga.errors.ErrorHandler(fake_log)

        with patch.object(appcataloga, "log", fake_log):
            _, response = appcataloga.handle_host_request(
                self._build_host_payload("backup"),
                err,
                fake_db,
            )

        self.assertEqual(fake_db.queued_tasks[0]["task_type"], appcataloga.k.HOST_TASK_CHECK_TYPE)
        self.assertEqual(response["status"], 1)
        self.assertFalse(fake_db.host_upserts[0]["IS_OFFLINE"])

    def test_stop_query_queues_backlog_rollback_task(self) -> None:
        fake_db = FakeDB()
        fake_log = FakeLog()
        err = appcataloga.errors.ErrorHandler(fake_log)

        with patch.object(appcataloga, "log", fake_log):
            _, response = appcataloga.handle_host_request(
                self._build_host_payload("STOP"),
                err,
                fake_db,
            )

        self.assertEqual(
            fake_db.queued_tasks[0]["task_type"],
            appcataloga.k.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
        )
        self.assertEqual(response["status"], 1)

    def test_backup_query_rejects_known_offline_host(self) -> None:
        fake_db = FakeDB()
        fake_db.host_status = {"status": 1, "IS_OFFLINE": True}
        fake_log = FakeLog()
        err = appcataloga.errors.ErrorHandler(fake_log)

        with patch.object(appcataloga, "log", fake_log):
            with self.assertRaises(ValueError):
                appcataloga.handle_host_request(
                    self._build_host_payload("backup"),
                    err,
                    fake_db,
                )

        self.assertEqual(fake_db.host_upserts, [])
        self.assertEqual(fake_db.queued_tasks, [])

    def test_stop_query_allows_known_offline_host(self) -> None:
        fake_db = FakeDB()
        fake_db.host_status = {"status": 1, "IS_OFFLINE": True}
        fake_log = FakeLog()
        err = appcataloga.errors.ErrorHandler(fake_log)

        with patch.object(appcataloga, "log", fake_log):
            _, response = appcataloga.handle_host_request(
                self._build_host_payload("stop"),
                err,
                fake_db,
            )

        self.assertEqual(
            fake_db.queued_tasks[0]["task_type"],
            appcataloga.k.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
        )
        self.assertNotIn("IS_OFFLINE", fake_db.host_upserts[0])
        self.assertEqual(response["status"], 1)


if __name__ == "__main__":
    unittest.main()
