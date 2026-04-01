"""
Validation tests for `appCataloga_backlog_management.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_backlog_management_worker.py -q
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
        backlog_worker = load_module_from_path(
            "test_backlog_management_worker_module",
            str(APP_ROOT / "appCataloga_backlog_management.py"),
        )


class FakeLog:
    """Small logger double for backlog worker tests."""

    def __init__(self) -> None:
        self.events = []

    def event(self, event_name: str, **fields) -> None:
        self.events.append((event_name, fields))

    def warning(self, message: str) -> None:
        self.events.append(("warning", {"message": message}))

    def error(self, message: str) -> None:
        self.events.append(("error", {"message": message}))


class FakeDB:
    """Minimal persistence double for backlog-control tests."""

    def __init__(self) -> None:
        self.update_calls = []
        self.host_task_updates = []
        self.statistics_calls = []

    def update_backlog_by_filter(self, **kwargs):
        self.update_calls.append(kwargs)
        if kwargs["new_type"] == backlog_worker.k.FILE_TASK_BACKUP_TYPE:
            return {
                "rows_updated": 3,
                "moved_to_backup": 3,
                "moved_to_discovery": 0,
            }
        return {
            "rows_updated": 2,
            "moved_to_backup": 0,
            "moved_to_discovery": 2,
        }

    def host_task_update(self, **kwargs):
        self.host_task_updates.append(kwargs)
        return {"success": True, "rows_affected": 1, "updated_fields": kwargs}

    def host_task_statistics_create(self, host_id):
        self.statistics_calls.append(host_id)


class BacklogManagementWorkerTests(unittest.TestCase):
    """Protect promotion and rollback semantics for backlog-control tasks."""

    def test_promote_task_moves_discovery_done_to_backup_pending(self) -> None:
        fake_db = FakeDB()
        fake_log = FakeLog()

        task = {
            "host_id": 33,
            "task_id": 44,
            "task_type": backlog_worker.k.HOST_TASK_BACKLOG_CONTROL_TYPE,
            "host_filter": {"mode": "ALL"},
            "now": backlog_worker.datetime.now(),
        }

        with patch.object(backlog_worker, "log", fake_log):
            outcome = backlog_worker._apply_backlog_task(fake_db, task)
            backlog_worker._finalize_backlog_success(fake_db, task=task, outcome=outcome)

        self.assertEqual(outcome["action"], "promote")
        self.assertEqual(fake_db.update_calls[0]["search_type"], backlog_worker.k.FILE_TASK_DISCOVERY)
        self.assertEqual(fake_db.update_calls[0]["search_status"], backlog_worker.k.TASK_DONE)
        self.assertEqual(fake_db.update_calls[0]["new_type"], backlog_worker.k.FILE_TASK_BACKUP_TYPE)
        self.assertEqual(fake_db.update_calls[0]["new_status"], backlog_worker.k.TASK_PENDING)
        self.assertEqual(fake_db.statistics_calls, [33])
        self.assertEqual(fake_db.host_task_updates[-1]["NU_STATUS"], backlog_worker.k.TASK_DONE)

    def test_rollback_task_cancels_pending_promotion_and_returns_backup_queue(self) -> None:
        fake_db = FakeDB()
        fake_log = FakeLog()

        task = {
            "host_id": 55,
            "task_id": 66,
            "task_type": backlog_worker.k.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
            "host_filter": {"mode": "RANGE", "start_date": "2025-01-01", "end_date": "2025-12-31"},
            "now": backlog_worker.datetime.now(),
        }

        with patch.object(backlog_worker, "log", fake_log):
            outcome = backlog_worker._apply_backlog_task(fake_db, task)

        self.assertEqual(outcome["action"], "rollback")
        self.assertEqual(len(fake_db.host_task_updates), 1)
        self.assertEqual(
            fake_db.host_task_updates[0]["where_dict"]["NU_TYPE"],
            backlog_worker.k.HOST_TASK_BACKLOG_CONTROL_TYPE,
        )
        self.assertEqual(
            fake_db.update_calls[0]["search_type"],
            backlog_worker.k.FILE_TASK_BACKUP_TYPE,
        )
        self.assertEqual(
            fake_db.update_calls[0]["search_status"],
            backlog_worker.k.TASK_PENDING,
        )
        self.assertEqual(
            fake_db.update_calls[0]["new_type"],
            backlog_worker.k.FILE_TASK_DISCOVERY,
        )
        self.assertEqual(fake_db.statistics_calls, [55])


if __name__ == "__main__":
    unittest.main()
