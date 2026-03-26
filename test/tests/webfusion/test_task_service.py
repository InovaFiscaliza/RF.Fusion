"""
Validation tests for `webfusion.modules.task.service`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_task_service.py -q

What is covered here:
    - safe reuse of durable `HOST_TASK` rows
    - refusal to expose internal-only task types through the UI service layer
"""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


def load_task_service():
    """Reload the task service so module-level constants stay fresh in tests."""
    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

    sys.modules.pop("modules.task.service", None)
    return importlib.import_module("modules.task.service")


class FakeCursor:
    """Very small cursor double that replays pre-seeded SELECT results."""

    def __init__(self, db):
        self.db = db
        self._last_result = []

    def execute(self, sql, params=None):
        compact_sql = " ".join(sql.split())
        self.db.executions.append((compact_sql, params))

        if compact_sql.startswith("SELECT"):
            if self.db.select_results:
                self._last_result = self.db.select_results.pop(0)
            else:
                self._last_result = []
        else:
            self._last_result = []

    def fetchall(self):
        return list(self._last_result)


class FakeDB:
    """DB double that records SQL shape and commit behavior."""

    def __init__(self, select_results=None):
        self.select_results = list(select_results or [])
        self.executions = []
        self.commit_calls = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commit_calls += 1


class TestTaskService(unittest.TestCase):
    """Protect the queue contract mirrored from `appCataloga`."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_task_service()

    def test_check_task_refreshes_existing_check_row(self):
        db = FakeDB(
            select_results=[
                [
                    {
                        "ID_HOST_TASK": 77,
                        "NU_TYPE": self.module.HOST_TASK_CHECK_TYPE,
                        "NU_STATUS": self.module.TASK_PENDING,
                        "FILTER": '{"mode":"NONE","file_path":"/mnt/internal/data","agent":"local"}',
                    }
                ]
            ]
        )
        filter_dict = {
            "mode": "LAST_N_FILES",
            "start_date": None,
            "end_date": None,
            "last_n_files": "25",
            "extension": ".bin",
            "file_path": "/mnt/internal/data",
            "file_name": None,
            "agent": "local",
        }

        result = self.module.queue_host_task_safe(
            db=db,
            host_id=1001,
            task_type=self.module.HOST_TASK_CHECK_TYPE,
            filter_dict=filter_dict,
            message="Created by WebFusion | Host Check | Backup (LAST_N_FILES) | Individual",
        )

        self.assertEqual(result, "refreshed")
        self.assertEqual(db.commit_calls, 1)
        self.assertEqual(len(db.executions), 2)
        self.assertIn("NU_TYPE = %s", db.executions[0][0])
        self.assertNotIn("NU_TYPE IN", db.executions[0][0])

        update_sql, update_params = db.executions[1]
        self.assertTrue(update_sql.startswith("UPDATE HOST_TASK"))
        self.assertEqual(update_params[0], self.module.HOST_TASK_CHECK_TYPE)
        self.assertEqual(update_params[1], self.module.TASK_PENDING)
        self.assertEqual(update_params[-1], 77)
        self.assertIn('"last_n_files": "25"', update_params[2])

    def test_running_check_task_is_preserved(self):
        db = FakeDB(
            select_results=[
                [
                    {
                        "ID_HOST_TASK": 88,
                        "NU_TYPE": self.module.HOST_TASK_CHECK_TYPE,
                        "NU_STATUS": self.module.TASK_RUNNING,
                        "FILTER": '{"mode":"NONE","file_path":"/mnt/internal/data","agent":"local"}',
                    }
                ]
            ]
        )

        result = self.module.queue_host_task_safe(
            db=db,
            host_id=1002,
            task_type=self.module.HOST_TASK_CHECK_TYPE,
            filter_dict={
                "mode": "NONE",
                "start_date": None,
                "end_date": None,
                "last_n_files": None,
                "extension": None,
                "file_path": "/mnt/internal/data",
                "file_name": None,
                "agent": "local",
            },
            message="Created by WebFusion | Host Check | Backup (NONE) | Individual",
        )

        self.assertEqual(result, "skipped_active")
        self.assertEqual(db.commit_calls, 0)
        self.assertEqual(len(db.executions), 1)

    def test_processing_row_is_not_reused_for_check_task(self):
        db = FakeDB(
            select_results=[
                []
            ]
        )

        result = self.module.queue_host_task_safe(
            db=db,
            host_id=1003,
            task_type=self.module.HOST_TASK_CHECK_TYPE,
            filter_dict={
                "mode": "NONE",
                "start_date": None,
                "end_date": None,
                "last_n_files": None,
                "extension": None,
                "file_path": "/mnt/internal/data",
                "file_name": None,
                "agent": "local",
            },
            message="Created by WebFusion | Host Check | Backup (NONE) | Individual",
        )

        self.assertEqual(result, "created")
        self.assertEqual(db.commit_calls, 1)
        self.assertEqual(len(db.executions), 2)
        self.assertIn("NU_TYPE = %s", db.executions[0][0])
        insert_sql, insert_params = db.executions[1]
        self.assertTrue(insert_sql.startswith("INSERT INTO HOST_TASK"))
        self.assertEqual(insert_params[1], self.module.HOST_TASK_CHECK_TYPE)

    def test_create_task_only_accepts_conventional_check_type(self):
        with self.assertRaises(ValueError):
            self.module.create_task(
                db=FakeDB(),
                hosts=[1],
                task_type=self.module.HOST_TASK_UPDATE_STATISTICS_TYPE,
                mode="NONE",
                filter_data={},
            )

        with self.assertRaises(ValueError):
            self.module.create_task(
                db=FakeDB(),
                hosts=[1],
                task_type=self.module.HOST_TASK_CHECK_CONNECTION_TYPE,
                mode="NONE",
                filter_data={},
            )

    def test_create_task_builds_check_request_for_selected_hosts(self):
        db = FakeDB(select_results=[[], []])

        summary = self.module.create_task(
            db=db,
            hosts=[11, 12],
            task_type=self.module.HOST_TASK_CHECK_TYPE,
            mode="LAST_N_FILES",
            filter_data={
                "start_date": None,
                "end_date": None,
                "last_n_files": "10",
                "extension": ".zip",
                "file_path": "/mnt/internal/inbox",
                "file_name": None,
            },
        )

        self.assertEqual(summary, {"queued_count": 2, "skipped_count": 0})
        inserts = [row for row in db.executions if row[0].startswith("INSERT INTO HOST_TASK")]
        self.assertEqual(len(inserts), 2)
        self.assertTrue(all(params[1] == self.module.HOST_TASK_CHECK_TYPE for _, params in inserts))
        self.assertTrue(all("Host Check | Backup (LAST_N_FILES)" in params[-1] for _, params in inserts))


if __name__ == "__main__":
    unittest.main()
