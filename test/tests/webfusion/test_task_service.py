from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


def load_task_service():
    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

    sys.modules.pop("modules.task.service", None)
    return importlib.import_module("modules.task.service")


class FakeCursor:
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
    def __init__(self, select_results=None):
        self.select_results = list(select_results or [])
        self.executions = []
        self.commit_calls = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commit_calls += 1


class TestTaskService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_task_service()

    def test_operational_task_refreshes_existing_processing_row(self):
        db = FakeDB(
            select_results=[
                [
                    {
                        "ID_HOST_TASK": 77,
                        "NU_TYPE": self.module.HOST_TASK_PROCESSING_TYPE,
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
            message="Created by WebFusion | Backup (LAST_N_FILES) | Individual",
        )

        self.assertEqual(result, "refreshed")
        self.assertEqual(db.commit_calls, 1)
        self.assertEqual(len(db.executions), 2)
        self.assertIn("NU_TYPE IN", db.executions[0][0])

        update_sql, update_params = db.executions[1]
        self.assertTrue(update_sql.startswith("UPDATE HOST_TASK"))
        self.assertEqual(update_params[0], self.module.HOST_TASK_CHECK_TYPE)
        self.assertEqual(update_params[1], self.module.TASK_PENDING)
        self.assertEqual(update_params[-1], 77)
        self.assertIn('"last_n_files": "25"', update_params[2])

    def test_running_operational_task_is_preserved(self):
        db = FakeDB(
            select_results=[
                [
                    {
                        "ID_HOST_TASK": 88,
                        "NU_TYPE": self.module.HOST_TASK_PROCESSING_TYPE,
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
            filter_dict=self.module.NONE_FILTER.copy(),
            message="Created by WebFusion | Backup (NONE) | Individual",
        )

        self.assertEqual(result, "skipped_active")
        self.assertEqual(db.commit_calls, 0)
        self.assertEqual(len(db.executions), 1)

    def test_statistics_task_active_row_is_not_duplicated(self):
        db = FakeDB(
            select_results=[
                [
                    {
                        "ID_HOST_TASK": 99,
                        "NU_TYPE": self.module.HOST_TASK_UPDATE_STATISTICS_TYPE,
                        "NU_STATUS": self.module.TASK_PENDING,
                        "FILTER": self.module._serialize_filter(self.module.NONE_FILTER),
                    }
                ]
            ]
        )

        result = self.module.queue_host_task_safe(
            db=db,
            host_id=1003,
            task_type=self.module.HOST_TASK_UPDATE_STATISTICS_TYPE,
            filter_dict=self.module.NONE_FILTER.copy(),
            message="Created by WebFusion | Update Statistics | Individual",
        )

        self.assertEqual(result, "skipped_active")
        self.assertEqual(db.commit_calls, 0)
        self.assertEqual(len(db.executions), 1)

    def test_statistics_task_done_row_is_reactivated(self):
        db = FakeDB(
            select_results=[
                [
                    {
                        "ID_HOST_TASK": 101,
                        "NU_TYPE": self.module.HOST_TASK_UPDATE_STATISTICS_TYPE,
                        "NU_STATUS": self.module.TASK_DONE,
                        "FILTER": self.module._serialize_filter(self.module.NONE_FILTER),
                    }
                ]
            ]
        )

        result = self.module.queue_host_task_safe(
            db=db,
            host_id=1004,
            task_type=self.module.HOST_TASK_UPDATE_STATISTICS_TYPE,
            filter_dict=self.module.NONE_FILTER.copy(),
            message="Created by WebFusion | Update Statistics | Individual",
        )

        self.assertEqual(result, "refreshed")
        self.assertEqual(db.commit_calls, 1)
        self.assertTrue(db.executions[1][0].startswith("UPDATE HOST_TASK"))

    def test_statistics_task_reuses_singleton_even_with_legacy_filter(self):
        db = FakeDB(
            select_results=[
                [
                    {
                        "ID_HOST_TASK": 111,
                        "NU_TYPE": self.module.HOST_TASK_UPDATE_STATISTICS_TYPE,
                        "NU_STATUS": self.module.TASK_DONE,
                        "FILTER": '{"legacy":"value"}',
                    }
                ]
            ]
        )

        result = self.module.queue_host_task_safe(
            db=db,
            host_id=1006,
            task_type=self.module.HOST_TASK_UPDATE_STATISTICS_TYPE,
            filter_dict=self.module.NONE_FILTER.copy(),
            message="Created by WebFusion | Update Statistics | Individual",
        )

        self.assertEqual(result, "refreshed")
        self.assertEqual(db.commit_calls, 1)
        self.assertTrue(db.executions[1][0].startswith("UPDATE HOST_TASK"))

    def test_connection_task_matches_filter_semantically(self):
        db = FakeDB(
            select_results=[
                [
                    {
                        "ID_HOST_TASK": 202,
                        "NU_TYPE": self.module.HOST_TASK_CHECK_CONNECTION_TYPE,
                        "NU_STATUS": self.module.TASK_DONE,
                        "FILTER": (
                            '{"agent":"local","end_date":null,"extension":null,'
                            '"file_name":null,"file_path":"/mnt/internal/data",'
                            '"last_n_files":null,"mode":"NONE","start_date":null}'
                        ),
                    }
                ]
            ]
        )

        result = self.module.queue_host_task_safe(
            db=db,
            host_id=1005,
            task_type=self.module.HOST_TASK_CHECK_CONNECTION_TYPE,
            filter_dict=self.module.NONE_FILTER.copy(),
            message="Created by WebFusion | Check Connection | Individual",
        )

        self.assertEqual(result, "refreshed")
        self.assertEqual(db.commit_calls, 1)
        self.assertTrue(db.executions[1][0].startswith("UPDATE HOST_TASK"))


if __name__ == "__main__":
    unittest.main()
