"""
Validation tests for `webfusion.modules.maintenance.service`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_maintenance_service.py -q

What is covered here:
    - conservative guardrails for offline hosts and unsupported manual actions
    - normalization of maintenance filters before they reach SQL
    - bulk-action summaries for blocked and missing queue rows
    - history-driven recreation summaries for backup/process retries
    - structured history filters for date and file fields
    - guard against unanchored history scans
"""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


def load_maintenance_service():
    """Reload the maintenance service so tests observe current module constants."""
    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

    sys.modules.pop("modules.maintenance.service", None)
    return importlib.import_module("modules.maintenance.service")


class TestMaintenanceService(unittest.TestCase):
    """Protect the small safe surface exposed for manual queue intervention."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_maintenance_service()

    def test_build_filters_normalizes_invalid_values(self):
        filters = self.module.build_filters(
            {
                "queue_kind": "FILE",
                "task_type": "abc",
                "status": "7x",
                "search": "  host-01  ",
                "limit": "9999",
            }
        )

        self.assertEqual(filters["queue_kind"], self.module.QUEUE_FILE_TASK)
        self.assertIsNone(filters["task_type"])
        self.assertIsNone(filters["status"])
        self.assertEqual(filters["search"], "host-01")
        self.assertEqual(filters["limit"], self.module.MAX_PAGE_LIMIT)

    def test_build_history_filters_keeps_structured_fields(self):
        filters = self.module.build_history_filters(
            {
                "history_phase": "backup",
                "history_host_name": "RFEye002264",
                "history_host_file_name": "sample.zip",
                "history_server_file_name": "p-1200--sample.zip",
                "history_message": "zip error",
                "history_date_field": "dt_backup",
                "history_date_from": "2026-07-08",
                "history_date_to": "2026-07-09",
                "history_limit": "200",
            }
        )

        self.assertEqual(filters["phase"], "backup")
        self.assertEqual(filters["host_name"], "RFEye002264")
        self.assertEqual(filters["host_file_name"], "sample.zip")
        self.assertEqual(filters["server_file_name"], "p-1200--sample.zip")
        self.assertEqual(filters["message"], "zip error")
        self.assertEqual(filters["date_field"], "DT_BACKUP")
        self.assertEqual(filters["date_from"], "2026-07-08")
        self.assertEqual(filters["date_to"], "2026-07-09")
        self.assertEqual(filters["limit"], 100)

    def test_history_filters_are_actionable_requires_one_anchor(self):
        self.assertFalse(
            self.module.history_filters_are_actionable(
                {
                    "host_name": "",
                    "host_file_name": "",
                    "server_file_name": "",
                    "message": "",
                    "date_from": "",
                    "date_to": "",
                }
            )
        )
        self.assertTrue(
            self.module.history_filters_are_actionable(
                {
                    "host_name": "",
                    "host_file_name": "sample.zip",
                    "server_file_name": "",
                    "message": "",
                    "date_from": "",
                    "date_to": "",
                }
            )
        )

    def test_validate_host_task_action_blocks_restart_for_offline_host(self):
        reason = self.module._validate_host_task_action(
            {
                "NU_TYPE": self.module.HOST_TASK_CHECK_TYPE,
                "IS_OFFLINE": 1,
            },
            self.module.ACTION_RESTART,
        )

        self.assertEqual(reason, "host_offline")

    def test_validate_file_task_action_blocks_suspend_for_processing_rows(self):
        reason = self.module._validate_file_task_action(
            {
                "NU_TYPE": self.module.FILE_TASK_PROCESS_TYPE,
                "IS_OFFLINE": 0,
            },
            self.module.ACTION_SUSPEND,
        )

        self.assertEqual(reason, "unsupported_suspend_type")

    def test_apply_bulk_action_reports_blocked_and_missing_rows(self):
        service = self.module
        original_loader = service._load_host_tasks_for_action
        original_applier = service._apply_host_task_action

        updated_task_ids = []

        try:
            service._load_host_tasks_for_action = lambda db, task_ids: [
                {
                    "ID_HOST_TASK": 10,
                    "FK_HOST": 101,
                    "NU_TYPE": service.HOST_TASK_CHECK_TYPE,
                    "NA_HOST_NAME": "host-online",
                    "IS_OFFLINE": 0,
                },
                {
                    "ID_HOST_TASK": 11,
                    "FK_HOST": 102,
                    "NU_TYPE": service.HOST_TASK_CHECK_TYPE,
                    "NA_HOST_NAME": "host-offline",
                    "IS_OFFLINE": 1,
                },
            ]
            service._apply_host_task_action = lambda db, row, action: updated_task_ids.append(
                int(row["ID_HOST_TASK"])
            )

            summary = service.apply_bulk_action(
                db=object(),
                queue_kind=service.QUEUE_HOST_TASK,
                task_ids=[10, 11, 99],
                action=service.ACTION_RESTART,
            )
        finally:
            service._load_host_tasks_for_action = original_loader
            service._apply_host_task_action = original_applier

        self.assertEqual(updated_task_ids, [10])
        self.assertEqual(summary["updated_count"], 1)
        self.assertEqual(summary["blocked_count"], 1)
        self.assertEqual(summary["missing_ids"], [99])
        self.assertEqual(summary["blocked_rows"][0]["reason"], "host_offline")

    def test_apply_history_recreate_action_reports_blocked_and_missing_rows(self):
        service = self.module
        original_loader = service._load_history_rows_for_recreation
        original_applier = service._apply_history_recreate_process_with_cursor
        original_publish = service._publish_summary_scope

        recreated_history_ids = []
        published_hosts = []

        class FakeCursor:
            def execute(self, sql, params=None):
                return None

        class FakeDB:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                return None

            def rollback(self):
                return None

        try:
            service._load_history_rows_for_recreation = lambda db, history_ids: [
                {
                    "ID_HISTORY": 20,
                    "FK_HOST": 201,
                    "NA_HOST_NAME": "host-online",
                    "ID_FILE_TASK": None,
                    "NU_STATUS_BACKUP": service.TASK_DONE,
                    "NU_STATUS_PROCESSING": service.TASK_ERROR,
                    "NA_SERVER_FILE_PATH": "/mnt/reposfi/trash",
                    "NA_SERVER_FILE_NAME": "sample.zip",
                },
                {
                    "ID_HISTORY": 21,
                    "FK_HOST": 202,
                    "NA_HOST_NAME": "host-missing-server",
                    "ID_FILE_TASK": None,
                    "NU_STATUS_BACKUP": service.TASK_DONE,
                    "NU_STATUS_PROCESSING": service.TASK_ERROR,
                    "NA_SERVER_FILE_PATH": None,
                    "NA_SERVER_FILE_NAME": None,
                },
            ]
            service._apply_history_recreate_process_with_cursor = lambda cursor, row, **kwargs: recreated_history_ids.append(
                int(row["ID_HISTORY"])
            )
            service._publish_summary_scope = lambda db, host_id, reason: published_hosts.append((host_id, reason))

            summary = service.apply_history_recreate_action(
                db=FakeDB(),
                history_ids=[20, 21, 99],
                action=service.ACTION_RECREATE_PROCESS,
            )
        finally:
            service._load_history_rows_for_recreation = original_loader
            service._apply_history_recreate_process_with_cursor = original_applier
            service._publish_summary_scope = original_publish

        self.assertEqual(recreated_history_ids, [20])
        self.assertEqual(
            published_hosts,
            [(201, "maintenance_recreate_process_from_history")],
        )
        self.assertEqual(summary["updated_count"], 1)
        self.assertEqual(summary["blocked_count"], 1)
        self.assertEqual(summary["missing_ids"], [99])
        self.assertEqual(summary["blocked_rows"][0]["reason"], "missing_server_identity")


if __name__ == "__main__":
    unittest.main()
