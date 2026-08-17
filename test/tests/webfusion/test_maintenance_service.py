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
    - bounded recent history queries without mandatory filters
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
                "host_id": "241",
                "task_type": "abc",
                "status": "7x",
                "search": "  host-01  ",
                "limit": "9999",
            }
        )

        self.assertEqual(filters["queue_kind"], self.module.QUEUE_FILE_TASK)
        self.assertEqual(filters["host_id"], 241)
        self.assertIsNone(filters["task_type"])
        self.assertIsNone(filters["status"])
        self.assertEqual(filters["search"], "host-01")
        self.assertEqual(filters["limit"], self.module.MAX_PAGE_LIMIT)

    def test_build_history_filters_keeps_structured_fields(self):
        filters = self.module.build_history_filters(
            {
                "history_host_id": "241",
                "history_host_file_name": "sample.zip",
                "history_server_file_name": "p-1200--sample.zip",
                "history_message": "zip error",
                "history_date_field": "dt_backup",
                "history_date_from": "2026-07-08",
                "history_date_to": "2026-07-09",
                "history_discovery_status": "0",
                "history_backup_status": "-1",
                "history_processing_status": "1",
                "history_limit": "200",
            }
        )

        self.assertEqual(filters["host_id"], 241)
        self.assertEqual(filters["host_file_name"], "sample.zip")
        self.assertEqual(filters["server_file_name"], "p-1200--sample.zip")
        self.assertEqual(filters["message"], "zip error")
        self.assertEqual(filters["date_field"], "DT_BACKUP")
        self.assertEqual(filters["date_from"], "2026-07-08")
        self.assertEqual(filters["date_to"], "2026-07-09")
        self.assertEqual(filters["discovery_status"], 0)
        self.assertEqual(filters["backup_status"], -1)
        self.assertEqual(filters["processing_status"], 1)
        self.assertEqual(filters["limit"], 200)

    def test_build_file_task_filters_keeps_the_selected_date_range(self):
        filters = self.module.build_file_task_filters(
            {
                "queue_kind": "file",
                "host_file_name": "sample.zip",
                "date_field": "dt_file_created_host",
                "date_from": "2026-08-01",
                "date_to": "2026-08-02",
            }
        )

        self.assertEqual(filters["host_file_name"], "sample.zip")
        self.assertEqual(filters["date_field"], "DT_FILE_CREATED_HOST")
        self.assertEqual(filters["date_from"], "2026-08-01")
        self.assertEqual(filters["date_to"], "2026-08-02")

    def test_validate_file_task_filters_requires_a_date_field(self):
        with self.assertRaisesRegex(ValueError, "campo de data"):
            self.module.validate_file_task_filters(
                {
                    "date_field": "",
                    "date_from": "2026-08-01",
                    "date_to": "",
                }
            )

    def test_history_filters_allow_a_bounded_recent_window(self):
        self.assertTrue(
            self.module.history_filters_are_actionable(
                {
                    "host_id": None,
                    "host_file_name": "",
                    "server_file_name": "",
                    "message": "",
                    "date_from": "",
                    "date_to": "",
                    "limit": 50,
                }
            )
        )
        self.assertTrue(
            self.module.history_filters_are_actionable(
                {
                    "host_id": None,
                    "host_file_name": "sample.zip",
                    "server_file_name": "",
                    "message": "",
                    "date_from": "",
                    "date_to": "",
                    "limit": 100,
                }
            )
        )
        self.assertFalse(
            self.module.history_filters_are_actionable(
                {
                    "host_id": None,
                    "host_file_name": "",
                    "server_file_name": "",
                    "message": "processing error",
                    "date_field": "",
                    "date_from": "",
                    "date_to": "",
                    "limit": 50,
                }
            )
        )

    def test_validate_history_filters_requires_date_field_for_dates(self):
        with self.assertRaisesRegex(ValueError, "campo de data"):
            self.module.validate_history_filters(
                {
                    "host_id": None,
                    "host_file_name": "",
                    "server_file_name": "",
                    "message": "",
                    "date_field": "",
                    "date_from": "2026-07-08",
                    "date_to": "",
                }
            )

    def test_validate_history_filters_rejects_message_only_search(self):
        with self.assertRaisesRegex(ValueError, "antes de refinar por mensagem"):
            self.module.validate_history_filters(
                {
                    "host_id": None,
                    "host_file_name": "",
                    "server_file_name": "",
                    "message": "processing error",
                    "date_field": "",
                    "date_from": "",
                    "date_to": "",
                    "limit": 50,
                }
            )

    def test_validate_history_filters_allows_unfiltered_bounded_window(self):
        self.module.validate_history_filters(
            {
                "host_id": None,
                "host_file_name": "",
                "server_file_name": "",
                "message": "",
                "date_field": "",
                "date_from": "",
                "date_to": "",
                "limit": 200,
            }
        )

    def test_list_history_allows_the_recent_bounded_window(self):
        executed = []

        class FakeCursor:
            def execute(self, sql, params=None):
                executed.append((sql, params))

            def fetchall(self):
                if "information_schema.statistics" in executed[-1][0]:
                    return [{"present": 1}]
                return []

        class FakeDB:
            def cursor(self):
                return FakeCursor()

        self.module.list_file_history(
            FakeDB(),
            {
                "host_id": None,
                "host_file_name": "",
                "server_file_name": "",
                "message": "",
                "date_field": "",
                "date_from": "",
                "date_to": "",
                "discovery_status": None,
                "backup_status": None,
                "processing_status": None,
                "limit": 200,
            },
        )

        sql, params = executed[-1]
        self.assertIn("ORDER BY h.DT_PROCESSED DESC, h.ID_HISTORY DESC", sql)
        self.assertEqual(params, (200,))

    def test_apply_history_action_rejects_unknown_target_before_querying(self):
        service = self.module
        original_loader = service._load_history_rows_for_action

        try:
            service._load_history_rows_for_action = lambda db, history_ids: self.fail(
                "The history loader must not run for an invalid target."
            )
            with self.assertRaisesRegex(ValueError, "etapa de destino"):
                service.apply_history_action(
                    db=object(),
                    history_ids=[20],
                    target_stage="unexpected_target",
                    target_status=service.TASK_PENDING,
                )
        finally:
            service._load_history_rows_for_action = original_loader

    def test_list_history_uses_exact_identity_filters(self):
        executed = []

        class FakeCursor:
            def execute(self, sql, params=None):
                executed.append((sql, params))

            def fetchall(self):
                return []

        class FakeDB:
            def cursor(self):
                return FakeCursor()

        self.module.list_file_history(
            FakeDB(),
            {
                "host_id": 241,
                "host_file_name": "sample.zip",
                "server_file_name": "p-1200--sample.zip",
                "message": "",
                "date_field": "",
                "date_from": "",
                "date_to": "",
                "discovery_status": 0,
                "backup_status": -1,
                "processing_status": 1,
                "limit": 50,
            },
        )

        sql, params = executed[-1]
        self.assertIn("h.FK_HOST = %s", sql)
        self.assertIn("h.NA_HOST_FILE_NAME = %s", sql)
        self.assertIn("h.NA_SERVER_FILE_NAME = %s", sql)
        self.assertIn("h.NU_STATUS_DISCOVERY = %s", sql)
        self.assertIn("h.NU_STATUS_BACKUP = %s", sql)
        self.assertIn("h.NU_STATUS_PROCESSING = %s", sql)
        self.assertNotIn("host.NA_HOST_NAME = %s", sql)
        self.assertNotIn("h.NA_HOST_FILE_NAME LIKE %s", sql)
        self.assertNotIn("h.NA_SERVER_FILE_NAME LIKE %s", sql)
        self.assertIn(241, params)
        self.assertIn("sample.zip", params)
        self.assertIn("p-1200--sample.zip", params)
        self.assertIn(0, params)
        self.assertIn(-1, params)
        self.assertIn(1, params)

    def test_list_history_falls_back_to_identity_order_without_the_index(self):
        executed = []

        class FakeCursor:
            def execute(self, sql, params=None):
                executed.append((sql, params))

            def fetchall(self):
                return []

        class FakeDB:
            def cursor(self):
                return FakeCursor()

        self.module.list_file_history(
            FakeDB(),
            {
                "host_id": None,
                "host_file_name": "",
                "server_file_name": "",
                "message": "",
                "date_field": "",
                "date_from": "",
                "date_to": "",
                "discovery_status": None,
                "backup_status": None,
                "processing_status": None,
                "limit": 50,
            },
        )

        sql, _ = executed[-1]
        self.assertIn("ORDER BY h.ID_HISTORY DESC", sql)

    def test_list_history_does_not_limit_rows_to_one_operation(self):
        executed = []

        class FakeCursor:
            def execute(self, sql, params=None):
                executed.append((sql, params))

            def fetchall(self):
                return []

        class FakeDB:
            def cursor(self):
                return FakeCursor()

        self.module.list_file_history(
            FakeDB(),
            {
                "host_id": 241,
                "host_file_name": "sample.zip",
                "server_file_name": "p-1200--sample.zip",
                "message": "",
                "date_field": "",
                "date_from": "",
                "date_to": "",
                "limit": 50,
            },
        )

        sql, params = executed[-1]
        self.assertNotIn("h.NU_STATUS_BACKUP = %s", sql)
        self.assertNotIn("h.NU_STATUS_PROCESSING = %s", sql)
        self.assertIn(241, params)

    def test_list_host_tasks_applies_selected_host(self):
        executed = []

        class FakeCursor:
            def execute(self, sql, params=None):
                executed.append((sql, params))

            def fetchall(self):
                return []

        class FakeDB:
            def cursor(self):
                return FakeCursor()

        self.module.list_host_tasks(
            FakeDB(),
            {
                "host_id": 241,
                "search": "",
                "task_type": None,
                "status": None,
                "limit": 50,
            },
        )

        sql, params = executed[0]
        self.assertIn("ht.FK_HOST = %s", sql)
        self.assertIn(241, params)

    def test_list_file_tasks_applies_the_selected_date_range(self):
        executed = []

        class FakeCursor:
            def execute(self, sql, params=None):
                executed.append((sql, params))

            def fetchall(self):
                return []

        class FakeDB:
            def cursor(self):
                return FakeCursor()

        self.module.list_file_tasks(
            FakeDB(),
            {
                "host_id": None,
                "search": "",
                "task_type": None,
                "status": None,
                "host_file_name": "sample.zip",
                "date_field": "DT_FILE_TASK",
                "date_from": "2026-08-01",
                "date_to": "2026-08-02",
                "limit": 50,
            },
        )

        sql, params = executed[0]
        self.assertIn("ft.NA_HOST_FILE_NAME = %s", sql)
        self.assertIn("ft.DT_FILE_TASK >= %s", sql)
        self.assertIn("ft.DT_FILE_TASK < %s", sql)
        self.assertIn("sample.zip", params)
        self.assertIn("2026-08-01", params)
        self.assertIn("2026-08-02", params)

    def test_list_file_task_hosts_uses_the_queue_host_index(self):
        executed = []

        class FakeCursor:
            def execute(self, sql, params=None):
                executed.append((sql, params))

            def fetchall(self):
                return []

        class FakeDB:
            def cursor(self):
                return FakeCursor()

        self.module.list_file_task_hosts(FakeDB())

        sql, params = executed[0]
        self.assertIn("FILE_TASK ft USE INDEX (FK_FILE_TASK_HOST)", sql)
        self.assertIn("ft.FK_HOST = h.ID_HOST", sql)
        self.assertIsNone(params)

    def test_apply_bulk_action_rejects_unknown_action_before_querying(self):
        service = self.module
        original_loader = service._load_host_tasks_for_action

        try:
            service._load_host_tasks_for_action = lambda db, task_ids: self.fail(
                "The task loader must not run for an invalid action."
            )
            with self.assertRaisesRegex(ValueError, "Unsupported queue action"):
                service.apply_bulk_action(
                    db=object(),
                    queue_kind=service.QUEUE_HOST_TASK,
                    task_ids=[10],
                    action="unexpected_action",
                )
        finally:
            service._load_host_tasks_for_action = original_loader

    def test_apply_bulk_action_rejects_unknown_queue_before_querying(self):
        service = self.module
        original_loader = service._load_host_tasks_for_action

        try:
            service._load_host_tasks_for_action = lambda db, task_ids: self.fail(
                "The task loader must not run for an invalid queue."
            )
            with self.assertRaisesRegex(ValueError, "Unsupported queue kind"):
                service.apply_bulk_action(
                    db=object(),
                    queue_kind="unexpected_queue",
                    task_ids=[10],
                    action=service.ACTION_RESTART,
                )
        finally:
            service._load_host_tasks_for_action = original_loader

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
                "ID_HISTORY": 20,
            },
            self.module.ACTION_SUSPEND,
        )

        self.assertEqual(reason, "unsupported_suspend_type")

    def test_validate_file_task_action_blocks_reprocess_without_server_identity(self):
        reason = self.module._validate_file_task_action(
            {
                "NU_TYPE": self.module.FILE_TASK_DISCOVERY_TYPE,
                "NU_STATUS": self.module.TASK_DONE,
                "IS_OFFLINE": 0,
                "ID_HISTORY": 20,
                "HISTORY_SERVER_FILE_PATH": None,
                "HISTORY_SERVER_FILE_NAME": None,
            },
            self.module.ACTION_REPROCESS,
        )

        self.assertEqual(reason, "missing_server_identity")

    def test_validate_file_task_action_blocks_backup_move_outside_discovery(self):
        reason = self.module._validate_file_task_action(
            {
                "NU_TYPE": self.module.FILE_TASK_PROCESS_TYPE,
                "NU_STATUS": self.module.TASK_DONE,
                "IS_OFFLINE": 0,
                "ID_HISTORY": 20,
            },
            self.module.ACTION_MOVE_TO_BACKUP,
        )

        self.assertEqual(reason, "move_to_backup_requires_discovery")

    def test_validate_history_action_allows_backup_after_completed_discovery(self):
        reason = self.module._validate_history_action(
            {
                "ID_FILE_TASK": None,
                "NU_STATUS_DISCOVERY": self.module.TASK_DONE,
                "NU_STATUS_BACKUP": self.module.TASK_PENDING,
                "IS_OFFLINE": 0,
                "NA_HOST_FILE_PATH": "/host",
                "NA_HOST_FILE_NAME": "sample.zip",
            },
            target_stage=self.module.HISTORY_TARGET_BACKUP,
            target_status=self.module.TASK_PENDING,
        )

        self.assertIsNone(reason)

    def test_validate_history_action_allows_redo_backup_after_processing(self):
        reason = self.module._validate_history_action(
            {
                "ID_FILE_TASK": None,
                "NU_STATUS_DISCOVERY": self.module.TASK_DONE,
                "NU_STATUS_BACKUP": self.module.TASK_DONE,
                "NU_STATUS_PROCESSING": self.module.TASK_DONE,
                "IS_OFFLINE": 0,
                "NA_HOST_FILE_PATH": "/host",
                "NA_HOST_FILE_NAME": "validated.zip",
            },
            target_stage=self.module.HISTORY_TARGET_BACKUP,
            target_status=self.module.TASK_PENDING,
        )

        self.assertIsNone(reason)

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

    def test_apply_bulk_action_reprocesses_file_task_with_history_identity(self):
        service = self.module
        original_loader = service._load_file_tasks_for_action
        original_applier = service._apply_file_task_action
        updated_task_ids = []

        try:
            service._load_file_tasks_for_action = lambda db, task_ids: [
                {
                    "ID_FILE_TASK": 30,
                    "FK_HOST": 301,
                    "NU_TYPE": service.FILE_TASK_DISCOVERY_TYPE,
                    "NU_STATUS": service.TASK_DONE,
                    "NA_HOST_NAME": "host-online",
                    "IS_OFFLINE": 0,
                    "ID_HISTORY": 40,
                    "HISTORY_SERVER_FILE_PATH": "/repository",
                    "HISTORY_SERVER_FILE_NAME": "sample.zip",
                }
            ]
            service._apply_file_task_action = lambda db, row, action: updated_task_ids.append(
                (int(row["ID_FILE_TASK"]), action)
            )

            summary = service.apply_bulk_action(
                db=object(),
                queue_kind=service.QUEUE_FILE_TASK,
                task_ids=[30],
                action=service.ACTION_REPROCESS,
            )
        finally:
            service._load_file_tasks_for_action = original_loader
            service._apply_file_task_action = original_applier

        self.assertEqual(updated_task_ids, [(30, service.ACTION_REPROCESS)])
        self.assertEqual(summary["action_label"], "Reprocessar")
        self.assertEqual(summary["updated_count"], 1)

    def test_apply_file_task_target_action_preserves_selected_status(self):
        service = self.module
        original_loader = service._load_file_tasks_for_action
        original_applier = service._apply_file_task_backup_transition
        applied = []

        try:
            service._load_file_tasks_for_action = lambda db, task_ids: [
                {
                    "ID_FILE_TASK": 50,
                    "FK_HOST": 501,
                    "NU_STATUS": service.TASK_DONE,
                    "NA_HOST_NAME": "host-online",
                    "IS_OFFLINE": 0,
                    "ID_HISTORY": 60,
                    "HISTORY_SERVER_FILE_PATH": "/repository",
                    "HISTORY_SERVER_FILE_NAME": "validated.zip",
                }
            ]
            service._apply_file_task_backup_transition = lambda db, row, **kwargs: applied.append(
                (int(row["ID_FILE_TASK"]), kwargs["task_status"], kwargs["publish_reason"])
            )

            summary = service.apply_file_task_target_action(
                db=object(),
                task_ids=[50],
                target_stage=service.HISTORY_TARGET_BACKUP,
                target_status=service.TASK_FROZEN,
            )
        finally:
            service._load_file_tasks_for_action = original_loader
            service._apply_file_task_backup_transition = original_applier

        self.assertEqual(
            applied,
            [(50, service.TASK_FROZEN, "backup_-3")],
        )
        self.assertEqual(summary["action_label"], "Backup: Congelada")
        self.assertEqual(summary["updated_count"], 1)

    def test_apply_file_task_target_action_rejects_unknown_target_before_querying(self):
        service = self.module
        original_loader = service._load_file_tasks_for_action

        try:
            service._load_file_tasks_for_action = lambda db, task_ids: self.fail(
                "The task loader must not run for an invalid target."
            )
            with self.assertRaisesRegex(ValueError, "etapa de destino"):
                service.apply_file_task_target_action(
                    db=object(),
                    task_ids=[50],
                    target_stage="unexpected_target",
                    target_status=service.TASK_PENDING,
                )
        finally:
            service._load_file_tasks_for_action = original_loader

    def test_apply_history_action_reports_blocked_and_missing_rows(self):
        service = self.module
        original_loader = service._load_history_rows_for_action
        original_applier = service._apply_history_process_with_cursor
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
            service._load_history_rows_for_action = lambda db, history_ids: [
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
            service._apply_history_process_with_cursor = lambda cursor, row, **kwargs: recreated_history_ids.append(
                int(row["ID_HISTORY"])
            )
            service._publish_summary_scope = lambda db, host_id, reason: published_hosts.append((host_id, reason))

            summary = service.apply_history_action(
                db=FakeDB(),
                history_ids=[20, 21, 99],
                target_stage=service.HISTORY_TARGET_PROCESS,
                target_status=service.TASK_PENDING,
            )
        finally:
            service._load_history_rows_for_action = original_loader
            service._apply_history_process_with_cursor = original_applier
            service._publish_summary_scope = original_publish

        self.assertEqual(recreated_history_ids, [20])
        self.assertEqual(
            published_hosts,
            [(201, "maintenance_history_process_1")],
        )
        self.assertEqual(summary["updated_count"], 1)
        self.assertEqual(summary["blocked_count"], 1)
        self.assertEqual(summary["missing_ids"], [99])
        self.assertEqual(summary["blocked_rows"][0]["reason"], "missing_server_identity")

    def test_apply_history_action_reprocesses_validated_file(self):
        service = self.module
        original_loader = service._load_history_rows_for_action
        original_applier = service._apply_history_process_with_cursor
        original_publish = service._publish_summary_scope
        reprocessed_history_ids = []
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
            service._load_history_rows_for_action = lambda db, history_ids: [
                {
                    "ID_HISTORY": 30,
                    "FK_HOST": 301,
                    "NA_HOST_NAME": "host-validated",
                    "ID_FILE_TASK": None,
                    "NU_STATUS_BACKUP": service.TASK_DONE,
                    "NU_STATUS_PROCESSING": service.TASK_DONE,
                    "NA_SERVER_FILE_PATH": "/repository",
                    "NA_SERVER_FILE_NAME": "validated.zip",
                    "DT_BACKUP": "2026-08-01 12:00:00",
                }
            ]
            service._apply_history_process_with_cursor = lambda cursor, row, **kwargs: reprocessed_history_ids.append(
                (int(row["ID_HISTORY"]), kwargs["message"])
            )
            service._publish_summary_scope = lambda db, host_id, reason: published_hosts.append((host_id, reason))

            summary = service.apply_history_action(
                db=FakeDB(),
                history_ids=[30],
                target_stage=service.HISTORY_TARGET_PROCESS,
                target_status=service.TASK_PENDING,
            )
        finally:
            service._load_history_rows_for_action = original_loader
            service._apply_history_process_with_cursor = original_applier
            service._publish_summary_scope = original_publish

        self.assertEqual([history_id for history_id, _ in reprocessed_history_ids], [30])
        self.assertIn("created from FILE_TASK_HISTORY action", reprocessed_history_ids[0][1])
        self.assertEqual(
            published_hosts,
            [(301, "maintenance_history_process_1")],
        )
        self.assertEqual(summary["action_label"], "Processamento: Aguardando")
        self.assertEqual(summary["updated_count"], 1)

    def test_apply_history_action_preserves_selected_initial_status(self):
        service = self.module
        original_loader = service._load_history_rows_for_action
        original_applier = service._apply_history_process_with_cursor
        original_publish = service._publish_summary_scope
        applied_statuses = []

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
            service._load_history_rows_for_action = lambda db, history_ids: [
                {
                    "ID_HISTORY": 40,
                    "FK_HOST": 401,
                    "NA_HOST_NAME": "host-validated",
                    "ID_FILE_TASK": None,
                    "NU_STATUS_BACKUP": service.TASK_DONE,
                    "NU_STATUS_PROCESSING": service.TASK_DONE,
                    "NA_SERVER_FILE_PATH": "/repository",
                    "NA_SERVER_FILE_NAME": "validated.zip",
                    "DT_BACKUP": "2026-08-01 12:00:00",
                }
            ]
            service._apply_history_process_with_cursor = lambda cursor, row, **kwargs: applied_statuses.append(
                kwargs["task_status"]
            )
            service._publish_summary_scope = lambda db, host_id, reason: None

            summary = service.apply_history_action(
                db=FakeDB(),
                history_ids=[40],
                target_stage=service.HISTORY_TARGET_PROCESS,
                target_status=service.TASK_FROZEN,
            )
        finally:
            service._load_history_rows_for_action = original_loader
            service._apply_history_process_with_cursor = original_applier
            service._publish_summary_scope = original_publish

        self.assertEqual(applied_statuses, [service.TASK_FROZEN])
        self.assertEqual(summary["action_label"], "Processamento: Congelada")


if __name__ == "__main__":
    unittest.main()
