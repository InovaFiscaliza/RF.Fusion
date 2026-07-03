"""
Validation tests for `dbHandlerBKP.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/db/test_dbhandler_bkp.py -q

What is covered here:
    - `HOST` locks in transient SFTP cooldown (`NU_PID = 0`) are preserved by
      the normal safe-release path
    - the janitor preserves recent cooldowns and releases only expired ones
    - expired cooldown release updates the expected HOST fields
    - FILE history/task timestamps remain caller-owned instead of being filled
      implicitly inside the handler
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import DB_ROOT, bind_real_package, bind_real_shared_package, ensure_app_paths, import_package_module


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        db_bkp_module = import_package_module("db", DB_ROOT, "dbHandlerBKP")


class FakeLog:
    """Collect structured log output emitted by the handler methods."""

    def __init__(self) -> None:
        self.entries: list[str] = []
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def entry(self, message: str) -> None:
        self.entries.append(message)

    def warning(self, message: str) -> None:
        self.warnings.append(message)

    def error(self, message: str) -> None:
        self.errors.append(message)


class HostCooldownTests(unittest.TestCase):
    """Validate the HOST cooldown contract used by discovery and backup."""

    def make_handler(self):
        """Build a bare handler instance with only the pieces these tests use."""

        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler._connect = lambda: None
        handler._disconnect = lambda: None
        handler._summary_publish_host_scope = lambda *args, **kwargs: None
        handler.db_connection = type(
            "FakeConnection",
            (),
            {"rollback": lambda self: None},
        )()
        return handler

    def test_host_release_safe_preserves_transient_busy_cooldown(self) -> None:
        """Safe release must not clear a host that is only in short cooldown."""

        handler = self.make_handler()
        updates = []
        handler.host_read_status = lambda host_id: {"IS_BUSY": True, "NU_PID": 0}
        handler.host_update = lambda **kwargs: updates.append(kwargs)

        handler.host_release_safe(host_id=10, current_pid=999)

        self.assertEqual(updates, [])

    def test_host_cleanup_stale_locks_preserves_recent_sftp_cooldown(self) -> None:
        """Recent cooldowns must not be released by the janitor prematurely."""

        handler = self.make_handler()
        released = []
        queued = []
        handler._select_raw = lambda *args, **kwargs: [
            {
                "ID_HOST": 11,
                "NA_HOST_NAME": "station-a",
                "DT_BUSY": datetime.now(),
                "NU_PID": 0,
                "FILE_RUNNING": 0,
                "HOST_PROCESSING_RUNNING": 0,
            }
        ]
        handler.host_update = lambda **kwargs: released.append(kwargs)
        handler.queue_host_task = lambda **kwargs: queued.append(kwargs)

        handler.host_cleanup_stale_locks(threshold_seconds=999)

        self.assertEqual(released, [])
        self.assertEqual(queued, [])

    def test_host_cleanup_stale_locks_releases_expired_sftp_cooldown(self) -> None:
        """Expired cooldowns must be reopened so backup/discovery can retry."""

        handler = self.make_handler()
        released = []
        queued = []
        handler._select_raw = lambda *args, **kwargs: [
            {
                "ID_HOST": 12,
                "NA_HOST_NAME": "station-b",
                "DT_BUSY": datetime.now()
                - timedelta(seconds=db_bkp_module.k.SFTP_BUSY_COOLDOWN_SECONDS + 5),
                "NU_PID": 0,
                "FILE_RUNNING": 0,
                "HOST_PROCESSING_RUNNING": 0,
            }
        ]
        handler.host_update = lambda **kwargs: released.append(kwargs)
        handler.queue_host_task = lambda **kwargs: queued.append(kwargs)

        handler.host_cleanup_stale_locks(threshold_seconds=999)

        self.assertEqual(
            released,
            [{"host_id": 12, "IS_BUSY": False, "NU_PID": 0}],
        )
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]["host_id"], 12)

    def test_release_expired_transient_busy_cooldowns_updates_expected_fields(self) -> None:
        """Expired cooldown release must reset BUSY state and clear DT_BUSY."""

        handler = self.make_handler()
        captured = {}

        def fake_update_row(*, table, data, where, commit):
            captured["table"] = table
            captured["data"] = data
            captured["where"] = where
            captured["commit"] = commit
            return 2

        handler._update_row = fake_update_row

        released = handler._release_expired_transient_busy_cooldowns(
            cooldown_seconds=7
        )

        self.assertEqual(released, 2)
        self.assertEqual(captured["table"], "HOST")
        self.assertEqual(
            captured["data"],
            {"IS_BUSY": False, "DT_BUSY": None, "NU_PID": 0},
        )
        self.assertEqual(captured["where"]["IS_BUSY"], True)
        self.assertEqual(captured["where"]["NU_PID"], 0)
        self.assertEqual(captured["commit"], True)
        self.assertIn("DT_BUSY__lt", captured["where"])

    def test_host_task_cleanup_stale_operational_tasks_normalizes_pending_without_timestamp(self) -> None:
        """Pending operational HOST_TASK without DT_HOST_TASK should get a janitor timestamp."""

        handler = self.make_handler()
        updates = []
        handler._select_raw = lambda *args, **kwargs: [
            {
                "ID_HOST_TASK": 41,
                "FK_HOST": 14,
                "NU_TYPE": db_bkp_module.k.HOST_TASK_PROCESSING_TYPE,
                "NU_STATUS": db_bkp_module.k.TASK_PENDING,
                "DT_HOST_TASK": None,
                "NU_PID": None,
                "IS_BUSY": False,
                "HOST_OWNER_PID": 0,
            }
        ]
        handler.host_task_update = lambda **kwargs: updates.append(kwargs)
        handler.host_update = lambda **kwargs: None

        handler.host_task_cleanup_stale_operational_tasks(stale_after_seconds=30)

        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["task_id"], 41)
        self.assertIsInstance(updates[0]["DT_HOST_TASK"], datetime)
        self.assertIn("timestamp normalized by janitor", updates[0]["NA_MESSAGE"])

    def test_host_task_cleanup_stale_operational_tasks_recovers_stale_running_and_releases_host(self) -> None:
        """Stale RUNNING operational HOST_TASK should return to PENDING and free stale host lock."""

        handler = self.make_handler()
        task_updates = []
        host_updates = []
        handler._select_raw = lambda *args, **kwargs: [
            {
                "ID_HOST_TASK": 42,
                "FK_HOST": 15,
                "NU_TYPE": db_bkp_module.k.HOST_TASK_PROCESSING_TYPE,
                "NU_STATUS": db_bkp_module.k.TASK_RUNNING,
                "DT_HOST_TASK": datetime.now() - timedelta(seconds=301),
                "NU_PID": 999001,
                "IS_BUSY": True,
                "HOST_OWNER_PID": 999001,
            }
        ]
        handler.host_task_update = lambda **kwargs: task_updates.append(kwargs)
        handler.host_update = lambda **kwargs: host_updates.append(kwargs)
        original_pid_exists = db_bkp_module.tools.pid_exists
        db_bkp_module.tools.pid_exists = lambda pid: False
        try:
            handler.host_task_cleanup_stale_operational_tasks(stale_after_seconds=300)
        finally:
            db_bkp_module.tools.pid_exists = original_pid_exists

        self.assertEqual(len(task_updates), 1)
        self.assertEqual(task_updates[0]["task_id"], 42)
        self.assertEqual(task_updates[0]["NU_STATUS"], db_bkp_module.k.TASK_PENDING)
        self.assertIn("TASK_PID_STALE", task_updates[0]["NA_MESSAGE"])
        self.assertEqual(
            host_updates,
            [{"host_id": 15, "IS_BUSY": False, "NU_PID": db_bkp_module.k.HOST_UNLOCKED_PID}],
        )


class HostConnectivityListTests(unittest.TestCase):
    """Validate the HOST snapshot read used by the maintenance sweep."""

    def make_handler(self):
        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler._connect = lambda: None
        handler._disconnect = lambda: None
        handler._summary_publish_host_scope = lambda *args, **kwargs: None
        handler.db_connection = type(
            "FakeConnection",
            (),
            {"rollback": lambda self: None},
        )()
        return handler

    def test_host_list_for_connectivity_check_keeps_offline_hosts_in_snapshot(self) -> None:
        """The maintenance sweep must read online and offline hosts alike."""

        handler = self.make_handler()
        captured = {}

        def fake_select_rows(*, table, where, order_by, cols):
            captured["table"] = table
            captured["where"] = where
            captured["order_by"] = order_by
            captured["cols"] = cols
            return []

        handler._select_rows = fake_select_rows

        handler.host_list_for_connectivity_check()

        self.assertEqual(captured["table"], "HOST")
        self.assertEqual(
            captured["where"]["#CUSTOM#HOST_ADDRESS"],
            "NA_HOST_ADDRESS IS NOT NULL AND TRIM(NA_HOST_ADDRESS) <> ''",
        )
        self.assertNotIn("IS_OFFLINE", captured["where"]["#CUSTOM#HOST_ADDRESS"])
        self.assertEqual(
            captured["order_by"],
            "IS_OFFLINE ASC, DT_LAST_CHECK IS NULL DESC, DT_LAST_CHECK ASC, ID_HOST ASC",
        )


class HostStatisticsRefreshTests(unittest.TestCase):
    """Validate durable HOST aggregates derived from FILE_TASK_HISTORY."""

    def make_handler(self):
        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler._connect = lambda: None
        handler._disconnect = lambda: None
        handler._summary_publish_host_scope = lambda *args, **kwargs: None
        handler.db_connection = type(
            "FakeConnection",
            (),
            {"rollback": lambda self: None},
        )()
        return handler

    def test_host_update_statistics_refreshes_nu_host_files_from_history(self) -> None:
        """HOST.NU_HOST_FILES must track the durable discovered-file total."""

        handler = self.make_handler()
        host_updates = []
        summary_scopes = []

        def fake_select_raw(sql, params):
            if "SUM(NU_STATUS_DISCOVERY  = 0)" in sql:
                return [
                    {
                        "total_discovered": 17,
                        "total_backup": 11,
                        "total_processed": 9,
                        "pending_backup": 3,
                        "pending_process": 2,
                        "error_discovery": 1,
                        "error_backup": 4,
                        "error_process": 5,
                        "last_discovered": datetime(2026, 5, 18, 10, 0, 0),
                        "last_backup": datetime(2026, 5, 19, 11, 0, 0),
                        "last_processed": datetime(2026, 5, 20, 12, 0, 0),
                    }
                ]

            if "AS pending_kb" in sql and "AS done_kb" in sql:
                return [
                    {
                        "pending_kb": 2048,
                        "done_kb": 4096,
                    }
                ]

            raise AssertionError(f"Unexpected SQL: {sql}")

        handler._select_raw = fake_select_raw
        handler.host_update = lambda **kwargs: host_updates.append(kwargs)
        handler._summary_publish_host_scope = lambda host_id, **kwargs: summary_scopes.append(
            {"host_id": host_id, **kwargs}
        )

        handler.host_update_statistics(88)

        self.assertEqual(len(host_updates), 1)
        self.assertEqual(host_updates[0]["host_id"], 88)
        self.assertEqual(host_updates[0]["NU_HOST_FILES"], 17)
        self.assertEqual(host_updates[0]["NU_DONE_FILE_DISCOVERY_TASKS"], 17)
        self.assertEqual(host_updates[0]["NU_DONE_FILE_BACKUP_TASKS"], 11)
        self.assertEqual(host_updates[0]["VL_PENDING_BACKUP_KB"], 2048)
        self.assertEqual(host_updates[0]["VL_DONE_BACKUP_KB"], 4096)
        self.assertEqual(summary_scopes, [])


class FileTimestampOwnershipTests(unittest.TestCase):
    """Validate that task/history timestamps are explicit at call sites."""

    def make_handler(self):
        """Build a lightweight handler without opening a real DB connection."""

        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler._connect = lambda: None
        handler._disconnect = lambda: None
        handler.db_connection = type(
            "FakeConnection",
            (),
            {"rollback": lambda self: None},
        )()
        return handler

    def test_file_history_update_does_not_inject_backup_timestamp(self) -> None:
        """Backup history updates must carry DT_BACKUP only when the caller passes it."""

        handler = self.make_handler()
        captured = {}

        def fake_update_row(*, table, data, where, commit):
            captured["table"] = table
            captured["data"] = data
            captured["where"] = where
            captured["commit"] = commit
            return 1

        handler._update_row = fake_update_row

        handler.file_history_update(
            task_type=db_bkp_module.k.FILE_TASK_BACKUP_TYPE,
            history_id=99,
            NU_STATUS_BACKUP=db_bkp_module.k.TASK_DONE,
            NA_MESSAGE="backup done",
        )

        self.assertEqual(captured["table"], "FILE_TASK_HISTORY")
        self.assertNotIn("DT_BACKUP", captured["data"])
        self.assertEqual(captured["where"], {"ID_HISTORY": 99})

    def test_file_task_update_does_not_inject_dt_file_task(self) -> None:
        """FILE_TASK updates must not touch DT_FILE_TASK unless the caller asks for it."""

        handler = self.make_handler()
        captured = {}

        def fake_update_row(*, table, data, where, commit):
            captured["table"] = table
            captured["data"] = data
            captured["where"] = where
            captured["commit"] = commit
            return 1

        handler._update_row = fake_update_row

        handler.file_task_update(
            task_id=77,
            NU_STATUS=db_bkp_module.k.TASK_ERROR,
            NA_MESSAGE="processing failed",
        )

        self.assertEqual(captured["table"], "FILE_TASK")
        self.assertNotIn("DT_FILE_TASK", captured["data"])
        self.assertEqual(captured["where"], {"ID_FILE_TASK": 77})


class FileDiscoveryDedupTests(unittest.TestCase):
    """Validate discovery deduplication against durable FILE_TASK_HISTORY rows."""

    class _Meta:
        """Tiny metadata stub used by the dedup tests."""

        def __init__(
            self,
            path: str,
            name: str,
            size_kb: int,
            extension: str = ".bin",
        ) -> None:
            self.NA_PATH = path
            self.NA_FILE = name
            self.VL_FILE_SIZE_KB = size_kb
            self.NA_EXTENSION = extension
            self.DT_FILE_CREATED = datetime(2026, 6, 1, 12, 0, 0)

    def make_handler(self):
        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler._connect = lambda: None
        handler._disconnect = lambda: None
        return handler

    def test_filter_existing_file_batch_uses_path_name_size_identity(self) -> None:
        """Matching path, name, and size should deduplicate regardless of timestamp."""

        handler = self.make_handler()
        captured = {}

        def fake_select_raw(sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return [
                {
                    "path": "/mnt/internal/data/a",
                    "name": "same.bin",
                    "size": 123,
                }
            ]

        handler._select_raw = fake_select_raw

        batch = [
            self._Meta("/mnt/internal/data/a", "same.bin", 123),
        ]

        result = handler.filter_existing_file_batch(
            host_id=99,
            batch=batch,
            batch_size=1000,
        )

        self.assertEqual(result, [])
        self.assertIn("h.NA_HOST_FILE_PATH = f.path", captured["sql"])
        self.assertEqual(
            captured["params"],
            ("/mnt/internal/data/a", "same.bin", 123, 99),
        )

    def test_filter_existing_file_batch_keeps_same_name_in_different_path(self) -> None:
        """Same filename in another directory must remain discoverable."""

        handler = self.make_handler()
        handler._select_raw = lambda sql, params: [
            {
                "path": "/mnt/internal/data/a",
                "name": "same.bin",
                "size": 123,
            }
        ]

        batch = [
            self._Meta("/mnt/internal/data/b", "same.bin", 123),
        ]

        result = handler.filter_existing_file_batch(
            host_id=99,
            batch=batch,
            batch_size=1000,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].NA_PATH, "/mnt/internal/data/b")

    def test_filter_existing_file_batch_keeps_same_path_name_with_larger_size(self) -> None:
        """A file that grew in place must return to backup."""

        handler = self.make_handler()
        handler._select_raw = lambda sql, params: [
            {
                "path": "/mnt/internal/data/a",
                "name": "same.bin",
                "size": 123,
            }
        ]

        batch = [
            self._Meta("/mnt/internal/data/a", "same.bin", 456),
        ]

        result = handler.filter_existing_file_batch(
            host_id=99,
            batch=batch,
            batch_size=1000,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].VL_FILE_SIZE_KB, 456)


class BackupQueueReconcileTests(unittest.TestCase):
    """Validate one-shot reconciliation of pending backup rows already on server."""

    def make_handler(self):
        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler._connect = lambda: None
        handler._disconnect = lambda: None
        handler._summary_publish_host_scope = lambda *args, **kwargs: None
        handler.db_connection = type(
            "FakeConnection",
            (),
            {
                "rollback": lambda self: None,
                "commit": lambda self: None,
                "autocommit": True,
            },
        )()
        handler.in_transaction = False
        return handler

    def test_file_task_list_pending_backup_with_server_artifact_filters_backup_queue(self) -> None:
        """Only pending BACKUP rows with repository artifacts should be listed."""

        handler = self.make_handler()
        captured = {}

        def fake_select_raw(sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return []

        handler._select_raw = fake_select_raw

        rows = handler.file_task_list_pending_backup_with_server_artifact(
            limit=25,
            host_id=88,
        )

        self.assertEqual(rows, [])
        self.assertIn("t.NU_TYPE = %s", captured["sql"])
        self.assertIn("t.NU_STATUS = %s", captured["sql"])
        self.assertIn("h.NA_SERVER_FILE_PATH IS NOT NULL", captured["sql"])
        self.assertIn("h.NA_SERVER_FILE_NAME IS NOT NULL", captured["sql"])
        self.assertIn("AND t.FK_HOST = %s", captured["sql"])
        self.assertEqual(
            captured["params"],
            (
                db_bkp_module.k.FILE_TASK_BACKUP_TYPE,
                db_bkp_module.k.TASK_PENDING,
                88,
                25,
            ),
        )

    def test_file_history_list_inconsistent_server_artifacts_lists_non_running_rows(self) -> None:
        """Reconciliation scan must include stale history or queue rows only."""

        handler = self.make_handler()
        captured = {}

        def fake_select_raw(sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return []

        handler._select_raw = fake_select_raw

        rows = handler.file_history_list_inconsistent_server_artifacts(
            limit=40,
            host_id=55,
        )

        self.assertEqual(rows, [])
        self.assertIn("LEFT JOIN FILE_TASK t", captured["sql"])
        self.assertIn("NOT EXISTS", captured["sql"])
        self.assertIn("t_running.NU_STATUS = %s", captured["sql"])
        self.assertIn("h.DT_PROCESSED IS NULL", captured["sql"])
        self.assertIn("AND h.FK_HOST = %s", captured["sql"])
        self.assertEqual(
            captured["params"],
            (
                db_bkp_module.k.TASK_RUNNING,
                db_bkp_module.k.TASK_DONE,
                db_bkp_module.k.TASK_DONE,
                55,
                40,
            ),
        )

    def test_file_history_list_processed_artifact_reconcile_candidates_joins_dim(self) -> None:
        """Processed reconciliation must read only artifacts already in DIM."""

        handler = self.make_handler()
        captured = {}

        def fake_select_raw(sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return []

        handler._select_raw = fake_select_raw

        rows = handler.file_history_list_processed_artifact_reconcile_candidates(
            limit=30,
            host_id=12,
            after_history_id=900,
        )

        self.assertEqual(rows, [])
        self.assertIn(f"JOIN {db_bkp_module.k.RFM_DATABASE_NAME}.DIM_SPECTRUM_FILE d", captured["sql"])
        self.assertIn("d.NA_VOLUME = %s", captured["sql"])
        self.assertIn("h.ID_HISTORY > %s", captured["sql"])
        self.assertEqual(
            captured["params"],
            (
                db_bkp_module.k.REPO_VOLUME_NAME.lower(),
                db_bkp_module.k.TASK_RUNNING,
                db_bkp_module.k.TASK_DONE,
                db_bkp_module.k.TASK_DONE,
                12,
                900,
                30,
            ),
        )

    def test_file_history_list_queue_reconcile_candidates_excludes_dim(self) -> None:
        """Queue reconciliation must skip artifacts already proven in DIM."""

        handler = self.make_handler()
        captured = {}

        def fake_select_raw(sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return []

        handler._select_raw = fake_select_raw

        rows = handler.file_history_list_queue_reconcile_candidates(
            limit=35,
            host_id=13,
            after_history_id=901,
        )

        self.assertEqual(rows, [])
        self.assertIn("JOIN FILE_TASK t", captured["sql"])
        self.assertIn(f"FROM {db_bkp_module.k.RFM_DATABASE_NAME}.DIM_SPECTRUM_FILE d", captured["sql"])
        self.assertIn("t.NU_TYPE <> %s", captured["sql"])
        self.assertIn("h.DT_PROCESSED IS NOT NULL", captured["sql"])
        self.assertEqual(
            captured["params"],
            (
                db_bkp_module.k.TASK_RUNNING,
                db_bkp_module.k.REPO_VOLUME_NAME.lower(),
                db_bkp_module.k.FILE_TASK_PROCESS_TYPE,
                db_bkp_module.k.TASK_PENDING,
                db_bkp_module.k.TASK_DONE,
                db_bkp_module.k.TASK_PENDING,
                13,
                901,
                35,
            ),
        )

    def test_file_task_promote_pending_backup_to_processing_updates_queue_and_history(self) -> None:
        """Promotion must move the queue row to PROCESS and mark backup done."""

        handler = self.make_handler()
        calls = []
        task_updates = []
        history_updates = []

        handler.begin_transaction = lambda: calls.append("begin")
        handler.commit = lambda: calls.append("commit")
        handler.rollback = lambda: calls.append("rollback")
        handler.file_task_update = lambda **kwargs: task_updates.append(kwargs) or {
            "rows_affected": 1
        }
        handler.file_history_update = lambda **kwargs: history_updates.append(kwargs) or {
            "rows_affected": 1
        }

        reconciled_at = datetime(2026, 6, 19, 12, 0, 0)
        candidate = {
            "ID_FILE_TASK": 501,
            "FK_HOST": 77,
            "NA_HOST_FILE_PATH": "/mnt/internal/data",
            "NA_HOST_FILE_NAME": "sample.bin",
            "NA_SERVER_FILE_PATH": "/mnt/reposfi/tmp/RFEYE001",
            "NA_SERVER_FILE_NAME": "p-123--sample.bin",
            "NA_EXTENSION_SERVER": ".bin",
            "VL_FILE_SIZE_KB_SERVER": 42,
            "DT_FILE_CREATED_SERVER": datetime(2026, 6, 18, 10, 0, 0),
            "DT_FILE_MODIFIED_SERVER": datetime(2026, 6, 18, 10, 0, 0),
            "DT_BACKUP": None,
        }

        result = handler.file_task_promote_pending_backup_to_processing(
            candidate=candidate,
            reconciled_at=reconciled_at,
        )

        self.assertEqual(calls, ["begin", "commit"])
        self.assertEqual(result["task_id"], 501)
        self.assertEqual(result["host_id"], 77)
        self.assertEqual(task_updates[0]["task_id"], 501)
        self.assertEqual(task_updates[0]["NU_TYPE"], db_bkp_module.k.FILE_TASK_PROCESS_TYPE)
        self.assertEqual(task_updates[0]["NU_STATUS"], db_bkp_module.k.TASK_PENDING)
        self.assertEqual(task_updates[0]["NA_SERVER_FILE_NAME"], "p-123--sample.bin")
        self.assertEqual(history_updates[0]["host_id"], 77)
        self.assertEqual(history_updates[0]["NU_STATUS_BACKUP"], db_bkp_module.k.TASK_DONE)
        self.assertEqual(
            history_updates[0]["NU_STATUS_PROCESSING"],
            db_bkp_module.k.TASK_PENDING,
        )
        self.assertNotIn("DT_PROCESSED", history_updates[0])
        self.assertEqual(history_updates[0]["DT_BACKUP"], reconciled_at)

    def test_file_task_promote_server_artifact_to_processing_accepts_discovery_row(self) -> None:
        """Repair may jump a rediscovered row straight to PROCESS/PENDING."""

        handler = self.make_handler()
        calls = []
        task_updates = []
        history_updates = []

        handler.begin_transaction = lambda: calls.append("begin")
        handler.commit = lambda: calls.append("commit")
        handler.rollback = lambda: calls.append("rollback")
        handler.file_task_update = lambda **kwargs: task_updates.append(kwargs) or {
            "rows_affected": 1
        }
        handler.file_history_update = lambda **kwargs: history_updates.append(kwargs) or {
            "rows_affected": 1
        }

        candidate = {
            "ID_FILE_TASK": 900,
            "FK_HOST": 88,
            "NA_HOST_FILE_PATH": "/mnt/internal/data",
            "NA_HOST_FILE_NAME": "regressed.bin",
            "NA_SERVER_FILE_PATH": "/mnt/reposfi/tmp/RFEYE900",
            "NA_SERVER_FILE_NAME": "p-900--regressed.bin",
            "NA_EXTENSION_SERVER": ".bin",
            "VL_FILE_SIZE_KB_SERVER": 99,
            "DT_FILE_CREATED_SERVER": datetime(2026, 6, 10, 10, 0, 0),
            "DT_FILE_MODIFIED_SERVER": datetime(2026, 6, 10, 10, 0, 0),
            "DT_BACKUP": datetime(2026, 6, 10, 10, 0, 0),
            "FILE_TASK_STATUS": db_bkp_module.k.TASK_DONE,
        }

        handler.file_task_promote_server_artifact_to_processing(
            candidate=candidate,
            reconciled_at=datetime(2026, 6, 19, 12, 0, 0),
        )

        self.assertEqual(calls, ["begin", "commit"])
        self.assertEqual(task_updates[0]["expected_status"], db_bkp_module.k.TASK_DONE)
        self.assertEqual(task_updates[0]["NU_TYPE"], db_bkp_module.k.FILE_TASK_PROCESS_TYPE)
        self.assertEqual(history_updates[0]["NU_STATUS_BACKUP"], db_bkp_module.k.TASK_DONE)
        self.assertEqual(
            history_updates[0]["NU_STATUS_PROCESSING"],
            db_bkp_module.k.TASK_PENDING,
        )

    def test_file_task_promote_pending_backup_to_processing_rolls_back_on_failure(self) -> None:
        """Any failed promotion step must roll the transaction back."""

        handler = self.make_handler()
        calls = []

        handler.begin_transaction = lambda: calls.append("begin")
        handler.commit = lambda: calls.append("commit")
        handler.rollback = lambda: calls.append("rollback")
        handler.file_task_update = lambda **kwargs: {"rows_affected": 0}
        handler.file_history_update = lambda **kwargs: {"rows_affected": 1}

        candidate = {
            "ID_FILE_TASK": 502,
            "FK_HOST": 78,
            "NA_HOST_FILE_PATH": "/mnt/internal/data",
            "NA_HOST_FILE_NAME": "sample.bin",
            "NA_SERVER_FILE_PATH": "/mnt/reposfi/tmp/RFEYE002",
            "NA_SERVER_FILE_NAME": "p-456--sample.bin",
            "NA_EXTENSION_SERVER": ".bin",
            "VL_FILE_SIZE_KB_SERVER": 84,
            "DT_FILE_CREATED_SERVER": datetime(2026, 6, 18, 10, 0, 0),
            "DT_FILE_MODIFIED_SERVER": datetime(2026, 6, 18, 10, 0, 0),
            "DT_BACKUP": None,
        }

        with self.assertRaises(RuntimeError):
            handler.file_task_promote_pending_backup_to_processing(
                candidate=candidate,
                reconciled_at=datetime(2026, 6, 19, 12, 0, 0),
            )

        self.assertEqual(calls, ["begin", "rollback"])

    def test_file_history_list_processing_retry_candidates_by_error_detail_filters_missing_queue(self) -> None:
        """Retry candidates must target errored history rows without live queue."""

        handler = self.make_handler()
        captured = {}

        def fake_select(sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return []

        handler._connect = lambda: None
        handler._disconnect = lambda: None
        handler._select_raw = fake_select

        handler.file_history_list_processing_retry_candidates_by_error_detail(
            error_detail=db_bkp_module.k.APP_ANALISE_NO_READABLE_FILES_IN_ZIP_DETAIL,
            limit=25,
            host_id=901,
            after_history_id=40,
        )

        self.assertIn("LEFT JOIN FILE_TASK t", captured["sql"])
        self.assertIn("h.NA_ERROR_DETAIL = %s", captured["sql"])
        self.assertIn("h.NU_STATUS_PROCESSING = %s", captured["sql"])
        self.assertIn("t.ID_FILE_TASK IS NULL", captured["sql"])
        self.assertEqual(
            captured["params"],
            (
                db_bkp_module.k.APP_ANALISE_NO_READABLE_FILES_IN_ZIP_DETAIL,
                db_bkp_module.k.TASK_ERROR,
                901,
                40,
                25,
            ),
        )

    def test_file_history_recreate_processing_task_upserts_queue_and_resets_history(self) -> None:
        """Recreation must restore PROCESS/PENDING atomically."""

        handler = self.make_handler()
        calls = []
        upserts = []
        history_updates = []
        summaries = []

        handler.begin_transaction = lambda: calls.append("begin")
        handler.commit = lambda: calls.append("commit")
        handler.rollback = lambda: calls.append("rollback")
        handler._upsert_row = lambda **kwargs: upserts.append(kwargs) or 1
        handler.file_history_update = lambda **kwargs: history_updates.append(kwargs) or {
            "rows_affected": 1
        }
        handler._summary_publish_host_scope = (
            lambda host_id, reason: summaries.append((host_id, reason))
        )

        recreated_at = datetime(2026, 7, 2, 12, 0, 0)
        candidate = {
            "ID_HISTORY": 700,
            "FK_HOST": 55,
            "NA_HOST_FILE_PATH": "/mnt/internal/data",
            "NA_HOST_FILE_NAME": "sample.zip",
            "NA_EXTENSION_HOST": ".zip",
            "VL_FILE_SIZE_KB_HOST": 12,
            "DT_FILE_CREATED_HOST": datetime(2026, 6, 10, 10, 0, 0),
            "DT_FILE_MODIFIED_HOST": datetime(2026, 6, 10, 10, 0, 0),
            "NA_SERVER_FILE_PATH": "/mnt/reposfi/trash",
            "NA_SERVER_FILE_NAME": "sample.zip",
            "NA_EXTENSION_SERVER": ".zip",
            "VL_FILE_SIZE_KB_SERVER": 12,
            "DT_FILE_CREATED_SERVER": datetime(2026, 6, 10, 10, 0, 0),
            "DT_FILE_MODIFIED_SERVER": datetime(2026, 6, 10, 10, 0, 0),
            "DT_BACKUP": None,
        }

        result = handler.file_history_recreate_processing_task(
            candidate=candidate,
            recreated_at=recreated_at,
        )

        self.assertEqual(calls, ["begin", "commit"])
        self.assertEqual(result["history_id"], 700)
        self.assertEqual(result["host_id"], 55)
        self.assertEqual(upserts[0]["table"], "FILE_TASK")
        self.assertEqual(upserts[0]["data"]["NU_TYPE"], db_bkp_module.k.FILE_TASK_PROCESS_TYPE)
        self.assertEqual(upserts[0]["data"]["NU_STATUS"], db_bkp_module.k.TASK_PENDING)
        self.assertEqual(upserts[0]["data"]["NA_SERVER_FILE_NAME"], "sample.zip")
        self.assertEqual(history_updates[0]["history_id"], 700)
        self.assertEqual(history_updates[0]["NU_STATUS_BACKUP"], db_bkp_module.k.TASK_DONE)
        self.assertEqual(
            history_updates[0]["NU_STATUS_PROCESSING"],
            db_bkp_module.k.TASK_PENDING,
        )
        self.assertNotIn("DT_PROCESSED", history_updates[0])
        self.assertEqual(history_updates[0]["DT_BACKUP"], recreated_at)
        self.assertEqual(
            summaries,
            [(55, "file_history_recreate_processing_task")],
        )

    def test_file_history_reconcile_processed_artifact_restores_done_and_deletes_queue(self) -> None:
        """Processed DIM evidence must restore DONE state and clear live queue."""

        handler = self.make_handler()
        calls = []
        history_updates = []
        deleted_tasks = []

        handler.begin_transaction = lambda: calls.append("begin")
        handler.commit = lambda: calls.append("commit")
        handler.rollback = lambda: calls.append("rollback")
        handler.file_history_update = lambda **kwargs: history_updates.append(kwargs) or {
            "rows_affected": 1
        }
        handler.file_task_delete = lambda task_id: deleted_tasks.append(task_id) or 1

        candidate = {
            "ID_HISTORY": 1200,
            "ID_FILE_TASK": 300,
            "FK_HOST": 66,
            "NA_HOST_FILE_PATH": "/mnt/internal/data",
            "NA_HOST_FILE_NAME": "processed.bin",
            "NA_SERVER_FILE_PATH": "/mnt/reposfi/2026/PE/x",
            "NA_SERVER_FILE_NAME": "p-1200--processed.bin",
            "NA_EXTENSION_SERVER": ".bin",
            "VL_FILE_SIZE_KB_SERVER": 123,
            "DT_FILE_CREATED_SERVER": datetime(2026, 6, 5, 8, 0, 0),
            "DT_FILE_MODIFIED_SERVER": datetime(2026, 6, 5, 8, 0, 0),
            "DT_BACKUP": None,
            "DT_PROCESSED": None,
        }
        repository_artifact = {
            "na_path": "/mnt/reposfi/2026/PE/x",
            "na_file": "p-1200--processed.bin",
            "NA_EXTENSION": ".bin",
            "VL_FILE_SIZE_KB": 123,
            "DT_FILE_CREATED": datetime(2026, 6, 5, 8, 0, 0),
            "DT_FILE_MODIFIED": datetime(2026, 6, 5, 8, 0, 0),
            "DT_FILE_LOGGED": datetime(2026, 6, 5, 8, 30, 0),
        }

        result = handler.file_history_reconcile_processed_artifact(
            candidate=candidate,
            repository_artifact=repository_artifact,
            reconciled_at=datetime(2026, 6, 19, 12, 0, 0),
        )

        self.assertEqual(calls, ["begin", "commit"])
        self.assertEqual(result["history_id"], 1200)
        self.assertEqual(result["deleted_task_rows"], 1)
        self.assertEqual(deleted_tasks, [300])
        self.assertEqual(history_updates[0]["history_id"], 1200)
        self.assertEqual(history_updates[0]["NU_STATUS_BACKUP"], db_bkp_module.k.TASK_DONE)
        self.assertEqual(
            history_updates[0]["NU_STATUS_PROCESSING"],
            db_bkp_module.k.TASK_DONE,
        )
        self.assertEqual(
            history_updates[0]["DT_PROCESSED"],
            repository_artifact["DT_FILE_LOGGED"],
        )


class GarbageCollectorQueryTests(unittest.TestCase):
    """Validate the history query that feeds payload garbage collection."""

    def make_handler(self):
        """Build a handler with only the SELECT path required by this group."""

        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler._connect = lambda: None
        return handler

    def test_file_history_get_gc_candidates_uses_quarantine_anchor(self) -> None:
        handler = self.make_handler()
        captured = {}

        def fake_select_rows(*, table, where, order_by, limit, cols):
            captured["table"] = table
            captured["where"] = where
            captured["order_by"] = order_by
            captured["limit"] = limit
            captured["cols"] = cols
            return []

        handler._select_rows = fake_select_rows

        rows = handler.file_history_get_gc_candidates(
            batch_size=25,
            quarantine_days=365,
        )

        self.assertEqual(rows, [])
        self.assertEqual(captured["table"], "FILE_TASK_HISTORY")
        self.assertEqual(captured["where"]["NU_STATUS_PROCESSING"], -1)
        self.assertEqual(captured["where"]["IS_PAYLOAD_DELETED"], 0)
        self.assertIn(
            "COALESCE(DT_PROCESSED, DT_FILE_CREATED_SERVER)",
            captured["where"]["#CUSTOM#QUARANTINE"],
        )
        self.assertEqual(
            captured["order_by"],
            "COALESCE(DT_PROCESSED, DT_FILE_CREATED_SERVER), ID_HISTORY",
        )
        self.assertEqual(captured["limit"], 25)
        self.assertEqual(
            captured["cols"],
            ["ID_HISTORY", "NA_SERVER_FILE_PATH", "NA_SERVER_FILE_NAME"],
        )

    def test_file_history_list_zip_error_server_restore_candidates_filters_zip_processing_errors(self) -> None:
        """Restoration query must target `.zip` history rows in processing error."""

        handler = self.make_handler()
        captured = {}

        def fake_select(sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return []

        handler._disconnect = lambda: None
        handler._select_raw = fake_select

        rows = handler.file_history_list_zip_error_server_restore_candidates(
            limit=40,
            host_id=901,
            after_history_id=300,
        )

        self.assertEqual(rows, [])
        self.assertIn("LOWER(COALESCE(h.NA_EXTENSION_HOST, '')) = '.zip'", captured["sql"])
        self.assertIn("h.NU_STATUS_PROCESSING = %s", captured["sql"])
        self.assertEqual(
            captured["params"],
            (
                db_bkp_module.k.TASK_ERROR,
                901,
                300,
                40,
            ),
        )

    def test_file_history_restore_server_artifact_metadata_updates_server_columns(self) -> None:
        """Restoration helper must forward only the recovered server metadata."""

        handler = self.make_handler()
        updates = []

        handler.file_history_update = lambda **kwargs: updates.append(kwargs) or {
            "rows_affected": 1
        }

        artifact = {
            "file_path": "/mnt/reposfi/trash/resolved_files",
            "file_name": "sample.zip",
            "extension": ".zip",
            "size_kb": 44,
            "dt_created": datetime(2026, 7, 3, 10, 0, 0),
            "dt_modified": datetime(2026, 7, 3, 10, 1, 0),
        }

        result = handler.file_history_restore_server_artifact_metadata(
            history_id=777,
            repository_artifact=artifact,
        )

        self.assertEqual(result["rows_affected"], 1)
        self.assertEqual(
            updates,
            [
                {
                    "history_id": 777,
                    "NA_SERVER_FILE_PATH": "/mnt/reposfi/trash/resolved_files",
                    "NA_SERVER_FILE_NAME": "sample.zip",
                    "NA_EXTENSION_SERVER": ".zip",
                    "VL_FILE_SIZE_KB_SERVER": 44,
                    "DT_FILE_CREATED_SERVER": datetime(2026, 7, 3, 10, 0, 0),
                    "DT_FILE_MODIFIED_SERVER": datetime(2026, 7, 3, 10, 1, 0),
                }
            ],
        )


class FileTaskSelectionTests(unittest.TestCase):
    """Validate backup FILE_TASK selection rules for priority and host fairness."""

    def make_handler(self):
        """Build a handler wired only for FILE_TASK selection tests."""

        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler._connect = lambda: None
        handler._disconnect = lambda: None
        handler._release_expired_transient_busy_cooldowns = lambda: None
        handler._summary_publish_host_scope = lambda *args, **kwargs: None
        return handler

    def test_read_file_task_backup_can_reserve_hosts_for_discovery_and_fair_by_host(self) -> None:
        """Backup selection should respect discovery reservations and rotate by host."""

        handler = self.make_handler()
        captured = {}

        def fake_select_custom(**kwargs):
            captured.update(kwargs)
            return []

        handler._select_custom = fake_select_custom

        result = handler.read_file_task(
            task_status=db_bkp_module.k.TASK_PENDING,
            task_type=db_bkp_module.k.FILE_TASK_BACKUP_TYPE,
            check_host_busy=True,
            check_host_offline=True,
            lock_host=True,
            reserve_hosts_for_discovery=True,
            fair_by_host=True,
        )

        self.assertIsNone(result)
        self.assertEqual(captured["table"], "FILE_TASK FT")
        self.assertEqual(captured["order_by"], (
            "CASE WHEN H.DT_LAST_BACKUP IS NULL THEN 0 ELSE 1 END ASC, "
            "H.DT_LAST_BACKUP ASC, FT.ID_FILE_TASK ASC"
        ))
        self.assertIn("#CUSTOM#host_discovery_reservation", captured["where"])
        self.assertIn("#CUSTOM#backup_host_round_robin", captured["where"])
        self.assertIn(
            f"HT_BLOCK.NU_TYPE IN ({db_bkp_module.k.HOST_TASK_CHECK_TYPE}, {db_bkp_module.k.HOST_TASK_PROCESSING_TYPE})",
            captured["where"]["#CUSTOM#host_discovery_reservation"],
        )
        self.assertIn(
            f"DATE_SUB(NOW(), INTERVAL {db_bkp_module.k.DISCOVERY_RESERVATION_TTL_SEC} SECOND)",
            captured["where"]["#CUSTOM#host_discovery_reservation"],
        )
        self.assertIn(
            "SELECT MIN(FT_HOST.ID_FILE_TASK)",
            captured["where"]["#CUSTOM#backup_host_round_robin"],
        )

    def test_read_file_task_default_selection_keeps_global_task_order(self) -> None:
        """Without the new flags, FILE_TASK selection should keep the legacy ordering."""

        handler = self.make_handler()
        captured = {}

        def fake_select_custom(**kwargs):
            captured.update(kwargs)
            return []

        handler._select_custom = fake_select_custom

        result = handler.read_file_task(
            task_status=db_bkp_module.k.TASK_PENDING,
            task_type=db_bkp_module.k.FILE_TASK_BACKUP_TYPE,
            check_host_busy=True,
            check_host_offline=True,
            lock_host=True,
        )

        self.assertIsNone(result)
        self.assertEqual(captured["order_by"], "FT.ID_FILE_TASK ASC")


class BacklogBudgetTests(unittest.TestCase):
    """Validate budget-limited backlog promotion without a real database."""

    def make_handler(self):
        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler._connect = lambda: None
        handler._disconnect = lambda: None
        handler._summary_publish_host_scope = lambda *args, **kwargs: None
        handler.db_connection = type(
            "FakeConnection",
            (),
            {"rollback": lambda self: None},
        )()
        return handler

    def test_update_backlog_by_filter_respects_max_total_gb_prefix_selection(self) -> None:
        handler = self.make_handler()
        captured = {}

        handler._select_rows = lambda **kwargs: [
            {"ID_FILE_TASK": 101, "VL_FILE_SIZE_KB_HOST": 20480},
            {"ID_FILE_TASK": 102, "VL_FILE_SIZE_KB_HOST": 15360},
            {"ID_FILE_TASK": 103, "VL_FILE_SIZE_KB_HOST": 10240},
        ]

        def fake_update_row(*, table, data, where, commit, extra_sql=""):
            captured["table"] = table
            captured["data"] = data
            captured["where"] = where
            captured["commit"] = commit
            captured["extra_sql"] = extra_sql
            return 1

        handler._update_row = fake_update_row

        summary = handler.update_backlog_by_filter(
            host_id=33,
            task_filter={
                "mode": "RANGE",
                "start_date": "2025-01-01",
                "end_date": "2025-12-31",
                "file_path": "/mnt/internal",
                "extension": ".zip",
                "max_total_gb": 0.03,
                "sort_order": "newest_first",
            },
            search_type=db_bkp_module.k.FILE_TASK_DISCOVERY,
            search_status=db_bkp_module.k.TASK_DONE,
            new_type=db_bkp_module.k.FILE_TASK_BACKUP_TYPE,
            new_status=db_bkp_module.k.TASK_PENDING,
        )

        self.assertEqual(summary["rows_updated"], 1)
        self.assertEqual(summary["moved_to_backup"], 1)
        self.assertEqual(summary["selected_total_kb"], 20480)
        self.assertEqual(captured["table"], "FILE_TASK")
        self.assertEqual(captured["where"]["FK_HOST"], 33)
        self.assertEqual(captured["where"]["ID_FILE_TASK__in"], [101])
        self.assertEqual(captured["where"]["NU_TYPE"], db_bkp_module.k.FILE_TASK_DISCOVERY)
        self.assertEqual(captured["where"]["NU_STATUS"], db_bkp_module.k.TASK_DONE)
        self.assertEqual(captured["commit"], True)
        self.assertEqual(captured["extra_sql"], "")
        self.assertFalse(
            any(key.startswith("#CUSTOM#") for key in captured["where"])
        )


class HostTaskQueueTests(unittest.TestCase):
    """Validate singleton-per-type HOST_TASK reuse semantics."""

    def make_handler(self):
        """Build a handler with only the queueing helpers these tests exercise."""

        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler.host_update_statistics = lambda host_id: None
        handler.host_read_status = lambda host_id: {"ID_HOST": host_id}
        return handler

    def test_queue_host_task_refreshes_pending_operational_task_for_host(self) -> None:
        """A queued CHECK should refresh the existing pending CHECK row."""

        handler = self.make_handler()
        created = []
        updated = []
        requested_types = []
        filter_dict = {"file_path": "/mnt/internal/inbox", "extensions": [".bin"]}

        def fake_check_host_task(**kwargs):
            requested_types.append(kwargs["NU_TYPE"])
            return [
                {
                    "HOST_TASK__ID_HOST_TASK": 101,
                    "HOST_TASK__NU_TYPE": db_bkp_module.k.HOST_TASK_CHECK_TYPE,
                    "HOST_TASK__NU_STATUS": db_bkp_module.k.TASK_PENDING,
                    "HOST_TASK__FILTER": json.dumps({"file_path": "/mnt/internal/data"}),
                }
            ]

        handler.check_host_task = fake_check_host_task
        handler.host_task_create = lambda **kwargs: created.append(kwargs)
        handler.host_task_update = lambda **kwargs: updated.append(kwargs)

        result = handler.queue_host_task(
            host_id=55,
            task_type=db_bkp_module.k.HOST_TASK_CHECK_TYPE,
            task_status=db_bkp_module.k.TASK_PENDING,
            filter_dict=filter_dict,
        )

        # The current contract is exact-type reuse: one durable CHECK row, one
        # durable PROCESSING row, and so on.
        self.assertEqual(requested_types, [db_bkp_module.k.HOST_TASK_CHECK_TYPE])
        self.assertEqual(created, [])
        self.assertEqual(len(updated), 1)
        self.assertEqual(updated[0]["task_id"], 101)
        self.assertEqual(updated[0]["FILTER"], filter_dict)
        self.assertEqual(updated[0]["NU_TYPE"], db_bkp_module.k.HOST_TASK_CHECK_TYPE)
        self.assertEqual(updated[0]["NU_STATUS"], db_bkp_module.k.TASK_PENDING)
        self.assertIsInstance(updated[0]["DT_HOST_TASK"], datetime)
        self.assertEqual(result, {"ID_HOST": 55})

    def test_queue_host_task_preserves_running_operational_task(self) -> None:
        """A running singleton row must not have its live filter overwritten."""

        handler = self.make_handler()
        created = []
        updated = []

        handler.check_host_task = lambda **kwargs: [
            {
                "HOST_TASK__ID_HOST_TASK": 102,
                "HOST_TASK__NU_TYPE": db_bkp_module.k.HOST_TASK_CHECK_TYPE,
                "HOST_TASK__NU_STATUS": db_bkp_module.k.TASK_RUNNING,
                "HOST_TASK__FILTER": '{"file_path":"/mnt/internal/data"}',
            }
        ]
        handler.host_task_create = lambda **kwargs: created.append(kwargs)
        handler.host_task_update = lambda **kwargs: updated.append(kwargs)

        handler.queue_host_task(
            host_id=56,
            task_type=db_bkp_module.k.HOST_TASK_CHECK_TYPE,
            task_status=db_bkp_module.k.TASK_PENDING,
            filter_dict={"file_path": "/mnt/internal/inbox"},
        )

        self.assertEqual(created, [])
        self.assertEqual(updated, [])
        self.assertTrue(
            any("db_running_singleton_preserved" in msg for msg in handler.log.warnings)
        )

    def test_queue_host_task_matches_non_operational_filter_semantically(self) -> None:
        """Equivalent FILTER payloads must still match semantically for non-operational tasks."""

        handler = self.make_handler()
        created = []
        updated = []

        handler.check_host_task = lambda **kwargs: [
            {
                "HOST_TASK__ID_HOST_TASK": 104,
                "HOST_TASK__NU_TYPE": db_bkp_module.k.HOST_TASK_CHECK_CONNECTION_TYPE,
                "HOST_TASK__NU_STATUS": db_bkp_module.k.TASK_ERROR,
                "HOST_TASK__FILTER": '{"b":2,"a":1}',
            }
        ]
        handler.host_task_create = lambda **kwargs: created.append(kwargs)
        handler.host_task_update = lambda **kwargs: updated.append(kwargs)

        handler.queue_host_task(
            host_id=59,
            task_type=db_bkp_module.k.HOST_TASK_CHECK_CONNECTION_TYPE,
            task_status=db_bkp_module.k.TASK_PENDING,
            filter_dict={"a": 1, "b": 2},
        )

        self.assertEqual(created, [])
        self.assertEqual(len(updated), 1)
        self.assertEqual(updated[0]["task_id"], 104)

    def test_queue_host_task_refreshes_terminal_match_in_place(self) -> None:
        """A terminal singleton row should be refreshed instead of duplicated."""

        handler = self.make_handler()
        created = []
        updated = []

        handler.check_host_task = lambda **kwargs: [
            {
                "HOST_TASK__ID_HOST_TASK": 103,
                "HOST_TASK__NU_TYPE": db_bkp_module.k.HOST_TASK_CHECK_TYPE,
                "HOST_TASK__NU_STATUS": db_bkp_module.k.TASK_ERROR,
                "HOST_TASK__FILTER": {"file_path": "/mnt/internal/data"},
            }
        ]
        handler.host_task_create = lambda **kwargs: created.append(kwargs)
        handler.host_task_update = lambda **kwargs: updated.append(kwargs)

        handler.queue_host_task(
            host_id=57,
            task_type=db_bkp_module.k.HOST_TASK_CHECK_TYPE,
            task_status=db_bkp_module.k.TASK_PENDING,
            filter_dict={"file_path": "/mnt/internal/data"},
        )

        self.assertEqual(created, [])
        self.assertEqual(len(updated), 1)
        self.assertEqual(updated[0]["task_id"], 103)
        self.assertEqual(updated[0]["FILTER"], {"file_path": "/mnt/internal/data"})
        self.assertEqual(updated[0]["NU_TYPE"], db_bkp_module.k.HOST_TASK_CHECK_TYPE)
        self.assertEqual(updated[0]["NU_STATUS"], db_bkp_module.k.TASK_PENDING)

    def test_queue_host_task_warns_when_multiple_operational_matches_exist(self) -> None:
        """Multiple rows of the same type should emit a warning and refresh one."""

        handler = self.make_handler()
        created = []
        updated = []

        handler.check_host_task = lambda **kwargs: [
            {
                "HOST_TASK__ID_HOST_TASK": 201,
                "HOST_TASK__NU_TYPE": db_bkp_module.k.HOST_TASK_PROCESSING_TYPE,
                "HOST_TASK__NU_STATUS": db_bkp_module.k.TASK_PENDING,
                "HOST_TASK__FILTER": {"file_path": "/mnt/internal/data"},
            },
            {
                "HOST_TASK__ID_HOST_TASK": 200,
                "HOST_TASK__NU_TYPE": db_bkp_module.k.HOST_TASK_PROCESSING_TYPE,
                "HOST_TASK__NU_STATUS": db_bkp_module.k.TASK_ERROR,
                "HOST_TASK__FILTER": '{"file_path":"/mnt/internal/data"}',
            },
        ]
        handler.host_task_create = lambda **kwargs: created.append(kwargs)
        handler.host_task_update = lambda **kwargs: updated.append(kwargs)

        handler.queue_host_task(
            host_id=58,
            task_type=db_bkp_module.k.HOST_TASK_PROCESSING_TYPE,
            task_status=db_bkp_module.k.TASK_PENDING,
            filter_dict={"file_path": "/mnt/internal/data"},
        )

        self.assertEqual(created, [])
        self.assertEqual(len(updated), 1)
        self.assertEqual(updated[0]["task_id"], 201)
        self.assertTrue(
            any("db_duplicate_singleton_rows" in msg for msg in handler.log.warnings)
        )

    def test_host_task_update_serializes_filter_canonically(self) -> None:
        """FILTER updates should persist canonical JSON instead of raw dict objects."""

        handler = self.make_handler()
        captured = {}
        handler.db_connection = type(
            "FakeConnection",
            (),
            {"rollback": lambda self: None},
        )()

        def fake_update_row(*, table, data, where, commit):
            captured["table"] = table
            captured["data"] = data
            captured["where"] = where
            captured["commit"] = commit
            return 1

        handler._update_row = fake_update_row

        handler.host_task_update(
            task_id=105,
            FILTER={"b": 2, "a": 1},
        )

        self.assertEqual(captured["table"], "HOST_TASK")
        self.assertEqual(captured["where"], {"ID_HOST_TASK": 105})
        self.assertEqual(captured["data"]["FILTER"], '{"a": 1, "b": 2}')

    def test_host_task_update_canonicalizes_persisted_error_message(self) -> None:
        """HOST_TASK error messages should be compacted before persistence."""

        handler = self.make_handler()
        captured = {}
        handler.db_connection = type(
            "FakeConnection",
            (),
            {"rollback": lambda self: None},
        )()

        def fake_update_row(*, table, data, where, commit):
            captured["table"] = table
            captured["data"] = data
            captured["where"] = where
            captured["commit"] = commit
            return 1

        handler._update_row = fake_update_row

        handler.host_task_update(
            task_id=106,
            NA_MESSAGE=(
                "Host Check Error | [ERROR] [stage=CONNECTIVITY] "
                "[type=TimeoutError] [code=CONNECTIVITY_CHECK_FAILED] "
                "Connectivity test failed [host_id=88] [task_id=106]"
            ),
        )

        self.assertEqual(captured["table"], "HOST_TASK")
        self.assertEqual(captured["where"], {"ID_HOST_TASK": 106})
        self.assertEqual(
            captured["data"]["NA_MESSAGE"],
            "Host Check Error | [ERROR] [stage=CONNECTIVITY] "
            "[code=CONNECTIVITY_CHECK_FAILED] Connectivity test failed",
        )


class HostTaskConnectivityLifecycleTests(unittest.TestCase):
    """Validate suspend/resume rules for host-dependent HOST_TASK rows."""

    def make_handler(self):
        """Build a minimal handler for suspend/resume lifecycle checks."""

        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler._summary_publish_host_scope = lambda *args, **kwargs: None
        return handler

    def test_host_task_suspend_by_host_excludes_statistics_tasks(self) -> None:
        """Connectivity suspension must skip statistics-only HOST_TASK rows."""

        handler = self.make_handler()
        calls = []

        def fake_update_row(*, table, data, where, commit):
            calls.append(
                {
                    "table": table,
                    "data": data,
                    "where": where,
                    "commit": commit,
                }
            )
            return 1

        handler._update_row = fake_update_row

        handler.host_task_suspend_by_host(host_id=77)

        expected_types = (
            db_bkp_module.k.HOST_TASK_CHECK_TYPE,
            db_bkp_module.k.HOST_TASK_PROCESSING_TYPE,
            db_bkp_module.k.HOST_TASK_CHECK_CONNECTION_TYPE,
        )

        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0]["table"], "HOST_TASK")
        self.assertEqual(calls[0]["data"]["NU_STATUS"], db_bkp_module.k.TASK_SUSPENDED)
        self.assertEqual(calls[0]["where"]["FK_HOST"], 77)
        self.assertEqual(calls[0]["where"]["NU_TYPE__in"], expected_types)
        self.assertNotIn(
            db_bkp_module.k.HOST_TASK_UPDATE_STATISTICS_TYPE,
            calls[0]["where"]["NU_TYPE__in"],
        )

    def test_host_task_resume_by_host_preserves_task_type_and_excludes_statistics(self) -> None:
        """Connectivity resume must not force every HOST_TASK back to PROCESSING."""

        handler = self.make_handler()
        calls = []

        def fake_update_row(*, table, data, where, commit):
            calls.append(
                {
                    "table": table,
                    "data": data,
                    "where": where,
                    "commit": commit,
                }
            )
            return 1

        handler._update_row = fake_update_row

        handler.host_task_resume_by_host(host_id=88, busy_timeout_seconds=123)

        expected_types = (
            db_bkp_module.k.HOST_TASK_CHECK_TYPE,
            db_bkp_module.k.HOST_TASK_PROCESSING_TYPE,
            db_bkp_module.k.HOST_TASK_CHECK_CONNECTION_TYPE,
        )

        self.assertEqual(len(calls), 3)
        for call in calls:
            self.assertEqual(call["table"], "HOST_TASK")
            self.assertEqual(call["data"]["NU_STATUS"], db_bkp_module.k.TASK_PENDING)
            self.assertIsNone(call["data"]["NU_PID"])
            self.assertNotIn("NU_TYPE", call["data"])
            self.assertEqual(call["where"]["FK_HOST"], 88)
            self.assertEqual(call["where"]["NU_TYPE__in"], expected_types)
            self.assertNotIn(
                db_bkp_module.k.HOST_TASK_UPDATE_STATISTICS_TYPE,
                call["where"]["NU_TYPE__in"],
            )
        self.assertIn("DT_HOST_TASK__lt", calls[2]["where"])

    def test_file_task_resume_by_host_clears_pid_when_requeueing(self) -> None:
        """Connectivity resume must drop stale worker ownership from FILE_TASK."""

        handler = self.make_handler()
        calls = []

        def fake_update_row(*, table, data, where, commit):
            calls.append(
                {
                    "table": table,
                    "data": data,
                    "where": where,
                    "commit": commit,
                }
            )
            return 1

        handler._update_row = fake_update_row

        handler.file_task_resume_by_host(host_id=91, busy_timeout_seconds=321)

        self.assertEqual(len(calls), 3)
        for call in calls:
            self.assertEqual(call["table"], "FILE_TASK")
            self.assertEqual(call["data"]["NU_STATUS"], db_bkp_module.k.TASK_PENDING)
            self.assertIsNone(call["data"]["NU_PID"])
            self.assertEqual(call["where"]["FK_HOST"], 91)
        self.assertIn("DT_FILE_TASK__lt", calls[2]["where"])


if __name__ == "__main__":
    unittest.main()
