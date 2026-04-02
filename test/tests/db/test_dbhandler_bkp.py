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
        handler._connect = lambda: None
        handler._disconnect = lambda: None
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
        self.assertTrue(
            any("Preserving transient host cooldown" in msg for msg in handler.log.entries)
        )

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
        self.assertTrue(
            any("Preserving transient SFTP cooldown" in msg for msg in handler.log.entries)
        )

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


class GarbageCollectorQueryTests(unittest.TestCase):
    """Validate the history query that feeds payload garbage collection."""

    def make_handler(self):
        """Build a handler with only the SELECT path required by this group."""

        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
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
            "COALESCE(DT_PROCESSED, DT_FILE_CREATED)",
            captured["where"]["#CUSTOM#QUARANTINE"],
        )
        self.assertEqual(
            captured["order_by"],
            "COALESCE(DT_PROCESSED, DT_FILE_CREATED), ID_HISTORY",
        )
        self.assertEqual(captured["limit"], 25)
        self.assertEqual(
            captured["cols"],
            ["ID_HISTORY", "NA_SERVER_FILE_PATH", "NA_SERVER_FILE_NAME"],
        )


class FileTaskSelectionTests(unittest.TestCase):
    """Validate backup FILE_TASK selection rules for priority and host fairness."""

    def make_handler(self):
        """Build a handler wired only for FILE_TASK selection tests."""

        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler._connect = lambda: None
        handler._disconnect = lambda: None
        handler._release_expired_transient_busy_cooldowns = lambda: None
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
        handler._connect = lambda: None
        handler._disconnect = lambda: None
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
            {"ID_FILE_TASK": 101, "VL_FILE_SIZE_KB": 20480},
            {"ID_FILE_TASK": 102, "VL_FILE_SIZE_KB": 15360},
            {"ID_FILE_TASK": 103, "VL_FILE_SIZE_KB": 10240},
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
            any("HOST_TASK already RUNNING" in msg for msg in handler.log.warnings)
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
            any("Multiple HOST_TASK rows matched" in msg for msg in handler.log.warnings)
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


class HostTaskConnectivityLifecycleTests(unittest.TestCase):
    """Validate suspend/resume rules for host-dependent HOST_TASK rows."""

    def make_handler(self):
        """Build a minimal handler for suspend/resume lifecycle checks."""

        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
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
            self.assertNotIn("NU_TYPE", call["data"])
            self.assertEqual(call["where"]["FK_HOST"], 88)
            self.assertEqual(call["where"]["NU_TYPE__in"], expected_types)
            self.assertNotIn(
                db_bkp_module.k.HOST_TASK_UPDATE_STATISTICS_TYPE,
                call["where"]["NU_TYPE__in"],
            )
        self.assertIn("DT_HOST_TASK__lt", calls[2]["where"])


if __name__ == "__main__":
    unittest.main()
