"""
Validation tests for `dbHandlerBKP.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/db/test_dbhandler_bkp.py -q

What is covered here:
    - `HOST` locks in transient SFTP cooldown (`NU_PID = 0`) are preserved by
      the normal safe-release path
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
import sqlite3

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


class HostSummaryRefreshRequestTests(unittest.TestCase):
    """Validate summary invalidation after one host state transition."""

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

    def test_request_host_summary_refresh_publishes_host_scope(self) -> None:
        """The operational handler must invalidate only the affected host."""

        handler = self.make_handler()
        summary_scopes = []
        handler._summary_publish_host_scope = lambda host_id, **kwargs: summary_scopes.append(
            {"host_id": host_id, **kwargs}
        )

        handler.host_request_summary_refresh(88, reason="processing_completed")

        self.assertEqual(
            summary_scopes,
            [{"host_id": 88, "reason": "processing_completed"}],
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

        handler.file_task_history_update(
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

        result = handler.file_task_history_filter_existing_batch(
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

        result = handler.file_task_history_filter_existing_batch(
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

        result = handler.file_task_history_filter_existing_batch(
            host_id=99,
            batch=batch,
            batch_size=1000,
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].VL_FILE_SIZE_KB, 456)


class StructuredErrorFieldsTests(unittest.TestCase):
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

    def test_merge_structured_error_fields_keeps_compact_error_payload(self) -> None:
        """Operational rows retain only the concise message, code and detail."""

        handler = self.make_handler()
        payload = handler._merge_structured_error_fields(
            {
                "NA_MESSAGE": (
                    "Processing Error | [ERROR] [stage=DB] "
                    "[code=UNCLASSIFIED] Failed to persist processed spectra batch"
                ),
                "NA_ERROR_DOMAIN": "PROCESSING",
                "NA_ERROR_SUMMARY": "Failed to persist processed spectra batch",
                "NA_ERROR_DETAIL": "Unexpected processing loop failure",
            }
        )

        self.assertEqual(
            payload["NA_MESSAGE"],
            "Failed to persist processed spectra batch",
        )
        self.assertEqual(payload["NA_ERROR_CODE"], "UNCLASSIFIED")
        self.assertEqual(payload["NU_ERROR_CLASSIFIER_VERSION"], 1)
        self.assertEqual(payload["NA_ERROR_DETAIL"], "Unexpected processing loop failure")
        self.assertNotIn("NA_ERROR_DOMAIN", payload)
        self.assertNotIn("NA_ERROR_STAGE", payload)
        self.assertNotIn("NA_ERROR_SUMMARY", payload)

class GarbageCollectorQueryTests(unittest.TestCase):
    """Validate the history query that feeds payload garbage collection."""

    def make_handler(self):
        """Build a handler with only the SELECT path required by this group."""

        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler._connect = lambda: None
        return handler

    def test_file_history_get_gc_candidates_requires_processed_quarantine_anchor(self) -> None:
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

        rows = handler.file_task_history_list_gc_candidates(
            batch_size=25,
            quarantine_days=365,
        )

        self.assertEqual(rows, [])
        self.assertEqual(captured["table"], "FILE_TASK_HISTORY")
        self.assertEqual(captured["where"]["NU_STATUS_PROCESSING"], -1)
        self.assertEqual(captured["where"]["IS_PAYLOAD_DELETED"], 0)
        self.assertEqual(
            captured["where"]["#CUSTOM#QUARANTINE"],
            "DT_PROCESSED IS NOT NULL AND DT_PROCESSED < NOW() - INTERVAL 365 DAY",
        )
        self.assertEqual(
            captured["order_by"],
            "DT_PROCESSED, ID_HISTORY",
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

        result = handler.file_task_read(
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
            "ORDER BY FT_HOST.ID_FILE_TASK ASC LIMIT 1",
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

        result = handler.file_task_read(
            task_status=db_bkp_module.k.TASK_PENDING,
            task_type=db_bkp_module.k.FILE_TASK_BACKUP_TYPE,
            check_host_busy=True,
            check_host_offline=True,
            lock_host=True,
        )

        self.assertIsNone(result)
        self.assertEqual(captured["order_by"], "FT.ID_FILE_TASK ASC")


    def make_priority_queue(self) -> tuple:
        """Build an isolated SQL queue with old, current, future, and undated files.

        Returns:
            tuple: Handler and SQLite connection; closed automatically after each test.
        """
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.create_function("YEAR", 1, lambda value: int(value[:4]) if value else None)
        connection.executescript("""
            CREATE TABLE HOST (ID_HOST INTEGER, DT_LAST_BACKUP TEXT,
                               IS_BUSY INTEGER DEFAULT 0, IS_OFFLINE INTEGER DEFAULT 0);
            CREATE TABLE FILE_TASK (ID_FILE_TASK INTEGER, FK_HOST INTEGER,
                NU_TYPE INTEGER, NU_STATUS INTEGER, DT_FILE_CREATED_HOST TEXT,
                NA_EXTENSION_HOST TEXT);
            INSERT INTO HOST (ID_HOST, DT_LAST_BACKUP) VALUES
                (1, NULL), (2, '2026-01-02'), (3, '2026-01-01');
        """)
        year = datetime.now().year
        connection.executemany("INSERT INTO FILE_TASK VALUES (?, ?, ?, ?, ?, ?)", [
            (1, 1, 1, 1, "2014-01-01", ".bin"),
            (2, 2, 1, 1, "2014-02-01", ".bin"),
            (3, 3, 1, 1, "2014-03-01", ".bin"),
            (4, 2, 1, 1, f"{year}-10-01", ".bin"),
            (5, 3, 1, 1, f"{year}-09-01", ".bin"),
            (6, 2, 1, 1, f"{year}-10-01", ".bin"),
            (7, 1, 1, 1, None, ".bin"),
            (8, 1, 1, 1, f"{year + 1}-01-01", ".bin"),
        ])
        handler = self.make_handler()

        def select_queue(**kwargs: object) -> list[dict]:
            clauses, params = [], []
            for key, value in kwargs["where"].items():
                if key.startswith("#CUSTOM#"):
                    clauses.append(value)
                else:
                    clauses.append(f"{key} = ?")
                    params.append(value)
            sql = ("SELECT FT.ID_FILE_TASK, FT.FK_HOST FROM FILE_TASK FT "
                   "JOIN HOST H ON H.ID_HOST = FT.FK_HOST WHERE "
                   + " AND ".join(clauses))
            if kwargs["order_by"]:
                sql += " ORDER BY " + kwargs["order_by"]
            sql += " LIMIT 1"
            return [{"FILE_TASK__ID_FILE_TASK": row[0], "HOST__ID_HOST": row[1]}
                    for row in connection.execute(sql, params)]

        handler._select_custom = select_queue
        return handler, connection

    def test_backup_current_year_rotates_hosts_before_older_backlog(self) -> None:
        """Current-year hosts rotate even when another host has never backed up."""
        handler, connection = self.make_priority_queue()
        selected = []
        for step in range(3):
            result = handler.file_task_read(
                task_type=1, check_host_busy=False, fair_by_host=True,
                newest_first=True, prefer_current_year=True,
            )
            _, host_id, task_id = result
            selected.append(task_id)
            connection.execute("DELETE FROM FILE_TASK WHERE ID_FILE_TASK = ?", (task_id,))
            connection.execute("UPDATE HOST SET DT_LAST_BACKUP = ? WHERE ID_HOST = ?",
                               (f"2026-12-{step + 1:02d}", host_id))
        self.assertEqual(selected, [5, 6, 4])
        self.assertEqual(handler.file_task_read(
            task_type=1, check_host_busy=False, fair_by_host=True,
            newest_first=True, prefer_current_year=True,
        )[2], 8)

    def test_processing_newest_source_date_wins_over_queue_id(self) -> None:
        """Processing favors source age globally, including deterministic ties and NULLs."""
        handler, connection = self.make_priority_queue()
        connection.execute("UPDATE FILE_TASK SET NU_TYPE = ?",
                           (db_bkp_module.k.FILE_TASK_PROCESS_TYPE,))
        selected = []
        while True:
            result = handler.file_task_read(
                task_type=db_bkp_module.k.FILE_TASK_PROCESS_TYPE,
                check_host_busy=False, newest_first=True,
            )
            if result is None:
                break
            selected.append(result[2])
            connection.execute("DELETE FROM FILE_TASK WHERE ID_FILE_TASK = ?", (result[2],))
        self.assertEqual(selected, [8, 6, 4, 5, 3, 2, 1, 7])

    def test_backup_priority_preserves_host_and_file_eligibility(self) -> None:
        """Busy/offline hosts and files outside the requested slice stay excluded."""
        handler, connection = self.make_priority_queue()
        connection.execute("UPDATE HOST SET IS_OFFLINE = 1 WHERE ID_HOST = 3")
        connection.execute("UPDATE HOST SET IS_BUSY = 1 WHERE ID_HOST = 1")
        connection.execute("UPDATE FILE_TASK SET NU_STATUS = 2 WHERE ID_FILE_TASK = 6")
        connection.execute("UPDATE FILE_TASK SET NA_EXTENSION_HOST = '.zip' WHERE ID_FILE_TASK = 4")
        result = handler.file_task_read(
            task_type=1, check_host_busy=True, check_host_offline=True,
            extension=".bin", fair_by_host=True, newest_first=True,
            prefer_current_year=True,
        )
        self.assertEqual(result[2], 2)

    def test_explicit_task_id_bypasses_priority_ordering(self) -> None:
        """An explicit task lookup still resolves the requested old file."""
        handler, _ = self.make_priority_queue()
        self.assertEqual(handler.file_task_read(
            task_id=1, check_host_busy=False, fair_by_host=True,
            newest_first=True, prefer_current_year=True,
        )[2], 1)


class HostTaskQueueTests(unittest.TestCase):
    """Validate singleton-per-type HOST_TASK reuse semantics."""

    def make_handler(self):
        """Build a handler with only the queueing helpers these tests exercise."""

        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
        handler.database = "BPDATA_TEST"
        handler.host_request_summary_refresh = lambda host_id, **kwargs: None
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

        handler.host_task_list = fake_check_host_task
        handler.host_task_create = lambda **kwargs: created.append(kwargs)
        handler.host_task_update = lambda **kwargs: updated.append(kwargs)

        result = handler.host_task_enqueue(
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

        handler.host_task_list = lambda **kwargs: [
            {
                "HOST_TASK__ID_HOST_TASK": 102,
                "HOST_TASK__NU_TYPE": db_bkp_module.k.HOST_TASK_CHECK_TYPE,
                "HOST_TASK__NU_STATUS": db_bkp_module.k.TASK_RUNNING,
                "HOST_TASK__FILTER": '{"file_path":"/mnt/internal/data"}',
            }
        ]
        handler.host_task_create = lambda **kwargs: created.append(kwargs)
        handler.host_task_update = lambda **kwargs: updated.append(kwargs)

        handler.host_task_enqueue(
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

        handler.host_task_list = lambda **kwargs: [
            {
                "HOST_TASK__ID_HOST_TASK": 104,
                "HOST_TASK__NU_TYPE": db_bkp_module.k.HOST_TASK_CHECK_CONNECTION_TYPE,
                "HOST_TASK__NU_STATUS": db_bkp_module.k.TASK_ERROR,
                "HOST_TASK__FILTER": '{"b":2,"a":1}',
            }
        ]
        handler.host_task_create = lambda **kwargs: created.append(kwargs)
        handler.host_task_update = lambda **kwargs: updated.append(kwargs)

        handler.host_task_enqueue(
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

        handler.host_task_list = lambda **kwargs: [
            {
                "HOST_TASK__ID_HOST_TASK": 103,
                "HOST_TASK__NU_TYPE": db_bkp_module.k.HOST_TASK_CHECK_TYPE,
                "HOST_TASK__NU_STATUS": db_bkp_module.k.TASK_ERROR,
                "HOST_TASK__FILTER": {"file_path": "/mnt/internal/data"},
            }
        ]
        handler.host_task_create = lambda **kwargs: created.append(kwargs)
        handler.host_task_update = lambda **kwargs: updated.append(kwargs)

        handler.host_task_enqueue(
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

        handler.host_task_list = lambda **kwargs: [
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

        handler.host_task_enqueue(
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
