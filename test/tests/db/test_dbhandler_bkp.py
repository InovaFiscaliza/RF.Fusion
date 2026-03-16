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
        handler = object.__new__(db_bkp_module.dbHandlerBKP)
        handler.log = FakeLog()
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


class FileTimestampOwnershipTests(unittest.TestCase):
    """Validate that task/history timestamps are explicit at call sites."""

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


if __name__ == "__main__":
    unittest.main()
