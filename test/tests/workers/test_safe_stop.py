"""
Validation tests for `safe_stop.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_safe_stop.py -q

What is covered here:
    - stale backup `.tmp` leftovers are purged only from the repository TMP area
    - empty per-host TMP directories are pruned after cleanup
"""

from __future__ import annotations

import tempfile
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import (
    APP_ROOT,
    DB_ROOT,
    bind_real_package,
    bind_real_shared_package,
    ensure_app_paths,
    load_module_from_path,
)


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        safe_stop = load_module_from_path(
            "test_safe_stop_module",
            str(APP_ROOT / "safe_stop.py"),
        )


class FakeLog:
    """Capture cleanup events without touching the production logger."""

    def __init__(self) -> None:
        self.entries = []
        self.warnings = []
        self.errors = []

    def entry(self, message: str) -> None:
        self.entries.append(message)

    def warning(self, message: str) -> None:
        self.warnings.append(message)

    def error(self, message: str) -> None:
        self.errors.append(message)


class FakeDB:
    """Small DB double for shutdown-state reconciliation tests."""

    def __init__(self) -> None:
        self.file_task_updates = []
        self.host_task_updates = []
        self.host_updates = []
        self.suspended_hosts = []
        self.connected = False
        self.disconnected = False

    def _connect(self) -> None:
        self.connected = True

    def _disconnect(self) -> None:
        self.disconnected = True

    def _select_rows(self, *, table, where, cols):
        if table == "FILE_TASK" and where == {"NU_STATUS": safe_stop.k.TASK_RUNNING}:
            return [
                {"ID_FILE_TASK": 11, "FK_HOST": 700, "NU_PID": 9991},
                {"ID_FILE_TASK": 12, "FK_HOST": 701, "NU_PID": 9992},
            ]
        if table == "HOST_TASK" and where == {"NU_STATUS": safe_stop.k.TASK_RUNNING}:
            return [
                {"ID_HOST_TASK": 21, "FK_HOST": 700, "NU_PID": 8881},
            ]
        if table == "HOST" and where == {"IS_BUSY": True}:
            return [
                {"ID_HOST": 700, "NU_PID": 9991},
            ]
        if table == "HOST" and where == {"IS_OFFLINE": True}:
            return [
                {"ID_HOST": 700},
            ]
        raise AssertionError(f"Unexpected _select_rows call: {table=} {where=} {cols=}")

    def file_task_update(self, **kwargs) -> None:
        self.file_task_updates.append(kwargs)

    def host_task_update(self, **kwargs) -> None:
        self.host_task_updates.append(kwargs)

    def host_update(self, **kwargs) -> None:
        self.host_updates.append(kwargs)

    def host_task_suspend_by_host(self, host_id: int) -> None:
        self.suspended_hosts.append(("host_task", host_id))

    def file_task_suspend_by_host(self, host_id: int) -> None:
        self.suspended_hosts.append(("file_task", host_id))

    def file_history_suspend_by_host(self, host_id: int) -> None:
        self.suspended_hosts.append(("file_history", host_id))


class SafeStopTmpCleanupTests(unittest.TestCase):
    """Validate shutdown cleanup of repository backup leftovers."""

    def test_cleanup_repository_tmp_files_deletes_only_tmp_files(self) -> None:
        fake_log = FakeLog()

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_tmp_root = Path(tmpdir) / "reposfi" / "tmp"
            host_dir = repo_tmp_root / "CWSM211004"
            host_dir.mkdir(parents=True)

            stale_tmp = host_dir / "payload.zip.tmp"
            keep_zip = host_dir / "payload.zip"
            keep_note = host_dir / "notes.txt"

            stale_tmp.write_text("partial", encoding="utf-8")
            keep_zip.write_text("done", encoding="utf-8")
            keep_note.write_text("keep", encoding="utf-8")

            with patch.object(safe_stop, "log", fake_log):
                deleted = safe_stop.cleanup_repository_tmp_files(
                    repo_tmp_root=str(repo_tmp_root)
                )

            self.assertEqual(deleted, 1)
            self.assertFalse(stale_tmp.exists())
            self.assertTrue(keep_zip.exists())
            self.assertTrue(keep_note.exists())

        self.assertFalse(fake_log.errors)

    def test_prune_empty_repository_tmp_dirs_removes_only_empty_descendants(self) -> None:
        fake_log = FakeLog()

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_tmp_root = Path(tmpdir) / "reposfi" / "tmp"
            empty_host_dir = repo_tmp_root / "CWSM211004"
            active_host_dir = repo_tmp_root / "RFEye002211"
            empty_host_dir.mkdir(parents=True)
            active_host_dir.mkdir(parents=True)

            (empty_host_dir / "stale.bin.tmp").write_text("partial", encoding="utf-8")
            (active_host_dir / "final.bin").write_text("valid", encoding="utf-8")

            with patch.object(safe_stop, "log", fake_log):
                safe_stop.cleanup_repository_tmp_files(repo_tmp_root=str(repo_tmp_root))
                removed = safe_stop.prune_empty_repository_tmp_dirs(
                    repo_tmp_root=str(repo_tmp_root)
                )

            self.assertEqual(removed, 1)
            self.assertFalse(empty_host_dir.exists())
            self.assertTrue(active_host_dir.exists())
            self.assertTrue(repo_tmp_root.exists())
        self.assertFalse(fake_log.errors)


class SafeStopStateCleanupTests(unittest.TestCase):
    """Validate shutdown reconciliation of offline work queues."""

    def test_cleanup_hosts_and_tasks_reasserts_offline_suspension_after_reset(self) -> None:
        fake_log = FakeLog()
        fake_db = FakeDB()

        with patch.object(safe_stop, "log", fake_log):
            with patch.object(safe_stop, "dbHandlerBKP", return_value=fake_db):
                with patch.object(
                    safe_stop,
                    "cleanup_repository_tmp_files",
                    return_value=0,
                ):
                    with patch.object(
                        safe_stop,
                        "prune_empty_repository_tmp_dirs",
                        return_value=0,
                    ):
                        safe_stop.cleanup_hosts_and_tasks()

        self.assertTrue(fake_db.connected)
        self.assertTrue(fake_db.disconnected)
        self.assertEqual(len(fake_db.file_task_updates), 2)
        self.assertEqual(
            [item["NU_STATUS"] for item in fake_db.file_task_updates],
            [safe_stop.k.TASK_PENDING, safe_stop.k.TASK_PENDING],
        )
        self.assertEqual(len(fake_db.host_task_updates), 1)
        self.assertEqual(
            fake_db.host_task_updates[0]["NU_STATUS"],
            safe_stop.k.TASK_PENDING,
        )
        self.assertEqual(
            fake_db.host_updates,
            [{
                "host_id": 700,
                "IS_BUSY": False,
                "NU_PID": safe_stop.k.HOST_UNLOCKED_PID,
                "DT_BUSY": None,
            }],
        )
        self.assertEqual(
            fake_db.suspended_hosts,
            [
                ("host_task", 700),
                ("file_task", 700),
                ("file_history", 700),
            ],
        )
        self.assertFalse(fake_log.errors)


if __name__ == "__main__":
    unittest.main()
