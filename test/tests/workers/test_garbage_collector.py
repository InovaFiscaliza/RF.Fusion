"""Validation tests for repository garbage collection.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest \
      /RFFusion/test/tests/workers/test_garbage_collector.py -q

The suite exercises only temporary folders. It never reads from nor removes
artifacts in the production repository.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import (
    APP_ROOT,
    DB_ROOT,
    SERVER_HANDLER_ROOT,
    bind_real_package,
    bind_real_shared_package,
    ensure_app_paths,
    load_module_from_path,
)


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        with bind_real_package("server_handler", SERVER_HANDLER_ROOT):
            garbage_worker = load_module_from_path(
                "test_garbage_collector_module",
                str(APP_ROOT / "appCataloga_garbage_collector.py"),
            )


gc_maintenance = garbage_worker.gc_maintenance


class FakeLog:
    """Collect GC events without using the operational logger."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def event(self, event_name: str, **fields) -> None:
        self.events.append((event_name, fields))

    def warning_event(self, event_name: str, **fields) -> None:
        self.events.append((event_name, fields))

    def error_event(self, event_name: str, **fields) -> None:
        self.events.append((event_name, fields))


class FakeDbBkp:
    """Minimal FILE_TASK_HISTORY double used by GC contract tests."""

    def __init__(self) -> None:
        self.history_calls: list[dict] = []
        self.history_updates: list[dict] = []

    def file_task_history_list_gc_candidates(
        self,
        *,
        batch_size: int,
        quarantine_days: int,
        trash_root: str,
        resolved_root: str,
    ) -> list[dict]:
        self.history_calls.append(
            {
                "batch_size": batch_size,
                "quarantine_days": quarantine_days,
                "trash_root": trash_root,
                "resolved_root": resolved_root,
            }
        )
        return []

    def file_task_history_update(self, **kwargs) -> None:
        self.history_updates.append(kwargs)


class GarbageCollectorTests(unittest.TestCase):
    """Validate retention, path isolation, and bounded GC batches."""

    def test_history_candidates_require_processed_timestamp(self) -> None:
        handler = object.__new__(garbage_worker.dbHandlerBKP)
        handler.schema = "BPDATA"
        handler._select_raw = Mock(return_value=[])

        handler.file_task_history_list_gc_candidates(
            batch_size=123,
            quarantine_days=365,
            trash_root="/repo/trash",
            resolved_root="/repo/trash/resolved_files",
        )

        _, params = handler._select_raw.call_args.args
        self.assertAlmostEqual(
            params[2],
            datetime.utcnow() - timedelta(days=365),
            delta=timedelta(seconds=5),
        )
        self.assertEqual(params[-1], 123)

    def test_collect_uses_distinct_retention_windows(self) -> None:
        fake_db = FakeDbBkp()
        fake_log = FakeLog()
        resolved_calls: list[dict] = []

        def fake_resolved_candidates(*, batch_size, quarantine_days, logger):
            resolved_calls.append(
                {
                    "batch_size": batch_size,
                    "quarantine_days": quarantine_days,
                    "logger": logger,
                }
            )
            return []

        with patch.object(
            gc_maintenance,
            "get_resolved_files_gc_candidates",
            side_effect=fake_resolved_candidates,
        ):
            with patch.object(gc_maintenance.k, "GC_BATCH_SIZE", 123):
                with patch.object(gc_maintenance.k, "GC_QUARANTINE_DAYS", 365):
                    with patch.object(
                        gc_maintenance.k,
                        "GC_RESOLVED_FILES_QUARANTINE_DAYS",
                        60,
                    ):
                        history_rows, resolved_rows = gc_maintenance.collect_gc_candidates(
                            fake_db,
                            trash_root="/repo/trash",
                            resolved_root="/repo/trash/resolved_files",
                            logger=fake_log,
                        )

        self.assertEqual(history_rows, [])
        self.assertEqual(resolved_rows, [])
        self.assertEqual(
            fake_db.history_calls,
            [
                {
                    "batch_size": 123,
                    "quarantine_days": 365,
                    "trash_root": "/repo/trash",
                    "resolved_root": "/repo/trash/resolved_files",
                }
            ],
        )
        self.assertEqual(
            resolved_calls,
            [{"batch_size": 123, "quarantine_days": 60, "logger": fake_log}],
        )

    def test_resolved_candidates_keep_only_oldest_bounded_batch(self) -> None:
        fake_log = FakeLog()

        with tempfile.TemporaryDirectory() as tmpdir:
            resolved_root = Path(tmpdir) / "resolved_files"
            resolved_root.mkdir()
            oldest = resolved_root / "oldest.bin"
            middle = resolved_root / "middle.bin"
            newest = resolved_root / "newest.bin"
            for candidate in (oldest, middle, newest):
                candidate.write_text("payload", encoding="utf-8")

            now = time.time()
            os.utime(oldest, (now - 7200, now - 7200))
            os.utime(middle, (now - 5400, now - 5400))
            os.utime(newest, (now - 3600, now - 3600))

            with patch.object(
                gc_maintenance,
                "build_resolved_files_trash_path",
                return_value=str(resolved_root),
            ):
                rows = gc_maintenance.get_resolved_files_gc_candidates(
                    batch_size=2,
                    quarantine_days=0,
                    logger=fake_log,
                )

        self.assertEqual(rows, [str(oldest), str(middle)])

    def test_delete_history_artifact_marks_only_safe_trash_file(self) -> None:
        fake_log = FakeLog()
        fake_db = FakeDbBkp()

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "reposfi"
            trash_root = repo_root / "trash"
            resolved_root = trash_root / "resolved_files"
            trash_root.mkdir(parents=True)
            resolved_root.mkdir()

            artifact = trash_root / "failed_payload.mat"
            artifact.write_text("payload", encoding="utf-8")
            rows = [
                {
                    "ID_HISTORY": 77,
                    "NA_SERVER_FILE_PATH": str(trash_root),
                    "NA_SERVER_FILE_NAME": artifact.name,
                }
            ]

            deleted = gc_maintenance.delete_history_artifacts(
                fake_db,
                rows,
                trash_root=str(trash_root),
                resolved_root=str(resolved_root),
                logger=fake_log,
            )

        self.assertEqual(deleted, 1)
        self.assertFalse(artifact.exists())
        self.assertEqual(len(fake_db.history_updates), 1)
        self.assertEqual(fake_db.history_updates[0]["history_id"], 77)
        self.assertEqual(fake_db.history_updates[0]["IS_PAYLOAD_DELETED"], 1)

    def test_history_cleanup_refuses_unsafe_file_names_and_symlinks(self) -> None:
        fake_log = FakeLog()
        fake_db = FakeDbBkp()

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "reposfi"
            trash_root = repo_root / "trash"
            resolved_root = trash_root / "resolved_files"
            outside_root = Path(tmpdir) / "outside"
            trash_root.mkdir(parents=True)
            resolved_root.mkdir()
            outside_root.mkdir()

            outside_artifact = outside_root / "outside.bin"
            outside_artifact.write_text("preserve", encoding="utf-8")
            link_path = trash_root / "linked_outside"
            link_path.symlink_to(outside_root, target_is_directory=True)

            rows = [
                {
                    "ID_HISTORY": 1,
                    "NA_SERVER_FILE_PATH": str(trash_root),
                    "NA_SERVER_FILE_NAME": str(outside_artifact),
                },
                {
                    "ID_HISTORY": 2,
                    "NA_SERVER_FILE_PATH": str(trash_root),
                    "NA_SERVER_FILE_NAME": "../outside/outside.bin",
                },
                {
                    "ID_HISTORY": 3,
                    "NA_SERVER_FILE_PATH": str(link_path),
                    "NA_SERVER_FILE_NAME": outside_artifact.name,
                },
            ]

            deleted = gc_maintenance.delete_history_artifacts(
                fake_db,
                rows,
                trash_root=str(trash_root),
                resolved_root=str(resolved_root),
                logger=fake_log,
            )

            outside_artifact_exists = outside_artifact.exists()

        self.assertEqual(deleted, 0)
        self.assertTrue(outside_artifact_exists)
        self.assertEqual(fake_db.history_updates, [])
        self.assertIn(
            "garbage_invalid_path_metadata",
            [event_name for event_name, _ in fake_log.events],
        )
        self.assertIn(
            "garbage_refused_outside_trash",
            [event_name for event_name, _ in fake_log.events],
        )

    def test_resolved_cleanup_refuses_symlink_outside_quarantine(self) -> None:
        fake_log = FakeLog()

        with tempfile.TemporaryDirectory() as tmpdir:
            resolved_root = Path(tmpdir) / "resolved_files"
            outside_root = Path(tmpdir) / "outside"
            resolved_root.mkdir()
            outside_root.mkdir()
            outside_artifact = outside_root / "outside.bin"
            outside_artifact.write_text("preserve", encoding="utf-8")
            linked_artifact = resolved_root / "linked.bin"
            linked_artifact.symlink_to(outside_artifact)

            deleted = gc_maintenance.delete_resolved_files_artifacts(
                [str(linked_artifact)],
                resolved_root=str(resolved_root),
                logger=fake_log,
            )

            outside_artifact_exists = outside_artifact.exists()

        self.assertEqual(deleted, 0)
        self.assertTrue(outside_artifact_exists)
        self.assertIn(
            "garbage_refused_outside_resolved_files",
            [event_name for event_name, _ in fake_log.events],
        )


if __name__ == "__main__":
    unittest.main()
