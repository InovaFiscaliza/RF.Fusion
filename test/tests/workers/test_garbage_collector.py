"""
Validation tests for `appCataloga_garbage_collector.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_garbage_collector.py -q

What is covered here:
    - main trash and `resolved_files` use distinct quarantine windows
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


class FakeLog:
    """Collect garbage-collector events without touching the real logger."""

    def __init__(self) -> None:
        self.events = []

    def service_start(self, service: str) -> None:
        self.events.append(("service_start", service))

    def event(self, event_name: str, **fields) -> None:
        self.events.append((event_name, fields))

    def warning(self, message: str) -> None:
        self.events.append(("warning", message))

    def error(self, message: str) -> None:
        self.events.append(("error", message))


class FakeDbBkp:
    """Minimal FILE_TASK_HISTORY double used by GC contract tests."""

    def __init__(self, *args, **kwargs) -> None:
        self.history_calls = []
        self.commit_calls = 0
        self.history_updates = []

    def file_history_get_gc_candidates(self, *, batch_size, quarantine_days):
        self.history_calls.append(
            {
                "batch_size": batch_size,
                "quarantine_days": quarantine_days,
            }
        )
        return []

    def file_history_update(self, **kwargs):
        self.history_updates.append(kwargs)

    def commit(self):
        self.commit_calls += 1


class GarbageCollectorTests(unittest.TestCase):
    """Validate the split retention policy between tracked and resolved artifacts."""

    def test_main_uses_shorter_quarantine_for_resolved_files(self) -> None:
        fake_log = FakeLog()
        resolved_calls = []
        db_instances = []

        def fake_db_factory(*args, **kwargs):
            db = FakeDbBkp(*args, **kwargs)
            db_instances.append(db)
            return db

        def fake_resolved_candidates(*, batch_size, quarantine_days):
            resolved_calls.append(
                {
                    "batch_size": batch_size,
                    "quarantine_days": quarantine_days,
                }
            )
            garbage_worker.process_status["running"] = False
            return []

        with patch.object(garbage_worker, "log", fake_log):
            with patch.object(garbage_worker, "dbHandlerBKP", side_effect=fake_db_factory):
                with patch.object(
                    garbage_worker,
                    "get_resolved_files_gc_candidates",
                    side_effect=fake_resolved_candidates,
                ):
                    with patch.object(garbage_worker.time, "sleep", side_effect=lambda *_: None):
                        with patch.object(garbage_worker.k, "GC_BATCH_SIZE", 123):
                            with patch.object(garbage_worker.k, "GC_QUARANTINE_DAYS", 365):
                                with patch.object(
                                    garbage_worker.k,
                                    "GC_RESOLVED_FILES_QUARANTINE_DAYS",
                                    60,
                                ):
                                    garbage_worker.process_status["running"] = True
                                    garbage_worker.main()

        # The canonical artifact in `trash` and the superseded source in
        # `resolved_files` intentionally age out on different clocks.
        self.assertEqual(len(db_instances), 1)
        self.assertEqual(
            db_instances[0].history_calls,
            [{"batch_size": 123, "quarantine_days": 365}],
        )
        self.assertEqual(
            resolved_calls,
            [{"batch_size": 123, "quarantine_days": 60}],
        )
        self.assertTrue(
            any(
                entry[0] == "garbage_configuration"
                and entry[1]["trash_quarantine_days"] == 365
                and entry[1]["resolved_quarantine_days"] == 60
                for entry in fake_log.events
                if isinstance(entry, tuple) and len(entry) == 2 and isinstance(entry[1], dict)
            )
        )

    def test_delete_history_artifacts_deletes_file_and_marks_history_row(self) -> None:
        fake_log = FakeLog()
        fake_db = FakeDbBkp()

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "reposfi"
            trash_root = repo_root / "trash"
            resolved_root = trash_root / "resolved_files"
            trash_root.mkdir(parents=True)
            resolved_root.mkdir(parents=True)

            artifact = trash_root / "failed_payload.mat"
            artifact.write_text("payload", encoding="utf-8")

            rows = [
                {
                    "ID_HISTORY": 77,
                    "NA_SERVER_FILE_PATH": str(trash_root),
                    "NA_SERVER_FILE_NAME": artifact.name,
                }
            ]

            with patch.object(garbage_worker, "log", fake_log):
                deleted = garbage_worker.delete_history_artifacts(
                    fake_db,
                    rows,
                    trash_root=str(trash_root),
                    resolved_root=str(resolved_root),
                )

        self.assertEqual(deleted, 1)
        self.assertFalse(artifact.exists())
        self.assertEqual(len(fake_db.history_updates), 1)
        self.assertEqual(fake_db.history_updates[0]["history_id"], 77)
        self.assertEqual(fake_db.history_updates[0]["IS_PAYLOAD_DELETED"], 1)
        self.assertIn(
            ("garbage_history_artifact_deleted", {"history_id": 77, "path": str(artifact)}),
            fake_log.events,
        )


if __name__ == "__main__":
    unittest.main()
