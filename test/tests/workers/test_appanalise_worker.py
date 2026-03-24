"""
Validation tests for `appCataloga_file_bin_proces_appAnalise.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_appanalise_worker.py -q

What is covered here:
    - export decision rules for different hostnames
    - file metadata resolution for history and output files
    - filesystem helpers used to move original and resolved artifacts
    - retry message generation for transient appAnalise failures
"""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import (
    APP_ROOT,
    DB_ROOT,
    STATIONS_ROOT,
    bind_real_package,
    bind_real_shared_package,
    ensure_app_paths,
    load_module_from_path,
)


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        with bind_real_package("stations", STATIONS_ROOT):
            worker = load_module_from_path(
                "test_appanalise_worker_module",
                str(APP_ROOT / "appCataloga_file_bin_proces_appAnalise.py"),
            )


class FakeWorkerLog:
    """Record worker log events so tests can assert meaningful side effects."""

    def __init__(self) -> None:
        self.entries = []
        self.errors = []
        self.warnings = []

    def entry(self, message: str) -> None:
        self.entries.append(message)

    def error(self, message: str) -> None:
        self.errors.append(message)

    def warning(self, message: str) -> None:
        self.warnings.append(message)

    def event(self, event: str, **fields) -> None:
        self.entries.append((event, fields))

    def error_event(self, event: str, **fields) -> None:
        self.errors.append((event, fields))

    def service_start(self, service: str) -> None:
        self.entries.append(("service_start", service))


class FakeDbBkp:
    """Minimal FILE_TASK persistence double with in-memory call recording."""

    def __init__(self) -> None:
        self.task_updates = []
        self.task_deletes = []
        self.history_updates = []
        self.statistics_updates = []

    def file_task_update(self, **kwargs) -> None:
        self.task_updates.append(kwargs)

    def file_task_delete(self, **kwargs) -> None:
        self.task_deletes.append(kwargs)

    def file_history_update(self, **kwargs) -> None:
        self.history_updates.append(kwargs)

    def host_task_statistics_create(self, **kwargs) -> None:
        self.statistics_updates.append(kwargs)


class FakeDbRfm:
    """Minimal RFDATA double for server-file registration on success."""

    def __init__(self) -> None:
        self.insert_file_calls = []
        self.bridge_calls = []

    def build_path(self, site_id: int) -> str:
        return f"site_{site_id}/catalog"

    def insert_file(self, **kwargs) -> int:
        self.insert_file_calls.append(kwargs)
        return 900 + len(self.insert_file_calls)

    def insert_bridge_spectrum_file(self, spectrum_ids, file_ids) -> None:
        self.bridge_calls.append((list(spectrum_ids), list(file_ids)))


class FakeErr:
    """Small error double matching the worker's finalization contract."""

    def __init__(self, message: str = "", triggered: bool = False) -> None:
        self._message = message
        self.triggered = triggered

    def format_error(self) -> str:
        return self._message


def build_test_file_meta(path: Path) -> dict:
    """Build worker-style file metadata for a real temporary file."""
    stat = path.stat()
    timestamp = datetime.fromtimestamp(stat.st_mtime)
    return worker.build_file_metadata(
        file_path=str(path.parent),
        file_name=path.name,
        extension=path.suffix,
        size_kb=max(1, int(stat.st_size / 1024) or 1),
        dt_created=timestamp,
        dt_modified=timestamp,
    )


class ShouldExportTests(unittest.TestCase):
    def test_should_export_disables_mat_for_rfeye_hosts(self) -> None:
        self.assertFalse(worker.should_export("rfeye001234"))

    def test_should_export_enables_mat_for_cw_hosts(self) -> None:
        self.assertTrue(worker.should_export("CWSM21100001"))

    def test_should_export_defaults_to_true_for_other_hosts(self) -> None:
        self.assertTrue(worker.should_export("unknown_station"))


class FileMetadataTests(unittest.TestCase):
    def test_build_file_metadata_generates_full_path(self) -> None:
        created = datetime(2026, 3, 16, 12, 0, 0)

        metadata = worker.build_file_metadata(
            file_path="/mnt/reposfi/tmp/CWSM211001",
            file_name="sample_DONE.mat",
            extension=".mat",
            size_kb=42,
            dt_created=created,
            dt_modified=created,
        )

        self.assertEqual(
            metadata["full_path"],
            "/mnt/reposfi/tmp/CWSM211001/sample_DONE.mat",
        )

    def test_resolve_history_file_metadata_prefers_processed_artifact(self) -> None:
        created = datetime(2026, 3, 16, 12, 0, 0)
        file_meta = {
            "file_name": "sample_DONE.mat",
            "extension": ".mat",
            "size_kb": 42,
            "dt_created": created,
            "dt_modified": created,
        }

        history = worker.resolve_history_file_metadata(
            file_was_processed=True,
            file_meta=file_meta,
            server_name="sample_DONE.zip",
            extension=".zip",
            vl_file_size_kb=10,
            dt_created=created,
            dt_modified=created,
        )

        self.assertEqual(history["name"], "sample_DONE.mat")
        self.assertEqual(history["extension"], ".mat")
        self.assertEqual(history["size_kb"], 42)

    def test_resolve_history_file_metadata_falls_back_to_original_file(self) -> None:
        created = datetime(2026, 3, 16, 12, 0, 0)

        history = worker.resolve_history_file_metadata(
            file_was_processed=False,
            file_meta=None,
            server_name="sample_DONE.zip",
            extension=".zip",
            vl_file_size_kb=10,
            dt_created=created,
            dt_modified=created,
        )

        self.assertEqual(history["name"], "sample_DONE.zip")
        self.assertEqual(history["extension"], ".zip")
        self.assertEqual(history["size_kb"], 10)

    def test_is_same_file_normalizes_equivalent_paths(self) -> None:
        file_a = {"full_path": "/mnt/reposfi/tmp/../tmp/file.zip"}
        file_b = {"full_path": "/mnt/reposfi/tmp/file.zip"}

        self.assertTrue(worker.is_same_file(file_a, file_b))

    def test_is_same_file_rejects_missing_metadata(self) -> None:
        self.assertFalse(worker.is_same_file(None, {"full_path": "/tmp/file.zip"}))


class FileMoveTests(unittest.TestCase):
    def test_move_file_if_present_moves_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_dir = Path(tmpdir) / "source"
            target_dir = Path(tmpdir) / "target"
            source_dir.mkdir()
            source_file = source_dir / "sample_DONE.mat"
            source_file.write_text("payload", encoding="utf-8")

            file_meta = {
                "file_path": str(source_dir),
                "file_name": source_file.name,
                "extension": ".mat",
                "size_kb": 1,
                "dt_created": datetime.now(),
                "dt_modified": datetime.now(),
                "full_path": str(source_file),
            }

            moved = worker.move_file_if_present(file_meta, str(target_dir))

            self.assertIsNotNone(moved)
            self.assertEqual(moved["file_path"], str(target_dir))
            self.assertTrue((target_dir / source_file.name).exists())
            self.assertFalse(source_file.exists())

    def test_move_file_if_present_ignores_absent_file(self) -> None:
        file_meta = {
            "file_path": "/tmp",
            "file_name": "missing.mat",
            "extension": ".mat",
            "size_kb": 1,
            "dt_created": datetime.now(),
            "dt_modified": datetime.now(),
            "full_path": "/tmp/missing.mat",
        }

        self.assertIsNone(worker.move_file_if_present(file_meta, "/tmp/target"))


class RetryTests(unittest.TestCase):
    def test_preflight_app_analise_connection_returns_false_without_claiming_task(self) -> None:
        class FakeApp:
            def check_connection(self) -> None:
                raise worker.errors.ExternalServiceTransientError("service down")

        fake_log = FakeWorkerLog()

        with patch.object(worker, "log", fake_log):
            self.assertFalse(worker.preflight_app_analise_connection(FakeApp()))

        self.assertEqual(len(fake_log.warnings), 1)
        self.assertIn("appanalise_unavailable_retry", fake_log.warnings[0])

    def test_return_task_to_pending_requeues_with_standard_message(self) -> None:
        class FakeDb:
            def __init__(self) -> None:
                self.calls = []

            def file_task_update(self, **kwargs) -> None:
                self.calls.append(kwargs)

        class FakeErr:
            def format_error(self) -> str:
                return "[ERROR] timeout"

        db = FakeDb()

        worker.return_task_to_pending(db, file_task_id=321, err=FakeErr())

        self.assertEqual(len(db.calls), 1)
        payload = db.calls[0]
        self.assertEqual(payload["task_id"], 321)
        self.assertEqual(payload["NU_STATUS"], worker.k.TASK_PENDING)
        self.assertNotIn("NU_PID", payload)
        self.assertIn("Processing Pending", payload["NA_MESSAGE"])
        self.assertIn("task returned for retry", payload["NA_MESSAGE"])
        self.assertIn("[ERROR] timeout", payload["NA_MESSAGE"])

    def test_resolve_task_after_attempt_retries_without_deleting_history(self) -> None:
        db = FakeDbBkp()

        result = worker.resolve_task_after_attempt(
            db,
            file_task_id=321,
            host_id=77,
            host_file_name="host_file.zip",
            host_path="/host/path",
            server_name="server_file.zip",
            extension=".zip",
            vl_file_size_kb=10,
            dt_created=datetime(2026, 3, 16, 12, 0, 0),
            dt_modified=datetime(2026, 3, 16, 12, 0, 0),
            file_was_processed=False,
            new_path=None,
            file_meta=None,
            source_file_meta=None,
            export=True,
            retry_later=True,
            err=FakeErr("[ERROR] timeout", triggered=True),
        )

        self.assertEqual(result["action"], "retry")
        self.assertEqual(len(db.task_updates), 1)
        self.assertEqual(len(db.task_deletes), 0)
        self.assertEqual(len(db.history_updates), 0)
        self.assertEqual(len(db.statistics_updates), 0)
        self.assertEqual(db.task_updates[0]["NU_STATUS"], worker.k.TASK_PENDING)
        self.assertIn("task returned for retry", db.task_updates[0]["NA_MESSAGE"])


class PathRuleTests(unittest.TestCase):
    def test_build_resolved_files_trash_path_uses_dedicated_subdir(self) -> None:
        resolved_trash = worker.build_resolved_files_trash_path()

        self.assertTrue(resolved_trash.endswith("/trash/resolved_files"))
        self.assertIn(worker.k.REPO_FOLDER, resolved_trash)


class WorkerFlowScenarioTests(unittest.TestCase):
    def test_main_does_not_read_file_task_when_appanalise_is_unavailable(self) -> None:
        fake_log = FakeWorkerLog()
        read_calls = []
        sleep_calls = []

        class FakeDbBkpMain:
            def __init__(self, *args, **kwargs) -> None:
                pass

            def read_file_task(self, **kwargs):
                read_calls.append(kwargs)
                return None

        class FakeDbRfmMain:
            def __init__(self, *args, **kwargs) -> None:
                self.in_transaction = False

        class FakeApp:
            def check_connection(self) -> None:
                worker.process_status["running"] = False
                raise worker.errors.ExternalServiceTransientError("service down")

        with patch.object(worker, "log", fake_log):
            with patch.object(worker, "dbHandlerBKP", FakeDbBkpMain):
                with patch.object(worker, "dbHandlerRFM", FakeDbRfmMain):
                    with patch.object(worker, "AppAnaliseConnection", FakeApp):
                        with patch.object(
                            worker.legacy,
                            "_random_jitter_sleep",
                            side_effect=lambda: sleep_calls.append("slept"),
                        ):
                            worker.process_status["running"] = True
                            worker.main()

        self.assertEqual(read_calls, [])
        self.assertEqual(sleep_calls, ["slept"])
        self.assertEqual(len(fake_log.warnings), 1)
        self.assertIn("appanalise_unavailable_retry", fake_log.warnings[0])

    def test_successful_export_promotes_mat_and_retires_original(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "reposfi"
            source_dir = repo_root / "incoming"
            source_dir.mkdir(parents=True)

            source_file = source_dir / "sample_DONE.zip"
            source_file.write_text("zip payload", encoding="utf-8")
            exported_file = source_dir / "sample_DONE.mat"
            exported_file.write_text("mat payload", encoding="utf-8")

            source_meta = build_test_file_meta(source_file)
            exported_meta = build_test_file_meta(exported_file)
            db_rfm = FakeDbRfm()
            db_bp = FakeDbBkp()
            fake_log = FakeWorkerLog()

            with patch.object(worker.k, "REPO_FOLDER", str(repo_root)):
                with patch.object(worker.k, "TRASH_FOLDER", "trash"):
                    with patch.object(worker, "log", fake_log):
                        new_path, final_meta = worker.finalize_successful_processing(
                            db_rfm=db_rfm,
                            spectrum_ids=[10, 11],
                            bin_data={
                                "spectrum": [
                                    SimpleNamespace(
                                        start_dateidx=datetime(2026, 1, 31, 20, 18, 51)
                                    )
                                ]
                            },
                            site_id=5,
                            hostname_bin="CWSM21100001",
                            file_meta=exported_meta,
                            source_file_meta=source_meta,
                            export=True,
                            filename=str(source_file),
                        )

                        result = worker.resolve_task_after_attempt(
                            db_bp,
                            file_task_id=99,
                            host_id=7,
                            host_file_name="host_sample.zip",
                            host_path="/host/path",
                            server_name=source_file.name,
                            extension=".zip",
                            vl_file_size_kb=source_meta["size_kb"],
                            dt_created=source_meta["dt_created"],
                            dt_modified=source_meta["dt_modified"],
                            file_was_processed=True,
                            new_path=new_path,
                            file_meta=final_meta,
                            source_file_meta=source_meta,
                            export=True,
                            retry_later=False,
                            err=FakeErr(),
                        )

            final_file = Path(final_meta["full_path"])
            resolved_source = repo_root / "trash" / "resolved_files" / source_file.name

            self.assertTrue(final_file.exists())
            self.assertEqual(final_file.read_text(encoding="utf-8"), "mat payload")
            self.assertTrue(resolved_source.exists())
            self.assertEqual(
                resolved_source.read_text(encoding="utf-8"),
                "zip payload",
            )
            self.assertFalse(source_file.exists())
            self.assertFalse(exported_file.exists())

            self.assertEqual(result["action"], "finalized")
            self.assertEqual(result["status"], worker.k.TASK_DONE)
            self.assertEqual(len(db_rfm.insert_file_calls), 1)
            self.assertEqual(db_rfm.insert_file_calls[0]["NA_FILE"], "sample_DONE.mat")
            self.assertEqual(len(db_rfm.bridge_calls), 1)
            self.assertEqual(len(db_bp.task_deletes), 1)
            self.assertEqual(len(db_bp.history_updates), 1)
            self.assertEqual(
                db_bp.history_updates[0]["NA_SERVER_FILE_NAME"],
                "sample_DONE.mat",
            )
            self.assertEqual(
                db_bp.history_updates[0]["NU_STATUS_PROCESSING"],
                worker.k.TASK_DONE,
            )
            self.assertTrue(
                any(
                    isinstance(item, tuple) and item[0] == "processing_completed"
                    for item in fake_log.entries
                )
            )

    def test_definitive_failure_moves_original_to_trash_and_partial_artifact_to_resolved_trash(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_root = Path(tmpdir) / "reposfi"
            source_dir = repo_root / "incoming"
            source_dir.mkdir(parents=True)

            source_file = source_dir / "sample_DONE.zip"
            source_file.write_text("zip payload", encoding="utf-8")
            partial_artifact = source_dir / "sample_DONE.mat"
            partial_artifact.write_text("partial mat", encoding="utf-8")

            source_meta = build_test_file_meta(source_file)
            partial_meta = build_test_file_meta(partial_artifact)
            db_bp = FakeDbBkp()
            fake_log = FakeWorkerLog()
            err = FakeErr("[ERROR] validation failed", triggered=True)

            with patch.object(worker.k, "REPO_FOLDER", str(repo_root)):
                with patch.object(worker.k, "TRASH_FOLDER", "trash"):
                    with patch.object(worker, "log", fake_log):
                        result = worker.resolve_task_after_attempt(
                            db_bp,
                            file_task_id=101,
                            host_id=8,
                            host_file_name="host_sample.zip",
                            host_path="/host/path",
                            server_name=source_file.name,
                            extension=".zip",
                            vl_file_size_kb=source_meta["size_kb"],
                            dt_created=source_meta["dt_created"],
                            dt_modified=source_meta["dt_modified"],
                            file_was_processed=False,
                            new_path=None,
                            file_meta=partial_meta,
                            source_file_meta=source_meta,
                            export=True,
                            retry_later=False,
                            err=err,
                        )

            trashed_source = repo_root / "trash" / source_file.name
            trashed_artifact = repo_root / "trash" / "resolved_files" / partial_artifact.name

            self.assertTrue(trashed_source.exists())
            self.assertEqual(trashed_source.read_text(encoding="utf-8"), "zip payload")
            self.assertTrue(trashed_artifact.exists())
            self.assertEqual(trashed_artifact.read_text(encoding="utf-8"), "partial mat")
            self.assertFalse(source_file.exists())
            self.assertFalse(partial_artifact.exists())

            self.assertEqual(result["action"], "finalized")
            self.assertEqual(result["status"], worker.k.TASK_ERROR)
            self.assertEqual(result["new_path"], str(repo_root / "trash"))
            self.assertEqual(len(db_bp.task_deletes), 1)
            self.assertEqual(len(db_bp.history_updates), 1)
            self.assertEqual(
                db_bp.history_updates[0]["NA_SERVER_FILE_NAME"],
                "sample_DONE.zip",
            )
            self.assertEqual(
                db_bp.history_updates[0]["NA_SERVER_FILE_PATH"],
                str(repo_root / "trash"),
            )
            self.assertEqual(
                db_bp.history_updates[0]["NU_STATUS_PROCESSING"],
                worker.k.TASK_ERROR,
            )
            self.assertIn("[ERROR] validation failed", db_bp.history_updates[0]["NA_MESSAGE"])
            self.assertEqual(len(db_bp.statistics_updates), 1)


if __name__ == "__main__":
    unittest.main()
