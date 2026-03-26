"""
Validation tests for `appCataloga_file_bkp.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_backup_worker.py -q

What is covered here:
    - worker ID detection from running process command lines
    - seed worker visibility when started without `worker=0`
    - shutdown broadcast to detached sibling workers
"""

from __future__ import annotations

import io
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import APP_ROOT, DB_ROOT, bind_real_package, bind_real_shared_package, ensure_app_paths, load_module_from_path


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        backup_worker = load_module_from_path(
            "test_backup_worker_module",
            str(APP_ROOT / "appCataloga_file_bkp.py"),
        )


class FakeLog:
    """Capture worker log output without pulling the real logging stack."""

    def __init__(self) -> None:
        self.entries = []
        self.warnings = []
        self.errors = []

    def event(self, message: str = "", **kwargs) -> None:
        if kwargs:
            message = f"{message} {kwargs}".strip()
        self.entries.append(message)

    def warning(self, message: str) -> None:
        self.warnings.append(message)

    def warning_event(self, message: str = "", **kwargs) -> None:
        if kwargs:
            message = f"{message} {kwargs}".strip()
        self.warnings.append(message)

    def error(self, message: str) -> None:
        self.errors.append(message)

    def error_event(self, message: str = "", **kwargs) -> None:
        if kwargs:
            message = f"{message} {kwargs}".strip()
        self.errors.append(message)


class FakeTaskDB:
    """Minimal FILE_TASK/HISTORY double used by backup flow tests."""

    def __init__(self) -> None:
        self.file_task_updates = []
        self.file_history_updates = []
        self.queued_tasks = []
        self.cooldown_calls = []

    def file_task_update(self, **kwargs):
        self.file_task_updates.append(kwargs)
        return {"success": True, "rows_affected": 1, "updated_fields": kwargs}

    def file_history_update(self, **kwargs):
        self.file_history_updates.append(kwargs)

    def queue_host_task(self, **kwargs):
        self.queued_tasks.append(kwargs)

    def host_start_transient_busy_cooldown(self, **kwargs):
        self.cooldown_calls.append(kwargs)
        return True

    def file_task_delete(self, *_args, **_kwargs):
        return True

    def file_history_delete(self, **_kwargs):
        return True


class WorkerDetectionTests(unittest.TestCase):
    """Protect worker-pool discovery and coordinated shutdown behavior."""

    def test_extract_worker_id_recognizes_seed_process_without_worker_arg(self) -> None:
        args = [
            "/opt/conda/envs/appdata/bin/python",
            "/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_file_bkp.py",
        ]

        worker_id = backup_worker.worker_pool.extract_worker_id_from_cmdline(
            args,
            "appCataloga_file_bkp.py",
        )

        self.assertEqual(worker_id, 0)

    def test_extract_worker_id_prefers_explicit_worker_argument(self) -> None:
        args = [
            "/opt/conda/envs/appdata/bin/python",
            "/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_file_bkp.py",
            "worker=3",
        ]

        worker_id = backup_worker.worker_pool.extract_worker_id_from_cmdline(
            args,
            "appCataloga_file_bkp.py",
        )

        self.assertEqual(worker_id, 3)

    def test_list_running_workers_includes_seed_worker_started_without_argument(self) -> None:
        # The seed worker is started without `worker=0`, so the pool detection
        # must still surface it as worker zero.
        fake_log = FakeLog()
        cmdlines = {
            "/proc/100/cmdline": (
                "/opt/conda/envs/appdata/bin/python\x00"
                "/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_file_bkp.py\x00"
            ),
            "/proc/101/cmdline": (
                "/opt/conda/envs/appdata/bin/python\x00"
                "/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_file_bkp.py\x00"
                "worker=1\x00"
            ),
        }

        def fake_open(path, *args, **kwargs):
            return io.StringIO(cmdlines[path])

        with patch.object(backup_worker, "log", fake_log):
            with patch.object(
                backup_worker.worker_pool.os,
                "popen",
                return_value=io.StringIO("100\n101\n"),
            ):
                with patch.object(
                    backup_worker.worker_pool.os.path,
                    "exists",
                    return_value=True,
                ):
                    with patch("builtins.open", side_effect=fake_open):
                        workers = backup_worker.worker_pool.list_running_workers(
                            "appCataloga_file_bkp.py",
                            logger=fake_log,
                        )

        self.assertEqual(workers, [0, 1])
        self.assertTrue(
            any("active_workers" in message for message in fake_log.entries)
        )

    def test_list_running_worker_processes_returns_pid_and_worker_id(self) -> None:
        cmdlines = {
            "/proc/100/cmdline": (
                "/opt/conda/envs/appdata/bin/python\x00"
                "/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_file_bkp.py\x00"
            ),
            "/proc/101/cmdline": (
                "/opt/conda/envs/appdata/bin/python\x00"
                "/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_file_bkp.py\x00"
                "worker=1\x00"
            ),
        }

        def fake_open(path, *args, **kwargs):
            return io.StringIO(cmdlines[path])

        with patch.object(
            backup_worker.worker_pool.os,
            "popen",
            return_value=io.StringIO("100\n101\n"),
        ):
            with patch.object(
                backup_worker.worker_pool.os.path,
                "exists",
                return_value=True,
            ):
                with patch("builtins.open", side_effect=fake_open):
                    processes = backup_worker.worker_pool.list_running_worker_processes(
                        "appCataloga_file_bkp.py"
                    )

        self.assertEqual(processes, [(100, 0), (101, 1)])

    def test_broadcast_shutdown_to_worker_pool_signals_only_siblings(self) -> None:
        fake_log = FakeLog()
        sent_signals = []

        with patch.object(backup_worker, "log", fake_log):
            with patch.object(
                backup_worker.worker_pool,
                "list_running_worker_processes",
                return_value=[(100, 0), (101, 1), (102, 2)],
            ):
                with patch.object(
                    backup_worker.worker_pool.os,
                    "getpid",
                    return_value=100,
                ):
                    with patch.object(
                        backup_worker.worker_pool.os,
                        "kill",
                        side_effect=lambda pid, sig: sent_signals.append((pid, sig)),
                    ):
                        backup_worker.process_status["shutdown_broadcast_sent"] = False
                        backup_worker.worker_pool.broadcast_shutdown_to_worker_pool(
                            "SIGINT",
                            process_status=backup_worker.process_status,
                            logger=fake_log,
                            script_path=str(APP_ROOT / "appCataloga_file_bkp.py"),
                        )

        self.assertEqual(
            sent_signals,
            [
                (101, backup_worker.signal.SIGTERM),
                (102, backup_worker.signal.SIGTERM),
            ],
        )
        self.assertTrue(
            any(
                "event=worker_pool_shutdown_broadcast" in message
                for message in fake_log.warnings
            )
        )


class BackupFlowTests(unittest.TestCase):
    """Validate the backup worker contracts around transfer and finalization."""

    def test_transfer_file_task_refreshes_remote_metadata_before_backup(self) -> None:
        refreshed_metadata = backup_worker.file_metadata.FileMetadata(
            NA_FULL_PATH="/remote/sample.bin",
            NA_PATH="/remote",
            NA_FILE="sample.bin",
            NA_EXTENSION=".bin",
            VL_FILE_SIZE_KB=5,
            DT_FILE_CREATED=datetime(2026, 1, 2, 3, 4, 5),
            DT_FILE_MODIFIED=datetime(2026, 1, 2, 3, 5, 6),
            DT_FILE_ACCESSED=None,
            NA_OWNER="0",
            NA_GROUP="0",
            NA_PERMISSIONS="-rw-r--r--",
        )

        class FakeSFTP:
            def __init__(self) -> None:
                self.log = FakeLog()

            def read_file_metadata(self, filename: str):
                return refreshed_metadata

            def size(self, filename: str) -> int:
                return 5 * 1024

            def transfer(self, remote_file: str, local_file: str) -> None:
                Path(local_file).write_bytes(b"x" * (5 * 1024))

        task = {
            "FILE_TASK__NA_EXTENSION": ".bin",
            "FILE_TASK__VL_FILE_SIZE_KB": 10,
            "FILE_TASK__DT_FILE_CREATED": datetime(2025, 1, 1, 1, 1, 1),
            "FILE_TASK__DT_FILE_MODIFIED": datetime(2025, 1, 1, 1, 1, 1),
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            # Start from stale local metadata so the test proves that the
            # worker trusts the freshly read remote file details instead.
            final_file = Path(tmpdir) / "server.bin"
            final_file.write_bytes(b"y" * (10 * 1024))

            with patch.object(
                backup_worker.timeout_utils,
                "run_with_timeout",
                side_effect=lambda fn, timeout: fn(),
            ):
                local_size_kb, remote_metadata = backup_worker.transfer_file_task(
                    sftp=FakeSFTP(),
                    remote_dir="/remote",
                    remote_filename="sample.bin",
                    local_path=tmpdir,
                    server_filename="server.bin",
                    task=task,
                )

            self.assertAlmostEqual(local_size_kb, 5.0)
            self.assertEqual(remote_metadata, refreshed_metadata)
            self.assertEqual(final_file.stat().st_size, 5 * 1024)

    def test_finalize_successful_backup_persists_refreshed_metadata(self) -> None:
        fake_db = FakeTaskDB()
        refreshed_metadata = backup_worker.file_metadata.FileMetadata(
            NA_FULL_PATH="/remote/sample.bin",
            NA_PATH="/remote",
            NA_FILE="sample.bin",
            NA_EXTENSION=".zip",
            VL_FILE_SIZE_KB=7,
            DT_FILE_CREATED=datetime(2026, 2, 3, 4, 5, 6),
            DT_FILE_MODIFIED=datetime(2026, 2, 3, 5, 6, 7),
            DT_FILE_ACCESSED=None,
            NA_OWNER="0",
            NA_GROUP="0",
            NA_PERMISSIONS="-rw-r--r--",
        )
        task = {
            "FILE_TASK__NA_HOST_FILE_PATH": "/remote",
            "FILE_TASK__NA_HOST_FILE_NAME": "sample.bin",
        }

        backup_worker._finalize_successful_backup(
            fake_db,
            worker_id=1,
            host_id=11,
            file_task_id=22,
            task=task,
            input_filename="/remote/sample.bin",
            server_filename="server.bin",
            server_file_path="/repo/tmp",
            refreshed_metadata=refreshed_metadata,
            updated_size_kb=7.0,
        )

        self.assertEqual(fake_db.file_history_updates[0]["NA_EXTENSION"], ".zip")
        self.assertEqual(fake_db.file_history_updates[0]["VL_FILE_SIZE_KB"], 7.0)
        self.assertEqual(
            fake_db.file_history_updates[0]["DT_FILE_CREATED"],
            refreshed_metadata.DT_FILE_CREATED,
        )
        self.assertEqual(
            fake_db.file_history_updates[0]["DT_FILE_MODIFIED"],
            refreshed_metadata.DT_FILE_MODIFIED,
        )
        self.assertEqual(fake_db.file_task_updates[0]["NA_EXTENSION"], ".zip")
        self.assertEqual(fake_db.file_task_updates[0]["VL_FILE_SIZE_KB"], 7.0)
        self.assertEqual(
            fake_db.file_task_updates[0]["DT_FILE_CREATED"],
            refreshed_metadata.DT_FILE_CREATED,
        )
        self.assertEqual(
            fake_db.file_task_updates[0]["DT_FILE_MODIFIED"],
            refreshed_metadata.DT_FILE_MODIFIED,
        )

    def test_requeue_transient_bootstrap_failure_returns_file_task_to_pending(self) -> None:
        fake_db = FakeTaskDB()
        fake_log = FakeLog()
        exc = RuntimeError("busy")
        task = {
            "FILE_TASK__NA_HOST_FILE_PATH": "/remote",
            "FILE_TASK__NA_HOST_FILE_NAME": "sample.bin",
        }

        with patch.object(backup_worker, "log", fake_log):
            with patch.object(
                backup_worker.errors,
                "get_transient_sftp_retry_detail",
                return_value="SSH busy retry",
            ):
                with patch.object(
                    backup_worker.errors,
                    "should_queue_host_check",
                    return_value=True,
                ):
                    with patch.object(
                        backup_worker.errors,
                        "is_timeout_like_sftp_init_error",
                        return_value=False,
                    ):
                        preserved = backup_worker._requeue_transient_bootstrap_failure(
                            fake_db,
                            worker_id=1,
                            host_id=11,
                            file_task_id=22,
                            task=task,
                            exc=exc,
                        )

        self.assertTrue(preserved)
        self.assertEqual(len(fake_db.file_task_updates), 1)
        self.assertEqual(
            fake_db.file_task_updates[0]["NU_STATUS"],
            backup_worker.k.TASK_PENDING,
        )
        self.assertEqual(len(fake_db.queued_tasks), 1)
        self.assertEqual(
            fake_db.queued_tasks[0]["task_type"],
            backup_worker.k.HOST_TASK_CHECK_CONNECTION_TYPE,
        )
        self.assertEqual(len(fake_db.cooldown_calls), 1)

    def test_persist_backup_error_clears_pid_and_queues_host_check_for_auth(self) -> None:
        fake_db = FakeTaskDB()
        fake_log = FakeLog()
        err = backup_worker.errors.ErrorHandler(fake_log)
        task = {
            "FILE_TASK__NA_HOST_FILE_PATH": "/remote",
            "FILE_TASK__NA_HOST_FILE_NAME": "sample.bin",
        }
        err.capture(
            "SSH authentication failed",
            stage="AUTH",
            exc=RuntimeError("bad credentials"),
            host_id=11,
            task_id=22,
        )

        with patch.object(backup_worker, "log", fake_log):
            backup_worker._persist_backup_error(
                fake_db,
                err,
                worker_id=1,
                host_id=11,
                file_task_id=22,
                task=task,
                input_filename="/remote/sample.bin",
                server_filename="server.bin",
                server_file_path="/repo/tmp",
            )

        self.assertEqual(len(fake_db.file_task_updates), 1)
        self.assertEqual(
            fake_db.file_task_updates[0]["NU_STATUS"],
            backup_worker.k.TASK_ERROR,
        )
        self.assertIsNone(fake_db.file_task_updates[0]["NU_PID"])
        self.assertEqual(len(fake_db.file_history_updates), 1)
        self.assertEqual(len(fake_db.queued_tasks), 1)
        self.assertEqual(
            fake_db.queued_tasks[0]["task_type"],
            backup_worker.k.HOST_TASK_CHECK_CONNECTION_TYPE,
        )


if __name__ == "__main__":
    unittest.main()
