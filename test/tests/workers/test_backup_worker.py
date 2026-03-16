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
import unittest
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
    def __init__(self) -> None:
        self.entries = []
        self.warnings = []

    def entry(self, message: str) -> None:
        self.entries.append(message)

    def warning(self, message: str) -> None:
        self.warnings.append(message)


class WorkerDetectionTests(unittest.TestCase):
    def test_extract_worker_id_recognizes_seed_process_without_worker_arg(self) -> None:
        args = [
            "/opt/conda/envs/appdata/bin/python",
            "/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appCataloga_file_bkp.py",
        ]

        worker_id = backup_worker.extract_worker_id_from_cmdline(
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

        worker_id = backup_worker.extract_worker_id_from_cmdline(
            args,
            "appCataloga_file_bkp.py",
        )

        self.assertEqual(worker_id, 3)

    def test_list_running_workers_includes_seed_worker_started_without_argument(self) -> None:
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
                backup_worker.os,
                "popen",
                return_value=io.StringIO("100\n101\n"),
            ):
                with patch.object(backup_worker.os.path, "exists", return_value=True):
                    with patch("builtins.open", side_effect=fake_open):
                        workers = backup_worker.list_running_workers(
                            "appCataloga_file_bkp.py"
                        )

        self.assertEqual(workers, [0, 1])
        self.assertTrue(
            any("active_workers=[0, 1]" in message for message in fake_log.entries)
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
            backup_worker.os,
            "popen",
            return_value=io.StringIO("100\n101\n"),
        ):
            with patch.object(backup_worker.os.path, "exists", return_value=True):
                with patch("builtins.open", side_effect=fake_open):
                    processes = backup_worker.list_running_worker_processes(
                        "appCataloga_file_bkp.py"
                    )

        self.assertEqual(processes, [(100, 0), (101, 1)])

    def test_broadcast_shutdown_to_worker_pool_signals_only_siblings(self) -> None:
        fake_log = FakeLog()
        sent_signals = []

        with patch.object(backup_worker, "log", fake_log):
            with patch.object(
                backup_worker,
                "list_running_worker_processes",
                return_value=[(100, 0), (101, 1), (102, 2)],
            ):
                with patch.object(backup_worker.os, "getpid", return_value=100):
                    with patch.object(
                        backup_worker.os,
                        "kill",
                        side_effect=lambda pid, sig: sent_signals.append((pid, sig)),
                    ):
                        backup_worker.process_status["shutdown_broadcast_sent"] = False
                        backup_worker.broadcast_shutdown_to_worker_pool("SIGINT")

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


if __name__ == "__main__":
    unittest.main()
