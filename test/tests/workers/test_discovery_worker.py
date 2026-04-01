"""
Validation tests for `appCataloga_discovery.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_discovery_worker.py -q

What is covered here:
    - transient SSH/SFTP bootstrap failures are requeued with cooldown
    - persisted HOST_TASK errors clear NU_PID and request host reconciliation
"""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import APP_ROOT, DB_ROOT, bind_real_package, bind_real_shared_package, ensure_app_paths, load_module_from_path


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        discovery_worker = load_module_from_path(
            "test_discovery_worker_module",
            str(APP_ROOT / "appCataloga_discovery.py"),
        )


class FakeLog:
    """Capture discovery events without depending on the production logger."""

    def __init__(self) -> None:
        self.events = []

    def event(self, event_name: str, **fields) -> None:
        self.events.append(("event", event_name, fields))

    def warning(self, message: str) -> None:
        self.events.append(("warning", message, {}))

    def warning_event(self, event_name: str, **fields) -> None:
        self.events.append(("warning_event", event_name, fields))

    def error(self, message: str) -> None:
        self.events.append(("error", message, {}))

    def error_event(self, event_name: str, **fields) -> None:
        self.events.append(("error_event", event_name, fields))


class FakeDB:
    """Minimal HOST/HOST_TASK double used by discovery error-path tests."""

    def __init__(self) -> None:
        self.host_task_updates = []
        self.queued_tasks = []
        self.cooldown_calls = []

    def host_task_update(self, **kwargs):
        self.host_task_updates.append(kwargs)
        return {"success": True, "rows_affected": 1, "updated_fields": kwargs}

    def queue_host_task(self, **kwargs):
        self.queued_tasks.append(kwargs)
        return {"HOST_TASK__ID_HOST_TASK": 99}

    def host_start_transient_busy_cooldown(self, **kwargs):
        self.cooldown_calls.append(kwargs)
        return True


class DiscoveryWorkerTests(unittest.TestCase):
    """Protect discovery behavior when bootstrap cannot proceed normally."""

    def test_requeue_transient_bootstrap_failure_returns_task_to_pending_and_starts_cooldown(self) -> None:
        fake_db = FakeDB()
        fake_log = FakeLog()
        exc = RuntimeError("busy")

        with patch.object(discovery_worker, "log", fake_log):
            with patch.object(
                discovery_worker.errors,
                "get_transient_sftp_retry_detail",
                return_value="SSH busy retry",
            ):
                with patch.object(
                    discovery_worker.errors,
                    "should_queue_host_check",
                    return_value=True,
                ):
                    with patch.object(
                        discovery_worker.errors,
                        "is_timeout_like_sftp_init_error",
                        return_value=False,
                    ):
                        preserved = discovery_worker._requeue_transient_bootstrap_failure(
                            fake_db,
                            host_id=11,
                            task_id=22,
                            exc=exc,
                        )

        # Transient bootstrap failures must keep the discovery row alive while
        # also asking the backend for a focused connectivity recheck.
        self.assertTrue(preserved)
        self.assertEqual(len(fake_db.host_task_updates), 1)
        self.assertEqual(
            fake_db.host_task_updates[0]["NU_STATUS"],
            discovery_worker.k.TASK_PENDING,
        )
        self.assertEqual(len(fake_db.queued_tasks), 1)
        self.assertEqual(
            fake_db.queued_tasks[0]["task_type"],
            discovery_worker.k.HOST_TASK_CHECK_CONNECTION_TYPE,
        )
        self.assertEqual(len(fake_db.cooldown_calls), 1)

    def test_persist_discovery_error_clears_pid_and_requests_host_check_for_auth(self) -> None:
        fake_db = FakeDB()
        fake_log = FakeLog()
        err = discovery_worker.errors.ErrorHandler(fake_log)
        err.capture(
            "SSH authentication failed",
            stage="AUTH",
            exc=RuntimeError("bad credentials"),
            host_id=33,
            task_id=44,
        )

        with patch.object(discovery_worker, "log", fake_log):
            discovery_worker._persist_discovery_error(
                fake_db,
                err,
                host_id=33,
                task_id=44,
                hostname="RFEye-test",
                processed=0,
                backlog_result={"queued_backlog_tasks": 0},
            )

        # Definitive discovery failures should free the row from any worker PID
        # and leave a connectivity follow-up queued for operators.
        self.assertEqual(len(fake_db.host_task_updates), 1)
        self.assertEqual(
            fake_db.host_task_updates[0]["NU_STATUS"],
            discovery_worker.k.TASK_ERROR,
        )
        self.assertEqual(
            fake_db.host_task_updates[0]["NU_PID"],
            discovery_worker.k.HOST_UNLOCKED_PID,
        )
        self.assertEqual(len(fake_db.queued_tasks), 1)
        self.assertEqual(
            fake_db.queued_tasks[0]["task_type"],
            discovery_worker.k.HOST_TASK_CHECK_CONNECTION_TYPE,
        )

    def test_queue_backlog_control_task_hands_off_promotion_to_dedicated_worker(self) -> None:
        fake_db = FakeDB()
        fake_log = FakeLog()

        with patch.object(discovery_worker, "log", fake_log):
            result = discovery_worker._queue_backlog_control_task(
                fake_db,
                task={"host_filter": {"mode": "ALL"}},
                task_id=44,
                host_id=33,
                hostname="CWSM211004",
                processed=12,
            )

        self.assertEqual(result["queued_backlog_tasks"], 1)
        self.assertEqual(len(fake_db.queued_tasks), 1)
        self.assertEqual(
            fake_db.queued_tasks[0]["task_type"],
            discovery_worker.k.HOST_TASK_BACKLOG_CONTROL_TYPE,
        )
        self.assertEqual(len(fake_db.host_task_updates), 1)
        self.assertEqual(
            fake_db.host_task_updates[0]["NU_STATUS"],
            discovery_worker.k.TASK_DONE,
        )


if __name__ == "__main__":
    unittest.main()
