"""
Validation tests for `appCataloga_summary_database.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_appCataloga_summary_database.py -q

What is covered here:
    - the daily 02:00 BRT reconcile schedule
    - the startup reconcile heuristic
    - the incremental batch checkpoint contract
    - worker failure finalization
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime
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
            summary_daemon = load_module_from_path(
                "test_appcataloga_summary_database_module",
                str(APP_ROOT / "appCataloga_summary_database.py"),
            )


class FakeLog:
    """Collect structured worker log calls for contract assertions."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []
        self.errors: list[tuple[str, dict]] = []

    def event(self, event: str, **fields) -> None:
        self.events.append((event, fields))

    def error_event(self, event: str, **fields) -> None:
        self.errors.append((event, fields))


class FakeSummaryDb:
    """Minimal dbHandlerSummary double for entrypoint helper tests."""

    def __init__(self, *, batch=None) -> None:
        self.batch = batch or []
        self.calls: list[tuple[str, object]] = []

    def configure_worker_session(self) -> None:
        self.calls.append(("configure_worker_session", None))

    def read_outbox_batch(self, consumer_name: str, *, batch_size: int):
        self.calls.append(
            (
                "read_outbox_batch",
                {
                    "consumer_name": consumer_name,
                    "batch_size": batch_size,
                },
            )
        )
        return list(self.batch)

    def mark_worker_start(self, consumer_name: str) -> None:
        self.calls.append(("mark_worker_start", consumer_name))

    def mark_worker_success(
        self,
        consumer_name: str,
        *,
        last_outbox_id: int,
        batch_size: int,
        event_count: int,
    ) -> None:
        self.calls.append(
            (
                "mark_worker_success",
                {
                    "consumer_name": consumer_name,
                    "last_outbox_id": last_outbox_id,
                    "batch_size": batch_size,
                    "event_count": event_count,
                },
            )
        )

    def drain_consumed_outbox(self, consumer_name: str) -> None:
        self.calls.append(("drain_consumed_outbox", consumer_name))

    def mark_worker_failure(self, consumer_name: str, *, error_message: str) -> None:
        self.calls.append(
            (
                "mark_worker_failure",
                {
                    "consumer_name": consumer_name,
                    "error_message": error_message,
                },
            )
        )


class FakeEngine:
    """Minimal refresh-engine double for incremental batch tests."""

    def __init__(self) -> None:
        self.refresh_calls = []

    def refresh_for_events(self, events) -> None:
        self.refresh_calls.append(events)


class SummaryDatabaseWorkerTests(unittest.TestCase):
    """Validate the renamed summary maintenance daemon entrypoint."""

    def test_next_2am_brt_rolls_same_day_then_next_day(self) -> None:
        before_cutoff = datetime(2026, 6, 11, 4, 30, 0)
        after_cutoff = datetime(2026, 6, 11, 5, 30, 0)

        same_day = summary_daemon._next_2am_brt(before_cutoff)
        next_day = summary_daemon._next_2am_brt(after_cutoff)

        self.assertEqual(same_day, datetime(2026, 6, 11, 5, 0, 0))
        self.assertEqual(next_day, datetime(2026, 6, 12, 5, 0, 0))

    def test_schedule_startup_reconcile_uses_staleness_decision(self) -> None:
        fake_log = FakeLog()
        fake_db = object()
        scheduled_at = datetime(2026, 6, 12, 5, 0, 0)

        with patch.object(summary_daemon, "log", fake_log):
            with patch.object(summary_daemon, "_needs_startup_reconcile", return_value=True):
                self.assertEqual(
                    summary_daemon._schedule_startup_reconcile(fake_db),
                    datetime.min,
                )

            with patch.object(summary_daemon, "_needs_startup_reconcile", return_value=False):
                with patch.object(summary_daemon, "_next_2am_brt", return_value=scheduled_at):
                    self.assertEqual(
                        summary_daemon._schedule_startup_reconcile(fake_db),
                        scheduled_at,
                    )

        self.assertEqual(
            [event for event, _fields in fake_log.events],
            [
                "summary_startup_reconcile_required",
                "summary_startup_reconcile_skipped",
            ],
        )

    def test_run_incremental_batch_advances_checkpoint_and_drains_queue(self) -> None:
        fake_db = FakeSummaryDb(
            batch=[
                {"ID_OUTBOX": 10, "NA_SCOPE_TYPE": "host", "NA_SCOPE_VALUE": "1"},
                {"ID_OUTBOX": 12, "NA_SCOPE_TYPE": "host", "NA_SCOPE_VALUE": "2"},
            ]
        )
        fake_engine = FakeEngine()

        with patch.object(summary_daemon.k, "SUMMARY_WORKER_CONSUMER_NAME", "summary_consumer"):
            with patch.object(summary_daemon.k, "SUMMARY_WORKER_BATCH_SIZE", 500):
                processed = summary_daemon._run_incremental_batch(fake_db, fake_engine)

        self.assertTrue(processed)
        self.assertEqual(len(fake_engine.refresh_calls), 1)
        self.assertEqual(fake_engine.refresh_calls[0][-1]["ID_OUTBOX"], 12)
        self.assertEqual(
            fake_db.calls,
            [
                (
                    "read_outbox_batch",
                    {
                        "consumer_name": "summary_consumer",
                        "batch_size": 500,
                    },
                ),
                ("mark_worker_start", "summary_consumer"),
                (
                    "mark_worker_success",
                    {
                        "consumer_name": "summary_consumer",
                        "last_outbox_id": 12,
                        "batch_size": 500,
                        "event_count": 2,
                    },
                ),
                ("drain_consumed_outbox", "summary_consumer"),
            ],
        )

    def test_finalize_error_marks_failure_and_logs_structured_event(self) -> None:
        fake_db = FakeSummaryDb()
        fake_log = FakeLog()

        with patch.object(summary_daemon, "log", fake_log):
            with patch.object(summary_daemon.k, "SUMMARY_WORKER_CONSUMER_NAME", "summary_consumer"):
                summary_daemon._finalize_error(fake_db, RuntimeError("boom"))

        self.assertEqual(
            fake_db.calls,
            [
                (
                    "mark_worker_failure",
                    {
                        "consumer_name": "summary_consumer",
                        "error_message": "RuntimeError('boom')",
                    },
                )
            ],
        )
        self.assertEqual(
            fake_log.errors,
            [
                (
                    "summary_worker_loop_failed",
                    {
                        "service": "appCataloga_summary_database",
                        "error": "RuntimeError('boom')",
                    },
                )
            ],
        )

if __name__ == "__main__":
    unittest.main()
