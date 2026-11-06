"""Verify janitor decisions independently from SQL and network probes."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _support import APP_ROOT, bind_test_config, load_module_from_path

sys.path.insert(0, str(APP_ROOT))
with bind_test_config():
    from db.dbHandlerBKP import dbHandlerBKP
    from shared import logging_utils
    import host_handler

    # These tests exercise janitors only. Network probes and their unmigrated
    # Summary dependency are outside this fixture and must never be invoked.
    with patch.object(
        host_handler,
        "host_connectivity",
        ModuleType("unused_network_probes"),
        create=True,
    ):
        maintenance = load_module_from_path(
            "test_postgres_host_maintenance",
            str(APP_ROOT / "host_handler/host_maintenance.py"),
        )


class HostMaintenanceTests(unittest.TestCase):
    """Protect cooldown, PID ownership, and per-item recovery isolation.

    Attributes:
        db (MagicMock): Public operational persistence API without database IO.
        log (MagicMock): Shared logger interface without filesystem output.
        now (datetime): UTC timestamp used to construct reservation snapshots.
    """

    def setUp(self) -> None:
        """Create fresh handler and logger doubles.

        Args:
            None.
        Returns:
            None.
        """
        self.db = MagicMock(spec=dbHandlerBKP)
        self.db.host_release_reservation.return_value = True
        self.db.host_list_busy_with_task_state.return_value = []
        self.db.host_task_list_with_host_state.return_value = []
        self.log = MagicMock(spec=logging_utils.log)
        self.now = datetime.utcnow()

    def test_recent_cooldown_is_preserved_and_expired_cooldown_is_released(
        self,
    ) -> None:
        """Keep a recent SFTP cooldown even without work, then recover an expired one.

        Args:
            None.
        Returns:
            None.
        """
        recent = self.now
        old = self.now - timedelta(
            seconds=maintenance.k.SFTP_BUSY_COOLDOWN_SECONDS + 10
        )
        self.db.host_list_busy_with_task_state.return_value = [
            {
                "ID_HOST": 1,
                "NU_PID": maintenance.k.HOST_TRANSIENT_BUSY_PID,
                "DT_BUSY": recent,
                "FILE_RUNNING": False,
                "HOST_PROCESSING_RUNNING": False,
            },
            {
                "ID_HOST": 2,
                "NU_PID": maintenance.k.HOST_TRANSIENT_BUSY_PID,
                "DT_BUSY": old,
                "FILE_RUNNING": False,
                "HOST_PROCESSING_RUNNING": False,
            },
        ]
        maintenance.cleanup_stale_host_locks(
            self.db, log=self.log, threshold_seconds=9999
        )
        self.db.host_release_reservation.assert_called_once_with(
            host_id=2,
            owner_pid=maintenance.k.HOST_TRANSIENT_BUSY_PID,
            busy_since=old,
        )
        self.assertEqual(self.db.host_task_enqueue.call_args.kwargs["host_id"], 2)

    def test_lost_release_race_does_not_enqueue_a_connection_check(self) -> None:
        """Do not schedule recovery when the inspected reservation no longer matches.

        Args:
            None.
        Returns:
            None.
        """
        self.db.host_list_busy_with_task_state.return_value = [
            {
                "ID_HOST": 1,
                "NU_PID": 999,
                "DT_BUSY": None,
                "FILE_RUNNING": False,
                "HOST_PROCESSING_RUNNING": False,
            },
        ]
        self.db.host_release_reservation.return_value = False
        maintenance.cleanup_stale_host_locks(
            self.db, log=self.log, threshold_seconds=10
        )
        self.db.host_task_enqueue.assert_not_called()

    def test_empty_work_queue_keeps_a_new_reservation_during_the_grace_period(
        self,
    ) -> None:
        """Preserve a new reservation while its worker has not started a task yet.

        Args:
            None.
        Returns:
            None.
        """
        self.db.host_list_busy_with_task_state.return_value = [
            {
                "ID_HOST": 1,
                "NU_PID": 999,
                "DT_BUSY": self.now,
                "FILE_RUNNING": False,
                "HOST_PROCESSING_RUNNING": False,
            }
        ]
        with patch.object(
            maintenance.tools, "pid_exists", return_value=False
        ) as exists:
            maintenance.cleanup_stale_host_locks(
                self.db, log=self.log, threshold_seconds=0
            )
        exists.assert_not_called()
        self.db.host_release_reservation.assert_not_called()

    def test_running_work_with_a_live_owner_keeps_its_reservation(self) -> None:
        """Preserve old reservations whose running owner still exists.

        Args:
            None.
        Returns:
            None.
        """
        self.db.host_list_busy_with_task_state.return_value = [
            {
                "ID_HOST": 1,
                "NU_PID": 999,
                "DT_BUSY": self.now - timedelta(hours=1),
                "FILE_RUNNING": True,
                "HOST_PROCESSING_RUNNING": False,
            },
        ]
        with patch.object(maintenance.tools, "pid_exists", return_value=True):
            maintenance.cleanup_stale_host_locks(
                self.db, log=self.log, threshold_seconds=10
            )
        self.db.host_release_reservation.assert_not_called()
        self.log.warning_event.assert_called_once()

    def test_pending_task_without_timestamp_is_normalized_without_releasing_host(
        self,
    ) -> None:
        """Supply a missing pending timestamp without changing its state or host owner.

        Args:
            None.
        Returns:
            None.
        """
        self.db.host_task_list_with_host_state.return_value = [
            {
                "ID_HOST_TASK": 2,
                "FK_HOST": 1,
                "NU_STATUS": maintenance.k.TASK_PENDING,
                "DT_HOST_TASK": None,
                "NU_PID": None,
                "IS_BUSY": False,
                "HOST_OWNER_PID": None,
            },
        ]
        maintenance.cleanup_stale_operational_tasks(
            self.db, log=self.log, stale_after_seconds=10
        )
        update = self.db.host_task_update.call_args.kwargs
        self.assertEqual(update["task_id"], 2)
        self.assertIsInstance(update["DT_HOST_TASK"], datetime)
        self.assertNotIn("NU_STATUS", update)
        self.db.host_release_reservation.assert_not_called()
        selected = self.db.host_task_list_with_host_state.call_args.kwargs
        self.assertEqual(len(selected["task_types"]), 6)
        self.assertIn(
            maintenance.k.HOST_TASK_INTERACTIVE_CHECK_TYPE, selected["task_types"]
        )

    def test_stale_task_requeues_but_preserves_a_live_foreign_host_owner(self) -> None:
        """Recover an abandoned task without releasing a newer live host owner.

        Args:
            None.
        Returns:
            None.
        """
        self.db.host_task_list_with_host_state.return_value = [
            {
                "ID_HOST_TASK": 2,
                "FK_HOST": 1,
                "NU_STATUS": maintenance.k.TASK_RUNNING,
                "DT_HOST_TASK": self.now - timedelta(hours=1),
                "NU_PID": 11,
                "IS_BUSY": True,
                "HOST_OWNER_PID": 22,
                "DT_BUSY": self.now,
            },
        ]
        with patch.object(
            maintenance.tools, "pid_exists", side_effect=lambda pid: pid == 22
        ):
            maintenance.cleanup_stale_operational_tasks(
                self.db, log=self.log, stale_after_seconds=10
            )
        self.assertEqual(
            self.db.host_task_update.call_args.kwargs["NU_STATUS"],
            maintenance.k.TASK_PENDING,
        )
        self.db.host_release_reservation.assert_not_called()

    def test_dead_matching_owner_is_released_with_the_observed_timestamp(self) -> None:
        """Use the original reservation snapshot for the conditional release.

        Args:
            None.
        Returns:
            None.
        """
        self.db.host_task_list_with_host_state.return_value = [
            {
                "ID_HOST_TASK": 2,
                "FK_HOST": 1,
                "NU_STATUS": maintenance.k.TASK_RUNNING,
                "DT_HOST_TASK": None,
                "NU_PID": 11,
                "IS_BUSY": True,
                "HOST_OWNER_PID": 11,
                "DT_BUSY": self.now,
            },
        ]
        with patch.object(maintenance.tools, "pid_exists", return_value=False):
            maintenance.cleanup_stale_operational_tasks(
                self.db, log=self.log, stale_after_seconds=10
            )
        self.db.host_release_reservation.assert_called_once_with(
            host_id=1, owner_pid=11, busy_since=self.now
        )

    def test_live_matching_task_owner_is_preserved_past_the_ttl(self) -> None:
        """Avoid requeuing genuinely running work based only on its age.

        Args:
            None.
        Returns:
            None.
        """
        self.db.host_task_list_with_host_state.return_value = [
            {
                "ID_HOST_TASK": 2,
                "FK_HOST": 1,
                "NU_STATUS": maintenance.k.TASK_RUNNING,
                "DT_HOST_TASK": self.now - timedelta(hours=1),
                "NU_PID": 11,
                "IS_BUSY": True,
                "HOST_OWNER_PID": 11,
                "DT_BUSY": self.now,
            },
        ]
        with patch.object(maintenance.tools, "pid_exists", return_value=True):
            maintenance.cleanup_stale_operational_tasks(
                self.db, log=self.log, stale_after_seconds=10
            )
        self.db.host_task_update.assert_not_called()
        self.db.host_release_reservation.assert_not_called()

    def test_one_cleanup_failure_does_not_skip_the_other_step(self) -> None:
        """Run the host-lock pass even when reading operational tasks fails.

        Args:
            None.
        Returns:
            None.
        """
        self.db.host_task_list_with_host_state.side_effect = RuntimeError("unavailable")
        maintenance.run_periodic_host_cleanup(
            db=self.db, log=self.log, task_stale_after_sec=10, host_busy_timeout_sec=20
        )
        self.db.host_list_busy_with_task_state.assert_called_once()
        self.log.error_event.assert_called_once()


if __name__ == "__main__":
    unittest.main()
