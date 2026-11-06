"""Verify backlog policy through public persistence APIs without starting a daemon."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, call, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _support import APP_ROOT, bind_test_config, load_module_from_path

sys.path.insert(0, str(APP_ROOT))
with bind_test_config():
    from db.dbHandlerBKP import dbHandlerBKP
    from server_handler import signal_runtime
    from shared import logging_utils

    test_log = MagicMock(spec=logging_utils.log)
    with patch.object(logging_utils, "log", return_value=test_log), patch.object(
        signal_runtime, "install_shutdown_handlers"
    ):
        worker = load_module_from_path(
            "test_postgres_backlog_worker",
            str(APP_ROOT / "appCataloga_backlog_management.py"),
        )


class BacklogManagementWorkerTests(unittest.TestCase):
    """Protect volume, ordering, and transaction ownership in the worker.

    Attributes:
        db (MagicMock): Public operational-handler interface without database IO.
        log (MagicMock): Structured logger double without filesystem output.
    """

    def setUp(self) -> None:
        """Create fresh persistence and logging doubles.

        Args:
            None.
        Returns:
            None.
        """
        self.db = MagicMock(spec=dbHandlerBKP)
        self.db.in_transaction = False
        self.db.file_task_update_backlog.return_value = 1
        self.log = MagicMock(spec=logging_utils.log)

    def promote(self, task_filter: dict[str, object]) -> dict[str, int]:
        """Apply the actual promotion policy with a controlled filter.

        Args:
            task_filter (dict[str, object]): Shared filter payload under test.
        Returns:
            dict[str, int]: Worker's transition summary.
        """
        return worker._update_backlog(
            db=self.db,
            logger=self.log,
            host_id=1,
            task_filter=task_filter,
            search_type=worker.k.FILE_TASK_DISCOVERY,
            search_status=worker.k.TASK_DONE,
            new_type=worker.k.FILE_TASK_BACKUP_TYPE,
            new_status=worker.k.TASK_PENDING,
        )

    def test_budget_selects_only_the_ordered_prefix_and_publishes_before_commit(
        self,
    ) -> None:
        """Stop at the first oversized file even when a later file would fit.

        Args:
            None.
        Returns:
            None.
        """
        self.db.file_task_list_for_backlog.return_value = [
            {"ID_FILE_TASK": 1, "VL_FILE_SIZE_KB_HOST": 20},
            {"ID_FILE_TASK": 2, "VL_FILE_SIZE_KB_HOST": 40},
            {"ID_FILE_TASK": 3, "VL_FILE_SIZE_KB_HOST": 1},
        ]
        result = self.promote(
            {"mode": "ALL", "max_total_gb": 50 / worker.k.KILOBYTES_PER_GB}
        )
        self.assertEqual(result["selected_total_kb"], 20)
        self.assertEqual(result["moved_to_backup"], 1)
        update = self.db.file_task_update_backlog.call_args.kwargs
        self.assertEqual(update["where"]["ID_FILE_TASK__in"], [1])
        self.assertEqual(update["where"]["NU_TYPE"], worker.k.FILE_TASK_DISCOVERY)
        self.assertEqual(update["where"]["NU_STATUS"], worker.k.TASK_DONE)
        self.assertIsNone(update["data"]["NA_ERROR_CODE"])
        self.db.host_request_summary_refresh.assert_called_once_with(
            host_id=1, reason="backlog_transition"
        )
        self.assertLess(
            self.db.mock_calls.index(
                call.host_request_summary_refresh(
                    host_id=1, reason="backlog_transition"
                )
            ),
            self.db.mock_calls.index(call.commit()),
        )
        self.db.rollback.assert_not_called()

    def test_last_selects_newest_rows_with_an_explicit_limit(self) -> None:
        """Keep LAST descending even when an oldest-first sort was requested.

        Args:
            None.
        Returns:
            None.
        """
        self.db.file_task_list_for_backlog.return_value = [
            {"ID_FILE_TASK": 9, "VL_FILE_SIZE_KB_HOST": 2}
        ]
        self.promote({"mode": "LAST", "last_n_files": 1, "sort_order": "oldest_first"})
        selection = self.db.file_task_list_for_backlog.call_args.kwargs
        self.assertEqual(selection["limit"], 1)
        self.assertEqual(
            selection["order_by"],
            (("DT_FILE_CREATED_HOST", "DESC NULLS LAST"), ("ID_FILE_TASK", "DESC")),
        )

    def test_empty_prefix_commits_without_writing_or_publishing(self) -> None:
        """Preserve the queue when the first candidate exceeds the volume budget.

        Args:
            None.
        Returns:
            None.
        """
        self.db.file_task_list_for_backlog.return_value = [
            {"ID_FILE_TASK": 1, "VL_FILE_SIZE_KB_HOST": 80}
        ]
        result = self.promote(
            {"mode": "ALL", "max_total_gb": 50 / worker.k.KILOBYTES_PER_GB}
        )
        self.assertEqual(result["rows_updated"], 0)
        self.db.file_task_update_backlog.assert_not_called()
        self.db.host_request_summary_refresh.assert_not_called()
        self.db.commit.assert_called_once_with()

    def test_none_filter_does_not_start_a_transaction(self) -> None:
        """Do no persistence work for the neutral filter.

        Args:
            None.
        Returns:
            None.
        """
        self.assertEqual(self.promote({"mode": "NONE"})["rows_updated"], 0)
        self.db.begin_transaction.assert_not_called()
        self.db.file_task_update_backlog.assert_not_called()

    def test_failure_rolls_back_only_a_self_owned_transaction(self) -> None:
        """Leave an external transaction to its caller after a failed write.

        Args:
            None.
        Returns:
            None.
        """
        self.db.file_task_update_backlog.side_effect = RuntimeError("write failed")
        for external in (False, True):
            with self.subTest(external=external):
                self.db.reset_mock()
                self.db.in_transaction = external
                with self.assertRaises(RuntimeError):
                    self.promote({"mode": "ALL"})
                self.assertEqual(self.db.rollback.call_count, int(not external))
                self.assertEqual(
                    self.db.begin_transaction.call_count, int(not external)
                )
                self.db.commit.assert_not_called()
                self.db.host_request_summary_refresh.assert_not_called()

    def test_external_success_does_not_commit_and_unknown_commit_is_not_replayed(
        self,
    ) -> None:
        """Preserve transaction ownership and an uncertain self-owned commit result.

        Args:
            None.
        Returns:
            None.
        """
        self.db.in_transaction = True
        self.promote({"mode": "ALL"})
        self.db.commit.assert_not_called()
        self.db.begin_transaction.assert_not_called()
        self.db.reset_mock()
        self.db.in_transaction = False
        self.db.commit.side_effect = RuntimeError("unknown commit result")
        with self.assertRaises(RuntimeError):
            self.promote({"mode": "ALL"})
        self.db.file_task_update_backlog.assert_called_once()
        self.db.rollback.assert_not_called()

    def test_worker_promotes_and_rolls_back_without_duplicate_publication(self) -> None:
        """Keep task-specific source/target states and cancel promotions before STOP.

        Args:
            None.
        Returns:
            None.
        """
        for task_type in (
            worker.k.HOST_TASK_BACKLOG_CONTROL_TYPE,
            worker.k.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
        ):
            with self.subTest(task_type=task_type):
                self.db.reset_mock()
                task = {
                    "host_id": 1,
                    "task_id": 2,
                    "task_type": task_type,
                    "host_filter": {"mode": "ALL"},
                    "now": worker.datetime.utcnow(),
                }
                with patch.object(worker, "log", self.log):
                    result = worker._do_work(self.db, task)
                self.db.host_request_summary_refresh.assert_called_once()
                update = self.db.file_task_update_backlog.call_args.kwargs
                if task_type == worker.k.HOST_TASK_BACKLOG_CONTROL_TYPE:
                    self.assertEqual(result["action"], "promote")
                    self.assertEqual(
                        update["data"]["NU_TYPE"], worker.k.FILE_TASK_BACKUP_TYPE
                    )
                else:
                    self.assertEqual(result["action"], "rollback")
                    self.assertEqual(
                        update["data"]["NU_TYPE"], worker.k.FILE_TASK_DISCOVERY
                    )
                    self.assertEqual(self.db.mock_calls[0][0], "host_task_update")


if __name__ == "__main__":
    unittest.main()
