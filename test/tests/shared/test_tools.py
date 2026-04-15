"""
Validation tests for `shared.tools`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/shared/test_tools.py -q

What is covered here:
    - standardized audit message formatting
    - timestamp normalization from PowerShell ISO strings
"""

from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import ensure_app_paths, import_package_module, SHARED_ROOT


ensure_app_paths()

import config as k

tools = import_package_module("app_shared", SHARED_ROOT, "tools")


class ComposeMessageTests(unittest.TestCase):
    def test_compose_message_with_file_detail_and_error(self) -> None:
        # Main contract: the message must be deterministic and audit-friendly.
        message = tools.compose_message(
            task_type=k.FILE_TASK_PROCESS_TYPE,
            task_status=k.TASK_ERROR,
            path="/tmp/data",
            name="sample.zip",
            detail="worker=APP_ANALISE",
            error="[ERROR] failed",
        )

        self.assertEqual(
            message,
            "Processing Error | file=/tmp/data/sample.zip | worker=APP_ANALISE | [ERROR] failed",
        )

    def test_compose_message_prefix_only(self) -> None:
        # Some callers persist only the status prefix and append details later.
        self.assertEqual(
            tools.compose_message(
                task_type=k.FILE_TASK_BACKUP_TYPE,
                task_status=k.TASK_RUNNING,
                prefix_only=True,
            ),
            "Backup Running",
        )

    def test_compose_message_path_without_name(self) -> None:
        # Discovery can know the directory before the final file name exists.
        self.assertEqual(
            tools.compose_message(
                task_type=k.FILE_TASK_DISCOVERY,
                task_status=k.TASK_PENDING,
                path="/mnt/reposfi/tmp",
            ),
            "Discovery Pending | path=/mnt/reposfi/tmp",
        )

    def test_compose_message_supports_frozen_status(self) -> None:
        self.assertEqual(
            tools.compose_message(
                task_type=k.FILE_TASK_PROCESS_TYPE,
                task_status=k.TASK_FROZEN,
                detail="manual review",
            ),
            "Processing Frozen | manual review",
        )


class ParsePsIsoTests(unittest.TestCase):
    def test_parse_ps_iso_truncates_ticks_and_timezone(self) -> None:
        # PowerShell can emit more than 6 digits for fractional seconds.
        value = tools.parse_ps_iso("2025-01-31T20:18:51.4479289-03:00")

        self.assertEqual(
            value,
            datetime(2025, 1, 31, 20, 18, 51, 447928),
        )


if __name__ == "__main__":
    unittest.main()
