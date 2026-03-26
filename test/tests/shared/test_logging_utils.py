"""
Validation tests for `shared.logging_utils`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/shared/test_logging_utils.py -q

What is covered here:
    - default "one entrypoint, one log file" naming
    - per-file rotation and retention behavior
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import ensure_app_paths, import_package_module, SHARED_ROOT


ensure_app_paths()

logging_utils = import_package_module("app_shared", SHARED_ROOT, "logging_utils")


class LoggingUtilsTests(unittest.TestCase):
    def test_logger_uses_one_default_file_per_entrypoint(self) -> None:
        # Each daemon should naturally land in its own file such as
        # `appCataloga_discovery.log` unless the caller overrides the path.
        logger = logging_utils.log(
            "appCataloga_discovery",
            target_file=False,
            target_screen=False,
            verbose=True,
        )

        self.assertTrue(
            logger.log_file_name.endswith("/appCataloga_discovery.log")
        )

    def test_rotation_is_applied_per_log_file(self) -> None:
        # Rotating one daemon log must not touch another daemon's file.
        with tempfile.TemporaryDirectory() as tmpdir:
            discovery_path = os.path.join(tmpdir, "appCataloga_discovery.log")
            maintenance_path = os.path.join(
                tmpdir,
                "appCataloga_host_maintenance.log",
            )

            discovery_logger = logging_utils.log(
                "appCataloga_discovery",
                target_file=True,
                target_screen=False,
                verbose=True,
                log_file_name=discovery_path,
            )
            maintenance_logger = logging_utils.log(
                "appCataloga_host_maintenance",
                target_file=True,
                target_screen=False,
                verbose=True,
                log_file_name=maintenance_path,
            )

            discovery_logger.max_file_size_bytes = 250
            discovery_logger.max_backup_files = 2
            maintenance_logger.max_file_size_bytes = 10_000
            maintenance_logger.max_backup_files = 2

            for index in range(12):
                discovery_logger.entry(
                    f"discovery iteration={index} payload={'x' * 80}"
                )

            maintenance_logger.entry("maintenance heartbeat")

            discovery_logger.close()
            maintenance_logger.close()

            self.assertTrue(os.path.exists(discovery_path))
            self.assertTrue(os.path.exists(f"{discovery_path}.1"))
            self.assertTrue(os.path.exists(f"{discovery_path}.2"))
            self.assertFalse(os.path.exists(f"{discovery_path}.3"))

            self.assertTrue(os.path.exists(maintenance_path))
            self.assertFalse(os.path.exists(f"{maintenance_path}.1"))


if __name__ == "__main__":
    unittest.main()
