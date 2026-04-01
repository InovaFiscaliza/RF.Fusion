"""
Validation tests for `safe_stop.py`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_safe_stop.py -q

What is covered here:
    - stale backup `.tmp` leftovers are purged only from the repository TMP area
    - empty per-host TMP directories are pruned after cleanup
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
    bind_real_package,
    bind_real_shared_package,
    ensure_app_paths,
    load_module_from_path,
)


ensure_app_paths()

with bind_real_shared_package():
    with bind_real_package("db", DB_ROOT):
        safe_stop = load_module_from_path(
            "test_safe_stop_module",
            str(APP_ROOT / "safe_stop.py"),
        )


class FakeLog:
    """Capture cleanup events without touching the production logger."""

    def __init__(self) -> None:
        self.entries = []
        self.warnings = []
        self.errors = []

    def entry(self, message: str) -> None:
        self.entries.append(message)

    def warning(self, message: str) -> None:
        self.warnings.append(message)

    def error(self, message: str) -> None:
        self.errors.append(message)


class SafeStopTmpCleanupTests(unittest.TestCase):
    """Validate shutdown cleanup of repository backup leftovers."""

    def test_cleanup_repository_tmp_files_deletes_only_tmp_files(self) -> None:
        fake_log = FakeLog()

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_tmp_root = Path(tmpdir) / "reposfi" / "tmp"
            host_dir = repo_tmp_root / "CWSM211004"
            host_dir.mkdir(parents=True)

            stale_tmp = host_dir / "payload.zip.tmp"
            keep_zip = host_dir / "payload.zip"
            keep_note = host_dir / "notes.txt"

            stale_tmp.write_text("partial", encoding="utf-8")
            keep_zip.write_text("done", encoding="utf-8")
            keep_note.write_text("keep", encoding="utf-8")

            with patch.object(safe_stop, "log", fake_log):
                deleted = safe_stop.cleanup_repository_tmp_files(
                    repo_tmp_root=str(repo_tmp_root)
                )

            self.assertEqual(deleted, 1)
            self.assertFalse(stale_tmp.exists())
            self.assertTrue(keep_zip.exists())
            self.assertTrue(keep_note.exists())

        self.assertFalse(fake_log.errors)

    def test_prune_empty_repository_tmp_dirs_removes_only_empty_descendants(self) -> None:
        fake_log = FakeLog()

        with tempfile.TemporaryDirectory() as tmpdir:
            repo_tmp_root = Path(tmpdir) / "reposfi" / "tmp"
            empty_host_dir = repo_tmp_root / "CWSM211004"
            active_host_dir = repo_tmp_root / "RFEye002211"
            empty_host_dir.mkdir(parents=True)
            active_host_dir.mkdir(parents=True)

            (empty_host_dir / "stale.bin.tmp").write_text("partial", encoding="utf-8")
            (active_host_dir / "final.bin").write_text("valid", encoding="utf-8")

            with patch.object(safe_stop, "log", fake_log):
                safe_stop.cleanup_repository_tmp_files(repo_tmp_root=str(repo_tmp_root))
                removed = safe_stop.prune_empty_repository_tmp_dirs(
                    repo_tmp_root=str(repo_tmp_root)
                )

            self.assertEqual(removed, 1)
            self.assertFalse(empty_host_dir.exists())
            self.assertTrue(active_host_dir.exists())
            self.assertTrue(repo_tmp_root.exists())
        self.assertFalse(fake_log.errors)


if __name__ == "__main__":
    unittest.main()
