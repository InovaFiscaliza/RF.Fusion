"""
Validation tests for `shared.file_utils`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/shared/test_file_utils.py -q

What is covered here:
    - MD5 checksum of file contents, including multi-block reads
"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path
import sys
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import ensure_app_paths, import_package_module, SHARED_ROOT


ensure_app_paths()


file_utils = import_package_module("app_shared", SHARED_ROOT, "file_utils")


class ComputeFileMd5Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def test_digest_matches_content_across_multiple_blocks(self) -> None:
        payload = bytes(range(256)) * 10
        path = self.root / "payload.bin"
        path.write_bytes(payload)

        # A small block size forces several reads, including a partial tail.
        with mock.patch.object(file_utils.k, "FILE_HASH_CHUNK_SIZE_BYTES", 7):
            digest = file_utils.compute_file_md5(str(path))

        self.assertEqual(digest, hashlib.md5(payload).hexdigest())

    def test_renamed_copy_has_same_digest(self) -> None:
        original = self.root / "015708-SMA_espectros.bin"
        backup = self.root / "p-3102b642--015708-SMA_espectros.bin"
        original.write_bytes(b"spectrum")
        backup.write_bytes(b"spectrum")

        self.assertEqual(
            file_utils.compute_file_md5(str(original)),
            file_utils.compute_file_md5(str(backup)),
        )

    def test_empty_file_returns_md5_of_empty_content(self) -> None:
        path = self.root / "empty.bin"
        path.write_bytes(b"")

        self.assertEqual(
            file_utils.compute_file_md5(str(path)),
            "d41d8cd98f00b204e9800998ecf8427e",
        )

    def test_missing_file_raises(self) -> None:
        with self.assertRaises(FileNotFoundError):
            file_utils.compute_file_md5(str(self.root / "missing.bin"))


if __name__ == "__main__":
    unittest.main()
