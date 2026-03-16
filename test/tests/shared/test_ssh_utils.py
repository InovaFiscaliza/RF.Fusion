"""
Validation tests for transient SSH/SFTP classification in `shared.errors`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/shared/test_ssh_utils.py -q

What is covered here:
    - classification of transient SSH/SFTP initialization failures
    - distinction between retryable transport noise and fatal auth/protocol errors
"""

from __future__ import annotations

import socket
import unittest
from pathlib import Path
import sys

import paramiko

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import SHARED_ROOT, ensure_app_paths, import_package_module


ensure_app_paths()

errors = import_package_module("app_shared", SHARED_ROOT, "errors")


class SftpInitErrorClassificationTests(unittest.TestCase):
    def test_marks_no_valid_connections_as_transient(self) -> None:
        exc = paramiko.ssh_exception.NoValidConnectionsError(
            {("127.0.0.1", 22): ConnectionRefusedError(111, "refused")}
        )

        self.assertTrue(errors.is_transient_sftp_init_error(exc))

    def test_marks_banner_timeout_style_ssh_exception_as_transient(self) -> None:
        exc = paramiko.SSHException("Error reading SSH protocol banner")

        self.assertTrue(errors.is_transient_sftp_init_error(exc))

    def test_marks_socket_timeout_as_transient(self) -> None:
        self.assertTrue(
            errors.is_transient_sftp_init_error(socket.timeout("timed out"))
        )

    def test_marks_authentication_failure_as_non_transient(self) -> None:
        exc = paramiko.AuthenticationException("bad credentials")

        self.assertFalse(errors.is_transient_sftp_init_error(exc))

    def test_marks_generic_value_error_as_non_transient(self) -> None:
        self.assertFalse(
            errors.is_transient_sftp_init_error(ValueError("bad config"))
        )


if __name__ == "__main__":
    unittest.main()
