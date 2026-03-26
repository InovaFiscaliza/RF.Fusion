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

from _support import (
    HOST_HANDLER_ROOT,
    SHARED_ROOT,
    bind_real_shared_package,
    ensure_app_paths,
    import_package_module,
)


ensure_app_paths()

with bind_real_shared_package():
    errors = import_package_module("app_shared", SHARED_ROOT, "errors")
    ssh_utils = import_package_module("app_host_handler", HOST_HANDLER_ROOT, "ssh_utils")


class FakeTransport:
    """Tiny transport double exposing the tuning knobs touched by ssh_utils."""

    def __init__(self) -> None:
        self.keepalive = None
        self.packetizer = type("Packetizer", (), {})()
        self.packetizer.REKEY_BYTES = None
        self.packetizer.REKEY_PACKETS = None
        self.window_size = None

    def set_keepalive(self, value) -> None:
        self.keepalive = value


class FakeSSHClient:
    """Small Paramiko stand-in that records the chosen connect address."""

    def __init__(self) -> None:
        self.connect_calls = []
        self.transport = FakeTransport()
        self.closed = False

    def set_missing_host_key_policy(self, _policy) -> None:
        pass

    def connect(self, **kwargs) -> None:
        self.connect_calls.append(kwargs)

    def get_transport(self):
        return self.transport

    def open_sftp(self):
        return object()

    def close(self) -> None:
        self.closed = True


class SftpInitErrorClassificationTests(unittest.TestCase):
    """Validate classification of retryable versus fatal SSH/SFTP bootstrap errors."""

    def test_classifies_no_valid_connections_timeout(self) -> None:
        exc = paramiko.ssh_exception.NoValidConnectionsError(
            {("127.0.0.1", 22): TimeoutError("timed out")}
        )

        details = errors.classify_no_valid_connections_error(exc)

        self.assertEqual(details["summary"], "timeout")
        self.assertTrue(details["has_timeout"])

    def test_classifies_no_valid_connections_refused(self) -> None:
        exc = paramiko.ssh_exception.NoValidConnectionsError(
            {("127.0.0.1", 22): ConnectionRefusedError(111, "refused")}
        )

        details = errors.classify_no_valid_connections_error(exc)

        self.assertEqual(details["summary"], "refused")
        self.assertTrue(details["has_refused"])

    def test_classifies_no_valid_connections_mixed(self) -> None:
        exc = paramiko.ssh_exception.NoValidConnectionsError(
            {
                ("127.0.0.1", 22): ConnectionRefusedError(111, "refused"),
                ("::1", 22): TimeoutError("timed out"),
            }
        )

        details = errors.classify_no_valid_connections_error(exc)

        self.assertEqual(details["summary"], "mixed")
        self.assertTrue(details["has_refused"])
        self.assertTrue(details["has_timeout"])

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

    def test_marks_socket_timeout_as_timeout_like(self) -> None:
        self.assertTrue(
            errors.is_timeout_like_sftp_init_error(socket.timeout("timed out"))
        )

    def test_marks_connection_refused_as_not_timeout_like(self) -> None:
        exc = paramiko.ssh_exception.NoValidConnectionsError(
            {("127.0.0.1", 22): ConnectionRefusedError(111, "refused")}
        )

        self.assertFalse(errors.is_timeout_like_sftp_init_error(exc))

    def test_uses_timeout_retry_detail_for_timeout_like_errors(self) -> None:
        detail = errors.get_transient_sftp_retry_detail(socket.timeout("timed out"))

        self.assertEqual(detail, errors.k.SSH_TIMEOUT_RETRY_DETAIL)

    def test_uses_busy_retry_detail_for_non_timeout_transient_errors(self) -> None:
        exc = paramiko.ssh_exception.NoValidConnectionsError(
            {("127.0.0.1", 22): ConnectionRefusedError(111, "refused")}
        )

        detail = errors.get_transient_sftp_retry_detail(exc)

        self.assertEqual(detail, errors.k.SFTP_BUSY_RETRY_DETAIL)

    def test_marks_authentication_failure_as_non_transient(self) -> None:
        exc = paramiko.AuthenticationException("bad credentials")

        self.assertFalse(errors.is_transient_sftp_init_error(exc))

    def test_marks_authentication_timeout_as_transient(self) -> None:
        exc = paramiko.AuthenticationException("Authentication timeout.")

        self.assertTrue(errors.is_transient_sftp_init_error(exc))

    def test_marks_authentication_timeout_as_timeout_like(self) -> None:
        exc = paramiko.AuthenticationException("Authentication timeout.")

        self.assertTrue(errors.is_timeout_like_sftp_init_error(exc))

    def test_marks_generic_value_error_as_non_transient(self) -> None:
        self.assertFalse(
            errors.is_transient_sftp_init_error(ValueError("bad config"))
        )


class SshConnectionResolutionTests(unittest.TestCase):
    """Validate address resolution rules used before opening the SFTP session."""

    def test_sftp_connection_prefers_primary_172_address(self) -> None:
        fake_client = FakeSSHClient()

        with unittest.mock.patch.object(
            ssh_utils.host_connectivity,
            "resolve_primary_host_address",
            return_value="172.24.1.147",
        ):
            with unittest.mock.patch.object(
                ssh_utils.paramiko,
                "SSHClient",
                return_value=fake_client,
            ):
                conn = ssh_utils.sftpConnection(
                    host_uid="RFEye002158",
                    host_addr="rfeye002158.anatel.gov.br",
                    port=2828,
                    user="root",
                    password="secret",
                    log=type("FakeLog", (), {"entry": lambda *a, **k: None, "error": lambda *a, **k: None})(),
                )

        self.assertEqual(conn.connect_addr, "172.24.1.147")
        self.assertEqual(fake_client.connect_calls[0]["hostname"], "172.24.1.147")


if __name__ == "__main__":
    unittest.main()
