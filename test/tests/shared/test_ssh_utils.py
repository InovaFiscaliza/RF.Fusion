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
import tempfile
import time
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
    ssh_utils = import_package_module("app_host_handler", HOST_HANDLER_ROOT, "host_ssh_utils")


class FakeTransport:
    """Tiny transport double exposing the tuning knobs touched by ssh_utils."""

    def __init__(self) -> None:
        self.keepalive = None
        self.closed = False
        self.packetizer = type("Packetizer", (), {})()
        self.packetizer.REKEY_BYTES = None
        self.packetizer.REKEY_PACKETS = None
        self.window_size = None

    def set_keepalive(self, value) -> None:
        self.keepalive = value

    def close(self) -> None:
        self.closed = True


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
        self.transport.close()


class FakeTransferLog:
    """Capture transfer watchdog logs without using the real logger."""

    def __init__(self) -> None:
        self.entries = []
        self.errors = []
        self.warnings = []

    def entry(self, message: str, **kwargs) -> None:
        if kwargs:
            message = f"{message} {kwargs}".strip()
        self.entries.append(message)

    def event(self, message: str = "", **kwargs) -> None:
        if kwargs:
            message = f"{message} {kwargs}".strip()
        self.entries.append(message)

    def warning_event(self, message: str = "", **kwargs) -> None:
        if kwargs:
            message = f"{message} {kwargs}".strip()
        self.warnings.append(message)

    def error_event(self, message: str = "", **kwargs) -> None:
        if kwargs:
            message = f"{message} {kwargs}".strip()
        self.errors.append(message)

    def error(self, message: str) -> None:
        self.errors.append(message)

    def warning(self, message: str) -> None:
        self.warnings.append(message)


class SshConnectionResolutionTests(unittest.TestCase):
    """Validate address assignment rules used before opening the SFTP session."""

    def test_sftp_connection_uses_provided_address(self) -> None:
        # Resolution is the caller's responsibility; sftpConnection connects
        # to whatever address it receives verbatim.
        fake_client = FakeSSHClient()

        with unittest.mock.patch.object(
            ssh_utils.paramiko,
            "SSHClient",
            return_value=fake_client,
        ):
            fake_log = type(
                "FakeLog",
                (),
                {
                    "event": lambda *a, **k: None,
                    "warning_event": lambda *a, **k: None,
                    "error_event": lambda *a, **k: None,
                },
            )()
            conn = ssh_utils.sftpConnection(
                host_uid="RFEye002158",
                host_addr="172.24.1.147",
                port=2828,
                user="root",
                password="secret",
                log=fake_log,
            )

        self.assertEqual(conn.connect_addr, "172.24.1.147")
        self.assertEqual(fake_client.connect_calls[0]["hostname"], "172.24.1.147")


class TransferWatchdogTests(unittest.TestCase):
    """Validate the progress watchdog used by backup SFTP transfers."""

    def _build_connection(self, *, sftp, ssh_client=None):
        conn = ssh_utils.sftpConnection.__new__(ssh_utils.sftpConnection)
        conn.host_uid = "RFEye002158"
        conn.host_addr = "rfeye002158.anatel.gov.br"
        conn.log = FakeTransferLog()
        conn.sftp = sftp
        conn.ssh_client = ssh_client or FakeSSHClient()
        return conn

    def test_transfer_allows_slow_but_progressing_download(self) -> None:
        class StreamingSFTP:
            def __init__(self) -> None:
                self.closed = False

            def get(self, _remote_file, local_file, callback=None) -> None:
                transferred = 0
                total = 6
                Path(local_file).write_bytes(b"")

                for chunk_size in (2, 2, 2):
                    if self.closed:
                        raise OSError("channel closed")
                    with open(local_file, "ab") as fh:
                        fh.write(b"x" * chunk_size)
                    transferred += chunk_size
                    if callback is not None:
                        callback(transferred, total)
                    time.sleep(0.03)

            def close(self) -> None:
                self.closed = True

        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = str(Path(tmpdir) / "sample.bin")
            conn = self._build_connection(sftp=StreamingSFTP())

            conn.transfer(
                "/remote/sample.bin",
                local_file,
                max_seconds=1.0,
                stall_timeout_seconds=0.2,
                progress_poll_seconds=0.01,
                heartbeat_seconds=0.02,
            )

            self.assertEqual(Path(local_file).read_bytes(), b"x" * 6)
            self.assertFalse(
                any("backup_transfer_abort" in entry for entry in conn.log.entries)
            )

    def test_transfer_aborts_when_progress_stalls(self) -> None:
        class StallingSFTP:
            def __init__(self) -> None:
                self.closed = False

            def get(self, _remote_file, local_file, callback=None) -> None:
                Path(local_file).write_bytes(b"xx")
                if callback is not None:
                    callback(2, 6)

                while not self.closed:
                    time.sleep(0.01)

                raise OSError("channel closed")

            def close(self) -> None:
                self.closed = True

        fake_ssh = FakeSSHClient()

        with tempfile.TemporaryDirectory() as tmpdir:
            local_file = str(Path(tmpdir) / "sample.bin")
            conn = self._build_connection(
                sftp=StallingSFTP(),
                ssh_client=fake_ssh,
            )

            with self.assertRaises(TimeoutError):
                conn.transfer(
                    "/remote/sample.bin",
                    local_file,
                    max_seconds=1.0,
                    stall_timeout_seconds=0.05,
                    progress_poll_seconds=0.01,
                    heartbeat_seconds=0,
                )

            self.assertTrue(conn.sftp.closed)
            self.assertTrue(fake_ssh.closed)
            self.assertTrue(
                any("backup_transfer_abort" in entry for entry in conn.log.entries)
            )


if __name__ == "__main__":
    unittest.main()
