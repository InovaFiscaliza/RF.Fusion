"""
Validation tests for `stations.appAnaliseConnection`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/stations/test_appanalise_connection.py -q

What is covered here:
    - protocol-shape validation before BIN normalization begins
    - distinction between valid payloads and service/protocol failures
"""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import (
    APP_ROOT,
    bind_real_shared_package,
    ensure_app_paths,
    load_module_from_path,
)


ensure_app_paths()

# The production module imports `shared` by package name. During test discovery,
# `/RFFusion/test/tests/shared` can shadow the real package, so we bind the
# source-tree package only for this import window.
with bind_real_shared_package():
    app_analise_module = load_module_from_path(
        "test_appanalise_connection_module",
        str(APP_ROOT / "stations" / "appAnaliseConnection.py"),
    )
AppAnaliseConnection = app_analise_module.AppAnaliseConnection
AppAnaliseErrors = app_analise_module.errors


class DetectProtocolErrorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = AppAnaliseConnection()

    def test_detect_protocol_error_accepts_valid_payload(self) -> None:
        # Minimal success contract: `Answer` is a dict and exposes `Spectra`.
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {"Spectra": [], "General": {}},
        }

        self.conn._detect_protocol_error(payload)

    def test_detect_protocol_error_rejects_answer_string(self) -> None:
        # Real failures from appAnalise can arrive as plain strings in `Answer`.
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": "tcpServerLib:FileNotFound",
        }

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.conn._detect_protocol_error(payload)

        self.assertIn("APP_ANALISE returned error in Answer", str(ctx.exception))

    def test_detect_protocol_error_retries_missing_file_when_source_still_exists(self) -> None:
        # If MATLAB says "FileNotFound" but the requested source file still
        # exists locally, that looks more like a service-side visibility issue
        # than a definitively bad processing task.
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": "tcpServerLib:FileNotFound",
        }

        with tempfile.NamedTemporaryFile() as tmp_file:
            with self.assertRaises(
                AppAnaliseErrors.ExternalServiceTransientError
            ) as ctx:
                self.conn._detect_protocol_error(
                    payload,
                    requested_full_path=tmp_file.name,
                )

        self.assertIn("still exists locally", str(ctx.exception))

    def test_detect_protocol_error_rejects_missing_file_when_source_is_gone(self) -> None:
        # The same protocol error becomes definitive if the source file really
        # is absent from the filesystem watched by appCataloga.
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": "tcpServerLib:FileNotFound",
        }
        missing_path = "/tmp/appCataloga_missing_input.bin"

        if os.path.exists(missing_path):
            os.unlink(missing_path)

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.conn._detect_protocol_error(
                payload,
                requested_full_path=missing_path,
            )

        self.assertIn("absent locally", str(ctx.exception))

    def test_detect_protocol_error_rejects_missing_spectra(self) -> None:
        # A dict-shaped `Answer` is still invalid if the expected payload body is missing.
        payload = {
            "Request": {"type": "FileRead"},
            "Answer": {"General": {}},
        }

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.conn._detect_protocol_error(payload)

        self.assertIn("Answer.Spectra", str(ctx.exception))

    def test_detect_protocol_error_rejects_top_level_error_field(self) -> None:
        # The protocol also supports an explicit top-level `Error` field.
        payload = {"Error": ["tcpServerLib", "PortClosed"]}

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.conn._detect_protocol_error(payload)

        self.assertIn("tcpServerLib - PortClosed", str(ctx.exception))

    def test_process_rejects_missing_source_before_request(self) -> None:
        # The worker should fail early if the source file is already gone,
        # instead of contacting appAnalise and producing a misleading retry.
        called = {"value": False}

        def fake_request_process(*args, **kwargs):
            called["value"] = True
            return {}

        self.conn._request_process = fake_request_process

        with self.assertRaises(AppAnaliseErrors.BinValidationError) as ctx:
            self.conn.process("/tmp", "appCataloga_missing_source.bin", export=True)

        self.assertIn("source file unavailable before request", str(ctx.exception))
        self.assertFalse(called["value"])


if __name__ == "__main__":
    unittest.main()
