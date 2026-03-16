"""
Validation tests for `stations.appAnaliseConnection`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/stations/test_appanalise_connection.py -q

What is covered here:
    - protocol-shape validation before BIN normalization begins
    - distinction between valid payloads and service/protocol failures
"""

from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
