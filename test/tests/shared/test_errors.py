"""
Validation tests for `shared.errors`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/shared/test_errors.py -q

What is covered here:
    - first-error retention in `ErrorHandler`
    - formatting and structured logging behavior
    - timeout execution helper behavior
"""

from __future__ import annotations

import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import ensure_app_paths, import_package_module, SHARED_ROOT


ensure_app_paths()

errors = import_package_module("app_shared", SHARED_ROOT, "errors")


class FakeLogger:
    """Small logger double used to assert structured error emission."""

    def __init__(self) -> None:
        self.error_events = []
        self.errors = []

    def error_event(self, event: str, **fields) -> None:
        self.error_events.append((event, fields))

    def error(self, message: str) -> None:
        self.errors.append(message)


class ErrorHandlerTests(unittest.TestCase):
    def test_capture_keeps_first_error_and_context(self) -> None:
        # Long-running workers should preserve the first meaningful failure.
        handler = errors.ErrorHandler(FakeLogger())

        handler.capture("first failure", stage="DISCOVERY", host_id=10)
        handler.capture("second failure", stage="BACKUP", host_id=20)

        self.assertTrue(handler.triggered)
        self.assertEqual(handler.reason, "first failure")
        self.assertEqual(handler.stage, "DISCOVERY")
        self.assertEqual(handler.context, {"host_id": 10})

    def test_format_error_includes_stage_type_and_context(self) -> None:
        # The persisted message must remain readable without the original stack.
        handler = errors.ErrorHandler(FakeLogger())
        handler.capture(
            "Processing failed",
            stage="PROCESS",
            exc=ValueError("bad payload"),
            task_id=99,
        )

        formatted = handler.format_error()

        self.assertIn("[ERROR]", formatted)
        self.assertIn("[stage=PROCESS]", formatted)
        self.assertIn("[type=ValueError]", formatted)
        self.assertIn("Processing failed", formatted)
        self.assertIn("[task_id=99]", formatted)

    def test_format_error_keeps_gps_reason_canonical_and_suffix_specific(self) -> None:
        # GPS aggregation should preserve the stable canonical part while
        # pushing the payload-specific suffix into detail.
        handler = errors.ErrorHandler(FakeLogger())
        handler.capture(
            "Invalid GPS reading: GNSS unavailable sentinel | all spectra in payload failed GPS validation",
            stage="PROCESS",
        )

        formatted = handler.format_error()

        self.assertIn("[code=GPS_GNSS_UNAVAILABLE]", formatted)
        self.assertIn("Invalid GPS reading: GNSS unavailable sentinel", formatted)
        self.assertIn(
            "[detail=all spectra in payload failed GPS validation]",
            formatted,
        )
        self.assertNotIn(
            "[detail=Invalid GPS reading: GNSS unavailable sentinel",
            formatted,
        )

    def test_log_error_uses_structured_logger_when_available(self) -> None:
        # Newer loggers should receive a normalized event instead of plain text.
        logger = FakeLogger()
        handler = errors.ErrorHandler(logger)
        handler.capture("Socket timeout", stage="NETWORK", exc=TimeoutError("boom"))

        handler.log_error(host_id=7)

        self.assertEqual(len(logger.error_events), 1)
        event, payload = logger.error_events[0]
        self.assertEqual(event, "error_handler_triggered")
        self.assertEqual(payload["stage"], "NETWORK")
        self.assertEqual(payload["reason"], "Socket timeout")
        self.assertEqual(payload["host_id"], 7)


class RunWithTimeoutTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        # The production executor lives in shared constants. Tests inject a
        # dedicated one so they do not depend on external bootstrap.
        cls.executor = ThreadPoolExecutor(max_workers=1)
        cls.original_executor = getattr(errors.constants, "_TIMEOUT_EXECUTOR", None)
        errors.constants._TIMEOUT_EXECUTOR = cls.executor

    @classmethod
    def tearDownClass(cls) -> None:
        cls.executor.shutdown(wait=True, cancel_futures=True)

        if cls.original_executor is None:
            delattr(errors.constants, "_TIMEOUT_EXECUTOR")
        else:
            errors.constants._TIMEOUT_EXECUTOR = cls.original_executor

    def test_run_with_timeout_returns_function_result(self) -> None:
        result = errors.run_with_timeout(lambda: "ok", timeout=0.5)
        self.assertEqual(result, "ok")

    def test_run_with_timeout_raises_timeout_error(self) -> None:
        # The wrapper must translate the futures timeout into the domain timeout.
        with self.assertRaises(errors.TimeoutError):
            errors.run_with_timeout(lambda: time.sleep(0.05), timeout=0.001)


if __name__ == "__main__":
    unittest.main()
