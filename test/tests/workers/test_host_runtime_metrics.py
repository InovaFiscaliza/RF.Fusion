"""Tests for current operational metric persistence on the summary snapshot."""

from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import APP_ROOT, bind_real_package, ensure_app_paths, load_module_from_path


ensure_app_paths()

with bind_real_package("host_handler", APP_ROOT / "host_handler"):
    host_runtime = load_module_from_path(
        "test_host_runtime_metrics_module",
        str(APP_ROOT / "host_handler" / "host_runtime.py"),
    )


class FakeSignalDb:
    """Record canonical snapshot signal updates."""

    def __init__(self) -> None:
        self.signal_updates = []

    def update_host_current_snapshot_signals(self, **kwargs) -> None:
        self.signal_updates.append(kwargs)


class HostRuntimeMetricTests(unittest.TestCase):
    """Protect current-state updates that feed the summary snapshot."""

    def test_discovery_outcome_keeps_filter_watermark_unchanged(self) -> None:
        db = FakeSignalDb()
        completed_at = datetime(2026, 7, 29, 12, 0, 0)

        with patch.object(host_runtime, "_get_snapshot_signal_db", return_value=db):
            host_runtime.record_discovery_outcome(
                12,
                completed_at=completed_at,
                discovered_file_count=0,
                discovered_volume_kb=0,
                logger=object(),
            )

        self.assertEqual(
            db.signal_updates,
            [
                {
                    "host_id": 12,
                    "values": {
                        "DT_LAST_DISCOVERY_COMPLETED_AT": completed_at,
                        "NU_LAST_DISCOVERY_FILE_COUNT": 0,
                        "VL_LAST_DISCOVERY_KB": 0.0,
                    },
                }
            ],
        )

    def test_discovery_outcome_records_last_scan_with_files(self) -> None:
        db = FakeSignalDb()
        completed_at = datetime(2026, 7, 29, 12, 0, 0)

        with patch.object(host_runtime, "_get_snapshot_signal_db", return_value=db):
            host_runtime.record_discovery_outcome(
                12,
                completed_at=completed_at,
                discovered_file_count=7,
                discovered_volume_kb=2 * 1024,
                logger=object(),
            )

        self.assertEqual(
            db.signal_updates[0]["values"]["DT_LAST_DISCOVERY_WITH_FILES"],
            completed_at,
        )
        self.assertEqual(
            db.signal_updates[0]["values"]["VL_LAST_DISCOVERY_KB"],
            2 * 1024,
        )

    def test_gps_unavailable_preserves_file_evidence(self) -> None:
        db = FakeSignalDb()
        evaluated_at = datetime(2026, 7, 29, 12, 0, 0)

        with patch.object(host_runtime, "_get_snapshot_signal_db", return_value=db):
            host_runtime.record_gps_gnss_unavailable(
                12,
                evaluated_at=evaluated_at,
                description="Invalid GPS reading",
                host_file_name="sample.bin",
                logger=object(),
            )

        self.assertEqual(
            db.signal_updates[0]["values"]["IS_GPS_GNSS_UNAVAILABLE"],
            True,
        )
        self.assertEqual(
            db.signal_updates[0]["values"]["DT_LAST_GPS_GNSS_UNAVAILABLE_AT"],
            evaluated_at,
        )
        self.assertEqual(
            db.signal_updates[0]["values"]["NA_LAST_GPS_GNSS_UNAVAILABLE_HOST_FILE_NAME"],
            "sample.bin",
        )


if __name__ == "__main__":
    unittest.main()
