"""
Focused tests for the incremental RFFUSION_SUMMARY Python engine.
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import APP_ROOT, ensure_app_paths, load_module_from_path


ensure_app_paths()

engine_module = load_module_from_path(
    "test_summary_worker_engine_module",
    str(APP_ROOT / "summary_handler" / "engine.py"),
)

DirtyScope = engine_module.DirtyScope
SummaryRefreshEngine = engine_module.SummaryRefreshEngine
_cwsm_signature = engine_module._cwsm_signature
_normalize_key = engine_module._normalize_key


class FakeSummaryDb:
    def __init__(self) -> None:
        self.started = []
        self.succeeded = []
        self.failed = []
        self.replaced = {}

    def summary_refresh_start(self, object_name):
        self.started.append(object_name)
        return datetime(2026, 5, 14, 0, 0, 0)

    def summary_refresh_success(self, object_name, *, started_at, row_count, high_watermark):
        self.succeeded.append((object_name, row_count, high_watermark))

    def summary_refresh_failure(self, object_name, *, started_at, error_message):
        self.failed.append((object_name, error_message))

    def replace_table_rows(self, table, rows):
        self.replaced[table] = rows
        return len(rows)


class FakeSummaryLog:
    def __init__(self) -> None:
        self.events = []
        self.warning_events = []

    def event(self, event, **fields):
        self.events.append((event, fields))

    def warning_event(self, event, **fields):
        self.warning_events.append((event, fields))


class SummaryWorkerEngineTests(unittest.TestCase):
    def test_dirty_scope_merges_all_event_payloads(self) -> None:
        scope = DirtyScope.from_events(
            [
                {
                    "JS_PAYLOAD": {
                        "host_ids": [10, 11],
                        "site_ids": [100],
                        "reference_months": ["2026-05-13", "2026-05"],
                    }
                },
                {
                    "JS_PAYLOAD": {
                        "host_ids": [11, 12],
                        "equipment_ids": [200, 201],
                        "full_reconcile": True,
                    }
                },
            ]
        )

        self.assertEqual(scope.host_ids, {10, 11, 12})
        self.assertEqual(scope.site_ids, {100})
        self.assertEqual(scope.equipment_ids, {200, 201})
        self.assertEqual(scope.reference_months, {"2026-05-01"})
        self.assertTrue(scope.full_reconcile)

    def test_refresh_for_events_runs_dependency_chain_in_order(self) -> None:
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        call_order = []
        engine._refresh_site_equipment_obs_summary = lambda **kwargs: call_order.append("SITE_EQUIPMENT_OBS_SUMMARY") or (1, "ok")
        engine._refresh_host_equipment_link = lambda: call_order.append("HOST_EQUIPMENT_LINK") or (1, "ok")
        engine._refresh_host_location_summary = lambda: call_order.append("HOST_LOCATION_SUMMARY") or (1, "ok")
        engine._refresh_map_site_station_summary = lambda: call_order.append("MAP_SITE_STATION_SUMMARY") or (1, "ok")
        engine._refresh_map_site_summary = lambda: call_order.append("MAP_SITE_SUMMARY") or (1, "ok")
        engine._refresh_host_monthly_metric = lambda **kwargs: call_order.append("HOST_MONTHLY_METRIC") or (1, "ok")
        engine._refresh_host_error_summary = lambda **kwargs: call_order.append("HOST_ERROR_SUMMARY") or (1, "ok")
        engine._refresh_server_error_summary = lambda: call_order.append("SERVER_ERROR_SUMMARY") or (1, "ok")
        engine._refresh_host_current_snapshot = lambda: call_order.append("HOST_CURRENT_SNAPSHOT") or (1, "ok")
        engine._refresh_server_current_summary = lambda: call_order.append("SERVER_CURRENT_SUMMARY") or (1, "ok")

        engine.refresh_for_events(
            [
                {"JS_PAYLOAD": {"site_ids": [100]}},
                {"JS_PAYLOAD": {"host_ids": [10], "reference_months": ["2026-05-01"]}},
            ]
        )

        self.assertEqual(
            call_order,
            [
                "SITE_EQUIPMENT_OBS_SUMMARY",
                "HOST_EQUIPMENT_LINK",
                "HOST_LOCATION_SUMMARY",
                "MAP_SITE_STATION_SUMMARY",
                "MAP_SITE_SUMMARY",
                "HOST_MONTHLY_METRIC",
                "HOST_ERROR_SUMMARY",
                "SERVER_ERROR_SUMMARY",
                "HOST_CURRENT_SNAPSHOT",
                "SERVER_CURRENT_SUMMARY",
            ],
        )
        self.assertEqual(db.started, call_order)
        self.assertFalse(db.failed)

    def test_refresh_all_uses_full_safe_sequence(self) -> None:
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        call_order = []
        for name in (
            "SITE_EQUIPMENT_OBS_SUMMARY",
            "HOST_EQUIPMENT_LINK",
            "HOST_LOCATION_SUMMARY",
            "MAP_SITE_STATION_SUMMARY",
            "MAP_SITE_SUMMARY",
            "HOST_MONTHLY_METRIC",
            "HOST_ERROR_SUMMARY",
            "SERVER_ERROR_SUMMARY",
            "HOST_CURRENT_SNAPSHOT",
            "SERVER_CURRENT_SUMMARY",
        ):
            setattr(
                engine,
                f"_refresh_{name.lower()}",
                lambda _name=name: call_order.append(_name) or (1, "ok"),
            )

        engine._refresh_site_equipment_obs_summary = lambda **kwargs: call_order.append("SITE_EQUIPMENT_OBS_SUMMARY") or (1, "ok")
        engine._refresh_host_equipment_link = lambda: call_order.append("HOST_EQUIPMENT_LINK") or (1, "ok")
        engine._refresh_host_location_summary = lambda: call_order.append("HOST_LOCATION_SUMMARY") or (1, "ok")
        engine._refresh_map_site_station_summary = lambda: call_order.append("MAP_SITE_STATION_SUMMARY") or (1, "ok")
        engine._refresh_map_site_summary = lambda: call_order.append("MAP_SITE_SUMMARY") or (1, "ok")
        engine._refresh_host_monthly_metric = lambda **kwargs: call_order.append("HOST_MONTHLY_METRIC") or (1, "ok")
        engine._refresh_host_error_summary = lambda **kwargs: call_order.append("HOST_ERROR_SUMMARY") or (1, "ok")
        engine._refresh_server_error_summary = lambda: call_order.append("SERVER_ERROR_SUMMARY") or (1, "ok")
        engine._refresh_host_current_snapshot = lambda: call_order.append("HOST_CURRENT_SNAPSHOT") or (1, "ok")
        engine._refresh_server_current_summary = lambda: call_order.append("SERVER_CURRENT_SUMMARY") or (1, "ok")

        engine.refresh_all(reason="test")

        self.assertEqual(len(call_order), 10)
        self.assertEqual(call_order[0], "SITE_EQUIPMENT_OBS_SUMMARY")
        self.assertEqual(call_order[-1], "SERVER_CURRENT_SUMMARY")

    def test_host_equipment_link_prefers_manual_override_and_exact_matches(self) -> None:
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        datasets = {
            "FROM BPDATA.HOST": [
                {"FK_HOST": 1, "NA_HOST_NAME": "CWSM-22010007"},
                {"FK_HOST": 2, "NA_HOST_NAME": "rfeye002073"},
            ],
            "FROM RFDATA.DIM_SPECTRUM_EQUIPMENT": [
                {"FK_EQUIPMENT": 10, "NA_EQUIPMENT": "cwsm211007"},
                {"FK_EQUIPMENT": 20, "NA_EQUIPMENT": "rfeye002073"},
            ],
            "FROM HOST_EQUIPMENT_LINK_OVERRIDE": [
                {"FK_HOST": 2, "FK_EQUIPMENT": 20},
            ],
        }

        def fake_select(sql, params=()):
            for marker, rows in datasets.items():
                if marker in sql:
                    return rows
            raise AssertionError(f"Unexpected SQL: {sql}")

        engine._select = fake_select
        row_count, watermark = engine._refresh_host_equipment_link()

        rows = db.replaced["HOST_EQUIPMENT_LINK"]
        self.assertEqual(row_count, 2)
        self.assertIn("hosts=2", watermark)
        by_equipment = {row["FK_EQUIPMENT"]: row for row in rows if row["IS_PRIMARY_LINK"] == 1}

        self.assertEqual(by_equipment[10]["FK_HOST"], 1)
        self.assertEqual(by_equipment[10]["NA_MATCH_TYPE"], "cwsm_signature")
        self.assertEqual(by_equipment[20]["FK_HOST"], 2)
        self.assertEqual(by_equipment[20]["NA_MATCH_TYPE"], "manual_override")
        self.assertEqual(by_equipment[20]["IS_MANUAL_OVERRIDE"], 1)

    def test_normalization_and_cwsm_signature_follow_sql_contract(self) -> None:
        self.assertEqual(_normalize_key(" CWSM-22010007 "), "cwsm22010007")
        self.assertEqual(_cwsm_signature("cwsm22010007"), "cwsm211007")
        self.assertEqual(_cwsm_signature("cwsm21100007"), "cwsm211007")
        self.assertIsNone(_cwsm_signature("rfeye002073"))

    def test_host_monthly_metric_skips_invalid_reference_month_rows(self) -> None:
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        engine._select = lambda sql, params=(): [
            {
                "FK_HOST": 10,
                "DT_REFERENCE_MONTH": "2026-05-01",
                "NA_HOST_NAME": "rfeye002010",
                "NU_DISCOVERED_FILES": 2,
                "VL_DISCOVERED_GB": 1.25,
                "NU_BACKUP_DONE_FILES": 1,
                "VL_BACKUP_DONE_GB": 0.5,
                "NU_BACKUP_PENDING_FILES": 1,
                "VL_BACKUP_PENDING_GB": 0.75,
                "NU_BACKUP_ERROR_FILES": 0,
                "VL_BACKUP_ERROR_GB": 0,
                "NU_PROCESSING_DONE_FILES": 0,
                "VL_PROCESSING_DONE_GB": 0,
                "NU_PROCESSING_PENDING_FILES": 1,
                "VL_PROCESSING_PENDING_GB": 0.75,
                "NU_PROCESSING_ERROR_FILES": 0,
                "VL_PROCESSING_ERROR_GB": 0,
            },
            {
                "FK_HOST": 11,
                "DT_REFERENCE_MONTH": None,
                "NA_HOST_NAME": "rfeye002011",
                "NU_DISCOVERED_FILES": 1,
                "VL_DISCOVERED_GB": 0.25,
                "NU_BACKUP_DONE_FILES": 0,
                "VL_BACKUP_DONE_GB": 0,
                "NU_BACKUP_PENDING_FILES": 1,
                "VL_BACKUP_PENDING_GB": 0.25,
                "NU_BACKUP_ERROR_FILES": 0,
                "VL_BACKUP_ERROR_GB": 0,
                "NU_PROCESSING_DONE_FILES": 0,
                "VL_PROCESSING_DONE_GB": 0,
                "NU_PROCESSING_PENDING_FILES": 1,
                "VL_PROCESSING_PENDING_GB": 0.25,
                "NU_PROCESSING_ERROR_FILES": 0,
                "VL_PROCESSING_ERROR_GB": 0,
            },
        ]

        row_count, watermark = engine._refresh_host_monthly_metric()

        self.assertEqual(row_count, 1)
        self.assertIn("skipped_invalid_month=1", watermark)
        self.assertEqual(len(db.replaced["HOST_MONTHLY_METRIC"]), 1)
        self.assertEqual(
            db.replaced["HOST_MONTHLY_METRIC"][0]["DT_REFERENCE_MONTH"],
            "2026-05-01",
        )
        self.assertEqual(
            log.warning_events[0][0],
            "summary_host_monthly_metric_invalid_month_skipped",
        )


if __name__ == "__main__":
    unittest.main()
