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

    def test_host_equipment_link_matches_cwsm_family_by_station_suffix(self) -> None:
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        datasets = {
            "FROM BPDATA.HOST": [
                {"FK_HOST": 1, "NA_HOST_NAME": "CWSM212031"},
                {"FK_HOST": 2, "NA_HOST_NAME": "CWSM220044"},
            ],
            "FROM RFDATA.DIM_SPECTRUM_EQUIPMENT": [
                {"FK_EQUIPMENT": 118, "NA_EQUIPMENT": "cwsm21100031"},
                {"FK_EQUIPMENT": 138, "NA_EQUIPMENT": "cwsm21100044"},
            ],
            "FROM HOST_EQUIPMENT_LINK_OVERRIDE": [],
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

        self.assertEqual(by_equipment[118]["FK_HOST"], 1)
        self.assertEqual(by_equipment[118]["NA_MATCH_TYPE"], "cwsm_signature")
        self.assertEqual(by_equipment[138]["FK_HOST"], 2)
        self.assertEqual(by_equipment[138]["NA_MATCH_TYPE"], "cwsm_signature")

    def test_site_equipment_obs_summary_preserves_county_and_district_ids(self) -> None:
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        engine._select = lambda sql, params=(): [
            {
                "FK_SITE": 237,
                "FK_EQUIPMENT": 133,
                "NA_SITE_NAME": "Enseada do Sua",
                "NA_SITE_LABEL": "Enseada do Sua",
                "FK_COUNTY": 3205309,
                "FK_DISTRICT": 181,
                "NA_COUNTY_NAME": "Vitoria",
                "NA_DISTRICT_NAME": "Enseada do Sua",
                "ID_STATE": 32,
                "NA_STATE_NAME": "Espirito Santo",
                "NA_STATE_CODE": "ES",
                "VL_LATITUDE": -20.31,
                "VL_LONGITUDE": -40.29,
                "VL_ALTITUDE": 3.0,
                "NU_GNSS_MEASUREMENTS": 12,
                "NA_EQUIPMENT": "ermxes03",
                "DT_FIRST_SEEN_AT": datetime(2025, 6, 28, 12, 22, 53),
                "DT_LAST_SEEN_AT": datetime(2026, 1, 10, 9, 1, 12),
                "NU_SPECTRUM_COUNT": 1925,
                "ID_LAST_SPECTRUM": 4219,
            }
        ]

        row_count, watermark = engine._refresh_site_equipment_obs_summary()

        self.assertEqual(row_count, 1)
        self.assertEqual(watermark, "rows=1")
        rows = db.replaced["SITE_EQUIPMENT_OBS_SUMMARY"]
        self.assertEqual(rows[0]["FK_COUNTY"], 3205309)
        self.assertEqual(rows[0]["FK_DISTRICT"], 181)

    def test_host_location_summary_propagates_county_and_district_ids(self) -> None:
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        engine._select = lambda sql, params=(): [
            {
                "FK_HOST": 10845,
                "FK_SITE": 237,
                "NA_HOST_NAME": "ERMxES03",
                "NA_SITE_NAME": "Enseada do Sua",
                "NA_SITE_LABEL": "Enseada do Sua",
                "FK_COUNTY": 3205309,
                "FK_DISTRICT": 181,
                "NA_COUNTY_NAME": "Vitoria",
                "NA_DISTRICT_NAME": "Enseada do Sua",
                "ID_STATE": 32,
                "NA_STATE_NAME": "Espirito Santo",
                "NA_STATE_CODE": "ES",
                "VL_LATITUDE": -20.31,
                "VL_LONGITUDE": -40.29,
                "VL_ALTITUDE": 3.0,
                "DT_FIRST_SEEN_AT": datetime(2025, 6, 28, 12, 22, 53),
                "DT_LAST_SEEN_AT": datetime(2026, 1, 10, 9, 1, 12),
                "NU_SPECTRUM_COUNT": 1925,
                "FK_EQUIPMENT": 133,
                "IS_CURRENT_LOCATION": 1,
                "IS_OFFLINE": 0,
                "VL_MATCH_CONFIDENCE": 1.0,
            }
        ]

        row_count, watermark = engine._refresh_host_location_summary()

        self.assertEqual(row_count, 1)
        self.assertEqual(watermark, "rows=1")
        rows = db.replaced["HOST_LOCATION_SUMMARY"]
        self.assertEqual(rows[0]["FK_COUNTY"], 3205309)
        self.assertEqual(rows[0]["FK_DISTRICT"], 181)
        self.assertEqual(rows[0]["NA_LOCALITY_LABEL"], "Enseada do Sua · Vitoria/ES")

    def test_map_site_summary_propagates_county_and_district_ids(self) -> None:
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        def fake_select(sql, params=()):
            if "FROM RFDATA.DIM_SPECTRUM_SITE s" in sql:
                return [
                    {
                        "ID_SITE": 237,
                        "NA_SITE": "Enseada do Sua",
                        "FK_COUNTY": 3205309,
                        "FK_DISTRICT": 181,
                        "NA_COUNTY": "Vitoria",
                        "NA_DISTRICT": "Enseada do Sua",
                        "ID_STATE": 32,
                        "NA_STATE": "Espirito Santo",
                        "LC_STATE": "ES",
                        "VL_LATITUDE": -20.31,
                        "VL_LONGITUDE": -40.29,
                        "VL_ALTITUDE": 3.0,
                        "NU_GNSS_MEASUREMENTS": 12,
                    }
                ]

            if sql.strip() == "SELECT * FROM MAP_SITE_STATION_SUMMARY":
                return [
                    {
                        "FK_SITE": 237,
                        "FK_HOST": 10845,
                        "NA_MAP_STATE": "online_current",
                        "NU_STATE_PRIORITY": 0,
                    }
                ]

            raise AssertionError(f"Unexpected SQL: {sql}")

        engine._select = fake_select

        row_count, watermark = engine._refresh_map_site_summary()

        self.assertEqual(row_count, 1)
        self.assertEqual(watermark, "rows=1")
        rows = db.replaced["MAP_SITE_SUMMARY"]
        self.assertEqual(rows[0]["FK_COUNTY"], 3205309)
        self.assertEqual(rows[0]["FK_DISTRICT"], 181)
        self.assertEqual(rows[0]["NA_MARKER_STATE"], "online_current")

    def test_normalization_and_cwsm_signature_follow_sql_contract(self) -> None:
        self.assertEqual(_normalize_key(" CWSM-22010007 "), "cwsm22010007")
        self.assertEqual(_cwsm_signature("cwsm22010007"), "cwsm211007")
        self.assertEqual(_cwsm_signature("cwsm21100007"), "cwsm211007")
        self.assertEqual(_cwsm_signature("cwsm21100031"), "cwsm212031")
        self.assertEqual(_cwsm_signature("cwsm21100044"), "cwsm220044")
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

    def test_host_current_snapshot_includes_current_month_backup_throughput(self) -> None:
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        captured = {}

        def fake_select(sql, params=()):
            if sql.strip() == "SELECT * FROM HOST_CURRENT_SNAPSHOT":
                raise AssertionError("HOST_CURRENT_SNAPSHOT should not be read here")

            if sql.strip() == "SELECT * FROM BPDATA.HOST":
                return [
                    {
                        "ID_HOST": 10,
                        "IS_OFFLINE": 0,
                        "IS_BUSY": 1,
                        "NA_HOST_NAME": "rfeye010",
                        "NA_HOST_ADDRESS": "10.0.0.10",
                        "NA_HOST_PORT": 22,
                        "NU_PID": None,
                        "DT_BUSY": None,
                        "DT_LAST_FAIL": None,
                        "DT_LAST_CHECK": None,
                        "NU_HOST_CHECK_ERROR": 0,
                        "DT_LAST_DISCOVERY": None,
                        "NU_DONE_FILE_DISCOVERY_TASKS": 12,
                        "NU_ERROR_FILE_DISCOVERY_TASKS": 0,
                        "DT_LAST_BACKUP": None,
                        "NU_PENDING_FILE_BACKUP_TASKS": 2,
                        "NU_DONE_FILE_BACKUP_TASKS": 9,
                        "NU_ERROR_FILE_BACKUP_TASKS": 1,
                        "VL_PENDING_BACKUP_KB": 512000,
                        "VL_DONE_BACKUP_KB": 1024000,
                        "DT_LAST_PROCESSING": None,
                        "NU_PENDING_FILE_PROCESS_TASKS": 4,
                        "NU_DONE_FILE_PROCESS_TASKS": 5,
                        "NU_ERROR_FILE_PROCESS_TASKS": 1,
                        "NU_HOST_FILES": 12,
                    },
                    {
                        "ID_HOST": 11,
                        "IS_OFFLINE": 1,
                        "IS_BUSY": 0,
                        "NA_HOST_NAME": "rfeye011",
                        "NA_HOST_ADDRESS": "10.0.0.11",
                        "NA_HOST_PORT": 22,
                        "NU_PID": None,
                        "DT_BUSY": None,
                        "DT_LAST_FAIL": None,
                        "DT_LAST_CHECK": None,
                        "NU_HOST_CHECK_ERROR": 0,
                        "DT_LAST_DISCOVERY": None,
                        "NU_DONE_FILE_DISCOVERY_TASKS": 8,
                        "NU_ERROR_FILE_DISCOVERY_TASKS": 0,
                        "DT_LAST_BACKUP": None,
                        "NU_PENDING_FILE_BACKUP_TASKS": 1,
                        "NU_DONE_FILE_BACKUP_TASKS": 4,
                        "NU_ERROR_FILE_BACKUP_TASKS": 0,
                        "VL_PENDING_BACKUP_KB": 256000,
                        "VL_DONE_BACKUP_KB": 512000,
                        "DT_LAST_PROCESSING": None,
                        "NU_PENDING_FILE_PROCESS_TASKS": 2,
                        "NU_DONE_FILE_PROCESS_TASKS": 3,
                        "NU_ERROR_FILE_PROCESS_TASKS": 0,
                        "NU_HOST_FILES": 8,
                    },
                ]

            if "FROM BPDATA.FILE_TASK_HISTORY" in sql:
                captured["history_params"] = params
                return [
                    {
                        "FK_HOST": 10,
                        "NU_BACKUP_DONE_THIS_MONTH": 14,
                        "VL_BACKUP_DONE_GB_THIS_MONTH": 18.58,
                    },
                    {
                        "FK_HOST": 11,
                        "NU_BACKUP_DONE_THIS_MONTH": 4,
                        "VL_BACKUP_DONE_GB_THIS_MONTH": 1.25,
                    },
                ]

            if "FROM BPDATA.FILE_TASK" in sql:
                return [
                    {
                        "FK_HOST": 10,
                        "NU_BACKUP_QUEUE_FILES_TOTAL": 3,
                        "VL_BACKUP_QUEUE_GB_TOTAL": 4.25,
                        "NU_PROCESSING_QUEUE_FILES_TOTAL": 6,
                        "VL_PROCESSING_QUEUE_GB_TOTAL": 7.75,
                    },
                    {
                        "FK_HOST": 11,
                        "NU_BACKUP_QUEUE_FILES_TOTAL": 0,
                        "VL_BACKUP_QUEUE_GB_TOTAL": 0.0,
                        "NU_PROCESSING_QUEUE_FILES_TOTAL": 1,
                        "VL_PROCESSING_QUEUE_GB_TOTAL": 0.25,
                    },
                ]

            if "FROM HOST_MONTHLY_METRIC" in sql:
                return [
                    {"FK_HOST": 10, "NU_DISCOVERED_FILES_TOTAL": 12},
                    {"FK_HOST": 11, "NU_DISCOVERED_FILES_TOTAL": 8},
                ]

            if "FROM HOST_EQUIPMENT_LINK" in sql and "NU_MATCHED_EQUIPMENT_TOTAL" in sql:
                return [
                    {"FK_HOST": 10, "NU_MATCHED_EQUIPMENT_TOTAL": 2},
                    {"FK_HOST": 11, "NU_MATCHED_EQUIPMENT_TOTAL": 1},
                ]

            if "FROM HOST_EQUIPMENT_LINK l" in sql and "NU_FACT_SPECTRUM_TOTAL" in sql:
                return [
                    {"FK_HOST": 10, "NU_FACT_SPECTRUM_TOTAL": 99},
                    {"FK_HOST": 11, "NU_FACT_SPECTRUM_TOTAL": 11},
                ]

            if "FROM HOST_LOCATION_SUMMARY" in sql:
                return []

            if "FROM HOST_ERROR_SUMMARY" in sql:
                return []

            raise AssertionError(f"Unexpected SQL: {sql}")

        class FrozenDateTime(datetime):
            @classmethod
            def utcnow(cls):
                return cls(2026, 5, 20, 17, 8, 30)

        original_datetime = engine_module.datetime
        engine_module.datetime = FrozenDateTime
        try:
            engine._select = fake_select
            row_count, watermark = engine._refresh_host_current_snapshot()
        finally:
            engine_module.datetime = original_datetime

        self.assertEqual(row_count, 2)
        self.assertEqual(watermark, "hosts=2")
        self.assertEqual(
            captured["history_params"],
            (datetime(2026, 5, 1, 0, 0, 0), datetime(2026, 6, 1, 0, 0, 0)),
        )

        rows = db.replaced["HOST_CURRENT_SNAPSHOT"]
        by_host = {row["ID_HOST"]: row for row in rows}
        self.assertEqual(by_host[10]["NU_BACKUP_DONE_THIS_MONTH"], 14)
        self.assertEqual(by_host[10]["VL_BACKUP_DONE_GB_THIS_MONTH"], 18.58)
        self.assertEqual(by_host[11]["NU_BACKUP_DONE_THIS_MONTH"], 4)
        self.assertEqual(by_host[11]["VL_BACKUP_DONE_GB_THIS_MONTH"], 1.25)

    def test_server_current_summary_sums_snapshot_current_month_throughput(self) -> None:
        db = FakeSummaryDb()
        log = FakeSummaryLog()
        engine = SummaryRefreshEngine(db=db, logger=log)

        def fake_select(sql, params=()):
            if sql.strip() == "SELECT * FROM HOST_CURRENT_SNAPSHOT":
                return [
                    {
                        "IS_OFFLINE": 0,
                        "IS_BUSY": 1,
                        "NU_HOST_FILES": 12,
                        "NU_PENDING_FILE_BACKUP_TASKS": 2,
                        "VL_PENDING_BACKUP_GB": 1.5,
                        "NU_ERROR_FILE_BACKUP_TASKS": 1,
                        "NU_BACKUP_QUEUE_FILES_TOTAL": 3,
                        "VL_BACKUP_QUEUE_GB_TOTAL": 4.25,
                        "NU_PENDING_FILE_PROCESS_TASKS": 4,
                        "NU_DONE_FILE_PROCESS_TASKS": 5,
                        "NU_ERROR_FILE_PROCESS_TASKS": 1,
                        "NU_PROCESSING_QUEUE_FILES_TOTAL": 6,
                        "VL_PROCESSING_QUEUE_GB_TOTAL": 7.75,
                        "NU_FACT_SPECTRUM_TOTAL": 99,
                        "NU_BACKUP_DONE_THIS_MONTH": 14,
                        "VL_BACKUP_DONE_GB_THIS_MONTH": 18.58,
                    },
                    {
                        "IS_OFFLINE": 1,
                        "IS_BUSY": 0,
                        "NU_HOST_FILES": 8,
                        "NU_PENDING_FILE_BACKUP_TASKS": 1,
                        "VL_PENDING_BACKUP_GB": 0.5,
                        "NU_ERROR_FILE_BACKUP_TASKS": 0,
                        "NU_BACKUP_QUEUE_FILES_TOTAL": 0,
                        "VL_BACKUP_QUEUE_GB_TOTAL": 0.0,
                        "NU_PENDING_FILE_PROCESS_TASKS": 2,
                        "NU_DONE_FILE_PROCESS_TASKS": 3,
                        "NU_ERROR_FILE_PROCESS_TASKS": 0,
                        "NU_PROCESSING_QUEUE_FILES_TOTAL": 1,
                        "VL_PROCESSING_QUEUE_GB_TOTAL": 0.25,
                        "NU_FACT_SPECTRUM_TOTAL": 11,
                        "NU_BACKUP_DONE_THIS_MONTH": 4,
                        "VL_BACKUP_DONE_GB_THIS_MONTH": 1.25,
                    },
                ]

            if "FROM SERVER_ERROR_SUMMARY" in sql:
                return [
                    {"NA_ERROR_SCOPE": "BACKUP", "NU_GROUPS": 2},
                    {"NA_ERROR_SCOPE": "PROCESSING", "NU_GROUPS": 3},
                ]

            raise AssertionError(f"Unexpected SQL: {sql}")

        class FrozenDateTime(datetime):
            @classmethod
            def utcnow(cls):
                return cls(2026, 5, 20, 17, 8, 30)

        original_datetime = engine_module.datetime
        engine_module.datetime = FrozenDateTime
        try:
            engine._select = fake_select
            row_count, watermark = engine._refresh_server_current_summary()
        finally:
            engine_module.datetime = original_datetime

        self.assertEqual(row_count, 1)
        self.assertEqual(watermark, "hosts=2;month=2026-05")

        payload = db.replaced["SERVER_CURRENT_SUMMARY"][0]
        self.assertEqual(payload["NA_CURRENT_MONTH_LABEL"], "2026-05")
        self.assertEqual(payload["NU_TOTAL_HOSTS"], 2)
        self.assertEqual(payload["NU_ONLINE_HOSTS"], 1)
        self.assertEqual(payload["NU_OFFLINE_HOSTS"], 1)
        self.assertEqual(payload["NU_BUSY_HOSTS"], 1)
        self.assertEqual(payload["NU_DISCOVERED_FILES_TOTAL"], 20)
        self.assertEqual(payload["NU_BACKUP_DONE_THIS_MONTH"], 18)
        self.assertEqual(payload["VL_BACKUP_DONE_GB_THIS_MONTH"], 19.83)
        self.assertEqual(payload["NU_BACKUP_ERROR_GROUPS"], 2)
        self.assertEqual(payload["NU_PROCESSING_ERROR_GROUPS"], 3)


if __name__ == "__main__":
    unittest.main()
