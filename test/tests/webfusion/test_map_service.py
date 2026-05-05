"""Validation tests for `webfusion.modules.map.service`."""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_PATH = Path("/RFFusion/src/webfusion/modules/map/service.py")


def load_map_service():
    """Import the map service with lightweight DB stubs only."""
    stub_db = types.ModuleType("db")
    stub_db.get_connection_bpdata = lambda: None
    stub_db.get_connection_rfdata = lambda: None
    stub_db.get_connection_summary = lambda: None

    previous_db = sys.modules.get("db")
    sys.modules["db"] = stub_db

    try:
        spec = importlib.util.spec_from_file_location(
            "webfusion_map_service_test",
            MODULE_PATH,
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Unable to load module from {MODULE_PATH}")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if previous_db is not None:
            sys.modules["db"] = previous_db
        else:
            sys.modules.pop("db", None)


class TestMapService(unittest.TestCase):
    """Validate the summary-backed landing-page map behavior."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_map_service()

    def test_summarize_site_marker_state_prefers_current_online_marker(self):
        stations = [
            {"map_state": self.module.POINT_STATE_OFFLINE_PREVIOUS},
            {"map_state": self.module.POINT_STATE_NO_HOST},
            {"map_state": self.module.POINT_STATE_ONLINE_PREVIOUS},
            {"map_state": self.module.POINT_STATE_ONLINE_CURRENT},
        ]

        self.assertEqual(
            self.module._summarize_site_marker_state(stations),
            self.module.POINT_STATE_ONLINE_CURRENT,
        )

    def test_sort_site_stations_keeps_highest_priority_states_first(self):
        stations = [
            {"map_state": self.module.POINT_STATE_OFFLINE_CURRENT, "host_name": "Zulu"},
            {"map_state": self.module.POINT_STATE_ONLINE_PREVIOUS, "host_name": "Bravo"},
            {"map_state": self.module.POINT_STATE_ONLINE_CURRENT, "host_name": "Alpha"},
            {"map_state": self.module.POINT_STATE_NO_HOST, "equipment_name": "Charlie"},
        ]

        ordered = self.module._sort_site_stations(stations)

        self.assertEqual(
            [station["map_state"] for station in ordered],
            [
                self.module.POINT_STATE_ONLINE_CURRENT,
                self.module.POINT_STATE_ONLINE_PREVIOUS,
                self.module.POINT_STATE_OFFLINE_CURRENT,
                self.module.POINT_STATE_NO_HOST,
            ],
        )

    def test_get_station_map_points_returns_stale_cache_while_refresh_is_scheduled(self):
        stale_points = [{"site_id": 10, "marker_state": self.module.POINT_STATE_OFFLINE_CURRENT}]
        scheduled = []

        original_points_cache = dict(self.module._MAP_POINTS_CACHE)
        original_schedule = self.module._schedule_map_refresh_async
        original_refresh = self.module._refresh_station_map_snapshot

        self.module._MAP_POINTS_CACHE["value"] = stale_points
        self.module._MAP_POINTS_CACHE["expires_at"] = 0.0
        self.module._schedule_map_refresh_async = lambda force=False: scheduled.append(force) or True
        self.module._refresh_station_map_snapshot = lambda: (_ for _ in ()).throw(
            AssertionError("stale cache should avoid synchronous snapshot rebuild")
        )

        try:
            result = self.module.get_station_map_points()
        finally:
            self.module._MAP_POINTS_CACHE.update(original_points_cache)
            self.module._schedule_map_refresh_async = original_schedule
            self.module._refresh_station_map_snapshot = original_refresh

        self.assertEqual(result, stale_points)
        self.assertEqual(scheduled, [False])

    def test_get_station_map_site_detail_returns_stale_cache_while_refresh_is_scheduled(self):
        stale_detail = {
            "site_id": 77,
            "stations": [],
            "marker_state": self.module.POINT_STATE_NO_HOST,
            "has_online_station": False,
            "has_online_host": False,
            "has_known_host": False,
        }
        scheduled = []

        original_site_cache = dict(self.module._SITE_DETAILS_CACHE)
        original_schedule = self.module._schedule_map_refresh_async
        original_build = self.module._build_site_detail

        self.module._SITE_DETAILS_CACHE.clear()
        self.module._SITE_DETAILS_CACHE[77] = {
            "expires_at": 0.0,
            "value": stale_detail,
        }
        self.module._schedule_map_refresh_async = lambda force=False: scheduled.append(force) or True
        self.module._build_site_detail = lambda site_id: (_ for _ in ()).throw(
            AssertionError("stale site detail should avoid synchronous rebuild")
        )

        try:
            result = self.module.get_station_map_site_detail(77)
        finally:
            self.module._SITE_DETAILS_CACHE.clear()
            self.module._SITE_DETAILS_CACHE.update(original_site_cache)
            self.module._schedule_map_refresh_async = original_schedule
            self.module._build_site_detail = original_build

        self.assertEqual(result, stale_detail)
        self.assertEqual(scheduled, [False])

    def test_build_station_map_dataset_prefers_materialized_summary_tables(self):
        site_rows = [
            {
                "ID_SITE": 77,
                "SITE_LABEL": "Site Teste",
                "COUNTY_NAME": "Brasilia",
                "DISTRICT_NAME": "Plano Piloto",
                "ID_STATE": 53,
                "NA_STATE": "Distrito Federal",
                "LC_STATE": "DF",
                "VL_LATITUDE": -15.793889,
                "VL_LONGITUDE": -47.882778,
                "VL_ALTITUDE": 1172.0,
                "NU_GNSS_MEASUREMENTS": 12,
                "NA_MARKER_STATE": self.module.POINT_STATE_ONLINE_CURRENT,
                "HAS_ONLINE_STATION": 1,
                "HAS_ONLINE_HOST": 1,
                "HAS_KNOWN_HOST": 1,
            }
        ]
        station_rows = [
            {
                "ID_SITE": 77,
                "ID_EQUIPMENT": 501,
                "ID_HOST": 101,
                "NA_EQUIPMENT": "RFEye002129",
                "NA_HOST_NAME": "RFEye002129",
                "IS_OFFLINE": 0,
                "IS_CURRENT_LOCATION": 1,
                "NA_MAP_STATE": self.module.POINT_STATE_ONLINE_CURRENT,
                "FIRST_SEEN_AT": "2026-04-01 00:00:00",
                "LAST_SEEN_AT": "2026-04-16 00:00:00",
                "NU_SPECTRUM_COUNT": 10,
            }
        ]

        with patch.object(self.module, "_load_summary_site_rows", return_value=site_rows):
            with patch.object(self.module, "_load_summary_station_rows", return_value=station_rows):
                points, site_details = self.module._build_station_map_dataset()

        self.assertEqual(len(points), 1)
        self.assertEqual(points[0]["site_id"], 77)
        self.assertEqual(points[0]["marker_state"], self.module.POINT_STATE_ONLINE_CURRENT)
        self.assertEqual(points[0]["station_names"], ["RFEye002129"])
        self.assertTrue(points[0]["has_online_station"])
        self.assertEqual(len(site_details[77]["stations"]), 1)
        self.assertEqual(site_details[77]["stations"][0]["host_id"], 101)
        self.assertTrue(site_details[77]["stations"][0]["is_current_location"])


if __name__ == "__main__":
    unittest.main()
