"""Validation tests for `webfusion.modules.map.service`."""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from datetime import datetime
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

    def test_filter_site_detail_by_date_recomputes_summary_from_remaining_stations(self):
        detail = {
            "site_id": 77,
            "stations": [
                {
                    "host_id": 101,
                    "host_name": "RFEye002129",
                    "equipment_name": "RFEye002129",
                    "map_state": self.module.POINT_STATE_ONLINE_CURRENT,
                    "first_seen_at": datetime(2026, 4, 1, 0, 0, 0),
                    "last_seen_at": datetime(2026, 4, 5, 0, 0, 0),
                },
                {
                    "host_id": None,
                    "host_name": None,
                    "equipment_name": "RFEye002130",
                    "map_state": self.module.POINT_STATE_NO_HOST,
                    "first_seen_at": datetime(2026, 4, 10, 0, 0, 0),
                    "last_seen_at": datetime(2026, 4, 15, 0, 0, 0),
                },
            ],
            "marker_state": self.module.POINT_STATE_ONLINE_CURRENT,
            "has_online_station": True,
            "has_online_host": True,
            "has_known_host": True,
        }

        filtered = self.module._filter_site_detail_by_date(
            detail,
            start_dt=datetime(2026, 4, 9, 0, 0, 0),
            end_before=datetime(2026, 4, 20, 0, 0, 0),
        )

        self.assertEqual(len(filtered["stations"]), 1)
        self.assertEqual(filtered["marker_state"], self.module.POINT_STATE_NO_HOST)
        self.assertFalse(filtered["has_online_station"])
        self.assertFalse(filtered["has_online_host"])
        self.assertFalse(filtered["has_known_host"])

    def test_get_station_map_points_rebuilds_snapshot_directly_from_summary(self):
        points = [{"site_id": 10, "marker_state": self.module.POINT_STATE_OFFLINE_CURRENT}]
        site_details = {
            10: {
                "site_id": 10,
                "stations": [],
                "marker_state": self.module.POINT_STATE_OFFLINE_CURRENT,
                "has_online_station": False,
                "has_online_host": False,
                "has_known_host": False,
            }
        }

        with patch.object(
            self.module,
            "_build_station_map_dataset_from_summary",
            return_value=(points, site_details),
        ) as build_snapshot:
            result = self.module.get_station_map_points()

        self.assertEqual(result, points)
        build_snapshot.assert_called_once_with()

    def test_get_station_map_site_detail_rebuilds_detail_directly_from_summary(self):
        detail = {
            "site_id": 77,
            "stations": [],
            "marker_state": self.module.POINT_STATE_NO_HOST,
            "has_online_station": False,
            "has_online_host": False,
            "has_known_host": False,
        }
        points = [{"site_id": 77}]
        site_details = {77: detail}

        with patch.object(
            self.module,
            "_build_station_map_dataset_from_summary",
            return_value=(points, site_details),
        ) as build_snapshot:
            result = self.module.get_station_map_site_detail(77)

        self.assertEqual(result, detail)
        build_snapshot.assert_called_once_with()

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
                points, site_details = self.module._build_station_map_dataset_from_summary()

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
