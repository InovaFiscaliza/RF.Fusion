"""
Validation tests for `webfusion.modules.map.service`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_map_service.py -q

What is covered here:
    - normalization of Celplan/CWSM receiver naming
    - map-side host reconciliation against `BPDATA`
"""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from pathlib import Path


MODULE_PATH = Path("/RFFusion/src/webfusion/modules/map/service.py")


def load_map_service():
    """Import the map service with lightweight DB stubs only."""
    stub_db = types.ModuleType("db")
    stub_db.get_connection_bpdata = lambda: None
    stub_db.get_connection_rfdata = lambda: None

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
    """Validate the receiver-to-host matching rules used by the map popup."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_map_service()

    def test_build_cwsm_signature_handles_short_host_name(self):
        self.assertEqual(
            self.module._build_cwsm_signature("cwsm211001"),
            "cwsm211001",
        )

    def test_build_cwsm_signature_handles_long_receiver_name(self):
        self.assertEqual(
            self.module._build_cwsm_signature("cwsm21100001"),
            "cwsm211001",
        )
        self.assertEqual(
            self.module._build_cwsm_signature("cwsm21120037"),
            "cwsm212037",
        )
        self.assertEqual(
            self.module._build_cwsm_signature("cwsm22010007"),
            "cwsm211007",
        )
        self.assertEqual(
            self.module._build_cwsm_signature("cwsm22010040"),
            "cwsm220040",
        )

    def test_find_host_for_equipment_matches_celplan_receiver_variants(self):
        host_index = {
            "raw": {
                "cwsm211001": {
                    "host_id": 101,
                    "host_name": "CWSM211001",
                    "is_offline": False,
                },
                "cwsm220040": {
                    "host_id": 202,
                    "host_name": "CWSM220040",
                    "is_offline": False,
                },
                "cwsm212037": {
                    "host_id": 303,
                    "host_name": "CWSM212037",
                    "is_offline": False,
                },
                "cwsm211007": {
                    "host_id": 404,
                    "host_name": "CWSM211007",
                    "is_offline": False,
                },
            },
            "normalized": {
                "cwsm211001": [
                    {
                        "host_id": 101,
                        "host_name": "CWSM211001",
                        "is_offline": False,
                    }
                ],
                "cwsm220040": [
                    {
                        "host_id": 202,
                        "host_name": "CWSM220040",
                        "is_offline": False,
                    }
                ],
                "cwsm212037": [
                    {
                        "host_id": 303,
                        "host_name": "CWSM212037",
                        "is_offline": False,
                    }
                ],
                "cwsm211007": [
                    {
                        "host_id": 404,
                        "host_name": "CWSM211007",
                        "is_offline": False,
                    }
                ],
            },
        }

        host = self.module._find_host_for_equipment(host_index, "cwsm21100001")
        self.assertIsNotNone(host)
        self.assertEqual(host["host_id"], 101)

        host = self.module._find_host_for_equipment(host_index, "cwsm22010040")
        self.assertIsNotNone(host)
        self.assertEqual(host["host_id"], 202)

        host = self.module._find_host_for_equipment(host_index, "cwsm21120037")
        self.assertIsNotNone(host)
        self.assertEqual(host["host_id"], 303)

        host = self.module._find_host_for_equipment(host_index, "cwsm22010007")
        self.assertIsNotNone(host)
        self.assertEqual(host["host_id"], 404)

    def test_classify_station_point_state_covers_all_status_and_location_cases(self):
        online_host = {
            "host_id": 11,
            "host_name": "RFEye002129",
            "is_offline": False,
        }
        offline_host = {
            "host_id": 22,
            "host_name": "RFEye002274",
            "is_offline": True,
        }

        self.assertEqual(
            self.module._classify_station_point_state(online_host, True),
            self.module.POINT_STATE_ONLINE_CURRENT,
        )
        self.assertEqual(
            self.module._classify_station_point_state(online_host, False),
            self.module.POINT_STATE_ONLINE_PREVIOUS,
        )
        self.assertEqual(
            self.module._classify_station_point_state(offline_host, True),
            self.module.POINT_STATE_OFFLINE_CURRENT,
        )
        self.assertEqual(
            self.module._classify_station_point_state(offline_host, False),
            self.module.POINT_STATE_OFFLINE_PREVIOUS,
        )
        self.assertEqual(
            self.module._classify_station_point_state(None, True),
            self.module.POINT_STATE_NO_HOST,
        )

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


if __name__ == "__main__":
    unittest.main()
