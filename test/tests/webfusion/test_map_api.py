"""Verify the shared map routes and their optional complete dataset."""

from copy import deepcopy
from datetime import datetime
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from flask import Flask

WEBFUSION_ROOT = Path(__file__).resolve().parents[3] / "src/webfusion"
sys.path.insert(0, str(WEBFUSION_ROOT))
from api.map_api import routes
from api.map_api import service
from api.zabbix_api.connection import ZABBIX_SECRET_FILE
from app import app as webfusion_app


class MapApiTests(unittest.TestCase):
    """Validate one map contract for both browser and external clients."""

    def test_points_and_complete_dataset_share_one_service_call(self) -> None:
        """Check optional details do not trigger another dataset query.

        Args:
            None.

        Returns:
            None. Assertions validate envelopes and query arguments.
        """
        app = Flask(__name__)
        app.register_blueprint(routes.map_api_bp)
        payload = {"points": [{"site_id": 77}], "site_details": [{"site_id": 77, "stations": []}]}
        for suffix, expected in (
            ("", {"points": payload["points"]}),
            ("?include_details=true", payload),
        ):
            with patch.object(routes, "get_station_map_dataset", return_value=deepcopy(payload)) as build:
                response = app.test_client().get("/api/map/stations" + suffix)
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json, expected)
                build.assert_called_once_with(start_date=None, end_date=None)

    def test_date_filter_keeps_points_and_details_aligned(self) -> None:
        """Verify date filtering removes unmatched sites from both arrays.

        Args:
            None.

        Returns:
            None. Assertions validate shared filtering and one snapshot read.
        """
        station = {"first_seen_at": datetime(2026, 9, 1), "last_seen_at": datetime(2026, 9, 11),
                   "map_state": "online_current", "host_id": 1, "host_name": "Host"}
        points = [{"site_id": 1}, {"site_id": 2}]
        details = {1: {"site_id": 1, "stations": [station]}, 2: {"site_id": 2, "stations": []}}
        with patch.object(service, "_build_station_map_dataset_from_summary", return_value=(points, details)) as build:
            payload = service.get_station_map_dataset("2026-09-01", "2026-09-11")
        self.assertEqual([point["site_id"] for point in payload["points"]], [1])
        self.assertEqual([detail["site_id"] for detail in payload["site_details"]], [1])
        self.assertEqual(payload["points"][0]["marker_state"], payload["site_details"][0]["marker_state"])
        self.assertEqual(points, [{"site_id": 1}, {"site_id": 2}])
        build.assert_called_once_with()

    def test_existing_error_behavior_is_preserved(self) -> None:
        """Keep the original empty-map response when summary reads fail.

        Args:
            None.

        Returns:
            None. Checks both map envelopes and the popup error response.
        """
        app = Flask(__name__)
        app.logger.disabled = True
        app.register_blueprint(routes.map_api_bp)
        with patch.object(routes, "get_station_map_dataset", side_effect=RuntimeError):
            self.assertEqual(app.test_client().get("/api/map/stations").json, {"points": []})
            self.assertEqual(app.test_client().get("/api/map/stations?include_details=true").json,
                             {"points": [], "site_details": []})
        with patch.object(routes, "get_station_map_site_detail", side_effect=RuntimeError):
            self.assertEqual(app.test_client().get("/api/map/stations/77").json,
                             {"site_id": 77, "stations": [], "has_online_host": False, "has_known_host": False})

    def test_app_has_no_duplicate_map_endpoints(self) -> None:
        """Check complete registration and absence of the removed aliases.

        Args:
            None.

        Returns:
            None. Asserts shared routes are registered exactly once.
        """
        paths = [rule.rule for rule in webfusion_app.url_map.iter_rules()]
        self.assertEqual(paths.count("/api/map/stations"), 1)
        self.assertEqual(paths.count("/api/map/stations/<int:site_id>"), 1)
        for path in ("/api/appanalise/map", "/api/appanalise/map/sites", "/api/appanalise/map/stations"):
            self.assertNotIn(path, paths)
            self.assertEqual(webfusion_app.test_client().get(path).status_code, 404)

    def test_zabbix_secret_path_did_not_move(self) -> None:
        """Check relocating the package does not redirect secret discovery.

        Args:
            None.

        Returns:
            None. Compares paths without reading or displaying credentials.
        """
        expected = WEBFUSION_ROOT.parent / "zabbix/.secret/zabbix_api.env"
        self.assertEqual(ZABBIX_SECRET_FILE, expected)


if __name__ == "__main__":
    unittest.main()
