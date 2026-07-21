"""
Validation tests for `shared.geolocation_utils`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/shared/test_geolocation_utils.py -q

What is covered here:
    - ordered district-candidate extraction from Nominatim addresses
    - removal of district labels that merely repeat the county/state
"""

from __future__ import annotations

import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _support import SHARED_ROOT, ensure_app_paths, import_package_module


ensure_app_paths()

geolocation_utils = import_package_module(
    "app_shared",
    SHARED_ROOT,
    "geolocation_utils",
)


class ReverseGeocodeSiteDataTests(unittest.TestCase):
    def test_reverse_geocode_site_data_retries_with_mirrored_latitude_for_fixed_point(self) -> None:
        initial_site = {
            "longitude": -49.280111,
            "latitude": 16.506472,
            "altitude": -1.0,
            "longitude_raw": [-49.280111],
            "latitude_raw": [16.506472],
            "altitude_raw": [-1.0],
            "geographic_path": None,
        }
        goias_location = SimpleNamespace(
            raw={
                "address": {
                    "state": "Goiás",
                    "city": "Goiânia",
                    "suburb": "Setor Central",
                }
            }
        )
        seen_points = []

        def fake_reverse(data, *, user_agent):
            seen_points.append((data["latitude"], data["longitude"]))
            if len(seen_points) == 1:
                return None
            return goias_location

        with patch.object(
            geolocation_utils,
            "reverse_geocode_with_retry",
            side_effect=fake_reverse,
        ):
            site_data = geolocation_utils.reverse_geocode_site_data(
                initial_site,
                user_agent="test-agent",
                required_address_field={
                    "state": ["state"],
                    "county": ["city"],
                    "district": ["suburb", "city_district", "neighbourhood"],
                },
            )

        self.assertEqual(seen_points[0], (16.506472, -49.280111))
        self.assertEqual(seen_points[1], (-16.506472, -49.280111))
        self.assertEqual(site_data["latitude"], -16.506472)
        self.assertEqual(site_data["latitude_raw"], [-16.506472])
        self.assertEqual(site_data["state"], "Goiás")
        self.assertEqual(site_data["county"], "Goiânia")


class MapLocationToSiteDataTests(unittest.TestCase):
    def test_map_location_to_site_data_rejects_missing_location(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            geolocation_utils.map_location_to_site_data(
                None,
                {},
                {
                    "state": ["state"],
                    "county": ["city"],
                    "district": ["suburb", "city_district", "neighbourhood"],
                },
            )

        self.assertIn("returned no location", str(ctx.exception))

    def test_map_location_to_site_data_keeps_ordered_district_candidates(self) -> None:
        location = SimpleNamespace(
            raw={
                "address": {
                    "suburb": "Campo Belo",
                    "city_district": "Zona Sul",
                    "city": "São Paulo",
                    "state": "São Paulo",
                }
            }
        )
        site_data = geolocation_utils.map_location_to_site_data(
            location,
            {},
            {
                "state": ["state"],
                "county": ["city"],
                "district": ["suburb", "city_district", "neighbourhood"],
            },
        )

        self.assertEqual(site_data["district"], "Campo Belo")
        self.assertEqual(
            site_data["district_candidates"],
            ["Campo Belo", "Zona Sul"],
        )

    def test_map_location_to_site_data_falls_back_to_county_when_district_repeats_it(self) -> None:
        location = SimpleNamespace(
            raw={
                "address": {
                    "city_district": "Salvador",
                    "city": "Salvador",
                    "state": "Bahia",
                }
            }
        )
        site_data = geolocation_utils.map_location_to_site_data(
            location,
            {},
            {
                "state": ["state"],
                "county": ["city"],
                "district": ["suburb", "city_district", "neighbourhood"],
            },
        )

        self.assertEqual(site_data["district"], "Salvador")
        self.assertEqual(site_data["district_candidates"], ["Salvador"])

    def test_map_location_to_site_data_uses_village_as_late_district_fallback(self) -> None:
        location = SimpleNamespace(
            raw={
                "address": {
                    "village": "Arapuá",
                    "town": "Capitão Poço",
                    "state": "Pará",
                }
            }
        )
        site_data = geolocation_utils.map_location_to_site_data(
            location,
            {},
            {
                "state": ["state"],
                "county": ["city", "town", "village"],
                "district": ["suburb", "city_district", "neighbourhood"],
            },
        )

        self.assertEqual(site_data["county"], "Capitão Poço")
        self.assertEqual(site_data["district"], "Arapuá")
        self.assertEqual(site_data["district_candidates"], ["Arapuá"])

    def test_map_location_to_site_data_falls_back_to_county_when_no_district_keys_exist(self) -> None:
        location = SimpleNamespace(
            raw={
                "address": {
                    "city": "Toledo",
                    "road": "Rodovia Alberto Dalcanale",
                    "state": "Paraná",
                }
            }
        )
        site_data = geolocation_utils.map_location_to_site_data(
            location,
            {},
            {
                "state": ["state"],
                "county": ["city", "town", "village"],
                "district": ["suburb", "city_district", "neighbourhood"],
            },
        )

        self.assertEqual(site_data["county"], "Toledo")
        self.assertEqual(site_data["district"], "Toledo")
        self.assertEqual(site_data["district_candidates"], ["Toledo"])


if __name__ == "__main__":
    unittest.main()
