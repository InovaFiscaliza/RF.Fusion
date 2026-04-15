"""
Validation tests for `webfusion.modules.spectrum.routes`.

How to run:
    /opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion/test_spectrum_routes.py -q

What is covered here:
    - inverted frequency intervals return a user-facing validation error
    - spectrum sort normalization keeps old links compatible with the new UI
"""

from __future__ import annotations

import importlib
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


def load_spectrum_routes():
    """Reload spectrum routes with light framework and service stubs."""
    root = str(WEBFUSION_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)

    fake_flask = ModuleType("flask")

    class FakeBlueprint:
        def __init__(self, *args, **kwargs):
            pass

        def before_request(self, func):
            return func

        def route(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

    fake_flask.Blueprint = FakeBlueprint
    fake_flask.Response = lambda *args, **kwargs: None
    fake_flask.current_app = SimpleNamespace(logger=SimpleNamespace(
        info=lambda *args, **kwargs: None,
        exception=lambda *args, **kwargs: None,
    ))
    fake_flask.jsonify = lambda *args, **kwargs: None
    fake_flask.render_template = lambda *args, **kwargs: None
    fake_flask.request = SimpleNamespace(args={}, method="GET")

    fake_service = ModuleType("modules.spectrum.service")
    fake_service.get_spectrum_data = lambda *args, **kwargs: ([], 0)
    fake_service.get_spectrum_file_data = lambda *args, **kwargs: ([], 0)
    fake_service.get_equipments = lambda: []
    fake_service.get_spectrum_locality_options = lambda *args, **kwargs: []
    fake_service.get_spectrum_site_option = lambda *args, **kwargs: None
    fake_service.get_spectrum_site_availability_range = lambda *args, **kwargs: None
    fake_service.get_file_by_file_id = lambda *args, **kwargs: None
    fake_service.get_file_by_spectrum_id = lambda *args, **kwargs: None
    fake_service.get_spectra_by_file_id = lambda *args, **kwargs: []

    fake_werkzeug_wsgi = ModuleType("werkzeug.wsgi")
    fake_werkzeug_wsgi.wrap_file = lambda *args, **kwargs: None

    sys.modules["flask"] = fake_flask
    sys.modules["modules.spectrum.service"] = fake_service
    sys.modules["werkzeug.wsgi"] = fake_werkzeug_wsgi
    sys.modules.pop("modules.spectrum.routes", None)
    return importlib.import_module("modules.spectrum.routes")


class TestSpectrumRoutes(unittest.TestCase):
    """Protect user-facing spectrum filter validation helpers."""

    @classmethod
    def setUpClass(cls):
        cls.module = load_spectrum_routes()

    def test_validate_frequency_bounds_accepts_open_or_ordered_ranges(self):
        self.assertIsNone(self.module._validate_frequency_bounds(None, 200.0))
        self.assertIsNone(self.module._validate_frequency_bounds(100.0, None))
        self.assertIsNone(self.module._validate_frequency_bounds(100.0, 200.0))
        self.assertIsNone(self.module._validate_frequency_bounds(200.0, 200.0))

    def test_validate_frequency_bounds_rejects_inverted_range(self):
        message = self.module._validate_frequency_bounds(200.0, 100.0)
        self.assertEqual(
            message,
            "Frequência inicial deve ser menor ou igual à frequência final.",
        )

    def test_normalize_spectrum_sort_accepts_new_compact_choices(self):
        selected_key, sort_by, sort_order = self.module._normalize_spectrum_sort(
            "oldest",
            None,
        )

        self.assertEqual(selected_key, "oldest")
        self.assertEqual(sort_by, "date_start")
        self.assertEqual(sort_order, "ASC")

    def test_normalize_spectrum_sort_maps_legacy_date_links_to_recentness(self):
        selected_key, sort_by, sort_order = self.module._normalize_spectrum_sort(
            "date_end",
            "DESC",
        )

        self.assertEqual(selected_key, "recent")
        self.assertEqual(sort_by, "date_start")
        self.assertEqual(sort_order, "DESC")

    def test_normalize_spectrum_sort_maps_legacy_frequency_links_to_frequency_fields(self):
        selected_key, sort_by, sort_order = self.module._normalize_spectrum_sort(
            "freq_end",
            "DESC",
        )

        self.assertEqual(selected_key, "freq_end")
        self.assertEqual(sort_by, "freq_end")
        self.assertEqual(sort_order, "ASC")

    def test_normalize_file_sort_rejects_unknown_choices(self):
        selected_key, sort_by, sort_order = self.module._normalize_file_sort(
            "unknown",
            "sideways",
        )

        self.assertEqual(selected_key, "recent")
        self.assertEqual(sort_by, "date_end")
        self.assertEqual(sort_order, "DESC")

    def test_normalize_file_sort_accepts_new_compact_choices(self):
        selected_key, sort_by, sort_order = self.module._normalize_file_sort(
            "file_name_desc",
            None,
        )

        self.assertEqual(selected_key, "file_name_desc")
        self.assertEqual(sort_by, "file_name")
        self.assertEqual(sort_order, "DESC")

    def test_normalize_file_sort_maps_legacy_date_links_to_recentness(self):
        selected_key, sort_by, sort_order = self.module._normalize_file_sort(
            "date_start",
            "ASC",
        )

        self.assertEqual(selected_key, "oldest")
        self.assertEqual(sort_by, "date_start")
        self.assertEqual(sort_order, "ASC")

    def test_normalize_file_sort_maps_legacy_count_links(self):
        selected_key, sort_by, sort_order = self.module._normalize_file_sort(
            "spectrum_count",
            "ASC",
        )

        self.assertEqual(selected_key, "spectrum_count_asc")
        self.assertEqual(sort_by, "spectrum_count")
        self.assertEqual(sort_order, "ASC")


if __name__ == "__main__":
    unittest.main()
