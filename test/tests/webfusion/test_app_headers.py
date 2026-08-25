"""Validation tests for the F5 identity-header diagnostic endpoint."""

import importlib
import sys
import unittest
from pathlib import Path


WEBFUSION_ROOT = Path("/RFFusion/src/webfusion")


class TestIdentityHeaderDebugRoute(unittest.TestCase):
    """Verify that the diagnostic route returns only the expected F5 fields."""

    @classmethod
    def setUpClass(cls):
        root = str(WEBFUSION_ROOT)
        if root not in sys.path:
            sys.path.insert(0, root)

        module = importlib.import_module("app")
        module.app.config.update(TESTING=True)
        cls.client = module.app.test_client()

    def test_returns_forwarded_identity_headers_without_cache(self):
        response = self.client.get(
            "/debug/headers",
            headers={
                "X-User-Name": "Maria Silva",
                "X-User-Email": "maria.silva@example.org",
                "X-User-Job-Title": "Analista",
                "X-User-Department": "Fiscalização",
                "X-User-Location": "Brasília",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Cache-Control"], "no-store")
        self.assertEqual(
            response.get_json(),
            {
                "X-User-Name": "Maria Silva",
                "X-User-Email": "maria.silva@example.org",
                "X-User-Job-Title": "Analista",
                "X-User-Department": "Fiscalização",
                "X-User-Location": "Brasília",
            },
        )

    def test_returns_null_when_f5_does_not_forward_a_header(self):
        response = self.client.get("/debug/headers")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.get_json(),
            {
                "X-User-Name": None,
                "X-User-Email": None,
                "X-User-Job-Title": None,
                "X-User-Department": None,
                "X-User-Location": None,
            },
        )


if __name__ == "__main__":
    unittest.main()
