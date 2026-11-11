"""Verify PostgreSQL configuration without reading operational credentials."""

from __future__ import annotations

import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from psycopg.conninfo import conninfo_to_dict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _support import CONFIG_ROOT, load_module_from_path, load_test_config


class PostgresConfigTests(unittest.TestCase):
    """Protect the explicit default database and environment overrides."""

    def test_default_and_empty_override_include_postgres_database(self) -> None:
        """Build a usable destination when the optional override is absent or empty.

        Args:
            None.
        Returns:
            None.
        """
        for override in (None, ""):
            with self.subTest(override=override), patch.dict(os.environ):
                os.environ.pop("PGUSER", None)
                os.environ.pop("PGPASSWORD", None)
                os.environ.pop("RFFUSION_PG_CONNINFO", None)
                if override is not None:
                    os.environ["RFFUSION_PG_CONNINFO"] = override
                config = load_test_config()
                self.assertEqual(conninfo_to_dict(config.PG_CONNINFO), {
                    "host": "10.88.0.35", "port": "5432", "dbname": "postgres",
                    "user": "postgres", "connect_timeout": "5",
                })

    def test_explicit_override_replaces_the_complete_connection(self) -> None:
        """Keep the supplied connection unchanged instead of merging default fields.

        Args:
            None.
        Returns:
            None.
        """
        override = "host=127.0.0.1 port=5544 dbname=custom user=custom"
        with patch.dict(os.environ, {"RFFUSION_PG_CONNINFO": override}):
            config = load_test_config()
        self.assertEqual(config.PG_CONNINFO, override)

    def test_postgres_user_is_independent_from_legacy_credentials(self) -> None:
        """Use environment credentials independently of the legacy MariaDB login.

        Args:
            None.
        Returns:
            None.
        """
        with patch.dict(os.environ, {
            "RFFUSION_PG_CONNINFO": "", "PGUSER": "postgres_test",
            "PGPASSWORD": "test-only-password",
        }):
            config = load_test_config()
        settings = conninfo_to_dict(config.PG_CONNINFO)
        self.assertEqual(settings["user"], "postgres_test")
        self.assertNotEqual(settings["user"], config.DB_USER_NAME)
        self.assertEqual(settings["password"], "test-only-password")

    def test_dedicated_secret_fields_supply_postgres_authentication(self) -> None:
        """Read the dedicated credentials without using the legacy login.

        Args:
            None.
        Returns:
            None.
        """
        credentials = {
            "DB_USER_NAME": "legacy-test", "DB_PASSWORD": "legacy-test",
            "DB_USER_NAME_PG": "postgres_test", "DB_PASSWORD_PG": "test-only's\\password",
        }
        with patch.dict(os.environ), patch("runpy.run_path", return_value=credentials):
            for name in ("PGUSER", "PGPASSWORD", "RFFUSION_PG_CONNINFO"):
                os.environ.pop(name, None)
            config = load_module_from_path("postgres_secret_config", str(CONFIG_ROOT / "config.py"))
        settings = conninfo_to_dict(config.PG_CONNINFO)
        self.assertEqual(settings["user"], credentials["DB_USER_NAME_PG"])
        self.assertEqual(settings["password"], credentials["DB_PASSWORD_PG"])

    def test_environment_overrides_dedicated_secret_credentials(self) -> None:
        """Keep environment values authoritative for debug and deployment overrides.

        Args:
            None.
        Returns:
            None.
        """
        credentials = {
            "DB_USER_NAME": "legacy-test", "DB_PASSWORD": "legacy-test",
            "DB_USER_NAME_PG": "secret_test", "DB_PASSWORD_PG": "secret-test-password",
        }
        with patch.dict(os.environ, {
            "PGUSER": "environment_test", "PGPASSWORD": "environment-test-password",
            "RFFUSION_PG_CONNINFO": "",
        }), patch("runpy.run_path", return_value=credentials):
            config = load_module_from_path("postgres_environment_config", str(CONFIG_ROOT / "config.py"))
        settings = conninfo_to_dict(config.PG_CONNINFO)
        self.assertEqual(settings["user"], "environment_test")
        self.assertEqual(settings["password"], "environment-test-password")


if __name__ == "__main__":
    unittest.main()
