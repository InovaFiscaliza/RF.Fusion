"""Check real worker handler construction without starting loops or services."""

from __future__ import annotations

from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from _support import (
    APP_ROOT,
    DB_ROOT,
    bind_real_package,
    bind_real_shared_package,
    bind_test_config,
    ensure_app_paths,
    load_module_from_path,
)


class PostgresWorkerStartupTests(unittest.TestCase):
    """Protect explicit PostgreSQL destinations in every entrypoint factory."""

    def test_entrypoints_construct_real_handlers_with_explicit_connections(self) -> None:
        """Build the actual handlers while forbidding network and signal setup.

        Args:
            None.
        Returns:
            None. No main loop, database connection, or operational logger runs.
        """
        ensure_app_paths()
        with bind_test_config() as config, bind_real_shared_package(), bind_real_package("db", DB_ROOT):
            from shared import logging_utils
            from server_handler import signal_runtime

            config.PG_CONNINFO = "dbname=unit_test"
            expected = {
                "appCataloga": (config.BKP_DATABASE_NAME,),
                "appCataloga_backlog_management": (config.BKP_DATABASE_NAME,),
                "appCataloga_discovery": (config.BKP_DATABASE_NAME,),
                "appCataloga_file_bin_process_appAnalise": (config.BKP_DATABASE_NAME, config.RFM_DATABASE_NAME),
                "appCataloga_file_bkp": (config.BKP_DATABASE_NAME,),
                "appCataloga_garbage_collector": (config.BKP_DATABASE_NAME,),
                "appCataloga_host_check": (config.BKP_DATABASE_NAME,),
                "appCataloga_host_maintenance": (config.BKP_DATABASE_NAME,),
                "appCataloga_summary_database": (config.SUMMARY_DATABASE_NAME, config.SUMMARY_DATABASE_NAME),
            }
            with patch.object(logging_utils, "log", return_value=MagicMock()), patch.object(
                signal_runtime, "install_shutdown_handlers"
            ), patch.object(psycopg.Connection, "connect") as connect:
                for name, schemas in expected.items():
                    with self.subTest(entrypoint=name):
                        worker = load_module_from_path(
                            f"startup_{name}", str(APP_ROOT / f"{name}.py")
                        )
                        result = worker._init_db()
                        handlers = result if isinstance(result, tuple) else (result,)
                        self.assertEqual(tuple(db.schema for db in handlers), schemas)
                        for db in handlers:
                            self.assertEqual(db._conninfo, config.PG_CONNINFO)
                            self.assertIsNone(db.db_connection)
                            db.close()
                        if name == "appCataloga":
                            summary = worker._init_summary_db()
                            self.assertEqual(summary.schema, config.SUMMARY_DATABASE_NAME)
                            self.assertEqual(summary._conninfo, config.PG_CONNINFO)
                            summary.close()
                connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
