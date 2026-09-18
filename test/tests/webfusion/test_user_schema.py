"""Exercise user migration and permissions against an isolated MariaDB schema."""

from __future__ import annotations

import importlib
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from uuid import uuid4

from pymysql.connections import Connection


ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "src/mariadb/scripts"


@unittest.skipUnless(os.environ.get("RFF_DB_TEST") == "1", "Requires isolated MariaDB integration database")
class TestUserSchema(unittest.TestCase):
    """Validate migration and CRUD without writing to the application schema.

    Attributes:
        database: Unique temporary schema name. Type: str.
        connection: Control connection for fixtures. Type: pymysql.Connection.
        handler: Real user persistence module. Type: module.
    """

    def setUp(self) -> None:
        """Create an isolated database and redirect handler connections.

        Args:
            None.

        Returns:
            None.
        """
        sys.path.insert(0, str(ROOT / "src/webfusion"))
        self.handler = importlib.import_module("auth.db_users")
        self.connect = importlib.import_module("db").get_connection_webfusion
        self.connection = self.connect()
        self.addCleanup(self.connection.close)
        self.database = "WEBFUSION_TEST_" + uuid4().hex
        with self.connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE `{self.database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        self.addCleanup(self.drop_database)
        self.connection.select_db(self.database)
        self.connection_patch = patch.object(self.handler, "get_connection_webfusion", self._open_test_connection)
        self.connection_patch.start()
        self.addCleanup(self.connection_patch.stop)

    def drop_database(self) -> None:
        """Remove only the unique schema created by this test.

        Args:
            None.

        Returns:
            None.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(f"DROP DATABASE `{self.database}`")

    def _open_test_connection(self) -> Connection:
        """Open a handler connection restricted to the temporary schema.

        Args:
            None.

        Returns:
            Test connection. Type: pymysql.Connection.
        """
        connection = self.connect()
        connection.select_db(self.database)
        return connection

    def execute_script(self, text: str) -> None:
        """Execute schema SQL after replacing its application database name.

        Args:
            text: Repository SQL without stored procedures. Type: str.

        Returns:
            None. Every statement targets the unique test schema.
        """
        text = text.replace("WEBFUSION", self.database)
        text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
        with self.connection.cursor() as cursor:
            for statement in text.split(";"):
                if statement.strip():
                    cursor.execute(statement)
        self.connection.commit()

    def test_legacy_migration_and_role_lifecycle(self) -> None:
        """Preserve identity, role state, and photos across migration and CRUD.

        Args:
            None.

        Returns:
            None.
        """
        schema = (SCRIPTS / "createWebFusionDB.sql").read_text()
        # Build the legacy layout without importing or mutating production rows.
        users_ddl = schema[schema.index("CREATE TABLE IF NOT EXISTS `USERS`"):schema.index("CREATE TABLE IF NOT EXISTS `USER_ROLES`")]
        users_ddl = users_ddl.replace("  `NA_URL_PROFILE_IMG` varchar(2048) DEFAULT NULL,\n", "")
        self.execute_script(users_ddl)
        with self.connection.cursor() as cursor:
            for table in ("ADMINS", "DEVELOPERS"):
                cursor.execute(f"CREATE TABLE {table} LIKE USERS")
                cursor.execute(f"ALTER TABLE {table} ADD IS_ACTIVE tinyint NOT NULL DEFAULT 1")
            cursor.execute("INSERT INTO USERS (NA_USER_NAME, NA_USER_EMAIL) VALUES ('Current name', 'both@example.org')")
            cursor.execute("INSERT INTO ADMINS (NA_USER_NAME, NA_USER_EMAIL, IS_ACTIVE) VALUES ('Old name', 'both@example.org', 1), ('Inactive', 'inactive@example.org', 0)")
            cursor.execute("INSERT INTO DEVELOPERS (NA_USER_NAME, NA_USER_EMAIL) VALUES ('Old name', 'both@example.org'), ('Developer only', 'dev@example.org')")
        self.connection.commit()

        migration = (SCRIPTS / "migrateWebFusionUsers.sql").read_text()
        self.execute_script(migration)
        self.execute_script(migration)
        both = self.handler.get_webfusion_user("both@example.org")
        self.assertEqual(both["NA_USER_NAME"], "Current name")
        self.assertEqual((both["IS_ADMIN"], both["IS_DEVELOPER"], both["NA_ROLE"]), (1, 1, "admin"))
        self.assertEqual(self.handler.get_access_role("dev@example.org"), "developer")
        self.assertIsNone(self.handler.get_access_role("inactive@example.org"))
        self.assertEqual(len(self.handler.list_webfusion_users()), 3)
        self.assertEqual(len(self.handler.list_webfusion_users(role="developer")), 2)
        self.assertEqual(len(self.handler.list_webfusion_users(role="none")), 1)
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT IS_ACTIVE FROM USER_ROLES WHERE NA_ROLE = 'admin' ORDER BY IS_ACTIVE")
            self.assertEqual([row["IS_ACTIVE"] for row in cursor.fetchall()], [0, 1])
        self.connection.commit()

        self.handler.update_webfusion_user_privileges(user_email="both@example.org", is_admin=False, is_developer=True)
        self.assertEqual(self.handler.get_access_role("both@example.org"), "developer")
        self.handler.update_webfusion_user_privileges(user_email="both@example.org", is_admin=False, is_developer=False)
        self.assertIsNone(self.handler.get_access_role("both@example.org"))
        self.handler.create_webfusion_user(user_email="new@example.org", user_name="New", job_title=None, department=None, location=None, is_admin=True, is_developer=True)
        self.assertEqual(self.handler.get_access_role("new@example.org"), "admin")
        self.handler.register_observed_user(user_email="new@example.org", user_name="Updated", job_title=None, department=None, location=None, profile_image_url="https://photos.example/new.jpg")
        self.handler.register_observed_user(user_email="new@example.org", user_name="Updated", job_title=None, department=None, location=None)
        self.assertEqual(self.handler.get_access_role("new@example.org"), "admin")
        self.assertEqual(self.handler.get_profile_image_url("new@example.org"), "https://photos.example/new.jpg")
        self.assertEqual(self.handler.get_webfusion_user("new@example.org")["NA_URL_PROFILE_IMG"], "https://photos.example/new.jpg")
        updated_photo = "/rffusion/static/img/profiles/updated.jpg"
        self.handler.update_profile_image_url("new@example.org", updated_photo)
        self.assertEqual(self.handler.get_profile_image_url("new@example.org"), updated_photo)
        self.assertEqual(self.handler.get_access_role("new@example.org"), "admin")
        with self.connection.cursor() as cursor:
            cursor.execute("UPDATE USERS SET DT_UPDATED_AT = '2020-01-01 00:00:00' WHERE NA_USER_EMAIL = 'new@example.org'")
        self.connection.commit()
        self.handler.update_profile_image_url("new@example.org", updated_photo)
        self.assertEqual(str(self.handler.get_webfusion_user("new@example.org")["DT_UPDATED_AT"]), "2020-01-01 00:00:00")
        with self.assertRaises(LookupError):
            self.handler.update_profile_image_url("missing@example.org", updated_photo)
        user_id = self.handler.get_webfusion_user("new@example.org")["ID_USER"]
        self.handler.delete_webfusion_user(user_email="new@example.org")
        self.assertIsNone(self.handler.get_webfusion_user("new@example.org"))
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS total FROM USER_ROLES WHERE ID_USER = %s", (user_id,))
            self.assertEqual(cursor.fetchone()["total"], 0)

    def test_fresh_schema_has_no_duplicated_profile_tables(self) -> None:
        """Verify new installations need only USERS and USER_ROLES.

        Args:
            None.

        Returns:
            None.
        """
        self.execute_script((SCRIPTS / "createWebFusionDB.sql").read_text())
        with self.connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            names = {next(iter(row.values())) for row in cursor.fetchall()}
        self.assertEqual(names, {"USERS", "USER_ROLES"})

    def test_privilege_failure_rolls_back_user_and_roles(self) -> None:
        """Roll back a user and its first role if the second role fails.

        Args:
            None.

        Returns:
            None.
        """
        self.execute_script((SCRIPTS / "createWebFusionDB.sql").read_text())
        original = self.handler._set_webfusion_role

        def fail_second_role(**kwargs: object) -> None:
            """Inject failure after an actual first privilege write.

            Args:
                kwargs: Role writer arguments. Type: dict[str, object].

            Returns:
                None.

            Raises:
                RuntimeError: For the developer assignment.
            """
            if kwargs["role"] == "developer":
                raise RuntimeError("Simulated second role failure")
            original(**kwargs)

        with patch.object(self.handler, "_set_webfusion_role", side_effect=fail_second_role):
            with self.assertRaises(RuntimeError):
                self.handler.create_webfusion_user(user_email="failed@example.org", user_name="Failed", job_title=None, department=None, location=None, is_admin=True, is_developer=True)
        self.assertIsNone(self.handler.get_webfusion_user("failed@example.org"))
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) AS total FROM USER_ROLES")
            self.assertEqual(cursor.fetchone()["total"], 0)
