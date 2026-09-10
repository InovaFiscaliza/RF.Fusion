"""Persist WebFusion users and access roles in the WEBFUSION database.

This module owns the authentication package SQL for user profiles in `USERS`
and active access roles in `ADMINS` and `DEVELOPERS`.
"""

from __future__ import annotations

import re
import pymysql
from db import get_connection_webfusion


USER_LIST_LIMIT = 200
WEBFUSION_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
ADMIN_ROLE_TABLE = "ADMINS"
DEVELOPER_ROLE_TABLE = "DEVELOPERS"
WEBFUSION_ROLE_TABLES = frozenset({ADMIN_ROLE_TABLE, DEVELOPER_ROLE_TABLE})
USER_ROLE_FILTERS = frozenset({"all", "admin", "developer", "none"})


def get_access_role(user_email: str) -> str | None:
    """Return the active WebFusion role assigned to an identity email.

    Args:
        user_email: Normalized email that identifies the user. Type: str.

    Returns:
        Active access role. Type: str | None. Returns `admin`, `developer`, or
        `None` when no active role exists.
    """
    connection = get_connection_webfusion()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 'admin' AS NA_ROLE FROM ADMINS "
                "WHERE NA_USER_EMAIL = %s AND IS_ACTIVE = 1 "
                "LIMIT 1",
                (user_email,),
            )
            row = cursor.fetchone()
            if row:
                # Admin is the effective role when both roles are active.
                return row["NA_ROLE"]

            cursor.execute(
                "SELECT 'developer' AS NA_ROLE FROM DEVELOPERS "
                "WHERE NA_USER_EMAIL = %s AND IS_ACTIVE = 1 "
                "LIMIT 1",
                (user_email,),
            )
            row = cursor.fetchone()
            return row["NA_ROLE"] if row else None
    finally:
        connection.close()


def get_webfusion_user(user_email: str) -> dict[str, object] | None:
    """Return one WebFusion user with the assigned access profile.

    Args:
        user_email: Email that identifies the user. Type: str. The value is
            normalized and must match the WebFusion email format.

    Returns:
        User profile. Type: dict[str, object] | None. When found, the dictionary
            contains `ID_USER`, `NA_USER_NAME`, `NA_USER_EMAIL`,
            `NA_JOB_TITLE`, `NA_DEPARTMENT`, `NA_LOCATION`, `DT_CREATED_AT`,
            `DT_UPDATED_AT`, `NA_ROLE`, `IS_ADMIN`, and `IS_DEVELOPER`.
            `NA_ROLE` is `admin`, `developer`, or `None`; admin takes precedence
            when both profiles are active. Returns `None` when no user exists.

    Raises:
        ValueError: If `user_email` has an invalid format.
    """
    normalized_email = _normalize_webfusion_email(user_email)
    connection = get_connection_webfusion()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    u.ID_USER,
                    u.NA_USER_NAME,
                    u.NA_USER_EMAIL,
                    u.NA_JOB_TITLE,
                    u.NA_DEPARTMENT,
                    u.NA_LOCATION,
                    u.DT_CREATED_AT,
                    u.DT_UPDATED_AT,
                    CASE
                        WHEN EXISTS(
                            SELECT 1 FROM ADMINS a
                            WHERE a.NA_USER_EMAIL = u.NA_USER_EMAIL
                              AND a.IS_ACTIVE = 1
                        ) THEN 'admin'
                        WHEN EXISTS(
                            SELECT 1 FROM DEVELOPERS d
                            WHERE d.NA_USER_EMAIL = u.NA_USER_EMAIL
                              AND d.IS_ACTIVE = 1
                        ) THEN 'developer'
                        ELSE NULL
                    END AS NA_ROLE,
                    EXISTS(
                        SELECT 1 FROM ADMINS a
                        WHERE a.NA_USER_EMAIL = u.NA_USER_EMAIL
                          AND a.IS_ACTIVE = 1
                    ) AS IS_ADMIN,
                    EXISTS(
                        SELECT 1 FROM DEVELOPERS d
                        WHERE d.NA_USER_EMAIL = u.NA_USER_EMAIL
                          AND d.IS_ACTIVE = 1
                    ) AS IS_DEVELOPER
                FROM USERS u
                WHERE u.NA_USER_EMAIL = %s
                LIMIT 1
                """,
                (normalized_email,),
            )
            return cursor.fetchone()
    finally:
        connection.close()


def register_observed_user(
    *,
    user_name: str | None,
    user_email: str,
    job_title: str | None,
    department: str | None,
    location: str | None,
) -> None:
    """Create or refresh an identity observed through the F5 headers.

    Args:
        user_name: Observed display name. Type: str | None.
        user_email: Email that identifies the profile. Type: str. Must match the
            WebFusion email format.
        job_title: Observed job title. Type: str | None.
        department: Observed department. Type: str | None.
        location: Observed location. Type: str | None.

    Returns:
        None. Persists the normalized profile in `USERS` and commits it.

    Raises:
        ValueError: If `user_email` has an invalid format.
        Exception: If a database operation fails after a rollback.
    """
    normalized_email = _normalize_webfusion_email(user_email)
    connection = get_connection_webfusion()
    try:
        with connection.cursor() as cursor:
            _upsert_webfusion_user(
                cursor=cursor,
                user_name=_normalize_optional_value(user_name),
                user_email=normalized_email,
                job_title=_normalize_optional_value(job_title),
                department=_normalize_optional_value(department),
                location=_normalize_optional_value(location),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def list_webfusion_users(
    *,
    email: str = "",
    job_title: str = "",
    department: str = "",
    role: str = "all",
) -> list[dict[str, object]]:
    """Return a bounded user directory with active role assignments.

    Args:
        email: Exact normalized email selected in the user combobox. Type: str.
            An empty value does not constrain the directory.
        job_title: Exact job title selected in the combobox. Type: str. An empty
            value does not constrain the directory.
        department: Exact department selected in the combobox. Type: str. An
            empty value does not constrain the directory.
        role: Role filter applied to the directory. Type: str. Accepted
            values are `all`, `admin`, `developer`, and `none`.

    Returns:
        Matching directory users. Type: list[dict[str, object]]. Each item has
        the required keys `ID_USER`, `NA_USER_NAME`, `NA_USER_EMAIL`,
        `NA_JOB_TITLE`, `NA_DEPARTMENT`, `NA_LOCATION`, `DT_CREATED_AT`,
        `DT_UPDATED_AT`, `IS_ADMIN`, and `IS_DEVELOPER`.
    """
    normalized_email = str(email or "").strip().casefold()
    normalized_job_title = str(job_title or "").strip()
    normalized_department = str(department or "").strip()
    normalized_role_filter = _normalize_role_filter(role)
    query = """
        SELECT
            u.ID_USER,
            u.NA_USER_NAME,
            u.NA_USER_EMAIL,
            u.NA_JOB_TITLE,
            u.NA_DEPARTMENT,
            u.NA_LOCATION,
            u.DT_CREATED_AT,
            u.DT_UPDATED_AT,
            EXISTS(
                SELECT 1
                FROM ADMINS a
                WHERE a.NA_USER_EMAIL = u.NA_USER_EMAIL
                  AND a.IS_ACTIVE = 1
            ) AS IS_ADMIN,
            EXISTS(
                SELECT 1
                FROM DEVELOPERS d
                WHERE d.NA_USER_EMAIL = u.NA_USER_EMAIL
                  AND d.IS_ACTIVE = 1
            ) AS IS_DEVELOPER
        FROM USERS u
    """
    filters: list[str] = []
    params: list[object] = []
    if normalized_email:
        filters.append("u.NA_USER_EMAIL = %s")
        params.append(normalized_email)

    if normalized_job_title:
        filters.append("COALESCE(u.NA_JOB_TITLE, '') = %s")
        params.append(normalized_job_title)

    if normalized_department:
        filters.append("COALESCE(u.NA_DEPARTMENT, '') = %s")
        params.append(normalized_department)

    role_conditions = {
        "admin": "EXISTS(SELECT 1 FROM ADMINS a WHERE a.NA_USER_EMAIL = u.NA_USER_EMAIL AND a.IS_ACTIVE = 1)",
        "developer": "EXISTS(SELECT 1 FROM DEVELOPERS d WHERE d.NA_USER_EMAIL = u.NA_USER_EMAIL AND d.IS_ACTIVE = 1)",
        "none": "NOT EXISTS(SELECT 1 FROM ADMINS a WHERE a.NA_USER_EMAIL = u.NA_USER_EMAIL AND a.IS_ACTIVE = 1) AND NOT EXISTS(SELECT 1 FROM DEVELOPERS d WHERE d.NA_USER_EMAIL = u.NA_USER_EMAIL AND d.IS_ACTIVE = 1)",
    }
    if normalized_role_filter in role_conditions:
        filters.append(role_conditions[normalized_role_filter])

    if filters:
        query += " WHERE " + " AND ".join(filters)

    query += """
        ORDER BY COALESCE(u.NA_USER_NAME, u.NA_USER_EMAIL), u.NA_USER_EMAIL
        LIMIT %s
    """
    connection = get_connection_webfusion()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, tuple(params) + (USER_LIST_LIMIT,))
            return list(cursor.fetchall())
    finally:
        connection.close()


def list_user_filter_options() -> dict[str, list[dict[str, str]]]:
    """Return the preloaded option records for user-directory comboboxes.

    Args:
        None.

    Returns:
        Filter options. Type: dict[str, list[dict[str, str]]]. Contains `users`,
        `job_titles`, and `departments`. User items contain `value` as the email
        and `label` as the name followed by email; the other lists use matching
        `value` and `label` fields.
    """
    connection = get_connection_webfusion()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT NA_USER_EMAIL AS value, "
                "CONCAT(COALESCE(NULLIF(NA_USER_NAME, ''), NA_USER_EMAIL), ' - ', NA_USER_EMAIL) AS label "
                "FROM USERS ORDER BY COALESCE(NULLIF(NA_USER_NAME, ''), NA_USER_EMAIL), NA_USER_EMAIL"
            )
            users = list(cursor.fetchall())
            cursor.execute(
                "SELECT DISTINCT NA_JOB_TITLE AS value, NA_JOB_TITLE AS label "
                "FROM USERS WHERE NULLIF(NA_JOB_TITLE, '') IS NOT NULL ORDER BY NA_JOB_TITLE"
            )
            job_titles = list(cursor.fetchall())
            cursor.execute(
                "SELECT DISTINCT NA_DEPARTMENT AS value, NA_DEPARTMENT AS label "
                "FROM USERS WHERE NULLIF(NA_DEPARTMENT, '') IS NOT NULL ORDER BY NA_DEPARTMENT"
            )
            departments = list(cursor.fetchall())
            return {
                "users": users,
                "job_titles": job_titles,
                "departments": departments,
            }
    finally:
        connection.close()


def create_webfusion_user(
    *,
    user_name: str | None,
    user_email: str,
    job_title: str | None,
    department: str | None,
    location: str | None,
    is_admin: bool,
    is_developer: bool,
) -> None:
    """Create a manual directory user and apply its selected privileges.

    Args:
        user_name: User display name. Type: str | None.
        user_email: Email that identifies the new user. Type: str. Must be
            unique and match the WebFusion email format.
        job_title: User job title. Type: str | None.
        department: User department. Type: str | None.
        location: User location. Type: str | None.
        is_admin: Whether to activate the `admin` role. Type: bool.
        is_developer: Whether to activate the `developer` role. Type: bool.

    Returns:
        None. Persists the user and selected roles in one transaction.

    Raises:
        ValueError: If the email is invalid or already registered.
        Exception: If a database operation fails after a rollback.
    """
    normalized_email = _normalize_webfusion_email(user_email)
    connection = get_connection_webfusion()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT ID_USER FROM USERS WHERE NA_USER_EMAIL = %s",
                (normalized_email,),
            )
            if cursor.fetchone() is not None:
                raise ValueError("Já existe um usuário cadastrado com este e-mail.")

            _upsert_webfusion_user(
                cursor=cursor,
                user_name=_normalize_optional_value(user_name),
                user_email=normalized_email,
                job_title=_normalize_optional_value(job_title),
                department=_normalize_optional_value(department),
                location=_normalize_optional_value(location),
            )
            _set_webfusion_role(
                cursor=cursor,
                user_email=normalized_email,
                role_table=ADMIN_ROLE_TABLE,
                is_active=is_admin,
            )
            _set_webfusion_role(
                cursor=cursor,
                user_email=normalized_email,
                role_table=DEVELOPER_ROLE_TABLE,
                is_active=is_developer,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def update_webfusion_user_privileges(
    *,
    user_email: str,
    is_admin: bool,
    is_developer: bool,
) -> None:
    """Activate or deactivate the two WebFusion privileges for one user.

    Args:
        user_email: Email that identifies an existing user. Type: str.
        is_admin: Whether to activate the `admin` role. Type: bool.
        is_developer: Whether to activate the `developer` role. Type: bool.

    Returns:
        None. Updates `admin` and `developer` roles in one transaction.

    Raises:
        ValueError: If the email is invalid.
        LookupError: If no matching user exists in WebFusion.
        Exception: If a database operation fails after a rollback.
    """
    normalized_email = _normalize_webfusion_email(user_email)
    connection = get_connection_webfusion()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT ID_USER FROM USERS WHERE NA_USER_EMAIL = %s",
                (normalized_email,),
            )
            if cursor.fetchone() is None:
                raise LookupError("O usuário informado não está cadastrado no WebFusion.")

            _set_webfusion_role(
                cursor=cursor,
                user_email=normalized_email,
                role_table=ADMIN_ROLE_TABLE,
                is_active=is_admin,
            )
            _set_webfusion_role(
                cursor=cursor,
                user_email=normalized_email,
                role_table=DEVELOPER_ROLE_TABLE,
                is_active=is_developer,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def delete_webfusion_user(*, user_email: str) -> None:
    """Delete one directory identity and its active or inactive role records.

    Args:
        user_email: Email that identifies the user to delete. Type: str.

    Returns:
        None. Deletes role records and the user profile in one transaction.

    Raises:
        ValueError: If the email is invalid.
        LookupError: If no matching user exists in WebFusion.
        Exception: If a database operation fails after a rollback.
    """
    normalized_email = _normalize_webfusion_email(user_email)
    connection = get_connection_webfusion()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT ID_USER FROM USERS WHERE NA_USER_EMAIL = %s",
                (normalized_email,),
            )
            if cursor.fetchone() is None:
                raise LookupError("O usuário informado não está cadastrado no WebFusion.")

            for role_table in WEBFUSION_ROLE_TABLES:
                cursor.execute(
                    f"DELETE FROM {role_table} WHERE NA_USER_EMAIL = %s",
                    (normalized_email,),
                )

            cursor.execute(
                "DELETE FROM USERS WHERE NA_USER_EMAIL = %s",
                (normalized_email,),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _upsert_webfusion_user(
    *,
    cursor: pymysql.cursors.Cursor,
    user_name: str | None,
    user_email: str,
    job_title: str | None,
    department: str | None,
    location: str | None,
) -> None:
    """Store the latest identity attributes for one normalized email.

    Args:
        cursor: Open cursor for a WEBFUSION transaction. Type:
            pymysql.cursors.Cursor.
        user_name: Display name to persist. Type: str | None.
        user_email: Normalized email that identifies the profile. Type: str.
        job_title: Job title to persist. Type: str | None.
        department: Department to persist. Type: str | None.
        location: Location to persist. Type: str | None.

    Returns:
        None. Inserts the profile or updates its attributes when the email
        already exists.
    """
    cursor.execute(
        "INSERT INTO USERS ("
        "NA_USER_NAME, NA_USER_EMAIL, NA_JOB_TITLE, NA_DEPARTMENT, NA_LOCATION"
        ") VALUES (%s, %s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "NA_USER_NAME = VALUES(NA_USER_NAME), "
        "NA_JOB_TITLE = VALUES(NA_JOB_TITLE), "
        "NA_DEPARTMENT = VALUES(NA_DEPARTMENT), "
        "NA_LOCATION = VALUES(NA_LOCATION)",
        (user_name, user_email, job_title, department, location),
    )


def _normalize_webfusion_email(user_email: str) -> str:
    """Return a normalized email key or reject a blank malformed value.

    Args:
        user_email: Email received from a route or identity header. Type: str.

    Returns:
        Normalized email without surrounding whitespace. Type: str.

    Raises:
        ValueError: If the value is blank or does not match the expected format.
    """
    normalized_email = str(user_email or "").strip().casefold()
    if not WEBFUSION_EMAIL_PATTERN.fullmatch(normalized_email):
        raise ValueError("Informe um e-mail válido.")
    return normalized_email


def _normalize_optional_value(value: str | None) -> str | None:
    """Store blank optional profile attributes as database NULL values.

    Args:
        value: Optional textual profile attribute. Type: str | None.

    Returns:
        Attribute without surrounding whitespace. Type: str | None. Returns
        `None` for missing or whitespace-only values.
    """
    normalized_value = str(value or "").strip()
    return normalized_value or None


def _normalize_role_filter(role_filter: str) -> str:
    """Return an allowed directory role filter.

    Args:
        role_filter: Raw role filter received from a directory query. Type: str.

    Returns:
        Allowed role filter. Type: str. Returns `all` for an absent or unsupported
        value, and otherwise returns `admin`, `developer`, or `none`.
    """
    normalized_role_filter = str(role_filter or "").strip().casefold()
    return normalized_role_filter if normalized_role_filter in USER_ROLE_FILTERS else "all"


def _set_webfusion_role(
    *,
    cursor: pymysql.cursors.Cursor,
    user_email: str,
    role_table: str,
    is_active: bool,
) -> None:
    """Synchronize one role row with the directory profile.

    Args:
        cursor: Open cursor for a WEBFUSION transaction. Type:
            pymysql.cursors.Cursor.
        user_email: Normalized email for the user. Type: str.
        role_table: Role table to synchronize. Type: str. Must be `ADMINS` or
            `DEVELOPERS`.
        is_active: Whether the role must be active. Type: bool.

    Returns:
        None. Deactivates an existing role or creates and activates it using the
        current profile attributes in `USERS`.

    Raises:
        ValueError: If `role_table` is not an allowed role table.
    """
    if role_table not in WEBFUSION_ROLE_TABLES:
        raise ValueError("Tabela de privilégio WebFusion não permitida.")

    if not is_active:
        cursor.execute(
            f"UPDATE {role_table} SET IS_ACTIVE = 0 WHERE NA_USER_EMAIL = %s",
            (user_email,),
        )
        return

    cursor.execute(
        f"INSERT INTO {role_table} ("
        "NA_USER_NAME, NA_USER_EMAIL, NA_JOB_TITLE, NA_DEPARTMENT, NA_LOCATION, IS_ACTIVE"
        ") SELECT "
        "COALESCE(NA_USER_NAME, NA_USER_EMAIL), NA_USER_EMAIL, NA_JOB_TITLE, "
        "NA_DEPARTMENT, NA_LOCATION, 1 "
        "FROM USERS WHERE NA_USER_EMAIL = %s "
        "ON DUPLICATE KEY UPDATE "
        "NA_USER_NAME = VALUES(NA_USER_NAME), "
        "NA_JOB_TITLE = VALUES(NA_JOB_TITLE), "
        "NA_DEPARTMENT = VALUES(NA_DEPARTMENT), "
        "NA_LOCATION = VALUES(NA_LOCATION), "
        "IS_ACTIVE = 1",
        (user_email,),
    )