"""Provide the four database connections used by WebFusion.

``RFDATA`` owns spectrum and repository metadata, ``BPDATA`` owns the live
operational queues, ``RFFUSION_SUMMARY`` owns the materialized read models, and
``WEBFUSION`` owns access-control and observed identities. Service modules
select the narrowest source for their request and are responsible for closing
every connection they open.

WebFusion directory writes stay separate from operational queue and history
mutations, which remain owned by their feature services.
"""

import re

import pymysql


USER_LIST_LIMIT = 200
WEBFUSION_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
ADMIN_ROLE_TABLE = "ADMINS"
DEVELOPER_ROLE_TABLE = "DEVELOPERS"
WEBFUSION_ROLE_TABLES = frozenset({ADMIN_ROLE_TABLE, DEVELOPER_ROLE_TABLE})

DB_CFG_RFDATA = {
    "host": "10.88.0.33",
    "port": 3306,
    "user": "root",
    "password": "changeme",
    "database": "RFDATA",
    "cursorclass": pymysql.cursors.DictCursor
}

DB_CFG_BPDATA = {
    "host": "10.88.0.33",
    "port": 3306,
    "user": "root",
    "password": "changeme",
    "database": "BPDATA",
    "cursorclass": pymysql.cursors.DictCursor
}

DB_CFG_RFFUSION_SUMMARY = {
    "host": "10.88.0.33",
    "port": 3306,
    "user": "root",
    "password": "changeme",
    "database": "RFFUSION_SUMMARY",
    "cursorclass": pymysql.cursors.DictCursor
}

DB_CFG_WEBFUSION = {
    "host": "10.88.0.33",
    "port": 3306,
    "user": "root",
    "password": "changeme",
    "database": "WEBFUSION",
    "cursorclass": pymysql.cursors.DictCursor
}

def get_connection_rfdata():
    """Open a DictCursor connection to the spectrum catalog database."""
    return pymysql.connect(**DB_CFG_RFDATA)


def get_connection_bpdata():
    """Open a DictCursor connection to the operational host/queue database."""
    return pymysql.connect(**DB_CFG_BPDATA)


def get_connection_summary():
    """Open a DictCursor connection to the materialized summary database."""
    return pymysql.connect(**DB_CFG_RFFUSION_SUMMARY)


def get_connection_webfusion():
    """Open a DictCursor connection to the WebFusion access-control database."""
    return pymysql.connect(**DB_CFG_WEBFUSION)


def get_access_role(user_email: str) -> str | None:
    """Return the active WebFusion role assigned to an identity email."""
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


def register_observed_user(
    *,
    user_name: str | None,
    user_email: str,
    job_title: str | None,
    department: str | None,
    location: str | None,
) -> None:
    """Create or refresh an identity observed through the F5 headers."""
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


def list_webfusion_users(search: str = "") -> list[dict[str, object]]:
    """Return a bounded user directory with active role assignments."""
    normalized_search = str(search or "").strip()
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
    params: tuple[object, ...] = ()
    if normalized_search:
        search_value = f"%{normalized_search}%"
        query += """
            WHERE u.NA_USER_EMAIL LIKE %s
               OR COALESCE(u.NA_USER_NAME, '') LIKE %s
               OR COALESCE(u.NA_JOB_TITLE, '') LIKE %s
               OR COALESCE(u.NA_DEPARTMENT, '') LIKE %s
               OR COALESCE(u.NA_LOCATION, '') LIKE %s
        """
        params = (search_value,) * 5

    query += """
        ORDER BY COALESCE(u.NA_USER_NAME, u.NA_USER_EMAIL), u.NA_USER_EMAIL
        LIMIT %s
    """
    connection = get_connection_webfusion()
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params + (USER_LIST_LIMIT,))
            return list(cursor.fetchall())
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
    """Create a manual directory user and apply its selected privileges."""
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
    """Activate or deactivate the two WebFusion privileges for one user."""
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
    """Delete one directory identity and its active or inactive role records."""
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

            # Role rows must be removed first if foreign keys are added later.
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
    """Store the latest identity attributes for one normalized email."""
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
    """Return a normalized email key or reject a blank malformed value."""
    normalized_email = str(user_email or "").strip().casefold()
    if not WEBFUSION_EMAIL_PATTERN.fullmatch(normalized_email):
        raise ValueError("Informe um e-mail válido.")
    return normalized_email


def _normalize_optional_value(value: str | None) -> str | None:
    """Store blank optional profile attributes as database NULL values."""
    normalized_value = str(value or "").strip()
    return normalized_value or None


def _set_webfusion_role(
    *,
    cursor: pymysql.cursors.Cursor,
    user_email: str,
    role_table: str,
    is_active: bool,
) -> None:
    """Synchronize one role row with the directory profile."""
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


HOST_CONNECTION_COLUMNS = frozenset(
    {
        "NA_HOST_PORT",
        "NA_HOST_USER",
        "NA_HOST_PASSWORD",
    }
)


def update_host_connection_value(*, host_id: int, column: str, value: int | str) -> None:
    """Persist one Zabbix-backed connection value for an operational host."""
    if column not in HOST_CONNECTION_COLUMNS:
        raise ValueError("Coluna de conexão operacional não permitida.")

    connection = get_connection_bpdata()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT ID_HOST FROM HOST WHERE ID_HOST = %s",
                (host_id,),
            )
            if cursor.fetchone() is None:
                raise LookupError("O host operacional não foi encontrado no RF.Fusion.")

            cursor.execute(
                f"UPDATE HOST SET {column} = %s WHERE ID_HOST = %s",
                (value, host_id),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
