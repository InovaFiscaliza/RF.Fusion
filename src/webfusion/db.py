"""Provide the database connections used by WebFusion.

``RFDATA`` owns spectrum and repository metadata, ``BPDATA`` owns the live
operational queues, ``RFFUSION_SUMMARY`` owns the materialized read models, and
``WEBFUSION`` owns access-control and observed identities. Service modules
select the narrowest source for their request and are responsible for closing
every connection they open.

WebFusion directory writes stay separate from operational queue and history
mutations, which remain owned by their feature services.
"""

from __future__ import annotations

from pathlib import Path
import runpy

import pymysql


SECRET_PATH = Path(__file__).with_name(".secret")
SECRET_CONFIG = runpy.run_path(str(SECRET_PATH))

# Keep cursor behavior in code because it is part of the database API contract.
DB_CFG_RFDATA = {
    **SECRET_CONFIG["DB_CFG_RFDATA"],
    "cursorclass": pymysql.cursors.DictCursor,
}
DB_CFG_BPDATA = {
    **SECRET_CONFIG["DB_CFG_BPDATA"],
    "cursorclass": pymysql.cursors.DictCursor,
}
DB_CFG_RFFUSION_SUMMARY = {
    **SECRET_CONFIG["DB_CFG_RFFUSION_SUMMARY"],
    "cursorclass": pymysql.cursors.DictCursor,
}
DB_CFG_WEBFUSION = {
    **SECRET_CONFIG["DB_CFG_WEBFUSION"],
    "cursorclass": pymysql.cursors.DictCursor,
}


def get_connection_rfdata() -> pymysql.connections.Connection:
    """Open a dictionary-cursor connection to the spectrum catalog database.

    Args:
        None.

    Returns:
        Open MariaDB connection. Type: pymysql.connections.Connection. Query
        results use dictionary rows because `cursorclass` is `DictCursor`.
    """
    return pymysql.connect(**DB_CFG_RFDATA)


def get_connection_bpdata() -> pymysql.connections.Connection:
    """Open a dictionary-cursor connection to the operational queue database.

    Args:
        None.

    Returns:
        Open MariaDB connection. Type: pymysql.connections.Connection. Query
        results use dictionary rows because `cursorclass` is `DictCursor`.
    """
    return pymysql.connect(**DB_CFG_BPDATA)


def get_connection_summary() -> pymysql.connections.Connection:
    """Open a dictionary-cursor connection to the materialized summary database.

    Args:
        None.

    Returns:
        Open MariaDB connection. Type: pymysql.connections.Connection. Query
        results use dictionary rows because `cursorclass` is `DictCursor`.
    """
    return pymysql.connect(**DB_CFG_RFFUSION_SUMMARY)


def get_connection_webfusion() -> pymysql.connections.Connection:
    """Open a dictionary-cursor connection to the WebFusion access-control database.

    Args:
        None.

    Returns:
        Open MariaDB connection. Type: pymysql.connections.Connection. Query
        results use dictionary rows because `cursorclass` is `DictCursor`.
    """
    return pymysql.connect(**DB_CFG_WEBFUSION)


HOST_CONNECTION_COLUMNS = frozenset(
    {
        "NA_HOST_PORT",
        "NA_HOST_USER",
        "NA_HOST_PASSWORD",
    }
)


def update_host_connection_value(*, host_id: int, column: str, value: int | str) -> None:
    """Persist one Zabbix-backed connection value for an operational host.

    Args:
        host_id: Identifier of the operational host to update. Type: int.
        column: Allowed `HOST` connection column. Type: str. Must be one of
            `NA_HOST_PORT`, `NA_HOST_USER`, or `NA_HOST_PASSWORD`.
        value: New port, username, or password value. Type: int | str.

    Returns:
        None. Commits the value to `BPDATA.HOST` when the host exists.

    Raises:
        ValueError: If `column` is not an allowed connection column.
        LookupError: If `host_id` does not identify an operational host.
        Exception: If a database operation fails after a rollback.
    """
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
