"""Small database connection helpers for WebFusion.

WebFusion reads from three schemas:

- ``RFDATA`` for spectrum and repository metadata
- ``BPDATA`` for hosts, queues, and processing history
- ``RFFUSION_SUMMARY`` for pre-aggregated read models used by dashboards

Keeping the connection helpers in one place makes the service modules easier to
read for anyone who is still getting comfortable with Flask applications.
"""

import pymysql

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

def get_connection_rfdata():
    """Open a DictCursor connection to the spectrum catalog database."""
    return pymysql.connect(**DB_CFG_RFDATA)


def get_connection_bpdata():
    """Open a DictCursor connection to the operational host/queue database."""
    return pymysql.connect(**DB_CFG_BPDATA)


def get_connection_summary():
    """Open a DictCursor connection to the materialized summary database."""
    return pymysql.connect(**DB_CFG_RFFUSION_SUMMARY)


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
