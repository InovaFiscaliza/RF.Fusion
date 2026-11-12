"""Provide the database connections used by WebFusion.

``RFDATA`` owns spectrum and repository metadata, ``BPDATA`` owns the live
operational queues, ``RFFUSION_SUMMARY`` owns the materialized read models, and
``WEBFUSION`` owns access-control and observed identities. Persistence handlers
select the narrowest source for their request and close every connection they
open. Transactional operations can instead use a caller-owned connection to keep
queue/history and checkpoint writes atomic.

This module contains connection setup only; domain SQL belongs to the handlers.
"""

from __future__ import annotations

import psycopg
from psycopg.rows import dict_row

from config import PG_APPLICATION_NAME, PG_CONNINFO


DbRow = dict[str, object]


def _connect(schema: str) -> psycopg.Connection[DbRow]:
    """Open an independent PostgreSQL session for one application schema.

    Args:
        schema (str): Server-owned schema selected by a public factory.
    Returns:
        psycopg.Connection[DbRow]: Dictionary rows, UTC and explicit search path.
            Caller owns commit, rollback and close; autocommit is disabled.
    Raises:
        ValueError: If explicit PostgreSQL settings are absent.
        psycopg.Error: If connection or session initialization fails.
    """
    if not PG_CONNINFO:
        raise ValueError("Configure RFFUSION_PG_CONNINFO or PG_CONNINFO in .secret.")
    connection = psycopg.connect(PG_CONNINFO, row_factory=dict_row, connect_timeout=5,
                                 application_name=PG_APPLICATION_NAME)
    try:
        connection.execute("SELECT set_config('search_path', %s, false)", (f'"{schema}", public',))
        connection.execute("SET TIME ZONE 'UTC'")
        connection.commit()
        return connection
    except Exception:
        connection.close()
        raise


def get_connection_rfdata() -> psycopg.Connection[DbRow]:
    """Open an analytical PostgreSQL session.

    Args:
        None.
    Returns:
        psycopg.Connection[DbRow]: Caller-owned session using RFDATA.
    Raises:
        psycopg.Error: If connection or session initialization fails.
    """
    return _connect("RFDATA")


def get_connection_bpdata() -> psycopg.Connection[DbRow]:
    """Open an operational PostgreSQL session.

    Args:
        None.
    Returns:
        psycopg.Connection[DbRow]: Caller-owned session using BPDATA.
    Raises:
        psycopg.Error: If connection or session initialization fails.
    """
    return _connect("BPDATA")


def get_connection_summary() -> psycopg.Connection[DbRow]:
    """Open a PostgreSQL session for derived read models.

    Args:
        None.
    Returns:
        psycopg.Connection[DbRow]: Caller-owned session using RFFUSION_SUMMARY.
    Raises:
        psycopg.Error: If connection or session initialization fails.
    """
    return _connect("RFFUSION_SUMMARY")


def get_connection_webfusion() -> psycopg.Connection[DbRow]:
    """Open a PostgreSQL session for identities and privileges.

    Args:
        None.
    Returns:
        psycopg.Connection[DbRow]: Caller-owned session using WEBFUSION.
    Raises:
        psycopg.Error: If connection or session initialization fails.
    """
    return _connect("WEBFUSION")
