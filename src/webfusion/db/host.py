"""Persist and query operational stations and server read models.

Read operations own and close their connections. Cursor helpers participate in
the caller transaction. No runtime probes or process caches belong here.
"""

from __future__ import annotations

from psycopg import Cursor

from modules.host.config import (
    FILE_TASK_BACKUP_TYPE,
    FILE_TASK_PROCESS_TYPE,
    TASK_PENDING,
    TASK_RUNNING,
    HOST_ACTIVITY_SOURCE,
    BACKUP_ACTIVITY_SOURCE,
    FILE_ACTIVITY_SOURCE,
    PROCESSED_FILE_SPECTRUM_METADATA_LIMIT,
)
from .config import (
    HOST_CONNECTION_COLUMNS,
    REPOSITORY_VOLUME,
)
from .connection import get_connection_bpdata, get_connection_rfdata, get_connection_summary
from .summary import (
    host_locations_query,
    host_operational_query,
    server_errors_query,
    server_totals_query,
)


def _build_host_filters(
    online_only: bool = False,
    search: str | None = None,
) -> tuple[list[str], list[object]]:
    """Build the station filters shared by the host projection queries.

    Args:
        online_only (bool): Keep only stations whose `IS_OFFLINE` flag is 0.
        search (str | None): Case-insensitive fragment of the station name.

    Returns:
        tuple[list[str], list[object]]: WHERE clauses without the keyword and
            their ordered bound values. Both lists are empty without filters.
    """

    where_clauses = []
    params = []

    if online_only:
        where_clauses.append('"IS_OFFLINE" = 0')

    if search:
        where_clauses.append('"NA_HOST_NAME" ILIKE %s')
        params.append(f"%{search}%")

    return where_clauses, params


def get_server_error_summary_rows(error_scope: str) -> list[dict[str, object]]:
    """Read grouped server error buckets from ``SERVER_ERROR_SUMMARY``.

    Args:
        error_scope (str): Error scope.

    Returns:
        list[dict[str, object]]: Rows containing TASK_STATE (str), ERROR_CODE (str |
            None), ERROR_SUMMARY (str) and ERROR_COUNT (int), ordered by count
            descending. Empty selection returns [].

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                "NA_TASK_STATE" AS "TASK_STATE",
                NULLIF(TRIM("NA_ERROR_CODE"), '') AS "ERROR_CODE",
                COALESCE(NULLIF(TRIM("NA_ERROR_SUMMARY"), ''), '(Sem mensagem)') AS "ERROR_SUMMARY",
                "NU_ERROR_COUNT" AS "ERROR_COUNT"
            FROM ({server_errors_query()}) server_errors
            WHERE "NA_ERROR_SCOPE" = %s
            ORDER BY "NU_ERROR_COUNT" DESC, "NA_TASK_STATE" ASC, "NA_ERROR_SUMMARY" ASC
            ''',
            (error_scope,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def get_summary_host_rows(
    search: str | None = None,
    online_only: bool = False,
) -> list[dict[str, object]]:
    """Read the server host table from the canonical host projection.

    This keeps the global dashboard table aligned with the same materialized
    snapshot used by the server-wide cards.

    Args:
        search (str | None): Search.
        online_only (bool): Online only.

    Returns:
        list[dict[str, object]]: Rows with ID_HOST (int), host name/address (str |
            None), port and flags (int | None), DT_* timestamps (datetime | None), NU_*
            integer counters, VL_* numeric GB volumes and PENDING_BACKUP_GB (numeric).
            Empty selection returns [].

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    conn = get_connection_summary()
    try:
        cur = conn.cursor()

        query = f'''
            SELECT
                "ID_HOST",
                "NA_HOST_NAME",
                "NA_HOST_ADDRESS",
                "NA_HOST_PORT",
                "IS_OFFLINE",
                "IS_BUSY",
                "DT_LAST_CHECK",
                "DT_LAST_DISCOVERY",
                "DT_LAST_BACKUP",
                "DT_LAST_PROCESSING",
                "NU_BACKUP_DONE_THIS_MONTH",
                "VL_BACKUP_DONE_GB_THIS_MONTH",
                "NU_BACKUP_PENDING_FILES_CURRENT",
                "NU_BACKUP_ERROR_FILES_CURRENT",
                "NU_PROCESSING_PENDING_FILES_CURRENT",
                "NU_PROCESSING_ERROR_FILES_CURRENT",
                "VL_PROCESSING_PENDING_GB_CURRENT",
                "VL_BACKUP_PENDING_GB_CURRENT" AS "PENDING_BACKUP_GB"
            FROM ({host_operational_query()}) operational
        '''

        where_clauses, params = _build_host_filters(
            online_only=online_only,
            search=search,
        )

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += ' ORDER BY "NA_HOST_NAME"'

        cur.execute(query, params)
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def get_host_monthly_metric_rows(host_id: int) -> list[dict[str, object]]:
    """Read monthly host metrics from the summary schema.

    Args:
        host_id (int): Operational station identifier.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(
            '''
            SELECT
                "DT_REFERENCE_MONTH",
                "NU_DISCOVERED_FILES",
                "VL_DISCOVERED_GB",
                "NU_BACKUP_DONE_FILES",
                "VL_BACKUP_DONE_GB",
                "NU_BACKUP_PENDING_FILES",
                "VL_BACKUP_PENDING_GB",
                "NU_BACKUP_ERROR_FILES",
                "VL_BACKUP_ERROR_GB",
                "NU_PROCESSING_DONE_FILES",
                "VL_PROCESSING_DONE_GB",
                "NU_PROCESSING_PENDING_FILES",
                "VL_PROCESSING_PENDING_GB",
                "NU_PROCESSING_ERROR_FILES",
                "VL_PROCESSING_ERROR_GB"
            FROM "HOST_MONTHLY_METRIC"
            WHERE "FK_HOST" = %s
            ORDER BY "DT_REFERENCE_MONTH" DESC NULLS LAST
            ''',
            (int(host_id),),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def get_host_error_summary_rows(
    host_id: int,
    error_scope: str,
) -> list[dict[str, object]]:
    """Read grouped host-specific error buckets from ``HOST_ERROR_SUMMARY``.

    Args:
        host_id (int): Operational station identifier.
        error_scope (str): Error scope.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(
            '''
            SELECT
                'ERROR' AS "TASK_STATE",
                NULLIF(TRIM("NA_ERROR_CODE"), '') AS "ERROR_CODE",
                COALESCE(NULLIF(TRIM("NA_ERROR_SUMMARY"), ''), '(Sem mensagem)') AS "ERROR_SUMMARY",
                "NU_ERROR_COUNT" AS "ERROR_COUNT"
            FROM "HOST_ERROR_METRIC"
            WHERE "FK_HOST" = %s
              AND "NA_ERROR_SCOPE" = %s
            ORDER BY "NU_ERROR_COUNT" DESC, "NA_ERROR_SUMMARY" ASC
            ''',
            (int(host_id), error_scope),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def get_all_hosts(
    online_only: bool = False,
    search: str | None = None,
) -> list[dict[str, object]]:
    """Return the host picker list used by the host page.

    Reads the current summary on each call, including for the upload selector.
    This is intentionally narrower than the server dashboard table because the
    host page mainly needs a quick navigation list, not a full operational grid.

    Args:
        online_only (bool): Online only.
        search (str | None): Search.

    Returns:
        list[dict[str, object]]: Rows with required ID_HOST (int), NA_HOST_NAME (str)
            and IS_OFFLINE (integer flag), ordered by station name. Empty selection
            returns [].

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    query = f'''
        SELECT "ID_HOST", "NA_HOST_NAME", "IS_OFFLINE"
        FROM ({host_operational_query()}) operational
    '''

    where_clauses, params = _build_host_filters(
        online_only=online_only,
        search=search,
    )

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += ' ORDER BY "NA_HOST_NAME"'

    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
    finally:
        conn.close()

    return rows


def get_server_current_summary_row() -> dict[str, object]:
    """Read the singleton row that backs the global `/server` summary cards.

    Args:
        None.

    Returns:
        dict[str, object]: Singleton ID_SUMMARY row with the selected NU_* integer
            counts, VL_* numeric GB volumes and NA_CURRENT_MONTH_LABEL (str). Empty
            selection returns {}.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                "ID_SUMMARY",
                "NA_CURRENT_MONTH_LABEL",
                "NU_TOTAL_HOSTS",
                "NU_ONLINE_HOSTS",
                "NU_OFFLINE_HOSTS",
                "NU_BUSY_HOSTS",
                "NU_DISCOVERED_FILES_TOTAL",
                "VL_DISCOVERED_GB_TOTAL",
                "NU_BACKUP_PENDING_FILES_TOTAL",
                "VL_BACKUP_PENDING_GB_TOTAL",
                "NU_BACKUP_ERROR_FILES_TOTAL",
                "VL_BACKUP_ERROR_GB_TOTAL",
                "NU_BACKUP_SUSPENDED_FILES_TOTAL",
                "VL_BACKUP_SUSPENDED_GB_TOTAL",
                "NU_BACKUP_DONE_FILES_TOTAL",
                "VL_BACKUP_DONE_GB_TOTAL",
                "NU_BACKUP_QUEUE_FILES_TOTAL",
                "VL_BACKUP_QUEUE_GB_TOTAL",
                "NU_BACKUP_QUEUE_RUNNING_FILES_TOTAL",
                "VL_BACKUP_QUEUE_RUNNING_GB_TOTAL",
                "NU_BACKUP_QUEUE_SUSPENDED_FILES_TOTAL",
                "VL_BACKUP_QUEUE_SUSPENDED_GB_TOTAL",
                "NU_PROCESSING_PENDING_FILES_TOTAL",
                "VL_PROCESSING_PENDING_GB_TOTAL",
                "NU_PROCESSING_DONE_FILES_TOTAL",
                "VL_PROCESSING_DONE_GB_TOTAL",
                "NU_PROCESSING_ERROR_FILES_TOTAL",
                "VL_PROCESSING_ERROR_GB_TOTAL",
                "NU_PROCESSING_FROZEN_FILES_TOTAL",
                "VL_PROCESSING_FROZEN_GB_TOTAL",
                "NU_PROCESSING_QUEUE_FILES_TOTAL",
                "VL_PROCESSING_QUEUE_GB_TOTAL",
                "NU_PROCESSING_QUEUE_RUNNING_FILES_TOTAL",
                "VL_PROCESSING_QUEUE_RUNNING_GB_TOTAL",
                "NU_PROCESSING_QUEUE_FROZEN_FILES_TOTAL",
                "VL_PROCESSING_QUEUE_FROZEN_GB_TOTAL",
                "NU_FACT_SPECTRUM_TOTAL",
                "NU_PAYLOAD_DELETED_FILES_TOTAL",
                "VL_PAYLOAD_DELETED_GB_TOTAL",
                "NU_BACKUP_DONE_THIS_MONTH",
                "VL_BACKUP_DONE_GB_THIS_MONTH"
            FROM ({server_totals_query()}) server_totals
            WHERE "ID_SUMMARY" = 1
            '''
        )
        row = cur.fetchone() or {}
    finally:
        conn.close()
    return row


def get_host_current_snapshot_row(host_id: int) -> dict[str, object]:
    """Read the summary snapshot row that backs one host drill-down.

    Args:
        host_id (int): Operational station identifier.

    Returns:
        dict[str, object]: Dictionary rows keyed by the SELECT aliases below; selected
            fields are present and nullable database values remain None. Empty selection
            returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                "ID_HOST",
                "NA_HOST_NAME",
                "NA_HOST_ADDRESS",
                "NA_HOST_PORT",
                "IS_OFFLINE",
                "IS_BUSY",
                "DT_LAST_CHECK",
                "DT_LAST_OFFLINE_AT",
                "NA_LAST_OFFLINE_DESCRIPTION",
                "DT_LAST_DISCOVERY_COMPLETED_AT",
                "NU_LAST_DISCOVERY_FILE_COUNT",
                "VL_LAST_DISCOVERY_KB",
                "DT_LAST_DISCOVERY_WITH_FILES",
                "DT_LAST_BACKUP",
                "NU_BACKUP_DONE_THIS_MONTH",
                "VL_BACKUP_DONE_GB_THIS_MONTH",
                "DT_LAST_PROCESSING",
                "NU_PROCESSING_DONE_THIS_MONTH",
                "VL_PROCESSING_DONE_GB_THIS_MONTH",
                "NU_BACKUP_QUEUE_FILES_TOTAL",
                "VL_BACKUP_QUEUE_GB_TOTAL",
                "NU_BACKUP_QUEUE_RUNNING_FILES_TOTAL",
                "VL_BACKUP_QUEUE_RUNNING_GB_TOTAL",
                "NU_BACKUP_QUEUE_SUSPENDED_FILES_TOTAL",
                "VL_BACKUP_QUEUE_SUSPENDED_GB_TOTAL",
                "NU_PROCESSING_QUEUE_FILES_TOTAL",
                "VL_PROCESSING_QUEUE_GB_TOTAL",
                "NU_PROCESSING_QUEUE_RUNNING_FILES_TOTAL",
                "VL_PROCESSING_QUEUE_RUNNING_GB_TOTAL",
                "NU_PROCESSING_QUEUE_FROZEN_FILES_TOTAL",
                "VL_PROCESSING_QUEUE_FROZEN_GB_TOTAL",
                "NU_DISCOVERED_FILES_TOTAL",
                "VL_DISCOVERED_GB_TOTAL",
                "NU_BACKUP_DONE_FILES_TOTAL",
                "VL_BACKUP_DONE_GB_TOTAL",
                "NU_PROCESSING_DONE_FILES_TOTAL",
                "VL_PROCESSING_DONE_GB_TOTAL",
                "NU_BACKUP_PENDING_FILES_CURRENT",
                "VL_BACKUP_PENDING_GB_CURRENT",
                "NU_BACKUP_ERROR_FILES_CURRENT",
                "VL_BACKUP_ERROR_GB_CURRENT",
                "NU_BACKUP_SUSPENDED_FILES_CURRENT",
                "VL_BACKUP_SUSPENDED_GB_CURRENT",
                "NU_PROCESSING_PENDING_FILES_CURRENT",
                "VL_PROCESSING_PENDING_GB_CURRENT",
                "NU_PROCESSING_ERROR_FILES_CURRENT",
                "VL_PROCESSING_ERROR_GB_CURRENT",
                "NU_PROCESSING_FROZEN_FILES_CURRENT",
                "VL_PROCESSING_FROZEN_GB_CURRENT",
                "NU_FACT_SPECTRUM_TOTAL",
                "NU_PAYLOAD_DELETED_FILES_TOTAL",
                "VL_PAYLOAD_DELETED_GB_TOTAL",
                "NA_CURRENT_SITE_LABEL",
                "NA_CURRENT_STATE_CODE",
                "NA_CURRENT_LOCALITY_LABEL",
                "VL_CURRENT_LATITUDE",
                "VL_CURRENT_LONGITUDE",
                "IS_SSH_FAILURE",
                "DT_LAST_SSH_EVALUATED_AT",
                "DT_LAST_SSH_FAILURE_AT",
                "NA_LAST_SSH_FAILURE_CODE",
                "NA_LAST_SSH_FAILURE_DESCRIPTION",
                "IS_GPS_GNSS_UNAVAILABLE",
                "DT_LAST_GPS_GNSS_EVALUATED_AT",
                "DT_LAST_GPS_GNSS_UNAVAILABLE_AT",
                "NA_LAST_GPS_GNSS_UNAVAILABLE_DESCRIPTION",
                "NA_LAST_GPS_GNSS_UNAVAILABLE_HOST_FILE_NAME"
            FROM ({host_operational_query()}) operational
            WHERE "ID_HOST" = %s
            LIMIT 1
            ''',
            (int(host_id),),
        )
        row = cur.fetchone() or {}
    finally:
        conn.close()
    return row


# Live and detail reads share these aliases, so each table has one activity mapper.
_HOST_TASK_ACTIVITY_COLUMNS = '''
    "ID_HOST_TASK" AS "TASK_ID",
    "FK_HOST" AS "HOST_ID",
    "NU_TYPE" AS "TASK_TYPE",
    "NU_STATUS" AS "TASK_STATUS",
    "NA_MESSAGE" AS "MESSAGE",
    "DT_HOST_TASK" AS "UPDATED_AT"
'''
_FILE_TASK_ACTIVITY_COLUMNS = '''
    "ID_FILE_TASK" AS "TASK_ID",
    "FK_HOST" AS "HOST_ID",
    "NU_TYPE" AS "TASK_TYPE",
    "NU_STATUS" AS "TASK_STATUS",
    "NA_HOST_FILE_NAME" AS "FILE_NAME",
    "NA_HOST_FILE_PATH" AS "FILE_PATH",
    "VL_FILE_SIZE_KB_HOST" AS "FILE_SIZE_KB",
    "NA_MESSAGE" AS "MESSAGE",
    "DT_FILE_TASK" AS "UPDATED_AT"
'''


def _host_activity(row: dict[str, object]) -> dict[str, object]:
    """Map one row selected with `_HOST_TASK_ACTIVITY_COLUMNS` to an activity.

    Args:
        row (dict[str, object]): HOST_TASK row with the activity aliases.

    Returns:
        dict[str, object]: Keys source (str), task_id/host_id/task_type/status
            (int), message (str | None) and updated_at (datetime | None).
    """
    return {
        "source": HOST_ACTIVITY_SOURCE,
        "task_id": int(row["TASK_ID"]),
        "host_id": int(row["HOST_ID"]),
        "task_type": int(row["TASK_TYPE"]),
        "status": int(row["TASK_STATUS"]),
        "message": row.get("MESSAGE") or None,
        "updated_at": row.get("UPDATED_AT"),
    }


def _file_activity(row: dict[str, object], source: str) -> dict[str, object]:
    """Map one row selected with `_FILE_TASK_ACTIVITY_COLUMNS` to an activity.

    Args:
        row (dict[str, object]): FILE_TASK row with the activity aliases.
        source (str): Activity source reported to the interface.

    Returns:
        dict[str, object]: Keys source (str), task_id/host_id/task_type/status
            (int), file_name/file_path/message (str | None), file_size_kb
            (numeric | None) and updated_at (datetime | None). A missing file
            name falls back to the host path.
    """
    return {
        "source": source,
        "task_id": int(row["TASK_ID"]),
        "host_id": int(row["HOST_ID"]),
        "task_type": int(row["TASK_TYPE"]),
        "status": int(row["TASK_STATUS"]),
        "file_name": row.get("FILE_NAME") or row.get("FILE_PATH") or None,
        "file_path": row.get("FILE_PATH") or None,
        "file_size_kb": row.get("FILE_SIZE_KB"),
        "message": row.get("MESSAGE") or None,
        "updated_at": row.get("UPDATED_AT"),
    }


def _read_active_host_activity(
    cursor: Cursor[dict[str, object]],
    host_id: int,
) -> dict[str, object] | None:
    """Return the host-level task currently claiming operational attention.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor.
        host_id (int): Operational station identifier.

    Returns:
        dict[str, object] | None: Activity built by `_host_activity`, preferring
            a running task over the oldest pending one; None without either.
    """
    cursor.execute(
        f'''
        SELECT {_HOST_TASK_ACTIVITY_COLUMNS}
        FROM "HOST_TASK"
        WHERE "FK_HOST" = %s
          AND "NU_STATUS" IN (%s, %s)
        ORDER BY
            CASE WHEN "NU_STATUS" = %s THEN 0 ELSE 1 END,
            "DT_HOST_TASK" ASC,
            "ID_HOST_TASK" ASC
        LIMIT 1
        ''',
        (host_id, TASK_PENDING, TASK_RUNNING, TASK_RUNNING),
    )
    row = cursor.fetchone()
    return _host_activity(row) if row else None


def _read_active_backup_activity(
    cursor: Cursor[dict[str, object]],
    host_id: int,
) -> dict[str, object] | None:
    """Return the running backup file, or the next pending backup file.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor.
        host_id (int): Operational station identifier.

    Returns:
        dict[str, object] | None: Activity built by `_file_activity` with the
            backup source; None when no backup file is pending or running.
    """
    cursor.execute(
        f'''
        SELECT {_FILE_TASK_ACTIVITY_COLUMNS}
        FROM "FILE_TASK"
        WHERE "FK_HOST" = %s
          AND "NU_TYPE" = %s
          AND "NU_STATUS" IN (%s, %s)
        ORDER BY
            CASE WHEN "NU_STATUS" = %s THEN 0 ELSE 1 END,
            "DT_FILE_TASK" ASC,
            "ID_FILE_TASK" ASC
        LIMIT 1
        ''',
        (
            host_id,
            FILE_TASK_BACKUP_TYPE,
            TASK_PENDING,
            TASK_RUNNING,
            TASK_RUNNING,
        ),
    )
    row = cursor.fetchone()
    return _file_activity(row, BACKUP_ACTIVITY_SOURCE) if row else None


def _read_running_file_activity(
    cursor: Cursor[dict[str, object]],
    host_id: int,
) -> dict[str, object] | None:
    """Return the oldest running file task of any stage.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor.
        host_id (int): Operational station identifier.

    Returns:
        dict[str, object] | None: Activity built by `_file_activity` with the
            file source; None when no file task is running.
    """
    cursor.execute(
        f'''
        SELECT {_FILE_TASK_ACTIVITY_COLUMNS}
        FROM "FILE_TASK"
        WHERE "FK_HOST" = %s
          AND "NU_STATUS" = %s
        ORDER BY "DT_FILE_TASK" ASC, "ID_FILE_TASK" ASC
        LIMIT 1
        ''',
        (host_id, TASK_RUNNING),
    )
    row = cursor.fetchone()
    return _file_activity(row, FILE_ACTIVITY_SOURCE) if row else None


def _read_host_activity_detail(
    cursor: Cursor[dict[str, object]],
    host_id: int,
    task_id: int,
) -> dict[str, object] | None:
    """Read one host-task row so an open dialog can reach its terminal state.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor.
        host_id (int): Operational station identifier.
        task_id (int): Persistent task identifier.

    Returns:
        dict[str, object] | None: Activity built by `_host_activity`; None when
            the task does not belong to the station.
    """
    cursor.execute(
        f'''
        SELECT {_HOST_TASK_ACTIVITY_COLUMNS}
        FROM "HOST_TASK"
        WHERE "ID_HOST_TASK" = %s
          AND "FK_HOST" = %s
        LIMIT 1
        ''',
        (task_id, host_id),
    )
    row = cursor.fetchone()
    return _host_activity(row) if row else None


def _read_file_activity_detail(
    cursor: Cursor[dict[str, object]],
    host_id: int,
    task_id: int,
    source: str,
) -> dict[str, object] | None:
    """Read one file row without losing it when the worker changes its stage.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor.
        host_id (int): Operational station identifier.
        task_id (int): Persistent task identifier.
        source (str): Activity source requested by the dialog.

    Returns:
        dict[str, object] | None: Activity built by `_file_activity` plus
            server_file_name (str | None); None when the task no longer exists.
    """
    cursor.execute(
        f'''
        SELECT {_FILE_TASK_ACTIVITY_COLUMNS},
            "NA_SERVER_FILE_NAME" AS "SERVER_FILE_NAME"
        FROM "FILE_TASK"
        WHERE "ID_FILE_TASK" = %s
          AND "FK_HOST" = %s
        LIMIT 1
        ''',
        (task_id, host_id),
    )
    row = cursor.fetchone()
    if not row:
        return None

    activity = _file_activity(row, source)
    activity["server_file_name"] = row.get("SERVER_FILE_NAME") or None
    return activity


def _read_file_history_activity(
    cursor: Cursor[dict[str, object]],
    host_id: int,
    file_path: str | None = None,
    file_name: str | None = None,
) -> dict[str, object] | None:
    """Return the durable processing result after its live task is deleted.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor in the active
            transaction.
        host_id (int): Operational station identifier.
        file_path (str | None): File path.
        file_name (str | None): File name.

    Returns:
        dict[str, object] | None: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor.execute(
        '''
        SELECT
            "FK_HOST" AS "HOST_ID",
            "NA_HOST_FILE_PATH" AS "FILE_PATH",
            "NA_HOST_FILE_NAME" AS "FILE_NAME",
            "NA_SERVER_FILE_NAME" AS "SERVER_FILE_NAME",
            "NU_STATUS_PROCESSING" AS "TASK_STATUS",
            "NA_MESSAGE" AS "MESSAGE",
            "DT_PROCESSED" AS "UPDATED_AT"
        FROM "FILE_TASK_HISTORY"
        WHERE "FK_HOST" = %s
          AND "NA_HOST_FILE_PATH" = %s
          AND "NA_HOST_FILE_NAME" = %s
        LIMIT 1
        ''',
        (host_id, file_path, file_name),
    )
    row = cursor.fetchone()
    if not row:
        return None

    return {
        "source": FILE_ACTIVITY_SOURCE,
        "task_id": 0,
        "host_id": int(row["HOST_ID"]),
        "task_type": FILE_TASK_PROCESS_TYPE,
        "status": int(row["TASK_STATUS"]),
        "file_name": row.get("FILE_NAME") or None,
        "file_path": row.get("FILE_PATH") or None,
        "server_file_name": row.get("SERVER_FILE_NAME") or None,
        "message": row.get("MESSAGE") or None,
        "updated_at": row.get("UPDATED_AT"),
    }


def get_processed_file_spectrum_metadata(
    server_file_name: str,
) -> dict[str, object] | None:
    """Read summary and per-spectrum metadata for one repository file.

    Args:
        server_file_name (str | None): Server file name.

    Returns:
        dict[str, object] | None: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    normalized_name = str(server_file_name or "").strip()
    if not normalized_name:
        return None

    connection = get_connection_rfdata()
    try:
        cursor = connection.cursor()
        cursor.execute(
            '''
            SELECT
                repos."ID_FILE",
                repos."NA_FILE",
                COUNT(DISTINCT bridge."FK_SPECTRUM") AS "SPECTRUM_COUNT",
                MIN(spectrum."DT_TIME_START") AS "TIME_START",
                MAX(spectrum."DT_TIME_END") AS "TIME_END",
                MIN(spectrum."NU_FREQ_START") AS "FREQUENCY_START",
                MAX(spectrum."NU_FREQ_END") AS "FREQUENCY_END",
                COUNT(DISTINCT spectrum."FK_SITE") AS "SITE_COUNT",
                COUNT(DISTINCT spectrum."FK_EQUIPMENT") AS "EQUIPMENT_COUNT"
            FROM "DIM_SPECTRUM_FILE" repos
            JOIN "BRIDGE_SPECTRUM_FILE" bridge
              ON bridge."FK_FILE" = repos."ID_FILE"
            JOIN "FACT_SPECTRUM" spectrum
              ON spectrum."ID_SPECTRUM" = bridge."FK_SPECTRUM"
            WHERE repos."NA_VOLUME" = %s
              AND repos."NA_FILE" = %s
            GROUP BY repos."ID_FILE", repos."NA_FILE"
            ORDER BY repos."ID_FILE" DESC
            LIMIT 1
            ''',
            (REPOSITORY_VOLUME, normalized_name),
        )
        metadata = cursor.fetchone()
        if not metadata:
            return None

        cursor.execute(
            '''
            SELECT
                spectrum."ID_SPECTRUM",
                spectrum."NU_FREQ_START" AS "FREQUENCY_START",
                spectrum."NU_FREQ_END" AS "FREQUENCY_END",
                spectrum."NA_DESCRIPTION" AS "DESCRIPTION",
                spectrum."DT_TIME_START" AS "TIME_START",
                spectrum."DT_TIME_END" AS "TIME_END",
                COALESCE(site."NA_SITE", CONCAT('Site ', spectrum."FK_SITE")) AS "LOCALITY",
                COALESCE(
                    equipment."NA_EQUIPMENT",
                    CONCAT('Equipamento ', spectrum."FK_EQUIPMENT")
                ) AS "EQUIPMENT"
            FROM "DIM_SPECTRUM_FILE" repos
            JOIN "BRIDGE_SPECTRUM_FILE" bridge
              ON bridge."FK_FILE" = repos."ID_FILE"
            JOIN "FACT_SPECTRUM" spectrum
              ON spectrum."ID_SPECTRUM" = bridge."FK_SPECTRUM"
            LEFT JOIN "DIM_SPECTRUM_SITE" site
              ON site."ID_SITE" = spectrum."FK_SITE"
            LEFT JOIN "DIM_SPECTRUM_EQUIPMENT" equipment
              ON equipment."ID_EQUIPMENT" = spectrum."FK_EQUIPMENT"
            WHERE repos."ID_FILE" = %s
            ORDER BY spectrum."DT_TIME_START" DESC NULLS LAST, spectrum."ID_SPECTRUM" DESC
            LIMIT %s
            ''',
            (metadata["ID_FILE"], PROCESSED_FILE_SPECTRUM_METADATA_LIMIT),
        )
        metadata["SPECTRA"] = cursor.fetchall() or []
        return metadata
    finally:
        connection.close()


def get_host_current_flags(host_id: int) -> dict[str, bool | None] | None:
    """Read current host flags without the display cache or task inference.

    Args:
        host_id (int): Operational station identifier.

    Returns:
        dict[str, bool | None] | None: Required IS_BUSY and IS_OFFLINE booleans plus
            IS_SSH_FAILURE and IS_GPS_GNSS_UNAVAILABLE (bool | None). Missing signal
            evidence is None; a missing station returns None.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    connection = get_connection_bpdata()
    try:
        cursor = connection.cursor()
        cursor.execute(
            'SELECT "IS_BUSY", "IS_OFFLINE" FROM "HOST" WHERE "ID_HOST" = %s',
            (host_id,),
        )
        host = cursor.fetchone()
    finally:
        connection.close()

    if not host:
        return None

    connection = get_connection_summary()
    try:
        cursor = connection.cursor()
        cursor.execute(
            f'''
            SELECT "IS_SSH_FAILURE", "IS_GPS_GNSS_UNAVAILABLE"
            FROM ({host_operational_query()}) operational
            WHERE "ID_HOST" = %s
            ''',
            (host_id,),
        )
        signals = cursor.fetchone() or {}
    finally:
        connection.close()

    return {
        "IS_BUSY": bool(host["IS_BUSY"]),
        "IS_OFFLINE": bool(host["IS_OFFLINE"]),
        "IS_SSH_FAILURE": (
            bool(signals["IS_SSH_FAILURE"])
            if signals.get("IS_SSH_FAILURE") is not None else None
        ),
        "IS_GPS_GNSS_UNAVAILABLE": (
            bool(signals["IS_GPS_GNSS_UNAVAILABLE"])
            if signals.get("IS_GPS_GNSS_UNAVAILABLE") is not None else None
        ),
    }


def load_host_current_activity(host_id: int) -> dict[str, object] | None:
    """Select the activity that best describes what a station is doing now.

    Args:
        host_id (int): Operational station identifier.

    Returns:
        dict[str, object] | None: Running host task, running backup file or any
            running file task, in this order; otherwise the pending host task or
            the next pending backup file. None when the station is idle.

    Raises:
        Exception: Database failure; propagated after closing the connection.
    """
    connection = get_connection_bpdata()
    try:
        cursor = connection.cursor()
        host_activity = _read_active_host_activity(cursor, int(host_id))
        if host_activity and host_activity["status"] == TASK_RUNNING:
            return host_activity

        backup_activity = _read_active_backup_activity(cursor, int(host_id))
        if backup_activity and backup_activity["status"] == TASK_RUNNING:
            return backup_activity

        file_activity = _read_running_file_activity(cursor, int(host_id))
        if file_activity:
            return file_activity

        # A waiting host task remains more descriptive than a future file,
        # but it must not hide a backup transfer that is already in progress.
        return host_activity or backup_activity
    finally:
        connection.close()


def load_host_activity_detail(
    host_id: int,
    source: str,
    task_id: int,
    file_path: str | None = None,
    file_name: str | None = None,
) -> dict[str, object] | None:
    """Follow one activity opened in the station dialog until it finishes.

    Args:
        host_id (int): Operational station identifier.
        source (str): Activity source previously returned to the dialog.
        task_id (int): Persistent task identifier.
        file_path (str | None): Host file path used after the task is deleted.
        file_name (str | None): Host file name used after the task is deleted.

    Returns:
        dict[str, object] | None: Current host or file activity. A finished file
            falls back to its FILE_TASK_HISTORY processing result when path and
            name are supplied. None for unknown sources or missing rows.

    Raises:
        Exception: Database failure; propagated after closing the connection.
    """
    connection = get_connection_bpdata()
    try:
        cursor = connection.cursor()
        if source == HOST_ACTIVITY_SOURCE:
            return _read_host_activity_detail(cursor, int(host_id), int(task_id))
        if source in {BACKUP_ACTIVITY_SOURCE, FILE_ACTIVITY_SOURCE}:
            activity = _read_file_activity_detail(
                cursor,
                int(host_id),
                int(task_id),
                source,
            )
            if activity:
                return activity
            if file_path and file_name:
                return _read_file_history_activity(
                    cursor,
                    int(host_id),
                    file_path,
                    file_name,
                )
        return None
    finally:
        connection.close()


def load_host_location_history(
    normalized_host_id: int,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Read the equipment links and site visits of one station.

    Args:
        normalized_host_id (int): Operational station identifier.

    Returns:
        tuple[list[dict[str, object]], list[dict[str, object]]]: Equipment rows
            (ID_EQUIPMENT: int, NA_EQUIPMENT: str, MATCH_TYPE: str, MATCH_CONFIDENCE:
            numeric) and visit rows (NU_VISIT: int, ID_SITE: int, ID_COUNTY/ID_DISTRICT:
            int | None, locality/geographic labels: str | None,
            FIRST_SEEN_AT/LAST_SEEN_AT: formatted str | None, SPECTRUM_COUNT: int,
            IS_OVERLAPPING: integer flag). Both lists may be empty.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(
            '''
            SELECT
                "FK_EQUIPMENT" AS "ID_EQUIPMENT",
                "NA_EQUIPMENT",
                "NA_MATCH_TYPE" AS "MATCH_TYPE",
                "VL_MATCH_CONFIDENCE" AS "MATCH_CONFIDENCE"
            FROM (SELECT link.*, equipment."NA_EQUIPMENT" FROM "RFFUSION_SUMMARY"."HOST_EQUIPMENT_LINK" link JOIN "RFDATA"."DIM_SPECTRUM_EQUIPMENT" equipment ON equipment."ID_EQUIPMENT" = link."FK_EQUIPMENT") linked
            WHERE "FK_HOST" = %s
              AND "IS_PRIMARY_LINK" = 1
            ORDER BY "VL_MATCH_CONFIDENCE" DESC, "NA_EQUIPMENT" ASC
            ''',
            (normalized_host_id,),
        )
        equipment_matches = cur.fetchall()

        cur.execute(
            f'''
            SELECT
                visit."NU_VISIT",
                visit."FK_SITE" AS "ID_SITE",
                location."FK_COUNTY" AS "ID_COUNTY",
                location."FK_DISTRICT" AS "ID_DISTRICT",
                location."NA_LOCALITY_LABEL" AS "LOCALITY_LABEL",
                location."NA_COUNTY_NAME" AS "COUNTY_NAME",
                location."NA_STATE_NAME" AS "STATE_NAME",
                location."NA_STATE_CODE" AS "STATE_CODE",
                to_char(visit."DT_FIRST_SEEN_AT", 'DD/MM/YYYY HH24:MI:SS') AS "FIRST_SEEN_AT",
                to_char(visit."DT_LAST_SEEN_AT", 'DD/MM/YYYY HH24:MI:SS') AS "LAST_SEEN_AT",
                visit."NU_SPECTRUM_COUNT" AS "SPECTRUM_COUNT",
                visit."IS_OVERLAPPING"
            FROM "HOST_LOCATION_VISIT" visit
            LEFT JOIN ({host_locations_query()}) location
              ON location."FK_HOST" = visit."FK_HOST" AND location."FK_SITE" = visit."FK_SITE"
            WHERE visit."FK_HOST" = %s
            ORDER BY visit."DT_FIRST_SEEN_AT" DESC NULLS LAST, visit."NU_VISIT" DESC
            ''',
            (normalized_host_id,),
        )
        location_history = cur.fetchall()
    finally:
        conn.close()
    return equipment_matches, location_history


def update_host_connection_value(
    *,
    host_id: int,
    column: str,
    value: int | str,
) -> None:
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
                'SELECT "ID_HOST" FROM "HOST" WHERE "ID_HOST" = %s',
                (host_id,),
            )
            if cursor.fetchone() is None:
                raise LookupError("O host operacional não foi encontrado no RF.Fusion.")

            cursor.execute(
                f'''UPDATE "HOST" SET "{column}" = %s WHERE "ID_HOST" = %s''',
                (value, host_id),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

