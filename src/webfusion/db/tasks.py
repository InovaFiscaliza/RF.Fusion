"""Persist bounded task queries and atomic queue/history transitions.

Connections and cursors belong to the caller. Each operation preserves its
existing commit/rollback boundary; outbox publication is an independent write.
"""

from __future__ import annotations

from datetime import datetime
import json

from psycopg import Connection, Cursor

from modules.tasks.config import (
    TASK_FROZEN,
    TASK_DONE,
    TASK_PENDING,
    HOST_TASK_INTERACTIVE_CHECK_TYPE,
    FILE_TASK_BACKUP_TYPE,
    FILE_TASK_PROCESS_TYPE,
)
from .config import (
    HISTORY_PROCESSED_RECENT_INDEX,
    HISTORY_ORDER_BY_ID,
    HISTORY_ORDER_BY_PROCESSED,
    HISTORY_DATE_FIELDS,
    FILE_TASK_DATE_FIELDS,
    HISTORY_PHASE_STATUS_FIELDS,
)


def _apply_common_filters(
    sql_parts: list[str],
    params: list[object],
    *,
    alias: str,
    host_id: int | None,
    search: str,
    task_type: int | None,
    status: int | None,
    search_columns: tuple[str, ...],
) -> None:
    """Append shared search, type, and status filters to one query.

    Args:
        sql_parts (list[str]): Sql parts.
        params (list[object]): Params.
        alias (str): Alias.
        host_id (int | None): Operational station identifier.
        search (str): Search.
        task_type (int | None): Validated queue stage/type code.
        status (int | None): Validated queue status.
        search_columns (tuple[str, ...]): Search columns.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    if host_id is not None:
        sql_parts.append(f'''{alias}."FK_HOST" = %s''')
        params.append(int(host_id))

    if task_type is not None:
        sql_parts.append(f'''{alias}."NU_TYPE" = %s''')
        params.append(int(task_type))

    if status is not None:
        sql_parts.append(f'''{alias}."NU_STATUS" = %s''')
        params.append(int(status))

    if not search:
        return

    like_value = f"%{search}%"
    clauses = [f"{column} ILIKE %s" for column in search_columns]
    sql_parts.append("(" + " OR ".join(clauses) + ")")
    params.extend([like_value] * len(search_columns))


def list_maintenance_hosts(
    db: Connection[dict[str, object]],
) -> list[dict[str, object]]:
    """Return the small host catalog used by maintenance selectors.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.

    Returns:
        list[dict[str, object]]: Rows with ID_HOST (int), NA_HOST_NAME (str) and
            IS_OFFLINE (integer flag), ordered by name and identifier. Empty selection
            returns [].

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor = db.cursor()
    cursor.execute(
        '''
        SELECT
            "ID_HOST",
            "NA_HOST_NAME",
            "IS_OFFLINE"
        FROM "HOST"
        ORDER BY "NA_HOST_NAME" ASC, "ID_HOST" ASC
        '''
    )
    return cursor.fetchall() or []


def list_file_task_hosts(db: Connection[dict[str, object]]) -> list[dict[str, object]]:
    """Return only hosts that currently have at least one file task.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.

    Returns:
        list[dict[str, object]]: Rows with ID_HOST (int), NA_HOST_NAME (str) and
            IS_OFFLINE (integer flag) for stations with queued files. Empty selection
            returns [].

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor = db.cursor()
    cursor.execute(
        '''
        SELECT
            h."ID_HOST",
            h."NA_HOST_NAME",
            h."IS_OFFLINE"
        FROM "HOST" h
        WHERE EXISTS (
            SELECT 1
            FROM "FILE_TASK" ft
            WHERE ft."FK_HOST" = h."ID_HOST"
        )
        ORDER BY h."NA_HOST_NAME" ASC, h."ID_HOST" ASC
        '''
    )
    return cursor.fetchall() or []


def _history_order_by(db: Connection[dict[str, object]]) -> str:
    """Use processing recency only after its supporting index is available.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.

    Returns:
        str: ORDER BY expression; processing recency when its index exists,
            otherwise the history identifier.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT 1
        FROM pg_catalog.pg_indexes
        WHERE schemaname = current_schema()
          AND tablename = 'FILE_TASK_HISTORY'
          AND indexname = %s
        LIMIT 1
        """,
        (HISTORY_PROCESSED_RECENT_INDEX,),
    )
    return HISTORY_ORDER_BY_PROCESSED if cursor.fetchall() else HISTORY_ORDER_BY_ID


def load_host_tasks_for_action(
    db: Connection[dict[str, object]],
    task_ids: list[int],
) -> list[dict[str, object]]:
    """Load the HOST_TASK rows targeted by one bulk action.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        task_ids (list[int]): Task ids.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    if not task_ids:
        return []

    placeholders = ", ".join(["%s"] * len(task_ids))
    cursor = db.cursor()
    cursor.execute(
        f'''
        SELECT
            ht."ID_HOST_TASK",
            ht."FK_HOST",
            ht."NU_TYPE",
            ht."NU_STATUS",
            ht."NU_PID",
            h."NA_HOST_NAME",
            h."IS_OFFLINE",
            h."IS_BUSY"
        FROM "HOST_TASK" ht
        JOIN "HOST" h
          ON h."ID_HOST" = ht."FK_HOST"
        WHERE ht."ID_HOST_TASK" IN ({placeholders})
        ''',
        tuple(task_ids),
    )
    return cursor.fetchall() or []


def load_file_tasks_for_action(
    db: Connection[dict[str, object]],
    task_ids: list[int],
) -> list[dict[str, object]]:
    """Load the FILE_TASK rows targeted by one bulk action.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        task_ids (list[int]): Task ids.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    if not task_ids:
        return []

    placeholders = ", ".join(["%s"] * len(task_ids))
    cursor = db.cursor()
    cursor.execute(
        f'''
        SELECT
            ft."ID_FILE_TASK",
            ft."FK_HOST",
            ft."NU_TYPE",
            ft."NU_STATUS",
            ft."NU_PID",
            ft."NA_HOST_FILE_PATH",
            ft."NA_HOST_FILE_NAME",
            ft."NA_SERVER_FILE_PATH",
            ft."NA_SERVER_FILE_NAME",
            fh."ID_HISTORY",
            fh."NA_SERVER_FILE_PATH" AS "HISTORY_SERVER_FILE_PATH",
            fh."NA_SERVER_FILE_NAME" AS "HISTORY_SERVER_FILE_NAME",
            fh."DT_BACKUP" AS "HISTORY_DT_BACKUP",
            h."NA_HOST_NAME",
            h."IS_OFFLINE",
            h."IS_BUSY"
        FROM "FILE_TASK" ft
        JOIN "HOST" h
          ON h."ID_HOST" = ft."FK_HOST"
        LEFT JOIN "FILE_TASK_HISTORY" fh
          ON fh."FK_HOST" = ft."FK_HOST"
         AND fh."NA_HOST_FILE_PATH" = ft."NA_HOST_FILE_PATH"
         AND fh."NA_HOST_FILE_NAME" = ft."NA_HOST_FILE_NAME"
        WHERE ft."ID_FILE_TASK" IN ({placeholders})
        ''',
        tuple(task_ids),
    )
    return cursor.fetchall() or []


def publish_summary_scope(
    db: Connection[dict[str, object]],
    host_id: int,
    reason: str,
) -> None:
    """Request a later host-summary refresh after a committed maintenance action.

    The outbox insert uses its own commit on purpose. A refresh request is useful
    but must never roll back the queue state already accepted by the operator.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        host_id (int): Operational station identifier.
        reason (str): Reason.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor = db.cursor()
    cursor.execute(
        '''
        INSERT INTO "RFFUSION_SUMMARY"."SUMMARY_OUTBOX"
            ("NA_SCOPE_TYPE", "NA_SCOPE_VALUE", "NA_SOURCE_HANDLER", "NA_REASON")
        VALUES (%s, %s, %s, %s)
        ON CONFLICT ("NA_SCOPE_TYPE", "NA_SCOPE_VALUE") DO UPDATE SET
            "ID_OUTBOX" = EXCLUDED."ID_OUTBOX", "NA_REASON" = EXCLUDED."NA_REASON",
            "NA_SOURCE_HANDLER" = EXCLUDED."NA_SOURCE_HANDLER", "DT_CREATED_AT" = CURRENT_TIMESTAMP
        ''',
        ("host", str(host_id), "webfusion_maintenance", reason),
    )
    db.commit()


def load_history_rows_for_action(
    db: Connection[dict[str, object]],
    history_ids: list[int],
) -> list[dict[str, object]]:
    """Lock history and matching queue rows within the caller's transaction.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        history_ids (list[int]): History ids.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    if not history_ids:
        return []

    placeholders = ", ".join(["%s"] * len(history_ids))
    cursor = db.cursor()
    cursor.execute(
        f'''
        SELECT
            h."ID_HISTORY",
            h."FK_HOST",
            host."NA_HOST_NAME",
            host."IS_OFFLINE",
            h."NA_HOST_FILE_PATH",
            h."NA_HOST_FILE_NAME",
            h."NA_EXTENSION_HOST",
            h."VL_FILE_SIZE_KB_HOST",
            h."DT_FILE_CREATED_HOST",
            h."DT_FILE_MODIFIED_HOST",
            h."NA_SERVER_FILE_PATH",
            h."NA_SERVER_FILE_NAME",
            h."NA_EXTENSION_SERVER",
            h."VL_FILE_SIZE_KB_SERVER",
            h."DT_FILE_CREATED_SERVER",
            h."DT_FILE_MODIFIED_SERVER",
            h."DT_BACKUP",
            h."DT_PROCESSED",
            h."NU_STATUS_DISCOVERY",
            h."NU_STATUS_BACKUP",
            h."NU_STATUS_PROCESSING",
            h."NA_MESSAGE",
            t."ID_FILE_TASK",
            t."NU_STATUS" AS "ACTIVE_TASK_STATUS"
        FROM "FILE_TASK_HISTORY" h
        JOIN "HOST" host
          ON host."ID_HOST" = h."FK_HOST"
        LEFT JOIN LATERAL (
            SELECT existing.* FROM "FILE_TASK" existing
            WHERE existing."FK_HOST" = h."FK_HOST"
              AND existing."NA_HOST_FILE_PATH" = h."NA_HOST_FILE_PATH"
              AND existing."NA_HOST_FILE_NAME" = h."NA_HOST_FILE_NAME"
            FOR UPDATE
        ) t ON true
        WHERE h."ID_HISTORY" IN ({placeholders})
        ORDER BY h."ID_HISTORY"
        FOR UPDATE OF h, host
        ''',
        tuple(history_ids),
    )
    return cursor.fetchall() or []


def _prepare_history_file_task(
    cursor: Cursor[dict[str, object]],
    row: dict[str, object],
    *,
    task_type: int,
    task_status: int,
    message: str,
) -> None:
    """Create a queue task or reset its frozen row from durable history.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor in the active
            transaction.
        row (dict[str, object]): Database row with identifiers, state and metadata
            selected by the domain query.
        task_type (int): Validated queue stage/type code.
        task_status (int): Validated initial queue status.
        message (str): Audit message persisted with the operation.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    server_path = row["NA_SERVER_FILE_PATH"] if task_type == FILE_TASK_PROCESS_TYPE else None
    server_name = row["NA_SERVER_FILE_NAME"] if task_type == FILE_TASK_PROCESS_TYPE else None
    server_extension = row["NA_EXTENSION_SERVER"] if task_type == FILE_TASK_PROCESS_TYPE else None
    server_size = row["VL_FILE_SIZE_KB_SERVER"] if task_type == FILE_TASK_PROCESS_TYPE else None
    server_created = row["DT_FILE_CREATED_SERVER"] if task_type == FILE_TASK_PROCESS_TYPE else None
    server_modified = row["DT_FILE_MODIFIED_SERVER"] if task_type == FILE_TASK_PROCESS_TYPE else None

    if row.get("ID_FILE_TASK") is not None:
        # Preserve the task identity and reject concurrent state changes.
        cursor.execute(
            '''
            UPDATE "FILE_TASK"
            SET "NU_TYPE" = %s, "NU_STATUS" = %s, "NU_PID" = NULL,
                "DT_FILE_TASK" = NOW(),
                "NA_SERVER_FILE_PATH" = %s, "NA_SERVER_FILE_NAME" = %s,
                "NA_EXTENSION_SERVER" = %s, "VL_FILE_SIZE_KB_SERVER" = %s,
                "DT_FILE_CREATED_SERVER" = %s, "DT_FILE_MODIFIED_SERVER" = %s,
                "NA_MESSAGE" = %s, "NA_ERROR_CODE" = NULL, "NA_ERROR_DETAIL" = NULL,
                "NU_ERROR_CLASSIFIER_VERSION" = NULL
            WHERE "ID_FILE_TASK" = %s AND "NU_STATUS" = %s
            ''',
            (
                task_type, task_status, server_path, server_name,
                server_extension, server_size, server_created, server_modified,
                message, int(row["ID_FILE_TASK"]), TASK_FROZEN,
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK", task_id=int(row["ID_FILE_TASK"]))
        return

    cursor.execute(
        '''
        INSERT INTO "FILE_TASK" (
            "FK_HOST",
            "NA_HOST_FILE_PATH",
            "NA_HOST_FILE_NAME",
            "NA_EXTENSION_HOST",
            "VL_FILE_SIZE_KB_HOST",
            "DT_FILE_CREATED_HOST",
            "DT_FILE_MODIFIED_HOST",
            "NU_PID",
            "NU_TYPE",
            "NU_STATUS",
            "DT_FILE_TASK",
            "NA_SERVER_FILE_PATH",
            "NA_SERVER_FILE_NAME",
            "NA_EXTENSION_SERVER",
            "VL_FILE_SIZE_KB_SERVER",
            "DT_FILE_CREATED_SERVER",
            "DT_FILE_MODIFIED_SERVER",
            "NA_MESSAGE"
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s
        )
        ''',
        (
            int(row["FK_HOST"]),
            row["NA_HOST_FILE_PATH"],
            row["NA_HOST_FILE_NAME"],
            row["NA_EXTENSION_HOST"],
            row["VL_FILE_SIZE_KB_HOST"],
            row["DT_FILE_CREATED_HOST"],
            row["DT_FILE_MODIFIED_HOST"],
            int(task_type),
            task_status,
            server_path,
            server_name,
            server_extension,
            server_size,
            server_created,
            server_modified,
            message,
        ),
    )
    if int(cursor.rowcount or 0) != 1:
        raise RuntimeError(
            f"FILE_TASK recreation affected {cursor.rowcount} rows "
            f"(expected 1 for history_id={row['ID_HISTORY']})"
        )


def _assert_single_row(
    cursor: Cursor[dict[str, object]],
    *,
    table: str,
    task_id: int,
) -> None:
    """Raise when a maintenance transition lost its optimistic status check.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor in the active
            transaction.
        table (str): Table.
        task_id (int): Persistent task identifier.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    if int(cursor.rowcount or 0) != 1:
        raise RuntimeError(
            f"{table} update affected {cursor.rowcount} rows "
            f"(expected 1 for task_id={task_id})"
        )


def apply_history_backup_with_cursor(
    cursor: Cursor[dict[str, object]],
    row: dict[str, object],
    *,
    task_status: int,
    message: str,
) -> None:
    """Create one backup task and reset history to the selected status.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor in the active
            transaction.
        row (dict[str, object]): Database row with identifiers, state and metadata
            selected by the domain query.
        task_status (int): Validated initial queue status.
        message (str): Audit message persisted with the operation.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    _prepare_history_file_task(
        cursor,
        row,
        task_type=FILE_TASK_BACKUP_TYPE,
        task_status=task_status,
        message=message,
    )
    cursor.execute(
        '''
        UPDATE "FILE_TASK_HISTORY"
        SET "DT_BACKUP" = NULL,
            "DT_PROCESSED" = NULL,
            "NU_STATUS_BACKUP" = %s,
            "NU_STATUS_PROCESSING" = %s,
            "NA_SERVER_FILE_PATH" = NULL,
            "NA_SERVER_FILE_NAME" = NULL,
            "NA_EXTENSION_SERVER" = NULL,
            "VL_FILE_SIZE_KB_SERVER" = NULL,
            "DT_FILE_CREATED_SERVER" = NULL,
            "DT_FILE_MODIFIED_SERVER" = NULL,
            "NA_MESSAGE" = %s,
            "NA_ERROR_CODE" = NULL,
            "NA_ERROR_DETAIL" = NULL,
            "NU_ERROR_CLASSIFIER_VERSION" = NULL
        WHERE "ID_HISTORY" = %s
        ''',
        (
            task_status,
            TASK_PENDING,
            message,
            int(row["ID_HISTORY"]),
        ),
    )
    if int(cursor.rowcount or 0) != 1:
        raise RuntimeError(
            f"FILE_TASK_HISTORY backup recreation affected {cursor.rowcount} rows "
            f"(expected 1 for history_id={row['ID_HISTORY']})"
        )


def apply_history_process_with_cursor(
    cursor: Cursor[dict[str, object]],
    row: dict[str, object],
    *,
    task_status: int,
    message: str,
    backup_at: datetime | None,
) -> None:
    """Create one processing task and reset history to the selected status.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor in the active
            transaction.
        row (dict[str, object]): Database row with identifiers, state and metadata
            selected by the domain query.
        task_status (int): Validated initial queue status.
        message (str): Audit message persisted with the operation.
        backup_at (datetime | None): Backup at.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    _prepare_history_file_task(
        cursor,
        row,
        task_type=FILE_TASK_PROCESS_TYPE,
        task_status=task_status,
        message=message,
    )
    cursor.execute(
        '''
        UPDATE "FILE_TASK_HISTORY"
        SET "DT_BACKUP" = %s,
            "DT_PROCESSED" = NULL,
            "NU_STATUS_BACKUP" = %s,
            "NU_STATUS_PROCESSING" = %s,
            "NA_MESSAGE" = %s,
            "NA_ERROR_CODE" = NULL,
            "NA_ERROR_DETAIL" = NULL,
            "NU_ERROR_CLASSIFIER_VERSION" = NULL
        WHERE "ID_HISTORY" = %s
        ''',
        (
            backup_at,
            TASK_DONE,
            task_status,
            message,
            int(row["ID_HISTORY"]),
        ),
    )
    if int(cursor.rowcount or 0) != 1:
        raise RuntimeError(
            f"FILE_TASK_HISTORY processing recreation affected {cursor.rowcount} rows "
            f"(expected 1 for history_id={row['ID_HISTORY']})"
        )


def history_phase_field(task_type: int) -> str:
    """Return the FILE_TASK_HISTORY phase column owned by one FILE_TASK type.

    Args:
        task_type (int): Validated queue stage/type code.

    Returns:
        str: `NU_STATUS_BACKUP` for backup tasks, otherwise
            `NU_STATUS_PROCESSING`.
    """
    if int(task_type) == FILE_TASK_BACKUP_TYPE:
        return "NU_STATUS_BACKUP"
    return "NU_STATUS_PROCESSING"


def load_host_tasks(
    db: Connection[dict[str, object]],
    filters: dict[str, object],
) -> list[dict[str, object]]:
    """List HOST_TASK rows matching the queue filters, newest first.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        filters (dict[str, object]): Normalized filters: optional host_id, search,
            task_type, status, dates and required limit.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    sql_parts = ["1 = 1"]
    params: list[object] = []
    _apply_common_filters(
        sql_parts,
        params,
        alias="ht",
        host_id=filters["host_id"],
        search=filters["search"],
        task_type=filters["task_type"],
        status=filters["status"],
        search_columns=('h."NA_HOST_NAME"', 'ht."NA_MESSAGE"'),
    )

    cursor = db.cursor()
    cursor.execute(
        f'''
        SELECT
            ht."ID_HOST_TASK",
            ht."FK_HOST",
            ht."NU_TYPE",
            ht."NU_STATUS",
            ht."NU_PID",
            ht."DT_HOST_TASK",
            ht."NA_MESSAGE",
            h."NA_HOST_NAME",
            h."IS_OFFLINE",
            h."IS_BUSY"
        FROM "HOST_TASK" ht
        JOIN "HOST" h
          ON h."ID_HOST" = ht."FK_HOST"
        WHERE {" AND ".join(sql_parts)}
        ORDER BY ht."DT_HOST_TASK" DESC NULLS LAST, ht."ID_HOST_TASK" DESC
        LIMIT %s
        ''',
        tuple(params + [filters["limit"]]),
    )
    rows = cursor.fetchall() or []
    return rows


def load_file_tasks(
    db: Connection[dict[str, object]],
    filters: dict[str, object],
) -> list[dict[str, object]]:
    """List FILE_TASK rows matching the queue filters, newest first.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        filters (dict[str, object]): Normalized filters: optional host_id, search,
            task_type, status, dates and required limit.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    sql_parts = ["1 = 1"]
    params: list[object] = []
    _apply_common_filters(
        sql_parts,
        params,
        alias="ft",
        host_id=filters["host_id"],
        search=filters["search"],
        task_type=filters["task_type"],
        status=filters["status"],
        search_columns=(
            'h."NA_HOST_NAME"',
            'ft."NA_HOST_FILE_NAME"',
            'ft."NA_SERVER_FILE_NAME"',
            'ft."NA_MESSAGE"',
        ),
    )

    if filters.get("host_file_name"):
        sql_parts.append('ft."NA_HOST_FILE_NAME" = %s')
        params.append(filters["host_file_name"])

    date_field = filters.get("date_field")
    if date_field in FILE_TASK_DATE_FIELDS:
        sql_field = FILE_TASK_DATE_FIELDS[date_field]
        if filters.get("date_from"):
            sql_parts.append(f"{sql_field} >= %s")
            params.append(filters["date_from"])
        if filters.get("date_to"):
            sql_parts.append(f"{sql_field} < %s")
            params.append(filters["date_to"])

    cursor = db.cursor()
    cursor.execute(
        f'''
        SELECT
            ft."ID_FILE_TASK",
            ft."FK_HOST",
            ft."NU_TYPE",
            ft."NU_STATUS",
            ft."NU_PID",
            ft."DT_FILE_TASK",
            ft."NA_MESSAGE",
            ft."NA_HOST_FILE_PATH",
            ft."NA_HOST_FILE_NAME",
            ft."NA_SERVER_FILE_PATH",
            ft."NA_SERVER_FILE_NAME",
            h."NA_HOST_NAME",
            h."IS_OFFLINE",
            h."IS_BUSY",
            fh."ID_HISTORY",
            fh."NA_SERVER_FILE_PATH" AS "HISTORY_SERVER_FILE_PATH",
            fh."NA_SERVER_FILE_NAME" AS "HISTORY_SERVER_FILE_NAME"
        FROM "FILE_TASK" ft
        JOIN "HOST" h
          ON h."ID_HOST" = ft."FK_HOST"
        LEFT JOIN "FILE_TASK_HISTORY" fh
          ON fh."FK_HOST" = ft."FK_HOST"
         AND fh."NA_HOST_FILE_PATH" = ft."NA_HOST_FILE_PATH"
         AND fh."NA_HOST_FILE_NAME" = ft."NA_HOST_FILE_NAME"
        WHERE {" AND ".join(sql_parts)}
        ORDER BY ft."DT_FILE_TASK" DESC NULLS LAST, ft."ID_FILE_TASK" DESC
        LIMIT %s
        ''',
        tuple(params + [filters["limit"]]),
    )
    rows = cursor.fetchall() or []
    return rows


def load_file_history(
    db: Connection[dict[str, object]],
    filters: dict[str, object],
) -> list[dict[str, object]]:
    """List FILE_TASK_HISTORY rows with their active queue task, if any.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        filters (dict[str, object]): Normalized filters: optional host_id, search,
            task_type, status, dates and required limit.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    sql_parts = ["1 = 1"]
    params: list[object] = []

    if filters["host_id"] is not None:
        sql_parts.append('h."FK_HOST" = %s')
        params.append(int(filters["host_id"]))

    if filters["host_file_name"]:
        sql_parts.append('h."NA_HOST_FILE_NAME" = %s')
        params.append(filters["host_file_name"])

    if filters["server_file_name"]:
        sql_parts.append('h."NA_SERVER_FILE_NAME" = %s')
        params.append(filters["server_file_name"])

    if filters["message"]:
        sql_parts.append('h."NA_MESSAGE" ILIKE %s')
        params.append(f"%{filters['message']}%")

    for filter_name, sql_field in HISTORY_PHASE_STATUS_FIELDS.items():
        status = filters.get(filter_name)
        if status is not None:
            sql_parts.append(f"{sql_field} = %s")
            params.append(int(status))

    date_field = filters["date_field"]
    if date_field in HISTORY_DATE_FIELDS:
        sql_field = HISTORY_DATE_FIELDS[date_field]
        if filters["date_from"]:
            sql_parts.append(f"{sql_field} >= %s")
            params.append(filters["date_from"])
        if filters["date_to"]:
            sql_parts.append(f"{sql_field} < %s")
            params.append(filters["date_to"])

    order_by = _history_order_by(db)
    cursor = db.cursor()
    cursor.execute(
        f'''
        SELECT
            h."ID_HISTORY",
            h."FK_HOST",
            host."NA_HOST_NAME",
            host."IS_OFFLINE",
            h."NU_STATUS_DISCOVERY",
            h."NU_STATUS_BACKUP",
            h."NU_STATUS_PROCESSING",
            h."NA_HOST_FILE_NAME",
            h."NA_SERVER_FILE_PATH",
            h."NA_SERVER_FILE_NAME",
            h."DT_DISCOVERED",
            h."DT_BACKUP",
            h."DT_PROCESSED",
            h."NA_MESSAGE",
            t."ID_FILE_TASK",
            t."NU_TYPE" AS "ACTIVE_TASK_TYPE",
            t."NU_STATUS" AS "ACTIVE_TASK_STATUS"
        FROM "FILE_TASK_HISTORY" h
        JOIN "HOST" host
          ON host."ID_HOST" = h."FK_HOST"
        LEFT JOIN "FILE_TASK" t
          ON t."FK_HOST" = h."FK_HOST"
         AND t."NA_HOST_FILE_PATH" = h."NA_HOST_FILE_PATH"
         AND t."NA_HOST_FILE_NAME" = h."NA_HOST_FILE_NAME"
        WHERE {" AND ".join(sql_parts)}
        ORDER BY {order_by}
        LIMIT %s
        ''',
        tuple(params + [filters["limit"]]),
    )
    rows = cursor.fetchall() or []
    return rows


def update_host_task_status(
    db: Connection[dict[str, object]],
    row: dict[str, object],
    status: int,
    message: str,
) -> None:
    """Set one HOST_TASK status, clear its worker PID and commit.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        row (dict[str, object]): Database row with identifiers, state and metadata
            selected by the domain query.
        status (int): Validated queue status.
        message (str): Audit message persisted with the operation.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor = db.cursor()
    cursor.execute(
        '''
        UPDATE "HOST_TASK"
        SET "NU_STATUS" = %s,
            "NU_PID" = NULL,
            "DT_HOST_TASK" = NOW(),
            "NA_MESSAGE" = %s
        WHERE "ID_HOST_TASK" = %s
        ''',
        (
            status,
            message,
            int(row["ID_HOST_TASK"]),
        ),
    )
    if int(cursor.rowcount or 0) != 1:
        raise RuntimeError(
            f"HOST_TASK update affected {cursor.rowcount} rows "
            f"(expected 1 for task_id={row['ID_HOST_TASK']})"
        )
    db.commit()


def reset_file_task_backup(
    db: Connection[dict[str, object]],
    row: dict[str, object],
    task_status: int,
    message: str,
) -> None:
    """Return one file to the backup stage and reset its history atomically.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        row (dict[str, object]): Database row with identifiers, state and metadata
            selected by the domain query.
        task_status (int): Validated initial queue status.
        message (str): Audit message persisted with the operation.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor = db.cursor()

    try:
        # A partial reset would make the queue and history disagree.
        cursor.execute(
            '''
            UPDATE "FILE_TASK"
            SET "NU_TYPE" = %s,
                "NU_STATUS" = %s,
                "NU_PID" = NULL,
                "DT_FILE_TASK" = NOW(),
                "NA_SERVER_FILE_PATH" = NULL,
                "NA_SERVER_FILE_NAME" = NULL,
                "NA_EXTENSION_SERVER" = NULL,
                "VL_FILE_SIZE_KB_SERVER" = NULL,
                "DT_FILE_CREATED_SERVER" = NULL,
                "DT_FILE_MODIFIED_SERVER" = NULL,
                "NA_MESSAGE" = %s,
                "NA_ERROR_CODE" = NULL,
                "NA_ERROR_DETAIL" = NULL,
                "NU_ERROR_CLASSIFIER_VERSION" = NULL
            WHERE "ID_FILE_TASK" = %s
              AND "NU_STATUS" = %s
            ''',
            (
                FILE_TASK_BACKUP_TYPE,
                task_status,
                message,
                int(row["ID_FILE_TASK"]),
                int(row["NU_STATUS"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK", task_id=int(row["ID_FILE_TASK"]))

        cursor.execute(
            '''
            UPDATE "FILE_TASK_HISTORY"
            SET "DT_BACKUP" = NULL,
                "DT_PROCESSED" = NULL,
                "NU_STATUS_BACKUP" = %s,
                "NU_STATUS_PROCESSING" = %s,
                "NA_SERVER_FILE_PATH" = NULL,
                "NA_SERVER_FILE_NAME" = NULL,
                "NA_EXTENSION_SERVER" = NULL,
                "VL_FILE_SIZE_KB_SERVER" = NULL,
                "DT_FILE_CREATED_SERVER" = NULL,
                "DT_FILE_MODIFIED_SERVER" = NULL,
                "IS_PAYLOAD_DELETED" = 0,
                "DT_PAYLOAD_DELETED" = NULL,
                "NA_MESSAGE" = %s,
                "NA_ERROR_CODE" = NULL,
                "NA_ERROR_DETAIL" = NULL,
                "NU_ERROR_CLASSIFIER_VERSION" = NULL
            WHERE "ID_HISTORY" = %s
            ''',
            (
                task_status,
                TASK_PENDING,
                message,
                int(row["ID_HISTORY"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK_HISTORY", task_id=int(row["ID_FILE_TASK"]))
        db.commit()
    except Exception:
        db.rollback()
        raise


def reset_file_task_process(
    db: Connection[dict[str, object]],
    row: dict[str, object],
    task_status: int,
    message: str,
) -> None:
    """Return one backed-up file to the processing stage atomically.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        row (dict[str, object]): Database row with identifiers, state and metadata
            selected by the domain query.
        task_status (int): Validated initial queue status.
        message (str): Audit message persisted with the operation.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor = db.cursor()

    try:
        # A partial reset would make the queue and history disagree.
        cursor.execute(
            '''
            UPDATE "FILE_TASK"
            SET "NU_TYPE" = %s,
                "NU_STATUS" = %s,
                "NU_PID" = NULL,
                "DT_FILE_TASK" = NOW(),
                "NA_SERVER_FILE_PATH" = %s,
                "NA_SERVER_FILE_NAME" = %s,
                "NA_MESSAGE" = %s,
                "NA_ERROR_CODE" = NULL,
                "NA_ERROR_DETAIL" = NULL,
                "NU_ERROR_CLASSIFIER_VERSION" = NULL
            WHERE "ID_FILE_TASK" = %s
              AND "NU_STATUS" = %s
            ''',
            (
                FILE_TASK_PROCESS_TYPE,
                task_status,
                row["HISTORY_SERVER_FILE_PATH"],
                row["HISTORY_SERVER_FILE_NAME"],
                message,
                int(row["ID_FILE_TASK"]),
                int(row["NU_STATUS"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK", task_id=int(row["ID_FILE_TASK"]))

        cursor.execute(
            '''
            UPDATE "FILE_TASK_HISTORY"
            SET "DT_BACKUP" = COALESCE("DT_BACKUP", NOW()),
                "DT_PROCESSED" = NULL,
                "NU_STATUS_BACKUP" = %s,
                "NU_STATUS_PROCESSING" = %s,
                "NA_MESSAGE" = %s,
                "NA_ERROR_CODE" = NULL,
                "NA_ERROR_DETAIL" = NULL,
                "NU_ERROR_CLASSIFIER_VERSION" = NULL
            WHERE "ID_HISTORY" = %s
            ''',
            (
                TASK_DONE,
                task_status,
                message,
                int(row["ID_HISTORY"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK_HISTORY", task_id=int(row["ID_FILE_TASK"]))
        db.commit()
    except Exception:
        db.rollback()
        raise


def update_file_task_status(
    db: Connection[dict[str, object]],
    row: dict[str, object],
    status: int,
    message: str,
    history_phase: str,
) -> None:
    """Set one FILE_TASK status and its history phase atomically.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        row (dict[str, object]): Database row with identifiers, state and metadata
            selected by the domain query.
        status (int): Validated queue status.
        message (str): Audit message persisted with the operation.
        history_phase (str): History phase.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor = db.cursor()

    try:
        # A partial status change would distort the materialized summaries.
        cursor.execute(
            '''
            UPDATE "FILE_TASK"
            SET "NU_STATUS" = %s,
                "NU_PID" = NULL,
                "DT_FILE_TASK" = NOW(),
                "NA_MESSAGE" = %s
            WHERE "ID_FILE_TASK" = %s
              AND "NU_STATUS" = %s
            ''',
            (
                status,
                message,
                int(row["ID_FILE_TASK"]),
                int(row["NU_STATUS"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK", task_id=int(row["ID_FILE_TASK"]))

        cursor.execute(
            f'''
            UPDATE "FILE_TASK_HISTORY"
            SET {history_phase} = %s,
                "NA_MESSAGE" = %s
            WHERE "ID_HISTORY" = %s
            ''',
            (
                status,
                message,
                int(row["ID_HISTORY"]),
            ),
        )
        _assert_single_row(cursor, table="FILE_TASK_HISTORY", task_id=int(row["ID_FILE_TASK"]))
        db.commit()
    except Exception:
        db.rollback()
        raise


def _serialize_filter(filter_value: dict[str, object] | str) -> str:
    """Return a deterministic JSON representation for HOST_TASK.FILTER.

    Args:
        filter_value (dict[str, object] | str): Filter value.

    Returns:
        str: JSON with sorted keys; unparsable text is returned unchanged.
    """
    if isinstance(filter_value, str):
        try:
            filter_value = json.loads(filter_value)
        except json.JSONDecodeError:
            return filter_value

    return json.dumps(filter_value, sort_keys=True, ensure_ascii=False)


def select_candidate_host_tasks(
    db: Connection[dict[str, object]],
    host_id: int,
    task_type: int,
) -> list[dict[str, object]]:
    """Load reusable HOST_TASK candidates for the requested exact type.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        host_id (int): Operational station identifier.
        task_type (int): Validated queue stage/type code.

    Returns:
        list[dict[str, object]]: Rows with ID_HOST_TASK (int), NU_TYPE (int), NU_STATUS
            (int) and FILTER (dict[str, object] | str | None), newest first. Empty
            selection returns [].

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor = db.cursor()
    cursor.execute(
        '''
        SELECT "ID_HOST_TASK", "NU_TYPE", "NU_STATUS", "FILTER"
        FROM "HOST_TASK"
        WHERE "FK_HOST" = %s
          AND "NU_TYPE" = %s
        ORDER BY "DT_HOST_TASK" DESC NULLS LAST, "ID_HOST_TASK" DESC
        ''',
        (host_id, task_type),
    )

    return cursor.fetchall() or []


def refresh_host_task(
    db: Connection[dict[str, object]],
    task_id: int,
    task_type: int,
    filter_dict: dict[str, object] | str,
    message: str,
) -> None:
    """Refresh an existing HOST_TASK row in place.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        task_id (int): Persistent task identifier.
        task_type (int): Validated queue stage/type code.
        filter_dict (dict[str, object] | str): Worker filter object or its existing JSON
            representation.
        message (str): Audit message persisted with the operation.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor = db.cursor()
    cursor.execute(
        '''
        UPDATE "HOST_TASK"
        SET "NU_TYPE" = %s,
            "NU_STATUS" = %s,
            "DT_HOST_TASK" = NOW(),
            "FILTER" = %s,
            "NA_MESSAGE" = %s
        WHERE "ID_HOST_TASK" = %s
        ''',
        (
            task_type,
            TASK_PENDING,
            _serialize_filter(filter_dict),
            f"Refreshed by WebFusion | {message}",
            task_id,
        ),
    )
    db.commit()


def create_host_task(
    db: Connection[dict[str, object]],
    host_id: int,
    task_type: int,
    filter_dict: dict[str, object] | str,
    message: str,
) -> int:
    """Insert a new HOST_TASK row.

    Args:
        db (Connection[dict[str, object]]): Caller-owned dictionary connection; existing
            transaction is preserved.
        host_id (int): Operational station identifier.
        task_type (int): Validated queue stage/type code.
        filter_dict (dict[str, object] | str): Worker filter object or its existing JSON
            representation.
        message (str): Audit message persisted with the operation.

    Returns:
        int: Identifier returned by PostgreSQL after inserting and committing the new
            HOST_TASK.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    cursor = db.cursor()
    cursor.execute(
        '''
        INSERT INTO "HOST_TASK"
        ("FK_HOST", "NU_TYPE", "DT_HOST_TASK", "NU_STATUS", "FILTER", "NA_MESSAGE")
        VALUES (%s, %s, NOW(), %s, %s, %s) RETURNING "ID_HOST_TASK"
        ''',
        (
            host_id,
            task_type,
            TASK_PENDING,
            _serialize_filter(filter_dict),
            message,
        ),
    )
    task_id = cursor.fetchone()["ID_HOST_TASK"]
    db.commit()
    return task_id


def read_connectivity_test_row(
    cursor: Cursor[dict[str, object]],
    host_id: int,
    task_id: int,
) -> dict[str, object] | None:
    """Read an interactive task scoped to its station and task type.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor.
        host_id (int): Operational station identifier.
        task_id (int): Interactive task identifier.

    Returns:
        dict[str, object] | None: Row with ID_HOST_TASK/FK_HOST/NU_STATUS (int),
            NA_MESSAGE (str | None), DT_HOST_TASK (datetime) and NA_HOST_NAME (str);
            None when the station/type/task combination is absent.

    Raises:
        Exception: Database failure; propagated to the caller.
    """
    cursor.execute(
        '''
        SELECT
            HT."ID_HOST_TASK",
            HT."FK_HOST",
            HT."NU_STATUS",
            HT."NA_MESSAGE",
            HT."DT_HOST_TASK",
            H."NA_HOST_NAME"
        FROM "HOST_TASK" HT
        JOIN "HOST" H ON H."ID_HOST" = HT."FK_HOST"
        WHERE HT."ID_HOST_TASK" = %s
          AND HT."FK_HOST" = %s
          AND HT."NU_TYPE" = %s
        LIMIT 1
        ''',
        (task_id, host_id, HOST_TASK_INTERACTIVE_CHECK_TYPE),
    )
    return cursor.fetchone()


def host_exists(cursor: Cursor[dict[str, object]], host_id: int) -> bool:
    """Check whether a station is present in the operational catalog.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor.
        host_id (int): Operational station identifier.

    Returns:
        bool: True when the operational station exists, otherwise False.

    Raises:
        Exception: Database failure; propagated to the caller.
    """
    cursor.execute('SELECT "ID_HOST" FROM "HOST" WHERE "ID_HOST" = %s LIMIT 1', (host_id,))
    return bool(cursor.fetchone())


def list_station_prefixes(db: Connection[dict[str, object]]) -> list[dict[str, object]]:
    """List station families and their catalog counts.

    Args:
        db (Connection[dict[str, object]]): Caller-owned operational connection.

    Returns:
        list[dict[str, object]]: Rows with PREFIX (str | None) and HOSTS (int), ordered
            by prefix. Empty selection returns [].

    Raises:
        Exception: Database failure; propagated to the caller.
    """
    cursor = db.cursor()
    cursor.execute('''
        SELECT
            CASE
                WHEN UPPER("NA_HOST_NAME") ILIKE 'UMS%' THEN 'UMS300'
                WHEN UPPER("NA_HOST_NAME") ILIKE 'ERMX%' THEN 'ERMX'
                ELSE REGEXP_SUBSTR(UPPER("NA_HOST_NAME"), '^[A-Z]+')
            END AS "PREFIX",
            COUNT(*) AS "HOSTS"
        FROM "HOST"
        GROUP BY "PREFIX"
        ORDER BY "PREFIX"
    ''')
    return cursor.fetchall()


def list_builder_hosts(
    db: Connection[dict[str, object]],
    online_only: bool,
) -> list[dict[str, object]]:
    """List stations for the individual task selector.

    Args:
        db (Connection[dict[str, object]]): Caller-owned operational connection.
        online_only (bool): Restrict the catalog to online stations.

    Returns:
        list[dict[str, object]]: Rows with ID_HOST (int), NA_HOST_NAME (str) and
            DT_LAST_DISCOVERY (datetime | None), ordered by name. Empty selection
            returns [].

    Raises:
        Exception: Database failure; propagated to the caller.
    """
    cursor = db.cursor()
    query = '''
        SELECT "ID_HOST", "NA_HOST_NAME", "DT_LAST_DISCOVERY"
        FROM "HOST"
    '''

    if online_only:
        query += ' WHERE "IS_OFFLINE" = 0'

    query += ' ORDER BY "NA_HOST_NAME"'

    cursor.execute(query)
    return cursor.fetchall()


def list_collective_hosts(
    db: Connection[dict[str, object]],
    online_only: bool,
    host_filter: str,
) -> list[dict[str, object]]:
    """List candidate stations before applying an explicit operator selection.

    Args:
        db (Connection[dict[str, object]]): Caller-owned operational connection.
        online_only (bool): Restrict the catalog to online stations.
        host_filter (str): Selected station family prefix, or ALL.

    Returns:
        list[dict[str, object]]: Rows with ID_HOST (int) and NA_HOST_NAME (str), ordered
            by name after family and online filters. Empty selection returns [].

    Raises:
        Exception: Database failure; propagated to the caller.
    """
    cursor = db.cursor()
    query = '''
        SELECT "ID_HOST", "NA_HOST_NAME"
        FROM "HOST"
        WHERE 1 = 1
    '''
    params = []

    if online_only:
        query += ' AND "IS_OFFLINE" = 0'

    # Apply the selected station family before creating a batch.
    if host_filter != "ALL":
        if str(host_filter).upper() == "UMS300":
            query += ' AND UPPER("NA_HOST_NAME") ILIKE %s'
            params.append("UMS%")
        else:
            query += ' AND "NA_HOST_NAME" ILIKE %s'
            params.append(f"{host_filter}%")

    query += ' ORDER BY "NA_HOST_NAME"'

    cursor.execute(query, tuple(params))

    return cursor.fetchall()
