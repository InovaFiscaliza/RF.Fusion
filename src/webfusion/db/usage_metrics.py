"""Persist monthly usage counters and transactional NGINX checkpoints.

Cursor operations never commit; the caller saves counters and offset together.
Standalone counter reads and increments close their own connections.
"""

from __future__ import annotations

from datetime import date, datetime

from psycopg import Cursor

from modules.server.config import (
    NGINX_CHECKPOINT_HASH_PREFIX,
    NGINX_DOWNLOAD_LOG_SOURCE as _NGINX_DOWNLOAD_LOG_SOURCE,
    NGINX_DOWNLOAD_METRIC as _NGINX_DOWNLOAD_METRIC,
)
from .config import (
    USAGE_COUNTER_TABLE as _COUNTER_TABLE,
    USAGE_CHECKPOINT_TABLE as _CHECKPOINT_TABLE,
)
from .connection import get_connection_summary


def load_month_rows_from_db() -> list[dict[str, object]]:
    """Read all persisted monthly metric rows from ``RFFUSION_SUMMARY``.

    Args:
        None.

    Returns:
        list[dict[str, object]]: Rows with metric_name (str), reference_month (date),
            and value (int). Rows are ordered by month descending and name. Empty
            selection returns [].

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    conn = get_connection_summary()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f'''
            SELECT
                "NA_METRIC_NAME" AS metric_name,
                "DT_REFERENCE_MONTH" AS reference_month,
                "NU_VALUE" AS value
            FROM "{_COUNTER_TABLE}"
            ORDER BY "DT_REFERENCE_MONTH" DESC NULLS LAST, "NA_METRIC_NAME"
            '''
        )
        return cursor.fetchall() or []
    finally:
        conn.close()


def increment_counter_by_month_in_db(
    *,
    counter_name: str,
    reference_month: date,
    amount: int,
) -> int:
    """Increase one durable metric for one specific month.

    Args:
        counter_name (str): Counter name.
        reference_month (date): Reference month; normalized to its first day.
        amount (int): Amount.

    Returns:
        int: All-time total for counter_name after the monthly increment has committed.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    normalized_month = reference_month.replace(day=1)
    conn = get_connection_summary()
    try:
        cursor = conn.cursor()

        cursor.execute(
            f'''
            INSERT INTO "{_COUNTER_TABLE}" ("NA_METRIC_NAME", "DT_REFERENCE_MONTH", "NU_VALUE")
            VALUES (%s, %s, %s)
            ON CONFLICT ("NA_METRIC_NAME", "DT_REFERENCE_MONTH") DO UPDATE SET "NU_VALUE" = "{_COUNTER_TABLE}"."NU_VALUE" + EXCLUDED."NU_VALUE"
            ''',
            (counter_name, normalized_month, int(amount)),
        )
        cursor.execute(
            f'''
            SELECT COALESCE(SUM("NU_VALUE"), 0) AS total_value
            FROM "{_COUNTER_TABLE}"
            WHERE "NA_METRIC_NAME" = %s
            ''',
            (counter_name,),
        )
        row = cursor.fetchone() or {}
        conn.commit()
        return int(row.get("total_value") or 0)
    finally:
        conn.close()


def save_checkpoint_with_cursor(
    cursor: Cursor[dict[str, object]],
    *,
    source_name: str,
    log_path: str,
    file_signature: str,
    last_offset: int,
    last_size: int,
    last_mtime_ns: int,
) -> None:
    """Save one checkpoint inside an existing transaction.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor in the active
            transaction.
        source_name (str): Source name.
        log_path (str): Log path.
        file_signature (str): File signature.
        last_offset (int): Last offset.
        last_size (int): Last size.
        last_mtime_ns (int): Last mtime ns.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    cursor.execute(
        f'''
        INSERT INTO "{_CHECKPOINT_TABLE}" (
            "NA_SOURCE_NAME",
            "NA_LOG_PATH",
            "NA_FILE_SIGNATURE",
            "NU_LAST_OFFSET",
            "NU_LAST_SIZE",
            "NU_LAST_MTIME_NS"
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT ("NA_SOURCE_NAME") DO UPDATE SET
            "NA_LOG_PATH" = EXCLUDED."NA_LOG_PATH",
            "NA_FILE_SIGNATURE" = EXCLUDED."NA_FILE_SIGNATURE",
            "NU_LAST_OFFSET" = EXCLUDED."NU_LAST_OFFSET",
            "NU_LAST_SIZE" = EXCLUDED."NU_LAST_SIZE",
            "NU_LAST_MTIME_NS" = EXCLUDED."NU_LAST_MTIME_NS"
        ''',
        (
            source_name,
            log_path,
            file_signature,
            int(last_offset),
            int(last_size),
            int(last_mtime_ns),
        ),
    )


def lock_checkpoint_for_update(
    cursor: Cursor[dict[str, object]],
    *,
    log_path: str,
) -> dict[str, object]:
    """Lock the NGINX checkpoint before reading its unseen log range.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor in the active
            transaction.
        log_path (str): Log path.

    Returns:
        dict[str, object]: Locked row with source_name/log_path (str), file_signature
            (str | None), last_offset/last_size/last_mtime_ns (int | None). Legacy
            signatures add legacy_month_counts (dict[date, int]); no counters or offsets
            are committed here.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    cursor.execute(
        f'''
        INSERT INTO "{_CHECKPOINT_TABLE}" ("NA_SOURCE_NAME", "NA_LOG_PATH")
        VALUES (%s, %s)
        ON CONFLICT ("NA_SOURCE_NAME") DO NOTHING
        ''',
        (_NGINX_DOWNLOAD_LOG_SOURCE, log_path),
    )
    cursor.execute(
        f'''
        SELECT
            "NA_SOURCE_NAME" AS source_name,
            "NA_LOG_PATH" AS log_path,
            "NA_FILE_SIGNATURE" AS file_signature,
            "NU_LAST_OFFSET" AS last_offset,
            "NU_LAST_SIZE" AS last_size,
            "NU_LAST_MTIME_NS" AS last_mtime_ns
        FROM "{_CHECKPOINT_TABLE}"
        WHERE "NA_SOURCE_NAME" = %s
        FOR UPDATE
        ''',
        (_NGINX_DOWNLOAD_LOG_SOURCE,),
    )
    checkpoint = cursor.fetchone() or {}
    signature = str(checkpoint.get("file_signature") or "")
    if signature and not signature.startswith(NGINX_CHECKPOINT_HASH_PREFIX):
        cursor.execute(
            f'''SELECT "DT_REFERENCE_MONTH", "NU_VALUE" FROM "{_COUNTER_TABLE}" WHERE "NA_METRIC_NAME" = %s''',
            (_NGINX_DOWNLOAD_METRIC,),
        )
        checkpoint["legacy_month_counts"] = {
            normalize_reference_month(row["DT_REFERENCE_MONTH"]): int(row["NU_VALUE"])
            for row in cursor.fetchall()
        }
    return checkpoint


def increment_nginx_months_with_cursor(
    cursor: Cursor[dict[str, object]],
    counts_by_month: dict[date, int],
) -> None:
    """Add parsed download counts inside the checkpoint transaction.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor in the active
            transaction.
        counts_by_month (dict[date, int]): Download counts keyed by first day of each
            reference month.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    for reference_month, value in counts_by_month.items():
        cursor.execute(
            f'''
            INSERT INTO "{_COUNTER_TABLE}" ("NA_METRIC_NAME", "DT_REFERENCE_MONTH", "NU_VALUE")
            VALUES (%s, %s, %s)
            ON CONFLICT ("NA_METRIC_NAME", "DT_REFERENCE_MONTH") DO UPDATE SET "NU_VALUE" = "{_COUNTER_TABLE}"."NU_VALUE" + EXCLUDED."NU_VALUE"
            ''',
            (
                _NGINX_DOWNLOAD_METRIC,
                reference_month.replace(day=1),
                int(value),
            ),
        )


def replace_nginx_months_with_cursor(
    cursor: Cursor[dict[str, object]],
    counts_by_month: dict[date, int],
) -> None:
    """Replace durable download counts with one complete log reconciliation.

    Args:
        cursor (Cursor[dict[str, object]]): Caller-owned dictionary cursor in the active
            transaction.
        counts_by_month (dict[date, int]): Download counts keyed by first day of each
            reference month.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    cursor.execute(
        f'''
        DELETE FROM "{_COUNTER_TABLE}"
        WHERE "NA_METRIC_NAME" = %s
        ''',
        (_NGINX_DOWNLOAD_METRIC,),
    )

    for reference_month, value in counts_by_month.items():
        cursor.execute(
            f'''
            INSERT INTO "{_COUNTER_TABLE}" ("NA_METRIC_NAME", "DT_REFERENCE_MONTH", "NU_VALUE")
            VALUES (%s, %s, %s)
            ''',
            (
                _NGINX_DOWNLOAD_METRIC,
                reference_month.replace(day=1),
                int(value),
            ),
        )


def normalize_reference_month(value: date | datetime | str) -> date:
    """Normalize one database or memory month reference into a date object.

    Args:
        value (date | datetime | str): Value.

    Returns:
        date: First day of the month supplied as date, datetime or ISO text.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    if isinstance(value, date):
        return value.replace(day=1)

    parsed = datetime.strptime(str(value or "").strip()[:10], "%Y-%m-%d")
    return parsed.date().replace(day=1)


def reset_persisted_usage_metrics() -> None:
    """Delete persisted counters and checkpoints for explicit test resets.

    Args:
        None.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    conn = get_connection_summary()
    try:
        cursor = conn.cursor()
        cursor.execute(f'''DELETE FROM "{_COUNTER_TABLE}"''')
        cursor.execute(f'''DELETE FROM "{_CHECKPOINT_TABLE}"''')
        conn.commit()
    finally:
        conn.close()
