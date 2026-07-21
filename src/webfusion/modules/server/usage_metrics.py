"""Persistent WebFusion usage counters grouped by reference month.

The server dashboard needs lightweight adoption signals that survive container
restarts without becoming a full audit trail. Persisting one monthly counter
row per metric keeps the write path simple while still allowing total, yearly,
and monthly aggregations for the `/server` page.
"""

from __future__ import annotations

import os
import re
from collections import defaultdict
from datetime import date, datetime
from threading import RLock


_COUNTER_LOCK = RLock()
_COUNTER_TABLE = "WEBFUSION_USAGE_METRIC_MONTHLY"
_CHECKPOINT_TABLE = "WEBFUSION_USAGE_LOG_CHECKPOINT"
_METRIC_NAMES = (
    "page_view_count",
    "spectrum_query_count",
    "download_action_count",
    "nginx_download_count",
)
_MONTHLY_COUNTERS: dict[tuple[str, str], int] = {}
_CHECKPOINTS_MEMORY: dict[str, dict[str, object]] = {}
_NGINX_DOWNLOAD_METRIC = "nginx_download_count"
_NGINX_DOWNLOAD_LOG_SOURCE = "nginx_access_downloads"
_NGINX_DOWNLOAD_LOG_PATH = "/var/log/nginx/access.log"
_NGINX_DOWNLOAD_PATH_PREFIXES = (
    "/downloads/",
    "/_repo_download/",
)
_NGINX_DOWNLOAD_STATUS_CODES = {200, 206}
_ACCESS_LOG_PATTERN = re.compile(
    r'^\S+ \S+ \S+ \[(?P<timestamp>[^\]]+)\] '
    r'"(?P<method>[A-Z]+) (?P<path>\S+) [^"]+" '
    r'(?P<status>\d{3}) (?P<body_bytes>\S+)'
)


def _use_memory_backend() -> bool:
    """Return whether this process should skip database persistence."""

    backend = str(os.getenv("WEBFUSION_USAGE_METRICS_BACKEND", "database")).strip().lower()
    return backend == "memory"


def _ensure_counter_name(counter_name: str) -> None:
    """Reject unexpected metric names early."""

    if counter_name not in _METRIC_NAMES:
        raise ValueError(f"Unknown usage metric: {counter_name}")


def _get_summary_connection():
    """Open one summary-schema connection only when persistence is needed."""

    from db import get_connection_summary

    return get_connection_summary()


def _get_nginx_download_log_path() -> str:
    """Return the filesystem path of the NGINX access log to ingest."""

    return str(
        os.getenv(
            "WEBFUSION_NGINX_DOWNLOAD_LOG_PATH",
            _NGINX_DOWNLOAD_LOG_PATH,
        )
        or ""
    ).strip()


def _get_current_month_start() -> date:
    """Return the first day of the current month."""

    today = date.today()
    return today.replace(day=1)


def _get_current_year_label() -> str:
    """Return the current year label used by the dashboard cards."""

    return str(date.today().year)


def _get_current_month_label() -> str:
    """Return the current `YYYY-MM` label used by the dashboard cards."""

    return _get_current_month_start().strftime("%Y-%m")


def _build_zero_totals() -> dict[str, int]:
    """Return one fresh metric dictionary initialized with zeros."""

    return {metric_name: 0 for metric_name in _METRIC_NAMES}


def _format_reference_month(reference_month: date) -> str:
    """Return the normalized `YYYY-MM-01` storage key for one month."""

    return reference_month.replace(day=1).isoformat()


def _normalize_reference_month(value) -> date:
    """Normalize one database or memory month reference into a date object."""

    if isinstance(value, date):
        return value.replace(day=1)

    parsed = datetime.strptime(str(value or "").strip()[:10], "%Y-%m-%d")
    return parsed.date().replace(day=1)


def _build_metric_row(period_key: str, period_field: str, values: dict[str, int]) -> dict[str, int | str]:
    """Shape one period row returned to the `/server` dashboard."""

    row = {
        period_field: period_key,
    }
    row.update({metric_name: int(values.get(metric_name) or 0) for metric_name in _METRIC_NAMES})
    return row


def _build_snapshot_from_month_rows(month_rows: list[dict[str, object]]) -> dict[str, object]:
    """Aggregate raw monthly metric rows into dashboard-friendly payloads."""

    current_year_label = _get_current_year_label()
    current_month_label = _get_current_month_label()
    totals = _build_zero_totals()
    current_year_totals = _build_zero_totals()
    current_month_totals = _build_zero_totals()
    annual_map: dict[str, dict[str, int]] = {}
    monthly_map: dict[str, dict[str, int]] = {}

    for row in month_rows:
        metric_name = str(row.get("metric_name") or "").strip()
        if metric_name not in totals:
            continue

        reference_month = _normalize_reference_month(row.get("reference_month"))
        month_label = reference_month.strftime("%Y-%m")
        year_label = reference_month.strftime("%Y")
        value = int(row.get("value") or 0)

        totals[metric_name] += value

        if year_label == current_year_label:
            current_year_totals[metric_name] += value

        if month_label == current_month_label:
            current_month_totals[metric_name] += value

        annual_map.setdefault(year_label, _build_zero_totals())
        annual_map[year_label][metric_name] += value

        monthly_map.setdefault(month_label, _build_zero_totals())
        monthly_map[month_label][metric_name] += value

    annual_breakdown = [
        _build_metric_row(year_label, "reference_year", annual_map[year_label])
        for year_label in sorted(annual_map.keys(), reverse=True)
    ]
    monthly_breakdown = [
        _build_metric_row(month_label, "reference_month", monthly_map[month_label])
        for month_label in sorted(monthly_map.keys(), reverse=True)
    ]

    return {
        "current_year_label": current_year_label,
        "current_month_label": current_month_label,
        "totals": totals,
        "current_year_totals": current_year_totals,
        "current_month_totals": current_month_totals,
        "annual_breakdown": annual_breakdown,
        "monthly_breakdown": monthly_breakdown,
    }


def _create_metrics_table(cursor) -> None:
    """Create the persistence table lazily for upgraded environments."""

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{_COUNTER_TABLE}` (
          `NA_METRIC_NAME` varchar(64) NOT NULL,
          `DT_REFERENCE_MONTH` date NOT NULL,
          `NU_VALUE` bigint(20) NOT NULL DEFAULT 0,
          `DT_UPDATED_AT` datetime NOT NULL DEFAULT current_timestamp()
            ON UPDATE current_timestamp(),
          PRIMARY KEY (`NA_METRIC_NAME`, `DT_REFERENCE_MONTH`),
          KEY `IX_WEBFUSION_USAGE_MONTH` (`DT_REFERENCE_MONTH`, `NA_METRIC_NAME`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
        """
    )


def _create_checkpoint_table(cursor) -> None:
    """Create the NGINX ingestion checkpoint table lazily."""

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS `{_CHECKPOINT_TABLE}` (
          `NA_SOURCE_NAME` varchar(64) NOT NULL,
          `NA_LOG_PATH` varchar(255) NOT NULL,
          `NA_FILE_SIGNATURE` varchar(128) DEFAULT NULL,
          `NU_LAST_OFFSET` bigint(20) NOT NULL DEFAULT 0,
          `NU_LAST_SIZE` bigint(20) NOT NULL DEFAULT 0,
          `NU_LAST_MTIME_NS` bigint(20) NOT NULL DEFAULT 0,
          `DT_UPDATED_AT` datetime NOT NULL DEFAULT current_timestamp()
            ON UPDATE current_timestamp(),
          PRIMARY KEY (`NA_SOURCE_NAME`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
        """
    )


def _load_month_rows_from_db() -> list[dict[str, object]]:
    """Read all persisted monthly metric rows from ``RFFUSION_SUMMARY``."""

    conn = _get_summary_connection()
    try:
        cursor = conn.cursor()
        _create_metrics_table(cursor)
        _create_checkpoint_table(cursor)
        conn.commit()
        cursor.execute(
            f"""
            SELECT
                `NA_METRIC_NAME` AS metric_name,
                `DT_REFERENCE_MONTH` AS reference_month,
                `NU_VALUE` AS value
            FROM `{_COUNTER_TABLE}`
            ORDER BY `DT_REFERENCE_MONTH` DESC, `NA_METRIC_NAME`
            """
        )
        return cursor.fetchall() or []
    finally:
        conn.close()


def _increment_counter_in_db(counter_name: str) -> int:
    """Increase one durable metric and return its all-time total."""

    reference_month = _get_current_month_start()
    return _increment_counter_by_month_in_db(
        counter_name=counter_name,
        reference_month=reference_month,
        amount=1,
    )


def _increment_counter_by_month_in_db(
    *,
    counter_name: str,
    reference_month: date,
    amount: int,
) -> int:
    """Increase one durable metric for one specific month."""

    normalized_month = reference_month.replace(day=1)
    conn = _get_summary_connection()
    try:
        cursor = conn.cursor()
        _create_metrics_table(cursor)
        cursor.execute(
            f"""
            INSERT INTO `{_COUNTER_TABLE}` (`NA_METRIC_NAME`, `DT_REFERENCE_MONTH`, `NU_VALUE`)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE `NU_VALUE` = `NU_VALUE` + VALUES(`NU_VALUE`)
            """,
            (counter_name, normalized_month, int(amount)),
        )
        cursor.execute(
            f"""
            SELECT COALESCE(SUM(`NU_VALUE`), 0) AS total_value
            FROM `{_COUNTER_TABLE}`
            WHERE `NA_METRIC_NAME` = %s
            """,
            (counter_name,),
        )
        row = cursor.fetchone() or {}
        conn.commit()
        return int(row.get("total_value") or 0)
    finally:
        conn.close()


def _load_month_rows_from_memory() -> list[dict[str, object]]:
    """Return the fallback in-memory monthly rows."""

    with _COUNTER_LOCK:
        return [
            {
                "metric_name": metric_name,
                "reference_month": reference_month,
                "value": value,
            }
            for (metric_name, reference_month), value in sorted(
                _MONTHLY_COUNTERS.items(),
                key=lambda item: (item[0][1], item[0][0]),
                reverse=True,
            )
        ]


def _increment_counter_in_memory(counter_name: str) -> int:
    """Increase one fallback metric and return its all-time total."""

    return _increment_counter_by_month_in_memory(
        counter_name=counter_name,
        reference_month=_get_current_month_start(),
        amount=1,
    )


def _increment_counter_by_month_in_memory(
    *,
    counter_name: str,
    reference_month: date,
    amount: int,
) -> int:
    """Increase one fallback metric for one specific month."""

    month_key = _format_reference_month(reference_month)

    with _COUNTER_LOCK:
        key = (counter_name, month_key)
        _MONTHLY_COUNTERS[key] = _MONTHLY_COUNTERS.get(key, 0) + int(amount)
        return sum(
            value
            for (metric_name, _), value in _MONTHLY_COUNTERS.items()
            if metric_name == counter_name
        )


def _load_checkpoint_from_db(source_name: str) -> dict[str, object] | None:
    """Read one persisted log-ingestion checkpoint."""

    conn = _get_summary_connection()
    try:
        cursor = conn.cursor()
        _create_checkpoint_table(cursor)
        cursor.execute(
            f"""
            SELECT
                `NA_SOURCE_NAME` AS source_name,
                `NA_LOG_PATH` AS log_path,
                `NA_FILE_SIGNATURE` AS file_signature,
                `NU_LAST_OFFSET` AS last_offset,
                `NU_LAST_SIZE` AS last_size,
                `NU_LAST_MTIME_NS` AS last_mtime_ns
            FROM `{_CHECKPOINT_TABLE}`
            WHERE `NA_SOURCE_NAME` = %s
            """,
            (source_name,),
        )
        return cursor.fetchone() or None
    finally:
        conn.close()


def _save_checkpoint_to_db(
    *,
    source_name: str,
    log_path: str,
    file_signature: str,
    last_offset: int,
    last_size: int,
    last_mtime_ns: int,
) -> None:
    """Persist one log-ingestion checkpoint."""

    conn = _get_summary_connection()
    try:
        cursor = conn.cursor()
        _create_checkpoint_table(cursor)
        cursor.execute(
            f"""
            INSERT INTO `{_CHECKPOINT_TABLE}` (
                `NA_SOURCE_NAME`,
                `NA_LOG_PATH`,
                `NA_FILE_SIGNATURE`,
                `NU_LAST_OFFSET`,
                `NU_LAST_SIZE`,
                `NU_LAST_MTIME_NS`
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                `NA_LOG_PATH` = VALUES(`NA_LOG_PATH`),
                `NA_FILE_SIGNATURE` = VALUES(`NA_FILE_SIGNATURE`),
                `NU_LAST_OFFSET` = VALUES(`NU_LAST_OFFSET`),
                `NU_LAST_SIZE` = VALUES(`NU_LAST_SIZE`),
                `NU_LAST_MTIME_NS` = VALUES(`NU_LAST_MTIME_NS`)
            """,
            (
                source_name,
                log_path,
                file_signature,
                int(last_offset),
                int(last_size),
                int(last_mtime_ns),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _load_checkpoint_from_memory(source_name: str) -> dict[str, object] | None:
    """Read one in-memory log-ingestion checkpoint."""

    with _COUNTER_LOCK:
        checkpoint = _CHECKPOINTS_MEMORY.get(source_name)
        return dict(checkpoint) if checkpoint is not None else None


def _save_checkpoint_to_memory(
    *,
    source_name: str,
    log_path: str,
    file_signature: str,
    last_offset: int,
    last_size: int,
    last_mtime_ns: int,
) -> None:
    """Persist one in-memory log-ingestion checkpoint."""

    with _COUNTER_LOCK:
        _CHECKPOINTS_MEMORY[source_name] = {
            "source_name": source_name,
            "log_path": log_path,
            "file_signature": file_signature,
            "last_offset": int(last_offset),
            "last_size": int(last_size),
            "last_mtime_ns": int(last_mtime_ns),
        }


def _load_checkpoint(source_name: str) -> dict[str, object] | None:
    """Read one checkpoint from the configured backend."""

    if _use_memory_backend():
        return _load_checkpoint_from_memory(source_name)

    try:
        return _load_checkpoint_from_db(source_name)
    except Exception:
        return _load_checkpoint_from_memory(source_name)


def _save_checkpoint(
    *,
    source_name: str,
    log_path: str,
    file_signature: str,
    last_offset: int,
    last_size: int,
    last_mtime_ns: int,
) -> None:
    """Persist one checkpoint into the configured backend."""

    if _use_memory_backend():
        _save_checkpoint_to_memory(
            source_name=source_name,
            log_path=log_path,
            file_signature=file_signature,
            last_offset=last_offset,
            last_size=last_size,
            last_mtime_ns=last_mtime_ns,
        )
        return

    try:
        _save_checkpoint_to_db(
            source_name=source_name,
            log_path=log_path,
            file_signature=file_signature,
            last_offset=last_offset,
            last_size=last_size,
            last_mtime_ns=last_mtime_ns,
        )
    except Exception:
        _save_checkpoint_to_memory(
            source_name=source_name,
            log_path=log_path,
            file_signature=file_signature,
            last_offset=last_offset,
            last_size=last_size,
            last_mtime_ns=last_mtime_ns,
        )


def _build_log_file_signature(stat_result: os.stat_result) -> str:
    """Return one stable identity string for the current log file."""

    return f"{stat_result.st_dev}:{stat_result.st_ino}"


def _parse_access_log_line(line: str) -> tuple[date, str, int] | None:
    """Parse one NGINX access-log line into month, path, and status."""

    match = _ACCESS_LOG_PATTERN.match(line.strip())
    if match is None:
        return None

    try:
        event_at = datetime.strptime(
            match.group("timestamp"),
            "%d/%b/%Y:%H:%M:%S %z",
        )
    except ValueError:
        return None

    request_path = match.group("path").split("?", 1)[0]
    return (
        event_at.date().replace(day=1),
        request_path,
        int(match.group("status")),
    )


def _is_countable_nginx_download(
    *,
    request_path: str,
    request_status: int,
) -> bool:
    """Return whether one access-log entry represents a successful download."""

    if request_status not in _NGINX_DOWNLOAD_STATUS_CODES:
        return False

    return any(
        request_path.startswith(prefix)
        for prefix in _NGINX_DOWNLOAD_PATH_PREFIXES
    )


def _read_nginx_download_counts(
    log_path: str,
    checkpoint: dict[str, object] | None,
) -> tuple[dict[date, int], dict[str, object] | None]:
    """Read only new download events from the configured access log."""

    try:
        stat_result = os.stat(log_path)
    except OSError:
        return {}, None

    file_signature = _build_log_file_signature(stat_result)
    previous_signature = str((checkpoint or {}).get("file_signature") or "").strip()
    previous_path = str((checkpoint or {}).get("log_path") or "").strip()
    previous_offset = int((checkpoint or {}).get("last_offset") or 0)
    previous_size = int((checkpoint or {}).get("last_size") or 0)
    previous_mtime_ns = int((checkpoint or {}).get("last_mtime_ns") or 0)
    start_offset = 0

    if previous_signature == file_signature and previous_path == log_path:
        if (
            stat_result.st_size >= previous_offset
            and stat_result.st_size >= previous_size
            and (
                stat_result.st_size > previous_size
                or stat_result.st_mtime_ns == previous_mtime_ns
            )
        ):
            start_offset = previous_offset

    counts_by_month: dict[date, int] = defaultdict(int)

    with open(log_path, "r", encoding="utf-8", errors="ignore") as handle:
        if start_offset > 0:
            handle.seek(start_offset)

        for line in handle:
            parsed_line = _parse_access_log_line(line)
            if parsed_line is None:
                continue

            reference_month, request_path, request_status = parsed_line
            if not _is_countable_nginx_download(
                request_path=request_path,
                request_status=request_status,
            ):
                continue

            counts_by_month[reference_month] += 1

        last_offset = handle.tell()

    return dict(counts_by_month), {
        "source_name": _NGINX_DOWNLOAD_LOG_SOURCE,
        "log_path": log_path,
        "file_signature": file_signature,
        "last_offset": int(last_offset),
        "last_size": int(last_offset),
        "last_mtime_ns": int(stat_result.st_mtime_ns),
    }


def sync_nginx_download_metrics() -> None:
    """Ingest successful NGINX download deliveries into monthly usage metrics."""

    log_path = _get_nginx_download_log_path()
    if not log_path:
        return

    with _COUNTER_LOCK:
        checkpoint = _load_checkpoint(_NGINX_DOWNLOAD_LOG_SOURCE)
        counts_by_month, next_checkpoint = _read_nginx_download_counts(
            log_path,
            checkpoint,
        )

        if next_checkpoint is None:
            return

        for reference_month, value in counts_by_month.items():
            if _use_memory_backend():
                _increment_counter_by_month_in_memory(
                    counter_name=_NGINX_DOWNLOAD_METRIC,
                    reference_month=reference_month,
                    amount=value,
                )
                continue

            _increment_counter_by_month_in_db(
                counter_name=_NGINX_DOWNLOAD_METRIC,
                reference_month=reference_month,
                amount=value,
            )

        _save_checkpoint(**next_checkpoint)


def _increment_counter(counter_name: str) -> int:
    """Increase one metric and return its all-time total."""

    _ensure_counter_name(counter_name)

    if _use_memory_backend():
        return _increment_counter_in_memory(counter_name)

    try:
        return _increment_counter_in_db(counter_name)
    except Exception:
        return _increment_counter_in_memory(counter_name)


def record_page_view() -> int:
    """Count one HTML page render served by WebFusion."""

    return _increment_counter("page_view_count")


def record_spectrum_query() -> int:
    """Count one spectrum search initiated from the query page."""

    return _increment_counter("spectrum_query_count")


def record_download_action() -> int:
    """Count one UI-initiated download action.

    This metric reflects the operator click in the interface, not transfer
    completion. Download delivery may still fail later in the browser or
    repository layer.
    """

    return _increment_counter("download_action_count")


def get_usage_metrics_snapshot() -> dict[str, object]:
    """Return total, yearly, monthly, and breakdown usage metrics."""

    sync_nginx_download_metrics()

    if _use_memory_backend():
        return _build_snapshot_from_month_rows(_load_month_rows_from_memory())

    try:
        return _build_snapshot_from_month_rows(_load_month_rows_from_db())
    except Exception:
        return _build_snapshot_from_month_rows(_load_month_rows_from_memory())


def reset_usage_metrics() -> None:
    """Reset counters for tests.

    Production code should not call this helper.
    """

    with _COUNTER_LOCK:
        _MONTHLY_COUNTERS.clear()
        _CHECKPOINTS_MEMORY.clear()

    if _use_memory_backend():
        return

    try:
        conn = _get_summary_connection()
        try:
            cursor = conn.cursor()
            _create_metrics_table(cursor)
            _create_checkpoint_table(cursor)
            cursor.execute(f"DELETE FROM `{_COUNTER_TABLE}`")
            cursor.execute(f"DELETE FROM `{_CHECKPOINT_TABLE}`")
            conn.commit()
        finally:
            conn.close()
    except Exception:
        return
