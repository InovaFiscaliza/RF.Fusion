"""Persistent WebFusion usage counters grouped by reference month.

The server dashboard needs lightweight adoption signals that survive container
restarts without becoming a full audit trail. Persisting one monthly counter
row per metric keeps the write path simple while still allowing total, yearly,
and monthly aggregations for the `/server` page.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from threading import Lock


_COUNTER_LOCK = Lock()
_COUNTER_TABLE = "WEBFUSION_USAGE_METRIC_MONTHLY"
_METRIC_NAMES = (
    "page_view_count",
    "spectrum_query_count",
    "download_action_count",
)
_MONTHLY_COUNTERS: dict[tuple[str, str], int] = {}


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


def _load_month_rows_from_db() -> list[dict[str, object]]:
    """Read all persisted monthly metric rows from ``RFFUSION_SUMMARY``."""

    conn = _get_summary_connection()
    try:
        cursor = conn.cursor()
        _create_metrics_table(cursor)
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
    conn = _get_summary_connection()
    try:
        cursor = conn.cursor()
        _create_metrics_table(cursor)
        cursor.execute(
            f"""
            INSERT INTO `{_COUNTER_TABLE}` (`NA_METRIC_NAME`, `DT_REFERENCE_MONTH`, `NU_VALUE`)
            VALUES (%s, %s, 1)
            ON DUPLICATE KEY UPDATE `NU_VALUE` = `NU_VALUE` + 1
            """,
            (counter_name, reference_month),
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

    reference_month = _get_current_month_start().isoformat()

    with _COUNTER_LOCK:
        key = (counter_name, reference_month)
        _MONTHLY_COUNTERS[key] = _MONTHLY_COUNTERS.get(key, 0) + 1
        return sum(
            value
            for (metric_name, _), value in _MONTHLY_COUNTERS.items()
            if metric_name == counter_name
        )


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

    if _use_memory_backend():
        return

    try:
        conn = _get_summary_connection()
        try:
            cursor = conn.cursor()
            _create_metrics_table(cursor)
            cursor.execute(f"DELETE FROM `{_COUNTER_TABLE}`")
            conn.commit()
        finally:
            conn.close()
    except Exception:
        return
