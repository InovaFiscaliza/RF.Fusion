"""Persistent WebFusion usage counters grouped by reference month.

The server dashboard needs lightweight adoption signals that survive container
restarts without becoming a full audit trail. Persisting one monthly counter
row per metric keeps the write path simple while still allowing total, yearly,
and monthly aggregations for the `/server` page.

Nginx download delivery is ingested through a checkpoint. The checkpoint and
its counters share one transaction, so a restart does not silently count the
same completed log range twice. The in-memory backend exists only for tests and
local development.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
import hashlib
import os
import re
from threading import RLock
from typing import BinaryIO

from .config import (
    NGINX_CHECKPOINT_HASH_PREFIX,
    NGINX_CHECKPOINT_READ_BYTES,
    NGINX_DOWNLOAD_LOG_SOURCE as _NGINX_DOWNLOAD_LOG_SOURCE,
    NGINX_DOWNLOAD_METRIC as _NGINX_DOWNLOAD_METRIC,
)
from db.connection import get_connection_summary
from db.usage_metrics import (
    load_month_rows_from_db as _load_month_rows_from_db,
    increment_counter_by_month_in_db as _increment_counter_by_month_in_db,
    save_checkpoint_with_cursor as _save_checkpoint_with_cursor,
    lock_checkpoint_for_update as _lock_checkpoint_for_update,
    increment_nginx_months_with_cursor as _increment_nginx_months_with_cursor,
    replace_nginx_months_with_cursor as _replace_nginx_months_with_cursor,
    normalize_reference_month as _normalize_reference_month,
    reset_persisted_usage_metrics,
)


_COUNTER_LOCK = RLock()
_METRIC_NAMES = (
    "page_view_count",
    "spectrum_query_count",
    "download_action_count",
    "nginx_download_count",
)
_MONTHLY_COUNTERS: dict[tuple[str, str], int] = {}
_CHECKPOINTS_MEMORY: dict[str, dict[str, object]] = {}
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
    """Reject unexpected names before they reach a dynamic counter operation."""

    if counter_name not in _METRIC_NAMES:
        raise ValueError(f"Unknown usage metric: {counter_name}")


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


def _increment_counter_in_db(counter_name: str) -> int:
    """Increase one durable metric and return its all-time total."""

    reference_month = _get_current_month_start()
    return _increment_counter_by_month_in_db(
        counter_name=counter_name,
        reference_month=reference_month,
        amount=1,
    )


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


def _load_checkpoint_from_memory(source_name: str) -> dict[str, object] | None:
    """Read the test-mode checkpoint without exposing the mutable stored value."""

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
    """Persist the test-mode checkpoint using the durable backend field shape."""

    with _COUNTER_LOCK:
        _CHECKPOINTS_MEMORY[source_name] = {
            "source_name": source_name,
            "log_path": log_path,
            "file_signature": file_signature,
            "last_offset": int(last_offset),
            "last_size": int(last_size),
            "last_mtime_ns": int(last_mtime_ns),
        }


def _build_log_file_signature(stat_result: os.stat_result) -> str:
    """Return the legacy device/inode signature for a one-time checkpoint upgrade."""

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


def _hash_log_prefix(handle: BinaryIO, offset: int):
    """Hash the consumed bytes without relying on filesystem identity.

    Args:
        handle: Open binary log, repositioned by this function (BinaryIO).
        offset: Number of previously consumed bytes (int).

    Returns:
        SHA-256 state positioned at offset, ready to include new lines.

    Raises:
        RuntimeError: If the log is truncated while its prefix is being read.
    """
    handle.seek(0)
    digest = hashlib.sha256()
    remaining = offset
    while remaining:
        block = handle.read(min(remaining, NGINX_CHECKPOINT_READ_BYTES))
        if not block:
            raise RuntimeError("NGINX log changed while validating its checkpoint")
        digest.update(block)
        remaining -= len(block)
    return digest


def _legacy_prefix_matches_counts(
    handle: BinaryIO, offset: int, expected: dict[date, int] | None,
) -> bool:
    """Check a reconciled legacy checkpoint when device/inode changed before upgrade.

    Args:
        handle: Open binary log (BinaryIO).
        offset: Legacy consumed byte count (int).
        expected: Persisted monthly download totals, or None if unavailable.

    Returns:
        True only when the complete prefix has the same monthly counts.
        This one-time bridge is unavailable for incomplete/rotated histories.
    """
    if expected is None:
        return False
    handle.seek(0)
    counts: dict[date, int] = defaultdict(int)
    while handle.tell() < offset:
        line = handle.readline(offset - handle.tell())
        if not line.endswith(b"\n"):
            return False
        parsed = _parse_access_log_line(line.decode("utf-8", errors="replace"))
        if parsed and _is_countable_nginx_download(request_path=parsed[1], request_status=parsed[2]):
            counts[parsed[0]] += 1
    return dict(counts) == {month: count for month, count in expected.items() if count}


def _read_nginx_download_counts(
    log_path: str,
    checkpoint: dict[str, object] | None,
) -> tuple[dict[date, int], dict[str, object] | None]:
    """Read complete new lines, identifying the consumed prefix by its content.

    Args:
        log_path: Active NGINX log path (str).
        checkpoint: Previous offset/signature metadata (dict | None). Legacy
            checkpoints can include legacy_month_counts for a reconciled upgrade.

    Returns:
        Monthly increments and next checkpoint (tuple[dict[date, int], dict | None]).
        Missing logs preserve the existing checkpoint. A changed consumed prefix
        denotes a new/truncated log; unchanged bytes survive inode/mtime changes.

    Raises:
        RuntimeError: On concurrent truncation or an ambiguous legacy migration.
    """
    try:
        handle = open(log_path, "rb")
    except FileNotFoundError:
        return {}, None

    previous = checkpoint or {}
    signature = str(previous.get("file_signature") or "")
    offset = int(previous.get("last_offset") or 0)
    start_offset = 0
    counts_by_month: dict[date, int] = defaultdict(int)
    with handle:
        # Stat the opened descriptor: rotation must not mix two different files.
        initial_stat = os.fstat(handle.fileno())
        digest = hashlib.sha256()
        if signature and offset > 0 and initial_stat.st_size >= offset:
            digest = _hash_log_prefix(handle, offset)
            content_signature = NGINX_CHECKPOINT_HASH_PREFIX + digest.hexdigest()
            if signature.startswith(NGINX_CHECKPOINT_HASH_PREFIX):
                same_content = signature == content_signature
            else:
                same_content = (
                    signature == _build_log_file_signature(initial_stat)
                    and previous.get("log_path") == log_path
                ) or _legacy_prefix_matches_counts(
                    handle, offset, previous.get("legacy_month_counts"),
                )
                if not same_content:
                    # An old checkpoint contains no content evidence. Refuse to
                    # add its history again; an explicit reconciliation is needed.
                    raise RuntimeError("Legacy NGINX checkpoint needs reconciliation before migration")
            if same_content:
                start_offset = offset
            else:
                digest = hashlib.sha256()

        handle.seek(start_offset)
        # Bound each pass to a snapshot; appends are processed on the next pass.
        limit = initial_stat.st_size
        while handle.tell() < limit:
            line_start = handle.tell()
            line = handle.readline(limit - line_start)
            if not line.endswith(b"\n"):
                handle.seek(line_start)
                break
            digest.update(line)
            parsed = _parse_access_log_line(line.decode("utf-8", errors="replace"))
            if parsed and _is_countable_nginx_download(request_path=parsed[1], request_status=parsed[2]):
                counts_by_month[parsed[0]] += 1
        last_offset = handle.tell()
        final_stat = os.fstat(handle.fileno())
        if final_stat.st_size < limit:
            raise RuntimeError("NGINX log was truncated during ingestion")

    return dict(counts_by_month), {
        "source_name": _NGINX_DOWNLOAD_LOG_SOURCE,
        "log_path": log_path,
        "file_signature": NGINX_CHECKPOINT_HASH_PREFIX + digest.hexdigest(),
        "last_offset": last_offset,
        "last_size": int(final_stat.st_size),
        "last_mtime_ns": int(final_stat.st_mtime_ns),
    }


def _sync_nginx_download_metrics_in_db(log_path: str) -> None:
    """Commit new download counts and their checkpoint together."""

    conn = get_connection_summary()
    try:
        cursor = conn.cursor()

        checkpoint = _lock_checkpoint_for_update(cursor, log_path=log_path)
        counts_by_month, next_checkpoint = _read_nginx_download_counts(
            log_path,
            checkpoint,
        )

        if next_checkpoint is None:
            conn.rollback()
            return

        _increment_nginx_months_with_cursor(cursor, counts_by_month)
        _save_checkpoint_with_cursor(cursor, **next_checkpoint)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _sync_nginx_download_metrics_in_memory(log_path: str) -> None:
    """Keep test-mode ingestion behavior aligned with the durable flow."""

    checkpoint = _load_checkpoint_from_memory(_NGINX_DOWNLOAD_LOG_SOURCE)
    if checkpoint:
        checkpoint["legacy_month_counts"] = {
            _normalize_reference_month(month): count
            for (metric, month), count in _MONTHLY_COUNTERS.items()
            if metric == _NGINX_DOWNLOAD_METRIC
        }
    counts_by_month, next_checkpoint = _read_nginx_download_counts(
        log_path,
        checkpoint,
    )

    if next_checkpoint is None:
        return

    for reference_month, value in counts_by_month.items():
        _increment_counter_by_month_in_memory(
            counter_name=_NGINX_DOWNLOAD_METRIC,
            reference_month=reference_month,
            amount=value,
        )

    _save_checkpoint_to_memory(**next_checkpoint)


def sync_nginx_download_metrics() -> None:
    """Ingest successful NGINX download deliveries into monthly usage metrics."""

    log_path = _get_nginx_download_log_path()
    if not log_path:
        return

    with _COUNTER_LOCK:
        if _use_memory_backend():
            _sync_nginx_download_metrics_in_memory(log_path)
            return

        _sync_nginx_download_metrics_in_db(log_path)


def _reconcile_nginx_download_metrics_in_db(log_path: str) -> dict[str, int]:
    """Replace download metrics with a complete read of the active log."""

    conn = get_connection_summary()
    try:
        cursor = conn.cursor()

        _lock_checkpoint_for_update(cursor, log_path=log_path)
        counts_by_month, next_checkpoint = _read_nginx_download_counts(log_path, None)

        if next_checkpoint is None:
            conn.rollback()
            return {}

        _replace_nginx_months_with_cursor(cursor, counts_by_month)
        _save_checkpoint_with_cursor(cursor, **next_checkpoint)
        conn.commit()
        return {
            _format_reference_month(reference_month)[:7]: int(value)
            for reference_month, value in sorted(counts_by_month.items())
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _reconcile_nginx_download_metrics_in_memory(log_path: str) -> dict[str, int]:
    """Replace test-mode download counts with the active log content."""

    counts_by_month, next_checkpoint = _read_nginx_download_counts(log_path, None)
    if next_checkpoint is None:
        return {}

    stale_keys = [
        key
        for key in _MONTHLY_COUNTERS
        if key[0] == _NGINX_DOWNLOAD_METRIC
    ]
    for key in stale_keys:
        del _MONTHLY_COUNTERS[key]

    for reference_month, value in counts_by_month.items():
        _MONTHLY_COUNTERS[
            (_NGINX_DOWNLOAD_METRIC, _format_reference_month(reference_month))
        ] = int(value)

    _save_checkpoint_to_memory(**next_checkpoint)
    return {
        _format_reference_month(reference_month)[:7]: int(value)
        for reference_month, value in sorted(counts_by_month.items())
    }


def reconcile_nginx_download_metrics() -> dict[str, int]:
    """Rebuild download metrics and checkpoint from the active NGINX log."""

    log_path = _get_nginx_download_log_path()
    if not log_path:
        return {}

    with _COUNTER_LOCK:
        if _use_memory_backend():
            return _reconcile_nginx_download_metrics_in_memory(log_path)

        return _reconcile_nginx_download_metrics_in_db(log_path)


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
        reset_persisted_usage_metrics()
    except Exception:
        return
