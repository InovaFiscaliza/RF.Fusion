"""In-memory usage counters for lightweight WebFusion adoption signals.

These counters intentionally favor low operational risk over audit depth.
They exist to answer "is the interface being used?" without introducing a
new database dependency or touching the web server layer.
"""

from __future__ import annotations

from threading import Lock


_COUNTER_LOCK = Lock()
_COUNTERS = {
    "page_view_count": 0,
    "spectrum_query_count": 0,
    "download_action_count": 0,
}


def _increment_counter(counter_name: str) -> int:
    """Increase one metric atomically and return the new value."""

    with _COUNTER_LOCK:
        _COUNTERS[counter_name] += 1
        return _COUNTERS[counter_name]


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


def get_usage_metrics_snapshot() -> dict[str, int]:
    """Return a stable copy of the current counters."""

    with _COUNTER_LOCK:
        return dict(_COUNTERS)


def reset_usage_metrics() -> None:
    """Reset counters for tests.

    Production code should not call this helper.
    """

    with _COUNTER_LOCK:
        for key in _COUNTERS:
            _COUNTERS[key] = 0
