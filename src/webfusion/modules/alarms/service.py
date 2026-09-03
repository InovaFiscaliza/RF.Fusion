"""Normalize appCataloga Zabbix problems for the WebFusion alarm console."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from modules.zabbix_configuration.service import (
    get_appcataloga_problems,
    get_zabbix_api_url,
)


SEVERITY_DETAILS = {
    0: ("Não classificado", "not-classified"),
    1: ("Informação", "information"),
    2: ("Aviso", "warning"),
    3: ("Média", "average"),
    4: ("Alta", "high"),
    5: ("Desastre", "disaster"),
}
DEFAULT_SEVERITY = ("Não classificado", "not-classified")
ZABBIX_EVENT_ENTRYPOINT = "tr_events.php"


def list_alarms() -> list[dict[str, Any]]:
    """Return current appCataloga alarms formatted for the operator table."""
    api_url = get_zabbix_api_url()
    return [
        _normalize_problem(problem, api_url=api_url)
        for problem in get_appcataloga_problems()
    ]


def _normalize_problem(problem: dict[str, Any], *, api_url: str) -> dict[str, Any]:
    """Convert one Zabbix problem response into an alarm table row."""
    severity_label, severity_class = SEVERITY_DETAILS.get(
        _as_int(problem.get("severity")),
        DEFAULT_SEVERITY,
    )
    event_id = str(problem.get("eventid") or "").strip()
    return {
        "event_id": event_id,
        "name": str(problem.get("name") or "Problema sem descrição"),
        "opened_at": _format_clock(problem.get("clock")),
        "severity_label": severity_label,
        "severity_class": severity_class,
        "hosts": _format_hosts(problem.get("hosts")),
        "acknowledged": str(problem.get("acknowledged") or "0") == "1",
        "suppressed": str(problem.get("suppressed") or "0") == "1",
        "tags": _normalize_tags(problem.get("tags")),
        "zabbix_url": _problem_url(
            api_url=api_url,
            event_id=event_id,
            trigger_id=str(problem.get("objectid") or "").strip(),
        ),
    }


def _format_clock(value: Any) -> str:
    """Format an event Unix timestamp in the WebFusion local presentation."""
    timestamp = _as_int(value)
    if timestamp <= 0:
        return "-"
    return datetime.fromtimestamp(timestamp).strftime("%d/%m/%Y %H:%M")


def _format_hosts(value: Any) -> str:
    """Join the Zabbix host labels associated with a problem."""
    if not isinstance(value, list):
        return "-"
    labels = [
        str(host.get("name") or host.get("host") or "").strip()
        for host in value
        if isinstance(host, dict)
    ]
    return ", ".join(label for label in labels if label) or "-"


def _normalize_tags(value: Any) -> list[dict[str, str]]:
    """Keep non-empty tag name and value pairs in their API order."""
    if not isinstance(value, list):
        return []
    return [
        {
            "name": str(tag.get("tag") or "").strip(),
            "value": str(tag.get("value") or "").strip(),
        }
        for tag in value
        if isinstance(tag, dict) and str(tag.get("tag") or "").strip()
    ]


def _problem_url(*, api_url: str, event_id: str, trigger_id: str) -> str | None:
    """Build the Zabbix problem-view URL for an event without changing host."""
    if (
        not event_id.isdigit()
        or int(event_id) <= 0
        or not trigger_id.isdigit()
        or int(trigger_id) <= 0
    ):
        return None
    parts = urlsplit(str(api_url or "").strip())
    if not parts.scheme or not parts.netloc:
        return None
    base_path = parts.path.rsplit("/", 1)[0]
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update({"triggerid": trigger_id, "eventid": event_id})
    return urlunsplit(
        (parts.scheme, parts.netloc, f"{base_path}/{ZABBIX_EVENT_ENTRYPOINT}", urlencode(query), "")
    )


def _as_int(value: Any) -> int:
    """Return an integer Zabbix field or zero when the API value is invalid."""
    try:
        return int(str(value or "0"))
    except (TypeError, ValueError):
        return 0