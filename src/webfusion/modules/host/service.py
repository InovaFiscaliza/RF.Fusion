"""Service layer for the host and server operational views.

This module is where WebFusion reconciles three different kinds of information:

- live runtime probes from the current container or host
- operational queue/history state from ``BPDATA``
- analytical locality context from ``RFDATA`` or materialized global summaries
    from ``RFFUSION_SUMMARY``

The `/server` and `/host` pages share much of that meaning, so the service code
stays together here instead of duplicating SQL, caching, and error-grouping
rules across several files.
"""

import os
import re
import shutil
import subprocess
import time
from datetime import datetime

from db import get_connection_bpdata, get_connection_summary


_RUNTIME_OVERVIEW_CACHE = {
    "expires_at": 0.0,
    "payload": None,
}

_SERVER_OVERVIEW_CACHE = {
    "expires_at": 0.0,
    "payload": None,
}

_SERVER_SUMMARY_CACHE = {
    "expires_at": 0.0,
    "payload": None,
}

_GROUPED_PROCESSING_ERRORS_CACHE = {
    "expires_at": 0.0,
    "payload": None,
}

_GROUPED_BACKUP_ERRORS_CACHE = {
    "expires_at": 0.0,
    "payload": None,
}

_HOST_PROCESSING_ERRORS_CACHE = {}
_HOST_BACKUP_ERRORS_CACHE = {}
_HOST_LIST_CACHE = {}
_SERVER_HOST_ROWS_CACHE = {}

_HOST_LOCATION_HISTORY_CACHE = {}

_HOST_STATISTICS_CACHE = {}

# Runtime probes, grouped diagnostics and per-host detail views have different
# costs and different freshness expectations, so the service keeps them on
# separate TTL buckets instead of one shared cache.
RUNTIME_OVERVIEW_CACHE_TTL_SECONDS = 300.0
SERVER_OVERVIEW_CACHE_TTL_SECONDS = 600.0
SERVER_SUMMARY_CACHE_TTL_SECONDS = 600.0
GROUPED_PROCESSING_ERRORS_CACHE_TTL_SECONDS = 600.0
GROUPED_BACKUP_ERRORS_CACHE_TTL_SECONDS = 600.0
HOST_PROCESSING_ERRORS_CACHE_TTL_SECONDS = 300.0
HOST_BACKUP_ERRORS_CACHE_TTL_SECONDS = 300.0
HOST_LIST_CACHE_TTL_SECONDS = 60.0
SERVER_HOST_ROWS_CACHE_TTL_SECONDS = 60.0
HOST_LOCATION_HISTORY_CACHE_TTL_SECONDS = 600.0
HOST_STATISTICS_CACHE_TTL_SECONDS = 60.0


def _format_bytes_human(num_bytes):
    """Convert a byte count into a compact human-readable label."""

    value = float(num_bytes or 0)
    units = ["B", "KB", "MB", "GB", "TB", "PB"]

    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024


def _canonicalize_processing_error_message(message):
    """Collapse volatile processing-error variants into a stable display key.

    Operators usually care about the failure class, not about file-specific
    details embedded in the raw message. Canonicalizing here prevents the UI
    from showing dozens of nearly identical buckets for one logical issue.
    """

    normalized = (message or "(Sem mensagem)").strip() or "(Sem mensagem)"

    if normalized == "(Sem mensagem)":
        return normalized

    if normalized.lower() == "processing error":
        return (
            "Processing Error | [ERROR] [stage=PROCESS] "
            "[code=UNCLASSIFIED] Processing failed without structured detail"
        )

    if " | [detail=" in normalized:
        normalized = normalized.split(" | [detail=", 1)[0]

    lowered = normalized.lower()

    code_match = re.search(r"\[code=([A-Z0-9_]+)\]", normalized)
    if code_match:
        code = code_match.group(1)
        coded_labels = {
            "GPS_GNSS_UNAVAILABLE": (
                "Processing Error | [ERROR] [stage=PROCESS] "
                "[code=GPS_GNSS_UNAVAILABLE] Invalid GPS reading: GNSS unavailable sentinel"
            ),
            "NO_VALID_SPECTRA": (
                "Processing Error | [ERROR] [stage=PROCESS] "
                "[code=NO_VALID_SPECTRA] BIN discarded: no valid spectra after validation"
            ),
            "SPECTRUM_LIST_EMPTY": (
                "Processing Error | [ERROR] [stage=PROCESS] "
                "[code=SPECTRUM_LIST_EMPTY] Spectrum list is empty"
            ),
            "HOSTNAME_MISSING": (
                "Processing Error | [ERROR] [stage=PROCESS] "
                "[code=HOSTNAME_MISSING] Hostname missing or invalid"
            ),
            "FILE_NOT_FOUND": (
                "Processing Error | [ERROR] [stage=PROCESS] "
                "[code=FILE_NOT_FOUND] File not found"
            ),
            "INVALID_DATETIME_MONTH": (
                "Processing Error | [ERROR] [stage=PROCESS] "
                "[code=INVALID_DATETIME_MONTH] Invalid datetime string: month out of range"
            ),
            "INVALID_BUFFER_SIZE": (
                "Processing Error | [ERROR] [stage=PROCESS] "
                "[code=INVALID_BUFFER_SIZE] Invalid binary buffer size"
            ),
            "SITE_GEOGRAPHIC_CODES_NOT_FOUND": (
                "Processing Error | [ERROR] [stage=SITE] "
                "[code=SITE_GEOGRAPHIC_CODES_NOT_FOUND] "
                "Error inserting site in DIM_SPECTRUM_SITE: geographic codes not found"
            ),
        }
        return coded_labels.get(code, normalized)

    if "gnss unavailable sentinel" in lowered and "[stage=process]" in lowered:
        return (
            "Processing Error | [ERROR] [stage=PROCESS] "
            "[code=GPS_GNSS_UNAVAILABLE] Invalid GPS reading: GNSS unavailable sentinel"
        )

    if "bin discarded: no valid spectra after validation" in lowered:
        return (
            "Processing Error | [ERROR] [stage=PROCESS] "
            "[code=NO_VALID_SPECTRA] BIN discarded: no valid spectra after validation"
        )

    if "spectrum list is empty" in lowered:
        return (
            "Processing Error | [ERROR] [stage=PROCESS] "
            "[code=SPECTRUM_LIST_EMPTY] Spectrum list is empty"
        )

    if (
        "'hostname'" in normalized
        or "hostname resolution failed:" in lowered
        or "hostname missing or invalid" in lowered
    ):
        return (
            "Processing Error | [ERROR] [stage=PROCESS] "
            "[code=HOSTNAME_MISSING] Hostname missing or invalid"
        )

    if "[type=filenotfounderror]" in lowered and "no such file or directory:" in lowered:
        return (
            "Processing Error | [ERROR] [stage=PROCESS] "
            "[code=FILE_NOT_FOUND] File not found"
        )

    if 'month out of range in datetime string "' in lowered:
        return (
            "Processing Error | [ERROR] [stage=PROCESS] "
            "[code=INVALID_DATETIME_MONTH] Invalid datetime string: month out of range"
        )

    if "buffer size must be a multiple of element size" in lowered:
        return (
            "Processing Error | [ERROR] [stage=PROCESS] "
            "[code=INVALID_BUFFER_SIZE] Invalid binary buffer size"
        )

    if "[stage=site]" in lowered and "error retrieving geographic codes:" in lowered:
        return (
            "Processing Error | [ERROR] [stage=SITE] "
            "[code=SITE_GEOGRAPHIC_CODES_NOT_FOUND] "
            "Error inserting site in DIM_SPECTRUM_SITE: geographic codes not found"
        )

    if (
        "[type=binvalidationerror]" in lowered
        and "app_analise returned invalid answer.spectra type:" in lowered
    ):
        return (
            "Processing Error | [ERROR] [stage=PROCESS] "
            "[code=APP_ANALISE_INVALID_SPECTRA_TYPE] "
            "APP_ANALISE returned invalid Answer.Spectra type"
        )

    if (
        "[type=binvalidationerror]" in lowered
        and "payload validation failed during processing" in lowered
    ):
        return (
            "Processing Error | [ERROR] [stage=PROCESS] "
            "[code=BIN_PAYLOAD_VALIDATION_FAILED] "
            "Payload validation failed during processing"
        )

    return normalized


def _merge_grouped_processing_errors(rows):
    """Merge raw message buckets into canonical processing-error groups.

    SQL groups by the literal message first; this second pass folds those raw
    buckets into the more stable categories used by the dashboards.
    """

    merged = {}

    for row in rows:
        structured_message = _format_structured_error_bucket(
            row,
            default_label="Processing Error",
        )
        raw_message = row.get("ERROR_SUMMARY") or row.get("ERROR_MESSAGE") or "(Sem mensagem)"
        error_count = int(row.get("ERROR_COUNT") or 0)
        canonical_message = (
            structured_message
            if structured_message
            else _canonicalize_processing_error_message(raw_message)
        )

        bucket = merged.setdefault(
            canonical_message,
            {
                "ERROR_MESSAGE": canonical_message,
                "ERROR_COUNT": 0,
            },
        )
        bucket["ERROR_COUNT"] += error_count

    return sorted(
        merged.values(),
        key=lambda item: (-item["ERROR_COUNT"], item["ERROR_MESSAGE"]),
    )


def _clone_rows(rows):
    """Return a shallow copy of row dictionaries kept inside TTL caches."""

    return [dict(row) for row in rows]


def _build_host_list_cache_key(*, online_only=False, search=None):
    """Normalize filter inputs used by host-list TTL caches."""

    normalized_search = (search or "").strip().lower()
    return (bool(online_only), normalized_search)


def _format_structured_error_bucket(row, *, default_label):
    """Render one grouped structured-error row into the legacy dashboard label."""
    summary = str(row.get("ERROR_SUMMARY") or "").strip()
    stage = str(row.get("ERROR_STAGE") or "").strip()
    code = str(row.get("ERROR_CODE") or "").strip()

    if not summary or not (stage or code):
        return None

    parts = [f"{default_label} |", "[ERROR]"]

    if stage:
        parts.append(f"[stage={stage}]")

    if code:
        parts.append(f"[code={code}]")

    parts.append(summary)
    return " ".join(parts)


def _canonicalize_backup_error_message(message):
    """Collapse volatile backup-error variants into a stable display key."""

    normalized = (message or "(Sem mensagem)").strip() or "(Sem mensagem)"

    if normalized == "(Sem mensagem)":
        return normalized

    if " | [detail=" in normalized:
        normalized = normalized.split(" | [detail=", 1)[0]

    if normalized.startswith("Backup Error | file="):
        parts = normalized.split(" | ", 2)
        if len(parts) == 3:
            normalized = f"{parts[0]} | {parts[2]}"

    lowered = normalized.lower()

    code_match = re.search(r"\[code=([A-Z0-9_]+)\]", normalized)
    if code_match:
        code = code_match.group(1)
        coded_labels = {
            "AUTH_FAILED": (
                "Backup Error | [ERROR] [stage=AUTH] "
                "[code=AUTH_FAILED] Authentication failed"
            ),
            "SSH_NEGOTIATION_FAILED": (
                "Backup Error | [ERROR] [stage=SSH] "
                "[code=SSH_NEGOTIATION_FAILED] SSH negotiation failed"
            ),
            "SSH_CONNECT_TIMEOUT": (
                "Backup Error | [ERROR] [stage=CONNECT] "
                "[code=SSH_CONNECT_TIMEOUT] SSH/SFTP connection timed out"
            ),
            "SFTP_INIT_FAILED": (
                "Backup Error | [ERROR] [stage=CONNECT] "
                "[code=SFTP_INIT_FAILED] SSH/SFTP initialization failed"
            ),
            "FILE_NOT_FOUND": (
                "Backup Error | [ERROR] [stage=TRANSFER] "
                "[code=FILE_NOT_FOUND] File not found"
            ),
            "TRANSFER_TIMEOUT": (
                "Backup Error | [ERROR] [stage=TRANSFER] "
                "[code=TRANSFER_TIMEOUT] File transfer timed out"
            ),
            "TRANSFER_PERMISSION_DENIED": (
                "Backup Error | [ERROR] [stage=TRANSFER] "
                "[code=TRANSFER_PERMISSION_DENIED] Permission denied during transfer"
            ),
            "SSH_TRANSFER_FAILED": (
                "Backup Error | [ERROR] [stage=TRANSFER] "
                "[code=SSH_TRANSFER_FAILED] SSH/SFTP transfer failed"
            ),
            "TRANSFER_IO_ERROR": (
                "Backup Error | [ERROR] [stage=TRANSFER] "
                "[code=TRANSFER_IO_ERROR] Filesystem error during transfer"
            ),
            "FILE_TRANSFER_FAILED": (
                "Backup Error | [ERROR] [stage=TRANSFER] "
                "[code=FILE_TRANSFER_FAILED] File transfer failed"
            ),
            "FINALIZE_UPDATE_FAILED": (
                "Backup Error | [ERROR] [stage=FINALIZE] "
                "[code=FINALIZE_UPDATE_FAILED] Post-transfer update failed"
            ),
            "TASK_LOCK_FAILED": (
                "Backup Error | [ERROR] [stage=LOCK] "
                "[code=TASK_LOCK_FAILED] Failed to lock HOST or FILE_TASK"
            ),
            "HOST_NOT_FOUND": (
                "Backup Error | [ERROR] [stage=HOST_READ] "
                "[code=HOST_NOT_FOUND] Host not found in database"
            ),
        }
        return coded_labels.get(code, normalized)

    if "[stage=auth]" in lowered and "authentication failed" in lowered:
        return (
            "Backup Error | [ERROR] [stage=AUTH] "
            "[code=AUTH_FAILED] Authentication failed"
        )

    if "[stage=ssh]" in lowered and "ssh negotiation failed" in lowered:
        return (
            "Backup Error | [ERROR] [stage=SSH] "
            "[code=SSH_NEGOTIATION_FAILED] SSH negotiation failed"
        )

    if "[stage=connect]" in lowered and "timed out" in lowered:
        return (
            "Backup Error | [ERROR] [stage=CONNECT] "
            "[code=SSH_CONNECT_TIMEOUT] SSH/SFTP connection timed out"
        )

    if "[stage=connect]" in lowered and "ssh/sftp initialization failed" in lowered:
        return (
            "Backup Error | [ERROR] [stage=CONNECT] "
            "[code=SFTP_INIT_FAILED] SSH/SFTP initialization failed"
        )

    if "[stage=transfer]" in lowered and "[type=filenotfounderror]" in lowered:
        return (
            "Backup Error | [ERROR] [stage=TRANSFER] "
            "[code=FILE_NOT_FOUND] File not found"
        )

    if "[stage=transfer]" in lowered and "[type=timeouterror]" in lowered:
        return (
            "Backup Error | [ERROR] [stage=TRANSFER] "
            "[code=TRANSFER_TIMEOUT] File transfer timed out"
        )

    if "[stage=transfer]" in lowered and "permission denied" in lowered:
        return (
            "Backup Error | [ERROR] [stage=TRANSFER] "
            "[code=TRANSFER_PERMISSION_DENIED] Permission denied during transfer"
        )

    if "[stage=transfer]" in lowered and "[type=sshexception]" in lowered:
        return (
            "Backup Error | [ERROR] [stage=TRANSFER] "
            "[code=SSH_TRANSFER_FAILED] SSH/SFTP transfer failed"
        )

    if "[stage=transfer]" in lowered and "file transfer failed" in lowered:
        return (
            "Backup Error | [ERROR] [stage=TRANSFER] "
            "[code=FILE_TRANSFER_FAILED] File transfer failed"
        )

    if "[stage=finalize]" in lowered and "post-transfer update failed" in lowered:
        return (
            "Backup Error | [ERROR] [stage=FINALIZE] "
            "[code=FINALIZE_UPDATE_FAILED] Post-transfer update failed"
        )

    if "[stage=lock]" in lowered and "failed to lock host or file_task" in lowered:
        return (
            "Backup Error | [ERROR] [stage=LOCK] "
            "[code=TASK_LOCK_FAILED] Failed to lock HOST or FILE_TASK"
        )

    if "[stage=host_read]" in lowered and "host not found in database" in lowered:
        return (
            "Backup Error | [ERROR] [stage=HOST_READ] "
            "[code=HOST_NOT_FOUND] Host not found in database"
        )

    return normalized


def _merge_grouped_backup_errors(rows):
    """Merge raw message buckets into canonical backup-error groups."""

    merged = {}

    for row in rows:
        structured_message = _format_structured_error_bucket(
            row,
            default_label="Backup Error",
        )
        raw_message = row.get("ERROR_SUMMARY") or row.get("ERROR_MESSAGE") or "(Sem mensagem)"
        error_count = int(row.get("ERROR_COUNT") or 0)
        canonical_message = (
            structured_message
            if structured_message
            else _canonicalize_backup_error_message(raw_message)
        )

        bucket = merged.setdefault(
            canonical_message,
            {
                "ERROR_MESSAGE": canonical_message,
                "ERROR_COUNT": 0,
            },
        )
        bucket["ERROR_COUNT"] += error_count

    return sorted(
        merged.values(),
        key=lambda item: (-item["ERROR_COUNT"], item["ERROR_MESSAGE"]),
    )


def _read_meminfo():
    """Read memory figures from ``/proc/meminfo``.

    This is intentionally simple: the dashboard needs only total, available,
    and used memory to give the operator a quick server-health view.
    """

    meminfo = {}

    with open("/proc/meminfo", "r", encoding="ascii") as handler:
        for line in handler:
            key, value = line.split(":", 1)
            meminfo[key] = int(value.strip().split()[0]) * 1024

    total = meminfo.get("MemTotal", 0)
    available = meminfo.get("MemAvailable", 0)
    used = max(total - available, 0)

    return {
        "total_bytes": total,
        "used_bytes": used,
        "available_bytes": available,
        "use_percent": round((used / total) * 100, 1) if total else 0,
    }


def _get_reposfi_usage(path="/mnt/reposfi"):
    """Return the repository mount usage shown in the server dashboard."""

    try:
        usage = shutil.disk_usage(path)
    except FileNotFoundError:
        return {
            "mounted": False,
            "path": path,
        }

    return {
        "mounted": True,
        "path": path,
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "use_percent": round((usage.used / usage.total) * 100, 1) if usage.total else 0,
    }


def _get_repo_root():
    """Resolve the shared repository root from the current module location."""

    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))


def _load_appanalise_settings():
    """Read active appAnalise connection settings from appCataloga ``config.py``.

    The parser intentionally matches only uncommented assignment lines so old
    example values do not leak into the WebFusion dashboard.
    """

    config_path = os.path.join(
        _get_repo_root(),
        "src",
        "appCataloga",
        "server_volume",
        "etc",
        "appCataloga",
        "config.py",
    )

    try:
        with open(config_path, "r", encoding="utf-8") as handler:
            config_text = handler.read()
    except OSError:
        return {
            "host": None,
            "port": None,
            "timeout": 2.0,
        }

    host_match = re.search(r'^\s*APP_ANALISE_HOST_ADD\s*=\s*["\']([^"\']+)["\']', config_text, re.MULTILINE)
    port_match = re.search(r"^\s*APP_ANALISE_HOST_PORT\s*=\s*(\d+)", config_text, re.MULTILINE)
    timeout_match = re.search(r"^\s*APP_ANALISE_CONNECT_TIMEOUT\s*=\s*(\d+)", config_text, re.MULTILINE)

    timeout_value = float(timeout_match.group(1)) if timeout_match else 2.0

    return {
        "host": host_match.group(1) if host_match else None,
        "port": int(port_match.group(1)) if port_match else None,
        "timeout": min(timeout_value, 2.0),
    }


def _extract_ping_latency_ms(output):
    """Parse latency from a ping output line when available."""

    if not output:
        return None

    match = re.search(r"time[=<]\s*([0-9]+(?:\.[0-9]+)?)\s*ms", output, re.IGNORECASE)

    if not match:
        return None

    try:
        return round(float(match.group(1)), 1)
    except (TypeError, ValueError):
        return None


def _check_appanalise_status():
    """Run a short ICMP preflight against the configured appAnalise host."""

    settings = _load_appanalise_settings()
    host = settings["host"]
    port = settings["port"]
    timeout = settings["timeout"]

    status = {
        "host": host,
        "port": port,
        "timeout": timeout,
        "online": False,
        "latency_ms": None,
        "error": None,
    }

    if not host:
        status["error"] = "appAnalise host is not configured"
        return status

    started_at = time.perf_counter()
    timeout_seconds = max(1, int(round(timeout or 1)))
    ping_command = ["ping", "-c", "1", "-W", str(timeout_seconds), host]

    try:
        result = subprocess.run(
            ping_command,
            capture_output=True,
            text=True,
            timeout=timeout_seconds + 1,
            check=False,
        )

        if result.returncode == 0:
            status["online"] = True
            status["latency_ms"] = _extract_ping_latency_ms(result.stdout)

            if status["latency_ms"] is None:
                status["latency_ms"] = round((time.perf_counter() - started_at) * 1000, 1)
        else:
            error_message = (result.stderr or result.stdout or "").strip()
            status["error"] = error_message or f"ping exited with code {result.returncode}"
    except FileNotFoundError:
        status["error"] = "ping command not available"
    except subprocess.TimeoutExpired:
        status["error"] = "ping timed out"
    except Exception as exc:
        status["error"] = str(exc)

    return status


def _get_runtime_overview():
    """Build the small infrastructure snapshot shown in the server page.

    These values change more often than the historical SQL aggregates, so they
    use a separate and shorter cache.
    """

    now = time.monotonic()

    if _RUNTIME_OVERVIEW_CACHE["payload"] and _RUNTIME_OVERVIEW_CACHE["expires_at"] > now:
        return _RUNTIME_OVERVIEW_CACHE["payload"]

    memory = _read_meminfo()
    reposfi = _get_reposfi_usage()
    appanalise = _check_appanalise_status()

    payload = {
        "memory": {
            **memory,
            "total_human": _format_bytes_human(memory["total_bytes"]),
            "used_human": _format_bytes_human(memory["used_bytes"]),
            "available_human": _format_bytes_human(memory["available_bytes"]),
        },
        "reposfi": {
            **reposfi,
            "total_human": _format_bytes_human(reposfi.get("total_bytes")),
            "used_human": _format_bytes_human(reposfi.get("used_bytes")),
            "free_human": _format_bytes_human(reposfi.get("free_bytes")),
        },
        "appanalise": appanalise,
    }

    _RUNTIME_OVERVIEW_CACHE["payload"] = payload
    _RUNTIME_OVERVIEW_CACHE["expires_at"] = now + RUNTIME_OVERVIEW_CACHE_TTL_SECONDS

    return payload


def _build_host_filters(prefix="", online_only=False, search=None):
    """Build reusable host-table filters for both live and summary queries."""

    where_clauses = []
    params = []

    if online_only:
        where_clauses.append(f"{prefix}IS_OFFLINE = 0")

    if search:
        where_clauses.append(f"{prefix}NA_HOST_NAME LIKE %s")
        params.append(f"%{search}%")

    return where_clauses, params


def _get_server_error_summary_rows(error_scope):
    """Read grouped server error buckets from ``SERVER_ERROR_SUMMARY``."""

    conn = get_connection_summary()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            NULLIF(TRIM(NA_ERROR_DOMAIN), '') AS ERROR_DOMAIN,
            NULLIF(TRIM(NA_ERROR_STAGE), '') AS ERROR_STAGE,
            NULLIF(TRIM(NA_ERROR_CODE), '') AS ERROR_CODE,
            COALESCE(NULLIF(TRIM(NA_ERROR_SUMMARY), ''), '(Sem mensagem)') AS ERROR_SUMMARY,
            NU_ERROR_COUNT AS ERROR_COUNT
        FROM SERVER_ERROR_SUMMARY
        WHERE NA_ERROR_SCOPE = %s
        ORDER BY NU_ERROR_COUNT DESC, NA_ERROR_SUMMARY ASC
        """,
        (error_scope,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def _get_server_current_summary_row():
    """Read the singleton row that backs the global `/server` summary cards."""

    conn = get_connection_summary()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            ID_SUMMARY,
            NA_CURRENT_MONTH_LABEL,
            NU_TOTAL_HOSTS,
            NU_ONLINE_HOSTS,
            NU_OFFLINE_HOSTS,
            NU_BUSY_HOSTS,
            NU_DISCOVERED_FILES_TOTAL,
            NU_BACKUP_PENDING_FILES_TOTAL,
            VL_BACKUP_PENDING_GB_TOTAL,
            NU_BACKUP_ERROR_FILES_TOTAL,
            NU_BACKUP_QUEUE_FILES_TOTAL,
            VL_BACKUP_QUEUE_GB_TOTAL,
            NU_PROCESSING_PENDING_FILES_TOTAL,
            NU_PROCESSING_DONE_FILES_TOTAL,
            NU_PROCESSING_ERROR_FILES_TOTAL,
            NU_PROCESSING_QUEUE_FILES_TOTAL,
            VL_PROCESSING_QUEUE_GB_TOTAL,
            NU_FACT_SPECTRUM_TOTAL,
            NU_BACKUP_DONE_THIS_MONTH,
            VL_BACKUP_DONE_GB_THIS_MONTH
        FROM SERVER_CURRENT_SUMMARY
        WHERE ID_SUMMARY = 1
        """
    )
    row = cur.fetchone() or {}
    conn.close()
    return row


def _get_server_discovered_files_total_fallback():
    """Derive discovered-file totals from monthly metrics when snapshots lag."""

    conn = get_connection_summary()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(metric.NU_DISCOVERED_FILES), 0) AS NU_DISCOVERED_FILES_TOTAL
        FROM HOST_MONTHLY_METRIC metric
        """
    )
    row = cur.fetchone() or {}
    conn.close()
    return int(row.get("NU_DISCOVERED_FILES_TOTAL") or 0)


def _get_summary_host_rows(search=None, online_only=False):
    """Read the server host table from ``HOST_CURRENT_SNAPSHOT``.

    This keeps the global dashboard table aligned with the same materialized
    snapshot used by the server-wide cards.
    """

    conn = get_connection_summary()
    cur = conn.cursor()

    query = """
        SELECT
            ID_HOST,
            NA_HOST_NAME,
            NA_HOST_ADDRESS,
            NA_HOST_PORT,
            IS_OFFLINE,
            IS_BUSY,
            DT_LAST_CHECK,
            DT_LAST_DISCOVERY,
            DT_LAST_BACKUP,
            DT_LAST_PROCESSING,
            NU_PENDING_FILE_BACKUP_TASKS,
            NU_ERROR_FILE_BACKUP_TASKS,
            NU_PENDING_FILE_PROCESS_TASKS,
            NU_ERROR_FILE_PROCESS_TASKS,
            VL_PENDING_BACKUP_GB AS PENDING_BACKUP_GB
        FROM HOST_CURRENT_SNAPSHOT
    """

    where_clauses, params = _build_host_filters(
        online_only=online_only,
        search=search,
    )

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " ORDER BY NA_HOST_NAME"

    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return rows


def _empty_host_location_payload():
    """Return the empty locality payload expected by the host page."""

    return {
        "equipment_matches": [],
        "location_history": [],
    }


def _get_host_current_snapshot_row(host_id):
    """Read the summary snapshot row that backs one host drill-down."""

    conn = get_connection_summary()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM HOST_CURRENT_SNAPSHOT
        WHERE ID_HOST = %s
        LIMIT 1
        """,
        (int(host_id),),
    )
    row = cur.fetchone() or {}
    conn.close()
    return row


def _get_host_monthly_metric_rows(host_id):
    """Read monthly host metrics from the summary schema."""

    conn = get_connection_summary()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            DT_REFERENCE_MONTH,
            NU_DISCOVERED_FILES,
            VL_DISCOVERED_GB,
            NU_BACKUP_DONE_FILES,
            VL_BACKUP_DONE_GB,
            NU_BACKUP_PENDING_FILES,
            VL_BACKUP_PENDING_GB,
            NU_BACKUP_ERROR_FILES,
            VL_BACKUP_ERROR_GB,
            NU_PROCESSING_DONE_FILES,
            VL_PROCESSING_DONE_GB,
            NU_PROCESSING_PENDING_FILES,
            VL_PROCESSING_PENDING_GB,
            NU_PROCESSING_ERROR_FILES,
            VL_PROCESSING_ERROR_GB
        FROM HOST_MONTHLY_METRIC
        WHERE FK_HOST = %s
        ORDER BY DT_REFERENCE_MONTH DESC
        """,
        (int(host_id),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def _get_host_error_summary_rows(host_id, error_scope):
    """Read grouped host-specific error buckets from ``HOST_ERROR_SUMMARY``."""

    conn = get_connection_summary()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            NULLIF(TRIM(NA_ERROR_STAGE), '') AS ERROR_STAGE,
            NULLIF(TRIM(NA_ERROR_CODE), '') AS ERROR_CODE,
            COALESCE(NULLIF(TRIM(NA_ERROR_SUMMARY), ''), '(Sem mensagem)') AS ERROR_SUMMARY,
            NU_ERROR_COUNT AS ERROR_COUNT
        FROM HOST_ERROR_SUMMARY
        WHERE FK_HOST = %s
          AND NA_ERROR_SCOPE = %s
        ORDER BY NU_ERROR_COUNT DESC, NA_ERROR_SUMMARY ASC
        """,
        (int(host_id), error_scope),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def _get_current_month_backup_summary_for_host(
    host_id,
    current_month_start,
    next_month_start,
):
    """Read the exact current-month backup throughput for one host.

    ``HOST_MONTHLY_METRIC`` is keyed by ``DT_FILE_CREATED`` and therefore cannot
    answer the host card that is explicitly about files backed up during the
    current calendar month. This narrower live query preserves that meaning
    while the rest of the page moves to summary-backed reads.
    """

    conn = get_connection_bpdata()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COUNT(*) AS BACKUP_DONE_THIS_MONTH,
            ROUND(COALESCE(SUM(VL_FILE_SIZE_KB), 0) / 1024 / 1024, 2) AS BACKUP_DONE_GB_THIS_MONTH
        FROM FILE_TASK_HISTORY
        WHERE FK_HOST = %s
          AND NU_STATUS_BACKUP = 0
          AND DT_BACKUP >= %s
          AND DT_BACKUP < %s
        """,
        (
            int(host_id),
            current_month_start.strftime("%Y-%m-%d %H:%M:%S"),
            next_month_start.strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    row = cur.fetchone() or {}
    conn.close()
    return row


def _build_host_yearly_breakdowns(monthly_rows):
    """Collapse month-grain summary rows into the annual tables shown on `/host`."""

    yearly = {}

    for row in monthly_rows:
        reference_month = row.get("DT_REFERENCE_MONTH")

        if reference_month is None:
            continue

        year = reference_month.year
        bucket = yearly.setdefault(
            year,
            {
                "REFERENCE_YEAR": year,
                "DISCOVERED_FILES": 0,
                "DISCOVERED_GB": 0.0,
                "BACKUP_DONE_FILES": 0,
                "BACKUP_DONE_GB": 0.0,
                "BACKUP_PENDING_FILES": 0,
                "BACKUP_PENDING_GB": 0.0,
                "BACKUP_ERROR_FILES": 0,
                "BACKUP_ERROR_GB": 0.0,
                "PROCESSING_DONE_FILES": 0,
                "PROCESSING_DONE_GB": 0.0,
                "PROCESSING_PENDING_FILES": 0,
                "PROCESSING_PENDING_GB": 0.0,
                "PROCESSING_ERROR_FILES": 0,
                "PROCESSING_ERROR_GB": 0.0,
            },
        )

        bucket["DISCOVERED_FILES"] += int(row.get("NU_DISCOVERED_FILES") or 0)
        bucket["DISCOVERED_GB"] += float(row.get("VL_DISCOVERED_GB") or 0)
        bucket["BACKUP_DONE_FILES"] += int(row.get("NU_BACKUP_DONE_FILES") or 0)
        bucket["BACKUP_DONE_GB"] += float(row.get("VL_BACKUP_DONE_GB") or 0)
        bucket["BACKUP_PENDING_FILES"] += int(row.get("NU_BACKUP_PENDING_FILES") or 0)
        bucket["BACKUP_PENDING_GB"] += float(row.get("VL_BACKUP_PENDING_GB") or 0)
        bucket["BACKUP_ERROR_FILES"] += int(row.get("NU_BACKUP_ERROR_FILES") or 0)
        bucket["BACKUP_ERROR_GB"] += float(row.get("VL_BACKUP_ERROR_GB") or 0)
        bucket["PROCESSING_DONE_FILES"] += int(row.get("NU_PROCESSING_DONE_FILES") or 0)
        bucket["PROCESSING_DONE_GB"] += float(row.get("VL_PROCESSING_DONE_GB") or 0)
        bucket["PROCESSING_PENDING_FILES"] += int(row.get("NU_PROCESSING_PENDING_FILES") or 0)
        bucket["PROCESSING_PENDING_GB"] += float(row.get("VL_PROCESSING_PENDING_GB") or 0)
        bucket["PROCESSING_ERROR_FILES"] += int(row.get("NU_PROCESSING_ERROR_FILES") or 0)
        bucket["PROCESSING_ERROR_GB"] += float(row.get("VL_PROCESSING_ERROR_GB") or 0)

    backup_rows = []
    processing_rows = []

    for year in sorted(yearly, reverse=True):
        bucket = yearly[year]
        backup_rows.append(
            {
                "REFERENCE_YEAR": bucket["REFERENCE_YEAR"],
                "DISCOVERED_FILES": bucket["DISCOVERED_FILES"],
                "DISCOVERED_GB": round(bucket["DISCOVERED_GB"], 2),
                "BACKUP_DONE_FILES": bucket["BACKUP_DONE_FILES"],
                "BACKUP_DONE_GB": round(bucket["BACKUP_DONE_GB"], 2),
                "BACKUP_PENDING_FILES": bucket["BACKUP_PENDING_FILES"],
                "BACKUP_PENDING_GB": round(bucket["BACKUP_PENDING_GB"], 2),
                "BACKUP_ERROR_FILES": bucket["BACKUP_ERROR_FILES"],
                "BACKUP_ERROR_GB": round(bucket["BACKUP_ERROR_GB"], 2),
            }
        )
        processing_rows.append(
            {
                "REFERENCE_YEAR": bucket["REFERENCE_YEAR"],
                "PROCESSING_DONE_FILES": bucket["PROCESSING_DONE_FILES"],
                "PROCESSING_DONE_GB": round(bucket["PROCESSING_DONE_GB"], 2),
                "PROCESSING_PENDING_FILES": bucket["PROCESSING_PENDING_FILES"],
                "PROCESSING_PENDING_GB": round(bucket["PROCESSING_PENDING_GB"], 2),
                "PROCESSING_ERROR_FILES": bucket["PROCESSING_ERROR_FILES"],
                "PROCESSING_ERROR_GB": round(bucket["PROCESSING_ERROR_GB"], 2),
            }
        )

    return backup_rows, processing_rows


def _get_host_location_history(host_id):
    """Read one host locality history payload from ``HOST_LOCATION_SUMMARY``."""

    normalized_host_id = int(host_id)
    now = time.monotonic()
    cached = _HOST_LOCATION_HISTORY_CACHE.get(normalized_host_id)

    if cached and cached["expires_at"] > now:
        return cached["payload"]

    conn = get_connection_summary()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            FK_EQUIPMENT AS ID_EQUIPMENT,
            NA_EQUIPMENT,
            NA_MATCH_TYPE AS MATCH_TYPE,
            VL_MATCH_CONFIDENCE AS MATCH_CONFIDENCE
        FROM HOST_EQUIPMENT_LINK
        WHERE FK_HOST = %s
          AND IS_ACTIVE = 1
          AND IS_PRIMARY_LINK = 1
        ORDER BY IS_MANUAL_OVERRIDE DESC, VL_MATCH_CONFIDENCE DESC, NA_EQUIPMENT ASC
        """,
        (normalized_host_id,),
    )
    equipment_matches = cur.fetchall()

    cur.execute(
        """
        SELECT
            FK_SITE AS ID_SITE,
            NA_LOCALITY_LABEL AS LOCALITY_LABEL,
            NA_COUNTY_NAME AS COUNTY_NAME,
            NA_STATE_NAME AS STATE_NAME,
            NA_STATE_CODE AS STATE_CODE,
            DT_FIRST_SEEN_AT AS FIRST_SEEN_AT,
            DT_LAST_SEEN_AT AS LAST_SEEN_AT,
            NU_SPECTRUM_COUNT AS SPECTRUM_COUNT
        FROM HOST_LOCATION_SUMMARY
        WHERE FK_HOST = %s
        ORDER BY
            IS_CURRENT_LOCATION DESC,
            COALESCE(DT_LAST_SEEN_AT, DT_FIRST_SEEN_AT) DESC,
            FK_SITE ASC
        """,
        (normalized_host_id,),
    )
    location_history = cur.fetchall()
    conn.close()

    for row in equipment_matches:
        row["ID_EQUIPMENT"] = int(row.get("ID_EQUIPMENT") or 0)
        row["MATCH_CONFIDENCE"] = float(row.get("MATCH_CONFIDENCE") or 0)

    for row in location_history:
        row["ID_SITE"] = int(row.get("ID_SITE") or 0)
        row["SPECTRUM_COUNT"] = int(row.get("SPECTRUM_COUNT") or 0)

    payload = {
        "equipment_matches": equipment_matches,
        "location_history": location_history,
    }
    _HOST_LOCATION_HISTORY_CACHE[normalized_host_id] = {
        "expires_at": now + HOST_LOCATION_HISTORY_CACHE_TTL_SECONDS,
        "payload": payload,
    }
    return payload


def get_server_processing_error_overview():
    """Return grouped processing errors for the global dashboard.

    The rows come from the summary schema and are then normalized into the more
    stable operator-facing buckets used by the UI.
    """

    now = time.monotonic()

    if (
        _GROUPED_PROCESSING_ERRORS_CACHE["payload"] is not None
        and _GROUPED_PROCESSING_ERRORS_CACHE["expires_at"] > now
    ):
        return _GROUPED_PROCESSING_ERRORS_CACHE["payload"]

    rows = _get_server_error_summary_rows("PROCESSING")

    normalized_rows = _merge_grouped_processing_errors(rows)
    payload = {
        "rows": normalized_rows,
        "error_group_count": len(normalized_rows),
        "error_total_occurrences": sum(row["ERROR_COUNT"] for row in normalized_rows),
    }

    _GROUPED_PROCESSING_ERRORS_CACHE["payload"] = payload
    _GROUPED_PROCESSING_ERRORS_CACHE["expires_at"] = (
        now + GROUPED_PROCESSING_ERRORS_CACHE_TTL_SECONDS
    )

    return payload


def get_server_backup_error_overview():
    """Return grouped backup errors for the global dashboard.

    The summary schema provides the raw grouped counts; this helper applies the
    same canonicalization layer used by the host-specific views.
    """

    now = time.monotonic()

    if (
        _GROUPED_BACKUP_ERRORS_CACHE["payload"] is not None
        and _GROUPED_BACKUP_ERRORS_CACHE["expires_at"] > now
    ):
        return _GROUPED_BACKUP_ERRORS_CACHE["payload"]

    rows = _get_server_error_summary_rows("BACKUP")

    normalized_rows = _merge_grouped_backup_errors(rows)
    payload = {
        "rows": normalized_rows,
        "error_group_count": len(normalized_rows),
        "error_total_occurrences": sum(row["ERROR_COUNT"] for row in normalized_rows),
    }

    _GROUPED_BACKUP_ERRORS_CACHE["payload"] = payload
    _GROUPED_BACKUP_ERRORS_CACHE["expires_at"] = (
        now + GROUPED_BACKUP_ERRORS_CACHE_TTL_SECONDS
    )

    return payload


def get_server_summary_metrics():
    """Return the materialized global server summary metrics.

    This is the main fast path behind the `/api/server/summary-metrics`
    endpoint and is intentionally sourced from ``RFFUSION_SUMMARY``.
    """

    now = time.monotonic()

    if _SERVER_SUMMARY_CACHE["payload"] and _SERVER_SUMMARY_CACHE["expires_at"] > now:
        return _SERVER_SUMMARY_CACHE["payload"]

    summary_row = _get_server_current_summary_row()

    if not summary_row or not summary_row.get("ID_SUMMARY"):
        raise RuntimeError("server_current_summary_missing")

    discovered_files_total = int(summary_row.get("NU_DISCOVERED_FILES_TOTAL") or 0)
    if discovered_files_total <= 0:
        fallback_discovered_total = _get_server_discovered_files_total_fallback()
        if fallback_discovered_total > 0:
            discovered_files_total = fallback_discovered_total

    payload = {
        "CURRENT_MONTH_LABEL": summary_row.get("NA_CURRENT_MONTH_LABEL") or datetime.utcnow().strftime("%Y-%m"),
        "BACKUP_DONE_THIS_MONTH": int(summary_row.get("NU_BACKUP_DONE_THIS_MONTH") or 0),
        "BACKUP_DONE_GB_THIS_MONTH": float(summary_row.get("VL_BACKUP_DONE_GB_THIS_MONTH") or 0),
        "DISCOVERED_FILES_TOTAL": discovered_files_total,
        "BACKUP_PENDING_FILES_TOTAL": int(summary_row.get("NU_BACKUP_PENDING_FILES_TOTAL") or 0),
        "BACKUP_PENDING_GB_TOTAL": float(summary_row.get("VL_BACKUP_PENDING_GB_TOTAL") or 0),
        "BACKUP_ERROR_FILES_TOTAL": int(summary_row.get("NU_BACKUP_ERROR_FILES_TOTAL") or 0),
        "BACKUP_QUEUE_FILES_TOTAL": int(summary_row.get("NU_BACKUP_QUEUE_FILES_TOTAL") or 0),
        "BACKUP_QUEUE_GB_TOTAL": float(summary_row.get("VL_BACKUP_QUEUE_GB_TOTAL") or 0),
        "PROCESSING_PENDING_FILES_TOTAL": int(summary_row.get("NU_PROCESSING_PENDING_FILES_TOTAL") or 0),
        "PROCESSING_DONE_FILES_TOTAL": int(summary_row.get("NU_PROCESSING_DONE_FILES_TOTAL") or 0),
        "FACT_SPECTRUM_TOTAL": int(summary_row.get("NU_FACT_SPECTRUM_TOTAL") or 0),
        "PROCESSING_QUEUE_FILES_TOTAL": int(summary_row.get("NU_PROCESSING_QUEUE_FILES_TOTAL") or 0),
        "PROCESSING_QUEUE_GB_TOTAL": float(summary_row.get("VL_PROCESSING_QUEUE_GB_TOTAL") or 0),
        "PROCESSING_ERROR_FILES_TOTAL": int(summary_row.get("NU_PROCESSING_ERROR_FILES_TOTAL") or 0),
    }

    _SERVER_SUMMARY_CACHE["payload"] = payload
    _SERVER_SUMMARY_CACHE["expires_at"] = now + SERVER_SUMMARY_CACHE_TTL_SECONDS
    return payload


def get_host_processing_error_overview(host_id):
    """Return grouped processing errors for one host from ``HOST_ERROR_SUMMARY``."""

    cache_key = int(host_id)
    now = time.monotonic()
    cached = _HOST_PROCESSING_ERRORS_CACHE.get(cache_key)

    if cached and cached["expires_at"] > now:
        return cached["payload"]

    rows = _get_host_error_summary_rows(cache_key, "PROCESSING")
    normalized_rows = _merge_grouped_processing_errors(rows)
    payload = {
        "rows": normalized_rows,
        "error_group_count": len(normalized_rows),
        "error_total_occurrences": sum(row["ERROR_COUNT"] for row in normalized_rows),
    }

    _HOST_PROCESSING_ERRORS_CACHE[cache_key] = {
        "expires_at": now + HOST_PROCESSING_ERRORS_CACHE_TTL_SECONDS,
        "payload": payload,
    }
    return payload


def get_host_backup_error_overview(host_id):
    """Return grouped backup errors for one host from ``HOST_ERROR_SUMMARY``."""

    cache_key = int(host_id)
    now = time.monotonic()
    cached = _HOST_BACKUP_ERRORS_CACHE.get(cache_key)

    if cached and cached["expires_at"] > now:
        return cached["payload"]

    rows = _get_host_error_summary_rows(cache_key, "BACKUP")
    normalized_rows = _merge_grouped_backup_errors(rows)
    payload = {
        "rows": normalized_rows,
        "error_group_count": len(normalized_rows),
        "error_total_occurrences": sum(row["ERROR_COUNT"] for row in normalized_rows),
    }

    _HOST_BACKUP_ERRORS_CACHE[cache_key] = {
        "expires_at": now + HOST_BACKUP_ERRORS_CACHE_TTL_SECONDS,
        "payload": payload,
    }
    return payload


def get_host_location_history_overview(host_id):
    """Return the summary-backed locality history for one host on demand."""

    return _get_host_location_history(host_id)


def get_all_hosts(online_only=False, search=None):
    """Return the host picker list used by the host page.

    This is intentionally narrower than the server dashboard table because the
    host page mainly needs a quick navigation list, not a full operational grid.
    """

    cache_key = _build_host_list_cache_key(
        online_only=online_only,
        search=search,
    )
    now = time.monotonic()
    cached = _HOST_LIST_CACHE.get(cache_key)

    if cached and cached["expires_at"] > now:
        return _clone_rows(cached["payload"])

    query = """
        SELECT ID_HOST, NA_HOST_NAME, IS_OFFLINE
        FROM HOST_CURRENT_SNAPSHOT
    """

    where_clauses, params = _build_host_filters(
        online_only=online_only,
        search=search,
    )

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " ORDER BY NA_HOST_NAME"

    conn = get_connection_summary()
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()

    _HOST_LIST_CACHE[cache_key] = {
        "expires_at": now + HOST_LIST_CACHE_TTL_SECONDS,
        "payload": _clone_rows(rows),
    }
    return rows


def get_server_overview(online_only=False, search=None):
    """Return the global dashboard plus the filtered host table.

    Important behavior:
        - the summary cards remain global
        - ``online_only`` and ``search`` affect only ``HOST_ROWS``
        - grouped processing diagnostics are loaded on demand by the page

    The summary counters come from ``RFFUSION_SUMMARY`` while the runtime probe
    section is appended afterward from live environment checks. This separation
    avoids hiding real server totals just because the operator filtered the
    table for navigation.
    """

    now = time.monotonic()

    if _SERVER_OVERVIEW_CACHE["payload"] and _SERVER_OVERVIEW_CACHE["expires_at"] > now:
        overview = dict(_SERVER_OVERVIEW_CACHE["payload"])
    else:
        host_summary = _get_server_current_summary_row()
        current_month_label = host_summary.get("NA_CURRENT_MONTH_LABEL") or datetime.utcnow().strftime("%Y-%m")
        total_hosts = int(host_summary.get("NU_TOTAL_HOSTS") or 0)
        online_hosts = int(host_summary.get("NU_ONLINE_HOSTS") or 0)
        offline_hosts = int(host_summary.get("NU_OFFLINE_HOSTS") or 0)
        busy_hosts = int(host_summary.get("NU_BUSY_HOSTS") or 0)

        overview = {
            "CURRENT_MONTH_LABEL": current_month_label,
            "TOTAL_HOSTS": total_hosts,
            "ONLINE_HOSTS": online_hosts,
            "OFFLINE_HOSTS": offline_hosts,
            "BUSY_HOSTS": busy_hosts,
            "BACKUP_DONE_THIS_MONTH": None,
            "BACKUP_DONE_GB_THIS_MONTH": None,
            "DISCOVERED_FILES_TOTAL": None,
            "DISCOVERED_GB_TOTAL": None,
            "BACKUP_DONE_FILES_TOTAL": None,
            "BACKUP_DONE_GB_TOTAL": None,
            "BACKUP_PENDING_FILES_TOTAL": None,
            "BACKUP_PENDING_GB_TOTAL": None,
            "BACKUP_ERROR_FILES_TOTAL": None,
            "BACKUP_ERROR_GB_TOTAL": None,
            "PROCESSING_DONE_FILES_TOTAL": None,
            "PROCESSING_DONE_GB_TOTAL": None,
            "PROCESSING_PENDING_FILES_TOTAL": None,
            "PROCESSING_PENDING_GB_TOTAL": None,
            "PROCESSING_ERROR_FILES_TOTAL": None,
            "PROCESSING_ERROR_GB_TOTAL": None,
        }

        _SERVER_OVERVIEW_CACHE["payload"] = dict(overview)
        _SERVER_OVERVIEW_CACHE["expires_at"] = now + SERVER_OVERVIEW_CACHE_TTL_SECONDS

    # Runtime probes are appended after the cached SQL summary because they
    # age faster than the historical counters and deserve a shorter TTL.
    runtime_overview = _get_runtime_overview()
    overview["SERVER_MEMORY"] = runtime_overview["memory"]
    overview["REPOSFI_USAGE"] = runtime_overview["reposfi"]
    overview["APP_ANALISE_STATUS"] = runtime_overview["appanalise"]

    return overview


def get_host_statistics(host_id):
    """Return the detailed operational picture for one station.

    The result mixes:
        - current operational counters from ``HOST_CURRENT_SNAPSHOT``
        - historical totals and annual breakdowns from ``HOST_MONTHLY_METRIC``
        - one narrow live query for the exact "backup done this month" card,
          which is keyed by ``DT_BACKUP`` rather than ``DT_FILE_CREATED``
    """

    normalized_host_id = int(host_id)
    now = time.monotonic()
    cached = _HOST_STATISTICS_CACHE.get(normalized_host_id)

    if cached and cached["expires_at"] > now:
        return dict(cached["payload"])

    row = _get_host_current_snapshot_row(normalized_host_id)

    if not row:
        return None

    row["PENDING_GB"] = round(float(row.get("VL_PENDING_BACKUP_GB") or 0), 2)
    row["DONE_GB"] = round(float(row.get("VL_DONE_BACKUP_GB") or 0), 2)

    current_month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if current_month_start.month == 12:
        next_month_start = current_month_start.replace(year=current_month_start.year + 1, month=1)
    else:
        next_month_start = current_month_start.replace(month=current_month_start.month + 1)

    monthly_rows = _get_host_monthly_metric_rows(normalized_host_id)
    current_month_backup = _get_current_month_backup_summary_for_host(
        normalized_host_id,
        current_month_start,
        next_month_start,
    )

    row["CURRENT_MONTH_LABEL"] = current_month_start.strftime("%Y-%m")
    row["BACKUP_DONE_THIS_MONTH"] = int(current_month_backup.get("BACKUP_DONE_THIS_MONTH") or 0)
    row["BACKUP_DONE_GB_THIS_MONTH"] = float(current_month_backup.get("BACKUP_DONE_GB_THIS_MONTH") or 0)

    totals = {
        "DISCOVERED_FILES_TOTAL": 0,
        "DISCOVERED_GB_TOTAL": 0.0,
        "BACKUP_DONE_FILES_TOTAL": 0,
        "BACKUP_DONE_GB_TOTAL": 0.0,
        "BACKUP_PENDING_FILES_TOTAL": 0,
        "BACKUP_PENDING_GB_TOTAL": 0.0,
        "BACKUP_ERROR_FILES_TOTAL": 0,
        "BACKUP_ERROR_GB_TOTAL": 0.0,
        "PROCESSING_DONE_FILES_TOTAL": 0,
        "PROCESSING_DONE_GB_TOTAL": 0.0,
        "PROCESSING_PENDING_FILES_TOTAL": 0,
        "PROCESSING_PENDING_GB_TOTAL": 0.0,
        "PROCESSING_ERROR_FILES_TOTAL": 0,
        "PROCESSING_ERROR_GB_TOTAL": 0.0,
    }

    for monthly_row in monthly_rows:
        totals["DISCOVERED_FILES_TOTAL"] += int(monthly_row.get("NU_DISCOVERED_FILES") or 0)
        totals["DISCOVERED_GB_TOTAL"] += float(monthly_row.get("VL_DISCOVERED_GB") or 0)
        totals["BACKUP_DONE_FILES_TOTAL"] += int(monthly_row.get("NU_BACKUP_DONE_FILES") or 0)
        totals["BACKUP_DONE_GB_TOTAL"] += float(monthly_row.get("VL_BACKUP_DONE_GB") or 0)
        totals["BACKUP_PENDING_FILES_TOTAL"] += int(monthly_row.get("NU_BACKUP_PENDING_FILES") or 0)
        totals["BACKUP_PENDING_GB_TOTAL"] += float(monthly_row.get("VL_BACKUP_PENDING_GB") or 0)
        totals["BACKUP_ERROR_FILES_TOTAL"] += int(monthly_row.get("NU_BACKUP_ERROR_FILES") or 0)
        totals["BACKUP_ERROR_GB_TOTAL"] += float(monthly_row.get("VL_BACKUP_ERROR_GB") or 0)
        totals["PROCESSING_DONE_FILES_TOTAL"] += int(monthly_row.get("NU_PROCESSING_DONE_FILES") or 0)
        totals["PROCESSING_DONE_GB_TOTAL"] += float(monthly_row.get("VL_PROCESSING_DONE_GB") or 0)
        totals["PROCESSING_PENDING_FILES_TOTAL"] += int(monthly_row.get("NU_PROCESSING_PENDING_FILES") or 0)
        totals["PROCESSING_PENDING_GB_TOTAL"] += float(monthly_row.get("VL_PROCESSING_PENDING_GB") or 0)
        totals["PROCESSING_ERROR_FILES_TOTAL"] += int(monthly_row.get("NU_PROCESSING_ERROR_FILES") or 0)
        totals["PROCESSING_ERROR_GB_TOTAL"] += float(monthly_row.get("VL_PROCESSING_ERROR_GB") or 0)

    row["DISCOVERED_FILES_TOTAL"] = totals["DISCOVERED_FILES_TOTAL"]
    row["DISCOVERED_GB_TOTAL"] = round(totals["DISCOVERED_GB_TOTAL"], 2)
    row["BACKUP_DONE_FILES_TOTAL"] = totals["BACKUP_DONE_FILES_TOTAL"]
    row["BACKUP_DONE_GB_TOTAL"] = round(totals["BACKUP_DONE_GB_TOTAL"], 2)
    row["BACKUP_PENDING_FILES_TOTAL"] = totals["BACKUP_PENDING_FILES_TOTAL"]
    row["BACKUP_PENDING_GB_TOTAL"] = round(totals["BACKUP_PENDING_GB_TOTAL"], 2)
    row["BACKUP_QUEUE_FILES_TOTAL"] = int(row.get("NU_BACKUP_QUEUE_FILES_TOTAL") or 0)
    row["BACKUP_QUEUE_GB_TOTAL"] = float(row.get("VL_BACKUP_QUEUE_GB_TOTAL") or 0)
    row["BACKUP_ERROR_FILES_TOTAL"] = totals["BACKUP_ERROR_FILES_TOTAL"]
    row["BACKUP_ERROR_GB_TOTAL"] = round(totals["BACKUP_ERROR_GB_TOTAL"], 2)
    row["PROCESSING_DONE_FILES_TOTAL"] = totals["PROCESSING_DONE_FILES_TOTAL"]
    row["PROCESSING_DONE_GB_TOTAL"] = round(totals["PROCESSING_DONE_GB_TOTAL"], 2)
    row["PROCESSING_PENDING_FILES_TOTAL"] = totals["PROCESSING_PENDING_FILES_TOTAL"]
    row["PROCESSING_PENDING_GB_TOTAL"] = round(totals["PROCESSING_PENDING_GB_TOTAL"], 2)
    row["PROCESSING_QUEUE_FILES_TOTAL"] = int(row.get("NU_PROCESSING_QUEUE_FILES_TOTAL") or 0)
    row["PROCESSING_QUEUE_GB_TOTAL"] = float(row.get("VL_PROCESSING_QUEUE_GB_TOTAL") or 0)
    row["PROCESSING_ERROR_FILES_TOTAL"] = totals["PROCESSING_ERROR_FILES_TOTAL"]
    row["PROCESSING_ERROR_GB_TOTAL"] = round(totals["PROCESSING_ERROR_GB_TOTAL"], 2)
    row["FACT_SPECTRUM_TOTAL"] = int(row.get("NU_FACT_SPECTRUM_TOTAL") or 0)

    backup_yearly_breakdown, processing_yearly_breakdown = _build_host_yearly_breakdowns(monthly_rows)
    row["BACKUP_YEARLY_BREAKDOWN"] = backup_yearly_breakdown
    row["PROCESSING_YEARLY_BREAKDOWN"] = processing_yearly_breakdown

    row["LAST_FAILURE_AT"] = row.get("DT_LAST_ERROR_AT")
    row["LAST_FAILURE_REASON"] = row.get("NA_LAST_ERROR_SUMMARY")
    row["DISPLAY_LAST_FAILURE_AT"] = row.get("DT_LAST_FAIL") or row["LAST_FAILURE_AT"]
    row["GROUPED_PROCESSING_ERRORS"] = None

    row["MATCHED_RFDATA_EQUIPMENTS"] = None
    row["LOCATION_HISTORY"] = None

    _HOST_STATISTICS_CACHE[normalized_host_id] = {
        "expires_at": now + HOST_STATISTICS_CACHE_TTL_SECONDS,
        "payload": dict(row),
    }
    return row


def get_hosts(search=None, online_only=False):
    """Return the filtered host table used by the server dashboard.

    Unlike `get_all_hosts()`, this table is meant for richer server-wide
    navigation and therefore carries more operational columns. The data comes
    from the summary snapshot so the server page does not need to reconstruct
    those totals from the live queue tables on every request.
    """

    cache_key = _build_host_list_cache_key(
        online_only=online_only,
        search=search,
    )
    now = time.monotonic()
    cached = _SERVER_HOST_ROWS_CACHE.get(cache_key)

    if cached and cached["expires_at"] > now:
        return _clone_rows(cached["payload"])

    rows = _get_summary_host_rows(search=search, online_only=online_only)

    # Add display-friendly labels without paying for another SQL roundtrip.
    for r in rows:
        r["STATUS_LABEL"] = "Offline" if r["IS_OFFLINE"] else "Online"
        r["BUSY_LABEL"] = "Busy" if r["IS_BUSY"] else "Idle"
        pending_backup_gb = r.get("PENDING_BACKUP_GB")

        if pending_backup_gb is None:
            pending_backup_kb = r.get("VL_PENDING_BACKUP_KB") or 0
            pending_backup_gb = round(pending_backup_kb / 1024 / 1024, 2)
            r["PENDING_BACKUP_GB"] = pending_backup_gb
        else:
            pending_backup_gb = float(pending_backup_gb or 0)
            r["PENDING_BACKUP_GB"] = pending_backup_gb

        if r.get("VL_PENDING_BACKUP_KB"):
            r["PENDING_BACKUP_MB"] = round(r["VL_PENDING_BACKUP_KB"] / 1024, 2)
        else:
            r["PENDING_BACKUP_MB"] = round(pending_backup_gb * 1024, 2)

    _SERVER_HOST_ROWS_CACHE[cache_key] = {
        "expires_at": now + SERVER_HOST_ROWS_CACHE_TTL_SECONDS,
        "payload": _clone_rows(rows),
    }
    return rows
