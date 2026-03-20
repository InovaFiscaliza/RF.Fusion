"""Service layer for host and server operational views.

This module does two kinds of work:

- lightweight runtime inspection from the container/host point of view
- heavier historical aggregation from ``BPDATA``

The host page and the server page share most of their data rules, so keeping
them together avoids duplicating SQL and business meaning in multiple places.
"""

import os
import re
import shutil
import socket
import time
from datetime import datetime

from db import get_connection_bpdata as get_connection, get_connection_rfdata


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

_RFDATA_EQUIPMENT_CACHE = {
    "expires_at": 0.0,
    "payload": None,
}

_HOST_LOCATION_HISTORY_CACHE = {}

_HOST_STATISTICS_CACHE = {}

RUNTIME_OVERVIEW_CACHE_TTL_SECONDS = 300.0
SERVER_OVERVIEW_CACHE_TTL_SECONDS = 600.0
SERVER_SUMMARY_CACHE_TTL_SECONDS = 600.0
GROUPED_PROCESSING_ERRORS_CACHE_TTL_SECONDS = 600.0
GROUPED_BACKUP_ERRORS_CACHE_TTL_SECONDS = 600.0
RFDATA_EQUIPMENT_CACHE_TTL_SECONDS = 600.0
HOST_PROCESSING_ERRORS_CACHE_TTL_SECONDS = 300.0
HOST_BACKUP_ERRORS_CACHE_TTL_SECONDS = 300.0
HOST_LOCATION_HISTORY_CACHE_TTL_SECONDS = 600.0
HOST_STATISTICS_CACHE_TTL_SECONDS = 60.0

NON_ALNUM_RE = re.compile(r"[^a-z0-9]+", re.IGNORECASE)
CWSM_KEY_RE = re.compile(r"^(cwsm)(\d+)$", re.IGNORECASE)


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
    """Collapse volatile processing-error variants into a stable display key."""

    normalized = (message or "(Sem mensagem)").strip() or "(Sem mensagem)"

    if normalized == "(Sem mensagem)":
        return normalized

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

    return normalized


def _merge_grouped_processing_errors(rows):
    """Merge raw message buckets into canonical processing-error groups."""

    merged = {}

    for row in rows:
        raw_message = row.get("ERROR_MESSAGE") or "(Sem mensagem)"
        error_count = int(row.get("ERROR_COUNT") or 0)
        canonical_message = _canonicalize_processing_error_message(raw_message)

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
        raw_message = row.get("ERROR_MESSAGE") or "(Sem mensagem)"
        error_count = int(row.get("ERROR_COUNT") or 0)
        canonical_message = _canonicalize_backup_error_message(raw_message)

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


def _check_appanalise_status():
    """Run a short TCP preflight against the configured appAnalise endpoint."""

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

    if not host or not port:
        status["error"] = "appAnalise endpoint is not configured"
        return status

    started_at = time.perf_counter()

    try:
        with socket.create_connection((host, port), timeout=timeout):
            status["online"] = True
            status["latency_ms"] = round((time.perf_counter() - started_at) * 1000, 1)
    except OSError as exc:
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
    """Build reusable WHERE fragments for host list queries."""

    where_clauses = []
    params = []

    if online_only:
        where_clauses.append(f"{prefix}IS_OFFLINE = 0")

    if search:
        where_clauses.append(f"{prefix}NA_HOST_NAME LIKE %s")
        params.append(f"%{search}%")

    return where_clauses, params


def _normalize_station_key(value):
    """Normalize host/equipment identifiers for tolerant matching."""

    if value is None:
        return ""

    return NON_ALNUM_RE.sub("", str(value).strip().lower())


def _build_cwsm_signature(normalized_key):
    """Collapse CelPlan receiver variants into a stable comparison key."""

    match = CWSM_KEY_RE.fullmatch(normalized_key or "")

    if not match:
        return None

    digits = match.group(2)

    if len(digits) < 6:
        return None

    return f"cwsm{digits[:3]}{digits[-3:]}"


def _build_station_alias_keys(value):
    """Build normalized aliases used to reconcile host/equipment identifiers."""

    normalized_key = _normalize_station_key(value)

    if not normalized_key:
        return set()

    aliases = {normalized_key}
    cwsm_signature = _build_cwsm_signature(normalized_key)

    if cwsm_signature:
        aliases.add(cwsm_signature)

    return aliases


def _load_rfdata_equipments():
    """Load spectrum equipment once for host/locality reconciliation."""

    conn = get_connection_rfdata()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT
            e.ID_EQUIPMENT,
            e.NA_EQUIPMENT
        FROM DIM_SPECTRUM_EQUIPMENT e
        JOIN FACT_SPECTRUM f
            ON f.FK_EQUIPMENT = e.ID_EQUIPMENT
        ORDER BY e.NA_EQUIPMENT
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def _get_cached_rfdata_equipments():
    """Reuse the RFDATA equipment list for a short period."""

    now = time.monotonic()

    if (
        _RFDATA_EQUIPMENT_CACHE["payload"] is not None
        and _RFDATA_EQUIPMENT_CACHE["expires_at"] > now
    ):
        return _RFDATA_EQUIPMENT_CACHE["payload"]

    payload = _load_rfdata_equipments()
    _RFDATA_EQUIPMENT_CACHE["payload"] = payload
    _RFDATA_EQUIPMENT_CACHE["expires_at"] = now + RFDATA_EQUIPMENT_CACHE_TTL_SECONDS
    return payload


def _equipment_matches_host(host_name, equipment_name):
    """Decide whether a spectrum equipment plausibly belongs to one host."""

    normalized_host = _normalize_station_key(host_name)
    normalized_equipment = _normalize_station_key(equipment_name)

    if not normalized_host or not normalized_equipment:
        return False

    if normalized_host == normalized_equipment:
        return True

    host_aliases = _build_station_alias_keys(host_name)
    equipment_aliases = _build_station_alias_keys(equipment_name)

    if host_aliases & equipment_aliases:
        return True

    for host_alias in host_aliases:
        for equipment_alias in equipment_aliases:
            if (
                host_alias
                and equipment_alias
                and (
                    host_alias.startswith(equipment_alias)
                    or equipment_alias.startswith(host_alias)
                )
            ):
                return True

    return False


def _get_host_equipment_matches(host_name):
    """Return all RFDATA equipments that reconcile to one host name."""

    matches = []

    for row in _get_cached_rfdata_equipments():
        equipment_name = row.get("NA_EQUIPMENT")

        if not equipment_name:
            continue

        if _equipment_matches_host(host_name, equipment_name):
            matches.append(
                {
                    "ID_EQUIPMENT": int(row["ID_EQUIPMENT"]),
                    "NA_EQUIPMENT": str(equipment_name),
                }
            )

    return matches


def _get_host_location_history(host_name):
    """Return the locality timeline observed for one host in RFDATA."""

    normalized_host_name = str(host_name or "").strip()

    if not normalized_host_name:
        return {
            "equipment_matches": [],
            "location_history": [],
        }

    now = time.monotonic()
    cached = _HOST_LOCATION_HISTORY_CACHE.get(normalized_host_name)

    if cached and cached["expires_at"] > now:
        return cached["payload"]

    equipment_matches = _get_host_equipment_matches(host_name)

    if not equipment_matches:
        payload = {
            "equipment_matches": [],
            "location_history": [],
        }
        _HOST_LOCATION_HISTORY_CACHE[normalized_host_name] = {
            "expires_at": now + HOST_LOCATION_HISTORY_CACHE_TTL_SECONDS,
            "payload": payload,
        }
        return payload

    equipment_ids = [row["ID_EQUIPMENT"] for row in equipment_matches]
    placeholders = ", ".join(["%s"] * len(equipment_ids))
    site_differs_from_county_sql = """
        NOT (
            COALESCE(CONVERT(s.NA_SITE USING utf8mb4) COLLATE utf8mb4_unicode_ci, '')
            <=>
            COALESCE(CONVERT(c.NA_COUNTY USING utf8mb4) COLLATE utf8mb4_unicode_ci, '')
        )
    """
    locality_display_sql = """
        TRIM(
            CONCAT(
                COALESCE(NULLIF(s.NA_SITE, ''), NULLIF(d.NA_DISTRICT, ''), c.NA_COUNTY, CONCAT('Site ', s.ID_SITE)),
                CASE
                    WHEN c.NA_COUNTY IS NOT NULL
                     AND (
                        s.NA_SITE IS NULL
                        OR s.NA_SITE = ''
                        OR {site_differs_from_county_sql}
                     )
                    THEN CONCAT(' · ', c.NA_COUNTY)
                    ELSE ''
                END,
                CASE
                    WHEN st.LC_STATE IS NOT NULL THEN CONCAT('/', st.LC_STATE)
                    ELSE ''
                END
            )
        )
    """.format(site_differs_from_county_sql=site_differs_from_county_sql)

    conn = get_connection_rfdata()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            s.ID_SITE,
            {locality_display_sql} AS LOCALITY_LABEL,
            c.NA_COUNTY AS COUNTY_NAME,
            st.NA_STATE AS STATE_NAME,
            st.LC_STATE AS STATE_CODE,
            MIN(f.DT_TIME_START) AS FIRST_SEEN_AT,
            MAX(f.DT_TIME_END) AS LAST_SEEN_AT,
            COUNT(*) AS SPECTRUM_COUNT
        FROM FACT_SPECTRUM f
        JOIN DIM_SPECTRUM_SITE s
            ON s.ID_SITE = f.FK_SITE
        LEFT JOIN DIM_SITE_DISTRICT d
            ON d.ID_DISTRICT = s.FK_DISTRICT
        LEFT JOIN DIM_SITE_COUNTY c
            ON c.ID_COUNTY = s.FK_COUNTY
        LEFT JOIN DIM_SITE_STATE st
            ON st.ID_STATE = s.FK_STATE
        WHERE f.FK_EQUIPMENT IN ({placeholders})
        GROUP BY
            s.ID_SITE,
            s.NA_SITE,
            d.NA_DISTRICT,
            c.NA_COUNTY,
            st.NA_STATE,
            st.LC_STATE
        ORDER BY
            FIRST_SEEN_AT DESC,
            LAST_SEEN_AT DESC
        """,
        equipment_ids,
    )
    location_history = cur.fetchall()
    conn.close()

    for row in location_history:
        row["SPECTRUM_COUNT"] = int(row.get("SPECTRUM_COUNT") or 0)

    payload = {
        "equipment_matches": equipment_matches,
        "location_history": location_history,
    }
    _HOST_LOCATION_HISTORY_CACHE[normalized_host_name] = {
        "expires_at": now + HOST_LOCATION_HISTORY_CACHE_TTL_SECONDS,
        "payload": payload,
    }
    return payload


def _get_host_fact_spectrum_total(host_name):
    """Count spectra in RFDATA for the equipments reconciled to one host."""

    equipment_matches = _get_host_equipment_matches(host_name)

    if not equipment_matches:
        return 0

    equipment_ids = [row["ID_EQUIPMENT"] for row in equipment_matches]
    placeholders = ", ".join(["%s"] * len(equipment_ids))

    conn = get_connection_rfdata()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT COUNT(*) AS FACT_SPECTRUM_TOTAL
        FROM FACT_SPECTRUM
        WHERE FK_EQUIPMENT IN ({placeholders})
        """,
        equipment_ids,
    )
    row = cur.fetchone() or {}
    conn.close()
    return int(row.get("FACT_SPECTRUM_TOTAL") or 0)


def _get_grouped_processing_errors(cur, where_clause="", params=None):
    """Fetch raw processing-failure buckets grouped by literal message."""

    query = """
        SELECT
            COALESCE(NULLIF(TRIM(NA_MESSAGE), ''), '(Sem mensagem)') AS ERROR_MESSAGE,
            COUNT(*) AS ERROR_COUNT
        FROM FILE_TASK_HISTORY
        WHERE NU_STATUS_PROCESSING = -1
    """

    if where_clause:
        query += f" AND {where_clause}"

    query += """
        GROUP BY COALESCE(NULLIF(TRIM(NA_MESSAGE), ''), '(Sem mensagem)')
        ORDER BY ERROR_COUNT DESC, ERROR_MESSAGE ASC
    """

    cur.execute(query, params or [])
    return cur.fetchall()


def _get_grouped_backup_errors(cur, where_clause="", params=None):
    """Fetch raw backup-failure buckets grouped by literal message."""

    query = """
        SELECT
            COALESCE(NULLIF(TRIM(NA_MESSAGE), ''), '(Sem mensagem)') AS ERROR_MESSAGE,
            COUNT(*) AS ERROR_COUNT
        FROM FILE_TASK_HISTORY
        WHERE NU_STATUS_BACKUP = -1
    """

    if where_clause:
        query += f" AND {where_clause}"

    query += """
        GROUP BY COALESCE(NULLIF(TRIM(NA_MESSAGE), ''), '(Sem mensagem)')
        ORDER BY ERROR_COUNT DESC, ERROR_MESSAGE ASC
    """

    cur.execute(query, params or [])
    return cur.fetchall()


def get_server_processing_error_overview():
    """Return grouped processing errors for the global dashboard on demand.

    The server page keeps this query out of the initial HTML render because the
    grouped scan can be one of the slowest parts of the dashboard. Operators
    only need it after expanding the diagnostic panel.
    """

    now = time.monotonic()

    if (
        _GROUPED_PROCESSING_ERRORS_CACHE["payload"] is not None
        and _GROUPED_PROCESSING_ERRORS_CACHE["expires_at"] > now
    ):
        return _GROUPED_PROCESSING_ERRORS_CACHE["payload"]

    conn = get_connection()
    cur = conn.cursor()
    rows = _get_grouped_processing_errors(cur)
    conn.close()

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
    """Return grouped backup errors for the global dashboard on demand."""

    now = time.monotonic()

    if (
        _GROUPED_BACKUP_ERRORS_CACHE["payload"] is not None
        and _GROUPED_BACKUP_ERRORS_CACHE["expires_at"] > now
    ):
        return _GROUPED_BACKUP_ERRORS_CACHE["payload"]

    conn = get_connection()
    cur = conn.cursor()
    rows = _get_grouped_backup_errors(cur)
    conn.close()

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
    """Return the heavy global FILE_TASK_HISTORY aggregates on demand."""

    now = time.monotonic()

    if _SERVER_SUMMARY_CACHE["payload"] and _SERVER_SUMMARY_CACHE["expires_at"] > now:
        return _SERVER_SUMMARY_CACHE["payload"]

    conn = get_connection()
    cur = conn.cursor()

    current_month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if current_month_start.month == 12:
        next_month_start = current_month_start.replace(year=current_month_start.year + 1, month=1)
    else:
        next_month_start = current_month_start.replace(month=current_month_start.month + 1)

    cur.execute(
        """
        SELECT
            COUNT(*) AS BACKUP_DONE_THIS_MONTH,
            ROUND(COALESCE(SUM(VL_FILE_SIZE_KB), 0) / 1024 / 1024, 2) AS BACKUP_DONE_GB_THIS_MONTH
        FROM FILE_TASK_HISTORY
        WHERE NU_STATUS_BACKUP = 0
          AND DT_BACKUP >= %s
          AND DT_BACKUP < %s
        """,
        (
            current_month_start.strftime("%Y-%m-%d %H:%M:%S"),
            next_month_start.strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    monthly_summary = cur.fetchone() or {}

    cur.execute(
        """
        SELECT
            COUNT(*) AS DISCOVERED_FILES_TOTAL,
            ROUND(COALESCE(SUM(VL_FILE_SIZE_KB), 0) / 1024 / 1024, 2) AS DISCOVERED_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_BACKUP = 0 THEN 1 ELSE 0 END) AS BACKUP_DONE_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_BACKUP = 0 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS BACKUP_DONE_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_BACKUP = 1 THEN 1 ELSE 0 END) AS BACKUP_PENDING_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_BACKUP = 1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS BACKUP_PENDING_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_BACKUP = -1 THEN 1 ELSE 0 END) AS BACKUP_ERROR_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_BACKUP = -1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS BACKUP_ERROR_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_PROCESSING = 0 THEN 1 ELSE 0 END) AS PROCESSING_DONE_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_PROCESSING = 0 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS PROCESSING_DONE_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_PROCESSING = 1 THEN 1 ELSE 0 END) AS PROCESSING_PENDING_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_PROCESSING = 1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS PROCESSING_PENDING_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_PROCESSING = -1 THEN 1 ELSE 0 END) AS PROCESSING_ERROR_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_PROCESSING = -1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS PROCESSING_ERROR_GB_TOTAL
        FROM FILE_TASK_HISTORY
        """
    )
    global_summary = cur.fetchone() or {}
    conn.close()

    spectrum_summary = {}
    try:
        rf_conn = get_connection_rfdata()
        rf_cur = rf_conn.cursor()
        rf_cur.execute(
            """
            SELECT COUNT(*) AS FACT_SPECTRUM_TOTAL
            FROM FACT_SPECTRUM
            """
        )
        spectrum_summary = rf_cur.fetchone() or {}
        rf_conn.close()
    except Exception:
        spectrum_summary = {}

    payload = {
        "CURRENT_MONTH_LABEL": current_month_start.strftime("%Y-%m"),
        "BACKUP_DONE_THIS_MONTH": int(monthly_summary.get("BACKUP_DONE_THIS_MONTH") or 0),
        "BACKUP_DONE_GB_THIS_MONTH": float(monthly_summary.get("BACKUP_DONE_GB_THIS_MONTH") or 0),
        "DISCOVERED_FILES_TOTAL": int(global_summary.get("DISCOVERED_FILES_TOTAL") or 0),
        "DISCOVERED_GB_TOTAL": float(global_summary.get("DISCOVERED_GB_TOTAL") or 0),
        "BACKUP_DONE_FILES_TOTAL": int(global_summary.get("BACKUP_DONE_FILES_TOTAL") or 0),
        "BACKUP_DONE_GB_TOTAL": float(global_summary.get("BACKUP_DONE_GB_TOTAL") or 0),
        "BACKUP_PENDING_FILES_TOTAL": int(global_summary.get("BACKUP_PENDING_FILES_TOTAL") or 0),
        "BACKUP_PENDING_GB_TOTAL": float(global_summary.get("BACKUP_PENDING_GB_TOTAL") or 0),
        "BACKUP_ERROR_FILES_TOTAL": int(global_summary.get("BACKUP_ERROR_FILES_TOTAL") or 0),
        "BACKUP_ERROR_GB_TOTAL": float(global_summary.get("BACKUP_ERROR_GB_TOTAL") or 0),
        "PROCESSING_DONE_FILES_TOTAL": int(global_summary.get("PROCESSING_DONE_FILES_TOTAL") or 0),
        "PROCESSING_DONE_GB_TOTAL": float(global_summary.get("PROCESSING_DONE_GB_TOTAL") or 0),
        "FACT_SPECTRUM_TOTAL": int(spectrum_summary.get("FACT_SPECTRUM_TOTAL") or 0),
        "PROCESSING_PENDING_FILES_TOTAL": int(global_summary.get("PROCESSING_PENDING_FILES_TOTAL") or 0),
        "PROCESSING_PENDING_GB_TOTAL": float(global_summary.get("PROCESSING_PENDING_GB_TOTAL") or 0),
        "PROCESSING_ERROR_FILES_TOTAL": int(global_summary.get("PROCESSING_ERROR_FILES_TOTAL") or 0),
        "PROCESSING_ERROR_GB_TOTAL": float(global_summary.get("PROCESSING_ERROR_GB_TOTAL") or 0),
    }

    _SERVER_SUMMARY_CACHE["payload"] = payload
    _SERVER_SUMMARY_CACHE["expires_at"] = now + SERVER_SUMMARY_CACHE_TTL_SECONDS
    return payload


def get_host_processing_error_overview(host_id):
    """Return grouped processing errors for one host on demand."""

    cache_key = int(host_id)
    now = time.monotonic()
    cached = _HOST_PROCESSING_ERRORS_CACHE.get(cache_key)

    if cached and cached["expires_at"] > now:
        return cached["payload"]

    conn = get_connection()
    cur = conn.cursor()
    rows = _get_grouped_processing_errors(
        cur,
        where_clause="FK_HOST = %s",
        params=[cache_key],
    )
    conn.close()

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
    """Return grouped backup errors for one host on demand."""

    cache_key = int(host_id)
    now = time.monotonic()
    cached = _HOST_BACKUP_ERRORS_CACHE.get(cache_key)

    if cached and cached["expires_at"] > now:
        return cached["payload"]

    conn = get_connection()
    cur = conn.cursor()
    rows = _get_grouped_backup_errors(
        cur,
        where_clause="FK_HOST = %s",
        params=[cache_key],
    )
    conn.close()

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
    """Return reconciled locality history for one host on demand."""

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT NA_HOST_NAME
        FROM BPDATA.HOST
        WHERE ID_HOST = %s
        """,
        (int(host_id),),
    )
    row = cur.fetchone() or {}
    conn.close()

    host_name = row.get("NA_HOST_NAME")

    if not host_name:
        return {
            "equipment_matches": [],
            "location_history": [],
        }

    return _get_host_location_history(host_name)


def get_all_hosts(online_only=False, search=None):
    """Return the host picker list used by the host page."""

    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT ID_HOST, NA_HOST_NAME, IS_OFFLINE
        FROM BPDATA.HOST
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


def get_server_overview(online_only=False, search=None):
    """Return the global dashboard plus the filtered host table.

    Important behavior:
        - the summary cards remain global
        - ``online_only`` and ``search`` affect only ``HOST_ROWS``
        - grouped processing diagnostics are loaded on demand by the page

    This separation avoids hiding real server totals just because the operator
    filtered the table for navigation.
    """

    now = time.monotonic()

    if _SERVER_OVERVIEW_CACHE["payload"] and _SERVER_OVERVIEW_CACHE["expires_at"] > now:
        overview = dict(_SERVER_OVERVIEW_CACHE["payload"])
    else:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                COUNT(*) AS TOTAL_HOSTS,
                SUM(CASE WHEN IS_OFFLINE = 0 THEN 1 ELSE 0 END) AS ONLINE_HOSTS,
                SUM(CASE WHEN IS_OFFLINE = 1 THEN 1 ELSE 0 END) AS OFFLINE_HOSTS,
                SUM(CASE WHEN IS_BUSY = 1 THEN 1 ELSE 0 END) AS BUSY_HOSTS
            FROM BPDATA.HOST
            """
        )
        host_summary = cur.fetchone() or {}
        conn.close()

        current_month_label = datetime.utcnow().strftime("%Y-%m")

        overview = {
            "CURRENT_MONTH_LABEL": current_month_label,
            "TOTAL_HOSTS": int(host_summary.get("TOTAL_HOSTS") or 0),
            "ONLINE_HOSTS": int(host_summary.get("ONLINE_HOSTS") or 0),
            "OFFLINE_HOSTS": int(host_summary.get("OFFLINE_HOSTS") or 0),
            "BUSY_HOSTS": int(host_summary.get("BUSY_HOSTS") or 0),
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

    # Keep the lower table filterable without mutating the global dashboard.
    overview["HOST_ROWS"] = get_hosts(search=search, online_only=online_only)
    runtime_overview = _get_runtime_overview()
    overview["SERVER_MEMORY"] = runtime_overview["memory"]
    overview["REPOSFI_USAGE"] = runtime_overview["reposfi"]
    overview["APP_ANALISE_STATUS"] = runtime_overview["appanalise"]

    return overview


def get_host_statistics(host_id):
    """Return the detailed operational picture for one station.

    The result mixes:
        - current counters from ``HOST``
        - monthly bandwidth-oriented backup totals from ``DT_BACKUP``
        - historical annual breakdowns keyed by ``DT_FILE_CREATED``

    That split is deliberate because each timestamp answers a different
    operational question.
    """

    normalized_host_id = int(host_id)
    now = time.monotonic()
    cached = _HOST_STATISTICS_CACHE.get(normalized_host_id)

    if cached and cached["expires_at"] > now:
        return dict(cached["payload"])

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            ID_HOST,
            NA_HOST_NAME,
            NA_HOST_ADDRESS,
            NA_HOST_PORT,
            IS_OFFLINE,
            IS_BUSY,
            NU_PID,
            DT_BUSY,
            DT_LAST_FAIL,
            DT_LAST_CHECK,
            NU_HOST_CHECK_ERROR,
            DT_LAST_DISCOVERY,
            NU_DONE_FILE_DISCOVERY_TASKS,
            NU_ERROR_FILE_DISCOVERY_TASKS,
            DT_LAST_BACKUP,
            NU_PENDING_FILE_BACKUP_TASKS,
            NU_DONE_FILE_BACKUP_TASKS,
            NU_ERROR_FILE_BACKUP_TASKS,
            VL_PENDING_BACKUP_KB,
            VL_DONE_BACKUP_KB,
            DT_LAST_PROCESSING,
            NU_PENDING_FILE_PROCESS_TASKS,
            NU_DONE_FILE_PROCESS_TASKS,
            NU_ERROR_FILE_PROCESS_TASKS,
            NU_HOST_FILES
        FROM BPDATA.HOST
        WHERE ID_HOST = %s
    """, (normalized_host_id,))

    row = cur.fetchone()

    if not row:
        conn.close()
        return None

    # Convert the host table counters to GB so the template can stay simple.
    row["PENDING_GB"] = round((row["VL_PENDING_BACKUP_KB"] or 0) / 1024 /1024, 2)
    row["DONE_GB"] = round((row["VL_DONE_BACKUP_KB"] or 0) / 1024 / 1024, 2)

    current_month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if current_month_start.month == 12:
        next_month_start = current_month_start.replace(year=current_month_start.year + 1, month=1)
    else:
        next_month_start = current_month_start.replace(month=current_month_start.month + 1)

    # Monthly backup totals follow DT_BACKUP because that is the bandwidth event.
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
            normalized_host_id,
            current_month_start.strftime("%Y-%m-%d %H:%M:%S"),
            next_month_start.strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    monthly_summary = cur.fetchone() or {}

    row["CURRENT_MONTH_LABEL"] = current_month_start.strftime("%Y-%m")
    row["BACKUP_DONE_THIS_MONTH"] = int(monthly_summary.get("BACKUP_DONE_THIS_MONTH") or 0)
    row["BACKUP_DONE_GB_THIS_MONTH"] = float(monthly_summary.get("BACKUP_DONE_GB_THIS_MONTH") or 0)

    # Global station summaries come from FILE_TASK_HISTORY because it preserves
    # the final operational state of each discovered file.
    cur.execute(
        """
        SELECT
            COUNT(*) AS DISCOVERED_FILES_TOTAL,
            ROUND(COALESCE(SUM(VL_FILE_SIZE_KB), 0) / 1024 / 1024, 2) AS DISCOVERED_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_BACKUP = 0 THEN 1 ELSE 0 END) AS BACKUP_DONE_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_BACKUP = 0 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS BACKUP_DONE_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_BACKUP = 1 THEN 1 ELSE 0 END) AS BACKUP_PENDING_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_BACKUP = 1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS BACKUP_PENDING_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_BACKUP = -1 THEN 1 ELSE 0 END) AS BACKUP_ERROR_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_BACKUP = -1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS BACKUP_ERROR_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_PROCESSING = 0 THEN 1 ELSE 0 END) AS PROCESSING_DONE_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_PROCESSING = 0 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS PROCESSING_DONE_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_PROCESSING = 1 THEN 1 ELSE 0 END) AS PROCESSING_PENDING_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_PROCESSING = 1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS PROCESSING_PENDING_GB_TOTAL,
            SUM(CASE WHEN NU_STATUS_PROCESSING = -1 THEN 1 ELSE 0 END) AS PROCESSING_ERROR_FILES_TOTAL,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_PROCESSING = -1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS PROCESSING_ERROR_GB_TOTAL
        FROM FILE_TASK_HISTORY
        WHERE FK_HOST = %s
        """,
        (normalized_host_id,),
    )
    global_summary = cur.fetchone() or {}

    row["DISCOVERED_FILES_TOTAL"] = int(global_summary.get("DISCOVERED_FILES_TOTAL") or 0)
    row["DISCOVERED_GB_TOTAL"] = float(global_summary.get("DISCOVERED_GB_TOTAL") or 0)
    row["BACKUP_DONE_FILES_TOTAL"] = int(global_summary.get("BACKUP_DONE_FILES_TOTAL") or 0)
    row["BACKUP_DONE_GB_TOTAL"] = float(global_summary.get("BACKUP_DONE_GB_TOTAL") or 0)
    row["BACKUP_PENDING_FILES_TOTAL"] = int(global_summary.get("BACKUP_PENDING_FILES_TOTAL") or 0)
    row["BACKUP_PENDING_GB_TOTAL"] = float(global_summary.get("BACKUP_PENDING_GB_TOTAL") or 0)
    row["BACKUP_ERROR_FILES_TOTAL"] = int(global_summary.get("BACKUP_ERROR_FILES_TOTAL") or 0)
    row["BACKUP_ERROR_GB_TOTAL"] = float(global_summary.get("BACKUP_ERROR_GB_TOTAL") or 0)
    row["PROCESSING_DONE_FILES_TOTAL"] = int(global_summary.get("PROCESSING_DONE_FILES_TOTAL") or 0)
    row["PROCESSING_DONE_GB_TOTAL"] = float(global_summary.get("PROCESSING_DONE_GB_TOTAL") or 0)
    row["PROCESSING_PENDING_FILES_TOTAL"] = int(global_summary.get("PROCESSING_PENDING_FILES_TOTAL") or 0)
    row["PROCESSING_PENDING_GB_TOTAL"] = float(global_summary.get("PROCESSING_PENDING_GB_TOTAL") or 0)
    row["PROCESSING_ERROR_FILES_TOTAL"] = int(global_summary.get("PROCESSING_ERROR_FILES_TOTAL") or 0)
    row["PROCESSING_ERROR_GB_TOTAL"] = float(global_summary.get("PROCESSING_ERROR_GB_TOTAL") or 0)

    # Spectrum totals enrich the host view, but the page should still render if
    # RFDATA is temporarily unavailable.
    try:
        row["FACT_SPECTRUM_TOTAL"] = _get_host_fact_spectrum_total(row.get("NA_HOST_NAME"))
    except Exception:
        row["FACT_SPECTRUM_TOTAL"] = 0

    # Annual backup reporting uses the file creation metadata, not the backup
    # timestamp, so operators can understand the age of the cataloged files.
    cur.execute(
        """
        SELECT
            YEAR(DT_FILE_CREATED) AS REFERENCE_YEAR,
            COUNT(*) AS DISCOVERED_FILES,
            ROUND(COALESCE(SUM(VL_FILE_SIZE_KB), 0) / 1024 / 1024, 2) AS DISCOVERED_GB,
            SUM(CASE WHEN NU_STATUS_BACKUP = 0 THEN 1 ELSE 0 END) AS BACKUP_DONE_FILES,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_BACKUP = 0 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS BACKUP_DONE_GB,
            SUM(CASE WHEN NU_STATUS_BACKUP = 1 THEN 1 ELSE 0 END) AS BACKUP_PENDING_FILES,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_BACKUP = 1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS BACKUP_PENDING_GB,
            SUM(CASE WHEN NU_STATUS_BACKUP = -1 THEN 1 ELSE 0 END) AS BACKUP_ERROR_FILES,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_BACKUP = -1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS BACKUP_ERROR_GB
        FROM FILE_TASK_HISTORY
        WHERE FK_HOST = %s
          AND DT_FILE_CREATED IS NOT NULL
        GROUP BY YEAR(DT_FILE_CREATED)
        ORDER BY REFERENCE_YEAR DESC
        """,
        (normalized_host_id,),
    )
    row["BACKUP_YEARLY_BREAKDOWN"] = cur.fetchall()

    # Processing follows the same yearly reference as backup for comparison.
    cur.execute(
        """
        SELECT
            YEAR(DT_FILE_CREATED) AS REFERENCE_YEAR,
            SUM(CASE WHEN NU_STATUS_PROCESSING = 0 THEN 1 ELSE 0 END) AS PROCESSING_DONE_FILES,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_PROCESSING = 0 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS PROCESSING_DONE_GB,
            SUM(CASE WHEN NU_STATUS_PROCESSING = 1 THEN 1 ELSE 0 END) AS PROCESSING_PENDING_FILES,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_PROCESSING = 1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS PROCESSING_PENDING_GB,
            SUM(CASE WHEN NU_STATUS_PROCESSING = -1 THEN 1 ELSE 0 END) AS PROCESSING_ERROR_FILES,
            ROUND(COALESCE(SUM(CASE WHEN NU_STATUS_PROCESSING = -1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS PROCESSING_ERROR_GB
        FROM FILE_TASK_HISTORY
        WHERE FK_HOST = %s
          AND DT_FILE_CREATED IS NOT NULL
        GROUP BY YEAR(DT_FILE_CREATED)
        ORDER BY REFERENCE_YEAR DESC
        """,
        (normalized_host_id,),
    )
    row["PROCESSING_YEARLY_BREAKDOWN"] = cur.fetchall()

    cur.execute(
        """
        SELECT
            FAILURE_AT,
            FAILURE_REASON
        FROM (
            SELECT
                DT_HOST_TASK AS FAILURE_AT,
                NA_MESSAGE AS FAILURE_REASON
            FROM HOST_TASK
            WHERE FK_HOST = %s
              AND NU_STATUS = -1
              AND NA_MESSAGE IS NOT NULL

            UNION ALL

            SELECT
                DT_FILE_TASK AS FAILURE_AT,
                NA_MESSAGE AS FAILURE_REASON
            FROM FILE_TASK
            WHERE FK_HOST = %s
              AND NU_STATUS = -1
              AND NA_MESSAGE IS NOT NULL
        ) AS failures
        ORDER BY FAILURE_AT DESC
        LIMIT 1
        """,
        (normalized_host_id, normalized_host_id),
    )
    last_failure = cur.fetchone() or {}
    row["LAST_FAILURE_AT"] = last_failure.get("FAILURE_AT")
    row["LAST_FAILURE_REASON"] = last_failure.get("FAILURE_REASON")
    row["DISPLAY_LAST_FAILURE_AT"] = row.get("DT_LAST_FAIL") or row["LAST_FAILURE_AT"]
    row["GROUPED_PROCESSING_ERRORS"] = None

    conn.close()

    row["MATCHED_RFDATA_EQUIPMENTS"] = None
    row["LOCATION_HISTORY"] = None

    _HOST_STATISTICS_CACHE[normalized_host_id] = {
        "expires_at": now + HOST_STATISTICS_CACHE_TTL_SECONDS,
        "payload": dict(row),
    }
    return row


def get_hosts(search=None, online_only=False):
    """Return the filtered host table used by the server dashboard."""

    conn = get_connection()
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
            VL_PENDING_BACKUP_KB
        FROM BPDATA.HOST
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

    # Add display-friendly labels without paying for another SQL roundtrip.
    for r in rows:
        r["STATUS_LABEL"] = "Offline" if r["IS_OFFLINE"] else "Online"
        r["BUSY_LABEL"] = "Busy" if r["IS_BUSY"] else "Idle"
        r["PENDING_BACKUP_GB"] = round((r["VL_PENDING_BACKUP_KB"] or 0) / 1024 / 1024, 2)

        if r["VL_PENDING_BACKUP_KB"]:
            r["PENDING_BACKUP_MB"] = round(r["VL_PENDING_BACKUP_KB"] / 1024, 2)
        else:
            r["PENDING_BACKUP_MB"] = 0

    return rows
