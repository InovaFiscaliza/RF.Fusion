"""Station-map helpers used by the WebFusion landing page.

The map is intentionally split between:

- fast point loading from ``RFDATA``
- later host enrichment using ``BPDATA``

That keeps the home page responsive while still allowing quick actions on the
station popup.
"""

import re
import time

from db import (
    get_connection_bpdata,
    get_connection_rfdata,
)


POINT_WKT_RE = re.compile(
    r"POINT\s*\(\s*([-\d\.]+)\s+([-\d\.]+)\s*\)",
    re.IGNORECASE,
)
NON_ALNUM_RE = re.compile(r"[^a-z0-9]+", re.IGNORECASE)
CWSM_KEY_RE = re.compile(r"^(cwsm)(\d+)$", re.IGNORECASE)

MAP_CACHE_TTL_SECONDS = 300

_HOST_INDEX_CACHE = {"expires_at": 0.0, "value": None}
_MAP_POINTS_CACHE = {"expires_at": 0.0, "value": None}
_SITE_DETAILS_CACHE = {}


def _parse_point_wkt(value):
    """
    Parse a WKT point in the form `POINT (lon lat)`.

    The map layer expects latitude/longitude numbers. Returning `None` allows
    the caller to skip malformed rows without killing the whole landing page.
    """
    if value is None:
        return None

    text = value.decode("utf-8", errors="ignore") if isinstance(value, bytes) else str(value)
    match = POINT_WKT_RE.search(text.strip())

    if not match:
        return None

    longitude = float(match.group(1))
    latitude = float(match.group(2))
    return latitude, longitude


def _normalize_station_key(value):
    """
    Normalize host/equipment identifiers for tolerant matching.
    """
    if value is None:
        return ""

    return NON_ALNUM_RE.sub("", str(value).strip().lower())


def _build_cwsm_signature(normalized_key):
    """
    Collapse CelPlan receiver variants into a stable comparison key.

    Why this exists:
        CelPlan hosts are registered in BPDATA with names like:
            CWSM211001
            CWSM220040

        But processed spectra may surface receiver identifiers such as:
            cwsm21100001
            cwsm22010040

        A safe compromise is to compare:
            - the CWSM prefix
            - the first 3 digits
            - the last 3 digits

        This keeps the inference strict enough to avoid broad substring
        matching while still reconciling the known CelPlan naming variants.
    """
    match = CWSM_KEY_RE.fullmatch(normalized_key or "")

    if not match:
        return None

    digits = match.group(2)

    if len(digits) < 6:
        return None

    return f"cwsm{digits[:3]}{digits[-3:]}"


def _build_station_alias_keys(value):
    """
    Build normalized aliases used to reconcile equipment and host identifiers.
    """
    normalized_key = _normalize_station_key(value)

    if not normalized_key:
        return set()

    aliases = {normalized_key}
    cwsm_signature = _build_cwsm_signature(normalized_key)

    if cwsm_signature:
        aliases.add(cwsm_signature)

    return aliases


def _load_hosts_index():
    """
    Load HOST rows once and index them by normalized host name.
    """
    conn = get_connection_bpdata()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            ID_HOST,
            NA_HOST_NAME,
            IS_OFFLINE
        FROM HOST
        ORDER BY NA_HOST_NAME
        """
    )

    rows = cur.fetchall()
    conn.close()

    raw_index = {}
    normalized_index = {}

    for row in rows:
        if not row.get("NA_HOST_NAME"):
            continue

        host = {
            "host_id": int(row["ID_HOST"]),
            "host_name": str(row["NA_HOST_NAME"]),
            "is_offline": bool(row["IS_OFFLINE"]),
        }
        raw_key = host["host_name"].strip().lower()
        raw_index[raw_key] = host

        for alias_key in _build_station_alias_keys(host["host_name"]):
            normalized_index.setdefault(alias_key, []).append(host)

    return {
        "raw": raw_index,
        "normalized": normalized_index,
    }


def _get_cached_hosts_index():
    """
    Reuse the HOST name index for a short period to avoid reloading it on every
    popup expansion.
    """
    now = time.time()

    if (
        _HOST_INDEX_CACHE["value"] is not None
        and _HOST_INDEX_CACHE["expires_at"] > now
    ):
        return _HOST_INDEX_CACHE["value"]

    value = _load_hosts_index()
    _HOST_INDEX_CACHE["value"] = value
    _HOST_INDEX_CACHE["expires_at"] = now + MAP_CACHE_TTL_SECONDS
    return value


def _load_site_rows():
    """
    Load site coordinates directly from DIM_SPECTRUM_SITE.

    The map should reflect the site catalog first. Equipment/spectrum presence
    is enrichment, not a prerequisite for plotting the point.
    """
    conn = get_connection_rfdata()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            s.ID_SITE,
            COALESCE(NULLIF(s.NA_SITE, ''), CONCAT('Site ', s.ID_SITE)) AS SITE_LABEL,
            c.NA_COUNTY AS COUNTY_NAME,
            d.NA_DISTRICT AS DISTRICT_NAME,
            st.ID_STATE,
            st.NA_STATE,
            st.LC_STATE,
            ST_AsText(s.GEO_POINT) AS GEO_WKT,
            s.NU_ALTITUDE,
            s.NU_GNSS_MEASUREMENTS
        FROM DIM_SPECTRUM_SITE s
        LEFT JOIN DIM_SITE_COUNTY c
            ON c.ID_COUNTY = s.FK_COUNTY
        LEFT JOIN DIM_SITE_DISTRICT d
            ON d.ID_DISTRICT = s.FK_DISTRICT
        LEFT JOIN DIM_SITE_STATE st
            ON st.ID_STATE = s.FK_STATE
        WHERE s.GEO_POINT IS NOT NULL
        ORDER BY s.ID_SITE
        """
    )

    rows = cur.fetchall()
    conn.close()
    return rows


def _load_equipment_rows_for_site(site_id):
    """
    Load known site/equipment relationships for a single site.

    The heavy FACT_SPECTRUM lookup is deferred until the user actually opens a
    popup, keeping the landing page fast.
    """
    conn = get_connection_rfdata()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT DISTINCT
            e.ID_EQUIPMENT,
            e.NA_EQUIPMENT
        FROM FACT_SPECTRUM f
        JOIN DIM_SPECTRUM_EQUIPMENT e
            ON e.ID_EQUIPMENT = f.FK_EQUIPMENT
        WHERE f.FK_SITE = %s
        ORDER BY e.NA_EQUIPMENT
        """,
        (site_id,),
    )

    rows = cur.fetchall()
    conn.close()
    return rows


def _build_site_detail(site_id):
    """
    Build the popup metadata for a single site.

    The popup is where we pay the cost of relating site/equipment information
    to host information. The landing page itself only needs the geographic
    points.
    """
    host_index = _get_cached_hosts_index()
    rows = _load_equipment_rows_for_site(site_id)
    stations = []

    for row in rows:
        equipment_name = (
            str(row["NA_EQUIPMENT"]).strip()
            if row["NA_EQUIPMENT"] is not None
            else ""
        )
        host = _find_host_for_equipment(host_index, equipment_name)
        station = {
            "equipment_id": (
                int(row["ID_EQUIPMENT"])
                if row["ID_EQUIPMENT"] is not None
                else None
            ),
            "equipment_name": equipment_name or None,
            "host_id": host["host_id"] if host else None,
            "host_name": host["host_name"] if host else None,
            "is_offline": host["is_offline"] if host else None,
        }

        if station not in stations:
            stations.append(station)

    return {
        "site_id": int(site_id),
        "stations": stations,
        "has_online_host": any(
            station["host_id"] is not None and station["is_offline"] is False
            for station in stations
        ),
        "has_known_host": any(
            station["host_id"] is not None
            for station in stations
        ),
    }


def _find_host_for_equipment(host_index, equipment_name):
    """
    Resolve an equipment name to a HOST row with a few safe fallbacks.

    The database model does not provide a direct site->host foreign key, so the
    map uses identifier reconciliation. Exact match remains the priority, but
    we also accept unique normalized matches such as:
        rfeye002264    <-> RFEye-002264
        cwsm211006     <-> CWSM211006.local
        cwsm21100001   <-> CWSM211001
        cwsm22010040   <-> CWSM220040
    """
    if not equipment_name:
        return None

    raw_key = str(equipment_name).strip().lower()
    exact = host_index["raw"].get(raw_key)

    if exact:
        return exact

    alias_keys = _build_station_alias_keys(equipment_name)

    if not alias_keys:
        return None

    for alias_key in alias_keys:
        normalized_matches = host_index["normalized"].get(alias_key, [])

        if len(normalized_matches) == 1:
            return normalized_matches[0]

    candidate_map = {}

    for host_key, hosts in host_index["normalized"].items():
        if not host_key:
            continue

        for alias_key in alias_keys:
            if host_key.startswith(alias_key) or alias_key.startswith(host_key):
                for host in hosts:
                    candidate_map[host["host_id"]] = host

    if len(candidate_map) == 1:
        return next(iter(candidate_map.values()))

    return None


def get_station_map_points():
    """
    Return map-ready station points for the landing page.

    Sites come from RFDATA, while host actions come from BPDATA. We join the
    two worlds in Python so the landing page does not depend on cross-database
    SQL privileges or spatial helper compatibility.
    """
    now = time.time()

    if (
        _MAP_POINTS_CACHE["value"] is not None
        and _MAP_POINTS_CACHE["expires_at"] > now
    ):
        return _MAP_POINTS_CACHE["value"]

    site_rows = _load_site_rows()

    result = []

    for row in site_rows:
        parsed = _parse_point_wkt(row.get("GEO_WKT"))

        if not parsed:
            continue

        latitude, longitude = parsed
        site_id = int(row["ID_SITE"])
        point = {
            "site_id": site_id,
            "site_label": str(row["SITE_LABEL"]),
            "county_name": (
                str(row["COUNTY_NAME"])
                if row["COUNTY_NAME"] is not None
                else None
            ),
            "district_name": (
                str(row["DISTRICT_NAME"])
                if row["DISTRICT_NAME"] is not None
                else None
            ),
            "state_id": int(row["ID_STATE"]) if row["ID_STATE"] is not None else None,
            "state_name": str(row["NA_STATE"]) if row["NA_STATE"] is not None else None,
            "state_code": str(row["LC_STATE"]) if row["LC_STATE"] is not None else None,
            "latitude": latitude,
            "longitude": longitude,
            "altitude": (
                float(row["NU_ALTITUDE"])
                if row["NU_ALTITUDE"] is not None
                else None
            ),
            "gnss_measurements": (
                int(row["NU_GNSS_MEASUREMENTS"])
                if row["NU_GNSS_MEASUREMENTS"] is not None
                else None
            ),
            "stations": [],
            "has_online_host": False,
            "has_known_host": False,
        }
        result.append(point)

    _MAP_POINTS_CACHE["value"] = result
    _MAP_POINTS_CACHE["expires_at"] = now + MAP_CACHE_TTL_SECONDS
    return result


def get_station_map_site_detail(site_id):
    """
    Return popup actions for a single site, using a short TTL cache.
    """
    now = time.time()
    cache_key = int(site_id)
    cached = _SITE_DETAILS_CACHE.get(cache_key)

    if cached and cached["expires_at"] > now:
        return cached["value"]

    value = _build_site_detail(cache_key)
    _SITE_DETAILS_CACHE[cache_key] = {
        "expires_at": now + MAP_CACHE_TTL_SECONDS,
        "value": value,
    }
    return value
