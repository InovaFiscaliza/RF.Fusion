"""Station-map helpers used by the WebFusion landing page.

The map is intentionally split between:

- fast point loading from ``RFDATA``
- later host enrichment using ``BPDATA``

That keeps the home page responsive while still allowing quick actions on the
station popup.
"""

import logging
import re
import threading
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
CWSM_SIGNATURE_OVERRIDES = {
    # Legacy Zabbix host naming for this fixed station diverges from the
    # receiver name embedded in the processed payload.
    "22010007": "211007",
}

MAP_CACHE_TTL_SECONDS = 300
MAP_BACKGROUND_REFRESH_POLL_SECONDS = 30.0
MAP_BACKGROUND_REFRESH_LEAD_SECONDS = 30.0

# The landing page communicates two independent ideas through marker state:
# online/offline and current/historical locality. Keeping the state taxonomy
# explicit here makes the downstream UI logic much easier to follow.
POINT_STATE_ONLINE_CURRENT = "online_current"
POINT_STATE_ONLINE_PREVIOUS = "online_previous"
POINT_STATE_OFFLINE_CURRENT = "offline_current"
POINT_STATE_OFFLINE_PREVIOUS = "offline_previous"
POINT_STATE_NO_HOST = "no_host"

POINT_STATE_PRIORITY = {
    POINT_STATE_ONLINE_CURRENT: 0,
    POINT_STATE_ONLINE_PREVIOUS: 1,
    POINT_STATE_OFFLINE_CURRENT: 2,
    POINT_STATE_OFFLINE_PREVIOUS: 3,
    POINT_STATE_NO_HOST: 4,
}

_HOST_INDEX_CACHE = {"expires_at": 0.0, "value": None}
_MAP_POINTS_CACHE = {"expires_at": 0.0, "value": None}
_SITE_DETAILS_CACHE = {}
_MAP_CACHE_WRITE_LOCK = threading.Lock()
_MAP_REFRESH_STATE_LOCK = threading.Lock()
_MAP_BACKGROUND_THREAD_LOCK = threading.Lock()
_MAP_REFRESH_RUNNING = False
_MAP_BACKGROUND_THREAD_STARTED = False

LOGGER = logging.getLogger(__name__)


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
            cwsm21120037
            cwsm22010040

        The long receiver identifier uses one extra digit family in the middle.
        For the fixed families we know today, the host-side 6-digit name is:
            cwsm2110xxxx -> cwsm211xxx
            cwsm2112xxxx -> cwsm212xxx
            cwsm2201xxxx -> cwsm220xxx

        That keeps the comparison strict while still reconciling the known
        CelPlan naming variants.
    """
    match = CWSM_KEY_RE.fullmatch(normalized_key or "")

    if not match:
        return None

    digits = match.group(2)

    if len(digits) < 6:
        return None

    if digits in CWSM_SIGNATURE_OVERRIDES:
        return f"cwsm{CWSM_SIGNATURE_OVERRIDES[digits]}"

    if len(digits) >= 8:
        family_prefix = {
            "2110": "211",
            "2112": "212",
            "2201": "220",
        }.get(digits[:4])

        if family_prefix:
            return f"cwsm{family_prefix}{digits[-3:]}"

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
    """Load HOST rows once and index them by normalized host name.

    The map repeatedly performs tolerant host/equipment matching. Building the
    host index once keeps those repeated lookups cheap.
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


def _load_station_site_rows():
    """Load one grouped observation row per equipment/site combination."""

    conn = get_connection_rfdata()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            f.FK_SITE AS ID_SITE,
            e.ID_EQUIPMENT,
            e.NA_EQUIPMENT,
            MIN(f.DT_TIME_START) AS FIRST_SEEN_AT,
            MAX(f.DT_TIME_END) AS LAST_SEEN_AT
        FROM FACT_SPECTRUM f
        JOIN DIM_SPECTRUM_EQUIPMENT e
            ON e.ID_EQUIPMENT = f.FK_EQUIPMENT
        GROUP BY
            f.FK_SITE,
            e.ID_EQUIPMENT,
            e.NA_EQUIPMENT
        ORDER BY
            f.FK_SITE,
            e.NA_EQUIPMENT
        """
    )

    rows = cur.fetchall()
    conn.close()
    return rows


def _build_default_site_detail(site_id):
    """Return the safe empty payload expected by the map popup."""

    return {
        "site_id": int(site_id),
        "stations": [],
        "marker_state": POINT_STATE_NO_HOST,
        "has_online_station": False,
        "has_online_host": False,
        "has_known_host": False,
    }


def _store_map_snapshot(points, site_details, now=None):
    """Atomically replace the in-memory point and popup caches.

    The marker list and the popup details describe the same snapshot of the
    world, so they should be published together to avoid cross-refresh drift.
    """

    current_time = time.time() if now is None else float(now)

    with _MAP_CACHE_WRITE_LOCK:
        _MAP_POINTS_CACHE["value"] = points
        _MAP_POINTS_CACHE["expires_at"] = current_time + MAP_CACHE_TTL_SECONDS
        _SITE_DETAILS_CACHE.clear()

        for site_id, detail in site_details.items():
            _SITE_DETAILS_CACHE[int(site_id)] = {
                "expires_at": current_time + MAP_CACHE_TTL_SECONDS,
                "value": detail,
            }


def _refresh_station_map_snapshot():
    """Rebuild the full station-map snapshot and publish it to caches."""

    points, site_details = _build_station_map_dataset()
    _store_map_snapshot(points, site_details)
    return points, site_details


def _is_map_cache_due(now=None):
    """Return whether the map snapshot should be refreshed soon."""

    current_time = time.time() if now is None else float(now)
    cached_points = _MAP_POINTS_CACHE["value"]

    if cached_points is None:
        return True

    return _MAP_POINTS_CACHE["expires_at"] <= (
        current_time + MAP_BACKGROUND_REFRESH_LEAD_SECONDS
    )


def _run_map_refresh_worker():
    """Refresh the map snapshot in a detached worker thread."""

    global _MAP_REFRESH_RUNNING

    try:
        _refresh_station_map_snapshot()
    except Exception:
        LOGGER.exception("failed_to_refresh_station_map_snapshot")
    finally:
        with _MAP_REFRESH_STATE_LOCK:
            _MAP_REFRESH_RUNNING = False


def _schedule_map_refresh_async(force=False):
    """Start a background refresh when the cache is missing or aging out.

    Requests should keep using a still-valid snapshot instead of blocking on a
    refresh, so this helper prefers "serve current cache, refresh in background"
    whenever possible.
    """

    global _MAP_REFRESH_RUNNING

    now = time.time()

    with _MAP_REFRESH_STATE_LOCK:
        if _MAP_REFRESH_RUNNING:
            return False

        if not force and not _is_map_cache_due(now):
            return False

        _MAP_REFRESH_RUNNING = True

    worker = threading.Thread(
        target=_run_map_refresh_worker,
        name="webfusion-map-refresh",
        daemon=True,
    )
    worker.start()
    return True


def _background_map_refresh_loop():
    """Keep the map snapshot warm without blocking user requests."""

    _schedule_map_refresh_async(force=True)

    while True:
        time.sleep(MAP_BACKGROUND_REFRESH_POLL_SECONDS)
        _schedule_map_refresh_async(force=False)


def start_station_map_background_refresh():
    """Ensure the map warm-up loop runs once per WebFusion process."""

    global _MAP_BACKGROUND_THREAD_STARTED

    with _MAP_BACKGROUND_THREAD_LOCK:
        if _MAP_BACKGROUND_THREAD_STARTED:
            return False

        _MAP_BACKGROUND_THREAD_STARTED = True

    worker = threading.Thread(
        target=_background_map_refresh_loop,
        name="webfusion-map-refresh-loop",
        daemon=True,
    )
    worker.start()
    return True


def _stringify_timestamp(value):
    """Return a stable sortable string for DB timestamps."""

    if value is None:
        return ""

    if hasattr(value, "isoformat"):
        return value.isoformat(sep=" ")

    return str(value)


def _station_observation_sort_key(row):
    """Build a deterministic key to decide the newest known locality."""

    return (
        _stringify_timestamp(row.get("LAST_SEEN_AT") or row.get("FIRST_SEEN_AT")),
        _stringify_timestamp(row.get("FIRST_SEEN_AT") or row.get("LAST_SEEN_AT")),
        int(row.get("ID_SITE") or 0),
    )


def _classify_station_point_state(host, is_current_location):
    """Map host availability plus locality role into one of the five UX states."""

    if not host:
        return POINT_STATE_NO_HOST

    if host["is_offline"]:
        return (
            POINT_STATE_OFFLINE_CURRENT
            if is_current_location
            else POINT_STATE_OFFLINE_PREVIOUS
        )

    return (
        POINT_STATE_ONLINE_CURRENT
        if is_current_location
        else POINT_STATE_ONLINE_PREVIOUS
    )


def _summarize_site_marker_state(stations):
    """Pick one marker state for a site that may aggregate several stations."""

    if not stations:
        return POINT_STATE_NO_HOST

    best_state = POINT_STATE_NO_HOST
    best_priority = POINT_STATE_PRIORITY[best_state]

    for station in stations:
        state_key = station.get("map_state") or POINT_STATE_NO_HOST
        priority = POINT_STATE_PRIORITY.get(state_key, POINT_STATE_PRIORITY[POINT_STATE_NO_HOST])

        if priority < best_priority:
            best_state = state_key
            best_priority = priority

    return best_state


def _sort_site_stations(stations):
    """Keep current/online stations first inside the popup."""

    return sorted(
        stations,
        key=lambda station: (
            POINT_STATE_PRIORITY.get(
                station.get("map_state") or POINT_STATE_NO_HOST,
                POINT_STATE_PRIORITY[POINT_STATE_NO_HOST],
            ),
            str(station.get("host_name") or station.get("equipment_name") or "").lower(),
        ),
    )


def _build_station_map_dataset():
    """Build both the point list and the popup detail cache payloads.

    The dataset is assembled in two stages:

    1. load one geographic point per known site
    2. enrich those points with station/equipment observations and host state

    Returning both map points and popup details from one builder guarantees
    that both surfaces reflect the same refresh cycle.
    """

    site_rows = _load_site_rows()
    host_index = _get_cached_hosts_index()
    station_rows = _load_station_site_rows()

    points = []
    points_by_site = {}
    site_details = {}

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
            "station_names": [],
            "marker_state": POINT_STATE_NO_HOST,
            "has_online_station": False,
            "has_online_host": False,
            "has_known_host": False,
        }
        points.append(point)
        points_by_site[site_id] = point
        site_details[site_id] = {
            "site_id": site_id,
            "stations": [],
            "marker_state": POINT_STATE_NO_HOST,
            "has_online_station": False,
            "has_online_host": False,
            "has_known_host": False,
        }

    # First identify the newest known site per equipment so later we can label
    # each observation as current or historical.
    latest_site_by_equipment = {}

    for row in station_rows:
        equipment_id = int(row["ID_EQUIPMENT"]) if row.get("ID_EQUIPMENT") is not None else None

        if equipment_id is None:
            continue

        current = latest_site_by_equipment.get(equipment_id)

        if current is None or _station_observation_sort_key(row) > _station_observation_sort_key(current):
            latest_site_by_equipment[equipment_id] = row

    for row in station_rows:
        site_id = int(row["ID_SITE"]) if row.get("ID_SITE") is not None else None
        equipment_id = int(row["ID_EQUIPMENT"]) if row.get("ID_EQUIPMENT") is not None else None

        if site_id not in points_by_site or equipment_id is None:
            continue

        equipment_name = (
            str(row["NA_EQUIPMENT"]).strip()
            if row["NA_EQUIPMENT"] is not None
            else ""
        )
        host = _find_host_for_equipment(host_index, equipment_name)
        latest_row = latest_site_by_equipment.get(equipment_id)
        is_current_location = bool(latest_row) and int(latest_row["ID_SITE"]) == site_id
        map_state = _classify_station_point_state(host, is_current_location)
        station = {
            "equipment_id": equipment_id,
            "equipment_name": equipment_name or None,
            "host_id": host["host_id"] if host else None,
            "host_name": host["host_name"] if host else None,
            "is_offline": host["is_offline"] if host else None,
            "is_current_location": is_current_location,
            "map_state": map_state,
        }

        site_details[site_id]["stations"].append(station)

    # Then collapse the per-station observations into one summarized state per
    # site, which is what the map can actually render as one marker.
    for site_id, detail in site_details.items():
        detail["stations"] = _sort_site_stations(detail["stations"])
        detail["marker_state"] = _summarize_site_marker_state(detail["stations"])
        detail["has_online_station"] = any(
            station["map_state"] in {POINT_STATE_ONLINE_CURRENT, POINT_STATE_ONLINE_PREVIOUS}
            for station in detail["stations"]
        )
        detail["has_online_host"] = detail["has_online_station"]
        detail["has_known_host"] = any(
            station["host_id"] is not None
            for station in detail["stations"]
        )

        point = points_by_site[site_id]
        point["marker_state"] = detail["marker_state"]
        point["has_online_station"] = detail["has_online_station"]
        point["has_online_host"] = detail["has_online_host"]
        point["has_known_host"] = detail["has_known_host"]
        point["station_names"] = [
            station["host_name"] or station["equipment_name"]
            for station in detail["stations"]
            if station.get("host_name") or station.get("equipment_name")
        ]

    return points, site_details


def _build_site_detail(site_id):
    """
    Build the popup metadata for a single site.

    The popup is where we pay the cost of relating site/equipment information
    to host information. The landing page itself only needs the geographic
    points.
    """
    points, site_details = _build_station_map_dataset()
    _ = points
    return site_details.get(int(site_id), _build_default_site_detail(site_id))


def _find_host_for_equipment(host_index, equipment_name):
    """
    Resolve an equipment name to a HOST row with a few safe fallbacks.

    The database model does not provide a direct site->host foreign key, so the
    map uses identifier reconciliation. Exact match remains the priority, but
    we also accept unique normalized matches such as:
        rfeye002264    <-> RFEye-002264
        cwsm211006     <-> CWSM211006.local
        cwsm21100001   <-> CWSM211001
        cwsm21120037   <-> CWSM212037
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

    if _MAP_POINTS_CACHE["value"] is not None:
        _schedule_map_refresh_async(force=False)
        return _MAP_POINTS_CACHE["value"]

    points, _site_details = _refresh_station_map_snapshot()
    return points


def get_station_map_site_detail(site_id):
    """Return popup actions for a single site, using a short TTL cache.

    Popup detail is cached separately so repeated hover/open interactions do
    not require rebuilding the whole station map dataset every time.
    """
    now = time.time()
    cache_key = int(site_id)
    cached = _SITE_DETAILS_CACHE.get(cache_key)

    if cached and cached["expires_at"] > now:
        return cached["value"]

    if cached:
        _schedule_map_refresh_async(force=False)
        return cached["value"]

    value = _build_site_detail(cache_key)
    _SITE_DETAILS_CACHE[cache_key] = {
        "expires_at": now + MAP_CACHE_TTL_SECONDS,
        "value": value,
    }
    return value
