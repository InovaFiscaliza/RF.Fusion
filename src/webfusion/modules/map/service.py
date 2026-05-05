"""Station-map helpers used by the WebFusion landing page.

The home page is intentionally fast to open, so this module works from the
materialized ``RFFUSION_SUMMARY`` tables instead of rebuilding the old
cross-database joins on every request.

The service publishes two closely related payloads:

- the point list plotted on the map
- the per-site popup details loaded on demand

Both are cached from the same snapshot so the browser does not observe a point
state that disagrees with the popup opened immediately afterward.
"""

import logging
import threading
import time
from datetime import datetime, timedelta

from db import get_connection_summary

MAP_CACHE_TTL_SECONDS = 300
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

_MAP_POINTS_CACHE = {"expires_at": 0.0, "value": None}
_SITE_DETAILS_CACHE = {}
_MAP_CACHE_WRITE_LOCK = threading.Lock()
_MAP_REFRESH_STATE_LOCK = threading.Lock()
_MAP_REFRESH_RUNNING = False

LOGGER = logging.getLogger(__name__)


def _normalize_map_date_value(value):
    """Parse one optional ``YYYY-MM-DD`` filter value from the request layer."""

    normalized_value = str(value or "").strip()

    if not normalized_value:
        return None, None

    try:
        parsed_value = datetime.strptime(normalized_value, "%Y-%m-%d")
    except ValueError:
        return None, None

    return normalized_value, parsed_value


def _parse_map_date_range(start_date=None, end_date=None):
    """Normalize the temporal filter into comparable datetime boundaries."""

    normalized_start, start_dt = _normalize_map_date_value(start_date)
    normalized_end, end_dt = _normalize_map_date_value(end_date)

    if start_dt and end_dt and start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt
        normalized_start, normalized_end = normalized_end, normalized_start

    end_before = end_dt + timedelta(days=1) if end_dt else None
    return normalized_start, normalized_end, start_dt, end_before


def _load_summary_site_rows():
    """Load one consolidated row per mapped site from ``MAP_SITE_SUMMARY``."""

    conn = get_connection_summary()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            FK_SITE AS ID_SITE,
            NA_SITE_LABEL AS SITE_LABEL,
            NA_COUNTY_NAME AS COUNTY_NAME,
            NA_DISTRICT_NAME AS DISTRICT_NAME,
            ID_STATE,
            NA_STATE_NAME AS NA_STATE,
            NA_STATE_CODE AS LC_STATE,
            VL_LATITUDE,
            VL_LONGITUDE,
            VL_ALTITUDE,
            NU_GNSS_MEASUREMENTS,
            NA_MARKER_STATE,
            HAS_ONLINE_STATION,
            HAS_ONLINE_HOST,
            HAS_KNOWN_HOST
        FROM MAP_SITE_SUMMARY
        ORDER BY FK_SITE
        """
    )

    rows = cur.fetchall()
    conn.close()
    return rows


def _load_summary_station_rows():
    """Load per-station rows that enrich each site popup from summary tables."""

    conn = get_connection_summary()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            FK_SITE AS ID_SITE,
            FK_EQUIPMENT AS ID_EQUIPMENT,
            FK_HOST AS ID_HOST,
            NA_EQUIPMENT,
            NA_HOST_NAME,
            IS_OFFLINE,
            IS_CURRENT_LOCATION,
            NA_MAP_STATE,
            DT_FIRST_SEEN_AT AS FIRST_SEEN_AT,
            DT_LAST_SEEN_AT AS LAST_SEEN_AT,
            NU_SPECTRUM_COUNT
        FROM MAP_SITE_STATION_SUMMARY
        ORDER BY FK_SITE, NU_STATE_PRIORITY, NA_HOST_NAME, NA_EQUIPMENT
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


def _build_map_point_station(row):
    """Shape one station row for map/filter/popup use."""

    return {
        "equipment_id": int(row["ID_EQUIPMENT"]) if row.get("ID_EQUIPMENT") is not None else None,
        "equipment_name": str(row["NA_EQUIPMENT"]).strip() if row.get("NA_EQUIPMENT") is not None else None,
        "host_id": int(row["ID_HOST"]) if row.get("ID_HOST") is not None else None,
        "host_name": str(row["NA_HOST_NAME"]).strip() if row.get("NA_HOST_NAME") is not None else None,
        "is_offline": bool(row["IS_OFFLINE"]) if row.get("IS_OFFLINE") is not None else None,
        "is_current_location": bool(row.get("IS_CURRENT_LOCATION")),
        "map_state": str(row.get("NA_MAP_STATE") or POINT_STATE_NO_HOST),
        "first_seen_at": row.get("FIRST_SEEN_AT"),
        "last_seen_at": row.get("LAST_SEEN_AT"),
        "spectrum_count": int(row.get("NU_SPECTRUM_COUNT") or 0),
    }


def _clone_station(station):
    """Copy one station payload kept inside the cached snapshot."""

    return dict(station)


def _build_public_point_station(station):
    """Strip cached-only fields before returning point payloads to the browser."""

    return {
        "equipment_id": station.get("equipment_id"),
        "equipment_name": station.get("equipment_name"),
        "host_id": station.get("host_id"),
        "host_name": station.get("host_name"),
        "is_offline": station.get("is_offline"),
        "is_current_location": station.get("is_current_location"),
        "map_state": station.get("map_state"),
    }


def _clone_site_detail(detail):
    """Copy one cached site-detail payload without sharing station lists."""

    cloned_detail = dict(detail)
    cloned_detail["stations"] = [
        _clone_station(station)
        for station in detail.get("stations", [])
    ]
    return cloned_detail


def _build_point_from_site_detail(base_point, detail):
    """Merge filtered station detail back into the plotted point payload."""

    point = {
        "site_id": int(base_point["site_id"]),
        "site_label": base_point.get("site_label"),
        "county_name": base_point.get("county_name"),
        "district_name": base_point.get("district_name"),
        "state_id": base_point.get("state_id"),
        "state_name": base_point.get("state_name"),
        "state_code": base_point.get("state_code"),
        "latitude": base_point.get("latitude"),
        "longitude": base_point.get("longitude"),
        "altitude": base_point.get("altitude"),
        "gnss_measurements": base_point.get("gnss_measurements"),
        "stations": [
            _build_public_point_station(station)
            for station in detail.get("stations", [])
        ],
        "station_names": [
            station.get("host_name") or station.get("equipment_name")
            for station in detail.get("stations", [])
            if station.get("host_name") or station.get("equipment_name")
        ],
        "marker_state": detail.get("marker_state") or POINT_STATE_NO_HOST,
        "has_online_station": bool(detail.get("has_online_station")),
        "has_online_host": bool(detail.get("has_online_host")),
        "has_known_host": bool(detail.get("has_known_host")),
    }
    return point


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


def _get_cached_site_detail_values():
    """Return the raw detail snapshot currently mirrored in the TTL cache."""

    return {
        int(site_id): cache_entry["value"]
        for site_id, cache_entry in _SITE_DETAILS_CACHE.items()
    }


def _get_station_map_snapshot():
    """Return the latest full map snapshot, rebuilding only when necessary."""

    now = time.time()
    cached_site_details = _get_cached_site_detail_values()

    if (
        _MAP_POINTS_CACHE["value"] is not None
        and _MAP_POINTS_CACHE["expires_at"] > now
        and cached_site_details
    ):
        return _MAP_POINTS_CACHE["value"], cached_site_details

    if _MAP_POINTS_CACHE["value"] is not None and cached_site_details:
        _schedule_map_refresh_async(force=False)
        return _MAP_POINTS_CACHE["value"], cached_site_details

    return _refresh_station_map_snapshot()


def _refresh_station_map_snapshot():
    """Rebuild the full map snapshot and atomically publish both cache layers."""

    points, site_details = _build_station_map_dataset()
    _store_map_snapshot(points, site_details)
    return points, site_details


def _is_map_cache_due(now=None):
    """Return whether the current map snapshot is missing or near expiry."""

    current_time = time.time() if now is None else float(now)
    cached_points = _MAP_POINTS_CACHE["value"]

    if cached_points is None:
        return True

    return _MAP_POINTS_CACHE["expires_at"] <= (
        current_time + MAP_BACKGROUND_REFRESH_LEAD_SECONDS
    )


def _run_map_refresh_worker():
    """Refresh the map snapshot inside the detached worker thread."""

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


def _summarize_site_marker_state(stations):
    """Collapse multiple station states into the single marker state shown on the map."""

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


def _station_overlaps_date_range(station, start_dt=None, end_before=None):
    """Return whether one station observation overlaps the requested period."""

    first_seen = station.get("first_seen_at")
    last_seen = station.get("last_seen_at")

    if first_seen is None and last_seen is None:
        return False

    interval_start = first_seen or last_seen
    interval_end = last_seen or first_seen

    if start_dt and interval_end and interval_end < start_dt:
        return False

    if end_before and interval_start and interval_start >= end_before:
        return False

    return True


def _filter_site_detail_by_date(detail, start_dt=None, end_before=None):
    """Filter one site detail payload by temporal overlap and recompute markers."""

    cloned_detail = _clone_site_detail(detail)

    if start_dt is None and end_before is None:
        return cloned_detail

    filtered_stations = [
        station
        for station in cloned_detail.get("stations", [])
        if _station_overlaps_date_range(station, start_dt=start_dt, end_before=end_before)
    ]

    cloned_detail["stations"] = _sort_site_stations(filtered_stations)

    if cloned_detail["stations"]:
        cloned_detail["marker_state"] = _summarize_site_marker_state(cloned_detail["stations"])
        cloned_detail["has_online_station"] = any(
            station["map_state"] in {POINT_STATE_ONLINE_CURRENT, POINT_STATE_ONLINE_PREVIOUS}
            for station in cloned_detail["stations"]
        )
        cloned_detail["has_online_host"] = cloned_detail["has_online_station"]
        cloned_detail["has_known_host"] = any(
            station.get("host_id") is not None
            for station in cloned_detail["stations"]
        )
    else:
        cloned_detail["marker_state"] = POINT_STATE_NO_HOST
        cloned_detail["has_online_station"] = False
        cloned_detail["has_online_host"] = False
        cloned_detail["has_known_host"] = False

    return cloned_detail


def _build_filtered_station_map_snapshot(points, site_details, start_dt=None, end_before=None):
    """Apply a temporal filter on top of the cached summary-backed snapshot."""

    if start_dt is None and end_before is None:
        return points, site_details

    filtered_points = []
    filtered_site_details = {}

    for base_point in points:
        site_id = int(base_point["site_id"])
        base_detail = site_details.get(site_id)

        if not base_detail:
            continue

        filtered_detail = _filter_site_detail_by_date(
            base_detail,
            start_dt=start_dt,
            end_before=end_before,
        )

        if not filtered_detail.get("stations"):
            continue

        filtered_site_details[site_id] = filtered_detail
        filtered_points.append(_build_point_from_site_detail(base_point, filtered_detail))

    return filtered_points, filtered_site_details


def _build_station_map_dataset_from_summary():
    """Build the full map payload from ``RFFUSION_SUMMARY`` read models.

    The returned tuple contains the plotted points plus the already-shaped
    popup payloads keyed by site id.
    """

    site_rows = _load_summary_site_rows()
    station_rows = _load_summary_station_rows()

    points = []
    points_by_site = {}
    site_details = {}

    for row in site_rows:
        latitude = row.get("VL_LATITUDE")
        longitude = row.get("VL_LONGITUDE")

        if latitude is None or longitude is None:
            continue

        site_id = int(row["ID_SITE"])
        point = {
            "site_id": site_id,
            "site_label": str(row.get("SITE_LABEL") or f"Site {site_id}"),
            "county_name": str(row["COUNTY_NAME"]) if row.get("COUNTY_NAME") is not None else None,
            "district_name": str(row["DISTRICT_NAME"]) if row.get("DISTRICT_NAME") is not None else None,
            "state_id": int(row["ID_STATE"]) if row.get("ID_STATE") is not None else None,
            "state_name": str(row["NA_STATE"]) if row.get("NA_STATE") is not None else None,
            "state_code": str(row["LC_STATE"]) if row.get("LC_STATE") is not None else None,
            "latitude": float(latitude),
            "longitude": float(longitude),
            "altitude": float(row["VL_ALTITUDE"]) if row.get("VL_ALTITUDE") is not None else None,
            "gnss_measurements": int(row["NU_GNSS_MEASUREMENTS"]) if row.get("NU_GNSS_MEASUREMENTS") is not None else None,
            "stations": [],
            "station_names": [],
            "marker_state": str(row.get("NA_MARKER_STATE") or POINT_STATE_NO_HOST),
            "has_online_station": bool(row.get("HAS_ONLINE_STATION")),
            "has_online_host": bool(row.get("HAS_ONLINE_HOST")),
            "has_known_host": bool(row.get("HAS_KNOWN_HOST")),
        }
        points.append(point)
        points_by_site[site_id] = point
        site_details[site_id] = {
            "site_id": site_id,
            "stations": [],
            "marker_state": point["marker_state"],
            "has_online_station": point["has_online_station"],
            "has_online_host": point["has_online_host"],
            "has_known_host": point["has_known_host"],
        }

    for row in station_rows:
        site_id = int(row["ID_SITE"]) if row.get("ID_SITE") is not None else None

        if site_id not in points_by_site:
            continue

        station = _build_map_point_station(row)
        site_details[site_id]["stations"].append(station)

    for site_id, detail in site_details.items():
        detail["stations"] = _sort_site_stations(detail["stations"])

        if detail["stations"]:
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
        point["stations"] = [
            _build_public_point_station(station)
            for station in detail["stations"]
        ]
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


def _build_station_map_dataset():
    """Return the canonical map dataset builder used by runtime code paths."""

    return _build_station_map_dataset_from_summary()


def _build_site_detail(site_id):
    """Build popup metadata for one site from the current snapshot builder."""
    points, site_details = _build_station_map_dataset()
    _ = points
    return site_details.get(int(site_id), _build_default_site_detail(site_id))


def get_station_map_points(start_date=None, end_date=None):
    """Return map-ready station points for the landing page.

    If a fresh snapshot already exists, it is returned immediately. If the
    cache is empty, the request rebuilds the snapshot synchronously once.
    """
    _normalized_start, _normalized_end, start_dt, end_before = _parse_map_date_range(
        start_date=start_date,
        end_date=end_date,
    )

    if start_dt is not None or end_before is not None:
        points, site_details = _get_station_map_snapshot()
        filtered_points, _filtered_site_details = _build_filtered_station_map_snapshot(
            points,
            site_details,
            start_dt=start_dt,
            end_before=end_before,
        )
        return filtered_points

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


def get_station_map_site_detail(site_id, start_date=None, end_date=None):
    """Return popup metadata for a single site, using a short TTL cache.

    Popup detail is cached separately so repeated hover/open interactions do
    not require rebuilding the whole station map dataset every time.
    """
    _normalized_start, _normalized_end, start_dt, end_before = _parse_map_date_range(
        start_date=start_date,
        end_date=end_date,
    )
    cache_key = int(site_id)

    if start_dt is not None or end_before is not None:
        _points, site_details = _get_station_map_snapshot()
        detail = site_details.get(cache_key)

        if not detail:
            return _build_default_site_detail(cache_key)

        filtered_detail = _filter_site_detail_by_date(
            detail,
            start_dt=start_dt,
            end_before=end_before,
        )

        if not filtered_detail.get("stations"):
            return _build_default_site_detail(cache_key)

        return filtered_detail

    now = time.time()
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
