"""Station-map helpers used by the WebFusion landing page.

The home page works from the materialized ``RFFUSION_SUMMARY`` tables instead
of rebuilding the old cross-database joins on every request.

The summary queries are intentionally cheap, so this module rebuilds the map
snapshot directly from the database instead of carrying an in-process cache
with TTLs, locks and background refresh state. That keeps the execution flow
linear and much easier to debug in production.
"""

from datetime import datetime, timedelta

from db import get_connection_summary

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
ONLINE_MARKER_STATES = {
    POINT_STATE_ONLINE_CURRENT,
    POINT_STATE_ONLINE_PREVIOUS,
}


def _parse_map_date_value(value):
    """Parse one optional ``YYYY-MM-DD`` filter value from the request layer."""

    value = str(value or "").strip()

    if not value:
        return None

    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None


def _parse_map_date_range(start_date=None, end_date=None):
    """Normalize the temporal filter into comparable datetime boundaries."""

    start_dt = _parse_map_date_value(start_date)
    end_dt = _parse_map_date_value(end_date)

    # The UI can submit inverted ranges while the user is editing manually.
    # Swapping the bounds keeps the filter forgiving without changing the
    # semantics of the requested period.
    if start_dt and end_dt and start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt

    end_before = end_dt + timedelta(days=1) if end_dt else None
    return start_dt, end_before


def _load_summary_site_rows():
    """Load one consolidated row per mapped site from ``MAP_SITE_SUMMARY``."""

    conn = get_connection_summary()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            FK_SITE AS ID_SITE,
            FK_COUNTY AS ID_COUNTY,
            FK_DISTRICT AS ID_DISTRICT,
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


def _recompute_site_detail_summary(detail):
    """Recompute marker and availability flags from the current station list.

    Several code paths mutate `detail["stations"]` first and then need the
    same derived fields to stay consistent. Keeping that recomputation in one
    place avoids drift between full-dataset builds and date-filtered views.
    """

    stations = _sort_site_stations(detail.get("stations", []))
    detail["stations"] = stations

    # Once all stations disappear, the site must degrade to the empty marker
    # state so the plotted point and popup keep telling the same story.
    if not stations:
        detail["marker_state"] = POINT_STATE_NO_HOST
        detail["has_online_station"] = False
        detail["has_online_host"] = False
        detail["has_known_host"] = False
        return detail

    detail["marker_state"] = _summarize_site_marker_state(stations)
    detail["has_online_station"] = any(
        station.get("map_state") in ONLINE_MARKER_STATES
        for station in stations
    )
    detail["has_online_host"] = detail["has_online_station"]
    detail["has_known_host"] = any(
        station.get("host_id") is not None
        for station in stations
    )
    return detail


def _apply_site_detail_to_point(point, detail):
    """Mirror the derived site detail back into one browser-facing map point."""

    stations = detail.get("stations", [])
    point["stations"] = [
        _build_public_point_station(station)
        for station in stations
    ]
    point["station_names"] = [
        station_name
        for station in stations
        if (station_name := station.get("host_name") or station.get("equipment_name"))
    ]
    point["marker_state"] = detail.get("marker_state") or POINT_STATE_NO_HOST
    point["has_online_station"] = bool(detail.get("has_online_station"))
    point["has_online_host"] = bool(detail.get("has_online_host"))
    point["has_known_host"] = bool(detail.get("has_known_host"))
    return point


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

    # Summary rows may carry only one edge of the observation interval. In
    # that case we treat the known timestamp as both bounds for overlap tests.
    interval_start = first_seen or last_seen
    interval_end = last_seen or first_seen

    if start_dt and interval_end and interval_end < start_dt:
        return False

    if end_before and interval_start and interval_start >= end_before:
        return False

    return True


def _filter_site_detail_by_date(detail, start_dt=None, end_before=None):
    """Filter one site detail payload by temporal overlap and recompute markers."""

    # Date filters produce a derived popup payload. Keeping the original detail
    # intact avoids mixing "full site" and "filtered site" state in one object.
    filtered_detail = dict(detail)
    filtered_detail["stations"] = [
        dict(station)
        for station in detail.get("stations", [])
    ]

    if start_dt is None and end_before is None:
        return filtered_detail

    filtered_detail["stations"] = [
        station
        for station in filtered_detail.get("stations", [])
        if _station_overlaps_date_range(station, start_dt=start_dt, end_before=end_before)
    ]
    return _recompute_site_detail_summary(filtered_detail)


def _build_station_map_dataset_from_summary():
    """Build the full map payload from ``RFFUSION_SUMMARY`` read models.

    The returned tuple contains the plotted points plus the already-shaped
    popup payloads keyed by site id. Site rows seed the locality metadata,
    then station rows refine the popup and final marker state.
    """

    site_rows = _load_summary_site_rows()
    station_rows = _load_summary_station_rows()

    points = []
    points_by_site = {}
    site_details = {}

    # Phase 1: build one map point per locality from the summary rows.
    for row in site_rows:
        latitude = row.get("VL_LATITUDE")
        longitude = row.get("VL_LONGITUDE")

        if latitude is None or longitude is None:
            continue

        site_id = int(row["ID_SITE"])
        point = {
            "site_id": site_id,
            "county_id": int(row["ID_COUNTY"]) if row.get("ID_COUNTY") is not None else None,
            "district_id": int(row["ID_DISTRICT"]) if row.get("ID_DISTRICT") is not None else None,
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
            "county_id": point["county_id"],
            "district_id": point["district_id"],
            "stations": [],
            "marker_state": point["marker_state"],
            "has_online_station": point["has_online_station"],
            "has_online_host": point["has_online_host"],
            "has_known_host": point["has_known_host"],
        }

    # Phase 2: attach station rows only to localities that are already plotted.
    for row in station_rows:
        raw_site_id = row.get("ID_SITE")

        if raw_site_id is None:
            continue

        site_id = int(raw_site_id)
        detail = site_details.get(site_id)

        if detail is None:
            continue

        detail["stations"].append(_build_map_point_station(row))

    # Phase 3: when station rows exist, they become the source of truth for
    # marker state and online flags. Without station rows, the summary-site
    # row keeps its seeded values.
    for site_id, detail in site_details.items():
        if detail["stations"]:
            _recompute_site_detail_summary(detail)

        _apply_site_detail_to_point(points_by_site[site_id], detail)

    return points, site_details


def get_station_map_points(start_date=None, end_date=None):
    """Return map-ready station points for the landing page.

    Each request rebuilds the snapshot from the summary tables so the runtime
    flow stays explicit and free of in-process cache state.
    """
    start_dt, end_before = _parse_map_date_range(
        start_date=start_date,
        end_date=end_date,
    )
    points, site_details = _build_station_map_dataset_from_summary()

    if start_dt is not None or end_before is not None:
        filtered_points = []

        # Temporal filters run on top of the already-shaped site detail so the
        # overlap rules stay in one place.
        for base_point in points:
            site_id = int(base_point["site_id"])
            detail = site_details.get(site_id)

            if not detail:
                continue

            filtered_detail = _filter_site_detail_by_date(
                detail,
                start_dt=start_dt,
                end_before=end_before,
            )

            if not filtered_detail.get("stations"):
                continue

            point = dict(base_point)
            _apply_site_detail_to_point(point, filtered_detail)
            filtered_points.append(point)

        return filtered_points

    return points


def get_station_map_site_detail(site_id, start_date=None, end_date=None):
    """Return popup metadata for a single site.

    The detail comes from the same summary-backed builder used by the map
    points, so popup and point state are derived by the same rules.
    """
    start_dt, end_before = _parse_map_date_range(
        start_date=start_date,
        end_date=end_date,
    )
    cache_key = int(site_id)
    _, site_details = _build_station_map_dataset_from_summary()
    detail = site_details.get(cache_key)

    if not detail:
        return _build_default_site_detail(cache_key)

    if start_dt is None and end_before is None:
        return detail

    filtered_detail = _filter_site_detail_by_date(
        detail,
        start_dt=start_dt,
        end_before=end_before,
    )

    # The popup API always returns the same shape. If the period removes
    # every station, callers still receive an explicit empty detail object.
    if not filtered_detail.get("stations"):
        return _build_default_site_detail(cache_key)

    return filtered_detail
