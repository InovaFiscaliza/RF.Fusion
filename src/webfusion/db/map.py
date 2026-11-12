"""Read site and station projections for the shared map API."""

from __future__ import annotations

from .connection import get_connection_summary
from .summary import (
    map_sites_query,
    map_stations_query,
)


def load_summary_site_rows() -> list[dict[str, object]]:
    """Load one consolidated row per mapped site from the site geography projection.

    Args:
        None.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    conn = get_connection_summary()
    try:
        cur = conn.cursor()

        cur.execute(
            f'''
            SELECT
                "FK_SITE" AS "ID_SITE",
                "FK_COUNTY" AS "ID_COUNTY",
                "FK_DISTRICT" AS "ID_DISTRICT",
                "NA_SITE_LABEL" AS "SITE_LABEL",
                "NA_COUNTY_NAME" AS "COUNTY_NAME",
                "NA_DISTRICT_NAME" AS "DISTRICT_NAME",
                "ID_STATE",
                "NA_STATE_NAME" AS "NA_STATE",
                "NA_STATE_CODE" AS "LC_STATE",
                "VL_LATITUDE",
                "VL_LONGITUDE",
                "VL_ALTITUDE",
                "NU_GNSS_MEASUREMENTS",
                "NA_MARKER_STATE",
                "HAS_ONLINE_STATION",
                "HAS_ONLINE_HOST",
                "HAS_KNOWN_HOST"
            FROM ({map_sites_query()}) map_sites
            ORDER BY "FK_SITE"
            '''
        )

        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def load_summary_station_rows() -> list[dict[str, object]]:
    """Load per-station rows that enrich each site popup from summary tables.

    Args:
        None.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    conn = get_connection_summary()
    try:
        cur = conn.cursor()

        cur.execute(
            f'''
            SELECT
                "FK_SITE" AS "ID_SITE",
                "FK_EQUIPMENT" AS "ID_EQUIPMENT",
                "FK_HOST" AS "ID_HOST",
                "NA_EQUIPMENT",
                "NA_HOST_NAME",
                "IS_OFFLINE",
                "IS_CURRENT_LOCATION",
                "NA_MAP_STATE",
                "DT_FIRST_SEEN_AT" AS "FIRST_SEEN_AT",
                "DT_LAST_SEEN_AT" AS "LAST_SEEN_AT",
                "NU_SPECTRUM_COUNT"
            FROM ({map_stations_query()}) map_stations
            ORDER BY "FK_SITE", "NU_STATE_PRIORITY", "NA_HOST_NAME", "NA_EQUIPMENT"
            '''
        )

        rows = cur.fetchall()
    finally:
        conn.close()
    return rows

