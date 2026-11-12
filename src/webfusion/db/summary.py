"""Compose shared read projections over canonical Summary aggregates.

These pure builders own only server-defined SQL. They open no connections and
accept no client input. Domain handlers bind request values and retain their
own filtering, result contracts and transaction boundaries.
"""


def _site_geography_query() -> str:
    """Compose authoritative site geography and locality labels.

    Args:
        None.

    Returns:
        str: RFDATA site identifiers, geographic names, coordinates and locality labels.
            The caller embeds this fixed SELECT and binds its own values.
    """
    return '''
    SELECT s."ID_SITE" AS "FK_SITE", s."FK_COUNTY", s."FK_DISTRICT",
        s."FK_STATE" AS "ID_STATE", s."NA_SITE" AS "NA_SITE_NAME",
        COALESCE(NULLIF(s."NA_SITE", ''), CONCAT('Site ', s."ID_SITE")) AS "NA_SITE_LABEL",
        CONCAT(locality.site_name,
            CASE WHEN locality.county IS NOT NULL AND LOWER(locality.site_name) <> LOWER(locality.county)
                 THEN ' · ' || locality.county ELSE '' END,
            CASE WHEN locality.state_code IS NOT NULL THEN '/' || locality.state_code ELSE '' END) AS "NA_LOCALITY_LABEL",
        c."NA_COUNTY" AS "NA_COUNTY_NAME", d."NA_DISTRICT" AS "NA_DISTRICT_NAME",
        st."NA_STATE" AS "NA_STATE_NAME", st."LC_STATE" AS "NA_STATE_CODE",
        public.ST_Y(s."GEO_POINT") AS "VL_LATITUDE", public.ST_X(s."GEO_POINT") AS "VL_LONGITUDE",
        s."NU_ALTITUDE" AS "VL_ALTITUDE", s."NU_GNSS_MEASUREMENTS"
    FROM "RFDATA"."DIM_SPECTRUM_SITE" s
    LEFT JOIN "RFDATA"."DIM_SITE_DISTRICT" d ON d."ID_DISTRICT" = s."FK_DISTRICT"
    LEFT JOIN "RFDATA"."DIM_SITE_COUNTY" c ON c."ID_COUNTY" = s."FK_COUNTY"
    LEFT JOIN "RFDATA"."DIM_SITE_STATE" st ON st."ID_STATE" = s."FK_STATE"
    CROSS JOIN LATERAL (
        SELECT COALESCE(NULLIF(BTRIM(s."NA_SITE"), ''), NULLIF(BTRIM(d."NA_DISTRICT"), ''),
            NULLIF(BTRIM(c."NA_COUNTY"), ''), CONCAT('Site ', s."ID_SITE")) AS site_name,
            NULLIF(BTRIM(c."NA_COUNTY"), '') AS county, NULLIF(BTRIM(st."LC_STATE"), '') AS state_code
    ) locality
    '''


def site_observations_query() -> str:
    """Compose site/equipment observations with their current-location flag.

    Args:
        None.

    Returns:
        str: Canonical site/equipment metrics joined with geography and equipment names;
            current locations retain the original deterministic ranking. The caller
            embeds this fixed SELECT and binds its own values.
    """
    return f'''
    SELECT m."FK_EQUIPMENT", m."DT_FIRST_SEEN_AT", m."DT_LAST_SEEN_AT", m."NU_SPECTRUM_COUNT", m."ID_LAST_SPECTRUM", m."DT_REFRESHED_AT", geo.*, e."NA_EQUIPMENT",
        (ROW_NUMBER() OVER (PARTITION BY m."FK_EQUIPMENT" ORDER BY COALESCE(m."DT_LAST_SEEN_AT", m."DT_FIRST_SEEN_AT") DESC NULLS LAST,
            COALESCE(m."DT_FIRST_SEEN_AT", m."DT_LAST_SEEN_AT") DESC NULLS LAST, m."FK_SITE" DESC) = 1)::int AS "IS_CURRENT_LOCATION"
    FROM "RFFUSION_SUMMARY"."SITE_EQUIPMENT_METRIC" m
    JOIN ({_site_geography_query()}) geo ON geo."FK_SITE" = m."FK_SITE"
    JOIN "RFDATA"."DIM_SPECTRUM_EQUIPMENT" e ON e."ID_EQUIPMENT" = m."FK_EQUIPMENT"
    '''


def host_locations_query() -> str:
    """Compose observed station locations from primary equipment links.

    Args:
        None.

    Returns:
        str: Station/site aggregates, matched equipment totals, date bounds, location
            flags and authoritative geography and connectivity. The caller embeds this
            fixed SELECT and binds its own values.
    """
    return f'''
    SELECT observed.*, geo.*, h."NA_HOST_NAME", h."IS_OFFLINE" AS "IS_OFFLINE_SNAPSHOT"
    FROM (
        SELECT link."FK_HOST", m."FK_SITE" AS site_id,
            MIN(m."DT_FIRST_SEEN_AT") AS "DT_FIRST_SEEN_AT", MAX(m."DT_LAST_SEEN_AT") AS "DT_LAST_SEEN_AT",
            SUM(m."NU_SPECTRUM_COUNT")::bigint AS "NU_SPECTRUM_COUNT", COUNT(DISTINCT m."FK_EQUIPMENT") AS "NU_MATCHED_EQUIPMENT_TOTAL",
            MAX(link."VL_MATCH_CONFIDENCE") AS "VL_MAX_MATCH_CONFIDENCE", MAX(m."DT_REFRESHED_AT") AS "DT_REFRESHED_AT",
            MAX(m."IS_CURRENT_LOCATION") AS "IS_CURRENT_LOCATION"
        FROM ({site_observations_query()}) m
        JOIN "RFFUSION_SUMMARY"."HOST_EQUIPMENT_LINK" link ON link."FK_EQUIPMENT" = m."FK_EQUIPMENT" AND link."IS_PRIMARY_LINK" = 1
        GROUP BY link."FK_HOST", m."FK_SITE"
    ) observed
    JOIN ({_site_geography_query()}) geo ON geo."FK_SITE" = observed.site_id
    JOIN "BPDATA"."HOST" h ON h."ID_HOST" = observed."FK_HOST"
    '''


def map_stations_query() -> str:
    """Compose station details and marker priorities for the shared map.

    Args:
        None.

    Returns:
        str: Site/equipment observations enriched with the best primary host link,
            connectivity, map state and state priority. The caller embeds this fixed
            SELECT and binds its own values.
    """
    return f'''
    SELECT o.*, owner."FK_HOST", h."NA_HOST_NAME", h."IS_OFFLINE",
        CASE WHEN h."ID_HOST" IS NULL THEN 'no_host'
             WHEN h."IS_OFFLINE" = 0 AND o."IS_CURRENT_LOCATION" = 1 THEN 'online_current'
             WHEN h."IS_OFFLINE" = 0 THEN 'online_previous'
             WHEN o."IS_CURRENT_LOCATION" = 1 THEN 'offline_current' ELSE 'offline_previous' END AS "NA_MAP_STATE",
        CASE WHEN h."ID_HOST" IS NULL THEN 4 WHEN h."IS_OFFLINE" = 0 THEN 1 - o."IS_CURRENT_LOCATION"
             ELSE 3 - o."IS_CURRENT_LOCATION" END AS "NU_STATE_PRIORITY"
    FROM ({site_observations_query()}) o
    LEFT JOIN LATERAL (
        SELECT link."FK_HOST" FROM "RFFUSION_SUMMARY"."HOST_EQUIPMENT_LINK" link
        WHERE link."FK_EQUIPMENT" = o."FK_EQUIPMENT" AND link."IS_PRIMARY_LINK" = 1
        ORDER BY link."VL_MATCH_CONFIDENCE" DESC, link."FK_HOST" ASC LIMIT 1
    ) owner ON true
    LEFT JOIN "BPDATA"."HOST" h ON h."ID_HOST" = owner."FK_HOST"
    '''


def map_sites_query() -> str:
    """Compose site geography for the shared map.

    Args:
        None.

    Returns:
        str: Site geography with the existing no_host marker and zero flags; the map
            service computes final states from station observations. The caller embeds
            this fixed SELECT and binds its own values.
    """
    return f'''
    SELECT geo.*, 'no_host'::text AS "NA_MARKER_STATE", 0 AS "HAS_ONLINE_STATION",
        0 AS "HAS_ONLINE_HOST", 0 AS "HAS_KNOWN_HOST"
    FROM ({_site_geography_query()}) geo
    '''


def host_operational_query() -> str:
    """Compose operational hosts with canonical Summary metrics.

    Args:
        None.

    Returns:
        str: Authoritative host fields, queue/acervo metrics, current-month throughput,
            spectrum totals and the selected current locality. The caller embeds this
            fixed SELECT and binds its own values.
    """
    return f'''
    SELECT h.*,
        CASE WHEN m."DT_REFERENCE_MONTH" = date_trunc('month', CURRENT_DATE)::date THEN COALESCE(m."NU_BACKUP_DONE_THIS_MONTH", 0) ELSE 0 END AS "NU_BACKUP_DONE_THIS_MONTH",
        CASE WHEN m."DT_REFERENCE_MONTH" = date_trunc('month', CURRENT_DATE)::date THEN COALESCE(m."VL_BACKUP_DONE_GB_THIS_MONTH", 0) ELSE 0 END AS "VL_BACKUP_DONE_GB_THIS_MONTH",
        COALESCE(m."NU_BACKUP_QUEUE_FILES_TOTAL", 0) AS "NU_BACKUP_QUEUE_FILES_TOTAL",
        COALESCE(m."VL_BACKUP_QUEUE_GB_TOTAL", 0) AS "VL_BACKUP_QUEUE_GB_TOTAL",
        COALESCE(m."NU_BACKUP_QUEUE_RUNNING_FILES_TOTAL", 0) AS "NU_BACKUP_QUEUE_RUNNING_FILES_TOTAL",
        COALESCE(m."VL_BACKUP_QUEUE_RUNNING_GB_TOTAL", 0) AS "VL_BACKUP_QUEUE_RUNNING_GB_TOTAL",
        COALESCE(m."NU_BACKUP_QUEUE_SUSPENDED_FILES_TOTAL", 0) AS "NU_BACKUP_QUEUE_SUSPENDED_FILES_TOTAL",
        COALESCE(m."VL_BACKUP_QUEUE_SUSPENDED_GB_TOTAL", 0) AS "VL_BACKUP_QUEUE_SUSPENDED_GB_TOTAL",
        COALESCE(m."NU_PROCESSING_QUEUE_FILES_TOTAL", 0) AS "NU_PROCESSING_QUEUE_FILES_TOTAL",
        COALESCE(m."VL_PROCESSING_QUEUE_GB_TOTAL", 0) AS "VL_PROCESSING_QUEUE_GB_TOTAL",
        COALESCE(m."NU_PROCESSING_QUEUE_RUNNING_FILES_TOTAL", 0) AS "NU_PROCESSING_QUEUE_RUNNING_FILES_TOTAL",
        COALESCE(m."VL_PROCESSING_QUEUE_RUNNING_GB_TOTAL", 0) AS "VL_PROCESSING_QUEUE_RUNNING_GB_TOTAL",
        COALESCE(m."NU_PROCESSING_QUEUE_FROZEN_FILES_TOTAL", 0) AS "NU_PROCESSING_QUEUE_FROZEN_FILES_TOTAL",
        COALESCE(m."VL_PROCESSING_QUEUE_FROZEN_GB_TOTAL", 0) AS "VL_PROCESSING_QUEUE_FROZEN_GB_TOTAL",
        COALESCE(m."NU_PAYLOAD_DELETED_FILES_TOTAL", 0) AS "NU_PAYLOAD_DELETED_FILES_TOTAL",
        COALESCE(m."VL_PAYLOAD_DELETED_GB_TOTAL", 0) AS "VL_PAYLOAD_DELETED_GB_TOTAL",
        CASE WHEN m."DT_REFERENCE_MONTH" = date_trunc('month', CURRENT_DATE)::date THEN COALESCE(m."NU_PROCESSING_DONE_THIS_MONTH", 0) ELSE 0 END AS "NU_PROCESSING_DONE_THIS_MONTH",
        CASE WHEN m."DT_REFERENCE_MONTH" = date_trunc('month', CURRENT_DATE)::date THEN COALESCE(m."VL_PROCESSING_DONE_GB_THIS_MONTH", 0) ELSE 0 END AS "VL_PROCESSING_DONE_GB_THIS_MONTH",
        COALESCE(m."NU_DISCOVERED_FILES_TOTAL", 0) AS "NU_DISCOVERED_FILES_TOTAL",
        COALESCE(m."VL_DISCOVERED_GB_TOTAL", 0) AS "VL_DISCOVERED_GB_TOTAL",
        COALESCE(m."NU_BACKUP_DONE_FILES_TOTAL", 0) AS "NU_BACKUP_DONE_FILES_TOTAL",
        COALESCE(m."VL_BACKUP_DONE_GB_TOTAL", 0) AS "VL_BACKUP_DONE_GB_TOTAL",
        COALESCE(m."NU_PROCESSING_DONE_FILES_TOTAL", 0) AS "NU_PROCESSING_DONE_FILES_TOTAL",
        COALESCE(m."VL_PROCESSING_DONE_GB_TOTAL", 0) AS "VL_PROCESSING_DONE_GB_TOTAL",
        COALESCE(m."NU_BACKUP_PENDING_FILES_CURRENT", 0) AS "NU_BACKUP_PENDING_FILES_CURRENT",
        COALESCE(m."VL_BACKUP_PENDING_GB_CURRENT", 0) AS "VL_BACKUP_PENDING_GB_CURRENT",
        COALESCE(m."NU_BACKUP_ERROR_FILES_CURRENT", 0) AS "NU_BACKUP_ERROR_FILES_CURRENT",
        COALESCE(m."VL_BACKUP_ERROR_GB_CURRENT", 0) AS "VL_BACKUP_ERROR_GB_CURRENT",
        COALESCE(m."NU_BACKUP_SUSPENDED_FILES_CURRENT", 0) AS "NU_BACKUP_SUSPENDED_FILES_CURRENT",
        COALESCE(m."VL_BACKUP_SUSPENDED_GB_CURRENT", 0) AS "VL_BACKUP_SUSPENDED_GB_CURRENT",
        COALESCE(m."NU_PROCESSING_PENDING_FILES_CURRENT", 0) AS "NU_PROCESSING_PENDING_FILES_CURRENT",
        COALESCE(m."VL_PROCESSING_PENDING_GB_CURRENT", 0) AS "VL_PROCESSING_PENDING_GB_CURRENT",
        COALESCE(m."NU_PROCESSING_ERROR_FILES_CURRENT", 0) AS "NU_PROCESSING_ERROR_FILES_CURRENT",
        COALESCE(m."VL_PROCESSING_ERROR_GB_CURRENT", 0) AS "VL_PROCESSING_ERROR_GB_CURRENT",
        COALESCE(m."NU_PROCESSING_FROZEN_FILES_CURRENT", 0) AS "NU_PROCESSING_FROZEN_FILES_CURRENT",
        COALESCE(m."VL_PROCESSING_FROZEN_GB_CURRENT", 0) AS "VL_PROCESSING_FROZEN_GB_CURRENT",
        COALESCE(spectra.total, 0) AS "NU_FACT_SPECTRUM_TOTAL",
        current_site."NA_LOCALITY_LABEL" AS "NA_CURRENT_LOCALITY_LABEL",
        current_site."NA_SITE_LABEL" AS "NA_CURRENT_SITE_LABEL",
        current_site."NA_STATE_CODE" AS "NA_CURRENT_STATE_CODE",
        current_site."VL_LATITUDE" AS "VL_CURRENT_LATITUDE",
        current_site."VL_LONGITUDE" AS "VL_CURRENT_LONGITUDE"
    FROM "BPDATA"."HOST" h
    LEFT JOIN "RFFUSION_SUMMARY"."HOST_METRIC" m ON m."FK_HOST" = h."ID_HOST"
    LEFT JOIN (
        SELECT link."FK_HOST", SUM(sm."NU_SPECTRUM_COUNT") AS total
        FROM "RFFUSION_SUMMARY"."HOST_EQUIPMENT_LINK" link
        JOIN "RFFUSION_SUMMARY"."SITE_EQUIPMENT_METRIC" sm ON sm."FK_EQUIPMENT" = link."FK_EQUIPMENT"
        WHERE link."IS_PRIMARY_LINK" = 1 GROUP BY link."FK_HOST"
    ) spectra ON spectra."FK_HOST" = h."ID_HOST"
    LEFT JOIN LATERAL (
        SELECT location.* FROM ({host_locations_query()}) location
        WHERE location."FK_HOST" = h."ID_HOST" AND location."IS_CURRENT_LOCATION" = 1
        ORDER BY location."DT_LAST_SEEN_AT" DESC NULLS LAST, location."FK_SITE" ASC LIMIT 1
    ) current_site ON true
    '''


def server_totals_query() -> str:
    """Compose server totals from operational hosts and canonical aggregates.

    Args:
        None.

    Returns:
        str: The singleton server row, including host counts, queue/acervo totals and
            current-month backup throughput in the existing GB units. The caller embeds
            this fixed SELECT and binds its own values.
    """
    return '''
    SELECT
        1 AS "ID_SUMMARY",
        to_char(CURRENT_DATE, 'YYYY-MM') AS "NA_CURRENT_MONTH_LABEL",
        (SELECT COUNT(*) FROM "BPDATA"."HOST") AS "NU_TOTAL_HOSTS",
        (SELECT COUNT(*) FROM "BPDATA"."HOST" WHERE "IS_OFFLINE" = 0) AS "NU_ONLINE_HOSTS",
        (SELECT COUNT(*) FROM "BPDATA"."HOST" WHERE "IS_OFFLINE" <> 0) AS "NU_OFFLINE_HOSTS",
        (SELECT COUNT(*) FROM "BPDATA"."HOST" WHERE "IS_BUSY" <> 0) AS "NU_BUSY_HOSTS",
        COALESCE(SUM(m."NU_DISCOVERED_FILES_TOTAL"), 0) AS "NU_DISCOVERED_FILES_TOTAL",
        COALESCE(SUM(m."VL_DISCOVERED_GB_TOTAL"), 0) AS "VL_DISCOVERED_GB_TOTAL",
        COALESCE(SUM(m."NU_BACKUP_DONE_FILES_TOTAL"), 0) AS "NU_BACKUP_DONE_FILES_TOTAL",
        COALESCE(SUM(m."VL_BACKUP_DONE_GB_TOTAL"), 0) AS "VL_BACKUP_DONE_GB_TOTAL",
        COALESCE(SUM(m."NU_BACKUP_PENDING_FILES_CURRENT"), 0) AS "NU_BACKUP_PENDING_FILES_TOTAL",
        COALESCE(SUM(m."VL_BACKUP_PENDING_GB_CURRENT"), 0) AS "VL_BACKUP_PENDING_GB_TOTAL",
        COALESCE(SUM(m."NU_BACKUP_ERROR_FILES_CURRENT"), 0) AS "NU_BACKUP_ERROR_FILES_TOTAL",
        COALESCE(SUM(m."VL_BACKUP_ERROR_GB_CURRENT"), 0) AS "VL_BACKUP_ERROR_GB_TOTAL",
        COALESCE(SUM(m."NU_BACKUP_SUSPENDED_FILES_CURRENT"), 0) AS "NU_BACKUP_SUSPENDED_FILES_TOTAL",
        COALESCE(SUM(m."VL_BACKUP_SUSPENDED_GB_CURRENT"), 0) AS "VL_BACKUP_SUSPENDED_GB_TOTAL",
        COALESCE(SUM(m."NU_BACKUP_QUEUE_FILES_TOTAL"), 0) AS "NU_BACKUP_QUEUE_FILES_TOTAL",
        COALESCE(SUM(m."VL_BACKUP_QUEUE_GB_TOTAL"), 0) AS "VL_BACKUP_QUEUE_GB_TOTAL",
        COALESCE(SUM(m."NU_BACKUP_QUEUE_RUNNING_FILES_TOTAL"), 0) AS "NU_BACKUP_QUEUE_RUNNING_FILES_TOTAL",
        COALESCE(SUM(m."VL_BACKUP_QUEUE_RUNNING_GB_TOTAL"), 0) AS "VL_BACKUP_QUEUE_RUNNING_GB_TOTAL",
        COALESCE(SUM(m."NU_BACKUP_QUEUE_SUSPENDED_FILES_TOTAL"), 0) AS "NU_BACKUP_QUEUE_SUSPENDED_FILES_TOTAL",
        COALESCE(SUM(m."VL_BACKUP_QUEUE_SUSPENDED_GB_TOTAL"), 0) AS "VL_BACKUP_QUEUE_SUSPENDED_GB_TOTAL",
        COALESCE(SUM(m."NU_PROCESSING_PENDING_FILES_CURRENT"), 0) AS "NU_PROCESSING_PENDING_FILES_TOTAL",
        COALESCE(SUM(m."VL_PROCESSING_PENDING_GB_CURRENT"), 0) AS "VL_PROCESSING_PENDING_GB_TOTAL",
        COALESCE(SUM(m."NU_PROCESSING_DONE_FILES_TOTAL"), 0) AS "NU_PROCESSING_DONE_FILES_TOTAL",
        COALESCE(SUM(m."VL_PROCESSING_DONE_GB_TOTAL"), 0) AS "VL_PROCESSING_DONE_GB_TOTAL",
        COALESCE(SUM(m."NU_PROCESSING_ERROR_FILES_CURRENT"), 0) AS "NU_PROCESSING_ERROR_FILES_TOTAL",
        COALESCE(SUM(m."VL_PROCESSING_ERROR_GB_CURRENT"), 0) AS "VL_PROCESSING_ERROR_GB_TOTAL",
        COALESCE(SUM(m."NU_PROCESSING_FROZEN_FILES_CURRENT"), 0) AS "NU_PROCESSING_FROZEN_FILES_TOTAL",
        COALESCE(SUM(m."VL_PROCESSING_FROZEN_GB_CURRENT"), 0) AS "VL_PROCESSING_FROZEN_GB_TOTAL",
        COALESCE(SUM(m."NU_PROCESSING_QUEUE_FILES_TOTAL"), 0) AS "NU_PROCESSING_QUEUE_FILES_TOTAL",
        COALESCE(SUM(m."VL_PROCESSING_QUEUE_GB_TOTAL"), 0) AS "VL_PROCESSING_QUEUE_GB_TOTAL",
        COALESCE(SUM(m."NU_PROCESSING_QUEUE_RUNNING_FILES_TOTAL"), 0) AS "NU_PROCESSING_QUEUE_RUNNING_FILES_TOTAL",
        COALESCE(SUM(m."VL_PROCESSING_QUEUE_RUNNING_GB_TOTAL"), 0) AS "VL_PROCESSING_QUEUE_RUNNING_GB_TOTAL",
        COALESCE(SUM(m."NU_PROCESSING_QUEUE_FROZEN_FILES_TOTAL"), 0) AS "NU_PROCESSING_QUEUE_FROZEN_FILES_TOTAL",
        COALESCE(SUM(m."VL_PROCESSING_QUEUE_FROZEN_GB_TOTAL"), 0) AS "VL_PROCESSING_QUEUE_FROZEN_GB_TOTAL",
        (SELECT COALESCE(SUM("NU_SPECTRUM_COUNT"), 0) FROM "RFFUSION_SUMMARY"."SITE_EQUIPMENT_METRIC") AS "NU_FACT_SPECTRUM_TOTAL",
        COALESCE(SUM(m."NU_PAYLOAD_DELETED_FILES_TOTAL"), 0) AS "NU_PAYLOAD_DELETED_FILES_TOTAL",
        COALESCE(SUM(m."VL_PAYLOAD_DELETED_GB_TOTAL"), 0) AS "VL_PAYLOAD_DELETED_GB_TOTAL",
        COALESCE(SUM(CASE WHEN m."DT_REFERENCE_MONTH" = date_trunc('month', CURRENT_DATE)::date THEN m."NU_BACKUP_DONE_THIS_MONTH" ELSE 0 END), 0) AS "NU_BACKUP_DONE_THIS_MONTH",
        COALESCE(SUM(CASE WHEN m."DT_REFERENCE_MONTH" = date_trunc('month', CURRENT_DATE)::date THEN m."VL_BACKUP_DONE_GB_THIS_MONTH" ELSE 0 END), 0) AS "VL_BACKUP_DONE_GB_THIS_MONTH"
    FROM "RFFUSION_SUMMARY"."HOST_METRIC" m
    '''


def server_errors_query() -> str:
    """Compose server error groups from the canonical host error metrics.

    Args:
        None.

    Returns:
        str: Counts grouped by error scope, task state, error code and error summary,
            preserving the existing grouping granularity. The caller embeds this fixed
            SELECT and binds its own values.
    """
    return '''
    SELECT "NA_ERROR_SCOPE", "NA_TASK_STATE", "NA_ERROR_CODE", "NA_ERROR_SUMMARY",
        SUM("NU_ERROR_COUNT") AS "NU_ERROR_COUNT"
    FROM "RFFUSION_SUMMARY"."HOST_ERROR_METRIC"
    GROUP BY "NA_ERROR_SCOPE", "NA_TASK_STATE", "NA_ERROR_CODE", "NA_ERROR_SUMMARY"
    '''
