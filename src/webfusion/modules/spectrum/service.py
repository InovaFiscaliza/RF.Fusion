"""Service queries for the spectrum and file views.

This module keeps the SQL close to the feature because the result grain matters
to the user interface:

- spectrum mode is intentionally one-row-per-spectrum
- file mode is intentionally one-row-per-repository-file

Those are not interchangeable queries with a different template. Each mode has
different allowed filters, grouping rules, sorting semantics, and download
behavior, so the service layer documents those differences explicitly.
"""

import logging
import os
import time
from datetime import datetime
from db import get_connection_rfdata as get_connection, get_connection_summary

LOGGER = logging.getLogger(__name__)

ALLOWED_SORT_FIELDS = {
    "date_start": "f.DT_TIME_START",
    "date_end": "f.DT_TIME_END",
    "freq_start": "f.NU_FREQ_START",
    "freq_end": "f.NU_FREQ_END",
    "rbw": "f.NU_RBW",
    "trace_count": "f.NU_TRACE_COUNT",
}

ALLOWED_SORT_ORDERS = ["ASC", "DESC"]

ALLOWED_FILE_SORT_FIELDS = {
    "date_start": "ctx.DT_TIME_START",
    "date_end": "ctx.DT_TIME_END",
    "file_name": "repos.NA_FILE",
    "spectrum_count": "stats.NU_SPECTRA",
}

SPECTRUM_QUERY_CACHE_TTL_SECONDS = 30
EQUIPMENT_CACHE_TTL_SECONDS = 300
FILE_PATH_CACHE_TTL_SECONDS = 300
# Full file-result pages reuse one heavy grouped query result for a few minutes
# so operators can move across pagination links without re-scanning the catalog.
FILE_RESULT_FULL_CACHE_TTL_SECONDS = 300
FILE_RESULT_CACHE_VERSION = 2

# These are intentionally tiny in-process caches. They smooth repeated clicks
# and back/forward navigation in one worker process, but they are not meant to
# be treated as a shared or authoritative cache layer.
_EQUIPMENT_CACHE = {"expires_at": 0.0, "value": None}
_SPECTRUM_QUERY_CACHE = {}
_FILE_PATH_CACHE = {}


def _load_summary_site_availability_row(equipment_id, site_id):
    """Load one equipment/site availability row from ``SITE_EQUIPMENT_OBS_SUMMARY``."""

    conn = get_connection_summary()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            FK_SITE AS ID_SITE,
            NA_SITE_LABEL AS LOCALITY_LABEL,
            DT_FIRST_SEEN_AT AS DATE_START,
            DT_LAST_SEEN_AT AS DATE_END,
            NU_SPECTRUM_COUNT AS SPECTRUM_COUNT
        FROM SITE_EQUIPMENT_OBS_SUMMARY
        WHERE FK_EQUIPMENT = %s
          AND FK_SITE = %s
        LIMIT 1
        """,
        (equipment_id, site_id),
    )
    row = cur.fetchone()
    conn.close()
    return row


def _load_summary_equipment_rows():
    """Load the equipment selector from summary-backed observations."""

    conn = get_connection_summary()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT
            FK_EQUIPMENT AS ID_EQUIPMENT,
            NA_EQUIPMENT
        FROM SITE_EQUIPMENT_OBS_SUMMARY
        WHERE NA_EQUIPMENT IS NOT NULL
          AND NA_EQUIPMENT <> ''
        ORDER BY NA_EQUIPMENT
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def _load_fact_equipment_rows():
    """Load the equipment selector directly from ``RFDATA`` as a fallback."""

    conn = get_connection()
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


def _coerce_text(value):
    """Normalize text values returned by different MySQL drivers."""

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    if value is None:
        return ""

    return str(value)


def _build_locality_base_sql(site_alias="s", district_alias="d", county_alias="c"):
    """Build the SQL fragment that resolves the best locality label."""

    return (
        f"COALESCE("
        f"NULLIF({site_alias}.NA_SITE, ''), "
        f"NULLIF({district_alias}.NA_DISTRICT, ''), "
        f"{county_alias}.NA_COUNTY, "
        f"CONCAT('Site ', {site_alias}.ID_SITE)"
        f")"
    )


def _build_text_difference_sql(left_expr, right_expr):
    """Compare text expressions with an explicit shared collation.

    The live schema mixes ``utf8mb4_general_ci`` and ``utf8mb4_unicode_ci`` in
    geographic dimensions. Comparing the columns directly can fail with MySQL
    error 1267, so locality labels normalize both sides before checking whether
    the site label already matches the county name.
    """

    left_sql = (
        f"COALESCE(CONVERT({left_expr} USING utf8mb4) "
        f"COLLATE utf8mb4_unicode_ci, '')"
    )
    right_sql = (
        f"COALESCE(CONVERT({right_expr} USING utf8mb4) "
        f"COLLATE utf8mb4_unicode_ci, '')"
    )
    return f"NOT ({left_sql} <=> {right_sql})"


def _build_locality_display_sql(
    site_alias="s",
    district_alias="d",
    county_alias="c",
    state_alias="st",
):
    """Build a user-facing locality label with county/state context.

    The county/state complement is rendered in parentheses when the site label
    differs from the municipality. This avoids labels such as
    ``Brasilia · Belem/PA``, which can read like two simultaneous locations to
    operators, even though the intent is ``site name inside county/state``.
    """

    base_sql = _build_locality_base_sql(
        site_alias=site_alias,
        district_alias=district_alias,
        county_alias=county_alias,
    )
    site_differs_from_county_sql = _build_text_difference_sql(
        f"{site_alias}.NA_SITE",
        f"{county_alias}.NA_COUNTY",
    )
    state_suffix_sql = (
        f"CASE "
        f"WHEN {state_alias}.LC_STATE IS NOT NULL "
        f"THEN CONCAT('/', {state_alias}.LC_STATE) "
        f"ELSE '' "
        f"END"
    )
    return f"""
        TRIM(
            CONCAT(
                {base_sql},
                CASE
                    WHEN {county_alias}.NA_COUNTY IS NOT NULL
                     AND (
                        {site_alias}.NA_SITE IS NULL
                        OR {site_alias}.NA_SITE = ''
                        OR {site_differs_from_county_sql}
                     )
                    THEN CONCAT(' (', {county_alias}.NA_COUNTY, {state_suffix_sql}, ')')
                    WHEN {state_alias}.LC_STATE IS NOT NULL
                    THEN CONCAT('/', {state_alias}.LC_STATE)
                    ELSE ''
                END,
                ''
            )
        )
    """


def _build_fact_filters(
    *,
    equipment_id=None,
    site_id=None,
    start_date=None,
    end_date=None,
    freq_start=None,
    freq_end=None,
    description=None,
    fact_alias="f",
    include_freq=True,
    include_description=True,
):
    """Build reusable ``FACT_SPECTRUM`` WHERE clauses.

    The same helper feeds two related result shapes:

    - spectrum-oriented rows, where frequency/description filters are natural
    - file-oriented rows derived from spectra, where callers may choose whether
      those spectrum-specific filters should participate in the result set

    ``include_freq`` and ``include_description`` exist so the service can keep
    one source of truth for SQL predicates while still making that business
    choice explicitly at each call site.
    """

    where_clauses = []
    params = []

    if equipment_id:
        where_clauses.append(f"{fact_alias}.FK_EQUIPMENT = %s")
        params.append(equipment_id)

    if site_id:
        where_clauses.append(f"{fact_alias}.FK_SITE = %s")
        params.append(site_id)

    if start_date:
        where_clauses.append(f"{fact_alias}.DT_TIME_END >= %s")
        params.append(start_date)

    if end_date:
        where_clauses.append(f"{fact_alias}.DT_TIME_START <= %s")
        params.append(end_date + " 23:59:59")

    if include_freq and freq_start is not None and freq_end is not None:
        where_clauses.append(f"{fact_alias}.NU_FREQ_START >= %s")
        params.append(freq_start)
        where_clauses.append(f"{fact_alias}.NU_FREQ_END <= %s")
        params.append(freq_end)
    elif include_freq and freq_start is not None:
        where_clauses.append(f"{fact_alias}.NU_FREQ_START >= %s")
        params.append(freq_start)
    elif include_freq and freq_end is not None:
        where_clauses.append(f"{fact_alias}.NU_FREQ_END <= %s")
        params.append(freq_end)

    if include_description and description:
        where_clauses.append(f"{fact_alias}.NA_DESCRIPTION LIKE %s")
        params.append(f"%{description}%")

    return where_clauses, params


def _build_where_sql(where_clauses):
    """Join WHERE clauses only when the filter list is not empty."""

    if not where_clauses:
        return ""

    return "WHERE " + " AND ".join(where_clauses)


def _format_br_datetime(value):
    """Format timestamps for Brazilian-facing UI surfaces."""

    if value in (None, ""):
        return value

    text = _coerce_text(value).strip()

    if not text:
        return value

    normalized_text = text.replace("Z", "+00:00")

    try:
        parsed = datetime.fromisoformat(normalized_text.replace(" ", "T"))
    except ValueError:
        return text

    if (
        parsed.hour == 0
        and parsed.minute == 0
        and parsed.second == 0
        and parsed.microsecond == 0
    ):
        return parsed.strftime("%d-%m-%Y")

    return parsed.strftime("%d-%m-%Y %H:%M:%S")


def _format_row_datetime_fields(rows, *field_names):
    """Format datetime-like fields in each row for template/API output."""

    for row in rows:
        for field_name in field_names:
            if field_name in row:
                row[field_name] = _format_br_datetime(row.get(field_name))


def _format_single_row_datetime_fields(row, *field_names):
    """Format datetime-like fields in one row for template/API output."""

    if not row:
        return row

    for field_name in field_names:
        if field_name in row:
            row[field_name] = _format_br_datetime(row.get(field_name))

    return row


def _postprocess_file_rows(rows):
    """Normalize aggregated locality labels for file-mode rows.

    SQL groups file-mode results by repository file, so locality labels arrive
    as a concatenated list. The UI wants two different presentations:

    - a short display label for the main table cell
    - a verbose list for tooltip/detail style surfaces

    Doing that split in Python keeps the SQL readable and avoids embedding UI
    phrasing such as ``"X localidades"`` inside the query itself.
    """

    for row in rows:
        raw_labels = [
            part.strip()
            for part in _coerce_text(row.get("LOCALITY_LABELS")).split("||")
            if part and part.strip()
        ]
        row["LOCALITY_COUNT"] = int(row.get("LOCALITY_COUNT") or 0)

        if len(raw_labels) == 1:
            row["LOCALITY_DISPLAY"] = raw_labels[0]
        elif len(raw_labels) > 1:
            row["LOCALITY_DISPLAY"] = f"{len(raw_labels)} localidades"
        else:
            row["LOCALITY_DISPLAY"] = "—"

        row["LOCALITY_DETAILS"] = " | ".join(raw_labels)


def _finalize_locality_options(rows):
    """Add stable option labels for the dynamic locality filter.

    Different ``ID_SITE`` values can collapse to the same human label after the
    locality formatter runs. When that happens the select box would otherwise
    show visually duplicated options, so the site id is appended only in the
    ambiguous cases.
    """

    label_counts = {}

    for row in rows:
        label = _coerce_text(row.get("LOCALITY_LABEL") or f"Site {row['ID_SITE']}")
        label_counts[label] = label_counts.get(label, 0) + 1

    options = []

    for row in rows:
        site_id = int(row["ID_SITE"])
        label = _coerce_text(row.get("LOCALITY_LABEL") or f"Site {site_id}")
        option_label = (
            f"{label} (site {site_id})"
            if label_counts[label] > 1
            else label
        )
        options.append(
            {
                "ID_SITE": site_id,
                "LOCALITY_LABEL": label,
                "OPTION_LABEL": option_label,
                "COUNTY_NAME": row.get("COUNTY_NAME"),
                "STATE_CODE": row.get("STATE_CODE"),
                "SPECTRUM_COUNT": int(row.get("SPECTRUM_COUNT") or 0),
                "DATE_START": row.get("DATE_START"),
                "DATE_END": row.get("DATE_END"),
            }
        )

    return options


def _get_cached_query(cache_key):
    """Return a cached query result when it is still fresh.

    These helpers are deliberately dumb: TTL-only, in-memory, and per-process.
    That keeps the service layer predictable while still saving repeated trips
    during quick filter tweaks or page navigation.
    """
    cached = _SPECTRUM_QUERY_CACHE.get(cache_key)

    if not cached:
        return None

    if cached["expires_at"] <= time.time():
        _SPECTRUM_QUERY_CACHE.pop(cache_key, None)
        return None

    return cached["value"]


def _set_cached_query(cache_key, value):
    """Cache one query result for a short per-process TTL."""
    _set_cached_query_with_ttl(
        cache_key,
        value,
        SPECTRUM_QUERY_CACHE_TTL_SECONDS,
    )


def _set_cached_query_with_ttl(cache_key, value, ttl_seconds):
    """Cache one query result with a caller-provided TTL."""
    _SPECTRUM_QUERY_CACHE[cache_key] = {
        "expires_at": time.time() + ttl_seconds,
        "value": value,
    }


def _get_cached_file_path(cache_key):
    """Return a cached repository file path when the lookup is still fresh."""
    cached = _FILE_PATH_CACHE.get(cache_key)

    if not cached:
        return None

    if cached["expires_at"] <= time.time():
        _FILE_PATH_CACHE.pop(cache_key, None)
        return None

    return cached["value"]


def _set_cached_file_path(cache_key, value):
    """Cache repository file resolution for repeated download actions."""
    _FILE_PATH_CACHE[cache_key] = {
        "expires_at": time.time() + FILE_PATH_CACHE_TTL_SECONDS,
        "value": value,
    }


def _reduce_latest_repo_file_rows(repo_rows):
    """Keep only the newest repository file row for each spectrum.

    ``DIM_SPECTRUM_FILE.ID_FILE`` is treated as a monotonic proxy for the most
    recent repository entry. That assumption matches the legacy operational
    download flow already exposed by the module.
    """

    latest_by_spectrum = {}

    for row in repo_rows:
        spectrum_id = row["ID_SPECTRUM"]
        current = latest_by_spectrum.get(spectrum_id)

        if current is None or int(row.get("ID_FILE") or 0) > int(
            current.get("ID_FILE") or 0
        ):
            latest_by_spectrum[spectrum_id] = row

    return latest_by_spectrum


def _attach_repository_file_metadata(rows, latest_repo_files):
    """Copy latest repository file metadata into the paginated spectrum rows."""

    for row in rows:
        file_row = latest_repo_files.get(row["ID_SPECTRUM"], {})
        row["NA_PATH"] = file_row.get("NA_PATH")
        row["NA_FILE"] = file_row.get("NA_FILE")
        row["NA_EXTENSION"] = file_row.get("NA_EXTENSION")
        row["VL_FILE_SIZE_KB"] = file_row.get("VL_FILE_SIZE_KB")

    return rows


def _fetch_latest_repo_files_for_spectra(cur, spectrum_ids):
    """Load repository file metadata only for the spectra shown on the page."""

    if not spectrum_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(spectrum_ids))
    cur.execute(
        f"""
        SELECT
            b.FK_SPECTRUM AS ID_SPECTRUM,
            d.ID_FILE,
            d.NA_PATH,
            d.NA_FILE,
            d.NA_EXTENSION,
            d.VL_FILE_SIZE_KB
        FROM BRIDGE_SPECTRUM_FILE b
        JOIN DIM_SPECTRUM_FILE d
            ON d.ID_FILE = b.FK_FILE
        WHERE d.NA_VOLUME = 'reposfi'
          AND b.FK_SPECTRUM IN ({placeholders})
        ORDER BY b.FK_SPECTRUM ASC, d.ID_FILE DESC
        """,
        spectrum_ids,
    )
    return _reduce_latest_repo_file_rows(cur.fetchall())


def get_equipments():
    """Return the equipment list used by the spectrum filters.

    This list changes rarely compared with the pace of page navigation, so a
    slightly longer cache is acceptable here and keeps the filter chrome fast.
    """
    now = time.time()

    if (
        _EQUIPMENT_CACHE["value"] is not None
        and _EQUIPMENT_CACHE["expires_at"] > now
    ):
        return _EQUIPMENT_CACHE["value"]

    result = []

    try:
        result = _load_summary_equipment_rows()
    except Exception:
        LOGGER.exception("failed_to_load_summary_spectrum_equipments")

    if not result:
        result = _load_fact_equipment_rows()

    if result:
        _EQUIPMENT_CACHE["value"] = result
        _EQUIPMENT_CACHE["expires_at"] = now + EQUIPMENT_CACHE_TTL_SECONDS
    return result


def get_spectrum_data(
    equipment_id=None,
    site_id=None,
    start_date=None,
    end_date=None,
    freq_start=None,
    freq_end=None,
    description=None,
    sort_by="date_start",
    sort_order="DESC",
    page=1,
    page_size=50,
):
    """Return paginated spectrum rows plus the total count."""
    if sort_by not in ALLOWED_SORT_FIELDS:
        sort_by = "date_start"

    if sort_order not in ALLOWED_SORT_ORDERS:
        sort_order = "DESC"

    try:
        page = int(page)
        if page < 1:
            page = 1
    except Exception:
        page = 1

    cache_key = (
        "spectrum",
        equipment_id,
        site_id,
        start_date,
        end_date,
        freq_start,
        freq_end,
        description,
        sort_by,
        sort_order,
        page,
        page_size,
    )
    cached = _get_cached_query(cache_key)

    if cached is not None:
        return cached

    where_clauses, params = _build_fact_filters(
        equipment_id=equipment_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias="f",
    )
    where_sql = _build_where_sql(where_clauses)
    locality_display_sql = _build_locality_display_sql()

    order_sql = f"""
        ORDER BY {ALLOWED_SORT_FIELDS[sort_by]} {sort_order},
                 f.ID_SPECTRUM DESC
    """

    offset = (page - 1) * page_size
    limit_sql = "LIMIT %s OFFSET %s"
    data_params = params + [page_size, offset]

    data_query = f"""
        SELECT
            f.ID_SPECTRUM,
            f.NA_DESCRIPTION,
            f.NU_FREQ_START,
            f.NU_FREQ_END,
            f.DT_TIME_START,
            f.DT_TIME_END,
            f.NU_TRACE_COUNT,
            f.NU_TRACE_LENGTH,
            f.NU_RBW,
            f.NU_VBW,
            f.NU_ATT_GAIN,
            e.NA_EQUIPMENT,
            f.FK_SITE AS ID_SITE,
            {locality_display_sql} AS LOCALITY_LABEL,
            c.NA_COUNTY AS COUNTY_NAME,
            st.LC_STATE AS STATE_CODE
        FROM FACT_SPECTRUM f
        JOIN DIM_SPECTRUM_EQUIPMENT e
            ON e.ID_EQUIPMENT = f.FK_EQUIPMENT
        JOIN DIM_SPECTRUM_SITE s
            ON s.ID_SITE = f.FK_SITE
        LEFT JOIN DIM_SITE_DISTRICT d
            ON d.ID_DISTRICT = s.FK_DISTRICT
        LEFT JOIN DIM_SITE_COUNTY c
            ON c.ID_COUNTY = s.FK_COUNTY
        LEFT JOIN DIM_SITE_STATE st
            ON st.ID_STATE = s.FK_STATE
        {where_sql}
        {order_sql}
        {limit_sql}
    """

    count_query = f"""
        SELECT COUNT(*) AS total
        FROM FACT_SPECTRUM f
        {where_sql}
    """

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(data_query, data_params)
    rows = cur.fetchall()
    spectrum_ids = [row["ID_SPECTRUM"] for row in rows]
    latest_repo_files = _fetch_latest_repo_files_for_spectra(cur, spectrum_ids)
    _attach_repository_file_metadata(rows, latest_repo_files)
    _format_row_datetime_fields(rows, "DT_TIME_START", "DT_TIME_END")

    cur.execute(count_query, params)
    result = cur.fetchone()
    total = result["total"] if result else 0

    conn.close()

    result = (rows, total)
    _set_cached_query(cache_key, result)
    return result


def get_spectrum_file_data(
    equipment_id=None,
    site_id=None,
    start_date=None,
    end_date=None,
    freq_start=None,
    freq_end=None,
    description=None,
    sort_by="date_start",
    sort_order="DESC",
    page=1,
    page_size=50
):
    """Return paginated file-mode rows plus the total count.

    The only active search semantics are spectrum-aware: filters always apply
    to spectra first. The result list shown to the user is then collapsed to
    unique repository files, one row per file.

    That preserves the more functional spectrum search while avoiding a UI that
    exposes internal spectrum identifiers as the primary result grain.
    """
    if sort_by not in ALLOWED_FILE_SORT_FIELDS:
        sort_by = "date_start"

    if sort_order not in ALLOWED_SORT_ORDERS:
        sort_order = "DESC"

    try:
        page = int(page)
        if page < 1:
            page = 1
    except Exception:
        page = 1

    cache_key = (
        "file",
        FILE_RESULT_CACHE_VERSION,
        equipment_id,
        site_id,
        start_date,
        end_date,
        freq_start,
        freq_end,
        description,
        sort_by,
        sort_order,
        page,
        page_size,
    )
    cached = _get_cached_query(cache_key)

    if cached is not None:
        return cached

    # Pagination should not force the grouped query to run again for every
    # page transition, so one full result set is cached per filter/sort state.
    full_cache_key = (
        "file_full",
        FILE_RESULT_CACHE_VERSION,
        equipment_id,
        site_id,
        start_date,
        end_date,
        freq_start,
        freq_end,
        description,
        sort_by,
        sort_order,
    )
    cached_full = _get_cached_query(full_cache_key)

    if cached_full is not None:
        full_rows, total = cached_full
        offset = (page - 1) * page_size
        page_rows = [dict(row) for row in full_rows[offset:offset + page_size]]
        result = (page_rows, total)
        _set_cached_query(cache_key, result)
        return result

    locality_display_sql = _build_locality_display_sql()
    where_sql = "WHERE repos.NA_VOLUME = 'reposfi'"

    order_sql = f"""
        ORDER BY {ALLOWED_FILE_SORT_FIELDS[sort_by]} {sort_order},
                 repos.ID_FILE DESC
    """

    fact_source_alias = "fs"
    fact_where_clauses, fact_params = _build_fact_filters(
        equipment_id=equipment_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias=fact_source_alias,
        include_freq=True,
        include_description=True,
    )
    fact_where_sql = _build_where_sql(fact_where_clauses)

    # The filtered FACT_SPECTRUM subquery is the true search. After spectra are
    # identified, the result is collapsed to unique repository files for UI
    # rendering and download workflows.
    filtered_spectra_sql = f"""
        SELECT
            ID_SPECTRUM,
            FK_SITE,
            DT_TIME_START,
            DT_TIME_END,
            NU_FREQ_START,
            NU_FREQ_END
        FROM FACT_SPECTRUM {fact_source_alias}
        {fact_where_sql}
    """

    # The search still matches files via filtered spectra, but the UI should
    # show how many spectra exist in the whole file once that file qualifies.
    data_query = f"""
        SELECT
            repos.ID_FILE,
            repos.NA_PATH,
            repos.NA_FILE,
            repos.NA_EXTENSION,
            repos.VL_FILE_SIZE_KB,
            ctx.DT_TIME_START,
            ctx.DT_TIME_END,
            ctx.NU_FREQ_START,
            ctx.NU_FREQ_END,
            stats.NU_SPECTRA,
            ctx.LOCALITY_COUNT,
            ctx.LOCALITY_LABELS
        FROM (
            SELECT DISTINCT
                repos.ID_FILE,
                repos.NA_PATH,
                repos.NA_FILE,
                repos.NA_EXTENSION,
                repos.VL_FILE_SIZE_KB
            FROM ({filtered_spectra_sql}) f
            JOIN BRIDGE_SPECTRUM_FILE b
                ON b.FK_SPECTRUM = f.ID_SPECTRUM
            JOIN DIM_SPECTRUM_FILE repos
                ON repos.ID_FILE = b.FK_FILE
            {where_sql}
        ) repos
        JOIN (
            SELECT
                repos.ID_FILE,
                MIN(f.DT_TIME_START) AS DT_TIME_START,
                MAX(f.DT_TIME_END) AS DT_TIME_END,
                MIN(f.NU_FREQ_START) AS NU_FREQ_START,
                MAX(f.NU_FREQ_END) AS NU_FREQ_END,
                COUNT(DISTINCT s.ID_SITE) AS LOCALITY_COUNT,
                GROUP_CONCAT(
                    DISTINCT {locality_display_sql}
                    ORDER BY {locality_display_sql} SEPARATOR '||'
                ) AS LOCALITY_LABELS
            FROM ({filtered_spectra_sql}) f
            JOIN BRIDGE_SPECTRUM_FILE b
                ON b.FK_SPECTRUM = f.ID_SPECTRUM
            JOIN DIM_SPECTRUM_FILE repos
                ON repos.ID_FILE = b.FK_FILE
            JOIN DIM_SPECTRUM_SITE s
                ON s.ID_SITE = f.FK_SITE
            LEFT JOIN DIM_SITE_DISTRICT d
                ON d.ID_DISTRICT = s.FK_DISTRICT
            LEFT JOIN DIM_SITE_COUNTY c
                ON c.ID_COUNTY = s.FK_COUNTY
            LEFT JOIN DIM_SITE_STATE st
                ON st.ID_STATE = s.FK_STATE
            {where_sql}
            GROUP BY repos.ID_FILE
        ) ctx
            ON ctx.ID_FILE = repos.ID_FILE
        JOIN (
            SELECT
                b.FK_FILE AS ID_FILE,
                COUNT(DISTINCT b.FK_SPECTRUM) AS NU_SPECTRA
            FROM BRIDGE_SPECTRUM_FILE b
            JOIN (
                SELECT DISTINCT repos.ID_FILE
                FROM ({filtered_spectra_sql}) f
                JOIN BRIDGE_SPECTRUM_FILE b
                    ON b.FK_SPECTRUM = f.ID_SPECTRUM
                JOIN DIM_SPECTRUM_FILE repos
                    ON repos.ID_FILE = b.FK_FILE
                {where_sql}
            ) matched_files
                ON matched_files.ID_FILE = b.FK_FILE
            GROUP BY b.FK_FILE
        ) stats
            ON stats.ID_FILE = repos.ID_FILE
        {order_sql}
    """

    conn = get_connection()
    cur = conn.cursor()

    data_params = fact_params + fact_params + fact_params
    cur.execute(data_query, data_params)
    full_rows = cur.fetchall()

    conn.close()

    _format_row_datetime_fields(full_rows, "DT_TIME_START", "DT_TIME_END")
    _postprocess_file_rows(full_rows)

    total = len(full_rows)
    _set_cached_query_with_ttl(
        full_cache_key,
        (full_rows, total),
        FILE_RESULT_FULL_CACHE_TTL_SECONDS,
    )

    offset = (page - 1) * page_size
    rows = [dict(row) for row in full_rows[offset:offset + page_size]]

    result = (rows, total)
    _set_cached_query(cache_key, result)
    return result


def _load_fact_locality_rows(
    *,
    equipment_id=None,
    start_date=None,
    end_date=None,
    freq_start=None,
    freq_end=None,
    description=None,
):
    """Load locality options directly from the live spectrum catalog.

    The selector follows the same spectrum-aware filters as the main search so
    operators do not pick a locality that cannot produce any file result.
    """

    where_clauses, params = _build_fact_filters(
        equipment_id=equipment_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias="f",
    )
    where_sql = _build_where_sql(where_clauses)
    locality_display_sql = _build_locality_display_sql()

    query = f"""
        SELECT
            s.ID_SITE,
            {locality_display_sql} AS LOCALITY_LABEL,
            c.NA_COUNTY AS COUNTY_NAME,
            st.LC_STATE AS STATE_CODE,
            MIN(f.DT_TIME_START) AS DATE_START,
            MAX(f.DT_TIME_END) AS DATE_END,
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
        {where_sql}
        GROUP BY
            s.ID_SITE,
            s.NA_SITE,
            d.NA_DISTRICT,
            c.NA_COUNTY,
            st.LC_STATE
        ORDER BY
            MAX(f.DT_TIME_END) DESC,
            LOCALITY_LABEL ASC
    """

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    _format_row_datetime_fields(rows, "DATE_START", "DATE_END")
    return rows


def _load_fact_site_availability_row(equipment_id, site_id):
    """Load one equipment/site availability row directly from ``FACT_SPECTRUM``."""

    locality_display_sql = _build_locality_display_sql()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            s.ID_SITE,
            {locality_display_sql} AS LOCALITY_LABEL,
            MIN(f.DT_TIME_START) AS DATE_START,
            MAX(f.DT_TIME_END) AS DATE_END,
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
        WHERE f.FK_EQUIPMENT = %s
          AND f.FK_SITE = %s
        GROUP BY
            s.ID_SITE,
            s.NA_SITE,
            d.NA_DISTRICT,
            c.NA_COUNTY,
            st.LC_STATE
        LIMIT 1
        """,
        (equipment_id, site_id),
    )
    row = cur.fetchone()
    conn.close()
    return _format_single_row_datetime_fields(row, "DATE_START", "DATE_END")


def get_spectrum_locality_options(
    *,
    equipment_id=None,
    start_date=None,
    end_date=None,
    freq_start=None,
    freq_end=None,
    description=None,
):
    """Return locality options aligned with the current unified search."""

    cache_key = (
        "locality_options",
        equipment_id,
        start_date,
        end_date,
        freq_start,
        freq_end,
        description,
    )
    cached = _get_cached_query(cache_key)

    if cached is not None:
        return cached

    rows = _load_fact_locality_rows(
        equipment_id=equipment_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
    )

    result = _finalize_locality_options(rows)

    if result:
        _set_cached_query(cache_key, result)

    return result


def get_spectrum_site_option(site_id):
    """Return one locality option by site id for preselected map navigation.

    This supports deep-link scenarios where the route already knows ``site_id``
    and needs to rehydrate the select option even before the full dependent
    locality query runs in the browser.
    """

    if site_id in (None, ""):
        return None

    locality_display_sql = _build_locality_display_sql()
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            s.ID_SITE,
            {locality_display_sql} AS LOCALITY_LABEL,
            c.NA_COUNTY AS COUNTY_NAME,
            st.LC_STATE AS STATE_CODE
        FROM DIM_SPECTRUM_SITE s
        LEFT JOIN DIM_SITE_DISTRICT d
            ON d.ID_DISTRICT = s.FK_DISTRICT
        LEFT JOIN DIM_SITE_COUNTY c
            ON c.ID_COUNTY = s.FK_COUNTY
        LEFT JOIN DIM_SITE_STATE st
            ON st.ID_STATE = s.FK_STATE
        WHERE s.ID_SITE = %s
        LIMIT 1
        """,
        (site_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return _finalize_locality_options([row])[0]


def get_spectrum_site_availability_range(*, equipment_id=None, site_id=None):
    """Return the observed date range for one equipment/locality pair.

    The route uses this as a lightweight hint when a preselected site no longer
    appears in the current locality list. The goal is explanatory UX
    ("available from X to Y here"), not a full result query.
    """

    if equipment_id in (None, "") or site_id in (None, ""):
        return None

    cache_key = ("site_availability_range", equipment_id, site_id)
    cached = _get_cached_query(cache_key)

    if cached is not None:
        return cached

    row = None

    try:
        row = _load_summary_site_availability_row(equipment_id, site_id)
    except Exception:
        LOGGER.exception(
            "failed_to_load_summary_spectrum_site_availability equipment_id=%s site_id=%s",
            equipment_id,
            site_id,
        )

    if not row:
        row = _load_fact_site_availability_row(equipment_id, site_id)

    if row:
        row["SPECTRUM_COUNT"] = int(row.get("SPECTRUM_COUNT") or 0)
        _format_single_row_datetime_fields(row, "DATE_START", "DATE_END")

    if row:
        _set_cached_query(cache_key, row)

    return row


def get_file_by_spectrum_id(spectrum_id):
    """Resolve the repository path for a single spectrum identifier.

    The unified page no longer renders one row per spectrum, but legacy links
    may still ask for a download starting from ``ID_SPECTRUM`` instead of the
    repository file id.
    """
    cache_key = ("spectrum_file_path", spectrum_id)
    cached = _get_cached_file_path(cache_key)

    if cached is not None:
        return cached
    conn = get_connection()
    cur = conn.cursor()

    # Legacy spectrum-based downloads still follow the same operational rule:
    # when several reposfi files point to the same spectrum, serve the newest.
    query = """
        SELECT
            repos.NA_PATH,
            repos.NA_FILE,
            repos.NA_EXTENSION
        FROM FACT_SPECTRUM f

        LEFT JOIN (
            SELECT
                b.FK_SPECTRUM,
                MAX(d.ID_FILE) AS ID_FILE
            FROM BRIDGE_SPECTRUM_FILE b
            JOIN DIM_SPECTRUM_FILE d
                ON d.ID_FILE = b.FK_FILE
            WHERE d.NA_VOLUME = 'reposfi'
            GROUP BY b.FK_SPECTRUM
        ) latest
            ON latest.FK_SPECTRUM = f.ID_SPECTRUM

        LEFT JOIN DIM_SPECTRUM_FILE repos
            ON repos.ID_FILE = latest.ID_FILE

        WHERE f.ID_SPECTRUM = %s
    """

    cur.execute(query, (spectrum_id,))
    result = cur.fetchone()

    conn.close()

    if not result:
        return None

    na_path = result["NA_PATH"]
    na_file = result["NA_FILE"]

    if not na_path or not na_file:
        return None

    file_path = os.path.join(na_path, na_file)
    _set_cached_file_path(cache_key, file_path)
    return file_path


def get_file_by_file_id(file_id):
    """Resolve the repository path for one repository file row."""
    cache_key = ("file_id_path", file_id)
    cached = _get_cached_file_path(cache_key)

    if cached is not None:
        return cached
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            NA_PATH,
            NA_FILE
        FROM DIM_SPECTRUM_FILE
        WHERE ID_FILE = %s
          AND NA_VOLUME = 'reposfi'
        """,
        (file_id,),
    )
    result = cur.fetchone()

    conn.close()

    if not result:
        return None

    na_path = result["NA_PATH"]
    na_file = result["NA_FILE"]

    if not na_path or not na_file:
        return None

    file_path = os.path.join(na_path, na_file)
    _set_cached_file_path(cache_key, file_path)
    return file_path


def get_spectra_by_file_id(
    file_id,
    *,
    equipment_id=None,
    site_id=None,
    start_date=None,
    end_date=None,
    freq_start=None,
    freq_end=None,
    description=None,
):
    """Return every spectrum linked to one file plus the active-match flag.

    The unified search returns one row per file, but the detail panel still
    shows the complete contents of that file. ``IS_MATCH`` marks the spectra
    that justified the file entering the result set so the UI can highlight
    them without hiding the rest.
    """
    cache_key = (
        "file_spectra",
        file_id,
        equipment_id,
        site_id,
        start_date,
        end_date,
        freq_start,
        freq_end,
        description,
    )
    cached = _get_cached_query(cache_key)

    if cached is not None:
        return cached

    match_clauses, match_params = _build_fact_filters(
        equipment_id=equipment_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias="f",
    )
    match_sql = " AND ".join(match_clauses) if match_clauses else "1 = 1"

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT
            f.ID_SPECTRUM,
            f.NA_DESCRIPTION,
            f.NU_FREQ_START,
            f.NU_FREQ_END,
            f.DT_TIME_START,
            f.DT_TIME_END,
            f.NU_RBW,
            f.NU_TRACE_COUNT,
            e.NA_EQUIPMENT,
            CASE
                WHEN {match_sql} THEN 1
                ELSE 0
            END AS IS_MATCH
        FROM BRIDGE_SPECTRUM_FILE b
        JOIN FACT_SPECTRUM f
            ON f.ID_SPECTRUM = b.FK_SPECTRUM
        JOIN DIM_SPECTRUM_EQUIPMENT e
            ON e.ID_EQUIPMENT = f.FK_EQUIPMENT
        WHERE b.FK_FILE = %s
        ORDER BY f.DT_TIME_START DESC, f.ID_SPECTRUM DESC
        """,
        match_params + [file_id],
    )

    rows = cur.fetchall()
    conn.close()

    _format_row_datetime_fields(rows, "DT_TIME_START", "DT_TIME_END")

    _set_cached_query(cache_key, rows)
    return rows
