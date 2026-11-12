"""Query catalog files, spectra and facets for the web interface.

Every read closes its own session, including connection-local temporary tables.
Caching, display formatting and pagination slicing belong to the service.
"""

from __future__ import annotations

from psycopg import Cursor

from .config import (
    REPOSITORY_VOLUME,
    SPECTRUM_FILE_SORT_FIELDS,
    SPECTRUM_FILTER_TEMP_TABLE,
)
from .connection import get_connection_rfdata, get_connection_summary
from .summary import (
    map_sites_query,
    site_observations_query,
)


def _build_locality_base_sql(
    site_alias: str = 's',
    district_alias: str = 'd',
    county_alias: str = 'c',
) -> str:
    """Build the SQL fragment that resolves the best locality label.

    Args:
        site_alias (str): Site alias.
        district_alias (str): District alias.
        county_alias (str): County alias.

    Returns:
        str: Server-owned SQL fragment; callers bind values separately.
    """

    return (
        f"COALESCE("
        f'''NULLIF({site_alias}."NA_SITE", ''), '''
        f'''NULLIF({district_alias}."NA_DISTRICT", ''), '''
        f'''{county_alias}."NA_COUNTY", '''
        f'''CONCAT('Site ', {site_alias}."ID_SITE")'''
        f")"
    )


def _build_text_difference_sql(left_expr: str, right_expr: str) -> str:
    """Compare nullable geographic labels without case differences.

    Args:
        left_expr (str): Left expr.
        right_expr (str): Right expr.

    Returns:
        str: Server-owned SQL fragment; callers bind values separately.
    """
    return f"LOWER(COALESCE({left_expr}, '')) IS DISTINCT FROM LOWER(COALESCE({right_expr}, ''))"


def _build_locality_display_sql(
    site_alias: str = 's',
    district_alias: str = 'd',
    county_alias: str = 'c',
    state_alias: str = 'st',
) -> str:
    """Build a locality label by joining raw RFDATA geography dimension tables.

    Used ONLY by ``load_fact_site_availability_row`` (the live RFDATA fallback
    path). All other display SQL uses ``_build_summary_locality_display_sql``
    instead, which reads pre-joined geography from the summary model and avoids
    this four-table dimension join.

    The county/state complement is rendered in parentheses when the site label
    differs from the municipality name. This avoids labels such as
    ``Brasilia (Belem/PA)``, which can look like two locations to operators —
    the intent is ``site name (county/state context)``.

    Args:
        site_alias (str): Site alias.
        district_alias (str): District alias.
        county_alias (str): County alias.
        state_alias (str): State alias.

    Returns:
        str: Server-owned SQL fragment; callers bind values separately.
    """

    base_sql = _build_locality_base_sql(
        site_alias=site_alias,
        district_alias=district_alias,
        county_alias=county_alias,
    )
    site_differs_from_county_sql = _build_text_difference_sql(
        f'''{site_alias}."NA_SITE"''',
        f'''{county_alias}."NA_COUNTY"''',
    )
    state_suffix_sql = (
        f"CASE "
        f'WHEN {state_alias}."LC_STATE" IS NOT NULL '
        f'''THEN CONCAT('/', {state_alias}."LC_STATE") '''
        f"ELSE '' "
        f"END"
    )
    return f'''
        TRIM(
            CONCAT(
                {base_sql},
                CASE
                    WHEN {county_alias}."NA_COUNTY" IS NOT NULL
                     AND (
                        {site_alias}."NA_SITE" IS NULL
                        OR {site_alias}."NA_SITE" = ''
                        OR {site_differs_from_county_sql}
                     )
                    THEN CONCAT(' (', {county_alias}."NA_COUNTY", {state_suffix_sql}, ')')
                    WHEN {state_alias}."LC_STATE" IS NOT NULL
                    THEN CONCAT('/', {state_alias}."LC_STATE")
                    ELSE ''
                END,
                ''
            )
        )
    '''


def _build_summary_locality_display_sql(
    *,
    summary_alias: str = 'sm',
    fallback_site_id_expr: str | None = None,
    site_name_expr: str | None = None,
    district_name_expr: str | None = None,
    county_name_expr: str | None = None,
    state_code_expr: str | None = None,
) -> str:
    """Build a locality label using pre-joined geography from the summary model.

    Preferred over ``_build_locality_display_sql`` because the summary model
    already carries geography as flat denormalized columns, so this SQL fragment
    needs no extra dimension joins.

    Callers can override individual column expressions when the table alias or
    column names differ from the SITE_EQUIPMENT_OBS_SUMMARY defaults — for
    example, the site geography projection uses ``NA_SITE_LABEL`` instead of
    ``NA_SITE_NAME``.

    Label resolution priority:
      1. Site name (``NA_SITE_NAME`` / overridden ``site_name_expr``)
      2. District name
      3. County name
      4. ``'Site <id>'`` as a last-resort fallback

    County/state context is appended in parentheses only when the site label
    differs from the county name, preventing redundant labels like
    ``Brasilia (Brasilia/DF)``.

    Args:
        summary_alias (str): Summary alias.
        fallback_site_id_expr (str): Fallback site id expr.
        site_name_expr (str): Site name expr.
        district_name_expr (str): District name expr.
        county_name_expr (str): County name expr.
        state_code_expr (str): State code expr.

    Returns:
        str: Server-owned SQL fragment; callers bind values separately.
    """

    fallback_site_id_expr = fallback_site_id_expr or f'''{summary_alias}."FK_SITE"'''
    site_name_expr = site_name_expr or f'''{summary_alias}."NA_SITE_NAME"'''
    district_name_expr = district_name_expr or f'''{summary_alias}."NA_DISTRICT_NAME"'''
    county_name_expr = county_name_expr or f'''{summary_alias}."NA_COUNTY_NAME"'''
    state_code_expr = state_code_expr or f'''{summary_alias}."NA_STATE_CODE"'''

    base_sql = (
        f"COALESCE("
        f"NULLIF({site_name_expr}, ''), "
        f"NULLIF({district_name_expr}, ''), "
        f"{county_name_expr}, "
        f"CONCAT('Site ', {fallback_site_id_expr})"
        f")"
    )
    site_differs_from_county_sql = _build_text_difference_sql(
        site_name_expr,
        county_name_expr,
    )
    state_suffix_sql = (
        f"CASE "
        f"WHEN {state_code_expr} IS NOT NULL AND {state_code_expr} <> '' "
        f"THEN CONCAT('/', {state_code_expr}) "
        f"ELSE '' "
        f"END"
    )
    return f"""
        TRIM(
            CONCAT(
                {base_sql},
                CASE
                    WHEN {county_name_expr} IS NOT NULL
                     AND (
                        {site_name_expr} IS NULL
                        OR {site_name_expr} = ''
                        OR {site_differs_from_county_sql}
                     )
                    THEN CONCAT(' (', {county_name_expr}, {state_suffix_sql}, ')')
                    WHEN {state_code_expr} IS NOT NULL AND {state_code_expr} <> ''
                    THEN CONCAT('/', {state_code_expr})
                    ELSE ''
                END,
                ''
            )
        )
    """


def _build_summary_site_equipment_join(
    *,
    fact_alias: str = 'f',
    summary_alias: str = 'sm',
    join_type: str = 'LEFT JOIN',
) -> str:
    """Build the `(site, equipment)` summary join used by spectrum/file queries.

    Args:
        fact_alias (str): Fact alias.
        summary_alias (str): Summary alias.
        join_type (str): Join type.

    Returns:
        str: Server-owned SQL fragment; callers bind values separately.
    """

    return f'''
        {join_type} ({site_observations_query()}) {summary_alias}
            ON {summary_alias}."FK_SITE" = {fact_alias}."FK_SITE"
           AND {summary_alias}."FK_EQUIPMENT" = {fact_alias}."FK_EQUIPMENT"
    '''


def _build_where_sql(where_clauses: list[str]) -> str:
    """Join WHERE clauses only when the filter list is not empty.

    Args:
        where_clauses (list[str]): Where clauses.

    Returns:
        str: Server-owned SQL fragment; callers bind values separately.
    """

    if not where_clauses:
        return ""

    return "WHERE " + " AND ".join(where_clauses)


def _build_fact_filters(
    *,
    equipment_id: int | str | None = None,
    state_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    freq_start: float | None = None,
    freq_end: float | None = None,
    description: str | None = None,
    fact_alias: str = 'f',
    site_alias: str = 's',
    include_freq: bool = True,
    include_description: bool = True,
) -> tuple[list[str], list[object]]:
    """Build reusable ``FACT_SPECTRUM`` WHERE clauses.

    The same helper feeds two related result shapes:

    - spectrum-oriented rows, where frequency/description filters are natural
    - file-oriented rows derived from spectra, where callers may choose whether
      those spectrum-specific filters should participate in the result set

    ``include_freq`` and ``include_description`` exist so the service can keep
    one source of truth for SQL predicates while still making that business
    choice explicitly at each call site.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.
        start_date (str | None): Inclusive start date in ISO format, or None.
        end_date (str | None): Inclusive end date in ISO format, or None.
        freq_start (float | None): Optional lower frequency bound in the existing
            catalog unit.
        freq_end (float | None): Optional upper frequency bound in the existing catalog
            unit.
        description (str | None): Optional spectrum description pattern.
        fact_alias (str): Fact alias.
        site_alias (str): Site alias.
        include_freq (bool): Include freq.
        include_description (bool): Include description.

    Returns:
        tuple[list[str], list[object]]: WHERE clauses without the keyword and
            their ordered bound values.
    """

    where_clauses = []
    params = []

    if equipment_id:
        where_clauses.append(f'''{fact_alias}."FK_EQUIPMENT" = %s''')
        params.append(equipment_id)

    if state_id:
        where_clauses.append(f'''{site_alias}."FK_STATE" = %s''')
        params.append(state_id)

    if district_id:
        where_clauses.append(f'''{site_alias}."FK_DISTRICT" = %s''')
        params.append(district_id)

    if site_id:
        where_clauses.append(f'''{fact_alias}."FK_SITE" = %s''')
        params.append(site_id)

    if start_date:
        where_clauses.append(f'''{fact_alias}."DT_TIME_END" >= %s''')
        params.append(start_date)

    if end_date:
        where_clauses.append(f'''{fact_alias}."DT_TIME_START" <= %s''')
        params.append(end_date + " 23:59:59")

    if include_freq and freq_start is not None and freq_end is not None:
        where_clauses.append(f'''{fact_alias}."NU_FREQ_START" >= %s''')
        params.append(freq_start)
        where_clauses.append(f'''{fact_alias}."NU_FREQ_END" <= %s''')
        params.append(freq_end)
    elif include_freq and freq_start is not None:
        where_clauses.append(f'''{fact_alias}."NU_FREQ_START" >= %s''')
        params.append(freq_start)
    elif include_freq and freq_end is not None:
        where_clauses.append(f'''{fact_alias}."NU_FREQ_END" <= %s''')
        params.append(freq_end)

    if include_description and description:
        where_clauses.append(f'''{fact_alias}."NA_DESCRIPTION" ILIKE %s''')
        params.append(f"%{description}%")

    return where_clauses, params


def _build_summary_scoped_fact_filters(
    *,
    equipment_id: int | str | None = None,
    state_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    freq_start: float | None = None,
    freq_end: float | None = None,
    description: str | None = None,
    fact_alias: str = 'f',
    summary_alias: str = 'sm',
    include_freq: bool = True,
    include_description: bool = True,
) -> tuple[list[str], list[object]]:
    """Build FACT_SPECTRUM predicates that resolve geographic scope via the summary
        join.

    Preferred over ``_build_fact_filters`` when the query already joins
    the site/equipment projection. Geographic filters (state, district) use
    summary columns (``ID_STATE``, ``FK_DISTRICT``) instead of
    ``DIM_SPECTRUM_SITE``, which avoids a second join to the geographic
    dimension tables.

    Returns (where_clauses, params) in the same format as ``_build_fact_filters``.
    Callers append the clauses to their WHERE block and bind params in order.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.
        start_date (str | None): Inclusive start date in ISO format, or None.
        end_date (str | None): Inclusive end date in ISO format, or None.
        freq_start (float | None): Optional lower frequency bound in the existing
            catalog unit.
        freq_end (float | None): Optional upper frequency bound in the existing catalog
            unit.
        description (str | None): Optional spectrum description pattern.
        fact_alias (str): Fact alias.
        summary_alias (str): Summary alias.
        include_freq (bool): Include freq.
        include_description (bool): Include description.

    Returns:
        tuple[list[str], list[object]]: WHERE clauses without the keyword and
            their ordered bound values.
    """

    where_clauses, params = _build_fact_filters(
        equipment_id=equipment_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias=fact_alias,
        include_freq=include_freq,
        include_description=include_description,
    )

    if state_id:
        where_clauses.append(f'''{summary_alias}."ID_STATE" = %s''')
        params.append(state_id)

    if district_id:
        where_clauses.append(f'''{summary_alias}."FK_DISTRICT" = %s''')
        params.append(district_id)

    return where_clauses, params


def _append_summary_geo_filters(
    where_clauses: list[str],
    params: list[object],
    *,
    equipment_id: int | str | None = None,
    state_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
    summary_alias: str = 'sm',
) -> None:
    """Append the geo filters supported by the site/equipment projection.

    The summary read model is intentionally used for responsive selector
    updates. It only understands geographic/equipment scoping, which is
    enough for the fast filter chrome shown before the user launches the full
    spectrum/file query.

    Args:
        where_clauses (list[str]): Where clauses.
        params (list[object]): Params.
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.
        summary_alias (str): Summary alias.

    Returns:
        None: Clauses and values are appended in place.
    """

    if equipment_id:
        where_clauses.append(f'''{summary_alias}."FK_EQUIPMENT" = %s''')
        params.append(equipment_id)

    if state_id:
        where_clauses.append(f'''{summary_alias}."ID_STATE" = %s''')
        params.append(state_id)

    if district_id:
        where_clauses.append(f'''{summary_alias}."FK_DISTRICT" = %s''')
        params.append(district_id)

    if site_id:
        where_clauses.append(f'''{summary_alias}."FK_SITE" = %s''')
        params.append(site_id)


def _materialize_filtered_spectra(
    cur: Cursor[dict[str, object]],
    filtered_spectra_sql: str,
    fact_params: list[object],
) -> None:
    """Store one file-search spectrum filter result in the current connection.

    File mode needs the same filtered spectra to identify repository files,
    calculate their visible context, and count their complete spectrum set.
    The PostgreSQL temporary table evaluates this filter once and belongs only
    to the request connection. Closing that connection removes the table.

    Args:
        cur (Cursor[dict[str, object]]): Caller-owned dictionary cursor in the active
            transaction.
        filtered_spectra_sql (str): Filtered spectra sql.
        fact_params (list[object]): Fact params.

    Returns:
        None: No useful value; transaction ownership follows the operation.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    cur.execute(
        f"""
        CREATE TEMPORARY TABLE {SPECTRUM_FILTER_TEMP_TABLE}
        AS
        {filtered_spectra_sql}
        """,
        fact_params,
    )


def load_summary_site_availability_row(
    equipment_id: int | str | None,
    site_id: int | str | None,
) -> dict[str, object] | None:
    """Load one equipment/site availability row from the site/equipment projection.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        site_id (int | str | None): Optional site identifier.

    Returns:
        dict[str, object] | None: Dictionary rows keyed by the SELECT aliases below;
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
                "NA_SITE_LABEL" AS "LOCALITY_LABEL",
                "DT_FIRST_SEEN_AT" AS "DATE_START",
                "DT_LAST_SEEN_AT" AS "DATE_END",
                "NU_SPECTRUM_COUNT" AS "SPECTRUM_COUNT"
            FROM ({site_observations_query()}) observations
            WHERE "FK_EQUIPMENT" = %s
              AND "FK_SITE" = %s
            LIMIT 1
            ''',
            (equipment_id, site_id),
        )
        row = cur.fetchone()
    finally:
        conn.close()
    return row


def load_summary_equipment_rows() -> list[dict[str, object]]:
    """Load the equipment selector from summary-backed observations.

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
            SELECT DISTINCT
                "FK_EQUIPMENT" AS "ID_EQUIPMENT",
                "NA_EQUIPMENT"
            FROM ({site_observations_query()}) observations
            WHERE "NA_EQUIPMENT" IS NOT NULL
              AND "NA_EQUIPMENT" <> ''
            ORDER BY "NA_EQUIPMENT"
            '''
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def load_fact_equipment_rows() -> list[dict[str, object]]:
    """Load the equipment selector directly from ``RFDATA`` as a fallback.

    Args:
        None.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()
        cur.execute(
            '''
            SELECT DISTINCT
                e."ID_EQUIPMENT",
                e."NA_EQUIPMENT"
            FROM "DIM_SPECTRUM_EQUIPMENT" e
            JOIN "FACT_SPECTRUM" f
                ON f."FK_EQUIPMENT" = e."ID_EQUIPMENT"
            ORDER BY e."NA_EQUIPMENT"
            '''
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def load_fact_locality_rows(
    *,
    equipment_id: int | str | None = None,
    state_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    freq_start: float | None = None,
    freq_end: float | None = None,
    description: str | None = None,
) -> list[dict[str, object]]:
    """Load locality options directly from the live spectrum catalog.

    The selector follows the same spectrum-aware filters as the main search so
    operators do not pick a locality that cannot produce any file result.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.
        start_date (str | None): Inclusive start date in ISO format, or None.
        end_date (str | None): Inclusive end date in ISO format, or None.
        freq_start (float | None): Optional lower frequency bound in the existing
            catalog unit.
        freq_end (float | None): Optional upper frequency bound in the existing catalog
            unit.
        description (str | None): Optional spectrum description pattern.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    where_clauses, params = _build_summary_scoped_fact_filters(
        equipment_id=equipment_id,
        state_id=state_id,
        district_id=district_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias="f",
        summary_alias="sm",
    )
    where_sql = _build_where_sql(where_clauses)
    locality_display_sql = _build_summary_locality_display_sql(
        summary_alias="sm",
        fallback_site_id_expr='f."FK_SITE"',
    )
    summary_join_sql = _build_summary_site_equipment_join(
        fact_alias="f",
        summary_alias="sm",
    )

    query = f'''
        SELECT
            f."FK_SITE" AS "ID_SITE",
            {locality_display_sql} AS "LOCALITY_LABEL",
            sm."NA_COUNTY_NAME" AS "COUNTY_NAME",
            sm."NA_STATE_CODE" AS "STATE_CODE",
            MIN(f."DT_TIME_START") AS "DATE_START",
            MAX(f."DT_TIME_END") AS "DATE_END",
            COUNT(*) AS "SPECTRUM_COUNT"
        FROM "FACT_SPECTRUM" f
        {summary_join_sql}
        {where_sql}
        GROUP BY
            f."FK_SITE",
            sm."NA_SITE_NAME",
            sm."NA_DISTRICT_NAME",
            sm."NA_COUNTY_NAME",
            sm."NA_STATE_CODE"
        ORDER BY
            MAX(f."DT_TIME_END") DESC NULLS LAST,
            "LOCALITY_LABEL" ASC
    '''

    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()
        cur.execute(query, params)
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def load_fact_equipment_filter_rows(
    *,
    state_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    freq_start: float | None = None,
    freq_end: float | None = None,
    description: str | None = None,
) -> list[dict[str, object]]:
    """Load equipment options scoped by every active non-equipment filter.

    Args:
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.
        start_date (str | None): Inclusive start date in ISO format, or None.
        end_date (str | None): Inclusive end date in ISO format, or None.
        freq_start (float | None): Optional lower frequency bound in the existing
            catalog unit.
        freq_end (float | None): Optional upper frequency bound in the existing catalog
            unit.
        description (str | None): Optional spectrum description pattern.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    where_clauses, params = _build_fact_filters(
        state_id=state_id,
        district_id=district_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias="f",
        site_alias="s",
    )
    where_sql = _build_where_sql(where_clauses)

    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                e."ID_EQUIPMENT",
                e."NA_EQUIPMENT",
                MIN(f."DT_TIME_START") AS "DATE_START",
                MAX(f."DT_TIME_END") AS "DATE_END",
                COUNT(*) AS "SPECTRUM_COUNT"
            FROM "FACT_SPECTRUM" f
            JOIN "DIM_SPECTRUM_EQUIPMENT" e
                ON e."ID_EQUIPMENT" = f."FK_EQUIPMENT"
            JOIN "DIM_SPECTRUM_SITE" s
                ON s."ID_SITE" = f."FK_SITE"
            {where_sql}
            GROUP BY e."ID_EQUIPMENT", e."NA_EQUIPMENT"
            ORDER BY e."NA_EQUIPMENT" ASC
            ''',
            params,
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def load_summary_equipment_filter_rows(
    *,
    state_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
) -> list[dict[str, object]]:
    """Load equipment options from the summary read model.

    Args:
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    where_clauses = [
        'sm."NA_EQUIPMENT" IS NOT NULL',
        'sm."NA_EQUIPMENT" <> \'\'',
    ]
    params = []

    _append_summary_geo_filters(
        where_clauses,
        params,
        state_id=state_id,
        district_id=district_id,
        site_id=site_id,
        summary_alias="sm",
    )

    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                sm."FK_EQUIPMENT" AS "ID_EQUIPMENT",
                sm."NA_EQUIPMENT",
                MIN(sm."DT_FIRST_SEEN_AT") AS "DATE_START",
                MAX(sm."DT_LAST_SEEN_AT") AS "DATE_END",
                SUM(sm."NU_SPECTRUM_COUNT") AS "SPECTRUM_COUNT"
            FROM ({site_observations_query()}) sm
            {_build_where_sql(where_clauses)}
            GROUP BY sm."FK_EQUIPMENT", sm."NA_EQUIPMENT"
            ORDER BY sm."NA_EQUIPMENT" ASC
            ''',
            params,
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def load_fact_state_rows(
    *,
    equipment_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    freq_start: float | None = None,
    freq_end: float | None = None,
    description: str | None = None,
) -> list[dict[str, object]]:
    """Load state options scoped by every active non-state filter.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.
        start_date (str | None): Inclusive start date in ISO format, or None.
        end_date (str | None): Inclusive end date in ISO format, or None.
        freq_start (float | None): Optional lower frequency bound in the existing
            catalog unit.
        freq_end (float | None): Optional upper frequency bound in the existing catalog
            unit.
        description (str | None): Optional spectrum description pattern.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    where_clauses, params = _build_fact_filters(
        equipment_id=equipment_id,
        district_id=district_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias="f",
        site_alias="s",
    )
    where_clauses.append('s."FK_STATE" IS NOT NULL')
    where_sql = _build_where_sql(where_clauses)

    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                st."ID_STATE",
                st."LC_STATE" AS "STATE_CODE",
                st."NA_STATE" AS "STATE_NAME",
                MIN(f."DT_TIME_START") AS "DATE_START",
                MAX(f."DT_TIME_END") AS "DATE_END",
                COUNT(*) AS "SPECTRUM_COUNT"
            FROM "FACT_SPECTRUM" f
            JOIN "DIM_SPECTRUM_SITE" s
                ON s."ID_SITE" = f."FK_SITE"
            JOIN "DIM_SITE_STATE" st
                ON st."ID_STATE" = s."FK_STATE"
            {where_sql}
            GROUP BY st."ID_STATE", st."LC_STATE", st."NA_STATE"
            ORDER BY st."LC_STATE" ASC, st."NA_STATE" ASC
            ''',
            params,
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def load_summary_state_rows(
    *,
    equipment_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
) -> list[dict[str, object]]:
    """Load state options from the summary read model.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    where_clauses = [
        'sm."NA_STATE_CODE" IS NOT NULL',
        'sm."NA_STATE_CODE" <> \'\'',
    ]
    params = []

    _append_summary_geo_filters(
        where_clauses,
        params,
        equipment_id=equipment_id,
        district_id=district_id,
        site_id=site_id,
        summary_alias="sm",
    )

    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                sm."ID_STATE",
                sm."NA_STATE_CODE" AS "STATE_CODE",
                sm."NA_STATE_NAME" AS "STATE_NAME",
                MIN(sm."DT_FIRST_SEEN_AT") AS "DATE_START",
                MAX(sm."DT_LAST_SEEN_AT") AS "DATE_END",
                SUM(sm."NU_SPECTRUM_COUNT") AS "SPECTRUM_COUNT"
            FROM ({site_observations_query()}) sm
            {_build_where_sql(where_clauses)}
            GROUP BY sm."ID_STATE", sm."NA_STATE_CODE", sm."NA_STATE_NAME"
            ORDER BY sm."NA_STATE_CODE" ASC, sm."NA_STATE_NAME" ASC
            ''',
            params,
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def load_fact_district_rows(
    *,
    equipment_id: int | str | None = None,
    state_id: int | str | None = None,
    site_id: int | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    freq_start: float | None = None,
    freq_end: float | None = None,
    description: str | None = None,
) -> list[dict[str, object]]:
    """Load district options keyed by `FK_DISTRICT` instead of `ID_SITE`.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        site_id (int | str | None): Optional site identifier.
        start_date (str | None): Inclusive start date in ISO format, or None.
        end_date (str | None): Inclusive end date in ISO format, or None.
        freq_start (float | None): Optional lower frequency bound in the existing
            catalog unit.
        freq_end (float | None): Optional upper frequency bound in the existing catalog
            unit.
        description (str | None): Optional spectrum description pattern.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    where_clauses, params = _build_fact_filters(
        equipment_id=equipment_id,
        state_id=state_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias="f",
        site_alias="s",
    )
    where_clauses.append('s."FK_DISTRICT" IS NOT NULL')
    where_sql = _build_where_sql(where_clauses)

    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                d."ID_DISTRICT",
                d."NA_DISTRICT" AS "DISTRICT_NAME",
                c."NA_COUNTY" AS "COUNTY_NAME",
                st."LC_STATE" AS "STATE_CODE",
                COUNT(DISTINCT s."ID_SITE") AS "SITE_COUNT",
                COUNT(*) AS "SPECTRUM_COUNT",
                MIN(f."DT_TIME_START") AS "DATE_START",
                MAX(f."DT_TIME_END") AS "DATE_END"
            FROM "FACT_SPECTRUM" f
            JOIN "DIM_SPECTRUM_SITE" s
                ON s."ID_SITE" = f."FK_SITE"
            JOIN "DIM_SITE_DISTRICT" d
                ON d."ID_DISTRICT" = s."FK_DISTRICT"
            LEFT JOIN "DIM_SITE_COUNTY" c
                ON c."ID_COUNTY" = s."FK_COUNTY"
            LEFT JOIN "DIM_SITE_STATE" st
                ON st."ID_STATE" = s."FK_STATE"
            {where_sql}
            GROUP BY d."ID_DISTRICT", d."NA_DISTRICT", c."NA_COUNTY", st."LC_STATE"
            ORDER BY d."NA_DISTRICT" ASC, c."NA_COUNTY" ASC, st."LC_STATE" ASC
            ''',
            params,
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def load_summary_district_rows(
    *,
    equipment_id: int | str | None = None,
    state_id: int | str | None = None,
    site_id: int | str | None = None,
) -> list[dict[str, object]]:
    """Load district options from the summary read model.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        site_id (int | str | None): Optional site identifier.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    where_clauses = ['sm."FK_DISTRICT" IS NOT NULL']
    params = []

    _append_summary_geo_filters(
        where_clauses,
        params,
        equipment_id=equipment_id,
        state_id=state_id,
        site_id=site_id,
        summary_alias="sm",
    )

    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                sm."FK_DISTRICT" AS "ID_DISTRICT",
                sm."NA_DISTRICT_NAME" AS "DISTRICT_NAME",
                sm."NA_COUNTY_NAME" AS "COUNTY_NAME",
                sm."NA_STATE_CODE" AS "STATE_CODE",
                COUNT(DISTINCT sm."FK_SITE") AS "SITE_COUNT",
                SUM(sm."NU_SPECTRUM_COUNT") AS "SPECTRUM_COUNT",
                MIN(sm."DT_FIRST_SEEN_AT") AS "DATE_START",
                MAX(sm."DT_LAST_SEEN_AT") AS "DATE_END"
            FROM ({site_observations_query()}) sm
            {_build_where_sql(where_clauses)}
            GROUP BY sm."FK_DISTRICT", sm."NA_DISTRICT_NAME", sm."NA_COUNTY_NAME", sm."NA_STATE_CODE"
            ORDER BY sm."NA_DISTRICT_NAME" ASC, sm."NA_COUNTY_NAME" ASC, sm."NA_STATE_CODE" ASC
            ''',
            params,
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows


def load_fact_filter_availability_row(
    *,
    equipment_id: int | str | None = None,
    state_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
    freq_start: float | None = None,
    freq_end: float | None = None,
    description: str | None = None,
) -> dict[str, object] | None:
    """Load observed dates and counts for the current non-date filters.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.
        freq_start (float | None): Optional lower frequency bound in the existing
            catalog unit.
        freq_end (float | None): Optional upper frequency bound in the existing catalog
            unit.
        description (str | None): Optional spectrum description pattern.

    Returns:
        dict[str, object] | None: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    where_clauses, params = _build_fact_filters(
        equipment_id=equipment_id,
        state_id=state_id,
        district_id=district_id,
        site_id=site_id,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias="f",
        site_alias="s",
    )
    where_sql = _build_where_sql(where_clauses)

    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                MIN(f."DT_TIME_START") AS "DATE_START",
                MAX(f."DT_TIME_END") AS "DATE_END",
                COUNT(*) AS "SPECTRUM_COUNT",
                COUNT(DISTINCT f."FK_SITE") AS "SITE_COUNT",
                COUNT(DISTINCT s."FK_DISTRICT") AS "DISTRICT_COUNT",
                COUNT(DISTINCT f."FK_EQUIPMENT") AS "EQUIPMENT_COUNT"
            FROM "FACT_SPECTRUM" f
            JOIN "DIM_SPECTRUM_SITE" s
                ON s."ID_SITE" = f."FK_SITE"
            {where_sql}
            ''',
            params,
        )
        return cur.fetchone()
    finally:
        conn.close()


def load_summary_filter_availability_row(
    *,
    equipment_id: int | str | None = None,
    state_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
) -> dict[str, object] | None:
    """Load the observed geo availability window from the summary model.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.

    Returns:
        dict[str, object] | None: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    where_clauses = []
    params = []

    _append_summary_geo_filters(
        where_clauses,
        params,
        equipment_id=equipment_id,
        state_id=state_id,
        district_id=district_id,
        site_id=site_id,
        summary_alias="sm",
    )

    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                MIN(sm."DT_FIRST_SEEN_AT") AS "DATE_START",
                MAX(sm."DT_LAST_SEEN_AT") AS "DATE_END",
                SUM(sm."NU_SPECTRUM_COUNT") AS "SPECTRUM_COUNT",
                COUNT(DISTINCT sm."FK_SITE") AS "SITE_COUNT",
                COUNT(DISTINCT sm."FK_DISTRICT") AS "DISTRICT_COUNT",
                COUNT(DISTINCT sm."FK_EQUIPMENT") AS "EQUIPMENT_COUNT"
            FROM ({site_observations_query()}) sm
            {_build_where_sql(where_clauses)}
            ''',
            params,
        )
        row = cur.fetchone()
    finally:
        conn.close()
    return row


def load_fact_site_availability_row(
    equipment_id: int | str | None,
    site_id: int | str | None,
) -> dict[str, object] | None:
    """Load one equipment/site availability row directly from ``FACT_SPECTRUM``.

    Fallback used when the summary model has no row for the given
    (equipment, site) pair. Resolves the locality label by joining the four
    geographic dimension tables (site, district, county, state).

    Dates are NOT formatted here. The caller
    ``get_spectrum_site_availability_range`` normalizes date fields after both
    the summary path and this fallback, ensuring consistent output regardless
    of which source ran. Formatting here would cause double-formatting when
    this path is taken.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        site_id (int | str | None): Optional site identifier.

    Returns:
        dict[str, object] | None: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """

    locality_display_sql = _build_locality_display_sql()
    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                s."ID_SITE",
                {locality_display_sql} AS "LOCALITY_LABEL",
                MIN(f."DT_TIME_START") AS "DATE_START",
                MAX(f."DT_TIME_END") AS "DATE_END",
                COUNT(*) AS "SPECTRUM_COUNT"
            FROM "FACT_SPECTRUM" f
            JOIN "DIM_SPECTRUM_SITE" s
                ON s."ID_SITE" = f."FK_SITE"
            LEFT JOIN "DIM_SITE_DISTRICT" d
                ON d."ID_DISTRICT" = s."FK_DISTRICT"
            LEFT JOIN "DIM_SITE_COUNTY" c
                ON c."ID_COUNTY" = s."FK_COUNTY"
            LEFT JOIN "DIM_SITE_STATE" st
                ON st."ID_STATE" = s."FK_STATE"
            WHERE f."FK_EQUIPMENT" = %s
              AND f."FK_SITE" = %s
            GROUP BY
                s."ID_SITE",
                s."NA_SITE",
                d."NA_DISTRICT",
                c."NA_COUNTY",
                st."LC_STATE"
            LIMIT 1
            ''',
            (equipment_id, site_id),
        )
        row = cur.fetchone()
    finally:
        conn.close()
    # Intentionally returning the raw row without date formatting.
    # get_spectrum_site_availability_range formats both the summary and fact
    # paths uniformly after the fact, avoiding a double-format on this path.
    return row


def load_selection_matches(
    *,
    state_id: int | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    freq_start: float | None = None,
    freq_end: float | None = None,
    description: str | None = None,
) -> list[dict[str, object]]:
    """Find site/equipment pairs with at least one matching spectrum.

    Args:
        state_id (int | str | None): Optional state identifier.
        start_date (str | None): Inclusive start date in ISO format, or None.
        end_date (str | None): Inclusive end date in ISO format, or None.
        freq_start (float | None): Optional lower frequency bound in the existing
            catalog unit.
        freq_end (float | None): Optional upper frequency bound in the existing catalog
            unit.
        description (str | None): Optional spectrum description pattern.

    Returns:
        list[dict[str, object]]: Dictionary rows keyed by the SELECT aliases below;
            selected fields are present and nullable database values remain None. Empty
            selection returns an empty collection or None as declared.

    Raises:
        Exception: Database or validation failure; errors propagate to the caller.
    """
    clauses, params = _build_fact_filters(
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
    )
    clauses[:0] = ['f."FK_SITE" = s."ID_SITE"', 'f."FK_EQUIPMENT" = e."ID_EQUIPMENT"']
    state_clause = 'WHERE s."FK_STATE" = %s' if state_id else "WHERE 1 = 1"
    if state_id:
        params.insert(0, state_id)
    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()
        # A scalar probe stops at one match instead of materializing every spectrum.
        cur.execute(
            f'''
            SELECT e."ID_EQUIPMENT", e."NA_EQUIPMENT",
                s."FK_DISTRICT" AS "ID_DISTRICT", d."NA_DISTRICT" AS "DISTRICT_NAME",
                c."NA_COUNTY" AS "COUNTY_NAME", st."LC_STATE" AS "STATE_CODE"
            FROM "DIM_SPECTRUM_SITE" s
            CROSS JOIN "DIM_SPECTRUM_EQUIPMENT" e
            LEFT JOIN "DIM_SITE_DISTRICT" d ON d."ID_DISTRICT" = s."FK_DISTRICT"
            LEFT JOIN "DIM_SITE_COUNTY" c ON c."ID_COUNTY" = s."FK_COUNTY"
            LEFT JOIN "DIM_SITE_STATE" st ON st."ID_STATE" = s."FK_STATE"
            {state_clause}
              AND (SELECT 1 FROM "FACT_SPECTRUM" f
                   {_build_where_sql(clauses)} LIMIT 1) IS NOT NULL
            ORDER BY d."NA_DISTRICT", c."NA_COUNTY", st."LC_STATE", e."NA_EQUIPMENT"
            ''',
            params,
        )
        return cur.fetchall()
    finally:
        conn.close()


def load_spectrum_file_rows(
    equipment_id: int | str | None,
    state_id: int | str | None,
    district_id: int | str | None,
    site_id: int | str | None,
    start_date: str | None,
    end_date: str | None,
    freq_start: float | None,
    freq_end: float | None,
    description: str | None,
    sort_by: str,
    sort_order: str,
) -> list[dict[str, object]]:
    """Read every repository file matched by the spectrum filters.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.
        start_date (str | None): Inclusive start date in ISO format, or None.
        end_date (str | None): Inclusive end date in ISO format, or None.
        freq_start (float | None): Optional lower frequency bound in the existing
            catalog unit.
        freq_end (float | None): Optional upper frequency bound in the existing catalog
            unit.
        description (str | None): Optional spectrum description pattern.
        sort_by (str): Key of `SPECTRUM_FILE_SORT_FIELDS`, validated by the service.
        sort_order (str): `ASC` or `DESC`, validated by the service.

    Returns:
        list[dict[str, object]]: One row per repository file with its path, size
            and the time, frequency, equipment and locality context of all its
            linked spectra. Empty selection returns [].

    Raises:
        Exception: Database failure; propagated after closing the connection.
    """
    locality_display_sql = _build_summary_locality_display_sql(
        summary_alias="sm",
        fallback_site_id_expr='f."FK_SITE"',
    )

    order_sql = f'''
        ORDER BY {SPECTRUM_FILE_SORT_FIELDS[sort_by]} {sort_order},
                 repos."ID_FILE" DESC
    '''

    fact_source_alias = "fs"
    fact_where_clauses, fact_params = _build_summary_scoped_fact_filters(
        equipment_id=equipment_id,
        state_id=state_id,
        district_id=district_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias=fact_source_alias,
        summary_alias="sm_filter",
        include_freq=True,
        include_description=True,
    )
    fact_where_sql = _build_where_sql(fact_where_clauses)
    filtered_summary_join_sql = _build_summary_site_equipment_join(
        fact_alias=fact_source_alias,
        summary_alias="sm_filter",
    )
    summary_join_sql = _build_summary_site_equipment_join(
        fact_alias="f",
        summary_alias="sm",
    )
    equipment_label_sql = 'COALESCE(sm."NA_EQUIPMENT", CONCAT(\'Equipamento \', f."FK_EQUIPMENT"))'

    # The filtered FACT_SPECTRUM subquery is the true search. After spectra are
    # identified, the result is collapsed to unique repository files for UI
    # rendering and download workflows.
    filtered_spectra_sql = f'''
        SELECT
            {fact_source_alias}."ID_SPECTRUM",
            {fact_source_alias}."FK_SITE",
            {fact_source_alias}."FK_EQUIPMENT",
            {fact_source_alias}."DT_TIME_START",
            {fact_source_alias}."DT_TIME_END",
            {fact_source_alias}."NU_FREQ_START",
            {fact_source_alias}."NU_FREQ_END"
        FROM "FACT_SPECTRUM" {fact_source_alias}
        {filtered_summary_join_sql}
        {fact_where_sql}
    '''

    # The search still matches files via filtered spectra, but the UI should
    # describe the complete contents of each qualifying file.
    data_query = f'''
        SELECT
            repos."ID_FILE",
            repos."NA_PATH",
            repos."NA_FILE",
            repos."NA_EXTENSION",
            repos."VL_FILE_SIZE_KB",
            ctx."DT_TIME_START",
            ctx."DT_TIME_END",
            ctx."NU_FREQ_START",
            ctx."NU_FREQ_END",
            ctx."EQUIPMENT_COUNT",
            ctx."EQUIPMENT_LABELS",
            ctx."NU_SPECTRA",
            ctx."LOCALITY_COUNT",
            ctx."LOCALITY_LABELS"
        FROM "DIM_SPECTRUM_FILE" repos
        JOIN (
            SELECT
                repos."ID_FILE",
                COUNT(DISTINCT b."FK_SPECTRUM") AS "NU_SPECTRA",
                MIN(f."DT_TIME_START") AS "DT_TIME_START",
                MAX(f."DT_TIME_END") AS "DT_TIME_END",
                MIN(f."NU_FREQ_START") AS "NU_FREQ_START",
                MAX(f."NU_FREQ_END") AS "NU_FREQ_END",
                COUNT(DISTINCT f."FK_EQUIPMENT") AS "EQUIPMENT_COUNT",
                STRING_AGG(DISTINCT {equipment_label_sql}, '||' ORDER BY {equipment_label_sql}) AS "EQUIPMENT_LABELS",
                COUNT(DISTINCT f."FK_SITE") AS "LOCALITY_COUNT",
                STRING_AGG(DISTINCT {locality_display_sql}, '||' ORDER BY {locality_display_sql}) AS "LOCALITY_LABELS"
            FROM (
                SELECT DISTINCT repos."ID_FILE"
                FROM {SPECTRUM_FILTER_TEMP_TABLE} f
                JOIN "BRIDGE_SPECTRUM_FILE" b
                    ON b."FK_SPECTRUM" = f."ID_SPECTRUM"
                JOIN "DIM_SPECTRUM_FILE" repos
                    ON repos."ID_FILE" = b."FK_FILE"
                WHERE repos."NA_VOLUME" = %s
            ) repos
            JOIN "BRIDGE_SPECTRUM_FILE" b
                ON b."FK_FILE" = repos."ID_FILE"
            JOIN "FACT_SPECTRUM" f
                ON f."ID_SPECTRUM" = b."FK_SPECTRUM"
            {summary_join_sql}
            GROUP BY repos."ID_FILE"
        ) ctx
            ON ctx."ID_FILE" = repos."ID_FILE"
        {order_sql}
    '''

    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()
        _materialize_filtered_spectra(
            cur,
            filtered_spectra_sql,
            fact_params,
        )
        cur.execute(data_query, (REPOSITORY_VOLUME,))
        full_rows = cur.fetchall()
    finally:
        conn.close()
    return full_rows


def load_spectrum_site_option(site_id: int | str | None) -> dict[str, object] | None:
    """Read the summary geography of one site for the spectrum filter chrome.

    Args:
        site_id (int | str | None): Site identifier selected in the interface.

    Returns:
        dict[str, object] | None: ID_SITE, ID_DISTRICT, ID_STATE, district,
            state, county and locality labels; None for an unknown site.

    Raises:
        Exception: Database failure; propagated after closing the connection.
    """
    locality_display_sql = _build_summary_locality_display_sql(
        summary_alias="ms",
        fallback_site_id_expr='ms."FK_SITE"',
        site_name_expr='ms."NA_SITE_LABEL"',
        district_name_expr='ms."NA_DISTRICT_NAME"',
        county_name_expr='ms."NA_COUNTY_NAME"',
        state_code_expr='ms."NA_STATE_CODE"',
    )
    conn = get_connection_summary()
    try:
        cur = conn.cursor()
        cur.execute(
            f'''
            SELECT
                ms."FK_SITE" AS "ID_SITE",
                ms."FK_DISTRICT" AS "ID_DISTRICT",
                ms."NA_DISTRICT_NAME" AS "DISTRICT_NAME",
                ms."ID_STATE",
                ms."NA_STATE_NAME" AS "STATE_NAME",
                {locality_display_sql} AS "LOCALITY_LABEL",
                ms."NA_COUNTY_NAME" AS "COUNTY_NAME",
                ms."NA_STATE_CODE" AS "STATE_CODE"
            FROM ({map_sites_query()}) ms
            WHERE ms."FK_SITE" = %s
            LIMIT 1
            ''',
            (site_id,),
        )
        row = cur.fetchone()
    finally:
        conn.close()
    return row


def load_file_by_spectrum_id(spectrum_id: int) -> dict[str, object] | None:
    """Resolve the newest repository file that contains one spectrum.

    Args:
        spectrum_id (int): Spectrum identifier.

    Returns:
        dict[str, object] | None: NA_PATH, NA_FILE and NA_EXTENSION, all None
            when the spectrum has no repository file; None for an unknown
            spectrum.

    Raises:
        Exception: Database failure; propagated after closing the connection.
    """
    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()

        # Legacy spectrum-based downloads still follow the same operational rule:
        # when several repository files point to the same spectrum, serve the newest.
        query = '''
            SELECT
                repos."NA_PATH",
                repos."NA_FILE",
                repos."NA_EXTENSION"
            FROM "FACT_SPECTRUM" f

            LEFT JOIN (
                SELECT
                    b."FK_SPECTRUM",
                    MAX(d."ID_FILE") AS "ID_FILE"
                FROM "BRIDGE_SPECTRUM_FILE" b
                JOIN "DIM_SPECTRUM_FILE" d
                    ON d."ID_FILE" = b."FK_FILE"
                WHERE d."NA_VOLUME" = %s
                GROUP BY b."FK_SPECTRUM"
            ) latest
                ON latest."FK_SPECTRUM" = f."ID_SPECTRUM"

            LEFT JOIN "DIM_SPECTRUM_FILE" repos
                ON repos."ID_FILE" = latest."ID_FILE"

            WHERE f."ID_SPECTRUM" = %s
        '''

        cur.execute(query, (REPOSITORY_VOLUME, spectrum_id))
        result = cur.fetchone()

    finally:
        conn.close()
    return result


def load_file_by_file_id(file_id: int) -> dict[str, object] | None:
    """Resolve one repository file selected for download.

    Args:
        file_id (int): Repository file identifier.

    Returns:
        dict[str, object] | None: NA_PATH and NA_FILE; None when the file is
            unknown or outside the repository volume.

    Raises:
        Exception: Database failure; propagated after closing the connection.
    """
    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()

        cur.execute(
            '''
            SELECT
                "NA_PATH",
                "NA_FILE"
            FROM "DIM_SPECTRUM_FILE"
            WHERE "ID_FILE" = %s
              AND "NA_VOLUME" = %s
            ''',
            (file_id, REPOSITORY_VOLUME),
        )
        result = cur.fetchone()

    finally:
        conn.close()
    return result


def load_spectra_by_file_id(
    file_id: int,
    equipment_id: int | str | None,
    state_id: int | str | None,
    district_id: int | str | None,
    site_id: int | str | None,
    start_date: str | None,
    end_date: str | None,
    freq_start: float | None,
    freq_end: float | None,
    description: str | None,
) -> list[dict[str, object]]:
    """Read every spectrum of one file and flag those matched by the filters.

    Args:
        file_id (int): Repository file identifier.
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.
        start_date (str | None): Inclusive start date in ISO format, or None.
        end_date (str | None): Inclusive end date in ISO format, or None.
        freq_start (float | None): Optional lower frequency bound in the existing
            catalog unit.
        freq_end (float | None): Optional upper frequency bound in the existing catalog
            unit.
        description (str | None): Optional spectrum description pattern.

    Returns:
        list[dict[str, object]]: Spectrum identifier, description, ranges, RBW,
            trace count, equipment label and IS_MATCH (0 or 1), newest first.
            Empty selection returns [].

    Raises:
        Exception: Database failure; propagated after closing the connection.
    """
    match_clauses, match_params = _build_summary_scoped_fact_filters(
        equipment_id=equipment_id,
        state_id=state_id,
        district_id=district_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        fact_alias="f",
        summary_alias="sm",
    )
    match_sql = " AND ".join(match_clauses) if match_clauses else "1 = 1"
    summary_join_sql = _build_summary_site_equipment_join(
        fact_alias="f",
        summary_alias="sm",
    )

    conn = get_connection_rfdata()
    try:
        cur = conn.cursor()

        # IS_MATCH is computed inline rather than via a subquery join so the DB
        # can evaluate the active filter conditions while scanning the file's
        # spectra in a single pass. The WHERE clause intentionally does NOT restrict
        # the result set: all spectra linked to the file are returned; only the
        # ones that caused the file to qualify are flagged for UI highlighting.
        cur.execute(
            f'''
            SELECT
                f."ID_SPECTRUM",
                f."NA_DESCRIPTION",
                f."NU_FREQ_START",
                f."NU_FREQ_END",
                f."DT_TIME_START",
                f."DT_TIME_END",
                f."NU_RBW",
                f."NU_TRACE_COUNT",
                COALESCE(sm."NA_EQUIPMENT", CONCAT('Equipamento ', f."FK_EQUIPMENT")) AS "NA_EQUIPMENT",
                CASE
                    WHEN {match_sql} THEN 1
                    ELSE 0
                END AS "IS_MATCH"
            FROM "BRIDGE_SPECTRUM_FILE" b
            JOIN "FACT_SPECTRUM" f
                ON f."ID_SPECTRUM" = b."FK_SPECTRUM"
            {summary_join_sql}
            WHERE b."FK_FILE" = %s
            ORDER BY f."DT_TIME_START" DESC NULLS LAST, f."ID_SPECTRUM" DESC
            ''',
            match_params + [file_id],
        )

        rows = cur.fetchall()
    finally:
        conn.close()
    return rows

