"""Service queries for file search, spectrum details and repository downloads.

The search page returns one row per repository file. Spectrum filters select
matching files, while each file's metadata describes all its linked spectra.
The expandable details return the individual spectra for a selected file.

Read model strategy
-------------------
Geography-only facet queries prefer ``SITE_EQUIPMENT_METRIC`` joined with dimensions
(the summary read model). That table is a materialized view pre-joined
with geography and pre-aggregated by (site, equipment). It answers most filter
queries cheaply because it avoids scanning ``FACT_SPECTRUM``.

Date, frequency and description predicates require individual spectra, so facet
queries bypass the summary for those predicates. Missing summary data also falls
back to a live ``RFDATA.FACT_SPECTRUM`` query joined with dimension tables. That
path is authoritative but significantly heavier.

The fallback is explicit at each call site on purpose. An engineer debugging
a slow filter refresh should be able to tell immediately which code path ran
and why.

The interactive page uses a compact dependent-filter snapshot. Detailed menu
options probe for one matching spectrum per site/equipment pair, without
aggregating every spectrum. The full facet API retains its counts and dates.

Caching
-------
Query results use ``_SPECTRUM_QUERY_CACHE`` with per-entry TTLs. Equipment
selectors and download paths have separate caches. These caches smooth repeated
clicks and back/forward navigation within one Waitress process. They are not
shared across processes or invalidated on DB writes and are not a consistency
layer.

When query logic changes in a breaking way, increment
``FILE_RESULT_CACHE_VERSION`` or ``FILTER_OPTION_CACHE_VERSION`` to discard
in-flight cache entries built by the old query shape.
"""

from datetime import datetime
import logging
import os
import time

from modules.spectrum.config import (
    ALLOWED_SORT_ORDERS,
    SPECTRUM_QUERY_CACHE_TTL_SECONDS,
    EQUIPMENT_CACHE_TTL_SECONDS,
    FILE_PATH_CACHE_TTL_SECONDS,
    FILE_RESULT_FULL_CACHE_TTL_SECONDS,
    FILE_RESULT_CACHE_VERSION,
    FILTER_OPTION_CACHE_VERSION,
)
from db.config import SPECTRUM_FILE_SORT_FIELDS
from db.spectrum import (
    load_summary_site_availability_row as _load_summary_site_availability_row,
    load_summary_equipment_rows as _load_summary_equipment_rows,
    load_fact_equipment_rows as _load_fact_equipment_rows,
    load_fact_locality_rows as _load_fact_locality_rows,
    load_fact_equipment_filter_rows as _load_fact_equipment_filter_rows,
    load_summary_equipment_filter_rows as _load_summary_equipment_filter_rows,
    load_fact_state_rows as _load_fact_state_rows,
    load_summary_state_rows as _load_summary_state_rows,
    load_fact_district_rows as _load_fact_district_rows,
    load_summary_district_rows as _load_summary_district_rows,
    load_fact_filter_availability_row as _load_fact_filter_availability_row,
    load_summary_filter_availability_row as _load_summary_filter_availability_row,
    load_fact_site_availability_row as _load_fact_site_availability_row,
    load_selection_matches as _load_selection_matches,
    load_spectrum_file_rows,
    load_spectrum_site_option,
    load_file_by_spectrum_id,
    load_file_by_file_id,
    load_spectra_by_file_id,
)


LOGGER = logging.getLogger(__name__)


# These are intentionally tiny in-process caches. They smooth repeated clicks
# and back/forward navigation in one worker process, but they are not meant to
# be treated as a shared or authoritative cache layer.
_EQUIPMENT_CACHE = {"expires_at": 0.0, "value": None}
_SPECTRUM_QUERY_CACHE = {}
_FILE_PATH_CACHE = {}


def _coerce_text(value):
    """Normalize text values returned by database drivers."""

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    if value is None:
        return ""

    return str(value)


def _format_br_datetime(value):
    """Format a timestamp value for Brazilian-facing UI surfaces (DD-MM-YYYY).

    Handles raw datetime objects, ISO-8601 strings, and byte strings returned
    by older database driver versions. When the time component is exactly midnight,
    the time portion is suppressed — whole-day ranges stored as ``00:00:00``
    would clutter the display unnecessarily.

    Returns the original value unchanged when parsing fails, so it is safe to
    call on already-formatted strings without corrupting them.
    """

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


def _format_iso_date(value):
    """Format one date-like value as ``YYYY-MM-DD`` for HTML ``<input type="date">``.

    Returns None for missing or empty values. Falls back to slicing the first
    10 characters when full ISO parsing fails, which tolerates legacy date-only
    strings that may arrive from older rows in the catalog.
    """

    if value in (None, ""):
        return None

    text = _coerce_text(value).strip()

    if not text:
        return None

    normalized_text = text.replace("Z", "+00:00")

    try:
        parsed = datetime.fromisoformat(normalized_text.replace(" ", "T"))
    except ValueError:
        return text[:10]

    return parsed.strftime("%Y-%m-%d")


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

        raw_equipments = [
            part.strip()
            for part in _coerce_text(row.get("EQUIPMENT_LABELS")).split("||")
            if part and part.strip()
        ]
        row["EQUIPMENT_COUNT"] = int(row.get("EQUIPMENT_COUNT") or 0)

        if len(raw_equipments) == 1:
            row["EQUIPMENT_DISPLAY"] = raw_equipments[0]
        elif len(raw_equipments) > 1:
            row["EQUIPMENT_DISPLAY"] = f"{len(raw_equipments)} estacoes"
        else:
            row["EQUIPMENT_DISPLAY"] = "—"

        row["EQUIPMENT_DETAILS"] = " | ".join(raw_equipments)


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


def _build_district_option_label(row):
    """Build one district-facing label with county/state disambiguation."""

    district_name = _coerce_text(
        row.get("DISTRICT_NAME") or row.get("NA_DISTRICT") or f"Distrito {row['ID_DISTRICT']}"
    )
    county_name = _coerce_text(row.get("COUNTY_NAME") or row.get("NA_COUNTY"))
    state_code = _coerce_text(row.get("STATE_CODE") or row.get("LC_STATE"))
    context_parts = []

    if county_name and county_name.casefold() != district_name.casefold():
        context_parts.append(county_name)

    if state_code:
        context_parts.append(state_code)

    if not context_parts:
        return district_name

    return f"{district_name} ({'/'.join(context_parts)})"


def _finalize_equipment_options(rows):
    """Normalize equipment options for the dynamic spectrum filters."""

    options = []

    for row in rows:
        equipment_id = int(row["ID_EQUIPMENT"])
        equipment_name = _coerce_text(
            row.get("NA_EQUIPMENT") or row.get("EQUIPMENT_NAME") or f"Equipamento {equipment_id}"
        )
        options.append(
            {
                "ID_EQUIPMENT": equipment_id,
                "NA_EQUIPMENT": equipment_name,
                "OPTION_LABEL": equipment_name,
                "SPECTRUM_COUNT": int(row.get("SPECTRUM_COUNT") or 0),
                "DATE_START": _format_iso_date(row.get("DATE_START")),
                "DATE_END": _format_iso_date(row.get("DATE_END")),
            }
        )

    return options


def _finalize_state_options(rows):
    """Normalize state options with human-friendly `UF - nome` labels."""

    options = []

    for row in rows:
        state_id = int(row["ID_STATE"])
        state_code = _coerce_text(row.get("STATE_CODE") or row.get("LC_STATE"))
        state_name = _coerce_text(row.get("STATE_NAME") or row.get("NA_STATE"))
        option_label = state_code or state_name or f"Estado {state_id}"

        if state_code and state_name:
            option_label = f"{state_code} - {state_name}"

        options.append(
            {
                "ID_STATE": state_id,
                "STATE_CODE": state_code,
                "STATE_NAME": state_name,
                "OPTION_LABEL": option_label,
                "SPECTRUM_COUNT": int(row.get("SPECTRUM_COUNT") or 0),
                "DATE_START": _format_iso_date(row.get("DATE_START")),
                "DATE_END": _format_iso_date(row.get("DATE_END")),
            }
        )

    return options


def _finalize_district_options(rows):
    """Normalize district options keyed by `FK_DISTRICT` instead of `ID_SITE`."""

    options = []

    for row in rows:
        district_id = int(row["ID_DISTRICT"])
        district_name = _coerce_text(row.get("DISTRICT_NAME") or row.get("NA_DISTRICT"))
        county_name = _coerce_text(row.get("COUNTY_NAME") or row.get("NA_COUNTY"))
        state_code = _coerce_text(row.get("STATE_CODE") or row.get("LC_STATE"))
        options.append(
            {
                "ID_DISTRICT": district_id,
                "DISTRICT_NAME": district_name,
                "COUNTY_NAME": county_name,
                "STATE_CODE": state_code,
                "OPTION_LABEL": _build_district_option_label(row),
                "SITE_COUNT": int(row.get("SITE_COUNT") or 0),
                "SPECTRUM_COUNT": int(row.get("SPECTRUM_COUNT") or 0),
                "DATE_START": _format_iso_date(row.get("DATE_START")),
                "DATE_END": _format_iso_date(row.get("DATE_END")),
            }
        )

    return options


def _finalize_availability_row(row):
    """Normalize the date availability helper returned to routes and JS."""

    if not row:
        return None

    return {
        "DATE_START": _format_iso_date(row.get("DATE_START")),
        "DATE_END": _format_iso_date(row.get("DATE_END")),
        "DATE_START_DISPLAY": _format_br_datetime(row.get("DATE_START")),
        "DATE_END_DISPLAY": _format_br_datetime(row.get("DATE_END")),
        "SPECTRUM_COUNT": int(row.get("SPECTRUM_COUNT") or 0),
        "SITE_COUNT": int(row.get("SITE_COUNT") or 0),
        "DISTRICT_COUNT": int(row.get("DISTRICT_COUNT") or 0),
        "EQUIPMENT_COUNT": int(row.get("EQUIPMENT_COUNT") or 0),
    }


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


def get_spectrum_file_data(
    equipment_id: int | str | None = None,
    state_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    freq_start: float | None = None,
    freq_end: float | None = None,
    description: str | None = None,
    sort_by: str = "date_start",
    sort_order: str = "DESC",
    page: int | str = 1,
    page_size: int = 50
) -> tuple[list[dict], int]:
    """Return paginated file-mode rows plus the total file count.

    The only active search semantics are spectrum-aware: filters always apply
    to spectra first. The result list shown to the user is then collapsed to
    unique repository files, one row per file.

    That preserves the more powerful spectrum search while presenting the
    file-oriented download workflow without exposing spectrum IDs as the primary
    result grain.

    Two-level caching strategy:
    - The full sorted result set (all pages) is cached under ``full_cache_key``
      for ``FILE_RESULT_FULL_CACHE_TTL_SECONDS``. Pagination jumps within the
      same filter/sort state are served from this in-process slice without
      re-running the heavy grouped query.
    - Individual pages are also cached under a page-scoped key for the standard
      short TTL, so that minor cache-expiry races between workers do not cause
      unnecessary full re-runs.

    SQL note: filtered spectra are materialized once in a connection-local
    temporary table to identify matching files. Metadata is then aggregated
    over every spectrum linked to those files, independently of search filters.

    Args:
        equipment_id: Optional equipment ID, as int or str.
        state_id: Optional state ID, as int or str.
        district_id: Optional district ID, as int or str.
        site_id: Optional locality ID, as int or str.
        start_date: Optional inclusive ISO start date, as str.
        end_date: Optional inclusive ISO end date, as str.
        freq_start: Optional lower frequency bound in MHz, as float.
        freq_end: Optional upper frequency bound in MHz, as float.
        description: Optional spectrum description pattern, as str.
        sort_by: Allowed file sort field, as str.
        sort_order: ASC or DESC, as str.
        page: One-based page number, as int or str; invalid values use 1.
        page_size: Maximum rows per page, as int.

    Returns:
        Tuple of rows (list of dicts) and total matching files (int). Each row
        contains file identity/path/name/extension and size, full-file date and
        frequency bounds, spectrum count, equipment/locality counts and labels,
        and formatted date, display and detail strings added by postprocessing.

    Raises:
        Exception: When the database query fails.
    """
    if sort_by not in SPECTRUM_FILE_SORT_FIELDS:
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
        state_id,
        district_id,
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
        state_id,
        district_id,
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

    full_rows = load_spectrum_file_rows(equipment_id=equipment_id, state_id=state_id, district_id=district_id, site_id=site_id, start_date=start_date, end_date=end_date, freq_start=freq_start, freq_end=freq_end, description=description, sort_by=sort_by, sort_order=sort_order)

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


def get_spectrum_locality_options(
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
    """Return locality options aligned with the current unified search.

    Args:
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.
        start_date (str | None): Inclusive ISO start date.
        end_date (str | None): Inclusive ISO end date.
        freq_start (float | None): Optional lower frequency in the existing catalog
            unit.
        freq_end (float | None): Optional upper frequency in the existing catalog unit.
        description (str | None): Optional spectrum description pattern.

    Returns:
        list[dict[str, object]]: Site options with ID_SITE (int),
            LOCALITY_LABEL/OPTION_LABEL (str), COUNTY_NAME/STATE_CODE (str | None),
            SPECTRUM_COUNT (int) and DATE_START/DATE_END (str | None, existing
            DD-MM-YYYY format). Empty selection returns [].

    Raises:
        Exception: Invalid task input or a database operation fails.
    """

    cache_key = (
        "locality_options",
        equipment_id,
        state_id,
        district_id,
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

    rows = _load_fact_locality_rows(
        equipment_id=equipment_id,
        state_id=state_id,
        district_id=district_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
    )

    _format_row_datetime_fields(rows, "DATE_START", "DATE_END")
    result = _finalize_locality_options(rows)

    if result:
        _set_cached_query(cache_key, result)


    return result


def get_spectrum_filter_options(
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
    include_districts: bool = True,
    include_availability: bool = True,
) -> dict:
    """Return facet options matching the active query and its observed date range.

    Args:
        equipment_id: Optional equipment identifier, as int or str.
        state_id: Optional state identifier, as int or str.
        district_id: Optional district identifier, as int or str.
        site_id: Optional map site identifier, as int or str.
        start_date: Optional ISO start date, as str.
        end_date: Optional ISO end date, as str.
        freq_start: Optional lower frequency bound in MHz, as float.
        freq_end: Optional upper frequency bound in MHz, as float.
        description: Optional description pattern, as str.
        include_districts: Whether to load district options, as bool.
        include_availability: Whether to load the observed range, as bool.

    Returns:
        A dict with required keys equipments, states and districts (lists of
        option dicts with ID_* and OPTION_LABEL), and availability (a dict
        containing DATE_START, DATE_END, their *_DISPLAY strings and counts,
        or None when no observations match). Each facet excludes its own ID
        constraint. Availability excludes dates so it can suggest a new range.

    Raises:
        Exception: When a required fact-table query fails.
    """

    cache_key = (
        "filter_options",
        FILTER_OPTION_CACHE_VERSION,
        equipment_id,
        state_id,
        district_id,
        site_id,
        start_date,
        end_date,
        freq_start,
        freq_end,
        description,
        include_districts,
        include_availability,
    )
    cached = _get_cached_query(cache_key)

    if cached is not None:
        return cached

    equipments = []
    states = []
    districts = []
    availability = None
    has_detail_filters = bool(
        start_date or end_date or description
        or freq_start is not None or freq_end is not None
    )
    has_observation_filters = bool(
        description or freq_start is not None or freq_end is not None
    )

    # --- equipment options ---
    # Summary rows cannot resolve date, frequency or plan predicates.
    if not has_detail_filters:
        try:
            equipments = _finalize_equipment_options(
                _load_summary_equipment_filter_rows(
                    state_id=state_id,
                    district_id=district_id,
                    site_id=site_id,
                )
            )
        except Exception:
            LOGGER.exception(
                "failed_to_load_summary_spectrum_equipment_filters state_id=%s district_id=%s site_id=%s",
                state_id,
                district_id,
                site_id,
            )

    if not equipments:
        if state_id or district_id or site_id or start_date or end_date or description or freq_start is not None or freq_end is not None:
            equipments = _finalize_equipment_options(
                _load_fact_equipment_filter_rows(
                    state_id=state_id,
                    district_id=district_id,
                    site_id=site_id,
                    start_date=start_date,
                    end_date=end_date,
                    freq_start=freq_start,
                    freq_end=freq_end,
                    description=description,
                )
            )
        else:
            # No active filters at all: reuse the cached global equipment list
            # rather than doing another summary scan.
            equipments = _finalize_equipment_options(get_equipments())

    # --- state options ---
    # Detail predicates must use the same fact filters across every facet.
    if not has_detail_filters:
        try:
            states = _finalize_state_options(
                _load_summary_state_rows(
                    equipment_id=equipment_id,
                    district_id=district_id,
                    site_id=site_id,
                )
            )
        except Exception:
            LOGGER.exception(
                "failed_to_load_summary_spectrum_state_filters equipment_id=%s district_id=%s site_id=%s",
                equipment_id,
                district_id,
                site_id,
            )

    if not states:
        states = _finalize_state_options(
            _load_fact_state_rows(
                equipment_id=equipment_id,
                district_id=district_id,
                site_id=site_id,
                start_date=start_date,
                end_date=end_date,
                freq_start=freq_start,
                freq_end=freq_end,
                description=description,
            )
        )

    districts = []
    if include_districts:
        # --- district options ---
        # Districts are the most granular geo dimension and the most expensive
        # to scan. The flag avoids this cost for state/equipment-only callers.
        if not has_detail_filters:
            try:
                districts = _finalize_district_options(
                    _load_summary_district_rows(
                        equipment_id=equipment_id,
                        state_id=state_id,
                        site_id=site_id,
                    )
                )
            except Exception:
                LOGGER.exception(
                    "failed_to_load_summary_spectrum_district_filters equipment_id=%s state_id=%s site_id=%s",
                    equipment_id,
                    state_id,
                    site_id,
                )

        if not districts:
            districts = _finalize_district_options(
                _load_fact_district_rows(
                    equipment_id=equipment_id,
                    state_id=state_id,
                    site_id=site_id,
                    start_date=start_date,
                    end_date=end_date,
                    freq_start=freq_start,
                    freq_end=freq_end,
                    description=description,
                )
            )

    availability = None
    if include_availability:
        # --- availability window ---
        # Dates do not constrain their own suggestion; frequency and plan do.
        if not has_observation_filters:
            try:
                availability = _finalize_availability_row(
                    _load_summary_filter_availability_row(
                        equipment_id=equipment_id,
                        state_id=state_id,
                        district_id=district_id,
                        site_id=site_id,
                    )
                )
            except Exception:
                LOGGER.exception(
                    "failed_to_load_summary_spectrum_availability equipment_id=%s state_id=%s district_id=%s site_id=%s",
                    equipment_id,
                    state_id,
                    district_id,
                    site_id,
                )

        if availability is None:
            availability = _finalize_availability_row(
                _load_fact_filter_availability_row(
                    equipment_id=equipment_id,
                    state_id=state_id,
                    district_id=district_id,
                    site_id=site_id,
                    freq_start=freq_start,
                    freq_end=freq_end,
                    description=description,
                )
            )

    result = {
        "equipments": equipments,
        "states": states,
        "districts": districts,
        "availability": availability if availability and availability["DATE_START"] and availability["DATE_END"] else None,
    }
    _set_cached_query(cache_key, result)
    return result


def get_spectrum_selection_options(
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
    include_districts: bool = True,
    include_availability: bool = True,
) -> dict:
    """Return one coherent, compact snapshot for the page's dependent filters.

    Args:
        equipment_id: Optional equipment ID, as int or str; scopes availability.
        state_id: Optional state ID, as int or str; scopes districts and equipment.
        district_id: Optional district ID, as int or str; scopes equipment.
        site_id: Optional map site ID, as int or str; scopes availability.
        start_date: Optional inclusive ISO start date, as str; scopes options.
        end_date: Optional inclusive ISO end date, as str; scopes options.
        freq_start: Optional lower frequency bound in MHz, as float.
        freq_end: Optional upper frequency bound in MHz, as float.
        description: Optional plan pattern, as str.
        include_districts: Whether to load districts, as bool.
        include_availability: Whether to load observed dates, as bool.

    Returns:
        A dict with equipments, states and districts, each a list of dicts
        containing the corresponding ID_* (int) and OPTION_LABEL (str).
        States remain global. Districts exclude equipment, district and site;
        equipment excludes equipment and site. Availability excludes dates,
        includes all other filters, and contains DATE_START, DATE_END and their
        *_DISPLAY strings, or None. Per-option counts and dates are omitted.

    Raises:
        Exception: When a required fact query fails.
    """
    cache_key = (
        "selection_options", FILTER_OPTION_CACHE_VERSION,
        equipment_id, state_id, district_id, site_id, start_date, end_date,
        freq_start, freq_end, description, include_districts, include_availability,
    )
    cached = _get_cached_query(cache_key)
    if cached is not None:
        return cached
    has_details = bool(start_date or end_date or description
                       or freq_start is not None or freq_end is not None)
    # Global states never depend on the current detail predicates.
    states = get_spectrum_filter_options(include_districts=False, include_availability=False)["states"]
    equipments = []
    districts = []
    needs_matches = has_details
    load_districts = include_districts and bool(state_id or has_details)
    if not has_details:
        try:
            equipments = _finalize_equipment_options(_load_summary_equipment_filter_rows(
                state_id=state_id, district_id=district_id,
            ))
            if load_districts:
                districts = _finalize_district_options(_load_summary_district_rows(state_id=state_id))
            needs_matches = not equipments or (load_districts and not districts)
        except Exception:
            LOGGER.exception("failed_to_load_summary_spectrum_selection_options")
            needs_matches = True
    if needs_matches:
        match_key = ("selection_matches", FILTER_OPTION_CACHE_VERSION, state_id,
                     start_date, end_date, freq_start, freq_end, description)
        matches = _get_cached_query(match_key)
        if matches is None:
            matches = _load_selection_matches(
                state_id=state_id, start_date=start_date, end_date=end_date,
                freq_start=freq_start, freq_end=freq_end, description=description,
            )
            _set_cached_query(match_key, matches)
        equipment_rows = {
            row["ID_EQUIPMENT"]: row for row in matches
            if not district_id or str(row["ID_DISTRICT"]) == str(district_id)
        }
        equipments = sorted(_finalize_equipment_options(equipment_rows.values()),
                            key=lambda row: row["OPTION_LABEL"].casefold())
        if load_districts:
            district_rows = {
                (row["ID_DISTRICT"], row.get("COUNTY_NAME"), row.get("STATE_CODE")): row
                for row in matches if row["ID_DISTRICT"] is not None
            }
            districts = _finalize_district_options(district_rows.values())

    availability = None
    if include_availability:
        availability_key = ("selection_availability", FILTER_OPTION_CACHE_VERSION,
                            equipment_id, state_id, district_id, site_id,
                            freq_start, freq_end, description)
        cached_availability = _get_cached_query(availability_key)
        if cached_availability is not None:
            availability = cached_availability["availability"]
        else:
            availability = _load_selection_availability(
                equipment_id=equipment_id, state_id=state_id, district_id=district_id,
                site_id=site_id, freq_start=freq_start, freq_end=freq_end, description=description,
            )
            _set_cached_query(availability_key, {"availability": availability})
    result = {"availability": availability}
    for key, rows, id_key in (("equipments", equipments, "ID_EQUIPMENT"),
                              ("states", states, "ID_STATE"),
                              ("districts", districts, "ID_DISTRICT")):
        result[key] = [{id_key: row[id_key], "OPTION_LABEL": row["OPTION_LABEL"]} for row in rows]
    _set_cached_query(cache_key, result)
    return result


def _load_selection_availability(
    *,
    equipment_id: int | str | None = None,
    state_id: int | str | None = None,
    district_id: int | str | None = None,
    site_id: int | str | None = None,
    freq_start: float | None = None,
    freq_end: float | None = None,
    description: str | None = None,
) -> dict | None:
    """Load exact observed dates without constraining them by input dates.

    Args:
        equipment_id: Optional equipment ID, as int or str.
        state_id: Optional state ID, as int or str.
        district_id: Optional district ID, as int or str.
        site_id: Optional map site ID, as int or str.
        freq_start: Optional lower frequency bound in MHz, as float.
        freq_end: Optional upper frequency bound in MHz, as float.
        description: Optional plan pattern, as str.

    Returns:
        A dict with DATE_START, DATE_END (ISO str) and their *_DISPLAY (str),
        or None if no observed dates are available.

    Raises:
        Exception: When the required fact query fails.
    """
    availability = None
    has_observation_filters = bool(description or freq_start is not None or freq_end is not None)
    geo = dict(equipment_id=equipment_id, state_id=state_id,
               district_id=district_id, site_id=site_id)
    if not has_observation_filters:
        try:
            availability = _finalize_availability_row(_load_summary_filter_availability_row(**geo))
        except Exception:
            LOGGER.exception("failed_to_load_summary_spectrum_selection_availability")
    if availability is None or not availability["DATE_START"] or not availability["DATE_END"]:
        availability = _finalize_availability_row(_load_fact_filter_availability_row(
            **geo, freq_start=freq_start, freq_end=freq_end, description=description,
        ))
    if availability and availability["DATE_START"] and availability["DATE_END"]:
        return {key: value for key, value in availability.items() if key.startswith("DATE_")}
    return None


def get_spectrum_site_option(site_id: int | str | None) -> dict[str, object] | None:
    """Return one locality option by site id for preselected map navigation.

    This supports deep-link scenarios where the route already knows ``site_id``
    and needs to rehydrate the select option even before the full dependent
    locality query runs in the browser.

    Args:
        site_id (int | str | None): Optional site identifier.

    Returns:
        dict[str, object] | None: Locality option with ID_SITE (int),
            LOCALITY_LABEL/OPTION_LABEL (str), geographic labels (str | None), optional
            ID_DISTRICT/ID_STATE (int | None), DISTRICT_NAME/STATE_NAME (str | None),
            count and dates. Missing selection returns None.

    Raises:
        Exception: Invalid task input or a database operation fails.
    """

    if site_id in (None, ""):
        return None

    row = load_spectrum_site_option(site_id=site_id)

    if not row:
        return None

    option = _finalize_locality_options([row])[0]
    option["ID_DISTRICT"] = int(row["ID_DISTRICT"]) if row.get("ID_DISTRICT") is not None else None
    option["DISTRICT_NAME"] = row.get("DISTRICT_NAME")
    option["ID_STATE"] = int(row["ID_STATE"]) if row.get("ID_STATE") is not None else None
    option["STATE_NAME"] = row.get("STATE_NAME")
    return option


def get_spectrum_site_availability_range(*, equipment_id=None, site_id=None):
    """Return the observed date range for one (equipment, site) pair.

    Used by the spectrum route when a preselected site no longer appears in the
    current locality list (e.g., filtered out by an active date range). The
    intent is explanatory UX — "available from X to Y at this site" — rather
    than a full result query.

    Tries the summary model first; falls back to a live FACT_SPECTRUM scan if
    the summary has no matching (equipment, site) row. Date fields are formatted
    after both paths so the caller receives a consistent dict regardless of
    which source ran.

    Returns None when either argument is empty or no matching rows exist.
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
        # Normalize date fields here for both the summary and fact paths.
        # The fact loader (_load_fact_site_availability_row) deliberately does
        # not format dates so this call is the single formatting point.
        _format_single_row_datetime_fields(row, "DATE_START", "DATE_END")

    if row:
        _set_cached_query(cache_key, row)

    return row


def get_file_by_spectrum_id(spectrum_id: int) -> str | None:
    """Resolve the repository path for a single spectrum identifier.

    The unified page no longer renders one row per spectrum, but legacy links
    may still ask for a download starting from ``ID_SPECTRUM`` instead of the
    repository file id.

    Args:
        spectrum_id (int): Spectrum identifier from an existing download link.

    Returns:
        str | None: Repository path for the newest reposfi file linked to this spectrum;
            None when file identity or path is absent.

    Raises:
        Exception: Invalid task input or a database operation fails.
    """
    cache_key = ("spectrum_file_path", spectrum_id)
    cached = _get_cached_file_path(cache_key)

    if cached is not None:
        return cached
    result = load_file_by_spectrum_id(spectrum_id=spectrum_id)

    if not result:
        return None

    na_path = result["NA_PATH"]
    na_file = result["NA_FILE"]

    if not na_path or not na_file:
        return None

    file_path = os.path.join(na_path, na_file)
    _set_cached_file_path(cache_key, file_path)
    return file_path


def get_file_by_file_id(file_id: int) -> str | None:
    """Resolve the repository path for one repository file row.

    Args:
        file_id (int): Repository file identifier.

    Returns:
        str | None: Repository path for the selected reposfi file; None when file
            identity or path is absent.

    Raises:
        Exception: Invalid task input or a database operation fails.
    """
    cache_key = ("file_id_path", file_id)
    cached = _get_cached_file_path(cache_key)

    if cached is not None:
        return cached
    result = load_file_by_file_id(file_id=file_id)

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
    file_id: int,
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
    """Return every spectrum linked to one file plus the active-match flag.

    The unified search returns one row per file, but the detail panel still
    shows the complete contents of that file. ``IS_MATCH`` marks the spectra
    that justified the file entering the result set so the UI can highlight
    them without hiding the rest.

    Args:
        file_id (int): Repository file identifier.
        equipment_id (int | str | None): Optional equipment identifier.
        state_id (int | str | None): Optional state identifier.
        district_id (int | str | None): Optional district identifier.
        site_id (int | str | None): Optional site identifier.
        start_date (str | None): Inclusive ISO start date.
        end_date (str | None): Inclusive ISO end date.
        freq_start (float | None): Optional lower frequency in the existing catalog
            unit.
        freq_end (float | None): Optional upper frequency in the existing catalog unit.
        description (str | None): Optional spectrum description pattern.

    Returns:
        list[dict[str, object]]: All linked spectra with ID_SPECTRUM (int),
            NA_DESCRIPTION (str | None), NU_FREQ_START/NU_FREQ_END/NU_RBW (numeric |
            None), NU_TRACE_COUNT (int | None), NA_EQUIPMENT (str), IS_MATCH (integer
            flag), and DT_TIME_START/DT_TIME_END (formatted str | None). Empty selection
            returns [].

    Raises:
        Exception: Invalid task input or a database operation fails.
    """
    cache_key = (
        "file_spectra",
        file_id,
        equipment_id,
        state_id,
        district_id,
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

    rows = load_spectra_by_file_id(file_id=file_id, equipment_id=equipment_id, state_id=state_id, district_id=district_id, site_id=site_id, start_date=start_date, end_date=end_date, freq_start=freq_start, freq_end=freq_end, description=description)

    _format_row_datetime_fields(rows, "DT_TIME_START", "DT_TIME_END")

    _set_cached_query(cache_key, rows)
    return rows
