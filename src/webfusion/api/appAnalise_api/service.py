"""Reuse paginated searches and serialize appAnalise table responses."""

from datetime import date, datetime
from dataclasses import replace
from decimal import Decimal
import math
from typing import cast

from .db_handler import AppAnaliseDB, TableResult
from .file_cache import FileQueryCache
from .filters import Filters


# Immutable IDs/counts are shared only within this process. SQL resources stay
# request-local; metadata for each page is always read through its own handler.
_FILE_QUERY_CACHE = FileQueryCache()


def get_file_page(filters: Filters, database: AppAnaliseDB) -> TableResult:
    """Reuse a bounded file index across HTTP pages without changing the client.

    Args:
        filters: Validated search and pagination values (Filters).
        database: Request-local handler owning SQL and connections (AppAnaliseDB).

    Returns:
        Current metadata for up to page_size + 1 ordered files (TableResult).
        Cached indices expire independently from current metadata reads.

    Raises:
        Exception: Index or metadata failures propagate to the HTTP handler.
    """
    search = replace(filters, page=1, page_size=1,
                     district_ids=tuple(sorted(set(filters.district_ids))))
    file_ids = cast(tuple[int, ...], _FILE_QUERY_CACHE.get_or_load(
        ("index", search), lambda: database.get_spectrum_file_index(search),
    ))
    offset = (filters.page - 1) * filters.page_size
    return database.get_spectrum_files_by_ids(file_ids[offset:offset + filters.page_size + 1])


def get_file_count(filters: Filters, database: AppAnaliseDB) -> int:
    """Cache the independent count query without using the listing index size.

    Args:
        filters: Validated search values (Filters); pagination is ignored.
        database: Request-local SQL handler (AppAnaliseDB).

    Returns:
        Matching file count using the legacy count JOIN scope (int).

    Raises:
        Exception: Query failures propagate and are never cached as zero.
    """
    search = replace(filters, page=1, page_size=1,
                     district_ids=tuple(sorted(set(filters.district_ids))))
    return cast(int, _FILE_QUERY_CACHE.get_or_load(
        ("count", search), lambda: database.get_spectrum_file_data_count(search),
    ))


def json_value(value: object) -> object:
    """Normalize database values before Flask serializes the response.

    Args:
        value: Database scalar or nested dict/list/tuple (object).

    Returns:
        JSON-compatible scalar, list or dictionary (object). Dates use ISO
        text without invented timezone; Decimal becomes a JSON number;
        missing/nonfinite numeric values become null. Integer IDs stay integers.

    Raises:
        TypeError: On unsupported database values instead of silently stringifying.
    """
    match value:
        case dict():
            return {key: json_value(item) for key, item in value.items()}
        case list() | tuple():
            return [json_value(item) for item in value]
        case datetime() | date():
            return value.isoformat()
        case Decimal():
            return json_value(float(value))
        case float():
            return value if math.isfinite(value) else None
        case None | str() | int():
            return value
        case _:
            raise TypeError(f"Unsupported response value: {type(value).__name__}")


def table_payload(result: TableResult) -> dict[str, object]:
    """Build a table envelope that retains column names when there are no rows.

    Args:
        result: Ordered columns and native database rows (TableResult).

    Returns:
        Required columns (list[str]) and rows (list[dict[str, object]]) keys
        in a JSON-compatible dictionary (dict[str, object]).
    """
    return {"columns": result.columns, "rows": json_value(result.rows)}
