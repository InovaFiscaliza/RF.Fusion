"""Serialize appAnalise query results into JSON-compatible table envelopes."""

from datetime import date, datetime
from decimal import Decimal
import math

from .db_handler import TableResult


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
