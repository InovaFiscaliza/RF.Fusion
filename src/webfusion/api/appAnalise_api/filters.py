"""Validate HTTP filter values without constructing SQL from client text."""

from dataclasses import dataclass
from datetime import datetime, time
import math

from . import config as k


@dataclass(frozen=True)
class Filters:
    """Hold validated values for the queries ported from DBHandler.m.

    Attributes:
        equipment_id: Optional equipment identifier (int | None).
        site_id: Optional site identifier (int | None).
        district_ids: District identifiers (tuple[int, ...]), empty by default.
        state_code: Optional trimmed state code (str | None).
        start_date: Inclusive day start for temporal overlap (datetime | None).
        end_date: Inclusive day end for temporal overlap (datetime | None).
        freq_start: Optional lower frequency bound (float | None), in database units.
        freq_end: Optional upper frequency bound (float | None), in database units.
        description: Optional LIKE input (str | None), preserving % and _ wildcards.
        page: One-based page number (int), defaults to config.DEFAULT_PAGE.
        page_size: Requested page size (int), defaults to config.DEFAULT_PAGE_SIZE.
    """

    equipment_id: int | None = None
    site_id: int | None = None
    district_ids: tuple[int, ...] = ()
    state_code: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    freq_start: float | None = None
    freq_end: float | None = None
    description: str | None = None
    page: int = k.DEFAULT_PAGE
    page_size: int = k.DEFAULT_PAGE_SIZE


def _parse_text(value: object, name: str) -> str | None:
    """Validate optional text and normalize blank values.

    Args:
        value: Untrusted scalar (object); null and empty strings mean absent.
        name: Field name used in validation errors (str).

    Returns:
        Trimmed text (str | None).

    Raises:
        ValueError: When a non-string value is supplied.
    """
    match value:
        case None:
            return None
        case str():
            return value.strip() or None
        case _:
            raise ValueError(f"{name} deve ser texto.")


def _parse_frequency(value: object, name: str) -> float | None:
    """Validate a finite numeric filter.

    Args:
        value: Number or numeric string (object), or null/blank for absence.
        name: Public field name (str).

    Returns:
        Finite value (float | None).

    Raises:
        ValueError: On booleans, containers, nonnumbers or nonfinite values.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{name} deve ser um número finito.")
    try:
        result = float(value)
    except (ValueError, OverflowError) as exc:
        raise ValueError(f"{name} deve ser um número finito.") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} deve ser um número finito.")
    return result


def _parse_identifier(value: object, name: str) -> int | None:
    """Validate an optional positive integer without float precision loss.

    Args:
        value: Integer, integral float or decimal integer string (object).
        name: Public field name (str).

    Returns:
        Positive integer (int | None); null/blank means absent.

    Raises:
        ValueError: On invalid, fractional, boolean or nonpositive input.
    """
    if value is None or value == "":
        return None
    try:
        if isinstance(value, bool) or not isinstance(value, (int, float, str)):
            raise ValueError
        result = int(value)
        if result < 1 or (isinstance(value, float) and value != result):
            raise ValueError
    except (ValueError, OverflowError) as exc:
        raise ValueError(f"{name} deve ser um inteiro positivo.") from exc
    return result


def _parse_date(value: object, name: str, *, end: bool = False) -> datetime | None:
    """Convert an ISO calendar date to the legacy inclusive day boundary.

    Args:
        value: ISO date or datetime string (object), null or blank.
        name: Public field name (str).
        end: Whether to use 23:59:59 instead of midnight (bool).

    Returns:
        Database-local day boundary (datetime | None), with no timezone conversion.

    Raises:
        ValueError: On invalid ISO dates or timezone-bearing timestamps.
    """
    value = _parse_text(value, name)
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is not None:
            raise ValueError
        boundary = time(23, 59, 59) if end else time.min
        return datetime.combine(parsed.date(), boundary)
    except ValueError as exc:
        raise ValueError(f"{name} deve ser uma data ISO sem fuso (AAAA-MM-DD).") from exc


def parse_filters(values: dict[str, object]) -> Filters:
    """Parse the documented camelCase appAnalise filters.

    Args:
        values: Optional fields listed in config.FILTER_NAMES (dict[str, object]).
            districtId accepts an integer, integer list, or comma-separated text;
            all other fields are scalars. Null/blank values mean absent.

    Returns:
        Validated query values (Filters).

    Raises:
        ValueError: On unknown fields, invalid types or exceeded request limits.
    """
    unknown = values.keys() - k.FILTER_NAMES
    if unknown:
        raise ValueError("Parâmetros desconhecidos: " + ", ".join(sorted(unknown)))
    match values.get("districtId"):
        case None | "":
            districts = []
        case str() as text:
            districts = text.split(",")
        case list() as items:
            districts = items
        case value:
            districts = [value]
    if len(districts) > k.MAX_DISTRICTS:
        raise ValueError(f"districtId aceita até {k.MAX_DISTRICTS} identificadores.")
    district_ids = tuple(_parse_identifier(value, "districtId") for value in districts)
    if None in district_ids:
        raise ValueError("districtId não aceita itens vazios.")
    page_size = _parse_identifier(values.get("pageSize"), "pageSize") or k.DEFAULT_PAGE_SIZE
    if page_size > k.MAX_PAGE_SIZE:
        raise ValueError(f"pageSize deve ser no máximo {k.MAX_PAGE_SIZE}.")
    return Filters(
        equipment_id=_parse_identifier(values.get("equipmentId"), "equipmentId"),
        site_id=_parse_identifier(values.get("siteId"), "siteId"),
        district_ids=district_ids,
        state_code=_parse_text(values.get("stateCode"), "stateCode"),
        start_date=_parse_date(values.get("startDate"), "startDate"),
        end_date=_parse_date(values.get("endDate"), "endDate", end=True),
        freq_start=_parse_frequency(values.get("freqStart"), "freqStart"),
        freq_end=_parse_frequency(values.get("freqEnd"), "freqEnd"),
        description=_parse_text(values.get("description"), "description"),
        page=_parse_identifier(values.get("page"), "page") or k.DEFAULT_PAGE,
        page_size=page_size,
    )
