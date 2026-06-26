#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Incremental refresh engine for the public RFFUSION_SUMMARY tables.

Architecture overview
---------------------
RFFUSION_SUMMARY is a *read-model* database that mirrors denormalized views of
BPDATA (operational backup/processing state) and RFDATA (spectrum measurements).
It exists so that the web dashboard and Grafana can query pre-aggregated data
without touching the transactional databases and without relying on MariaDB stored
events, which caused heavy deadlocks under concurrent workloads.

The engine is driven by ``appCataloga_summary_database.py``, which runs as
a long-lived daemon.  Each polling loop either:

  * performs a **full reconcile** on startup when needed and on the daily
    02:00 BRT maintenance slot by calling :meth:`refresh_all`;
  * or processes an incremental **outbox batch** by calling
    :meth:`refresh_for_events`, which coalesces the outbox rows into a
    :class:`DirtyScope` and refreshes only the affected summary objects.

Dependency order of public summary objects
------------------------------------------
Rebuild always follows this sequence (later tables depend on earlier ones)::

    SITE_EQUIPMENT_OBS_SUMMARY   ← RFDATA.FACT_SPECTRUM + DIM_*
    HOST_EQUIPMENT_LINK          ← BPDATA.HOST + RFDATA.DIM_SPECTRUM_EQUIPMENT
    HOST_LOCATION_SUMMARY        ← SITE_EQUIPMENT_OBS_SUMMARY + HOST_EQUIPMENT_LINK
    MAP_SITE_STATION_SUMMARY     ← SITE_EQUIPMENT_OBS_SUMMARY + HOST_EQUIPMENT_LINK
    MAP_SITE_SUMMARY             ← MAP_SITE_STATION_SUMMARY
    HOST_MONTHLY_METRIC          ← BPDATA.FILE_TASK_HISTORY
    HOST_ERROR_SUMMARY           ← four BPDATA error sources (see _read_error_events)
    SERVER_ERROR_SUMMARY         ← HOST_ERROR_SUMMARY (cross-host rollup)
    HOST_CURRENT_SNAPSHOT        ← BPDATA.HOST + several summary tables above
    SERVER_CURRENT_SUMMARY       ← HOST_CURRENT_SNAPSHOT + HOST_MONTHLY_METRIC

Two write strategies are used:
  * **replace_table_rows** — truncate + bulk-insert; safe for full reconciles.
  * **upsert_rows / delete + upsert** — surgical update for incremental scopes.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple

import config as k

if TYPE_CHECKING:
    from db.dbHandlerSummary import dbHandlerSummary


class SummaryLogger(Protocol):
    """Minimal logger contract required by the summary refresh engine."""

    def event(self, event: str, **fields: Any) -> None:
        ...

    def warning_event(self, event: str, **fields: Any) -> None:
        ...


def _safe_int(value: Any) -> Optional[int]:
    """Convert a DB value to ``int``, returning ``None`` for SQL NULLs."""
    if value is None:
        return None
    return int(value)


def _coalesce_text(*values: Any) -> Optional[str]:
    """Return the first non-None, non-empty string among the candidates.

    Mirrors SQL ``COALESCE`` for Python-side label building where the DB query
    cannot know which column will be populated for a given row.

    Args:
        *values: Any number of candidate values.  Each is converted with
                 ``str(v).strip()`` before the emptiness check.

    Returns:
        The first non-empty string, or ``None`` if all candidates are empty.
    """
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _kb_to_gb(value_kb: Any) -> float:
    """Convert a kilobyte counter to gigabytes rounded to two decimal places.

    Used when projecting BPDATA's KB-denominated size columns into the GB
    columns expected by summary tables and dashboards.

    Args:
        value_kb: Raw KB value from the database (``None`` treated as zero).

    Returns:
        Float rounded to 2 decimal places (e.g., 1 048 576 KB → 1.0 GB).
    """
    return round(float(value_kb or 0) / 1024 / 1024, 2)


def _month_start(value: Any) -> Optional[str]:
    """Normalize a date-like value into the ``YYYY-MM-01`` monthly bucket string.

    Used to map any date/datetime coming from the DB into the consistent bucket
    key stored in ``DT_REFERENCE_MONTH`` columns.  Accepts datetime objects,
    ``YYYY-MM-DD`` strings, and ``YYYY-MM`` strings.

    Args:
        value: A ``datetime``, a ``%Y-%m-%d`` string, or a ``%Y-%m`` string.
               ``None`` and empty strings return ``None``.

    Returns:
        A ``'YYYY-MM-01'`` string, or ``None`` if the value cannot be parsed.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-01")
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10:
        text = text[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-01")
    except ValueError:
        try:
            return datetime.strptime(text, "%Y-%m").strftime("%Y-%m-01")
        except ValueError:
            return None


def _normalize_key(value: Any) -> str:
    """Produce a punctuation-free lowercase token for fuzzy name comparison.

    Host names (``BPDATA.HOST.NA_HOST_NAME``) and equipment names
    (``RFDATA.DIM_SPECTRUM_EQUIPMENT.NA_EQUIPMENT``) often differ only in
    separators (dashes, underscores, spaces), case, or light formatting.  This
    function strips all such noise so that ``_refresh_host_equipment_link`` can
    compare normalized tokens directly.

    Args:
        value: Any value; ``None`` produces an empty string.

    Returns:
        Lowercase alphanumeric-only string (all ``-_. /:;,()[]`` removed).
    """
    text = str(value or "").strip().lower()
    for char in "-_. /:;,()[]":
        text = text.replace(char, "")
    return text


def _cwsm_family_prefix_for_station_suffix(station_suffix: int) -> Optional[str]:
    """Return the short-host CWSM family prefix for one station suffix.

    CelPlan long receiver identifiers are not fully self-describing: several
    historical ``cwsm2110xxxx`` equipment rows actually belong to the
    ``CWSM212xxx`` or ``CWSM220xxx`` short-host families.  Operationally the
    family is determined by the station suffix range, not only by the long-form
    prefix persisted in RFDATA.

    The currently observed catalog uses these ranges:

    - ``01``–``22``  → ``CWSM211xxx``
    - ``26``–``37``  → ``CWSM212xxx``
    - ``38``–``47``  → ``CWSM220xxx``

    Args:
        station_suffix: Integer value derived from the last two digits of the
            long-form CWSM receiver identifier.

    Returns:
        The 3-digit family prefix used by the short host name, or ``None`` when
        the suffix is outside the known CelPlan allocation ranges.
    """
    if 1 <= station_suffix <= 22:
        return "211"
    if 26 <= station_suffix <= 37:
        return "212"
    if 38 <= station_suffix <= 47:
        return "220"
    return None


def _cwsm_signature(normalized_key: str) -> Optional[str]:
    """Collapse known CWSM equipment/host naming variants into a canonical signature.

    CWSM-series RFEye equipment and their Zabbix host entries use several
    historical naming schemes whose numeric suffixes encode the same physical
    unit differently.  This function maps all known variants to the same
    ``cwsmXXXYYY`` token so that ``_refresh_host_equipment_link`` can match
    them even when exact-normalized comparison fails.

    Special case: equipment ``cwsm22010007`` is mapped to ``cwsm211007`` to
    correct a known data-entry error in historical records.

    Args:
        normalized_key: Output of :func:`_normalize_key` for the host or
                        equipment name.  Only strings starting with ``'cwsm'``
                        are processed; others return ``None``.

    Returns:
        A ``'cwsmXXXYYY'`` signature string, or ``None`` if the key does not
        match the expected CWSM prefix/digit pattern.
    """
    if not normalized_key or not normalized_key.startswith("cwsm"):
        return None

    digits = normalized_key[4:]  # strip the leading 'cwsm' prefix
    if not digits or not digits.isdigit() or len(digits) < 6:
        return None

    if digits == "22010007":              # known data-entry error → fixed signature
        return "cwsm211007"

    if len(digits) >= 8:
        # CelPlan short host families are allocated by station suffix range,
        # not strictly by the long receiver prefix stored in RFDATA.  That is
        # why identifiers such as `cwsm21100031` and `cwsm21100044` must match
        # `CWSM212031` and `CWSM220044`, respectively.
        family_prefix = _cwsm_family_prefix_for_station_suffix(int(digits[-2:]))
        if family_prefix:
            return f"cwsm{family_prefix}{digits[-3:]}"

        prefix = digits[:4]              # first four digits encode the unit series
        if prefix == "2110":
            return f"cwsm211{digits[-3:]}"  # e.g. cwsm21100007 → cwsm211007
        if prefix == "2112":
            return f"cwsm212{digits[-3:]}"
        if prefix == "2201":
            return f"cwsm220{digits[-3:]}"

    # Shorter variants: take first 3 + last 3 digits as the canonical token.
    return f"cwsm{digits[:3]}{digits[-3:]}"


def _map_state(is_host_known: bool, is_offline: bool, is_current_location: bool) -> str:
    """Classify one map-marker state from host presence, connectivity, and recency.

    The five resulting states drive the pin colour and dashboard filter chips in
    the webfusion map view.  ``is_current_location`` is true when the
    (equipment, site) pair is the most-recently-seen location for that equipment.

    Args:
        is_host_known:      Whether the equipment has a matched BPDATA host.
        is_offline:         Whether the matched host is currently marked offline.
        is_current_location: Whether this (equipment, site) pair represents the
                             equipment's most recent observation location.

    Returns:
        One of: ``'no_host'``, ``'online_current'``, ``'online_previous'``,
        ``'offline_current'``, ``'offline_previous'``.
    """
    if not is_host_known:
        return "no_host"
    if is_offline:
        return "offline_current" if is_current_location else "offline_previous"
    return "online_current" if is_current_location else "online_previous"


def _map_priority(state: str) -> int:
    """Return the sort priority for a map-marker state (lower = more prominent).

    Used by ``MAP_SITE_SUMMARY`` to derive the best representative state when a
    site has multiple equipment with different states.  The site-level pin
    colour corresponds to the state with the lowest (best) priority value.

    Priority table::

        0  online_current    — equipment recently seen, host online
        1  online_previous   — equipment seen in the past, host online
        2  offline_current   — equipment recently seen, host offline
        3  offline_previous  — equipment seen in the past, host offline
        4  no_host / unknown — equipment not matched to any host

    Args:
        state: A :func:`_map_state` return value.

    Returns:
        Integer priority (0–4), defaulting to 4 for unknown states.
    """
    return {
        "online_current": 0,
        "online_previous": 1,
        "offline_current": 2,
        "offline_previous": 3,
    }.get(state, 4)


def _build_locality_label(row: Dict[str, Any]) -> str:
    """Build the human-readable locality label stored in ``NA_LOCALITY_LABEL``.

    The label is shown in map tooltips and dashboard list views.  It tries to
    compose a descriptive string in the form::

        <site_name> · <county>/<state_code>

    Falling back gracefully when fields are absent.  The county suffix is
    omitted when it duplicates the site name; the state code is always appended
    when available.

    Args:
        row: A dict containing any combination of ``NA_SITE_NAME``,
             ``NA_DISTRICT_NAME``, ``NA_COUNTY_NAME``, ``NA_STATE_CODE``,
             and ``FK_SITE``.

    Returns:
        A non-empty display string, always at least ``'Site <FK_SITE>'``.
    """
    site_name = _coalesce_text(
        row.get("NA_SITE_NAME"),
        row.get("NA_DISTRICT_NAME"),
        row.get("NA_COUNTY_NAME"),
        f"Site {row['FK_SITE']}",
    )
    county = _coalesce_text(row.get("NA_COUNTY_NAME"))
    state_code = _coalesce_text(row.get("NA_STATE_CODE"))
    label = site_name or f"Site {row['FK_SITE']}"

    if county and (not site_name or site_name.lower() != county.lower()):
        label = f"{label} · {county}"
    if state_code:
        label = f"{label}/{state_code}"
    return label.strip()


class DirtyScope:
    """Coalesced invalidation scope extracted from canonical outbox rows.

    Each ``SUMMARY_OUTBOX`` row carries one explicit dirty-scope key
    (host/site/equipment/reference_month/full_reconcile). ``DirtyScope`` merges
    a batch of those rows into one consolidated set of affected keys so the
    engine can refresh only the minimum set of summary objects.

    When any row carries the ``full_reconcile`` scope, the scope short-circuits:
    ``full_reconcile`` is set to ``True`` and
    :meth:`SummaryRefreshEngine.refresh_for_events` delegates to a full
    :meth:`SummaryRefreshEngine.refresh_all` pass.

    Attributes:
        host_ids:         FK_HOST values invalidated by this batch.
        site_ids:         FK_SITE values invalidated by this batch.
        equipment_ids:    FK_EQUIPMENT values invalidated by this batch.
        reference_months: ``YYYY-MM-01`` month buckets invalidated by this batch.
        full_reconcile:   When ``True``, a complete rebuild is required.
    """

    def __init__(self) -> None:
        """Initialize an empty scope; populate via :meth:`from_outbox_rows`."""
        self.host_ids: set[int] = set()
        self.site_ids: set[int] = set()
        self.equipment_ids: set[int] = set()
        self.reference_months: set[str] = set()
        self.full_reconcile: bool = False

    @classmethod
    def from_outbox_rows(cls, rows: Sequence[Dict[str, Any]]) -> "DirtyScope":
        """Merge many canonical outbox rows into one consolidated refresh scope.

        Args:
            rows: List of dicts as returned by
                  :meth:`dbHandlerSummary.read_outbox_batch`.

        Returns:
            A populated :class:`DirtyScope` instance.

        Raises:
            RuntimeError: When one outbox row has an invalid scope type or value.
        """
        scope = cls()
        for row in rows:
            row_id = row.get("ID_OUTBOX")
            scope_type = row.get("NA_SCOPE_TYPE")
            scope_value = row.get("NA_SCOPE_VALUE")

            try:
                if scope_type == k.SUMMARY_SCOPE_HOST:
                    scope.host_ids.add(int(scope_value))
                elif scope_type == k.SUMMARY_SCOPE_SITE:
                    scope.site_ids.add(int(scope_value))
                elif scope_type == k.SUMMARY_SCOPE_EQUIPMENT:
                    scope.equipment_ids.add(int(scope_value))
                elif scope_type == k.SUMMARY_SCOPE_REFERENCE_MONTH:
                    normalized_month = _month_start(scope_value)
                    if normalized_month is None:
                        raise ValueError(f"invalid reference month {scope_value!r}")
                    scope.reference_months.add(normalized_month)
                elif scope_type == k.SUMMARY_SCOPE_FULL_RECONCILE:
                    if str(scope_value) != k.SUMMARY_SCOPE_FULL_RECONCILE_KEY:
                        raise ValueError(
                            f"invalid full_reconcile key {scope_value!r}"
                        )
                    scope.full_reconcile = True
                else:
                    raise ValueError(f"unknown scope type {scope_type!r}")
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    "Invalid SUMMARY_OUTBOX row "
                    f"ID_OUTBOX={row_id!r} "
                    f"NA_SCOPE_TYPE={scope_type!r} "
                    f"NA_SCOPE_VALUE={scope_value!r}: {exc}"
                ) from exc
        return scope


class SummaryRefreshEngine:
    """Stateless refresh engine for the public RFFUSION_SUMMARY read models.

    The engine owns all SQL reads from BPDATA/RFDATA and all writes to
    RFFUSION_SUMMARY.  It is intentionally stateless between calls — each
    ``refresh_*`` invocation reads fresh data from the source databases.

    The engine never calls ``_connect`` / ``_disconnect`` directly; instead,
    every read goes through :meth:`_select` and every write goes through the
    ``db`` handler's public methods (``replace_table_rows``, ``upsert_rows``,
    ``execute_delete``).  Refresh lifecycle state (start/success/failure) is
    tracked via :class:`dbHandlerSummary` helper methods.

    Attributes:
        db:  A :class:`dbHandlerSummary` instance shared with the worker loop.
        log: The application logger (``logging_utils.log()``).
    """

    def __init__(self, db: dbHandlerSummary, logger: SummaryLogger) -> None:
        """Bind the summary DB handler and application logger.

        Args:
            db:     A connected :class:`dbHandlerSummary` instance.
            logger: Application logger supporting ``.event()``,
                    and ``.warning_event()`` methods.
        """
        self.db = db
        self.log = logger

    def refresh_all(self, *, reason: str) -> List[str]:
        """Rebuild every public summary object in dependency order.

        This is the **full reconcile** path.  It runs at worker startup when
        freshness requires it and on the scheduled daily reconcile window.
        All summary tables are rebuilt from scratch using the
        ``replace_table_rows`` strategy (truncate + bulk-insert).

        Args:
            reason: Free-form label logged with the completion event
                    (e.g. ``'scheduled_full_reconcile'``, ``'outbox_full_reconcile'``).

        Returns:
            Ordered list of object names that were successfully refreshed.
        """
        refreshed = [
            self._run_refresh(object_name, refresh_fn)
            for object_name, refresh_fn in (
                ("SITE_EQUIPMENT_OBS_SUMMARY", self._refresh_site_equipment_obs_summary),
                ("HOST_EQUIPMENT_LINK", self._refresh_host_equipment_link),
                ("HOST_LOCATION_SUMMARY", self._refresh_host_location_summary),
                ("MAP_SITE_STATION_SUMMARY", self._refresh_map_site_station_summary),
                ("MAP_SITE_SUMMARY", self._refresh_map_site_summary),
                ("HOST_MONTHLY_METRIC", self._refresh_host_monthly_metric),
                ("HOST_ERROR_SUMMARY", self._refresh_host_error_summary),
                ("SERVER_ERROR_SUMMARY", self._refresh_server_error_summary),
                ("HOST_CURRENT_SNAPSHOT", self._refresh_host_current_snapshot),
                ("SERVER_CURRENT_SUMMARY", self._refresh_server_current_summary),
            )
        ]
        self.log.event(
            "summary_full_reconcile_completed",
            component="summary_engine",
            operation="refresh_all",
            reason=reason,
            objects=refreshed,
        )
        return refreshed

    def refresh_for_events(self, events: Sequence[Dict[str, Any]]) -> List[str]:
        """Coalesce one outbox batch and refresh only the affected summary objects.

        This is the **incremental** path.  After merging the batch into a
        :class:`DirtyScope`, the engine decides which summary objects need
        refreshing based on which entity types are present in the scope:

        - ``site_ids`` or ``equipment_ids`` → SITE_EQUIPMENT_OBS_SUMMARY,
          HOST_EQUIPMENT_LINK, HOST_LOCATION_SUMMARY, MAP_SITE_STATION_SUMMARY,
          MAP_SITE_SUMMARY.
        - ``host_ids`` or ``reference_months`` → HOST_MONTHLY_METRIC,
          HOST_ERROR_SUMMARY, SERVER_ERROR_SUMMARY.
        - Any of the above → HOST_CURRENT_SNAPSHOT, SERVER_CURRENT_SUMMARY.
        - ``full_reconcile`` outbox scope → delegates to :meth:`refresh_all`.

        For objects that support scoped incremental refresh the engine uses the
        delete-then-upsert strategy: it deletes only the rows matching the dirty
        scope and re-inserts the freshly computed rows for those keys.

        Args:
            events: List of outbox row dicts as returned by
                    :meth:`dbHandlerSummary.read_outbox_batch`.

        Returns:
            List of object names that were actually refreshed (empty if the
            scope contained no relevant keys).
        """
        scope = DirtyScope.from_outbox_rows(events)
        if scope.full_reconcile:
            return self.refresh_all(reason="outbox_full_reconcile")

        refreshed: List[str] = []

        if scope.site_ids or scope.equipment_ids:
            refreshed.append(
                self._run_refresh(
                    "SITE_EQUIPMENT_OBS_SUMMARY",
                    lambda: self._refresh_site_equipment_obs_summary(
                        site_ids=scope.site_ids,
                        equipment_ids=scope.equipment_ids,
                    ),
                )
            )
            for object_name, refresh_fn in (
                ("HOST_EQUIPMENT_LINK", self._refresh_host_equipment_link),
                ("HOST_LOCATION_SUMMARY", self._refresh_host_location_summary),
                ("MAP_SITE_STATION_SUMMARY", self._refresh_map_site_station_summary),
                ("MAP_SITE_SUMMARY", self._refresh_map_site_summary),
            ):
                refreshed.append(self._run_refresh(object_name, refresh_fn))

        if scope.host_ids or scope.reference_months:
            refreshed.append(
                self._run_refresh(
                    "HOST_MONTHLY_METRIC",
                    lambda: self._refresh_host_monthly_metric(
                        host_ids=scope.host_ids,
                        reference_months=scope.reference_months,
                    ),
                )
            )
            refreshed.append(
                self._run_refresh(
                    "HOST_ERROR_SUMMARY",
                    lambda: self._refresh_host_error_summary(host_ids=scope.host_ids),
                )
            )
            refreshed.append(self._run_refresh("SERVER_ERROR_SUMMARY", self._refresh_server_error_summary))

        if scope.host_ids or scope.site_ids or scope.equipment_ids or scope.reference_months:
            for object_name, refresh_fn in (
                ("HOST_CURRENT_SNAPSHOT", self._refresh_host_current_snapshot),
                ("SERVER_CURRENT_SUMMARY", self._refresh_server_current_summary),
            ):
                refreshed.append(self._run_refresh(object_name, refresh_fn))

        if refreshed:
            self.log.event(
                "summary_incremental_refresh_completed",
                component="summary_engine",
                operation="refresh_for_events",
                refreshed=refreshed,
                host_ids=sorted(scope.host_ids),
                site_ids=sorted(scope.site_ids),
                equipment_ids=sorted(scope.equipment_ids),
                reference_months=sorted(scope.reference_months),
                events=len(events),
            )
        return refreshed

    def _run_refresh(
        self,
        object_name: str,
        refresh_fn: Callable[[], Tuple[int, str]],
    ) -> str:
        """Execute one refresh step with lifecycle bookkeeping.

        Calls ``refresh_fn()``, which must return ``(row_count, watermark)``.
        The start timestamp is captured locally. On success,
        :meth:`dbHandlerSummary.summary_refresh_success` appends the success row
        to the rolling audit log; on exception,
        :meth:`dbHandlerSummary.summary_refresh_failure` appends the failure row
        and re-raises so the worker loop can catch it.

        Args:
            object_name: The summary table name used in the audit log.
            refresh_fn:  A zero-argument callable returning ``(int, str)``:
                         ``(row_count, high_watermark)``.

        Returns:
            ``object_name`` on success (allows callers to collect refreshed names).

        Raises:
            Exception: Whatever ``refresh_fn`` raises, after recording failure.
        """
        started_at = datetime.utcnow()
        try:
            row_count, watermark = refresh_fn()
        except Exception as exc:
            self.db.summary_refresh_failure(
                object_name,
                started_at=started_at,
                error_message=str(exc),
            )
            raise

        self.db.summary_refresh_success(
            object_name,
            started_at=started_at,
            row_count=row_count,
            high_watermark=watermark,
        )
        return object_name

    def _select(self, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        """Execute one read-only parameterized query and return rows as dicts.

        Handles the ``_connect`` / ``_disconnect`` lifecycle so that individual
        refresh methods do not need to manage the connection directly.  Uses
        :meth:`dbHandlerBase._select_raw` which goes through ``self.cursor``.

        Args:
            sql:    Plain SQL string with ``%s`` placeholders.
            params: Positional parameter values matching the placeholders.

        Returns:
            List of ``{column_name: value}`` dicts, one per result row.
        """
        self.db._connect()
        try:
            return self.db._select_raw(sql, tuple(params))
        finally:
            self.db._disconnect()

    def _build_in_clause(
        self,
        column: str,
        values: Iterable[Any],
        params: List[Any],
    ) -> Optional[str]:
        """Build one parameterized SQL ``IN`` clause and extend the params list.

        Appends the non-None values from ``values`` to ``params`` in-place and
        returns the corresponding ``column IN (%s, %s, ...)`` fragment, or
        ``None`` when the value set is empty (so callers can skip the WHERE
        fragment entirely).

        Args:
            column: Fully-qualified column reference (e.g. ``'f.FK_HOST'``).
            values: Iterable of candidate values; ``None`` entries are ignored.
            params: Mutable list to extend with the bound values.

        Returns:
            SQL fragment string, or ``None`` if the value set is empty.
        """
        normalized = [value for value in values if value is not None]
        if not normalized:
            return None
        params.extend(normalized)
        placeholders = ", ".join(["%s"] * len(normalized))
        return f"{column} IN ({placeholders})"

    def _delete_with_scope(
        self,
        table: str,
        *,
        site_ids: Optional[Iterable[int]] = None,
        equipment_ids: Optional[Iterable[int]] = None,
        host_ids: Optional[Iterable[int]] = None,
        reference_months: Optional[Iterable[str]] = None,
    ) -> int:
        """Delete only the summary rows covered by the current dirty scope.

        This is the first half of the delete-then-upsert incremental strategy.
        When a scope contains both ``host_ids`` and ``reference_months``, their
        filter is ANDed together (rows must match both host AND month), then
        ORed against any site/equipment filters so that all affected rows are
        removed in a single ``DELETE`` statement.

        Each read model owns its own composite primary key, so this method
        issues a SQL-based ``DELETE … WHERE`` rather than using the generic
        :meth:`dbHandlerBase._delete_row` helper.

        Args:
            table:            Target RFFUSION_SUMMARY table name (no schema prefix).
            site_ids:         FK_SITE values whose rows should be deleted.
            equipment_ids:    FK_EQUIPMENT values whose rows should be deleted.
            host_ids:         FK_HOST values whose rows should be deleted.
            reference_months: ``YYYY-MM-01`` month strings whose rows should be
                              deleted.

        Returns:
            Number of rows deleted (``cursor.rowcount``).  Returns ``0`` when
            all scope iterables are empty (no-op guard).
        """
        clauses: List[str] = []
        params: List[Any] = []

        site_clause = self._build_in_clause("FK_SITE", site_ids or [], params)
        equipment_clause = self._build_in_clause("FK_EQUIPMENT", equipment_ids or [], params)
        host_clause = self._build_in_clause("FK_HOST", host_ids or [], params)
        month_clause = self._build_in_clause("DT_REFERENCE_MONTH", reference_months or [], params)

        # Start with site, equipment, and host as independent OR candidates.
        for clause in (site_clause, equipment_clause, host_clause):
            if clause:
                clauses.append(clause)

        if month_clause and host_clause:
            # When both host and month are present, AND them so we delete only
            # (host, month) combinations — not every row for those hosts.
            # Any separately provided site/equipment clauses are ORed on top.
            clauses = [f"({host_clause} AND {month_clause})"]
            if site_clause or equipment_clause:
                for extra_clause in (site_clause, equipment_clause):
                    if extra_clause:
                        clauses.append(extra_clause)
        elif month_clause:
            # Month alone: delete all rows for those months regardless of host.
            clauses.append(month_clause)

        if not clauses:  # empty scope → nothing to delete
            return 0

        # Each read model owns its own primary key shape, so scope deletion is SQL-based.
        sql = f"DELETE FROM {table} WHERE " + " OR ".join(f"({clause})" for clause in clauses)
        return self.db.execute_delete(sql, params)

    def _resolve_site_equipment_refresh_scope(
        self,
        *,
        site_ids: Optional[Iterable[int]] = None,
        equipment_ids: Optional[Iterable[int]] = None,
    ) -> List[int]:
        """Expand a dirty site/equipment scope into full affected equipment ids.

        Incremental site-scoped refreshes must recompute every row for the
        affected equipment across all sites. Otherwise an update touching only
        one historical site can incorrectly re-mark that old site as the
        equipment's current location because the newer sites were absent from
        the filtered query.

        The scope is resolved from two sources:
        - current RFDATA facts for the dirty sites;
        - existing summary rows for those sites, so stale rows can still be
          removed even if the source rows disappeared later.
        """
        affected_equipment_ids = {
            int(value)
            for value in (equipment_ids or [])
            if value is not None
        }
        normalized_site_ids = {
            int(value)
            for value in (site_ids or [])
            if value is not None
        }

        if not normalized_site_ids:
            return sorted(affected_equipment_ids)

        params: List[Any] = []
        site_clause = self._build_in_clause(
            "FK_SITE",
            sorted(normalized_site_ids),
            params,
        )
        if site_clause is None:
            return sorted(affected_equipment_ids)

        source_rows = self._select(
            f"""
            SELECT DISTINCT FK_EQUIPMENT
            FROM RFDATA.FACT_SPECTRUM
            WHERE {site_clause}
            """,
            tuple(params),
        )
        for row in source_rows:
            if row.get("FK_EQUIPMENT") is not None:
                affected_equipment_ids.add(int(row["FK_EQUIPMENT"]))

        existing_rows = self._select(
            f"""
            SELECT DISTINCT FK_EQUIPMENT
            FROM SITE_EQUIPMENT_OBS_SUMMARY
            WHERE {site_clause}
            """,
            tuple(params),
        )
        for row in existing_rows:
            if row.get("FK_EQUIPMENT") is not None:
                affected_equipment_ids.add(int(row["FK_EQUIPMENT"]))

        return sorted(affected_equipment_ids)

    def _refresh_site_equipment_obs_summary(
        self,
        *,
        site_ids: Optional[Iterable[int]] = None,
        equipment_ids: Optional[Iterable[int]] = None,
    ) -> Tuple[int, str]:
        """Refresh the per-(site, equipment) observation summary from RFDATA.

        Reads ``RFDATA.FACT_SPECTRUM`` joined to site/county/district/state
        dimension tables and aggregates first-seen, last-seen, and spectrum
        counts per (FK_SITE, FK_EQUIPMENT) pair.  An additional Python-side
        pass marks ``IS_CURRENT_LOCATION = 1`` for the site where each
        equipment was most recently observed.

        Incremental refreshes widen any dirty site scope into the full set of
        affected equipment ids and then recompute every site row for those
        equipment ids. This keeps ``IS_CURRENT_LOCATION`` globally unique per
        equipment instead of only unique inside one partial site slice.

        Write strategy:
            - Full scope (no site/equipment filter): ``replace_table_rows``
              (truncate + bulk-insert).
            - Partial scope: delete every row for the affected equipment ids,
              then upsert the fully recomputed replacements for those
              equipment ids.

        Args:
            site_ids:      Optional FK_SITE filter.  When ``None`` or empty,
                           all sites are refreshed.
            equipment_ids: Optional FK_EQUIPMENT filter.  Combined with
                           ``site_ids`` using OR (either match triggers refresh).

        Returns:
            ``(row_count, watermark)`` where ``watermark`` is
            ``'rows=<n>'``.
        """
        params: List[Any] = []
        clauses: List[str] = []
        scoped_equipment_ids: List[int] = []

        if site_ids or equipment_ids:
            scoped_equipment_ids = self._resolve_site_equipment_refresh_scope(
                site_ids=site_ids,
                equipment_ids=equipment_ids,
            )
            if not scoped_equipment_ids:
                return 0, "rows=0"
            equipment_clause = self._build_in_clause(
                "f.FK_EQUIPMENT",
                scoped_equipment_ids,
                params,
            )
            if equipment_clause:
                clauses.append(equipment_clause)
        else:
            site_clause = self._build_in_clause("f.FK_SITE", site_ids or [], params)
            equipment_clause = self._build_in_clause("f.FK_EQUIPMENT", equipment_ids or [], params)
            if site_clause:
                clauses.append(site_clause)
            if equipment_clause:
                clauses.append(equipment_clause)

        where_sql = ""
        if clauses:
            where_sql = "WHERE " + " OR ".join(f"({clause})" for clause in clauses)

        rows = self._select(
            f"""
            SELECT
                f.FK_SITE,
                f.FK_EQUIPMENT,
                s.NA_SITE AS NA_SITE_NAME,
                COALESCE(NULLIF(s.NA_SITE, ''), CONCAT('Site ', s.ID_SITE)) AS NA_SITE_LABEL,
                s.FK_COUNTY,
                s.FK_DISTRICT,
                c.NA_COUNTY AS NA_COUNTY_NAME,
                d.NA_DISTRICT AS NA_DISTRICT_NAME,
                st.ID_STATE,
                st.NA_STATE AS NA_STATE_NAME,
                st.LC_STATE AS NA_STATE_CODE,
                ST_Y(s.GEO_POINT) AS VL_LATITUDE,
                ST_X(s.GEO_POINT) AS VL_LONGITUDE,
                s.NU_ALTITUDE AS VL_ALTITUDE,
                s.NU_GNSS_MEASUREMENTS,
                e.NA_EQUIPMENT,
                MIN(f.DT_TIME_START) AS DT_FIRST_SEEN_AT,
                MAX(f.DT_TIME_END) AS DT_LAST_SEEN_AT,
                COUNT(*) AS NU_SPECTRUM_COUNT,
                MAX(f.ID_SPECTRUM) AS ID_LAST_SPECTRUM
            FROM RFDATA.FACT_SPECTRUM f
            JOIN RFDATA.DIM_SPECTRUM_EQUIPMENT e
              ON e.ID_EQUIPMENT = f.FK_EQUIPMENT
            JOIN RFDATA.DIM_SPECTRUM_SITE s
              ON s.ID_SITE = f.FK_SITE
            LEFT JOIN RFDATA.DIM_SITE_COUNTY c
              ON c.ID_COUNTY = s.FK_COUNTY
            LEFT JOIN RFDATA.DIM_SITE_DISTRICT d
              ON d.ID_DISTRICT = s.FK_DISTRICT
            LEFT JOIN RFDATA.DIM_SITE_STATE st
              ON st.ID_STATE = s.FK_STATE
            {where_sql}
            GROUP BY
                f.FK_SITE,
                f.FK_EQUIPMENT,
                s.NA_SITE,
                s.FK_COUNTY,
                s.FK_DISTRICT,
                c.NA_COUNTY,
                d.NA_DISTRICT,
                st.ID_STATE,
                st.NA_STATE,
                st.LC_STATE,
                ST_Y(s.GEO_POINT),
                ST_X(s.GEO_POINT),
                s.NU_ALTITUDE,
                s.NU_GNSS_MEASUREMENTS,
                e.NA_EQUIPMENT
            """,
            tuple(params),
        )

        # First pass: find the most-recent (site, timestamp) per equipment so we can
        # mark IS_CURRENT_LOCATION = 1 only on the latest observation site.
        # Tuple key: (last_seen, first_seen, FK_SITE) — all fields compared lexicographically.
        newest_by_equipment: Dict[int, Tuple[Any, Any, int]] = {}
        for row in rows:
            equipment_id = int(row["FK_EQUIPMENT"])
            candidate = (
                row.get("DT_LAST_SEEN_AT") or row.get("DT_FIRST_SEEN_AT"),
                row.get("DT_FIRST_SEEN_AT") or row.get("DT_LAST_SEEN_AT"),
                int(row["FK_SITE"]),
            )
            if equipment_id not in newest_by_equipment or candidate > newest_by_equipment[equipment_id]:
                newest_by_equipment[equipment_id] = candidate

        refreshed_at = datetime.utcnow()
        payload_rows: List[Dict[str, Any]] = []
        # Second pass: build payload rows; compare each (equipment, site) marker
        # against the per-equipment best tuple to set IS_CURRENT_LOCATION.
        for row in rows:
            equipment_id = int(row["FK_EQUIPMENT"])
            site_id = int(row["FK_SITE"])
            marker = (
                row.get("DT_LAST_SEEN_AT") or row.get("DT_FIRST_SEEN_AT"),
                row.get("DT_FIRST_SEEN_AT") or row.get("DT_LAST_SEEN_AT"),
                site_id,
            )
            payload_rows.append(
                {
                    "FK_SITE": site_id,
                    "FK_EQUIPMENT": equipment_id,
                    "NA_SITE_NAME": row.get("NA_SITE_NAME"),
                    "NA_SITE_LABEL": row.get("NA_SITE_LABEL"),
                    "FK_COUNTY": row.get("FK_COUNTY"),
                    "FK_DISTRICT": row.get("FK_DISTRICT"),
                    "NA_COUNTY_NAME": row.get("NA_COUNTY_NAME"),
                    "NA_DISTRICT_NAME": row.get("NA_DISTRICT_NAME"),
                    "ID_STATE": row.get("ID_STATE"),
                    "NA_STATE_NAME": row.get("NA_STATE_NAME"),
                    "NA_STATE_CODE": row.get("NA_STATE_CODE"),
                    "VL_LATITUDE": row.get("VL_LATITUDE"),
                    "VL_LONGITUDE": row.get("VL_LONGITUDE"),
                    "VL_ALTITUDE": row.get("VL_ALTITUDE"),
                    "NU_GNSS_MEASUREMENTS": row.get("NU_GNSS_MEASUREMENTS"),
                    "NA_EQUIPMENT": row.get("NA_EQUIPMENT"),
                    "DT_FIRST_SEEN_AT": row.get("DT_FIRST_SEEN_AT"),
                    "DT_LAST_SEEN_AT": row.get("DT_LAST_SEEN_AT"),
                    "NU_SPECTRUM_COUNT": int(row.get("NU_SPECTRUM_COUNT") or 0),
                    "ID_LAST_SPECTRUM": row.get("ID_LAST_SPECTRUM"),
                    "IS_CURRENT_LOCATION": 1 if newest_by_equipment.get(equipment_id) == marker else 0,
                    "DT_REFRESHED_AT": refreshed_at,
                }
            )

        if not clauses:
            self.db.replace_table_rows("SITE_EQUIPMENT_OBS_SUMMARY", payload_rows)
        else:
            self._delete_with_scope(
                "SITE_EQUIPMENT_OBS_SUMMARY",
                equipment_ids=scoped_equipment_ids,
            )
            if payload_rows:
                self.db.upsert_rows(
                    table="SITE_EQUIPMENT_OBS_SUMMARY",
                    rows=payload_rows,
                    unique_keys=["FK_SITE", "FK_EQUIPMENT"],
                )

        watermark = f"rows={len(payload_rows)}"
        return len(payload_rows), watermark

    def _refresh_host_equipment_link(self) -> Tuple[int, str]:
        """Rebuild the host-to-equipment name-matching reconciliation table.

        Reads all host names from ``BPDATA.HOST`` and all equipment names from
        ``RFDATA.DIM_SPECTRUM_EQUIPMENT`` and attempts to reconcile them through
        three matching strategies (in descending confidence order):

        1. **exact_normalized** (1.00): Normalized names are identical.
        2. **cwsm_signature** (0.95): Both produce the same
           :func:`_cwsm_signature` token (handles known numerical suffix
           variants of CWSM-series equipment).
        3. **prefix_match** (0.60): One normalized name starts with the other
           (minimum 6-character prefix required to reduce false positives).
        4. **manual_override** (2.00): An explicit row in
           ``HOST_EQUIPMENT_LINK_OVERRIDE`` forces the link regardless of name
           similarity.

        For equipment with multiple candidate hosts only the highest-confidence
        match is marked ``IS_PRIMARY_LINK = 1`` (single best match, no tie).
        All candidates are written with ``IS_ACTIVE = 1`` so callers can audit
        the full matching landscape. The public table keeps only the fields
        consumed by downstream summary builders and ``webfusion`` readers.

        Write strategy: always ``replace_table_rows`` (full rebuild) because
        the reconciliation is global and any host or equipment rename can affect
        any row.

        Returns:
            ``(row_count, watermark)`` where ``watermark`` is
            ``'hosts=<n>;equipments=<m>'``.
        """
        hosts = self._select(
            """
            SELECT ID_HOST AS FK_HOST, NA_HOST_NAME
            FROM BPDATA.HOST
            WHERE NA_HOST_NAME IS NOT NULL
              AND TRIM(NA_HOST_NAME) <> ''
            """
        )
        equipments = self._select(
            """
            SELECT ID_EQUIPMENT AS FK_EQUIPMENT, NA_EQUIPMENT
            FROM RFDATA.DIM_SPECTRUM_EQUIPMENT
            WHERE NA_EQUIPMENT IS NOT NULL
              AND TRIM(NA_EQUIPMENT) <> ''
            """
        )
        overrides = self._select(
            """
            SELECT FK_HOST, FK_EQUIPMENT
            FROM HOST_EQUIPMENT_LINK_OVERRIDE
            WHERE IS_ACTIVE = 1
            """
        )

        # Pre-compute normalized keys and CWSM signatures once; reused in the O(H×E) loop.
        host_map = {}
        for row in hosts:
            normalized = _normalize_key(row["NA_HOST_NAME"])
            host_map[int(row["FK_HOST"])] = {
                "FK_HOST": int(row["FK_HOST"]),
                "NA_HOST_NAME": row["NA_HOST_NAME"],
                "NA_HOST_NAME_NORMALIZED": normalized,
                "NA_HOST_SIGNATURE": _cwsm_signature(normalized),
            }

        equipment_map = {}
        for row in equipments:
            normalized = _normalize_key(row["NA_EQUIPMENT"])
            equipment_map[int(row["FK_EQUIPMENT"])] = {
                "FK_EQUIPMENT": int(row["FK_EQUIPMENT"]),
                "NA_EQUIPMENT": row["NA_EQUIPMENT"],
                "NA_EQUIPMENT_NAME_NORMALIZED": normalized,
                "NA_EQUIPMENT_SIGNATURE": _cwsm_signature(normalized),
            }

        # Keyed by (FK_HOST, FK_EQUIPMENT); keeps only the best match per pair.
        candidates: Dict[Tuple[int, int], Dict[str, Any]] = {}

        def _consider(host_row: Dict[str, Any], equipment_row: Dict[str, Any], match_type: str, confidence: float, is_manual: int) -> None:
            """Record the best reconciliation candidate for a (host, equipment) pair.

            If a candidate already exists for this pair, it is replaced only
            when the new candidate's (confidence, is_manual, match_type) tuple
            is strictly greater than the existing one.

            This function captures ``candidates`` from the enclosing scope.
            """
            key = (host_row["FK_HOST"], equipment_row["FK_EQUIPMENT"])
            candidate = {
                "FK_HOST": host_row["FK_HOST"],
                "FK_EQUIPMENT": equipment_row["FK_EQUIPMENT"],
                "NA_HOST_NAME": host_row["NA_HOST_NAME"],
                "NA_EQUIPMENT": equipment_row["NA_EQUIPMENT"],
                "NA_HOST_NAME_NORMALIZED": host_row["NA_HOST_NAME_NORMALIZED"],
                "NA_EQUIPMENT_NAME_NORMALIZED": equipment_row["NA_EQUIPMENT_NAME_NORMALIZED"],
                "NA_HOST_SIGNATURE": host_row["NA_HOST_SIGNATURE"],
                "NA_EQUIPMENT_SIGNATURE": equipment_row["NA_EQUIPMENT_SIGNATURE"],
                "NA_MATCH_TYPE": match_type,
                "VL_MATCH_CONFIDENCE": confidence,
                "IS_MANUAL_OVERRIDE": is_manual,
            }
            current = candidates.get(key)
            # Replace the existing candidate only if the new one wins on the
            # lexicographic tuple (confidence, is_manual, match_type).
            if current is None or (
                candidate["VL_MATCH_CONFIDENCE"],
                candidate["IS_MANUAL_OVERRIDE"],
                candidate["NA_MATCH_TYPE"],
            ) > (
                current["VL_MATCH_CONFIDENCE"],
                current["IS_MANUAL_OVERRIDE"],
                current["NA_MATCH_TYPE"],
            ):
                candidates[key] = candidate

        # Materialise equipment list once outside the host loop to avoid repeated dict.values() calls.
        equipment_rows = list(equipment_map.values())
        for host_row in host_map.values():
            host_normalized = host_row["NA_HOST_NAME_NORMALIZED"]
            host_signature = host_row["NA_HOST_SIGNATURE"]
            for equipment_row in equipment_rows:
                equipment_normalized = equipment_row["NA_EQUIPMENT_NAME_NORMALIZED"]
                equipment_signature = equipment_row["NA_EQUIPMENT_SIGNATURE"]
                if host_normalized and host_normalized == equipment_normalized:
                    _consider(host_row, equipment_row, "exact_normalized", 1.00, 0)
                elif (
                    host_signature
                    and equipment_signature
                    and host_signature == equipment_signature
                ):
                    _consider(host_row, equipment_row, "cwsm_signature", 0.95, 0)
                elif (
                    len(host_normalized) >= 6
                    and len(equipment_normalized) >= 6
                    and (
                        host_normalized.startswith(equipment_normalized)
                        or equipment_normalized.startswith(host_normalized)
                    )
                ):
                    _consider(host_row, equipment_row, "prefix_match", 0.60, 0)

        for override in overrides:
            host_row = host_map.get(int(override["FK_HOST"]))
            equipment_row = equipment_map.get(int(override["FK_EQUIPMENT"]))
            if host_row and equipment_row:
                _consider(host_row, equipment_row, "manual_override", 2.00, 1)

        # Group candidates by equipment so each equipment gets its own ranked list.
        grouped_by_equipment: Dict[int, List[Dict[str, Any]]] = {}
        for candidate in candidates.values():
            grouped_by_equipment.setdefault(candidate["FK_EQUIPMENT"], []).append(candidate)

        payload_rows: List[Dict[str, Any]] = []
        # Tiebreaker rank when confidence and is_manual are equal (lower is better).
        match_type_rank = {
            "manual_override": 0,
            "exact_normalized": 1,
            "cwsm_signature": 2,
            "prefix_match": 3,
        }

        for equipment_id, equipment_candidates in grouped_by_equipment.items():
            # Sort descending by confidence, then by manual flag, then by type rank, then by host id.
            equipment_candidates.sort(
                key=lambda item: (
                    -float(item["VL_MATCH_CONFIDENCE"]),
                    -int(item["IS_MANUAL_OVERRIDE"]),
                    match_type_rank.get(item["NA_MATCH_TYPE"], 99),
                    int(item["FK_HOST"]),
                )
            )

            top = equipment_candidates[0]
            # Count how many candidates share the top (confidence, is_manual) pair.
            # IS_PRIMARY_LINK = 1 only when there is a single unambiguous best match.
            top_tie_count = sum(
                1
                for item in equipment_candidates
                if float(item["VL_MATCH_CONFIDENCE"]) == float(top["VL_MATCH_CONFIDENCE"])
                and int(item["IS_MANUAL_OVERRIDE"]) == int(top["IS_MANUAL_OVERRIDE"])
            )

            for index, candidate in enumerate(equipment_candidates):
                payload_rows.append(
                    {
                        "FK_HOST": candidate["FK_HOST"],
                        "FK_EQUIPMENT": candidate["FK_EQUIPMENT"],
                        "NA_EQUIPMENT": candidate["NA_EQUIPMENT"],
                        "NA_MATCH_TYPE": candidate["NA_MATCH_TYPE"],
                        "VL_MATCH_CONFIDENCE": candidate["VL_MATCH_CONFIDENCE"],
                        "IS_MANUAL_OVERRIDE": candidate["IS_MANUAL_OVERRIDE"],
                        "IS_PRIMARY_LINK": 1 if index == 0 and top_tie_count == 1 else 0,
                        "IS_ACTIVE": 1,
                    }
                )

        self.db.replace_table_rows("HOST_EQUIPMENT_LINK", payload_rows)
        watermark = f"hosts={len(hosts)};equipments={len(equipments)}"
        return len(payload_rows), watermark

    def _refresh_host_location_summary(self) -> Tuple[int, str]:
        """Rebuild the per-(host, site) locality rollup for dashboards and maps.

        Joins ``SITE_EQUIPMENT_OBS_SUMMARY`` with ``HOST_EQUIPMENT_LINK``
        (primary links only) to produce one row per (FK_HOST, FK_SITE) pair.
        Aggregates spectrum counts, matched equipment sets, first/last-seen
        dates, and the best match-confidence across all equipment at that site.

        The ``NA_LOCALITY_LABEL`` field is built by :func:`_build_locality_label`
        using site/county/state fields from the SITE_EQUIPMENT_OBS_SUMMARY.

        ``IS_CURRENT_LOCATION = 1`` when any equipment at this (host, site) has
        ``IS_CURRENT_LOCATION = 1`` in SITE_EQUIPMENT_OBS_SUMMARY.

        Write strategy: always ``replace_table_rows`` (the link table is always
        rebuilt in full before this step, so a partial scope is impractical).

        Returns:
            ``(row_count, watermark)`` where ``watermark`` is ``'rows=<n>'``.
        """
        rows = self._select(
            """
            SELECT
                link.FK_HOST,
                obs.FK_SITE,
                host.NA_HOST_NAME,
                obs.NA_SITE_NAME,
                obs.NA_SITE_LABEL,
                obs.FK_COUNTY,
                obs.FK_DISTRICT,
                obs.NA_COUNTY_NAME,
                obs.NA_DISTRICT_NAME,
                obs.ID_STATE,
                obs.NA_STATE_NAME,
                obs.NA_STATE_CODE,
                obs.VL_LATITUDE,
                obs.VL_LONGITUDE,
                obs.VL_ALTITUDE,
                obs.DT_FIRST_SEEN_AT,
                obs.DT_LAST_SEEN_AT,
                obs.NU_SPECTRUM_COUNT,
                obs.FK_EQUIPMENT,
                obs.IS_CURRENT_LOCATION,
                host.IS_OFFLINE,
                link.VL_MATCH_CONFIDENCE
            FROM SITE_EQUIPMENT_OBS_SUMMARY obs
            JOIN HOST_EQUIPMENT_LINK link
              ON link.FK_EQUIPMENT = obs.FK_EQUIPMENT
             AND link.IS_ACTIVE = 1
             AND link.IS_PRIMARY_LINK = 1
            JOIN BPDATA.HOST host
              ON host.ID_HOST = link.FK_HOST
            """
        )

        grouped: Dict[Tuple[int, int], Dict[str, Any]] = {}
        for row in rows:
            key = (int(row["FK_HOST"]), int(row["FK_SITE"]))
            current = grouped.get(key)
            if current is None:
                current = {
                    "FK_HOST": key[0],
                    "FK_SITE": key[1],
                    "NA_HOST_NAME": row.get("NA_HOST_NAME"),
                    "NA_SITE_NAME": row.get("NA_SITE_NAME"),
                    "NA_SITE_LABEL": row.get("NA_SITE_LABEL"),
                    "FK_COUNTY": row.get("FK_COUNTY"),
                    "FK_DISTRICT": row.get("FK_DISTRICT"),
                    "NA_COUNTY_NAME": row.get("NA_COUNTY_NAME"),
                    "NA_DISTRICT_NAME": row.get("NA_DISTRICT_NAME"),
                    "ID_STATE": row.get("ID_STATE"),
                    "NA_STATE_NAME": row.get("NA_STATE_NAME"),
                    "NA_STATE_CODE": row.get("NA_STATE_CODE"),
                    "VL_LATITUDE": row.get("VL_LATITUDE"),
                    "VL_LONGITUDE": row.get("VL_LONGITUDE"),
                    "VL_ALTITUDE": row.get("VL_ALTITUDE"),
                    "DT_FIRST_SEEN_AT": row.get("DT_FIRST_SEEN_AT"),
                    "DT_LAST_SEEN_AT": row.get("DT_LAST_SEEN_AT"),
                    "NU_SPECTRUM_COUNT": 0,
                    "MATCHED_EQUIPMENT_IDS": set(),
                    "IS_CURRENT_LOCATION": 0,
                    "IS_OFFLINE_SNAPSHOT": 0,
                    "VL_MAX_MATCH_CONFIDENCE": None,
                }
                grouped[key] = current

            current["NU_SPECTRUM_COUNT"] += int(row.get("NU_SPECTRUM_COUNT") or 0)
            current["MATCHED_EQUIPMENT_IDS"].add(int(row["FK_EQUIPMENT"]))
            current["IS_CURRENT_LOCATION"] = max(
                int(current["IS_CURRENT_LOCATION"]),
                int(row.get("IS_CURRENT_LOCATION") or 0),
            )
            current["IS_OFFLINE_SNAPSHOT"] = max(
                int(current["IS_OFFLINE_SNAPSHOT"]),
                int(row.get("IS_OFFLINE") or 0),
            )
            match_conf = row.get("VL_MATCH_CONFIDENCE")
            if match_conf is not None:
                current["VL_MAX_MATCH_CONFIDENCE"] = max(
                    float(match_conf),
                    float(current["VL_MAX_MATCH_CONFIDENCE"] or 0),
                )
            if row.get("DT_FIRST_SEEN_AT") and (
                current["DT_FIRST_SEEN_AT"] is None
                or row["DT_FIRST_SEEN_AT"] < current["DT_FIRST_SEEN_AT"]
            ):
                current["DT_FIRST_SEEN_AT"] = row["DT_FIRST_SEEN_AT"]
            if row.get("DT_LAST_SEEN_AT") and (
                current["DT_LAST_SEEN_AT"] is None
                or row["DT_LAST_SEEN_AT"] > current["DT_LAST_SEEN_AT"]
            ):
                current["DT_LAST_SEEN_AT"] = row["DT_LAST_SEEN_AT"]

        refreshed_at = datetime.utcnow()
        payload_rows = []
        for row in grouped.values():
            payload_rows.append(
                {
                    "FK_HOST": row["FK_HOST"],
                    "FK_SITE": row["FK_SITE"],
                    "NA_HOST_NAME": row.get("NA_HOST_NAME"),
                    "NA_LOCALITY_LABEL": _build_locality_label(row),
                    "NA_SITE_LABEL": row.get("NA_SITE_LABEL"),
                    "FK_COUNTY": row.get("FK_COUNTY"),
                    "FK_DISTRICT": row.get("FK_DISTRICT"),
                    "NA_COUNTY_NAME": row.get("NA_COUNTY_NAME"),
                    "NA_DISTRICT_NAME": row.get("NA_DISTRICT_NAME"),
                    "ID_STATE": row.get("ID_STATE"),
                    "NA_STATE_NAME": row.get("NA_STATE_NAME"),
                    "NA_STATE_CODE": row.get("NA_STATE_CODE"),
                    "VL_LATITUDE": row.get("VL_LATITUDE"),
                    "VL_LONGITUDE": row.get("VL_LONGITUDE"),
                    "VL_ALTITUDE": row.get("VL_ALTITUDE"),
                    "DT_FIRST_SEEN_AT": row.get("DT_FIRST_SEEN_AT"),
                    "DT_LAST_SEEN_AT": row.get("DT_LAST_SEEN_AT"),
                    "NU_SPECTRUM_COUNT": int(row["NU_SPECTRUM_COUNT"]),
                    "NU_MATCHED_EQUIPMENT_TOTAL": len(row["MATCHED_EQUIPMENT_IDS"]),
                    "IS_CURRENT_LOCATION": int(row["IS_CURRENT_LOCATION"]),
                    "IS_OFFLINE_SNAPSHOT": int(row["IS_OFFLINE_SNAPSHOT"]),
                    "VL_MAX_MATCH_CONFIDENCE": row.get("VL_MAX_MATCH_CONFIDENCE"),
                    "DT_REFRESHED_AT": refreshed_at,
                }
            )

        self.db.replace_table_rows("HOST_LOCATION_SUMMARY", payload_rows)
        watermark = f"rows={len(payload_rows)}"
        return len(payload_rows), watermark

    def _refresh_map_site_station_summary(self) -> Tuple[int, str]:
        """Rebuild the station-level map-marker table consumed by ``webfusion``.

        Each row represents one (FK_SITE, FK_EQUIPMENT) pair enriched with the
        matched host name, connectivity state, and observation dates.  The
        ``NA_MAP_STATE`` column (one of the five :func:`_map_state` values) is
        computed in Python from ``FK_HOST``, ``IS_OFFLINE``, and
        ``IS_CURRENT_LOCATION``.

        This table is the source for ``MAP_SITE_SUMMARY`` aggregation.

        Write strategy: always ``replace_table_rows`` (depends on
        SITE_EQUIPMENT_OBS_SUMMARY and HOST_EQUIPMENT_LINK, both rebuilt in
        full before this step).

        Returns:
            ``(row_count, watermark)`` where ``watermark`` is ``'rows=<n>'``.
        """
        rows = self._select(
            """
            SELECT
                obs.FK_SITE,
                obs.FK_EQUIPMENT,
                obs.NA_SITE_LABEL,
                obs.NA_EQUIPMENT,
                obs.IS_CURRENT_LOCATION,
                obs.DT_FIRST_SEEN_AT,
                obs.DT_LAST_SEEN_AT,
                obs.NU_SPECTRUM_COUNT,
                link.FK_HOST,
                link.NA_MATCH_TYPE,
                link.VL_MATCH_CONFIDENCE,
                host.NA_HOST_NAME,
                host.IS_OFFLINE
            FROM SITE_EQUIPMENT_OBS_SUMMARY obs
            LEFT JOIN HOST_EQUIPMENT_LINK link
              ON link.FK_EQUIPMENT = obs.FK_EQUIPMENT
             AND link.IS_ACTIVE = 1
             AND link.IS_PRIMARY_LINK = 1
            LEFT JOIN BPDATA.HOST host
              ON host.ID_HOST = link.FK_HOST
            """
        )

        refreshed_at = datetime.utcnow()
        payload_rows = []
        for row in rows:
            is_host_known = row.get("FK_HOST") is not None
            is_offline = bool(row.get("IS_OFFLINE") or 0)
            is_current = bool(row.get("IS_CURRENT_LOCATION") or 0)
            state = _map_state(is_host_known, is_offline, is_current)
            payload_rows.append(
                {
                    "FK_SITE": int(row["FK_SITE"]),
                    "FK_EQUIPMENT": int(row["FK_EQUIPMENT"]),
                    "FK_HOST": _safe_int(row.get("FK_HOST")),
                    "NA_SITE_LABEL": row.get("NA_SITE_LABEL"),
                    "NA_EQUIPMENT": row.get("NA_EQUIPMENT"),
                    "NA_HOST_NAME": row.get("NA_HOST_NAME"),
                    "IS_OFFLINE": int(row.get("IS_OFFLINE")) if row.get("IS_OFFLINE") is not None else None,
                    "IS_CURRENT_LOCATION": int(row.get("IS_CURRENT_LOCATION") or 0),
                    "NA_MAP_STATE": state,
                    "NU_STATE_PRIORITY": _map_priority(state),
                    "DT_FIRST_SEEN_AT": row.get("DT_FIRST_SEEN_AT"),
                    "DT_LAST_SEEN_AT": row.get("DT_LAST_SEEN_AT"),
                    "NU_SPECTRUM_COUNT": int(row.get("NU_SPECTRUM_COUNT") or 0),
                    "NA_MATCH_TYPE": row.get("NA_MATCH_TYPE"),
                    "VL_MATCH_CONFIDENCE": row.get("VL_MATCH_CONFIDENCE"),
                    "DT_REFRESHED_AT": refreshed_at,
                }
            )

        self.db.replace_table_rows("MAP_SITE_STATION_SUMMARY", payload_rows)
        watermark = f"rows={len(payload_rows)}"
        return len(payload_rows), watermark

    def _refresh_map_site_summary(self) -> Tuple[int, str]:
        """Rebuild the site-level map-marker table aggregated from station state.

        For each site in ``RFDATA.DIM_SPECTRUM_SITE`` (with a non-NULL
        ``GEO_POINT``), aggregates the station-level state counters from
        ``MAP_SITE_STATION_SUMMARY`` and derives a single ``NA_MARKER_STATE``
        using the lowest (best) :func:`_map_priority` among all stations at
        that site.

        Sites that have no station rows still appear in the output with all
        counters at zero and ``NA_MARKER_STATE = 'no_host'``.

        Write strategy: always ``replace_table_rows``.

        Returns:
            ``(row_count, watermark)`` where ``watermark`` is ``'rows=<n>'``.
        """
        site_rows = self._select(
            """
            SELECT
                s.ID_SITE,
                s.NA_SITE,
                s.FK_COUNTY,
                s.FK_DISTRICT,
                c.NA_COUNTY,
                d.NA_DISTRICT,
                st.ID_STATE,
                st.NA_STATE,
                st.LC_STATE,
                ST_Y(s.GEO_POINT) AS VL_LATITUDE,
                ST_X(s.GEO_POINT) AS VL_LONGITUDE,
                s.NU_ALTITUDE,
                s.NU_GNSS_MEASUREMENTS
            FROM RFDATA.DIM_SPECTRUM_SITE s
            LEFT JOIN RFDATA.DIM_SITE_COUNTY c
              ON c.ID_COUNTY = s.FK_COUNTY
            LEFT JOIN RFDATA.DIM_SITE_DISTRICT d
              ON d.ID_DISTRICT = s.FK_DISTRICT
            LEFT JOIN RFDATA.DIM_SITE_STATE st
              ON st.ID_STATE = s.FK_STATE
            WHERE s.GEO_POINT IS NOT NULL
            """
        )
        station_rows = self._select("SELECT * FROM MAP_SITE_STATION_SUMMARY")

        aggregates: Dict[int, Dict[str, Any]] = {}
        for row in station_rows:
            site_id = int(row["FK_SITE"])
            agg = aggregates.setdefault(
                site_id,
                {
                    "NU_STATION_COUNT": 0,
                    "NU_KNOWN_HOST_COUNT": 0,
                    "NU_ONLINE_CURRENT_COUNT": 0,
                    "NU_ONLINE_PREVIOUS_COUNT": 0,
                    "NU_OFFLINE_CURRENT_COUNT": 0,
                    "NU_OFFLINE_PREVIOUS_COUNT": 0,
                    "NU_NO_HOST_COUNT": 0,
                    "MIN_PRIORITY": 4,
                },
            )
            agg["NU_STATION_COUNT"] += 1
            if row.get("FK_HOST") is not None:
                agg["NU_KNOWN_HOST_COUNT"] += 1
            state = row.get("NA_MAP_STATE")
            if state == "online_current":
                agg["NU_ONLINE_CURRENT_COUNT"] += 1
            elif state == "online_previous":
                agg["NU_ONLINE_PREVIOUS_COUNT"] += 1
            elif state == "offline_current":
                agg["NU_OFFLINE_CURRENT_COUNT"] += 1
            elif state == "offline_previous":
                agg["NU_OFFLINE_PREVIOUS_COUNT"] += 1
            else:
                agg["NU_NO_HOST_COUNT"] += 1
            agg["MIN_PRIORITY"] = min(
                agg["MIN_PRIORITY"],
                int(row["NU_STATE_PRIORITY"]) if row.get("NU_STATE_PRIORITY") is not None else 4,
            )

        refreshed_at = datetime.utcnow()
        payload_rows = []
        for row in site_rows:
            site_id = int(row["ID_SITE"])
            agg = aggregates.get(
                site_id,
                {
                    "NU_STATION_COUNT": 0,
                    "NU_KNOWN_HOST_COUNT": 0,
                    "NU_ONLINE_CURRENT_COUNT": 0,
                    "NU_ONLINE_PREVIOUS_COUNT": 0,
                    "NU_OFFLINE_CURRENT_COUNT": 0,
                    "NU_OFFLINE_PREVIOUS_COUNT": 0,
                    "NU_NO_HOST_COUNT": 0,
                    "MIN_PRIORITY": 4,
                },
            )
            marker_state = {
                0: "online_current",
                1: "online_previous",
                2: "offline_current",
                3: "offline_previous",
            }.get(agg["MIN_PRIORITY"], "no_host")
            payload_rows.append(
                {
                    "FK_SITE": site_id,
                    "NA_SITE_LABEL": _coalesce_text(row.get("NA_SITE"), f"Site {site_id}") or f"Site {site_id}",
                    "FK_COUNTY": row.get("FK_COUNTY"),
                    "FK_DISTRICT": row.get("FK_DISTRICT"),
                    "NA_COUNTY_NAME": row.get("NA_COUNTY"),
                    "NA_DISTRICT_NAME": row.get("NA_DISTRICT"),
                    "ID_STATE": row.get("ID_STATE"),
                    "NA_STATE_NAME": row.get("NA_STATE"),
                    "NA_STATE_CODE": row.get("LC_STATE"),
                    "VL_LATITUDE": row.get("VL_LATITUDE"),
                    "VL_LONGITUDE": row.get("VL_LONGITUDE"),
                    "VL_ALTITUDE": row.get("NU_ALTITUDE"),
                    "NU_GNSS_MEASUREMENTS": row.get("NU_GNSS_MEASUREMENTS"),
                    "NA_MARKER_STATE": marker_state,
                    "NU_STATION_COUNT": agg["NU_STATION_COUNT"],
                    "NU_KNOWN_HOST_COUNT": agg["NU_KNOWN_HOST_COUNT"],
                    "NU_ONLINE_CURRENT_COUNT": agg["NU_ONLINE_CURRENT_COUNT"],
                    "NU_ONLINE_PREVIOUS_COUNT": agg["NU_ONLINE_PREVIOUS_COUNT"],
                    "NU_OFFLINE_CURRENT_COUNT": agg["NU_OFFLINE_CURRENT_COUNT"],
                    "NU_OFFLINE_PREVIOUS_COUNT": agg["NU_OFFLINE_PREVIOUS_COUNT"],
                    "NU_NO_HOST_COUNT": agg["NU_NO_HOST_COUNT"],
                    "HAS_ONLINE_STATION": 1 if agg["NU_ONLINE_CURRENT_COUNT"] + agg["NU_ONLINE_PREVIOUS_COUNT"] > 0 else 0,
                    "HAS_ONLINE_HOST": 1 if agg["NU_ONLINE_CURRENT_COUNT"] + agg["NU_ONLINE_PREVIOUS_COUNT"] > 0 else 0,
                    "HAS_KNOWN_HOST": 1 if agg["NU_KNOWN_HOST_COUNT"] > 0 else 0,
                    "DT_REFRESHED_AT": refreshed_at,
                }
            )

        self.db.replace_table_rows("MAP_SITE_SUMMARY", payload_rows)
        watermark = f"rows={len(payload_rows)}"
        return len(payload_rows), watermark

    def _refresh_host_monthly_metric(
        self,
        *,
        host_ids: Optional[Iterable[int]] = None,
        reference_months: Optional[Iterable[str]] = None,
    ) -> Tuple[int, str]:
        """Refresh per-host monthly file/size counters from ``FILE_TASK_HISTORY``.

        Aggregates backup and processing status counts and volumes grouped by
        (FK_HOST, DT_REFERENCE_MONTH).  Rows with ``DT_FILE_CREATED_HOST`` before
        ``1000-01-01`` (invalid sentinel dates) are silently skipped.
        Rows where the Python-side :func:`_month_start` normalisation fails
        (e.g. NULL month) are logged as warnings and skipped.

        When ``reference_months`` is provided, each month generates a
        ``(f.DT_FILE_CREATED_HOST >= start AND f.DT_FILE_CREATED_HOST < next_month_start)``
        range clause rather than an exact-match, so partial months are correctly
        included during incremental refreshes.

        Write strategy:
            - Full scope: ``replace_table_rows``.
            - Partial scope: ``_delete_with_scope`` then ``upsert_rows`` on
              the (FK_HOST, DT_REFERENCE_MONTH) unique key.

        Args:
            host_ids:         Optional FK_HOST filter.
            reference_months: Optional list of ``YYYY-MM-01`` strings to restrict
                              which monthly buckets are recomputed.

        Returns:
            ``(row_count, watermark)`` where ``watermark`` includes the count
            of rows with invalid month values that were skipped.
        """
        params: List[Any] = []
        # Both guards pre-filter rows with NULL or epoch-zero dates which can
        # produce wildly wrong month buckets when formatted with DATE_FORMAT.
        clauses = [
            "f.DT_FILE_CREATED_HOST IS NOT NULL",
            "f.DT_FILE_CREATED_HOST >= '1000-01-01 00:00:00'",
        ]
        host_clause = self._build_in_clause("f.FK_HOST", host_ids or [], params)
        if host_clause:
            clauses.append(host_clause)

        normalized_months = [month for month in (reference_months or []) if month]
        if normalized_months:
            month_subclauses = []
            for month in normalized_months:
                month_start = datetime.strptime(month, "%Y-%m-%d")
                # "day=28 + 4 days" is a portable trick to always land in the next
                # month regardless of how many days the current month has.
                next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
                month_subclauses.append("(f.DT_FILE_CREATED_HOST >= %s AND f.DT_FILE_CREATED_HOST < %s)")
                params.extend([month_start, next_month])
            # Combine month ranges with OR; the whole block is ANDed with other clauses.
            clauses.append("(" + " OR ".join(month_subclauses) + ")")


        rows = self._select(
            f"""
            SELECT
                f.FK_HOST,
                DATE_FORMAT(f.DT_FILE_CREATED_HOST, '%Y-%m-01') AS DT_REFERENCE_MONTH,
                COUNT(*) AS NU_DISCOVERED_FILES,
                ROUND(COALESCE(SUM(f.VL_FILE_SIZE_KB_HOST), 0) / 1024 / 1024, 2) AS VL_DISCOVERED_GB,
                SUM(CASE WHEN f.NU_STATUS_BACKUP = 0 THEN 1 ELSE 0 END) AS NU_BACKUP_DONE_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_BACKUP = 0 THEN f.VL_FILE_SIZE_KB_HOST ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_BACKUP_DONE_GB,
                SUM(CASE WHEN f.NU_STATUS_BACKUP = 1 THEN 1 ELSE 0 END) AS NU_BACKUP_PENDING_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_BACKUP = 1 THEN f.VL_FILE_SIZE_KB_HOST ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_BACKUP_PENDING_GB,
                SUM(CASE WHEN f.NU_STATUS_BACKUP = -1 THEN 1 ELSE 0 END) AS NU_BACKUP_ERROR_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_BACKUP = -1 THEN f.VL_FILE_SIZE_KB_HOST ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_BACKUP_ERROR_GB,
                SUM(CASE WHEN f.NU_STATUS_PROCESSING = 0 THEN 1 ELSE 0 END) AS NU_PROCESSING_DONE_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_PROCESSING = 0 THEN f.VL_FILE_SIZE_KB_HOST ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_PROCESSING_DONE_GB,
                SUM(CASE WHEN f.NU_STATUS_PROCESSING = 1 THEN 1 ELSE 0 END) AS NU_PROCESSING_PENDING_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_PROCESSING = 1 THEN f.VL_FILE_SIZE_KB_HOST ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_PROCESSING_PENDING_GB,
                SUM(CASE WHEN f.NU_STATUS_PROCESSING = -1 THEN 1 ELSE 0 END) AS NU_PROCESSING_ERROR_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_PROCESSING = -1 THEN f.VL_FILE_SIZE_KB_HOST ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_PROCESSING_ERROR_GB
            FROM BPDATA.FILE_TASK_HISTORY f
            WHERE {" AND ".join(clauses)}
            GROUP BY
                f.FK_HOST,
                DATE_FORMAT(f.DT_FILE_CREATED_HOST, '%Y-%m-01')
            """,
            params,
        )

        payload_rows = []
        invalid_rows = []
        for row in rows:
            reference_month = _month_start(row.get("DT_REFERENCE_MONTH"))
            if reference_month is None:
                invalid_rows.append(
                    {
                        "FK_HOST": row.get("FK_HOST"),
                        "DT_REFERENCE_MONTH": row.get("DT_REFERENCE_MONTH"),
                    }
                )
                continue

            payload_rows.append(
                {
                    "FK_HOST": int(row["FK_HOST"]),
                    "DT_REFERENCE_MONTH": reference_month,
                    "NU_DISCOVERED_FILES": int(row.get("NU_DISCOVERED_FILES") or 0),
                    "VL_DISCOVERED_GB": row.get("VL_DISCOVERED_GB") or 0,
                    "NU_BACKUP_DONE_FILES": int(row.get("NU_BACKUP_DONE_FILES") or 0),
                    "VL_BACKUP_DONE_GB": row.get("VL_BACKUP_DONE_GB") or 0,
                    "NU_BACKUP_PENDING_FILES": int(row.get("NU_BACKUP_PENDING_FILES") or 0),
                    "VL_BACKUP_PENDING_GB": row.get("VL_BACKUP_PENDING_GB") or 0,
                    "NU_BACKUP_ERROR_FILES": int(row.get("NU_BACKUP_ERROR_FILES") or 0),
                    "VL_BACKUP_ERROR_GB": row.get("VL_BACKUP_ERROR_GB") or 0,
                    "NU_PROCESSING_DONE_FILES": int(row.get("NU_PROCESSING_DONE_FILES") or 0),
                    "VL_PROCESSING_DONE_GB": row.get("VL_PROCESSING_DONE_GB") or 0,
                    "NU_PROCESSING_PENDING_FILES": int(row.get("NU_PROCESSING_PENDING_FILES") or 0),
                    "VL_PROCESSING_PENDING_GB": row.get("VL_PROCESSING_PENDING_GB") or 0,
                    "NU_PROCESSING_ERROR_FILES": int(row.get("NU_PROCESSING_ERROR_FILES") or 0),
                    "VL_PROCESSING_ERROR_GB": row.get("VL_PROCESSING_ERROR_GB") or 0,
                }
            )

        if invalid_rows:
            self.log.warning_event(
                "summary_host_monthly_metric_invalid_month_skipped",
                component="summary_engine",
                operation="refresh_host_monthly_metric",
                skipped=len(invalid_rows),
                examples=invalid_rows[:5],
            )

        if not host_ids and not normalized_months:
            self.db.replace_table_rows("HOST_MONTHLY_METRIC", payload_rows)
        else:
            self._delete_with_scope(
                "HOST_MONTHLY_METRIC",
                host_ids=host_ids,
                reference_months=normalized_months,
            )
            if payload_rows:
                self.db.upsert_rows(
                    table="HOST_MONTHLY_METRIC",
                    rows=payload_rows,
                    unique_keys=["FK_HOST", "DT_REFERENCE_MONTH"],
                )

        watermark = f"rows={len(payload_rows)};skipped_invalid_month={len(invalid_rows)}"
        return len(payload_rows), watermark

    def _read_error_events(self, host_ids: Optional[Iterable[int]] = None) -> List[Dict[str, Any]]:
        """Read canonical error events from four BPDATA sources with optional host scoping.

        The result set is a plain list of dicts with a uniform schema across all four
        source tables so callers can group/aggregate without table-specific branching.

        The UNION ALL has four legs:
            1. FILE_TASK_HISTORY rows where NU_STATUS_BACKUP = -1     (BACKUP errors)
            2. FILE_TASK_HISTORY rows where NU_STATUS_PROCESSING = -1 (PROCESSING errors)
            3. FILE_TASK rows where NU_STATUS = -1                    (queue-level errors)
            4. HOST_TASK rows where NU_STATUS = -1                    (host-task errors)

        PARAM ORDER — IMPORTANT:
            Legs 1 and 2 both reference the same ``{history_filter}`` f-string fragment,
            so the history host-id params must appear **twice** in the final params tuple —
            once for each leg that uses that filter.  Legs 3 and 4 use their own distinct
            filter fragments, each contributing one copy of the host-id params.

            Correct order: [hist, hist, task, host_task]  →  4 × N values for N host ids.

        Args:
            host_ids: Optional collection of FK_HOST values to restrict the query.
                      When ``None`` or empty, all hosts are included (no WHERE clause
                      fragment is injected).

        Returns:
            List of row dicts with keys: NA_SOURCE_TABLE, ID_SOURCE_ROW,
            NA_ERROR_SCOPE, FK_HOST, NA_HOST_NAME, DT_EVENT_AT,
            NA_ERROR_DOMAIN, NA_ERROR_STAGE, NA_ERROR_CODE,
            NA_ERROR_SUMMARY, NA_ERROR_DETAIL, NA_RAW_MESSAGE.
        """
        # Build filter fragments and parameter lists independently so the same
        # host-id list can be replicated correctly for each UNION ALL leg.
        host_ids_list = list(host_ids or [])
        p_hist: List[Any] = []
        p_task: List[Any] = []
        p_host_task: List[Any] = []

        host_clause_history = self._build_in_clause("f.FK_HOST", host_ids_list, p_hist)
        host_clause_task = self._build_in_clause("t.FK_HOST", host_ids_list, p_task)
        host_clause_host_task = self._build_in_clause("ht.FK_HOST", host_ids_list, p_host_task)

        history_filter = f" AND {host_clause_history}" if host_clause_history else ""
        task_filter = f" AND {host_clause_task}" if host_clause_task else ""
        host_task_filter = f" AND {host_clause_host_task}" if host_clause_host_task else ""

        # Leg 1 (BACKUP) and leg 2 (PROCESSING) both use {history_filter},
        # so p_hist must be included twice: once per leg.
        params: List[Any] = p_hist + p_hist + p_task + p_host_task

        return self._select(
            f"""
            SELECT
                'FILE_TASK_HISTORY' AS NA_SOURCE_TABLE,
                f.ID_HISTORY AS ID_SOURCE_ROW,
                'BACKUP' AS NA_ERROR_SCOPE,
                f.FK_HOST,
                h.NA_HOST_NAME,
                COALESCE(
                    f.DT_BACKUP,
                    f.DT_DISCOVERED,
                    f.DT_FILE_CREATED_HOST,
                    f.DT_FILE_MODIFIED_HOST
                ) AS DT_EVENT_AT,
                NULLIF(TRIM(f.NA_ERROR_DOMAIN), '') AS NA_ERROR_DOMAIN,
                NULLIF(TRIM(f.NA_ERROR_STAGE), '') AS NA_ERROR_STAGE,
                NULLIF(TRIM(f.NA_ERROR_CODE), '') AS NA_ERROR_CODE,
                COALESCE(NULLIF(TRIM(f.NA_ERROR_SUMMARY), ''), NULLIF(TRIM(f.NA_MESSAGE), ''), '(Sem mensagem)') AS NA_ERROR_SUMMARY,
                f.NA_ERROR_DETAIL,
                f.NA_MESSAGE AS NA_RAW_MESSAGE
            FROM BPDATA.FILE_TASK_HISTORY f
            LEFT JOIN BPDATA.HOST h ON h.ID_HOST = f.FK_HOST
            WHERE f.NU_STATUS_BACKUP = -1
            {history_filter}

            UNION ALL

            SELECT
                'FILE_TASK_HISTORY' AS NA_SOURCE_TABLE,
                f.ID_HISTORY AS ID_SOURCE_ROW,
                'PROCESSING' AS NA_ERROR_SCOPE,
                f.FK_HOST,
                h.NA_HOST_NAME,
                COALESCE(
                    f.DT_PROCESSED,
                    f.DT_BACKUP,
                    f.DT_DISCOVERED,
                    f.DT_FILE_CREATED_HOST,
                    f.DT_FILE_MODIFIED_HOST
                ) AS DT_EVENT_AT,
                NULLIF(TRIM(f.NA_ERROR_DOMAIN), '') AS NA_ERROR_DOMAIN,
                NULLIF(TRIM(f.NA_ERROR_STAGE), '') AS NA_ERROR_STAGE,
                NULLIF(TRIM(f.NA_ERROR_CODE), '') AS NA_ERROR_CODE,
                COALESCE(NULLIF(TRIM(f.NA_ERROR_SUMMARY), ''), NULLIF(TRIM(f.NA_MESSAGE), ''), '(Sem mensagem)') AS NA_ERROR_SUMMARY,
                f.NA_ERROR_DETAIL,
                f.NA_MESSAGE AS NA_RAW_MESSAGE
            FROM BPDATA.FILE_TASK_HISTORY f
            LEFT JOIN BPDATA.HOST h ON h.ID_HOST = f.FK_HOST
            WHERE f.NU_STATUS_PROCESSING = -1
            {history_filter}

            UNION ALL

            SELECT
                'FILE_TASK' AS NA_SOURCE_TABLE,
                t.ID_FILE_TASK AS ID_SOURCE_ROW,
                CASE
                    WHEN t.NU_TYPE = 1 THEN 'BACKUP_QUEUE'
                    WHEN t.NU_TYPE = 2 THEN 'PROCESSING_QUEUE'
                    ELSE 'FILE_TASK'
                END AS NA_ERROR_SCOPE,
                t.FK_HOST,
                h.NA_HOST_NAME,
                COALESCE(
                    t.DT_FILE_TASK,
                    t.DT_FILE_CREATED_HOST,
                    t.DT_FILE_MODIFIED_HOST
                ) AS DT_EVENT_AT,
                NULLIF(TRIM(t.NA_ERROR_DOMAIN), '') AS NA_ERROR_DOMAIN,
                NULLIF(TRIM(t.NA_ERROR_STAGE), '') AS NA_ERROR_STAGE,
                NULLIF(TRIM(t.NA_ERROR_CODE), '') AS NA_ERROR_CODE,
                COALESCE(NULLIF(TRIM(t.NA_ERROR_SUMMARY), ''), NULLIF(TRIM(t.NA_MESSAGE), ''), '(Sem mensagem)') AS NA_ERROR_SUMMARY,
                t.NA_ERROR_DETAIL,
                t.NA_MESSAGE AS NA_RAW_MESSAGE
            FROM BPDATA.FILE_TASK t
            LEFT JOIN BPDATA.HOST h ON h.ID_HOST = t.FK_HOST
            WHERE t.NU_STATUS = -1
            {task_filter}

            UNION ALL

            SELECT
                'HOST_TASK' AS NA_SOURCE_TABLE,
                ht.ID_HOST_TASK AS ID_SOURCE_ROW,
                'HOST_TASK' AS NA_ERROR_SCOPE,
                ht.FK_HOST,
                h.NA_HOST_NAME,
                ht.DT_HOST_TASK AS DT_EVENT_AT,
                NULL AS NA_ERROR_DOMAIN,
                NULL AS NA_ERROR_STAGE,
                NULL AS NA_ERROR_CODE,
                COALESCE(NULLIF(TRIM(ht.NA_MESSAGE), ''), '(Sem mensagem)') AS NA_ERROR_SUMMARY,
                NULL AS NA_ERROR_DETAIL,
                ht.NA_MESSAGE AS NA_RAW_MESSAGE
            FROM BPDATA.HOST_TASK ht
            LEFT JOIN BPDATA.HOST h ON h.ID_HOST = ht.FK_HOST
            WHERE ht.NU_STATUS = -1
            {host_task_filter}
            """,
            params,
        )

    def _refresh_host_error_summary(
        self,
        *,
        host_ids: Optional[Iterable[int]] = None,
    ) -> Tuple[int, str]:
        """Refresh grouped error buckets per host from canonical error events.

        Calls :meth:`_read_error_events` to union all four BPDATA error sources,
        then groups the result by
        (FK_HOST, NA_ERROR_SCOPE, NA_ERROR_DOMAIN, NA_ERROR_STAGE, NA_ERROR_CODE,
        NA_ERROR_SUMMARY_HASH). Each group tracks error count plus the
        timestamp and source-row id of the latest event. Older audit-heavy
        fields are not persisted in the public read model anymore.

        The ``NA_ERROR_SUMMARY_HASH`` key lets the table survive mesage text
        changes without creating duplicate rows (grouping is hash-stable).

        Write strategy:
            - Full scope: ``replace_table_rows``.
            - Scoped by ``host_ids``: ``_delete_with_scope`` then ``upsert_rows``
              on the multi-column unique key.

        Args:
            host_ids: Optional FK_HOST filter.  When ``None``, all hosts are
                      included and a full rebuild is performed.

        Returns:
            ``(row_count, watermark)`` where ``watermark`` is ``'rows=<n>'``.
        """
        rows = self._read_error_events(host_ids=host_ids)
        grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

        for row in rows:
            host_id = _safe_int(row.get("FK_HOST"))
            if host_id is None:
                continue
            error_summary = _coalesce_text(
                row.get("NA_ERROR_SUMMARY"),
                row.get("NA_RAW_MESSAGE"),
                "(Sem mensagem)",
            ) or "(Sem mensagem)"
            error_summary_hash = hashlib.sha256(
                error_summary.encode("utf-8")
            ).hexdigest()
            key = (
                host_id,
                row.get("NA_ERROR_SCOPE"),
                row.get("NA_ERROR_DOMAIN"),
                row.get("NA_ERROR_STAGE"),
                row.get("NA_ERROR_CODE"),
                error_summary_hash,
                error_summary,
            )
            current = grouped.get(key)
            if current is None:
                current = {
                    "FK_HOST": host_id,
                    "NA_ERROR_SCOPE": key[1],
                    "NA_ERROR_DOMAIN": key[2],
                    "NA_ERROR_STAGE": key[3],
                    "NA_ERROR_CODE": key[4],
                    "NA_ERROR_SUMMARY_HASH": key[5],
                    "NA_ERROR_SUMMARY": key[6],
                    "NU_ERROR_COUNT": 0,
                    "DT_LAST_SEEN_AT": row.get("DT_EVENT_AT"),
                    "ID_LAST_SOURCE_ROW": row.get("ID_SOURCE_ROW"),
                }
                grouped[key] = current

            current["NU_ERROR_COUNT"] += 1
            event_sort_key = (
                row.get("DT_EVENT_AT") or datetime(1970, 1, 1),
                int(row.get("ID_SOURCE_ROW") or 0),
            )
            if event_sort_key >= (
                current.get("DT_LAST_SEEN_AT") or datetime(1970, 1, 1),
                int(current.get("ID_LAST_SOURCE_ROW") or 0),
            ):
                current["DT_LAST_SEEN_AT"] = row.get("DT_EVENT_AT")
                current["ID_LAST_SOURCE_ROW"] = row.get("ID_SOURCE_ROW")

        payload_rows = [
            {
                "FK_HOST": row["FK_HOST"],
                "NA_ERROR_SCOPE": row["NA_ERROR_SCOPE"],
                "NA_ERROR_DOMAIN": row["NA_ERROR_DOMAIN"],
                "NA_ERROR_STAGE": row["NA_ERROR_STAGE"],
                "NA_ERROR_CODE": row["NA_ERROR_CODE"],
                "NA_ERROR_SUMMARY_HASH": row["NA_ERROR_SUMMARY_HASH"],
                "NA_ERROR_SUMMARY": row["NA_ERROR_SUMMARY"],
                "NU_ERROR_COUNT": int(row["NU_ERROR_COUNT"]),
                "DT_LAST_SEEN_AT": row["DT_LAST_SEEN_AT"],
                "ID_LAST_SOURCE_ROW": row["ID_LAST_SOURCE_ROW"],
            }
            for row in grouped.values()
        ]

        if not host_ids:
            self.db.replace_table_rows("HOST_ERROR_SUMMARY", payload_rows)
        else:
            self._delete_with_scope("HOST_ERROR_SUMMARY", host_ids=host_ids)
            if payload_rows:
                self.db.upsert_rows(
                    table="HOST_ERROR_SUMMARY",
                    rows=payload_rows,
                    unique_keys=[
                        "FK_HOST",
                        "NA_ERROR_SCOPE",
                        "NA_ERROR_DOMAIN",
                        "NA_ERROR_STAGE",
                        "NA_ERROR_CODE",
                        "NA_ERROR_SUMMARY_HASH",
                    ],
                )

        watermark = f"rows={len(payload_rows)}"
        return len(payload_rows), watermark

    def _refresh_server_error_summary(self) -> Tuple[int, str]:
        """Rebuild the server-wide error summary by rolling up HOST_ERROR_SUMMARY.

        All per-host error group rows from ``HOST_ERROR_SUMMARY`` are merged
        into cross-host buckets keyed on
        (NA_ERROR_SCOPE, NA_ERROR_DOMAIN, NA_ERROR_STAGE, NA_ERROR_CODE,
        NA_ERROR_SUMMARY_HASH). Error counts are summed into the exact UI
        payload consumed by the global diagnostics page.

        Note: all four BPDATA error sources (FILE_TASK_HISTORY, FILE_TASK,
        HOST_TASK) carry a non-NULL FK_HOST in every row, so there are no
        "hostless" events to handle here; ``HOST_ERROR_SUMMARY`` already
        captures the complete error picture.

        The resulting table is consumed by the server-level dashboard panels.

        Write strategy: always ``replace_table_rows``.

        Returns:
            ``(row_count, watermark)`` where ``watermark`` is ``'rows=<n>'``.
        """
        host_rows = self._select("SELECT * FROM HOST_ERROR_SUMMARY")
        grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

        for row in host_rows:
            key = (
                row.get("NA_ERROR_SCOPE"),
                row.get("NA_ERROR_DOMAIN"),
                row.get("NA_ERROR_STAGE"),
                row.get("NA_ERROR_CODE"),
                row.get("NA_ERROR_SUMMARY_HASH"),
                row.get("NA_ERROR_SUMMARY"),
            )
            current = grouped.get(key)
            if current is None:
                current = {
                    "NA_ERROR_SCOPE": key[0],
                    "NA_ERROR_DOMAIN": key[1],
                    "NA_ERROR_STAGE": key[2],
                    "NA_ERROR_CODE": key[3],
                    "NA_ERROR_SUMMARY_HASH": key[4],
                    "NA_ERROR_SUMMARY": key[5],
                    "NU_ERROR_COUNT": 0,
                }
                grouped[key] = current

            current["NU_ERROR_COUNT"] += int(row.get("NU_ERROR_COUNT") or 0)

        payload_rows = [
            {
                "NA_ERROR_SCOPE": row["NA_ERROR_SCOPE"],
                "NA_ERROR_DOMAIN": row["NA_ERROR_DOMAIN"],
                "NA_ERROR_STAGE": row["NA_ERROR_STAGE"],
                "NA_ERROR_CODE": row["NA_ERROR_CODE"],
                "NA_ERROR_SUMMARY_HASH": row["NA_ERROR_SUMMARY_HASH"],
                "NA_ERROR_SUMMARY": row["NA_ERROR_SUMMARY"],
                "NU_ERROR_COUNT": int(row["NU_ERROR_COUNT"]),
            }
            for row in grouped.values()
        ]

        self.db.replace_table_rows("SERVER_ERROR_SUMMARY", payload_rows)
        watermark = f"rows={len(payload_rows)}"
        return len(payload_rows), watermark

    def _refresh_host_current_snapshot(self) -> Tuple[int, str]:
        """Rebuild the per-host dashboard snapshot from multiple summary sources.

        For each host in ``BPDATA.HOST``, assembles a single denormalized row
        from:

        - ``BPDATA.HOST`` — connectivity flags, task counters, KB sizes.
        - ``BPDATA.FILE_TASK`` — real-time queue depth (pending only).
        - ``BPDATA.FILE_TASK_HISTORY`` — current-month backup throughput
          grouped by ``DT_BACKUP``.
        - ``HOST_MONTHLY_METRIC`` — total discovered-file count (preferred over
          the raw ``NU_HOST_FILES`` counter which may lag).
        - ``HOST_EQUIPMENT_LINK`` — matched equipment count.
        - ``SITE_EQUIPMENT_OBS_SUMMARY`` — total spectrum fact count.
        - ``HOST_LOCATION_SUMMARY`` — current site label and UF
          (most recent ``IS_CURRENT_LOCATION = 1`` row).
        - ``HOST_ERROR_SUMMARY`` — most recent error code/summary.

        BPDATA KB columns are converted to GB by :func:`_kb_to_gb` before
        storage (summary tables use GB throughout).

        Write strategy: always ``replace_table_rows`` (always full rebuild;
        this table is cheap to rebuild because it is one row per host).

        Returns:
            ``(row_count, watermark)`` where ``watermark`` is
            ``'hosts=<n>'``.
        """
        hosts = self._select("SELECT * FROM BPDATA.HOST")
        queue_rows = self._select(
            """
            SELECT
                FK_HOST,
                SUM(CASE WHEN NU_TYPE = 1 AND NU_STATUS = 1 THEN 1 ELSE 0 END) AS NU_BACKUP_QUEUE_FILES_TOTAL,
                ROUND(COALESCE(SUM(CASE WHEN NU_TYPE = 1 AND NU_STATUS = 1 THEN VL_FILE_SIZE_KB_HOST ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_BACKUP_QUEUE_GB_TOTAL,
                SUM(CASE WHEN NU_TYPE = 2 AND NU_STATUS = 1 THEN 1 ELSE 0 END) AS NU_PROCESSING_QUEUE_FILES_TOTAL,
                ROUND(COALESCE(SUM(CASE WHEN NU_TYPE = 2 AND NU_STATUS = 1 THEN VL_FILE_SIZE_KB_HOST ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_PROCESSING_QUEUE_GB_TOTAL
            FROM BPDATA.FILE_TASK
            GROUP BY FK_HOST
            """
        )
        current_month_start = datetime.utcnow().replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        if current_month_start.month == 12:
            next_month_start = current_month_start.replace(
                year=current_month_start.year + 1,
                month=1,
            )
        else:
            next_month_start = current_month_start.replace(
                month=current_month_start.month + 1,
            )
        backup_month_rows = self._select(
            """
            SELECT
                FK_HOST,
                COUNT(*) AS NU_BACKUP_DONE_THIS_MONTH,
                ROUND(COALESCE(SUM(VL_FILE_SIZE_KB_HOST), 0) / 1024 / 1024, 2) AS VL_BACKUP_DONE_GB_THIS_MONTH
            FROM BPDATA.FILE_TASK_HISTORY
            WHERE NU_STATUS_BACKUP = 0
              AND DT_BACKUP >= %s
              AND DT_BACKUP < %s
            GROUP BY FK_HOST
            """,
            (current_month_start, next_month_start),
        )
        monthly_rows = self._select(
            """
            SELECT
                FK_HOST,
                SUM(NU_DISCOVERED_FILES) AS NU_DISCOVERED_FILES_TOTAL
            FROM HOST_MONTHLY_METRIC
            GROUP BY FK_HOST
            """
        )
        link_rows = self._select(
            """
            SELECT FK_HOST, COUNT(*) AS NU_MATCHED_EQUIPMENT_TOTAL
            FROM HOST_EQUIPMENT_LINK
            WHERE IS_ACTIVE = 1
              AND IS_PRIMARY_LINK = 1
            GROUP BY FK_HOST
            """
        )
        spectrum_rows = self._select(
            """
            SELECT
                l.FK_HOST,
                SUM(obs.NU_SPECTRUM_COUNT) AS NU_FACT_SPECTRUM_TOTAL
            FROM HOST_EQUIPMENT_LINK l
            JOIN SITE_EQUIPMENT_OBS_SUMMARY obs
              ON obs.FK_EQUIPMENT = l.FK_EQUIPMENT
            WHERE l.IS_ACTIVE = 1
              AND l.IS_PRIMARY_LINK = 1
            GROUP BY l.FK_HOST
            """
        )
        current_location_rows = self._select(
            """
            SELECT
                FK_HOST,
                NA_SITE_LABEL,
                NA_STATE_CODE,
                DT_LAST_SEEN_AT,
                DT_FIRST_SEEN_AT
            FROM HOST_LOCATION_SUMMARY
            WHERE IS_CURRENT_LOCATION = 1
            ORDER BY
                FK_HOST ASC,
                COALESCE(DT_LAST_SEEN_AT, DT_FIRST_SEEN_AT) DESC,
                COALESCE(DT_FIRST_SEEN_AT, DT_LAST_SEEN_AT) DESC,
                FK_SITE DESC
            """
        )
        last_error_rows = self._select(
            """
            SELECT
                FK_HOST,
                NA_ERROR_CODE,
                NA_ERROR_SUMMARY,
                DT_LAST_SEEN_AT,
                ID_LAST_SOURCE_ROW
            FROM HOST_ERROR_SUMMARY
            ORDER BY
                FK_HOST ASC,
                COALESCE(DT_LAST_SEEN_AT, '1970-01-01 00:00:00') DESC,
                COALESCE(ID_LAST_SOURCE_ROW, 0) DESC
            """
        )

        queue_map = {int(row["FK_HOST"]): row for row in queue_rows}
        backup_month_map = {int(row["FK_HOST"]): row for row in backup_month_rows}
        monthly_map = {int(row["FK_HOST"]): row for row in monthly_rows}
        link_map = {int(row["FK_HOST"]): row for row in link_rows}
        spectrum_map = {int(row["FK_HOST"]): row for row in spectrum_rows}
        location_map: Dict[int, Dict[str, Any]] = {}
        for row in current_location_rows:
            host_id = int(row["FK_HOST"])
            location_map.setdefault(host_id, row)
        last_error_map: Dict[int, Dict[str, Any]] = {}
        for row in last_error_rows:
            host_id = int(row["FK_HOST"])
            last_error_map.setdefault(host_id, row)

        payload_rows = []
        for host in hosts:
            host_id = int(host["ID_HOST"])
            queue = queue_map.get(host_id, {})
            backup_month = backup_month_map.get(host_id, {})
            monthly = monthly_map.get(host_id, {})
            link_stats = link_map.get(host_id, {})
            spectrum_stats = spectrum_map.get(host_id, {})
            current_location = location_map.get(host_id, {})
            last_error = last_error_map.get(host_id, {})

            payload_rows.append(
                {
                    "ID_HOST": host_id,
                    "NA_HOST_NAME": host.get("NA_HOST_NAME"),
                    "NA_HOST_ADDRESS": host.get("NA_HOST_ADDRESS"),
                    "NA_HOST_PORT": host.get("NA_HOST_PORT"),
                    "IS_OFFLINE": int(host.get("IS_OFFLINE") or 0),
                    "IS_BUSY": int(host.get("IS_BUSY") or 0),
                    "NU_PID": host.get("NU_PID"),
                    "DT_BUSY": host.get("DT_BUSY"),
                    "DT_LAST_FAIL": host.get("DT_LAST_FAIL"),
                    "DT_LAST_CHECK": host.get("DT_LAST_CHECK"),
                    "NU_HOST_CHECK_ERROR": host.get("NU_HOST_CHECK_ERROR"),
                    "DT_LAST_DISCOVERY": host.get("DT_LAST_DISCOVERY"),
                    "DT_LAST_BACKUP": host.get("DT_LAST_BACKUP"),
                    "NU_PENDING_FILE_BACKUP_TASKS": host.get("NU_PENDING_FILE_BACKUP_TASKS"),
                    "NU_ERROR_FILE_BACKUP_TASKS": host.get("NU_ERROR_FILE_BACKUP_TASKS"),
                    "NU_BACKUP_DONE_THIS_MONTH": int(backup_month.get("NU_BACKUP_DONE_THIS_MONTH") or 0),
                    "VL_PENDING_BACKUP_GB": _kb_to_gb(host.get("VL_PENDING_BACKUP_KB")),
                    "VL_BACKUP_DONE_GB_THIS_MONTH": backup_month.get("VL_BACKUP_DONE_GB_THIS_MONTH") or 0,
                    "VL_DONE_BACKUP_GB": _kb_to_gb(host.get("VL_DONE_BACKUP_KB")),
                    "DT_LAST_PROCESSING": host.get("DT_LAST_PROCESSING"),
                    "NU_PENDING_FILE_PROCESS_TASKS": host.get("NU_PENDING_FILE_PROCESS_TASKS"),
                    "NU_ERROR_FILE_PROCESS_TASKS": host.get("NU_ERROR_FILE_PROCESS_TASKS"),
                    "NU_HOST_FILES": int(
                        monthly.get("NU_DISCOVERED_FILES_TOTAL")
                        or host.get("NU_HOST_FILES")
                        or 0
                    ),
                    "NU_BACKUP_QUEUE_FILES_TOTAL": int(queue.get("NU_BACKUP_QUEUE_FILES_TOTAL") or 0),
                    "VL_BACKUP_QUEUE_GB_TOTAL": queue.get("VL_BACKUP_QUEUE_GB_TOTAL") or 0,
                    "NU_PROCESSING_QUEUE_FILES_TOTAL": int(queue.get("NU_PROCESSING_QUEUE_FILES_TOTAL") or 0),
                    "VL_PROCESSING_QUEUE_GB_TOTAL": queue.get("VL_PROCESSING_QUEUE_GB_TOTAL") or 0,
                    "NU_MATCHED_EQUIPMENT_TOTAL": int(link_stats.get("NU_MATCHED_EQUIPMENT_TOTAL") or 0),
                    "NU_FACT_SPECTRUM_TOTAL": int(spectrum_stats.get("NU_FACT_SPECTRUM_TOTAL") or 0),
                    "NA_CURRENT_SITE_LABEL": current_location.get("NA_SITE_LABEL"),
                    "NA_CURRENT_STATE_CODE": current_location.get("NA_STATE_CODE"),
                    "NA_LAST_ERROR_CODE": last_error.get("NA_ERROR_CODE"),
                    "NA_LAST_ERROR_SUMMARY": last_error.get("NA_ERROR_SUMMARY"),
                    "DT_LAST_ERROR_AT": last_error.get("DT_LAST_SEEN_AT"),
                }
            )

        self.db.replace_table_rows("HOST_CURRENT_SNAPSHOT", payload_rows)
        watermark = f"hosts={len(payload_rows)}"
        return len(payload_rows), watermark

    def _refresh_server_current_summary(self) -> Tuple[int, str]:
        """Rebuild the single-row server-level dashboard summary.

        Aggregates across all rows in ``HOST_CURRENT_SNAPSHOT`` to produce
        server-wide totals (host counts, file counts, queue depths, GB volumes,
        spectrum count). Current-month backup throughput is summed from the
        per-host snapshot fields already materialized from ``DT_BACKUP``.
        Historical processing-done totals come from ``HOST_MONTHLY_METRIC``
        because the host snapshot no longer persists the redundant lifetime
        counter copied from ``BPDATA.HOST``.

        The table always contains exactly one row with
        ``ID_SUMMARY = 1``.  The month label (``NA_CURRENT_MONTH_LABEL``) uses
        UTC time so it is consistent with the worker's timezone.

        Write strategy: ``replace_table_rows`` with a single-element list.

        Returns:
            ``(1, watermark)`` always (one row written); ``watermark`` is
            ``'hosts=<n>;month=<YYYY-MM>'``.
        """
        snapshot_rows = self._select(
            """
            SELECT
                IS_OFFLINE,
                IS_BUSY,
                NU_HOST_FILES,
                NU_PENDING_FILE_BACKUP_TASKS,
                VL_PENDING_BACKUP_GB,
                NU_ERROR_FILE_BACKUP_TASKS,
                NU_PENDING_FILE_PROCESS_TASKS,
                NU_ERROR_FILE_PROCESS_TASKS,
                NU_PROCESSING_QUEUE_FILES_TOTAL,
                VL_PROCESSING_QUEUE_GB_TOTAL,
                NU_BACKUP_QUEUE_FILES_TOTAL,
                VL_BACKUP_QUEUE_GB_TOTAL,
                NU_FACT_SPECTRUM_TOTAL,
                NU_BACKUP_DONE_THIS_MONTH,
                VL_BACKUP_DONE_GB_THIS_MONTH
            FROM HOST_CURRENT_SNAPSHOT
            """
        )
        processing_totals = self._select(
            """
            SELECT
                COALESCE(SUM(NU_PROCESSING_DONE_FILES), 0) AS NU_PROCESSING_DONE_FILES_TOTAL
            FROM HOST_MONTHLY_METRIC
            """
        )
        current_month = datetime.utcnow().strftime("%Y-%m-01")
        processing_done_files_total = 0
        if processing_totals:
            processing_done_files_total = int(
                processing_totals[0].get("NU_PROCESSING_DONE_FILES_TOTAL") or 0
            )
        payload_row = {
            "ID_SUMMARY": 1,
            "NA_CURRENT_MONTH_LABEL": current_month[:7],
            "NU_TOTAL_HOSTS": len(snapshot_rows),
            "NU_ONLINE_HOSTS": sum(1 for row in snapshot_rows if int(row.get("IS_OFFLINE") or 0) == 0),
            "NU_OFFLINE_HOSTS": sum(1 for row in snapshot_rows if int(row.get("IS_OFFLINE") or 0) == 1),
            "NU_BUSY_HOSTS": sum(1 for row in snapshot_rows if int(row.get("IS_BUSY") or 0) == 1),
            "NU_DISCOVERED_FILES_TOTAL": sum(int(row.get("NU_HOST_FILES") or 0) for row in snapshot_rows),
            "NU_BACKUP_PENDING_FILES_TOTAL": sum(int(row.get("NU_PENDING_FILE_BACKUP_TASKS") or 0) for row in snapshot_rows),
            "VL_BACKUP_PENDING_GB_TOTAL": round(sum(float(row.get("VL_PENDING_BACKUP_GB") or 0) for row in snapshot_rows), 2),
            "NU_BACKUP_ERROR_FILES_TOTAL": sum(int(row.get("NU_ERROR_FILE_BACKUP_TASKS") or 0) for row in snapshot_rows),
            "NU_BACKUP_QUEUE_FILES_TOTAL": sum(int(row.get("NU_BACKUP_QUEUE_FILES_TOTAL") or 0) for row in snapshot_rows),
            "VL_BACKUP_QUEUE_GB_TOTAL": round(sum(float(row.get("VL_BACKUP_QUEUE_GB_TOTAL") or 0) for row in snapshot_rows), 2),
            "NU_PROCESSING_PENDING_FILES_TOTAL": sum(int(row.get("NU_PENDING_FILE_PROCESS_TASKS") or 0) for row in snapshot_rows),
            "NU_PROCESSING_DONE_FILES_TOTAL": processing_done_files_total,
            "NU_PROCESSING_ERROR_FILES_TOTAL": sum(int(row.get("NU_ERROR_FILE_PROCESS_TASKS") or 0) for row in snapshot_rows),
            "NU_PROCESSING_QUEUE_FILES_TOTAL": sum(int(row.get("NU_PROCESSING_QUEUE_FILES_TOTAL") or 0) for row in snapshot_rows),
            "VL_PROCESSING_QUEUE_GB_TOTAL": round(sum(float(row.get("VL_PROCESSING_QUEUE_GB_TOTAL") or 0) for row in snapshot_rows), 2),
            "NU_FACT_SPECTRUM_TOTAL": sum(int(row.get("NU_FACT_SPECTRUM_TOTAL") or 0) for row in snapshot_rows),
            "NU_BACKUP_DONE_THIS_MONTH": sum(int(row.get("NU_BACKUP_DONE_THIS_MONTH") or 0) for row in snapshot_rows),
            "VL_BACKUP_DONE_GB_THIS_MONTH": round(sum(float(row.get("VL_BACKUP_DONE_GB_THIS_MONTH") or 0) for row in snapshot_rows), 2),
        }

        self.db.replace_table_rows("SERVER_CURRENT_SUMMARY", [payload_row])
        watermark = f"hosts={len(snapshot_rows)};month={current_month[:7]}"
        return 1, watermark
