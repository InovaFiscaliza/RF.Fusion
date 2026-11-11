"""Materialize canonical PostgreSQL aggregates without duplicating read projections."""
from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
import hashlib
from typing import Any, Dict, Iterable, List, Optional, Tuple

from db import constants as db_constants
from db.dbHandlerSummary import dbHandlerSummary
from db.dbHandlerBase import DbRow
from shared import errors, logging_utils


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


def _restore_error_event_fields(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize legacy structured messages for the compact read model."""
    raw_message = row.get("NA_RAW_MESSAGE")
    classified = errors.classify_persisted_error_message(raw_message)
    normalized = dict(row)
    normalized["NA_ERROR_CODE"] = _coalesce_text(
        row.get("NA_ERROR_CODE"),
        classified.get("NA_ERROR_CODE"),
    )
    is_legacy_structured_message = any(
        token in str(raw_message or "") for token in ("[ERROR]", "[stage=", "[code=")
    )
    error_summary = _coalesce_text(
        (classified.get("NA_ERROR_SUMMARY") if is_legacy_structured_message else None),
        raw_message,
        row.get("NA_ERROR_SUMMARY"),
        "(Sem mensagem)",
    )
    normalized["NA_ERROR_SUMMARY"] = (
        errors.canonical_error_summary(
            normalized["NA_ERROR_CODE"],
            error_summary,
        )
        or "(Sem mensagem)"
    )
    normalized["NA_ERROR_DETAIL"] = _coalesce_text(
        row.get("NA_ERROR_DETAIL"),
        classified.get("NA_ERROR_DETAIL"),
    )
    return normalized


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


def _cwsm_signature(normalized_key: str) -> Optional[str]:
    """Reduce a CelPlan host or equipment name to its station signature.

    A CelPlan station is identified by its last two digits: host ``CWSM212031``
    and equipment ``cwsm21100031`` are the same station. The middle digits
    differ between the host and receiver forms and are not used.

    Args:
        normalized_key: Output of :func:`_normalize_key`. Only ``cwsm`` names
            followed by 6 to 8 digits are considered.

    Returns:
        ``cwsm`` plus the last two digits, or ``None`` for other names and for
        station ``00``.
    """
    if not normalized_key.startswith("cwsm"):
        return None

    digits = normalized_key[4:]
    if not digits.isdigit() or not 6 <= len(digits) <= 8 or digits[-2:] == "00":
        return None

    return f"cwsm{digits[-2:]}"


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
    def from_outbox_rows(cls, rows: Sequence[DbRow]) -> "DirtyScope":
        """Merge many canonical outbox rows into one consolidated refresh scope.

        Args:
            rows: List of dicts as returned by
                  :meth:`dbHandlerSummary.outbox_list_pending`.

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
                if scope_type == db_constants['summary']['scopes']['host']:
                    scope.host_ids.add(int(scope_value))
                elif scope_type == db_constants['summary']['scopes']['site']:
                    scope.site_ids.add(int(scope_value))
                elif scope_type == db_constants['summary']['scopes']['equipment']:
                    scope.equipment_ids.add(int(scope_value))
                elif scope_type == db_constants['summary']['scopes']['reference_month']:
                    normalized_month = _month_start(scope_value)
                    if normalized_month is None:
                        raise ValueError(f"invalid reference month {scope_value!r}")
                    scope.reference_months.add(normalized_month)
                elif scope_type == db_constants['summary']['scopes']['full_reconcile']:
                    if str(scope_value) != db_constants['summary']['full_reconcile_key']:
                        raise ValueError(f"invalid full_reconcile key {scope_value!r}")
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
    """Own matching, error normalization and observed-visit overlap decisions.

    Database methods own all SQL. Related aggregates publish together; consumers
    query small canonical results and authoritative source metadata.

    Attributes:
        db (dbHandlerSummary): Caller-owned PostgreSQL aggregate handler.
        log (logging_utils.log): Application logger supplied by the caller.
    """

    def __init__(self, db: dbHandlerSummary, logger: logging_utils.log) -> None:
        """Bind the engine to an explicitly configured persistence session.

        Args:
            db (dbHandlerSummary): Canonical Summary handler.
            logger (logging_utils.log): Application logger.
        Returns:
            None.
        """
        self.db = db
        self.log = logger

    def refresh_all(self, *, reason: str) -> list[str]:
        """Rebuild only the six derived aggregates, preserving usage state.

        Args:
            reason (str): Sanitized operational rebuild reason.
        Returns:
            list[str]: Canonical objects published in dependency order.
        Raises:
            Exception: Failure rolls back the self-owned publication transaction.
        """
        return self._publish(
            [
                ("HOST_EQUIPMENT_LINK", self._refresh_host_equipment_link),
                ("SITE_EQUIPMENT_METRIC", self._refresh_site_equipment),
                ("HOST_LOCATION_VISIT", self.refresh_location_timeline),
                ("HOST_MONTHLY_METRIC", self._refresh_host_monthly),
                ("HOST_ERROR_METRIC", self._refresh_host_error_summary),
                ("HOST_METRIC", self._refresh_host_metric),
            ],
            reason=reason,
        )

    def refresh_for_events(self, events: Sequence[DbRow]) -> list[str]:
        """Coalesce invalidations without refreshing unrelated global projections.

        Args:
            events (Sequence[DbRow]): Outbox rows with NA_SCOPE_TYPE and
                NA_SCOPE_VALUE str, optionally the other outbox fields.
        Returns:
            list[str]: Canonical aggregates refreshed; empty for an empty scope.
        """
        scope = DirtyScope.from_outbox_rows(events)
        if scope.full_reconcile:
            return self.refresh_all(reason="outbox_full_reconcile")
        operations: list[tuple[str, Callable[[], tuple[int, str]]]] = []
        if scope.site_ids or scope.equipment_ids:
            equipment_ids = self.db.site_equipment_list_affected(
                site_ids=sorted(scope.site_ids),
                equipment_ids=sorted(scope.equipment_ids),
            )
            old_hosts = set(
                self.db.host_visit_list_hosts(
                    host_ids=scope.host_ids,
                    site_ids=scope.site_ids,
                    equipment_ids=set(equipment_ids),
                )
            )
            operations.extend(
                [
                    ("HOST_EQUIPMENT_LINK", self._refresh_host_equipment_link),
                    (
                        "SITE_EQUIPMENT_METRIC",
                        lambda: self._refresh_site_equipment(
                            equipment_ids=equipment_ids
                        ),
                    ),
                    (
                        "HOST_LOCATION_VISIT",
                        lambda: self.refresh_location_timeline(
                            host_ids=old_hosts,
                            site_ids=scope.site_ids,
                            equipment_ids=set(equipment_ids),
                        ),
                    ),
                ]
            )
        hosts = sorted(scope.host_ids) if scope.host_ids else None
        months = (
            [date.fromisoformat(month) for month in sorted(scope.reference_months)]
            if scope.reference_months
            else None
        )
        if scope.host_ids or scope.reference_months:
            operations.append(
                (
                    "HOST_MONTHLY_METRIC",
                    lambda: self._refresh_host_monthly(
                        host_ids=hosts, reference_months=months
                    ),
                )
            )
            # File-month events also invalidate current throughput, whose time
            # axis is the phase completion date rather than file creation.
            operations.append(
                ("HOST_METRIC", lambda: self._refresh_host_metric(host_ids=hosts))
            )
        if scope.host_ids:
            operations.append(
                (
                    "HOST_ERROR_METRIC",
                    lambda: self._refresh_host_error_summary(host_ids=hosts),
                )
            )
        return (
            self._publish(operations, reason="outbox_incremental") if operations else []
        )

    def _publish(
        self,
        operations: Sequence[tuple[str, Callable[[], tuple[int, str]]]],
        *,
        reason: str,
    ) -> list[str]:
        """Publish dependent aggregates atomically without closing caller transactions.

        Args:
            operations (Sequence[tuple[str, Callable[[], tuple[int, str]]]]):
                Object names and materializers returning row count and watermark.
            reason (str): Sanitized diagnostic reason.
        Returns:
            list[str]: Successfully published object names, in execution order.
        Raises:
            Exception: Materialization/commit failure; no automatic SQL replay.
        """
        owns_transaction = not self.db.in_transaction
        if owns_transaction:
            self.db.begin_transaction()
        object_name = "summary"
        started_at = datetime.utcnow()
        try:
            for object_name, materialize in operations:
                started_at = datetime.utcnow()
                count, watermark = materialize()
                self.db.refresh_log_record(
                    object_name,
                    started_at=started_at,
                    success=True,
                    row_count=count,
                    high_watermark=watermark,
                )
        except Exception as error:
            if owns_transaction:
                self.db.rollback()
                self.db.refresh_log_record(
                    object_name,
                    started_at=started_at,
                    success=False,
                    error_message=type(error).__name__,
                )
            raise
        else:
            if owns_transaction:
                self.db.commit()
        objects = [name for name, _ in operations]
        self.log.event(
            "summary_refresh_completed",
            component="summary_engine",
            reason=reason,
            objects=objects,
        )
        return objects

    def _refresh_site_equipment(
        self, *, equipment_ids: Sequence[int] | None = None
    ) -> tuple[int, str]:
        """Replace every site of the selected equipment from a single aggregation.

        Args:
            equipment_ids (Sequence[int] | None): None rebuilds all, empty none.
        Returns:
            tuple[int, str]: Published count and diagnostic watermark.
        """
        rows = self.db.site_equipment_list_sources(equipment_ids=equipment_ids)
        count = self.db.site_equipment_replace(rows, equipment_ids=equipment_ids)
        return count, f"rows={count}"

    def _refresh_host_metric(
        self, *, host_ids: Sequence[int] | None = None
    ) -> tuple[int, str]:
        """Publish operational counts for the explicit UTC throughput month.

        Args:
            host_ids (Sequence[int] | None): None rebuilds all, empty none.
        Returns:
            tuple[int, str]: Published host count and reference month watermark.
        """
        start = datetime.utcnow().replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        end = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
        rows = self.db.host_metric_list_sources(
            current_month_start=start, next_month_start=end, host_ids=host_ids
        )
        count = self.db.host_metric_replace(rows, host_ids=host_ids)
        return count, f"hosts={count};month={start:%Y-%m}"

    def _refresh_host_monthly(
        self,
        *,
        host_ids: Sequence[int] | None = None,
        reference_months: Sequence[date] | None = None,
    ) -> tuple[int, str]:
        """Publish archive metrics on the file creation time axis.

        Args:
            host_ids (Sequence[int] | None): None rebuilds all, empty none.
            reference_months (Sequence[date] | None): First days of file creation
                months; None rebuilds all, empty none.
        Returns:
            tuple[int, str]: Published monthly bucket count and watermark.
        """
        rows = self.db.host_monthly_list_sources(
            host_ids=host_ids, reference_months=reference_months
        )
        count = self.db.host_monthly_replace(
            rows, host_ids=host_ids, reference_months=reference_months
        )
        return count, f"rows={count}"

    def _refresh_host_equipment_link(self) -> tuple[int, str]:
        """Rebuild automatic host/equipment links, preserving ambiguous matches.

        Args:
            None.
        Returns:
            tuple[int, str]: Published candidate count and source cardinality watermark.
        """
        sources = self.db.host_equipment_list_sources()
        hosts = [
            {"FK_HOST": row["ID_HOST"], "NA_HOST_NAME": row["NA_HOST_NAME"]}
            for row in sources["hosts"]
            if str(row.get("NA_HOST_NAME") or "").strip()
        ]
        equipments = [
            {"FK_EQUIPMENT": row["ID_EQUIPMENT"], "NA_EQUIPMENT": row["NA_EQUIPMENT"]}
            for row in sources["equipment"]
            if str(row.get("NA_EQUIPMENT") or "").strip()
        ]
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
        candidates: Dict[Tuple[int, int], Dict[str, Any]] = {}

        def _consider(
            host_row: Dict[str, Any],
            equipment_row: Dict[str, Any],
            match_type: str,
            confidence: float,
        ) -> None:
            """Record the best reconciliation candidate for a (host, equipment) pair.

            Keep the highest confidence and match type in the enclosing candidates.

            Args:
                host_row (dict[str, Any]): Host identity, name, normalized name
                    and nullable CWSM signature from host_map.
                equipment_row (dict[str, Any]): Equipment identity, name,
                    normalized name and nullable CWSM signature from equipment_map.
                match_type (str): Automatic matching rule applied to this pair.
                confidence (float): Rule confidence used to choose the primary link.
            Returns:
                None.
            """
            key = (host_row["FK_HOST"], equipment_row["FK_EQUIPMENT"])
            candidate = {
                "FK_HOST": host_row["FK_HOST"],
                "FK_EQUIPMENT": equipment_row["FK_EQUIPMENT"],
                "NA_HOST_NAME": host_row["NA_HOST_NAME"],
                "NA_EQUIPMENT": equipment_row["NA_EQUIPMENT"],
                "NA_HOST_NAME_NORMALIZED": host_row["NA_HOST_NAME_NORMALIZED"],
                "NA_EQUIPMENT_NAME_NORMALIZED": equipment_row[
                    "NA_EQUIPMENT_NAME_NORMALIZED"
                ],
                "NA_HOST_SIGNATURE": host_row["NA_HOST_SIGNATURE"],
                "NA_EQUIPMENT_SIGNATURE": equipment_row["NA_EQUIPMENT_SIGNATURE"],
                "NA_MATCH_TYPE": match_type,
                "VL_MATCH_CONFIDENCE": confidence,
            }
            current = candidates.get(key)
            if current is None or (
                candidate["VL_MATCH_CONFIDENCE"],
                candidate["NA_MATCH_TYPE"],
            ) > (
                current["VL_MATCH_CONFIDENCE"],
                current["NA_MATCH_TYPE"],
            ):
                candidates[key] = candidate

        equipment_rows = list(equipment_map.values())
        for host_row in host_map.values():
            host_normalized = host_row["NA_HOST_NAME_NORMALIZED"]
            host_signature = host_row["NA_HOST_SIGNATURE"]
            for equipment_row in equipment_rows:
                equipment_normalized = equipment_row["NA_EQUIPMENT_NAME_NORMALIZED"]
                equipment_signature = equipment_row["NA_EQUIPMENT_SIGNATURE"]
                if host_normalized and host_normalized == equipment_normalized:
                    _consider(host_row, equipment_row, "exact_normalized", 1.0)
                elif (
                    host_signature
                    and equipment_signature
                    and (host_signature == equipment_signature)
                ):
                    _consider(host_row, equipment_row, "cwsm_signature", 0.95)
                elif (
                    len(host_normalized) >= 6
                    and len(equipment_normalized) >= 6
                    and (
                        host_normalized.startswith(equipment_normalized)
                        or equipment_normalized.startswith(host_normalized)
                    )
                ):
                    _consider(host_row, equipment_row, "prefix_match", 0.6)
        grouped_by_equipment: Dict[int, List[Dict[str, Any]]] = {}
        for candidate in candidates.values():
            grouped_by_equipment.setdefault(candidate["FK_EQUIPMENT"], []).append(
                candidate
            )
        payload_rows: List[Dict[str, Any]] = []
        match_type_rank = {
            "exact_normalized": 1,
            "cwsm_signature": 2,
            "prefix_match": 3,
        }
        for equipment_id, equipment_candidates in grouped_by_equipment.items():
            equipment_candidates.sort(
                key=lambda item: (
                    -float(item["VL_MATCH_CONFIDENCE"]),
                    match_type_rank.get(item["NA_MATCH_TYPE"], 99),
                    int(item["FK_HOST"]),
                )
            )
            top = equipment_candidates[0]
            top_tie_count = sum(
                (
                    1
                    for item in equipment_candidates
                    if float(item["VL_MATCH_CONFIDENCE"])
                    == float(top["VL_MATCH_CONFIDENCE"])
                )
            )
            for index, candidate in enumerate(equipment_candidates):
                payload_rows.append(
                    {
                        "FK_HOST": candidate["FK_HOST"],
                        "FK_EQUIPMENT": candidate["FK_EQUIPMENT"],
                        "NA_MATCH_TYPE": candidate["NA_MATCH_TYPE"],
                        "VL_MATCH_CONFIDENCE": candidate["VL_MATCH_CONFIDENCE"],
                        "IS_PRIMARY_LINK": 1
                        if index == 0 and top_tie_count == 1
                        else 0,
                    }
                )
        self.db.host_equipment_replace(payload_rows)
        watermark = f"hosts={len(hosts)};equipments={len(equipments)}"
        return (len(payload_rows), watermark)

    def refresh_location_timeline(
        self,
        *,
        host_ids: set[int] | None = None,
        site_ids: set[int] | None = None,
        equipment_ids: set[int] | None = None,
    ) -> tuple[int, str]:
        """Publish observed visits, preserving returns and conflicting intervals.

        Args:
            host_ids: Affected host IDs (set[int]) or None.
            site_ids: Affected site IDs (set[int]) or None.
            equipment_ids: Affected equipment IDs (set[int]) or None.
                All three None rebuild every linked or previously published host.

        Returns:
            tuple[int, str]: Published visit count and compact refresh watermark.

        Raises:
            Exception: Read or publish failure; the failing host stays unchanged.
        """
        hosts = self.db.host_visit_list_hosts(
            host_ids=host_ids, site_ids=site_ids, equipment_ids=equipment_ids
        )
        row_count = 0
        for host_id in hosts:
            visits = self.db.host_visit_list_sources(host_id)
            active_visits = []
            refreshed_at = datetime.utcnow()
            for visit in visits:
                visit["IS_OVERLAPPING"] = 0
                visit["DT_REFRESHED_AT"] = refreshed_at
                active_visits = [
                    previous
                    for previous in active_visits
                    if previous["DT_LAST_SEEN_AT"] >= visit["DT_FIRST_SEEN_AT"]
                ]
                for previous in active_visits:
                    if previous["FK_SITE"] != visit["FK_SITE"]:
                        previous["IS_OVERLAPPING"] = 1
                        visit["IS_OVERLAPPING"] = 1
                active_visits.append(visit)
            row_count += self.db.host_visit_replace(host_id, visits)
        return (row_count, f"hosts={len(hosts)} visits={row_count}")

    def _refresh_host_error_summary(
        self, *, host_ids: Sequence[int] | None = None
    ) -> tuple[int, str]:
        """Normalize source errors and group current held states at host granularity.

        Args:
            host_ids (Sequence[int] | None): None rebuilds all hosts, empty none.
        Returns:
            tuple[int, str]: Published error bucket count and diagnostic watermark.
        """
        rows = [
            _restore_error_event_fields(row)
            for row in self.db.host_error_list_sources(host_ids=host_ids)
        ]
        grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for row in rows:
            host_id = _safe_int(row.get("FK_HOST"))
            if host_id is None:
                continue
            raw_error_summary = (
                _coalesce_text(
                    row.get("NA_ERROR_SUMMARY"),
                    row.get("NA_RAW_MESSAGE"),
                    "(Sem mensagem)",
                )
                or "(Sem mensagem)"
            )
            error_summary = (
                errors.canonical_error_summary(
                    row.get("NA_ERROR_CODE"), raw_error_summary
                )
                or "(Sem mensagem)"
            )
            error_summary_hash = hashlib.sha256(
                error_summary.encode("utf-8")
            ).hexdigest()
            key = (
                host_id,
                row.get("NA_ERROR_SCOPE"),
                row.get("NA_TASK_STATE"),
                row.get("NA_ERROR_CODE"),
                error_summary_hash,
                error_summary,
            )
            current = grouped.get(key)
            if current is None:
                current = {
                    "FK_HOST": host_id,
                    "NA_ERROR_SCOPE": key[1],
                    "NA_TASK_STATE": key[2],
                    "NA_ERROR_CODE": key[3],
                    "NA_ERROR_SUMMARY_HASH": key[4],
                    "NA_ERROR_SUMMARY": key[5],
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
                "NA_TASK_STATE": row["NA_TASK_STATE"],
                "NA_ERROR_CODE": row["NA_ERROR_CODE"],
                "NA_ERROR_SUMMARY_HASH": row["NA_ERROR_SUMMARY_HASH"],
                "NA_ERROR_SUMMARY": row["NA_ERROR_SUMMARY"],
                "NU_ERROR_COUNT": int(row["NU_ERROR_COUNT"]),
                "DT_LAST_SEEN_AT": row["DT_LAST_SEEN_AT"],
                "ID_LAST_SOURCE_ROW": row["ID_LAST_SOURCE_ROW"],
            }
            for row in grouped.values()
        ]
        self.db.host_error_replace(payload_rows, host_ids=host_ids)
        watermark = f"rows={len(payload_rows)}"
        return (len(payload_rows), watermark)
