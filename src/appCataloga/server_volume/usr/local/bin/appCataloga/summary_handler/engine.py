#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Incremental refresh engine for the public RFFUSION_SUMMARY tables.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def _safe_int(value: Any) -> Optional[int]:
    """Normalize numeric DB values into `int` while preserving NULL semantics."""
    if value is None:
        return None
    return int(value)


def _safe_float(value: Any) -> Optional[float]:
    """Normalize numeric DB values into `float` while preserving NULL semantics."""
    if value is None:
        return None
    return float(value)


def _coalesce_text(*values: Any) -> Optional[str]:
    """Return the first non-empty textual representation from the candidates."""
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _error_summary_text(raw_summary: Any, raw_message: Any) -> str:
    """Pick the stable error text used for summary grouping and dashboards."""
    return _coalesce_text(raw_summary, raw_message, "(Sem mensagem)") or "(Sem mensagem)"


def _summary_hash(summary: str) -> str:
    """Generate the stable grouping hash stored by error summary tables."""
    return hashlib.sha256(summary.encode("utf-8")).hexdigest()


def _kb_to_gb(value_kb: Any) -> float:
    """Convert KB counters from operational tables into rounded GB values."""
    return round(float(value_kb or 0) / 1024 / 1024, 2)


def _month_start(value: Any) -> Optional[str]:
    """Normalize date-like values into the monthly bucket `YYYY-MM-01`."""
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
    """Strip punctuation noise so host and equipment aliases compare cleanly."""
    text = str(value or "").strip().lower()
    for char in "-_. /:;,()[]":
        text = text.replace(char, "")
    return text


def _cwsm_signature(normalized_key: str) -> Optional[str]:
    """Collapse known CWSM naming variants into one reconciliation signature."""
    if not normalized_key or not normalized_key.startswith("cwsm"):
        return None

    digits = normalized_key[4:]
    if not digits or not digits.isdigit() or len(digits) < 6:
        return None

    if digits == "22010007":
        return "cwsm211007"

    if len(digits) >= 8:
        prefix = digits[:4]
        if prefix == "2110":
            return f"cwsm211{digits[-3:]}"
        if prefix == "2112":
            return f"cwsm212{digits[-3:]}"
        if prefix == "2201":
            return f"cwsm220{digits[-3:]}"

    return f"cwsm{digits[:3]}{digits[-3:]}"


def _map_state(is_host_known: bool, is_offline: bool, is_current_location: bool) -> str:
    """Classify one site marker using host presence, connectivity, and recency."""
    if not is_host_known:
        return "no_host"
    if is_offline:
        return "offline_current" if is_current_location else "offline_previous"
    return "online_current" if is_current_location else "online_previous"


def _map_priority(state: str) -> int:
    """Define deterministic precedence when multiple marker states compete."""
    return {
        "online_current": 0,
        "online_previous": 1,
        "offline_current": 2,
        "offline_previous": 3,
    }.get(state, 4)


def _build_locality_label(row: Dict[str, Any]) -> str:
    """Build the compact locality label consumed by map-facing summary tables."""
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


def _sort_key_for_event_time(row: Dict[str, Any]) -> Tuple[datetime, int]:
    """Rank canonical error events newest-first with a source-row tiebreaker."""
    event_at = row.get("DT_EVENT_AT") or datetime(1970, 1, 1)
    source_row = int(row.get("ID_SOURCE_ROW") or 0)
    return event_at, source_row


class DirtyScope:
    """Coalesced invalidation scope extracted from one outbox batch."""

    def __init__(self) -> None:
        """Initialize empty buckets for incremental summary refresh."""
        self.host_ids: set[int] = set()
        self.site_ids: set[int] = set()
        self.equipment_ids: set[int] = set()
        self.reference_months: set[str] = set()
        self.full_reconcile: bool = False

    @classmethod
    def from_events(cls, events: Sequence[Dict[str, Any]]) -> "DirtyScope":
        """Merge many append-only outbox rows into one refresh scope."""
        scope = cls()
        for event in events:
            payload = event.get("JS_PAYLOAD") or {}
            scope.host_ids.update(
                int(value) for value in payload.get("host_ids", []) if value is not None
            )
            scope.site_ids.update(
                int(value) for value in payload.get("site_ids", []) if value is not None
            )
            scope.equipment_ids.update(
                int(value) for value in payload.get("equipment_ids", []) if value is not None
            )
            scope.reference_months.update(
                month
                for month in (
                    _month_start(value) for value in payload.get("reference_months", [])
                )
                if month is not None
            )
            scope.full_reconcile = scope.full_reconcile or bool(
                payload.get("full_reconcile")
            )
        return scope


class SummaryRefreshEngine:
    """Owns the safe Python refresh path for public summary tables."""

    def __init__(self, db, logger) -> None:
        """Bind the summary DB handler and application logger."""
        self.db = db
        self.log = logger

    def refresh_all(self, *, reason: str) -> List[str]:
        """Rebuild every public summary object in dependency order."""
        refreshed: List[str] = []
        # Keep the sequence explicit because later tables depend on earlier ones.
        refreshed.append(self._run_refresh("SITE_EQUIPMENT_OBS_SUMMARY", self._refresh_site_equipment_obs_summary))
        refreshed.append(self._run_refresh("HOST_EQUIPMENT_LINK", self._refresh_host_equipment_link))
        refreshed.append(self._run_refresh("HOST_LOCATION_SUMMARY", self._refresh_host_location_summary))
        refreshed.append(self._run_refresh("MAP_SITE_STATION_SUMMARY", self._refresh_map_site_station_summary))
        refreshed.append(self._run_refresh("MAP_SITE_SUMMARY", self._refresh_map_site_summary))
        refreshed.append(self._run_refresh("HOST_MONTHLY_METRIC", self._refresh_host_monthly_metric))
        refreshed.append(self._run_refresh("HOST_ERROR_SUMMARY", self._refresh_host_error_summary))
        refreshed.append(self._run_refresh("SERVER_ERROR_SUMMARY", self._refresh_server_error_summary))
        refreshed.append(self._run_refresh("HOST_CURRENT_SNAPSHOT", self._refresh_host_current_snapshot))
        refreshed.append(self._run_refresh("SERVER_CURRENT_SUMMARY", self._refresh_server_current_summary))
        self.log.event(
            "summary_full_reconcile_completed",
            reason=reason,
            objects=refreshed,
        )
        return refreshed

    def refresh_for_events(self, events: Sequence[Dict[str, Any]]) -> List[str]:
        """Coalesce one outbox batch and refresh only the affected scopes."""
        scope = DirtyScope.from_events(events)
        if scope.full_reconcile:
            return self.refresh_all(reason="outbox_full_reconcile")

        refreshed: List[str] = []

        if scope.site_ids or scope.equipment_ids:
            # Site/equipment changes fan out into link and map read models first.
            refreshed.append(
                self._run_refresh(
                    "SITE_EQUIPMENT_OBS_SUMMARY",
                    lambda: self._refresh_site_equipment_obs_summary(
                        site_ids=scope.site_ids,
                        equipment_ids=scope.equipment_ids,
                    ),
                )
            )
            refreshed.append(self._run_refresh("HOST_EQUIPMENT_LINK", self._refresh_host_equipment_link))
            refreshed.append(self._run_refresh("HOST_LOCATION_SUMMARY", self._refresh_host_location_summary))
            refreshed.append(self._run_refresh("MAP_SITE_STATION_SUMMARY", self._refresh_map_site_station_summary))
            refreshed.append(self._run_refresh("MAP_SITE_SUMMARY", self._refresh_map_site_summary))

        if scope.host_ids or scope.reference_months:
            # Host/month scopes drive operational metrics and grouped errors.
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
            refreshed.append(self._run_refresh("HOST_CURRENT_SNAPSHOT", self._refresh_host_current_snapshot))
            refreshed.append(self._run_refresh("SERVER_CURRENT_SUMMARY", self._refresh_server_current_summary))

        refreshed = [name for name in refreshed if name]
        if refreshed:
            self.log.event(
                "summary_incremental_refresh_completed",
                refreshed=refreshed,
                host_ids=sorted(scope.host_ids),
                site_ids=sorted(scope.site_ids),
                equipment_ids=sorted(scope.equipment_ids),
                reference_months=sorted(scope.reference_months),
                events=len(events),
            )
        return refreshed

    def _run_refresh(self, object_name: str, refresh_fn) -> str:
        """Wrap one refresh step with state-table and log bookkeeping."""
        started_at = self.db.summary_refresh_start(object_name)
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
        """Execute one read-only query through the shared summary handler."""
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
        """Build one parameterized `IN` clause and extend the parameter list."""
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
        """Delete only the summary rows covered by the current dirty scope."""
        clauses: List[str] = []
        params: List[Any] = []

        site_clause = self._build_in_clause("FK_SITE", site_ids or [], params)
        equipment_clause = self._build_in_clause("FK_EQUIPMENT", equipment_ids or [], params)
        host_clause = self._build_in_clause("FK_HOST", host_ids or [], params)
        month_clause = self._build_in_clause("DT_REFERENCE_MONTH", reference_months or [], params)

        for clause in (site_clause, equipment_clause, host_clause):
            if clause:
                clauses.append(clause)

        if month_clause and host_clause:
            clauses = [f"({host_clause} AND {month_clause})"]
            if site_clause or equipment_clause:
                for extra_clause in (site_clause, equipment_clause):
                    if extra_clause:
                        clauses.append(extra_clause)
        elif month_clause:
            clauses.append(month_clause)

        if not clauses:
            return 0

        # Each read model owns its own primary key shape, so scope deletion is SQL-based.
        sql = f"DELETE FROM {table} WHERE " + " OR ".join(f"({clause})" for clause in clauses)
        return self.db.execute_delete(sql, params)

    def _refresh_site_equipment_obs_summary(
        self,
        *,
        site_ids: Optional[Iterable[int]] = None,
        equipment_ids: Optional[Iterable[int]] = None,
    ) -> Tuple[int, str]:
        """Refresh the site/equipment observation summary read model."""
        params: List[Any] = []
        clauses: List[str] = []
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
            params,
        )

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
                site_ids=site_ids,
                equipment_ids=equipment_ids,
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
        """Refresh the host-to-equipment reconciliation table."""
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

        def _consider(host_row: Dict[str, Any], equipment_row: Dict[str, Any], match_type: str, confidence: float, is_manual: int) -> None:
            """Keep only the strongest reconciliation candidate per host/equipment pair."""
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

        grouped_by_equipment: Dict[int, List[Dict[str, Any]]] = {}
        for candidate in candidates.values():
            grouped_by_equipment.setdefault(candidate["FK_EQUIPMENT"], []).append(candidate)

        refresh_time = datetime.utcnow()
        payload_rows: List[Dict[str, Any]] = []
        match_type_rank = {
            "manual_override": 0,
            "exact_normalized": 1,
            "cwsm_signature": 2,
            "prefix_match": 3,
        }

        for equipment_id, equipment_candidates in grouped_by_equipment.items():
            equipment_candidates.sort(
                key=lambda item: (
                    -float(item["VL_MATCH_CONFIDENCE"]),
                    -int(item["IS_MANUAL_OVERRIDE"]),
                    match_type_rank.get(item["NA_MATCH_TYPE"], 99),
                    int(item["FK_HOST"]),
                )
            )

            top = equipment_candidates[0]
            top_tie_count = sum(
                1
                for item in equipment_candidates
                if float(item["VL_MATCH_CONFIDENCE"]) == float(top["VL_MATCH_CONFIDENCE"])
                and int(item["IS_MANUAL_OVERRIDE"]) == int(top["IS_MANUAL_OVERRIDE"])
            )

            for index, candidate in enumerate(equipment_candidates):
                payload_rows.append(
                    {
                        **candidate,
                        "IS_PRIMARY_LINK": 1 if index == 0 and top_tie_count == 1 else 0,
                        "IS_ACTIVE": 1,
                        "DT_REFRESHED_AT": refresh_time,
                    }
                )

        self.db.replace_table_rows("HOST_EQUIPMENT_LINK", payload_rows)
        watermark = f"hosts={len(hosts)};equipments={len(equipments)}"
        return len(payload_rows), watermark

    def _refresh_host_location_summary(self) -> Tuple[int, str]:
        """Refresh the host locality rollup used by dashboards and maps."""
        rows = self._select(
            """
            SELECT
                link.FK_HOST,
                obs.FK_SITE,
                host.NA_HOST_NAME,
                obs.NA_SITE_NAME,
                obs.NA_SITE_LABEL,
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
        """Refresh the station-level map markers exposed to `webfusion`."""
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
        """Refresh the site-level map markers aggregated from station state."""
        site_rows = self._select(
            """
            SELECT
                s.ID_SITE,
                s.NA_SITE,
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
                int(row.get("NU_STATE_PRIORITY") or 4),
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
        """Refresh monthly host counters derived from `FILE_TASK_HISTORY`."""
        params: List[Any] = []
        clauses = [
            "f.DT_FILE_CREATED IS NOT NULL",
            "f.DT_FILE_CREATED >= '1000-01-01 00:00:00'",
        ]
        host_clause = self._build_in_clause("f.FK_HOST", host_ids or [], params)
        if host_clause:
            clauses.append(host_clause)

        normalized_months = [month for month in (reference_months or []) if month]
        if normalized_months:
            month_subclauses = []
            for month in normalized_months:
                month_start = datetime.strptime(month, "%Y-%m-%d")
                next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
                month_subclauses.append("(f.DT_FILE_CREATED >= %s AND f.DT_FILE_CREATED < %s)")
                params.extend([month_start, next_month])
            clauses.append("(" + " OR ".join(month_subclauses) + ")")

        rows = self._select(
            f"""
            SELECT
                f.FK_HOST,
                DATE_FORMAT(f.DT_FILE_CREATED, '%Y-%m-01') AS DT_REFERENCE_MONTH,
                h.NA_HOST_NAME,
                COUNT(*) AS NU_DISCOVERED_FILES,
                ROUND(COALESCE(SUM(f.VL_FILE_SIZE_KB), 0) / 1024 / 1024, 2) AS VL_DISCOVERED_GB,
                SUM(CASE WHEN f.NU_STATUS_BACKUP = 0 THEN 1 ELSE 0 END) AS NU_BACKUP_DONE_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_BACKUP = 0 THEN f.VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_BACKUP_DONE_GB,
                SUM(CASE WHEN f.NU_STATUS_BACKUP = 1 THEN 1 ELSE 0 END) AS NU_BACKUP_PENDING_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_BACKUP = 1 THEN f.VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_BACKUP_PENDING_GB,
                SUM(CASE WHEN f.NU_STATUS_BACKUP = -1 THEN 1 ELSE 0 END) AS NU_BACKUP_ERROR_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_BACKUP = -1 THEN f.VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_BACKUP_ERROR_GB,
                SUM(CASE WHEN f.NU_STATUS_PROCESSING = 0 THEN 1 ELSE 0 END) AS NU_PROCESSING_DONE_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_PROCESSING = 0 THEN f.VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_PROCESSING_DONE_GB,
                SUM(CASE WHEN f.NU_STATUS_PROCESSING = 1 THEN 1 ELSE 0 END) AS NU_PROCESSING_PENDING_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_PROCESSING = 1 THEN f.VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_PROCESSING_PENDING_GB,
                SUM(CASE WHEN f.NU_STATUS_PROCESSING = -1 THEN 1 ELSE 0 END) AS NU_PROCESSING_ERROR_FILES,
                ROUND(COALESCE(SUM(CASE WHEN f.NU_STATUS_PROCESSING = -1 THEN f.VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_PROCESSING_ERROR_GB
            FROM BPDATA.FILE_TASK_HISTORY f
            JOIN BPDATA.HOST h
              ON h.ID_HOST = f.FK_HOST
            WHERE {" AND ".join(clauses)}
            GROUP BY
                f.FK_HOST,
                DATE_FORMAT(f.DT_FILE_CREATED, '%Y-%m-01'),
                h.NA_HOST_NAME
            """,
            params,
        )

        refreshed_at = datetime.utcnow()
        payload_rows = []
        invalid_rows = []
        for row in rows:
            reference_month = _month_start(row.get("DT_REFERENCE_MONTH"))
            if reference_month is None:
                invalid_rows.append(
                    {
                        "FK_HOST": row.get("FK_HOST"),
                        "NA_HOST_NAME": row.get("NA_HOST_NAME"),
                        "DT_REFERENCE_MONTH": row.get("DT_REFERENCE_MONTH"),
                    }
                )
                continue

            payload_rows.append(
                {
                    "FK_HOST": int(row["FK_HOST"]),
                    "DT_REFERENCE_MONTH": reference_month,
                    "NA_HOST_NAME": row.get("NA_HOST_NAME"),
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
                    "DT_REFRESHED_AT": refreshed_at,
                }
            )

        if invalid_rows:
            self.log.warning_event(
                "summary_host_monthly_metric_invalid_month_skipped",
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
        """Read canonical error events with optional host-level scoping."""
        params: List[Any] = []
        host_clause_history = self._build_in_clause("f.FK_HOST", host_ids or [], params)
        host_clause_task = self._build_in_clause("t.FK_HOST", host_ids or [], params)
        host_clause_host_task = self._build_in_clause("ht.FK_HOST", host_ids or [], params)
        history_filter = f" AND {host_clause_history}" if host_clause_history else ""
        task_filter = f" AND {host_clause_task}" if host_clause_task else ""
        host_task_filter = f" AND {host_clause_host_task}" if host_clause_host_task else ""

        return self._select(
            f"""
            SELECT
                'FILE_TASK_HISTORY' AS NA_SOURCE_TABLE,
                f.ID_HISTORY AS ID_SOURCE_ROW,
                'BACKUP' AS NA_ERROR_SCOPE,
                f.FK_HOST,
                h.NA_HOST_NAME,
                COALESCE(f.DT_BACKUP, f.DT_DISCOVERED, f.DT_FILE_CREATED, f.DT_FILE_MODIFIED) AS DT_EVENT_AT,
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
                COALESCE(f.DT_PROCESSED, f.DT_BACKUP, f.DT_DISCOVERED, f.DT_FILE_CREATED, f.DT_FILE_MODIFIED) AS DT_EVENT_AT,
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
                COALESCE(t.DT_FILE_TASK, t.DT_FILE_CREATED, t.DT_FILE_MODIFIED) AS DT_EVENT_AT,
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
        """Refresh grouped host error buckets from canonical error events."""
        rows = self._read_error_events(host_ids=host_ids)
        grouped: Dict[Tuple[Any, ...], Dict[str, Any]] = {}

        for row in rows:
            host_id = _safe_int(row.get("FK_HOST"))
            if host_id is None:
                continue
            error_summary = _error_summary_text(
                row.get("NA_ERROR_SUMMARY"),
                row.get("NA_RAW_MESSAGE"),
            )
            key = (
                host_id,
                _coalesce_text(row.get("NA_HOST_NAME"), f"Host {host_id}") or f"Host {host_id}",
                row.get("NA_ERROR_SCOPE"),
                row.get("NA_ERROR_DOMAIN"),
                row.get("NA_ERROR_STAGE"),
                row.get("NA_ERROR_CODE"),
                _summary_hash(error_summary),
                error_summary,
            )
            current = grouped.get(key)
            if current is None:
                current = {
                    "FK_HOST": host_id,
                    "NA_HOST_NAME": key[1],
                    "NA_ERROR_SCOPE": key[2],
                    "NA_ERROR_DOMAIN": key[3],
                    "NA_ERROR_STAGE": key[4],
                    "NA_ERROR_CODE": key[5],
                    "NA_ERROR_SUMMARY_HASH": key[6],
                    "NA_ERROR_SUMMARY": key[7],
                    "NU_ERROR_COUNT": 0,
                    "DT_FIRST_SEEN_AT": row.get("DT_EVENT_AT"),
                    "DT_LAST_SEEN_AT": row.get("DT_EVENT_AT"),
                    "NA_LAST_SOURCE_TABLE": row.get("NA_SOURCE_TABLE"),
                    "ID_LAST_SOURCE_ROW": row.get("ID_SOURCE_ROW"),
                    "NA_LAST_ERROR_DETAIL": row.get("NA_ERROR_DETAIL"),
                    "NA_LAST_RAW_MESSAGE": row.get("NA_RAW_MESSAGE"),
                }
                grouped[key] = current

            current["NU_ERROR_COUNT"] += 1
            event_at = row.get("DT_EVENT_AT")
            if event_at and (
                current["DT_FIRST_SEEN_AT"] is None or event_at < current["DT_FIRST_SEEN_AT"]
            ):
                current["DT_FIRST_SEEN_AT"] = event_at
            if _sort_key_for_event_time(row) >= (
                current.get("DT_LAST_SEEN_AT") or datetime(1970, 1, 1),
                int(current.get("ID_LAST_SOURCE_ROW") or 0),
            ):
                current["DT_LAST_SEEN_AT"] = event_at
                current["NA_LAST_SOURCE_TABLE"] = row.get("NA_SOURCE_TABLE")
                current["ID_LAST_SOURCE_ROW"] = row.get("ID_SOURCE_ROW")
                current["NA_LAST_ERROR_DETAIL"] = row.get("NA_ERROR_DETAIL")
                current["NA_LAST_RAW_MESSAGE"] = row.get("NA_RAW_MESSAGE")

        refreshed_at = datetime.utcnow()
        payload_rows = []
        for row in grouped.values():
            payload_rows.append(
                {
                    "FK_HOST": row["FK_HOST"],
                    "NA_HOST_NAME": row["NA_HOST_NAME"],
                    "NA_ERROR_SCOPE": row["NA_ERROR_SCOPE"],
                    "NA_ERROR_DOMAIN": row["NA_ERROR_DOMAIN"],
                    "NA_ERROR_STAGE": row["NA_ERROR_STAGE"],
                    "NA_ERROR_CODE": row["NA_ERROR_CODE"],
                    "NA_ERROR_SUMMARY_HASH": row["NA_ERROR_SUMMARY_HASH"],
                    "NA_ERROR_SUMMARY": row["NA_ERROR_SUMMARY"],
                    "NU_ERROR_COUNT": int(row["NU_ERROR_COUNT"]),
                    "DT_FIRST_SEEN_AT": row["DT_FIRST_SEEN_AT"],
                    "DT_LAST_SEEN_AT": row["DT_LAST_SEEN_AT"],
                    "NA_LAST_SOURCE_TABLE": row["NA_LAST_SOURCE_TABLE"],
                    "ID_LAST_SOURCE_ROW": row["ID_LAST_SOURCE_ROW"],
                    "NA_LAST_ERROR_DETAIL": row["NA_LAST_ERROR_DETAIL"],
                    "NA_LAST_RAW_MESSAGE": row["NA_LAST_RAW_MESSAGE"],
                    "DT_REFRESHED_AT": refreshed_at,
                }
            )

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
        """Refresh server-wide grouped error buckets for the dashboard."""
        host_rows = self._select("SELECT * FROM HOST_ERROR_SUMMARY")
        hostless_rows = self._read_error_events(host_ids=None)
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
                    "DT_FIRST_SEEN_AT": row.get("DT_FIRST_SEEN_AT"),
                    "DT_LAST_SEEN_AT": row.get("DT_LAST_SEEN_AT"),
                    "NA_LAST_SOURCE_TABLE": row.get("NA_LAST_SOURCE_TABLE"),
                    "ID_LAST_SOURCE_ROW": row.get("ID_LAST_SOURCE_ROW"),
                    "NA_LAST_ERROR_DETAIL": row.get("NA_LAST_ERROR_DETAIL"),
                    "NA_LAST_RAW_MESSAGE": row.get("NA_LAST_RAW_MESSAGE"),
                }
                grouped[key] = current

            current["NU_ERROR_COUNT"] += int(row.get("NU_ERROR_COUNT") or 0)
            if row.get("DT_FIRST_SEEN_AT") and (
                current["DT_FIRST_SEEN_AT"] is None
                or row["DT_FIRST_SEEN_AT"] < current["DT_FIRST_SEEN_AT"]
            ):
                current["DT_FIRST_SEEN_AT"] = row["DT_FIRST_SEEN_AT"]
            row_key = (
                row.get("DT_LAST_SEEN_AT") or datetime(1970, 1, 1),
                int(row.get("ID_LAST_SOURCE_ROW") or 0),
            )
            current_key = (
                current.get("DT_LAST_SEEN_AT") or datetime(1970, 1, 1),
                int(current.get("ID_LAST_SOURCE_ROW") or 0),
            )
            if row_key >= current_key:
                current["DT_LAST_SEEN_AT"] = row.get("DT_LAST_SEEN_AT")
                current["NA_LAST_SOURCE_TABLE"] = row.get("NA_LAST_SOURCE_TABLE")
                current["ID_LAST_SOURCE_ROW"] = row.get("ID_LAST_SOURCE_ROW")
                current["NA_LAST_ERROR_DETAIL"] = row.get("NA_LAST_ERROR_DETAIL")
                current["NA_LAST_RAW_MESSAGE"] = row.get("NA_LAST_RAW_MESSAGE")

        for row in hostless_rows:
            if row.get("FK_HOST") is not None:
                continue
            error_summary = _error_summary_text(
                row.get("NA_ERROR_SUMMARY"),
                row.get("NA_RAW_MESSAGE"),
            )
            key = (
                row.get("NA_ERROR_SCOPE"),
                row.get("NA_ERROR_DOMAIN"),
                row.get("NA_ERROR_STAGE"),
                row.get("NA_ERROR_CODE"),
                _summary_hash(error_summary),
                error_summary,
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
                    "DT_FIRST_SEEN_AT": row.get("DT_EVENT_AT"),
                    "DT_LAST_SEEN_AT": row.get("DT_EVENT_AT"),
                    "NA_LAST_SOURCE_TABLE": row.get("NA_SOURCE_TABLE"),
                    "ID_LAST_SOURCE_ROW": row.get("ID_SOURCE_ROW"),
                    "NA_LAST_ERROR_DETAIL": row.get("NA_ERROR_DETAIL"),
                    "NA_LAST_RAW_MESSAGE": row.get("NA_RAW_MESSAGE"),
                }
                grouped[key] = current
            current["NU_ERROR_COUNT"] += 1
            if row.get("DT_EVENT_AT") and (
                current["DT_FIRST_SEEN_AT"] is None
                or row["DT_EVENT_AT"] < current["DT_FIRST_SEEN_AT"]
            ):
                current["DT_FIRST_SEEN_AT"] = row["DT_EVENT_AT"]
            if _sort_key_for_event_time(row) >= (
                current.get("DT_LAST_SEEN_AT") or datetime(1970, 1, 1),
                int(current.get("ID_LAST_SOURCE_ROW") or 0),
            ):
                current["DT_LAST_SEEN_AT"] = row.get("DT_EVENT_AT")
                current["NA_LAST_SOURCE_TABLE"] = row.get("NA_SOURCE_TABLE")
                current["ID_LAST_SOURCE_ROW"] = row.get("ID_SOURCE_ROW")
                current["NA_LAST_ERROR_DETAIL"] = row.get("NA_ERROR_DETAIL")
                current["NA_LAST_RAW_MESSAGE"] = row.get("NA_RAW_MESSAGE")

        refreshed_at = datetime.utcnow()
        payload_rows = [
            {
                "NA_ERROR_SCOPE": row["NA_ERROR_SCOPE"],
                "NA_ERROR_DOMAIN": row["NA_ERROR_DOMAIN"],
                "NA_ERROR_STAGE": row["NA_ERROR_STAGE"],
                "NA_ERROR_CODE": row["NA_ERROR_CODE"],
                "NA_ERROR_SUMMARY_HASH": row["NA_ERROR_SUMMARY_HASH"],
                "NA_ERROR_SUMMARY": row["NA_ERROR_SUMMARY"],
                "NU_ERROR_COUNT": int(row["NU_ERROR_COUNT"]),
                "DT_FIRST_SEEN_AT": row["DT_FIRST_SEEN_AT"],
                "DT_LAST_SEEN_AT": row["DT_LAST_SEEN_AT"],
                "NA_LAST_SOURCE_TABLE": row["NA_LAST_SOURCE_TABLE"],
                "ID_LAST_SOURCE_ROW": row["ID_LAST_SOURCE_ROW"],
                "NA_LAST_ERROR_DETAIL": row["NA_LAST_ERROR_DETAIL"],
                "NA_LAST_RAW_MESSAGE": row["NA_LAST_RAW_MESSAGE"],
                "DT_REFRESHED_AT": refreshed_at,
            }
            for row in grouped.values()
        ]

        self.db.replace_table_rows("SERVER_ERROR_SUMMARY", payload_rows)
        watermark = f"rows={len(payload_rows)}"
        return len(payload_rows), watermark

    def _refresh_host_current_snapshot(self) -> Tuple[int, str]:
        """Refresh the current host dashboard snapshot table."""
        hosts = self._select("SELECT * FROM BPDATA.HOST")
        queue_rows = self._select(
            """
            SELECT
                FK_HOST,
                SUM(CASE WHEN NU_TYPE = 1 AND NU_STATUS = 1 THEN 1 ELSE 0 END) AS NU_BACKUP_QUEUE_FILES_TOTAL,
                ROUND(COALESCE(SUM(CASE WHEN NU_TYPE = 1 AND NU_STATUS = 1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_BACKUP_QUEUE_GB_TOTAL,
                SUM(CASE WHEN NU_TYPE = 2 AND NU_STATUS = 1 THEN 1 ELSE 0 END) AS NU_PROCESSING_QUEUE_FILES_TOTAL,
                ROUND(COALESCE(SUM(CASE WHEN NU_TYPE = 2 AND NU_STATUS = 1 THEN VL_FILE_SIZE_KB ELSE 0 END), 0) / 1024 / 1024, 2) AS VL_PROCESSING_QUEUE_GB_TOTAL
            FROM BPDATA.FILE_TASK
            GROUP BY FK_HOST
            """
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
            SELECT *
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
            SELECT *
            FROM HOST_ERROR_SUMMARY
            ORDER BY
                FK_HOST ASC,
                COALESCE(DT_LAST_SEEN_AT, '1970-01-01 00:00:00') DESC,
                COALESCE(ID_LAST_SOURCE_ROW, 0) DESC
            """
        )

        queue_map = {int(row["FK_HOST"]): row for row in queue_rows}
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

        refreshed_at = datetime.utcnow()
        payload_rows = []
        for host in hosts:
            host_id = int(host["ID_HOST"])
            queue = queue_map.get(host_id, {})
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
                    "NU_DONE_FILE_DISCOVERY_TASKS": host.get("NU_DONE_FILE_DISCOVERY_TASKS"),
                    "NU_ERROR_FILE_DISCOVERY_TASKS": host.get("NU_ERROR_FILE_DISCOVERY_TASKS"),
                    "DT_LAST_BACKUP": host.get("DT_LAST_BACKUP"),
                    "NU_PENDING_FILE_BACKUP_TASKS": host.get("NU_PENDING_FILE_BACKUP_TASKS"),
                    "NU_DONE_FILE_BACKUP_TASKS": host.get("NU_DONE_FILE_BACKUP_TASKS"),
                    "NU_ERROR_FILE_BACKUP_TASKS": host.get("NU_ERROR_FILE_BACKUP_TASKS"),
                    "VL_PENDING_BACKUP_GB": _kb_to_gb(host.get("VL_PENDING_BACKUP_KB")),
                    "VL_DONE_BACKUP_GB": _kb_to_gb(host.get("VL_DONE_BACKUP_KB")),
                    "DT_LAST_PROCESSING": host.get("DT_LAST_PROCESSING"),
                    "NU_PENDING_FILE_PROCESS_TASKS": host.get("NU_PENDING_FILE_PROCESS_TASKS"),
                    "NU_DONE_FILE_PROCESS_TASKS": host.get("NU_DONE_FILE_PROCESS_TASKS"),
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
                    "FK_CURRENT_SITE": current_location.get("FK_SITE"),
                    "NA_CURRENT_SITE_LABEL": current_location.get("NA_SITE_LABEL"),
                    "NA_CURRENT_STATE_CODE": current_location.get("NA_STATE_CODE"),
                    "VL_CURRENT_LATITUDE": current_location.get("VL_LATITUDE"),
                    "VL_CURRENT_LONGITUDE": current_location.get("VL_LONGITUDE"),
                    "DT_CURRENT_SITE_LAST_SEEN": current_location.get("DT_LAST_SEEN_AT"),
                    "NA_LAST_ERROR_SCOPE": last_error.get("NA_ERROR_SCOPE"),
                    "NA_LAST_ERROR_CODE": last_error.get("NA_ERROR_CODE"),
                    "NA_LAST_ERROR_SUMMARY": last_error.get("NA_ERROR_SUMMARY"),
                    "DT_LAST_ERROR_AT": last_error.get("DT_LAST_SEEN_AT"),
                    "DT_REFRESHED_AT": refreshed_at,
                }
            )

        self.db.replace_table_rows("HOST_CURRENT_SNAPSHOT", payload_rows)
        watermark = f"hosts={len(payload_rows)}"
        return len(payload_rows), watermark

    def _refresh_server_current_summary(self) -> Tuple[int, str]:
        """Refresh the one-row server dashboard summary."""
        snapshot_rows = self._select("SELECT * FROM HOST_CURRENT_SNAPSHOT")
        current_month = datetime.utcnow().strftime("%Y-%m-01")
        metric_rows = self._select(
            """
            SELECT
                SUM(NU_BACKUP_DONE_FILES) AS NU_BACKUP_DONE_THIS_MONTH,
                ROUND(COALESCE(SUM(VL_BACKUP_DONE_GB), 0), 2) AS VL_BACKUP_DONE_GB_THIS_MONTH
            FROM HOST_MONTHLY_METRIC
            WHERE DT_REFERENCE_MONTH = %s
            """,
            (current_month,),
        )
        server_error_rows = self._select(
            """
            SELECT
                NA_ERROR_SCOPE,
                COUNT(*) AS NU_GROUPS
            FROM SERVER_ERROR_SUMMARY
            GROUP BY NA_ERROR_SCOPE
            """
        )

        backup_done_metrics = metric_rows[0] if metric_rows else {}
        error_group_map = {
            row["NA_ERROR_SCOPE"]: int(row.get("NU_GROUPS") or 0)
            for row in server_error_rows
        }

        refreshed_at = datetime.utcnow()
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
            "NU_PROCESSING_DONE_FILES_TOTAL": sum(int(row.get("NU_DONE_FILE_PROCESS_TASKS") or 0) for row in snapshot_rows),
            "NU_PROCESSING_ERROR_FILES_TOTAL": sum(int(row.get("NU_ERROR_FILE_PROCESS_TASKS") or 0) for row in snapshot_rows),
            "NU_PROCESSING_QUEUE_FILES_TOTAL": sum(int(row.get("NU_PROCESSING_QUEUE_FILES_TOTAL") or 0) for row in snapshot_rows),
            "VL_PROCESSING_QUEUE_GB_TOTAL": round(sum(float(row.get("VL_PROCESSING_QUEUE_GB_TOTAL") or 0) for row in snapshot_rows), 2),
            "NU_FACT_SPECTRUM_TOTAL": sum(int(row.get("NU_FACT_SPECTRUM_TOTAL") or 0) for row in snapshot_rows),
            "NU_BACKUP_DONE_THIS_MONTH": int(backup_done_metrics.get("NU_BACKUP_DONE_THIS_MONTH") or 0),
            "VL_BACKUP_DONE_GB_THIS_MONTH": backup_done_metrics.get("VL_BACKUP_DONE_GB_THIS_MONTH") or 0,
            "NU_BACKUP_ERROR_GROUPS": int(error_group_map.get("BACKUP", 0)),
            "NU_PROCESSING_ERROR_GROUPS": int(error_group_map.get("PROCESSING", 0)),
            "DT_REFRESHED_AT": refreshed_at,
        }

        self.db.replace_table_rows("SERVER_CURRENT_SUMMARY", [payload_row])
        watermark = f"hosts={len(snapshot_rows)};month={current_month[:7]}"
        return 1, watermark
