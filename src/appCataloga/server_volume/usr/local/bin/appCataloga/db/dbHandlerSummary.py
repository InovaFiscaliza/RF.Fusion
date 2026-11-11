"""Persist canonical Summary aggregates; source metadata stays in BKP/RFM."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
import struct
from typing import TYPE_CHECKING, cast

import psycopg

import config as k
from . import constants as db_constants
from .dbHandlerBase import DbHandler, DbRow

if TYPE_CHECKING:
    from shared import logging_utils


class dbHandlerSummary(DbHandler):
    """Own derived aggregates and worker persistence through DbHandler.

    Source labels, geography and operational signals are never materialized here.
    Usage metrics and their log checkpoints are outside every rebuild operation.

    Attributes:
        schema (str): Explicit Summary schema inherited from DbHandler.
        log (logging_utils.log): Caller logger inherited from DbHandler.
        reuse_connection (bool): Session reuse policy; required for singleton locks.
        SCHEMA_<TABLE> (str): Owning schema of each read-only source table; the
            base CRUD qualifies those tables with it instead of the Summary schema.
    """

    # Source routing is separate from the DDL; methods choose their projections.
    SCHEMA_HOST = k.BKP_DATABASE_NAME
    SCHEMA_HOST_TASK = k.BKP_DATABASE_NAME
    SCHEMA_FILE_TASK = k.BKP_DATABASE_NAME
    SCHEMA_FILE_TASK_HISTORY = k.BKP_DATABASE_NAME
    SCHEMA_DIM_SPECTRUM_EQUIPMENT = k.RFM_DATABASE_NAME
    SCHEMA_FACT_SPECTRUM = k.RFM_DATABASE_NAME
    SCHEMA_DIM_SPECTRUM_SITE = k.RFM_DATABASE_NAME
    SCHEMA_DIM_SITE_STATE = k.RFM_DATABASE_NAME
    SCHEMA_DIM_SITE_COUNTY = k.RFM_DATABASE_NAME
    SCHEMA_DIM_SITE_DISTRICT = k.RFM_DATABASE_NAME


    def __init__(
        self,
        database: str,
        log: logging_utils.log,
        *,
        conninfo: str,
        reuse_connection: bool = True,
    ) -> None:
        """Configure the summary schema without opening a session.

        Args:
            database (str): Exact summary schema name.
            log (logging_utils.log): Application logger.
            conninfo (str): Libpq settings with explicit dbname.
            reuse_connection (bool): Keep idle sessions; required for locks.
        Returns:
            None.
        """
        super().__init__(
            database, log, conninfo=conninfo, reuse_connection=reuse_connection
        )

    def worker_session_configure(self) -> None:
        """Set the bounded worker lock wait for this reusable session.

        PostgreSQL already defaults to READ COMMITTED, so only the lock wait is set.

        Args:
            None.
        Returns:
            None.
        Raises:
            RuntimeError: If session reuse is disabled or a transaction is open.
        """
        if not self.reuse_connection or self.in_transaction:
            raise RuntimeError(
                "Worker configuration requires an idle reusable session."
            )
        self._setting_write("lock_timeout", f"{db_constants['summary']['limits']['lock_timeout_sec']}s")

    def worker_lock_acquire(self, name: str) -> bool:
        """Try to acquire the worker singleton on this reusable session.

        Args:
            name (str): Stable lock identity shared by all worker instances.
        Returns:
            bool: True when acquired, False for an expected competing owner.
        Raises:
            RuntimeError: Session reuse is disabled.
        """
        if not self.reuse_connection:
            raise RuntimeError("The worker singleton requires a reusable session.")
        return self._advisory_lock_try(name)

    def worker_lock_release(self, name: str) -> None:
        """Release the singleton and close the owning session.

        Args:
            name (str): Previously acquired singleton identity.
        Returns:
            None.
        Raises:
            RuntimeError: No session exists or this session does not own the lock.
        """
        if self.db_connection is None:
            raise RuntimeError("No singleton-owning session exists.")
        try:
            if not self._advisory_unlock(name):
                raise RuntimeError("The session does not own the worker singleton.")
        finally:
            self.close()

    def worker_state_read(self, consumer_name: str) -> DbRow:
        """Read or initialize a durable consumer checkpoint without overwriting it.

        Args:
            consumer_name (str): Consumer identity, stored as NA_CONSUMER.
        Returns:
            DbRow: All declared worker fields, including ID_LAST_OUTBOX (int),
                NA_STATUS (str), nullable timestamps and diagnostic metadata.
        Raises:
            RuntimeError: State disappeared during initialization.
        """
        where_conditions = {"NA_CONSUMER": consumer_name}
        row = self._select_row(table="SUMMARY_WORKER_STATE", where=where_conditions)
        if row is not None:
            return row
        self._insert_row(
            table="SUMMARY_WORKER_STATE", data=where_conditions, skip_duplicates=True
        )
        row = self._select_row(table="SUMMARY_WORKER_STATE", where=where_conditions)
        if row is None:
            raise RuntimeError("Consumer state missing after initialization.")
        return row

    def worker_state_start(self, consumer_name: str) -> None:
        """Stamp a running refresh pass without advancing its checkpoint.

        Args:
            consumer_name (str): Initialized consumer identity.
        Returns:
            None.
        """
        data = {
            "DT_LAST_START": datetime.utcnow(),
            "NA_STATUS": db_constants['summary']['worker_states']['running'],
            "NA_ERROR_MESSAGE": None,
        }
        self._worker_state_update(consumer_name, data)

    def worker_state_success(
        self,
        consumer_name: str,
        *,
        last_outbox_id: int,
        batch_size: int,
        event_count: int,
    ) -> None:
        """Persist the consumed checkpoint and successful pass metadata.

        Args:
            consumer_name (str): Initialized consumer identity.
            last_outbox_id (int): Last processed ID, or the watermark captured
                before a full reconcile starts.
            batch_size (int): Configured event limit.
            event_count (int): Number of consumed events.
        Returns:
            None.
        """
        now = datetime.utcnow()
        data = {
            "ID_LAST_OUTBOX": last_outbox_id,
            "DT_LAST_END": now,
            "DT_LAST_SUCCESS": now,
            "NU_LAST_BATCH_SIZE": batch_size,
            "NU_LAST_EVENT_COUNT": event_count,
            "NA_STATUS": db_constants['summary']['worker_states']['idle'],
            "NA_ERROR_MESSAGE": None,
        }
        self._worker_state_update(consumer_name, data)

    def worker_state_failure(self, consumer_name: str, *, error_message: str) -> None:
        """Record a failed pass while retaining its retry checkpoint.

        Args:
            consumer_name (str): Initialized consumer identity.
            error_message (str): Caller-sanitized diagnostic text.
        Returns:
            None.
        """
        now = datetime.utcnow()
        data = {
            "DT_LAST_END": now,
            "DT_LAST_FAILURE": now,
            "NA_STATUS": db_constants['summary']['worker_states']['error'],
            "NA_ERROR_MESSAGE": error_message,
        }
        self._worker_state_update(consumer_name, data)

    def _worker_state_update(
        self, consumer_name: str, data: Mapping[str, object]
    ) -> None:
        """Apply one pass transition to an initialized consumer checkpoint row.

        Args:
            consumer_name (str): Initialized consumer identity.
            data (Mapping[str, object]): SUMMARY_WORKER_STATE replacements.
        Returns:
            None.
        """
        where_conditions = {"NA_CONSUMER": consumer_name}
        self._update_row(
            table="SUMMARY_WORKER_STATE", data=data, where=where_conditions
        )

    def outbox_list_pending(
        self, consumer_name: str, *, batch_size: int
    ) -> list[DbRow]:
        """Read dirty scopes strictly after the durable consumer checkpoint.

        Args:
            consumer_name (str): Consumer identity, initialized if absent.
            batch_size (int): Positive maximum number of events.
        Returns:
            list[DbRow]: Events ordered by ID_OUTBOX (int), with NA_SCOPE_TYPE
                and NA_SCOPE_VALUE (str), nullable NA_SOURCE_HANDLER/NA_REASON
                (str) and DT_CREATED_AT (datetime).
        """
        state = self.worker_state_read(consumer_name)
        return self._select_batch(
            table="SUMMARY_OUTBOX",
            where={"ID_OUTBOX__gt": state["ID_LAST_OUTBOX"]},
            order_by=(("ID_OUTBOX", "ASC"),),
            limit=batch_size,
        )

    def outbox_read_watermark(self) -> int:
        """Capture the last published event before a full rebuild starts.

        Args:
            None.
        Returns:
            int: Largest visible ID_OUTBOX, or zero for an empty queue.
        """
        rows = self._select_batch(
            table="SUMMARY_OUTBOX",
            columns=("ID_OUTBOX",),
            order_by=(("ID_OUTBOX", "DESC"),),
            limit=1,
        )
        return cast(int, rows[0]["ID_OUTBOX"]) if rows else 0

    def outbox_delete_consumed(self, consumer_name: str) -> int:
        """Delete only events at or behind the durable checkpoint.

        Args:
            consumer_name (str): Consumer identity.
        Returns:
            int: Number of deleted events, zero before the first checkpoint.
        """
        checkpoint = cast(int, self.worker_state_read(consumer_name)["ID_LAST_OUTBOX"])
        if checkpoint <= 0:
            return 0
        return self._delete_batch(
            table="SUMMARY_OUTBOX", where={"ID_OUTBOX__lte": checkpoint}
        )

    def refresh_log_record(
        self,
        object_name: str,
        *,
        started_at: datetime,
        success: bool,
        row_count: int = 0,
        high_watermark: str | None = None,
        error_message: str | None = None,
    ) -> None:
        """Audit one materialization and isolate optional retention in a savepoint.

        Args:
            object_name (str): Canonical aggregate identity.
            started_at (datetime): UTC start timestamp.
            success (bool): Whether publication completed successfully.
            row_count (int): Number of published rows, zero on failure.
            high_watermark (str | None): Optional diagnostic watermark.
            error_message (str | None): Sanitized failure description.
        Returns:
            None.
        """
        data = {
            "NA_OBJECT_NAME": object_name,
            "DT_STARTED_AT": started_at,
            "DT_FINISHED_AT": datetime.utcnow(),
            "IS_SUCCESS": int(success),
            "NU_ROW_COUNT": row_count,
            "NA_SOURCE_HIGH_WATERMARK": high_watermark,
            "NA_ERROR_MESSAGE": error_message,
        }
        with self._unit_of_work():
            self._insert_row(table="SUMMARY_REFRESH_LOG", data=data)
            try:
                with self._savepoint():
                    self._refresh_log_prune(max_rows=db_constants['summary']['limits']['refresh_log_max_rows'])
            except psycopg.Error as error:
                self.log.warning_event(
                    "summary_refresh_log_prune_failed",
                    db_handler=type(self).__name__,
                    error=type(error).__name__,
                )

    def _refresh_log_prune(self, *, max_rows: int) -> int:
        """Keep the newest configured audit window without resetting identities.

        Args:
            max_rows (int): Window size, clamped to at least one.
        Returns:
            int: Number of removed old audit rows.
        """
        window = max(1, max_rows)
        rows = self._select_batch(
            table="SUMMARY_REFRESH_LOG",
            columns=("ID_REFRESH_LOG",),
            order_by=(("ID_REFRESH_LOG", "DESC"),),
            limit=window,
        )
        if len(rows) < window:
            return 0
        where_conditions = {"ID_REFRESH_LOG__lt": rows[-1]["ID_REFRESH_LOG"]}
        return self._delete_batch(table="SUMMARY_REFRESH_LOG", where=where_conditions)

    def host_equipment_list_sources(self) -> dict[str, list[DbRow]]:
        """Read host and equipment catalogs for automatic matching.

        Args:
            None.
        Returns:
            dict[str, list[DbRow]]: Required keys hosts (ID_HOST int, NA_HOST_NAME
                nullable str) and equipment (ID_EQUIPMENT int, NA_EQUIPMENT
                nullable str). No existing links are used as matching inputs.
        """
        return {
            "hosts": self._select_batch(
                table="HOST", columns=("ID_HOST", "NA_HOST_NAME")
            ),
            "equipment": self._select_batch(
                table="DIM_SPECTRUM_EQUIPMENT", columns=("ID_EQUIPMENT", "NA_EQUIPMENT")
            ),
        }

    def host_equipment_replace(self, rows: Sequence[DbRow]) -> int:
        """Atomically publish the complete matching result, without source metadata.

        Args:
            rows (Sequence[DbRow]): FK_HOST/FK_EQUIPMENT int, NA_MATCH_TYPE str,
                VL_MATCH_CONFIDENCE Decimal and IS_PRIMARY_LINK smallint;
                empty clears all derived links.
        Returns:
            int: Number of published candidates.
        """
        return self._replace_scope("HOST_EQUIPMENT_LINK", rows)

    def site_equipment_list_sources(
        self,
        *,
        equipment_ids: Sequence[int] | None = None,
    ) -> list[DbRow]:
        """Aggregate spectra once at the canonical site/equipment grain.

        Args:
            equipment_ids (Sequence[int] | None): Selected equipment; None
                means full rebuild, empty means no equipment.
        Returns:
            list[DbRow]: FK_SITE/FK_EQUIPMENT int, nullable first/last timestamps,
                NU_SPECTRUM_COUNT and ID_LAST_SPECTRUM int. No labels or geography.
        """
        where_conditions: DbRow = {"FK_SITE__ne": None, "FK_EQUIPMENT__ne": None}
        if equipment_ids is not None:
            where_conditions["FK_EQUIPMENT__in"] = tuple(equipment_ids)
        aggregates = {
            "DT_FIRST_SEEN_AT": ("min", "DT_TIME_START", None),
            "DT_LAST_SEEN_AT": ("max", "DT_TIME_END", None),
            "NU_SPECTRUM_COUNT": ("count", None, None),
            "ID_LAST_SPECTRUM": ("max", "ID_SPECTRUM", None),
        }
        return self._select_aggregate(
            "FACT_SPECTRUM",
            aggregates,
            group_by=("FK_SITE", "FK_EQUIPMENT"),
            where=where_conditions,
        )

    def site_equipment_list_affected(
        self,
        *,
        site_ids: Sequence[int],
        equipment_ids: Sequence[int],
    ) -> list[int]:
        """Include both current and obsolete owners of invalidated sites.

        Args:
            site_ids (Sequence[int]): Invalidated site identities.
            equipment_ids (Sequence[int]): Explicitly invalidated equipment.
        Returns:
            list[int]: Sorted union of old and current equipment owners, including
                explicit identities with no remaining spectra.
        """
        owners = set(equipment_ids)
        if not site_ids:
            return sorted(owners)
        where_conditions = {"FK_SITE__in": tuple(site_ids), "FK_EQUIPMENT__ne": None}
        for table in ("FACT_SPECTRUM", "SITE_EQUIPMENT_METRIC"):
            rows = self._select_batch(
                table=table,
                where=where_conditions,
                columns=("FK_EQUIPMENT",),
                distinct=True,
            )
            owners.update(cast(int, row["FK_EQUIPMENT"]) for row in rows)
        return sorted(owners)

    def site_equipment_replace(
        self,
        rows: Sequence[DbRow],
        *,
        equipment_ids: Sequence[int] | None = None,
    ) -> int:
        """Publish every site of selected equipment, including obsolete removals.

        Args:
            rows (Sequence[DbRow]): site_equipment_list_sources rows, optionally
                including DT_REFRESHED_AT datetime.
            equipment_ids (Sequence[int] | None): None replaces all rows;
                empty targets none. Every supplied row must belong to this scope.
        Returns:
            int: Number of inserted aggregate rows.
        Raises:
            ValueError: A supplied equipment lies outside the selected scope.
        """
        return self._replace_scope(
            "SITE_EQUIPMENT_METRIC", rows, FK_EQUIPMENT=equipment_ids
        )

    def host_visit_list_hosts(
        self,
        *,
        host_ids: set[int] | None = None,
        site_ids: set[int] | None = None,
        equipment_ids: set[int] | None = None,
    ) -> list[int]:
        """Find current and obsolete host histories affected by dirty scopes.

        Args:
            host_ids (set[int] | None): Explicit hosts, included even without links.
            site_ids (set[int] | None): Current or historical affected sites.
            equipment_ids (set[int] | None): Affected linked equipment.
                All three None request every linked or published host.
        Returns:
            list[int]: Sorted unique host identities.
        """
        if host_ids is None and site_ids is None and equipment_ids is None:
            hosts: set[int] = set()
            for table in ("HOST_EQUIPMENT_LINK", "HOST_LOCATION_VISIT"):
                rows = self._select_batch(table=table, columns=("FK_HOST",), distinct=True)
                hosts.update(cast(int, row["FK_HOST"]) for row in rows)
            return sorted(hosts)
        hosts = set(host_ids or ())
        equipment = set(equipment_ids or ())
        if site_ids:
            sites = {"FK_SITE__in": tuple(site_ids)}
            # Equipment observed at a dirty site and the hosts that visited it.
            for table, field, target in (
                ("SITE_EQUIPMENT_METRIC", "FK_EQUIPMENT", equipment),
                ("HOST_LOCATION_VISIT", "FK_HOST", hosts),
            ):
                rows = self._select_batch(
                    table=table, where=sites, columns=(field,), distinct=True
                )
                target.update(cast(int, row[field]) for row in rows)
        if equipment:
            rows = self._select_batch(
                table="HOST_EQUIPMENT_LINK",
                where={"FK_EQUIPMENT__in": tuple(equipment)},
                columns=("FK_HOST",),
                distinct=True,
            )
            hosts.update(cast(int, row["FK_HOST"]) for row in rows)
        return sorted(hosts)

    def host_visit_list_sources(self, host_id: int) -> list[DbRow]:
        """Aggregate consecutive site visits from one host's dated spectra.

        Args:
            host_id: Host identifier (int); only active primary links are used.

        Returns:
            Chronologically ordered list[dict] with required keys FK_HOST (int),
            NU_VISIT (int), FK_SITE (int), DT_FIRST_SEEN_AT (datetime),
            DT_LAST_SEEN_AT (datetime), NU_SPECTRUM_COUNT (int).
            Spectra lacking a start time or site are excluded.

        Raises:
            Exception: Read failure or per-host query timeout.
        """
        columns = ("ID_SPECTRUM", "FK_SITE", "DT_TIME_START", "DT_TIME_END")
        order_by = (("DT_TIME_START", "ASC"), ("FK_SITE", "ASC"), ("ID_SPECTRUM", "ASC"))
        with self._unit_of_work():
            with self._local_setting(
                "statement_timeout", f"{db_constants['summary']['limits']['timeline_query_timeout_sec']}s"
            ):
                spectra = self._select_batch(
                    table="FACT_SPECTRUM",
                    where={"DT_TIME_START__ne": None, "FK_SITE__ne": None},
                    columns=columns,
                    join=("HOST_EQUIPMENT_LINK", "FK_EQUIPMENT", "FK_EQUIPMENT"),
                    joined_where={"FK_HOST": host_id, "IS_PRIMARY_LINK": 1},
                    order_by=order_by,
                )
        # A visit is a run of consecutive spectra at the same site.
        visits: list[DbRow] = []
        for spectrum in spectra:
            start = cast(datetime, spectrum["DT_TIME_START"])
            end = max(start, cast(datetime | None, spectrum["DT_TIME_END"]) or start)
            if not visits or visits[-1]["FK_SITE"] != spectrum["FK_SITE"]:
                visits.append(
                    {
                        "FK_HOST": host_id,
                        "NU_VISIT": len(visits) + 1,
                        "FK_SITE": spectrum["FK_SITE"],
                        "DT_FIRST_SEEN_AT": start,
                        "DT_LAST_SEEN_AT": end,
                        "NU_SPECTRUM_COUNT": 0,
                    }
                )
            visit = visits[-1]
            visit["DT_LAST_SEEN_AT"] = max(cast(datetime, visit["DT_LAST_SEEN_AT"]), end)
            visit["NU_SPECTRUM_COUNT"] = cast(int, visit["NU_SPECTRUM_COUNT"]) + 1
        return visits

    def host_visit_replace(self, host_id: int, rows: Sequence[DbRow]) -> int:
        """Atomically replace one host's visit sequence, including obsolete rows.

        Args:
            host_id (int): Host owning every supplied row.
            rows (Sequence[DbRow]): Required FK_HOST, NU_VISIT, FK_SITE and
                NU_SPECTRUM_COUNT (int), DT_FIRST_SEEN_AT/DT_LAST_SEEN_AT and
                DT_REFRESHED_AT (datetime), IS_OVERLAPPING (0/1).
                Empty removes the previous sequence.
        Returns:
            int: Number of visits inserted.
        Raises:
            ValueError: A supplied row belongs to another host.
        """
        return self._replace_scope("HOST_LOCATION_VISIT", rows, FK_HOST=(host_id,))

    def host_metric_list_sources(
        self,
        *,
        current_month_start: datetime,
        next_month_start: datetime,
        host_ids: Sequence[int] | None = None,
    ) -> list[DbRow]:
        """Read one operational aggregate per selected host with bounded source scans.

        Args:
            current_month_start (datetime): Inclusive UTC operational month start.
            next_month_start (datetime): Exclusive UTC month end.
            host_ids (Sequence[int] | None): None selects every registered host;
                empty selects none. The same scope filters queue and history scans.
        Returns:
            list[DbRow]: Complete HOST_METRIC records: FK_HOST int,
                DT_REFERENCE_MONTH date, NU_* counts and VL_* volumes in GB.
                Missing activity is zero; source labels and signals are excluded.
        """
        scope = None if host_ids is None else tuple(host_ids)
        queue, history = self._host_metric_aggregates(
            current_month_start, next_month_start
        )
        value_fields = self._host_metric_value_fields(queue, history)
        where_hosts = None if scope is None else {"FK_HOST__in": scope}
        by_host: dict[object, DbRow] = {}
        for table, aggregates in (("FILE_TASK", queue), ("FILE_TASK_HISTORY", history)):
            for row in self._select_aggregate(
                table, aggregates, group_by=("FK_HOST",), where=where_hosts
            ):
                by_host.setdefault(row["FK_HOST"], {}).update(row)
        hosts = self._select_batch(
            table="HOST",
            where=None if scope is None else {"ID_HOST__in": scope},
            columns=("ID_HOST",),
        )
        reference_month = current_month_start.date()
        results: list[DbRow] = []
        for host in hosts:
            sums = by_host.get(host["ID_HOST"], {})
            sums["KB_PAYLOAD_DELETED_GB_TOTAL"] = sum(
                cast(int | Decimal, sums.get(name) or 0)
                for name in ("KB_PAYLOAD_SERVER", "KB_PAYLOAD_HOST")
            )
            row: DbRow = {"FK_HOST": host["ID_HOST"], "DT_REFERENCE_MONTH": reference_month}
            for field in value_fields:
                if field.startswith("VL_"):
                    row[field] = self._kb_to_gb(sums.get("KB_" + field.removeprefix("VL_")))
                else:
                    row[field] = sums.get(field) or 0
            results.append(row)
        return results

    def _host_metric_aggregates(
        self, current_month_start: datetime, next_month_start: datetime
    ) -> tuple[
        dict[str, tuple[str, str | None, DbRow]],
        dict[str, tuple[str, str | None, DbRow]],
    ]:
        """Resolve explicit metrics grouped by source and phase in summary.host_metrics.

        Each phase shares its task type and status column. Its metrics name
        the state, count and volume; month_column adds the caller's date range.
        Deleted payloads retain their separate server-size/host-size fallback.

        Args:
            current_month_start (datetime): Inclusive UTC operational month start.
            next_month_start (datetime): Exclusive UTC month end.
        Returns:
            tuple[dict, dict]: Queue and history output names mapped to
                (aggregate function, source column or None, filter mapping).
                KB_* outputs are converted to VL_* GB values by the reader.
        Raises:
            KeyError: If a required definition or referenced constant is absent.
        """
        states = db_constants['tasks']['states']
        file_types = db_constants['tasks']['file_types']
        aggregates: dict[str, dict[str, tuple[str, str | None, DbRow]]] = {}
        for source, phases in db_constants['summary']['host_metrics'].items():
            metrics: dict[str, tuple[str, str | None, DbRow]] = {}
            for phase in phases.values():
                phase_filters: DbRow = {}
                task_type = phase.get('task_type')
                if task_type is not None:
                    phase_filters['NU_TYPE'] = file_types[task_type]
                for definition in phase['metrics']:
                    filters: DbRow = {
                        **phase_filters,
                        phase['status_column']: states[definition['state']],
                    }
                    month_column = definition.get('month_column')
                    if month_column is not None:
                        filters[f"{month_column}__gte"] = current_month_start
                        filters[f"{month_column}__lt"] = next_month_start
                    self._add_metric_pair(
                        metrics, definition['count'], definition['volume'], filters
                    )
            aggregates[source] = metrics

        queue = aggregates['queue']
        history = aggregates['history']
        # Deleted payloads prefer the server size and fall back to the host size.
        deleted: DbRow = {"IS_PAYLOAD_DELETED": 1}
        history["NU_PAYLOAD_DELETED_FILES_TOTAL"] = ("count", None, deleted)
        history["KB_PAYLOAD_SERVER"] = (
            "sum",
            "VL_FILE_SIZE_KB_SERVER",
            {**deleted, "VL_FILE_SIZE_KB_SERVER__ne": None},
        )
        history["KB_PAYLOAD_HOST"] = (
            "sum",
            "VL_FILE_SIZE_KB_HOST",
            {**deleted, "VL_FILE_SIZE_KB_SERVER": None},
        )
        return queue, history

    @staticmethod
    def _host_metric_value_fields(
        queue: Mapping[str, object], history: Mapping[str, object]
    ) -> tuple[str, ...]:
        """Derive published count/GB names from the aggregate calculations.

        Args:
            queue (Mapping[str, object]): Queue aggregate output definitions.
            history (Mapping[str, object]): History aggregate output definitions.
        Returns:
            tuple[str, ...]: Ordered published metrics, excluding intermediate
                payload sums; these are combined into the deleted-payload volume.
        """
        fields = {
            "VL_" + name.removeprefix("KB_") if name.startswith("KB_") else name
            for name in (*queue, *history)
            if name not in ("KB_PAYLOAD_SERVER", "KB_PAYLOAD_HOST")
        }
        fields.add("VL_PAYLOAD_DELETED_GB_TOTAL")
        return tuple(sorted(fields))

    def host_read_operational(self, *, host_id: int) -> DbRow:
        """Combine one host's canonical state, small aggregates and current site.

        Args:
            host_id (int): Operational host identity.
        Returns:
            DbRow: ID_HOST int, host name/address/port, connectivity/busy state,
                check/discovery/phase timestamps, direct operational evidence,
                NU_* counts and VL_* GB volumes, metric reference month and
                refresh time, current locality/site/state labels and coordinates.
                Missing hosts return {}. Throughput outside the current UTC
                month is zero; DT_METRIC_REFERENCE_MONTH exposes its stored age.
                Credentials and worker PIDs are never projected.
        """
        source_fields = (
            "ID_HOST",
            "NA_HOST_NAME",
            "NA_HOST_ADDRESS",
            "NA_HOST_PORT",
            "IS_OFFLINE",
            "IS_BUSY",
            "DT_LAST_CHECK",
            "DT_LAST_DISCOVERY",
            "DT_LAST_BACKUP",
            "DT_LAST_PROCESSING",
            "DT_LAST_OFFLINE_AT",
            "NA_LAST_OFFLINE_DESCRIPTION",
            *sorted(db_constants['hosts']['signal_fields']),
        )
        hosts = self._select_batch(
            table="HOST", where={"ID_HOST": host_id}, columns=source_fields
        )
        if not hosts:
            return {}
        result = hosts[0]
        metrics = self._select_batch(
            table="HOST_METRIC", where={"FK_HOST": host_id}, limit=1
        )
        metric = metrics[0] if metrics else {}
        current_month = datetime.utcnow().date().replace(day=1)
        stale_month = metric.get("DT_REFERENCE_MONTH") != current_month
        month_start = datetime.combine(current_month, datetime.min.time())
        month_end = (month_start + timedelta(days=32)).replace(day=1)
        queue, history = self._host_metric_aggregates(month_start, month_end)
        for field in self._host_metric_value_fields(queue, history):
            # Throughput belongs to the month it was computed for.
            if field.endswith("_THIS_MONTH") and stale_month:
                result[field] = 0
            else:
                result[field] = metric.get(field) or 0
        result["DT_METRIC_REFERENCE_MONTH"] = metric.get("DT_REFERENCE_MONTH")
        result["DT_REFRESHED_AT"] = metric.get("DT_REFRESHED_AT")
        observations = self._select_batch(
            table="SITE_EQUIPMENT_METRIC",
            columns=("FK_SITE", "NU_SPECTRUM_COUNT", "DT_FIRST_SEEN_AT", "DT_LAST_SEEN_AT"),
            join=("HOST_EQUIPMENT_LINK", "FK_EQUIPMENT", "FK_EQUIPMENT"),
            joined_where={"FK_HOST": host_id, "IS_PRIMARY_LINK": 1},
        )
        result["NU_FACT_SPECTRUM_TOTAL"] = sum(
            cast(int, row["NU_SPECTRUM_COUNT"] or 0) for row in observations
        )
        result.update(self._read_current_site(observations))
        return result

    def _read_current_site(self, observations: Sequence[DbRow]) -> DbRow:
        """Describe the most recently observed site of a host.

        The current site has the latest last-seen instant, then the latest
        first-seen instant, then the highest site ID; missing instants sort last.

        Args:
            observations (Sequence[DbRow]): FK_SITE int and nullable
                DT_FIRST_SEEN_AT/DT_LAST_SEEN_AT datetime of the host's links.
        Returns:
            DbRow: NA_CURRENT_SITE_LABEL, NA_CURRENT_STATE_CODE and
                NA_CURRENT_LOCALITY_LABEL nullable str, VL_CURRENT_LATITUDE and
                VL_CURRENT_LONGITUDE nullable float. All None without a site.
        """
        current: DbRow = {
            "NA_CURRENT_SITE_LABEL": None,
            "NA_CURRENT_STATE_CODE": None,
            "NA_CURRENT_LOCALITY_LABEL": None,
            "VL_CURRENT_LATITUDE": None,
            "VL_CURRENT_LONGITUDE": None,
        }

        def recency(row: DbRow) -> tuple[object, ...]:
            last = row["DT_LAST_SEEN_AT"] or row["DT_FIRST_SEEN_AT"]
            first = row["DT_FIRST_SEEN_AT"] or row["DT_LAST_SEEN_AT"]
            return (last is not None, last or datetime.min, first is not None, first or datetime.min, row["FK_SITE"])

        if not observations:
            return current
        site_id = max(observations, key=recency)["FK_SITE"]
        sites = self._select_batch(
            table="DIM_SPECTRUM_SITE",
            where={"ID_SITE": site_id},
            columns=("NA_SITE", "FK_STATE", "FK_COUNTY", "FK_DISTRICT", "GEO_POINT"),
        )
        if not sites:
            return current
        site = sites[0]
        current["NA_CURRENT_SITE_LABEL"] = site["NA_SITE"]
        locality = site["NA_SITE"]
        for table, key, field, label in (
            ("DIM_SITE_COUNTY", "ID_COUNTY", "FK_COUNTY", "NA_COUNTY"),
            ("DIM_SITE_DISTRICT", "ID_DISTRICT", "FK_DISTRICT", "NA_DISTRICT"),
            ("DIM_SITE_STATE", "ID_STATE", "FK_STATE", "LC_STATE"),
        ):
            if site[field] is None:
                continue
            rows = self._select_batch(
                table=table, where={key: site[field]}, columns=(label,)
            )
            value = rows[0][label] if rows else None
            if table == "DIM_SITE_STATE":
                current["NA_CURRENT_STATE_CODE"] = value
            elif value is not None:
                # District overrides county, which overrides the site label.
                locality = value
        current["NA_CURRENT_LOCALITY_LABEL"] = locality
        coordinates = self._point_coordinates(cast(str | None, site["GEO_POINT"]))
        if coordinates is not None:
            current["VL_CURRENT_LONGITUDE"], current["VL_CURRENT_LATITUDE"] = coordinates
        return current

    @staticmethod
    def _point_coordinates(geometry: str | None) -> tuple[float, float] | None:
        """Decode x/y from the hex EWKB text PostgreSQL returns for a Point.

        Psycopg has no PostGIS adapter here, so geometry arrives as the
        server's text output. Only the 2D coordinates are read; SRID, Z and M
        flags are skipped.

        Args:
            geometry (str | None): Hex-encoded (E)WKB, or None.
        Returns:
            tuple[float, float] | None: (x, y), or None for a missing value.
        Raises:
            ValueError: If the value is not a point geometry.
        """
        if geometry is None:
            return None
        raw = bytes.fromhex(geometry)
        order = "<" if raw[0] == 1 else ">"
        (kind,) = struct.unpack(order + "I", raw[1:5])
        if kind & 0x0FFFFFFF != 1:
            raise ValueError("Site geometry is not a point.")
        offset = 9 if kind & 0x20000000 else 5
        x, y = struct.unpack(order + "dd", raw[offset : offset + 16])
        return x, y

    def host_metric_replace(
        self,
        rows: Sequence[DbRow],
        *,
        host_ids: Sequence[int] | None = None,
    ) -> int:
        """Publish operational aggregates for selected hosts without copying state.

        Args:
            rows (Sequence[DbRow]): FK_HOST int, DT_REFERENCE_MONTH date and
                declared NU_* counts / VL_* GB values, optional refresh timestamp.
                Counts describe queue and current archive; *_THIS_MONTH values
                use the operational month stored in DT_REFERENCE_MONTH.
            host_ids (Sequence[int] | None): None means all, empty means none.
        Returns:
            int: Number of inserted host aggregate rows.
        Raises:
            ValueError: A row lies outside the selected host scope.
        """
        return self._replace_scope("HOST_METRIC", rows, FK_HOST=host_ids)

    @staticmethod
    def _add_metric_pair(
        aggregates: dict[str, tuple[str, str | None, DbRow]],
        count: str,
        volume: str,
        condition: DbRow,
    ) -> None:
        """Register a file count and its host-size sum under the same filter.

        The sum is keyed as KB_<volume without VL_> so callers convert it to
        the published VL_ column with _kb_to_gb.

        Args:
            aggregates (dict[str, tuple[str, str | None, DbRow]]): Spec being built.
            count (str): Published NU_ count column.
            volume (str): Published VL_ GB column.
            condition (DbRow): Mapping filter shared by both aggregates.
        Returns:
            None.
        """
        aggregates[count] = ("count", None, condition)
        aggregates["KB_" + volume.removeprefix("VL_")] = (
            "sum",
            "VL_FILE_SIZE_KB_HOST",
            condition,
        )

    @staticmethod
    def _kb_to_gb(kilobytes: object) -> Decimal:
        """Convert a KB sum to GB rounded half away from zero to two decimals.

        Args:
            kilobytes (object): Integer, Decimal or None (treated as zero).
        Returns:
            Decimal: Gigabytes with two decimal places.
        """
        gigabytes = Decimal(cast(int, kilobytes or 0)) / Decimal(db_constants['persistence']['kilobytes_per_gb'])
        return gigabytes.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def host_monthly_list_sources(
        self,
        *,
        host_ids: Sequence[int] | None = None,
        reference_months: Sequence[date] | None = None,
    ) -> list[DbRow]:
        """Aggregate archive states by file creation month, distinct from throughput.

        Args:
            host_ids (Sequence[int] | None): None selects all hosts; empty none.
            reference_months (Sequence[date] | None): First days of creation
                months; None selects all valid months, empty none.
        Returns:
            list[DbRow]: FK_HOST int, DT_REFERENCE_MONTH date, discovered count
                and GB volume plus backup/processing DONE/PENDING/ERROR counts
                and GB volumes. Invalid or missing creation dates are excluded.
        Raises:
            ValueError: A reference month is not its first day.
        """
        if reference_months is not None and any(
            month.day != 1 for month in reference_months
        ):
            raise ValueError("Reference months must be first days.")
        if (host_ids is not None and not host_ids) or (
            reference_months is not None and not reference_months
        ):
            return []
        aggregates: dict[str, tuple[str, str | None, DbRow]] = {}
        self._add_metric_pair(aggregates, "NU_DISCOVERED_FILES", "VL_DISCOVERED_GB", {})
        for phase in ("BACKUP", "PROCESSING"):
            for label, status in (
                ("DONE", db_constants['tasks']['states']['done']),
                ("PENDING", db_constants['tasks']['states']['pending']),
                ("ERROR", db_constants['tasks']['states']['error']),
            ):
                self._add_metric_pair(
                    aggregates,
                    f"NU_{phase}_{label}_FILES",
                    f"VL_{phase}_{label}_GB",
                    {f"NU_STATUS_{phase}": status},
                )
        where_conditions: DbRow = {
            "FK_HOST__ne": None,
            "DT_FILE_CREATED_HOST__ne": None,
        }
        if host_ids is not None:
            where_conditions["FK_HOST__in"] = tuple(host_ids)
        months = None if reference_months is None else set(reference_months)
        if months:
            # One range keeps the (FK_HOST, DT_FILE_CREATED_HOST) index usable;
            # months inside it that were not requested are dropped below.
            last = max(months)
            where_conditions["DT_FILE_CREATED_HOST__gte"] = min(months)
            where_conditions["DT_FILE_CREATED_HOST__lt"] = (
                last + timedelta(days=32)
            ).replace(day=1)
        rows = self._select_aggregate(
            "FILE_TASK_HISTORY",
            aggregates,
            group_by=("FK_HOST",),
            month_buckets={"DT_REFERENCE_MONTH": "DT_FILE_CREATED_HOST"},
            where=where_conditions,
        )
        results: list[DbRow] = []
        for row in rows:
            if months is not None and row["DT_REFERENCE_MONTH"] not in months:
                continue
            for name in [name for name in row if name.startswith("KB_")]:
                row["VL_" + name.removeprefix("KB_")] = self._kb_to_gb(row.pop(name))
            results.append(row)
        return results

    def host_monthly_replace(
        self,
        rows: Sequence[DbRow],
        *,
        host_ids: Sequence[int] | None = None,
        reference_months: Sequence[date] | None = None,
    ) -> int:
        """Replace only the selected archive-month scope in one transaction.

        Args:
            rows (Sequence[DbRow]): Complete host_monthly_list_sources rows.
            host_ids (Sequence[int] | None): None selects all hosts; empty none.
            reference_months (Sequence[date] | None): None selects all months;
                empty none. Supplied host/month scopes are intersected.
        Returns:
            int: Number of inserted monthly aggregates.
        Raises:
            ValueError: Rows lie outside the selected host or month scope.
        """
        return self._replace_scope(
            "HOST_MONTHLY_METRIC",
            rows,
            FK_HOST=host_ids,
            DT_REFERENCE_MONTH=reference_months,
        )

    def host_error_list_sources(
        self, *, host_ids: Sequence[int] | None = None
    ) -> list[DbRow]:
        """Read actionable discovery/backup/processing/queue states for grouping.

        Args:
            host_ids (Sequence[int] | None): None reads all hosts, empty none.
        Returns:
            list[DbRow]: FK_HOST and ID_SOURCE_ROW int, NA_SOURCE_TABLE and
                NA_ERROR_SCOPE/NA_TASK_STATE str, DT_EVENT_AT nullable datetime,
                NA_ERROR_CODE/NA_ERROR_SUMMARY/NA_RAW_MESSAGE/NA_ERROR_DETAIL
                nullable str. Messages are normalized by the domain engine.
        """
        task_states = {
            db_constants['tasks']['states']['error']: db_constants['summary']['task_states']['error'],
            db_constants['tasks']['states']['suspended']: db_constants['summary']['task_states']['suspended'],
            db_constants['tasks']['states']['frozen']: db_constants['summary']['task_states']['frozen'],
        }
        queue_scopes = {
            db_constants['tasks']['file_types']['backup']: "BACKUP_QUEUE",
            db_constants['tasks']['file_types']['processing']: "PROCESSING_QUEUE",
        }
        hosts = (
            {"FK_HOST__ne": None}
            if host_ids is None
            else {"FK_HOST__in": tuple(host_ids)}
        )
        events: list[DbRow] = []
        for source in db_constants['summary']['error_sources']:
            table = source["table"]
            identity = source["identity"]
            scope = source["scope"]
            status_field = source["status_column"]
            timestamps = source["timestamps"]
            states = tuple(db_constants["tasks"]["states"][name] for name in source["states"])
            # Queue type chooses the phase; file sources also carry diagnostics.
            optional = {"NU_TYPE"} if table == "FILE_TASK" else set()
            if table != "HOST_TASK":
                optional.update(("NA_ERROR_CODE", "NA_ERROR_DETAIL"))
            columns = sorted(
                {identity, "FK_HOST", status_field, "NA_MESSAGE", *timestamps, *optional}
            )
            rows = self._select_batch(
                table=table,
                where={f"{status_field}__in": states, **hosts},
                columns=columns,
            )
            for row in rows:
                events.append(
                    {
                        "NA_SOURCE_TABLE": table,
                        "ID_SOURCE_ROW": row[identity],
                        # Queue rows report their phase through the task type.
                        "NA_ERROR_SCOPE": queue_scopes.get(
                            cast(int, row.get("NU_TYPE")), scope
                        ),
                        "FK_HOST": row["FK_HOST"],
                        "DT_EVENT_AT": next(
                            (row[name] for name in timestamps if row[name] is not None),
                            None,
                        ),
                        "NA_TASK_STATE": task_states[cast(int, row[status_field])],
                        "NA_ERROR_CODE": row.get("NA_ERROR_CODE"),
                        "NA_ERROR_SUMMARY": row["NA_MESSAGE"],
                        "NA_RAW_MESSAGE": row["NA_MESSAGE"],
                        "NA_ERROR_DETAIL": row.get("NA_ERROR_DETAIL"),
                    }
                )
        return events

    def host_error_replace(
        self,
        rows: Sequence[DbRow],
        *,
        host_ids: Sequence[int] | None = None,
    ) -> int:
        """Publish canonical error/held-state buckets without a server copy.

        Args:
            rows (Sequence[DbRow]): FK_HOST int, NA_ERROR_SCOPE/NA_TASK_STATE str,
                nullable NA_ERROR_CODE str, NA_ERROR_SUMMARY_HASH str SHA-256,
                nullable NA_ERROR_SUMMARY str, NU_ERROR_COUNT int, nullable
                DT_LAST_SEEN_AT datetime and ID_LAST_SOURCE_ROW int.
            host_ids (Sequence[int] | None): None replaces all, empty none.
        Returns:
            int: Number of published buckets.
        Raises:
            ValueError: A row lies outside the selected host scope.
        """
        return self._replace_scope("HOST_ERROR_METRIC", rows, FK_HOST=host_ids)

    def _replace_scope(
        self, table: str, rows: Sequence[DbRow], **scopes: Sequence[object] | None
    ) -> int:
        """Replace one derived scope atomically after checking every new row.

        Each keyword names a scope column and its allowed values; None leaves
        that column unrestricted. Restricted columns are intersected. Without
        any restriction the whole table is rebuilt. An empty value set matches
        no stored row, so an empty selection never becomes a global rebuild.

        Args:
            table (str): Canonical table fixed by the public entity operation.
            rows (Sequence[DbRow]): Homogeneous replacement records.
            **scopes (Sequence[object] | None): Scope column to allowed values.
        Returns:
            int: Number of inserted records.
        Raises:
            ValueError: A row lacks a scope column or lies outside the scope.
        """
        where_conditions: DbRow = {}
        for field, values in scopes.items():
            if values is None:
                continue
            allowed = set(values)
            if any(field not in row or row[field] not in allowed for row in rows):
                raise ValueError("Replacement rows must belong to their deletion scope.")
            where_conditions[f"{field}__in"] = tuple(values)
        with self._unit_of_work():
            if where_conditions:
                self._delete_batch(table=table, where=where_conditions)
            else:
                self._delete_all(table)
            return self._insert_batch(table=table, data=rows)
