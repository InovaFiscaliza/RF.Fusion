"""Persist operational hosts, queues, history, and Summary invalidations.

Keep PostgreSQL statements and workflow persistence in this handler. Workers
still own task claims and finalization; this module does not transfer files or
run discovery. The MariaDB implementation remains unchanged in backup/.
"""

from __future__ import annotations

from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import json
import os
from typing import TYPE_CHECKING, cast

import psycopg
from psycopg import sql
from psycopg.pq import TransactionStatus
from psycopg.types.json import Json

import config as k
from .dbHandlerBase import DbHandler, DbRow
from shared import constants, errors, tools
from shared.file_metadata import FileMetadata

if TYPE_CHECKING:
    from shared import logging_utils


class dbHandlerBKP(DbHandler):

    """Own PostgreSQL persistence for the operational BKP domain.

    Preserve queue selection, host reservation, file identities, and lifecycle
    timestamps while delegating session management to DbHandler. Compound writes
    own a transaction only when the caller has not already opened one. Summary
    publication participates in that transaction through an isolated savepoint.

    Attributes:
        schema (str): Operational schema selected by the database argument.
        log (logging_utils.log): Shared logger supplied by the caller.
        reuse_connection (bool): Retain sessions between worker iterations;
            defaults to True. Session and transaction state belong to DbHandler.
        VALID_FIELDS_HOST (set[str]): Supported host columns.
        VALID_FIELDS_HOST_TASK (set[str]): Supported host-task columns.
        VALID_FIELDS_FILE_TASK (set[str]): Supported live file-task columns.
        VALID_FIELDS_FILE_TASK_HISTORY (set[str]): Supported lifecycle columns.
        PRIMARY_KEY_HOST (tuple[str, ...]): Host primary-key declaration.
        PRIMARY_KEY_HOST_TASK (tuple[str, ...]): Host-task primary key.
        PRIMARY_KEY_FILE_TASK (tuple[str, ...]): File-task primary key.
        PRIMARY_KEY_FILE_TASK_HISTORY (tuple[str, ...]): File-history primary key.
    """

    VALID_FIELDS_HOST = {
        # PRIMARY KEYS & BASIC INFO
        "ID_HOST",
        "NA_HOST_NAME",
        "NA_HOST_ADDRESS",
        "NA_HOST_PORT",
        "NA_HOST_USER",
        "NA_HOST_PASSWORD",
        # TIMESTAMPS
        "DT_LAST_BACKUP",
        "DT_LAST_PROCESSING",
        "DT_LAST_DISCOVERY",
        # CONNECTIVITY + EXECUTION STATE
        "IS_OFFLINE",
        "IS_BUSY",
        "NU_PID",
        "DT_LAST_FAIL",
        "DT_LAST_CHECK",
        "DT_BUSY",
        "DT_LAST_OFFLINE_AT",
        "NA_LAST_OFFLINE_DESCRIPTION",
        # Operational diagnostics
        "NU_HOST_CHECK_ERROR",
    }

    PRIMARY_KEY_HOST = ("ID_HOST",)

    VALID_FIELDS_HOST_TASK = {
        "ID_HOST_TASK",
        "FK_HOST",
        "NU_TYPE",
        "DT_HOST_TASK",
        "NU_STATUS",
        "NU_PID",
        "FILTER",
        "NA_MESSAGE",
    }

    PRIMARY_KEY_HOST_TASK = ("ID_HOST_TASK",)

    VALID_FIELDS_FILE_TASK = {
        "ID_FILE_TASK",
        "FK_HOST",
        "DT_FILE_TASK",
        "NU_TYPE",
        "NA_HOST_FILE_PATH",
        "NA_HOST_FILE_NAME",
        "NA_SERVER_FILE_PATH",
        "NA_SERVER_FILE_NAME",
        "NU_STATUS",
        "NU_PID",
        "NA_EXTENSION_HOST",
        "VL_FILE_SIZE_KB_HOST",
        "DT_FILE_CREATED_HOST",
        "DT_FILE_MODIFIED_HOST",
        "NA_EXTENSION_SERVER",
        "VL_FILE_SIZE_KB_SERVER",
        "DT_FILE_CREATED_SERVER",
        "DT_FILE_MODIFIED_SERVER",
        "NA_MESSAGE",
        "NA_ERROR_CODE",
        "NA_ERROR_DETAIL",
        "NU_ERROR_CLASSIFIER_VERSION",
    }

    PRIMARY_KEY_FILE_TASK = ("ID_FILE_TASK",)

    VALID_FIELDS_FILE_TASK_HISTORY = {
        "ID_HISTORY",
        "FK_HOST",
        "DT_DISCOVERED",
        "DT_BACKUP",
        "DT_PROCESSED",
        "NU_STATUS_DISCOVERY",
        "NU_STATUS_BACKUP",
        "NU_STATUS_PROCESSING",
        "NA_HOST_FILE_PATH",
        "NA_HOST_FILE_NAME",
        "NA_SERVER_FILE_PATH",
        "NA_SERVER_FILE_NAME",
        "NA_EXTENSION_HOST",
        "VL_FILE_SIZE_KB_HOST",
        "DT_FILE_CREATED_HOST",
        "DT_FILE_MODIFIED_HOST",
        "NA_EXTENSION_SERVER",
        "VL_FILE_SIZE_KB_SERVER",
        "DT_FILE_CREATED_SERVER",
        "DT_FILE_MODIFIED_SERVER",
        "NA_MESSAGE",
        "NA_ERROR_CODE",
        "NA_ERROR_DETAIL",
        "NU_ERROR_CLASSIFIER_VERSION",
        "IS_PAYLOAD_DELETED",
        "DT_PAYLOAD_DELETED",
    }

    PRIMARY_KEY_FILE_TASK_HISTORY = ("ID_HISTORY",)

    def __init__(
        self,
        database: str,
        log: logging_utils.log,
        reuse_connection: bool = True,
        *,
        conninfo: str,
    ) -> None:
        """Configure one operational schema without opening a database session.

        Args:
            database (str): Case-sensitive PostgreSQL schema, normally BPDATA.
                The established argument name is retained for domain callers.
            log (logging_utils.log): Shared operational logger.
            reuse_connection (bool): Retain an idle session between operations.
            conninfo (str): Explicit libpq settings including dbname. No MariaDB
                credential or configuration fallback is used.

        Returns:
            None.

        Raises:
            TypeError: If schema or connection settings have an invalid type.
            ValueError: If required connection settings or the schema are missing.
        """
        super().__init__(
            database, log, conninfo=conninfo, reuse_connection=reuse_connection
        )

    # --- HOST ---

    def host_upsert(self, **kwargs: object) -> None:
        """Create or refresh the supplied host fields by its explicit ID_HOST.

        Omitted columns remain unchanged on conflict. Convert operational flags
        to the existing smallint representation and invalidate the host snapshot.

        Args:
            **kwargs (object): Nonempty VALID_FIELDS_HOST column/value pairs;
                ID_HOST is required. IS_BUSY and IS_OFFLINE accept 0/1 or booleans.

        Returns:
            None.

        Raises:
            ValueError: If fields or the required host identity are invalid.
            psycopg.Error: If the host write fails.
        """
        payload = self._host_storage_fields(kwargs)
        with self._unit_of_work():
            self._upsert_row(table="HOST", data=payload, conflict_columns=("ID_HOST",))
            self._summary_publish_host_scope(
                cast(int, payload["ID_HOST"]), reason="host_upsert"
            )

    def host_read_status(self, host_id: int) -> DbRow:
        """Read host state and expose timestamps as UTC epoch seconds.

        Args:
            host_id (int): Explicit host primary key.

        Returns:
            DbRow: All supported host columns plus status=1, or only status=0
            when the host does not exist. Datetimes become integer epoch seconds.

        Raises:
            psycopg.Error: If reading the host fails.
        """
        where_conditions = {"ID_HOST": host_id}
        row = self._select_row(table="HOST", where=where_conditions)
        if row is None:
            return {"status": 0}
        for name, value in row.items():
            if name.startswith("DT_") and isinstance(value, datetime):
                row[name] = int(value.replace(tzinfo=timezone.utc).timestamp())
        row["status"] = 1
        return row

    def host_list_for_connectivity_check(self) -> list[DbRow]:
        """List addressable hosts without excluding offline or busy stations.

        Args:
            None.

        Returns:
            list[DbRow]: Access credentials, flags, and last-check timestamp,
            ordered by online state, never-checked first, oldest check, and ID.

        Raises:
            psycopg.Error: If reading the connectivity snapshot fails.
        """
        where_conditions = {"NA_HOST_ADDRESS__ne": None, "NA_HOST_ADDRESS__trim_ne": ""}
        columns = (
            "ID_HOST",
            "NA_HOST_NAME",
            "NA_HOST_ADDRESS",
            "NA_HOST_PORT",
            "NA_HOST_USER",
            "NA_HOST_PASSWORD",
            "IS_BUSY",
            "IS_OFFLINE",
            "DT_LAST_CHECK",
        )
        order_by = (
            ("IS_OFFLINE", "ASC NULLS FIRST"),
            ("DT_LAST_CHECK", "ASC NULLS FIRST"),
            ("ID_HOST", "ASC"),
        )
        return self._select_batch(
            table="HOST", where=where_conditions, columns=columns, order_by=order_by
        )

    def host_read_last_discovery(self, host_id: int) -> datetime | None:
        """Read the technical discovery watermark without masking database errors.

        Args:
            host_id (int): Host primary key.

        Returns:
            datetime | None: Stored UTC watermark, or None for a missing host
            or an unset watermark.

        Raises:
            psycopg.Error: If reading the watermark fails.
        """
        where_conditions = {"ID_HOST": host_id}
        columns = ("DT_LAST_DISCOVERY",)
        row = self._select_row(table="HOST", where=where_conditions, columns=columns)
        return cast(datetime | None, row["DT_LAST_DISCOVERY"]) if row else None

    def host_release_by_pid(self, pid: int) -> None:
        """Release the busy flag of hosts still associated with one process.

        Args:
            pid (int): Process identity to match in NU_PID.

        Returns:
            None. Matching hosts become free and use HOST_UNLOCKED_PID.

        Raises:
            psycopg.Error: If the conditional release fails.
        """
        data = {"IS_BUSY": 0, "NU_PID": k.HOST_UNLOCKED_PID}
        where_conditions = {"NU_PID": pid}
        self._update_batch(table="HOST", data=data, where=where_conditions)

    def host_update(
        self,
        host_id: int,
        reset: bool = False,
        check_busy_timeout: bool = False,
        busy_timeout_seconds: int = k.HOST_BUSY_TIMEOUT,
        **kwargs: object,
    ) -> None:
        """Update operational host state while preserving established counter deltas.

        Plain positive/negative numbers are deltas unless reset=True; zero is an
        assignment. Flags and timestamps are always assignments. INC/DEC tuples
        request explicit arithmetic. None retains its established no-change meaning.
        A timeout release clears DT_BUSY and uses the unlocked PID.

        Args:
            host_id (int): Host primary key.
            reset (bool): Assign numeric values instead of treating them as deltas.
            check_busy_timeout (bool): Check the current busy age before updating.
            busy_timeout_seconds (int): Maximum busy age for the optional check.
            **kwargs (object): Supported host columns and values; numeric columns
                may receive ("INC" | "DEC", int | float).

        Returns:
            None. Empty effective updates perform no write.

        Raises:
            ValueError: If a field, flag, or arithmetic instruction is invalid.
            psycopg.Error: If reading or updating host state fails.
        """
        if kwargs:
            self._validate_fields(tuple(kwargs), self.VALID_FIELDS_HOST)
        with self._unit_of_work():
            clear_busy = False
            if check_busy_timeout:
                where_conditions = {"ID_HOST": host_id}
                columns = ("IS_BUSY", "DT_BUSY")
                row = self._select_row(
                    table="HOST", where=where_conditions, columns=columns
                )
                clear_busy = bool(
                    row
                    and row["IS_BUSY"]
                    and row["DT_BUSY"]
                    and (
                        datetime.utcnow() - cast(datetime, row["DT_BUSY"])
                    ).total_seconds()
                    > busy_timeout_seconds
                )
            payload = self._host_storage_fields(kwargs)
            if clear_busy:
                payload.update(IS_BUSY=0, DT_BUSY=None, NU_PID=k.HOST_UNLOCKED_PID)
            assignments: list[sql.Composable] = []
            params: list[object] = []
            data: DbRow = {}
            for name, value in payload.items():
                if value is None and not (clear_busy and name == "DT_BUSY"):
                    continue
                column = sql.Identifier(name)
                if isinstance(value, tuple):
                    if (
                        len(value) != 2
                        or value[0] not in ("INC", "DEC")
                        or not isinstance(value[1], (int, float))
                    ):
                        raise ValueError(
                            "Arithmetic updates require (INC or DEC, number)."
                        )
                    symbol = sql.SQL("+" if value[0] == "INC" else "-")
                    assignments.append(
                        sql.SQL("{} = {} {} %s").format(column, column, symbol)
                    )
                    params.append(value[1])
                elif (
                    not reset
                    and name not in ("IS_BUSY", "IS_OFFLINE")
                    and isinstance(value, (int, float))
                    and value != 0
                ):
                    assignments.append(sql.SQL("{} = {} + %s").format(column, column))
                    params.append(value)
                else:
                    data[name] = value
            if not assignments and not data:
                return
            where_conditions = {"ID_HOST": host_id}
            if assignments:
                # Arithmetic must execute in SQL to avoid lost concurrent increments.
                for name, value in data.items():
                    assignments.append(sql.SQL("{} = %s").format(sql.Identifier(name)))
                    params.append(value)
                query = sql.SQL('UPDATE {} SET {} WHERE "ID_HOST" = %s').format(
                    sql.Identifier(self.schema, "HOST"), sql.SQL(", ").join(assignments)
                )
                self._execute(query=query, params=(*params, host_id))
            else:
                self._update_row(table="HOST", data=data, where=where_conditions)
            self._summary_publish_host_scope(host_id, reason="host_update")

    def host_request_summary_refresh(self, host_id: int, *, reason: str) -> None:
        """Invalidate one host's derived models without scheduling a statistics task.

        Args:
            host_id (int): Host whose derived data may be stale.
            reason (str): Short operational reason stored in the outbox.

        Returns:
            None. Publication failures are logged for periodic reconciliation.
        """
        self._summary_publish_host_scope(host_id, reason=reason)

    def host_release_safe(self, host_id: int, current_pid: int) -> None:
        """Release a host lock only when it is safe to do so.

        Decision order:
            1. already free -> no-op
            2. transient cooldown -> preserve
            3. owned by current worker -> release
            4. stale PID -> release
            5. active foreign owner -> preserve

        Args:
            host_id (int): Parent host identity.
            current_pid (int): PID permitted to release its own reservation.

        Returns:
            None. Preserve free hosts, active foreign owners, and transient cooldowns.

        Raises:
            ValueError: If required identity, metadata, or limits are invalid.
            psycopg.Error: If the requested persistence operation fails.
        """
        where_conditions = {"ID_HOST": host_id}
        columns = ("IS_BUSY", "NU_PID", "DT_BUSY")
        row = self._select_row(table="HOST", where=where_conditions, columns=columns)
        if not row or not row["IS_BUSY"] or row["NU_PID"] == k.HOST_TRANSIENT_BUSY_PID:
            return
        pid = cast(int | None, row["NU_PID"])
        if (
            pid == current_pid
            or pid in (None, k.HOST_UNLOCKED_PID)
            or not tools.pid_exists(pid)
        ):
            self.host_release_reservation(
                host_id=host_id,
                owner_pid=pid,
                busy_since=cast(datetime | None, row["DT_BUSY"]),
            )

    def host_list_busy_with_task_state(
        self, *, task_status: int, host_task_type: int
    ) -> list[DbRow]:
        """Read busy hosts with existence flags for the requested running work.

        Args:
            task_status (int): State matched in FILE_TASK and HOST_TASK.
            host_task_type (int): HOST_TASK type included in the existence check.

        Returns:
            list[DbRow]: ID_HOST, DT_BUSY, NU_PID, FILE_RUNNING,
            and HOST_PROCESSING_RUNNING. Timestamps and nullable PIDs are native
            database values; existence flags are booleans. No liveness checks,
            timeout decisions, or writes are performed.

        Raises:
            psycopg.Error: If the maintenance snapshot cannot be read.
        """
        return self._select_raw(
            query=sql.SQL(
                """
                SELECT
                    H."ID_HOST",
                    H."DT_BUSY",
                    H."NU_PID",
                    EXISTS(
                        SELECT 1
                        FROM {schema}."FILE_TASK" FT
                        WHERE FT."FK_HOST" = H."ID_HOST"
                        AND FT."NU_STATUS" = %s
                    ) AS "FILE_RUNNING",
                    EXISTS(
                        SELECT 1
                        FROM {schema}."HOST_TASK" HT
                        WHERE HT."FK_HOST" = H."ID_HOST"
                        AND HT."NU_STATUS" = %s
                        AND HT."NU_TYPE" = %s
                    ) AS "HOST_PROCESSING_RUNNING"
                FROM {schema}."HOST" H
                WHERE H."IS_BUSY" = 1
            """
            ).format(
                schema=sql.Identifier(self.schema),
            ),
            params=(task_status, task_status, host_task_type),
        )

    def _host_storage_fields(self, data: Mapping[str, object]) -> DbRow:
        """Normalize only host flags while retaining the remaining update values.

        Args:
            data (Mapping[str, object]): Host values already authorized by the
                caller or subsequently checked by the CRUD builder.

        Returns:
            DbRow: Copy with non-null IS_BUSY/IS_OFFLINE represented as integers.

        Raises:
            ValueError: If a supplied operational flag is invalid.
        """
        return {
            name: self._storage_flag(value)
            if name in ("IS_BUSY", "IS_OFFLINE") and value is not None
            else value
            for name, value in data.items()
        }

    def _host_release_expired_cooldowns(
        self, cooldown_seconds: int = k.SFTP_BUSY_COOLDOWN_SECONDS
    ) -> int:
        """Release only expired reservations owned by the transient-cooldown sentinel.

        Args:
            cooldown_seconds (int): Minimum busy age before release.

        Returns:
            int: Released host count; unrelated active worker reservations are preserved.

        Raises:
            psycopg.Error: If the conditional release fails.
        """
        data = {"IS_BUSY": 0, "DT_BUSY": None, "NU_PID": k.HOST_UNLOCKED_PID}
        where_conditions = {
            "IS_BUSY": 1,
            "NU_PID": k.HOST_TRANSIENT_BUSY_PID,
            "DT_BUSY__lt": datetime.utcnow() - timedelta(seconds=cooldown_seconds),
        }
        return self._update_batch(
            table="HOST",
            data=data,
            where=where_conditions,
        )

    def host_release_reservation(
        self, host_id: int, owner_pid: int | None, busy_since: datetime | None
    ) -> bool:
        """Release only the reservation inspected by a safe-release or janitor pass.

        Args:
            host_id (int): Host primary key.
            owner_pid (int | None): Owner observed during the eligibility check.
            busy_since (datetime | None): Observed reservation timestamp; compare
                both fields so a newer reservation cannot be released by stale data.

        Returns:
            bool: True when that exact reservation was released; False when the
            host changed after selection. A successful release invalidates its snapshot.

        Raises:
            psycopg.Error: If the conditional update fails.
        """
        with self._unit_of_work():
            data = {"IS_BUSY": 0, "NU_PID": k.HOST_UNLOCKED_PID}
            where_conditions = {
                "ID_HOST": host_id,
                "IS_BUSY": 1,
                "NU_PID": owner_pid,
                "DT_BUSY": busy_since,
            }
            affected = self._update_batch(
                table="HOST",
                data=data,
                where=where_conditions,
            )
            if affected:
                self._summary_publish_host_scope(
                    host_id, reason="host_reservation_released"
                )
            return affected == 1

    def _host_reserve(self, host_id: int, require_online: bool) -> bool:
        """Acquire a free host through one conditional update without a blind commit.

        Args:
            host_id (int): Candidate host primary key.
            require_online (bool): Also require IS_OFFLINE=0 when reserving.

        Returns:
            bool: True only for one matched free host; False is an expected race loss.

        Raises:
            psycopg.Error: If reservation SQL fails.
        """
        where_conditions: DbRow = {"ID_HOST": host_id, "IS_BUSY": 0}
        if require_online:
            where_conditions["IS_OFFLINE"] = 0
        data = {"IS_BUSY": 1, "DT_BUSY": datetime.utcnow(), "NU_PID": os.getpid()}
        return (
            self._update_batch(
                table="HOST",
                data=data,
                where=where_conditions,
            )
            == 1
        )

    def _host_update_phase_markers_from_history(
        self, *, host_id: int, history_update: Mapping[str, object]
    ) -> None:
        """Advance dated successful phase markers without overwriting newer history.

        Args:
            host_id (int): Parent host primary key.
            history_update (Mapping[str, object]): Explicit phase statuses and
                completion timestamps supplied to the history update.

        Returns:
            None. Only DONE phases with a non-null completion timestamp advance.

        Raises:
            psycopg.Error: If updating the parent's markers fails.
        """
        assignments: list[sql.Composable] = []
        params: list[object] = []
        for phase, source, target in (
            ("NU_STATUS_BACKUP", "DT_BACKUP", "DT_LAST_BACKUP"),
            ("NU_STATUS_PROCESSING", "DT_PROCESSED", "DT_LAST_PROCESSING"),
        ):
            if (
                history_update.get(phase) == k.TASK_DONE
                and history_update.get(source) is not None
            ):
                column = sql.Identifier(target)
                assignments.append(
                    sql.SQL(
                        "{} = CASE WHEN {} IS NULL OR {} < %s THEN %s ELSE {} END"
                    ).format(column, column, column, column)
                )
                params.extend((history_update[source], history_update[source]))
        if assignments:
            self._execute(
                query=sql.SQL('UPDATE {} SET {} WHERE "ID_HOST" = %s').format(
                    sql.Identifier(self.schema, "HOST"), sql.SQL(", ").join(assignments)
                ),
                params=(*params, host_id),
            )

    # --- HOST_TASK ---

    def host_task_list(self, **kwargs: object) -> list[DbRow]:
        """Read host tasks with exact or membership filters and stable legacy keys.

        Args:
            **kwargs (object): Supported HOST_TASK columns mapped to a scalar
                equality value or a list/tuple/set of allowed values.

        Returns:
            list[DbRow]: Rows keyed as HOST_TASK__COLUMN, newest task ID first.

        Raises:
            ValueError: If a requested field is unsupported.
            psycopg.Error: If task lookup fails.
        """
        if kwargs:
            self._validate_fields(tuple(kwargs), self.VALID_FIELDS_HOST_TASK)
        where_conditions = {
            f"{name}__in"
            if isinstance(value, (list, tuple, set))
            else name: tuple(value)
            if isinstance(value, (list, tuple, set))
            else value
            for name, value in kwargs.items()
        }
        order_by = (("ID_HOST_TASK", "DESC"),)
        rows = self._select_batch(
            table="HOST_TASK", where=where_conditions, order_by=order_by
        )
        return [
            {f"HOST_TASK__{name}": value for name, value in row.items()} for row in rows
        ]

    def host_task_create(self, **kwargs: object) -> int:
        """Insert a host task with canonical JSON and return its generated identity.

        Args:
            **kwargs (object): Supported HOST_TASK fields. Defaults are PENDING,
                the current UTC timestamp, a neutral FILTER, and an initial message.

        Returns:
            int: ID_HOST_TASK obtained with RETURNING, without sequence inference.

        Raises:
            ValueError: If fields or the JSON filter are invalid.
            psycopg.Error: If task creation fails.
        """
        if kwargs:
            self._validate_fields(tuple(kwargs), self.VALID_FIELDS_HOST_TASK)
        payload = dict(kwargs)
        payload.setdefault("DT_HOST_TASK", datetime.utcnow())
        payload.setdefault("NU_STATUS", k.TASK_PENDING)
        payload.setdefault(
            "NA_MESSAGE",
            f"New HOST_TASK created for host {payload.get('FK_HOST', 'UNKNOWN')}",
        )
        payload["FILTER"] = Json(
            json.loads(self._host_task_serialize_filter(payload.get("FILTER")))
        )
        with self._unit_of_work():
            row = self._insert_row(
                table="HOST_TASK", data=payload, returning=("ID_HOST_TASK",)
            )
            if row is None:
                raise RuntimeError("HOST_TASK insert returned no identity.")
            self._summary_publish_host_scope(
                cast(int | None, payload.get("FK_HOST")), reason="host_task_create"
            )
            return cast(int, row["ID_HOST_TASK"])

    def host_task_read(
        self,
        task_id: int | None = None,
        task_status: int | None = k.TASK_PENDING,
        task_type: int | list[int] | None = None,
        check_host_busy: bool = False,
        check_host_offline: bool = False,
        lock_host: bool = False,
    ) -> DbRow | None:
        """Select one host task and optionally reserve its currently available host.

        Args:
            task_id (int | None): Explicit ID bypassing queue eligibility filters.
            task_status (int | None): Required queue status when no ID is given.
            task_type (int | list[int] | None): Optional task type or type set.
            check_host_busy (bool): Require a free host and expire old cooldowns.
            check_host_offline (bool): Require an online host.
            lock_host (bool): Reserve the host when check_host_busy is also True.

        Returns:
            DbRow | None: HOST_TASK__* and HOST__* fields plus decoded host_filter,
            or None when no candidate exists or its host reservation was lost.
            This method does not claim the task's NU_STATUS.

        Raises:
            psycopg.Error: If selection or conditional host reservation fails.
        """
        if check_host_busy:
            self._host_release_expired_cooldowns()
        conditions: list[sql.Composable] = []
        params: list[object] = []
        if task_id:
            conditions.append(sql.SQL('t."ID_HOST_TASK" = %s'))
            params.append(task_id)
        else:
            conditions.append(
                sql.SQL(
                    't."NU_STATUS" IS NULL'
                    if task_status is None
                    else 't."NU_STATUS" = %s'
                )
            )
            if task_status is not None:
                params.append(task_status)
            if isinstance(task_type, list):
                conditions.append(sql.SQL('t."NU_TYPE" = ANY(%s::smallint[])'))
                params.append(task_type)
            elif task_type is not None:
                conditions.append(sql.SQL('t."NU_TYPE" = %s'))
                params.append(task_type)
            if check_host_busy:
                conditions.append(sql.SQL('h."IS_BUSY" = 0'))
            if check_host_offline:
                conditions.append(sql.SQL('h."IS_OFFLINE" = 0'))
        row = self._select_task_with_host(
            table="HOST_TASK",
            conditions=conditions,
            params=params,
            order_by=sql.SQL('t."DT_HOST_TASK" ASC NULLS FIRST, t."ID_HOST_TASK" ASC'),
        )
        if row is None or (
            lock_host
            and check_host_busy
            and not self._host_reserve(
                cast(int, row["HOST__ID_HOST"]), check_host_offline
            )
        ):
            return None
        raw = row.get("HOST_TASK__FILTER")
        try:
            decoded = (
                json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
            )
        except (ValueError, UnicodeDecodeError):
            decoded = None
        row["host_filter"] = decoded if isinstance(decoded, dict) else {}
        return row

    def host_task_update(
        self,
        task_id: int | None = None,
        where_dict: Mapping[str, object] | None = None,
        expected_status: int | None = None,
        **kwargs: object,
    ) -> DbRow:
        """Update host tasks, optionally using a status comparison for a safe claim.

        Args:
            task_id (int | None): Task primary key when where_dict is absent.
            where_dict (Mapping[str, object] | None): Explicit supported filters;
                copied before adding expected_status so caller data is not mutated.
            expected_status (int | None): Required current NU_STATUS.
            **kwargs (object): Supported replacements. FILTER is canonical JSON;
                PENDING clears NU_PID and RUNNING defaults it to the logger PID.

        Returns:
            DbRow: success, rows_affected, and updated_fields. A lost status claim
            returns success=True and rows_affected=0; empty updates return False.

        Raises:
            ValueError: If identity, fields, filters, or JSON are invalid.
            psycopg.Error: If task update fails.
        """
        if task_id is None and not where_dict:
            raise ValueError(
                "host_task_update requires task_id or nonempty where_dict."
            )
        payload = dict(kwargs)
        if not payload:
            return {"success": False, "rows_affected": 0, "updated_fields": {}}
        self._validate_fields(tuple(payload), self.VALID_FIELDS_HOST_TASK)
        if payload.get("NU_STATUS") == k.TASK_PENDING:
            payload["NU_PID"] = None
        elif payload.get("NU_STATUS") == k.TASK_RUNNING:
            payload.setdefault("NU_PID", self.log.pid)
        if "NA_MESSAGE" in payload:
            payload["NA_MESSAGE"] = errors.canonicalize_persisted_error_message(
                cast(str | None, payload["NA_MESSAGE"])
            )
        storage = dict(payload)
        if "FILTER" in payload:
            payload["FILTER"] = self._host_task_serialize_filter(payload["FILTER"])
            storage["FILTER"] = Json(json.loads(cast(str, payload["FILTER"])))
        where_conditions = dict(where_dict) if where_dict else {"ID_HOST_TASK": task_id}
        if expected_status is not None:
            where_conditions["NU_STATUS"] = expected_status
        with self._unit_of_work():
            affected = self._update_batch(
                table="HOST_TASK", data=storage, where=where_conditions
            )
            host_id = cast(int | None, where_conditions.get("FK_HOST"))
            if affected and task_id is not None:
                host_id = self._task_read_host_id(table="HOST_TASK", task_id=task_id)
            if affected:
                self._summary_publish_host_scope(host_id, reason="host_task_update")
            return {
                "success": True,
                "rows_affected": affected,
                "updated_fields": payload,
            }

    def host_task_suspend_by_host(self, host_id: int) -> None:
        """Suspend host-dependent pending/running host tasks after connectivity loss.

        Args:
            host_id (int): Host whose CHECK, PROCESSING, and CONNECTION tasks depend
                on connectivity. Statistics and backlog task types are excluded.

        Returns:
            None. Affected tasks use SUSPENDED and keep their existing identity.

        Raises:
            psycopg.Error: If task suspension fails.
        """
        with self._unit_of_work():
            data = {
                "NU_STATUS": k.TASK_SUSPENDED,
                "NA_MESSAGE": "Host unreachable. Tasks suspended automatically.",
            }
            where_conditions = {
                "FK_HOST": host_id,
                "NU_STATUS__in": (k.TASK_PENDING, k.TASK_RUNNING),
                "NU_TYPE__in": k.HOST_TASK_CONNECTIVITY_TYPES,
            }
            affected = self._update_batch(
                table="HOST_TASK",
                data=data,
                where=where_conditions,
            )
            if affected:
                self._summary_publish_host_scope(
                    host_id, reason="host_task_suspend_by_host"
                )

    def host_task_resume_by_host(
        self, host_id: int, busy_timeout_seconds: int = k.HOST_BUSY_TIMEOUT
    ) -> None:
        """Resume eligible host tasks without changing their type or reviving FROZEN.

        Args:
            host_id (int): Recovered host identity.
            busy_timeout_seconds (int): Minimum age for reclaiming RUNNING tasks.

        Returns:
            None. SUSPENDED, ERROR, and stale RUNNING host-dependent tasks become
            PENDING and clear NU_PID. Statistics tasks remain untouched.

        Raises:
            psycopg.Error: If any part of the resumption fails.
        """
        base = {"FK_HOST": host_id, "NU_TYPE__in": k.HOST_TASK_CONNECTIVITY_TYPES}
        conditions = (
            {"NU_STATUS": k.TASK_SUSPENDED},
            {"NU_STATUS": k.TASK_ERROR},
            {
                "NU_STATUS": k.TASK_RUNNING,
                "DT_HOST_TASK__lt": datetime.utcnow()
                - timedelta(seconds=busy_timeout_seconds),
            },
        )
        data = {
            "NU_STATUS": k.TASK_PENDING,
            "NU_PID": None,
            "NA_MESSAGE": "Host reachable again — task resumed automatically",
        }
        with self._unit_of_work():
            affected = 0
            for condition in conditions:
                where_conditions = {**base, **condition}
                affected += self._update_batch(
                    table="HOST_TASK", data=data, where=where_conditions
                )
            if affected:
                self._summary_publish_host_scope(
                    host_id, reason="host_task_resume_by_host"
                )

    def host_task_list_with_host_state(
        self, *, task_types: Sequence[int], task_statuses: Sequence[int]
    ) -> list[DbRow]:
        """Read selected host tasks and their host reservation in one snapshot.

        Args:
            task_types (Sequence[int]): Task types to include; empty matches none.
            task_statuses (Sequence[int]): Task states to include; empty matches none.

        Returns:
            list[DbRow]: ID_HOST_TASK, FK_HOST, NU_STATUS, DT_HOST_TASK,
            NU_PID, IS_BUSY, HOST_OWNER_PID, and DT_BUSY. Nullable timestamps and
            PIDs retain their database values. Recovery decisions belong to the caller.

        Raises:
            psycopg.Error: If reading tasks and host reservations fails.
        """
        query = sql.SQL(
            """
            SELECT HT."ID_HOST_TASK", HT."FK_HOST", HT."NU_STATUS",
                   HT."DT_HOST_TASK", HT."NU_PID", H."IS_BUSY",
                   H."NU_PID" AS "HOST_OWNER_PID", H."DT_BUSY"
            FROM {schema}."HOST_TASK" HT
            JOIN {schema}."HOST" H ON H."ID_HOST" = HT."FK_HOST"
            WHERE HT."NU_TYPE" = ANY(%s::smallint[])
              AND HT."NU_STATUS" = ANY(%s::smallint[])
            """
        ).format(schema=sql.Identifier(self.schema))
        params = (list(task_types), list(task_statuses))
        return self._select_raw(query=query, params=params)

    def host_task_enqueue(
        self, host_id: int, task_type: int, task_status: int, filter_dict: DbRow
    ) -> DbRow:
        """Deterministically enqueue or refresh the singleton HOST_TASK row for one type.

        Queue contract:
            - one durable row per `FK_HOST + NU_TYPE`
            - RUNNING rows are preserved
            - PENDING or terminal rows are refreshed in place
            - a new row is created only when none exists for that type

        The summary outbox is invalidated after the queue mutation. Derived
        counters are never recalculated into ``BPDATA.HOST``.

        Args:
            host_id (int): Parent host identity.
            task_type (int): Singleton task type within the host.
            task_status (int): Target state for a new or reusable non-running task.
            filter_dict (dict[str, object]): Canonical shared filter payload.

        Returns:
            DbRow: Current host status. Existing RUNNING tasks are preserved without replacement.

        Raises:
            ValueError: If required identity, metadata, or limits are invalid.
            psycopg.Error: If the requested persistence operation fails.
        """
        tasks = self.host_task_list(FK_HOST=host_id, NU_TYPE=task_type)
        existing = self._host_task_find_reusable(tasks)
        match_count = len(tasks)
        if match_count > 1:
            self.log.warning_event(
                "db_duplicate_singleton_rows",
                operation="host_task_enqueue",
                table="HOST_TASK",
                host_id=host_id,
                task_type=task_type,
                matches=match_count,
            )
        if existing:
            status = existing["HOST_TASK__NU_STATUS"]
            if status == k.TASK_RUNNING:
                self.log.warning_event(
                    "db_running_singleton_preserved",
                    operation="host_task_enqueue",
                    table="HOST_TASK",
                    host_id=host_id,
                    task_id=existing["HOST_TASK__ID_HOST_TASK"],
                    task_type=task_type,
                )
            else:
                self.host_task_update(
                    task_id=existing["HOST_TASK__ID_HOST_TASK"],
                    FILTER=filter_dict,
                    NU_TYPE=task_type,
                    NU_STATUS=task_status,
                    DT_HOST_TASK=datetime.utcnow(),
                    NA_MESSAGE=f"HOST_TASK refreshed by host_task_enqueue (type={task_type}, previous status={status})",
                )
        else:
            self.host_task_create(
                NU_TYPE=task_type,
                NU_STATUS=task_status,
                FK_HOST=host_id,
                FILTER=filter_dict,
            )
        self.host_request_summary_refresh(host_id, reason="host_task_enqueue")
        return self.host_read_status(host_id)

    @staticmethod
    def _host_task_serialize_filter(filter_value: object) -> str:
        """Canonicalize the JSON object used by host-task scheduling and responses.

        Args:
            filter_value (object): Dict, JSON object text/bytes, or None for NONE_FILTER.

        Returns:
            str: JSON object text with sorted keys and preserved Unicode.

        Raises:
            ValueError: If decoding fails or the JSON value is not an object.
        """
        if filter_value is None:
            normalized = dict(k.NONE_FILTER)
        elif isinstance(filter_value, dict):
            normalized = filter_value
        elif isinstance(filter_value, (str, bytes, bytearray)):
            try:
                normalized = json.loads(filter_value)
            except (ValueError, UnicodeDecodeError) as error:
                raise ValueError("FILTER must contain a valid JSON object.") from None
        else:
            raise ValueError("FILTER must contain a JSON object.")
        if not isinstance(normalized, dict):
            raise ValueError("FILTER must contain a JSON object.")
        try:
            return json.dumps(normalized, sort_keys=True, ensure_ascii=False)
        except (TypeError, ValueError):
            raise ValueError("FILTER must contain JSON-serializable values.") from None

    def _host_task_find_reusable(self, tasks: Sequence[DbRow]) -> DbRow | None:
        """Pick the best reusable row for the singleton-per-type HOST_TASK contract.

        Preference order:
            1. PENDING task
            2. RUNNING task
            3. Otherwise the newest terminal row

        Args:
            tasks (Sequence[DbRow]): Newest-first rows keyed with HOST_TASK__ prefixes.

        Returns:
            DbRow | None: First PENDING row, then first RUNNING row, then the
            newest terminal row. None means there is no reusable task.
        """
        for status in (k.TASK_PENDING, k.TASK_RUNNING):
            for task in tasks:
                if task.get("HOST_TASK__NU_STATUS") == status:
                    return task
        return tasks[0] if tasks else None

    # --- FILE_TASK ---

    def file_task_read(
        self,
        task_id: int | None = None,
        task_status: int | None = k.TASK_PENDING,
        task_type: int | None = None,
        check_host_busy: bool = True,
        check_host_offline: bool = False,
        extension: str | None = None,
        lock_host: bool = False,
        reserve_hosts_for_discovery: bool = False,
        fair_by_host: bool = False,
        newest_first: bool = False,
        prefer_current_year: bool = False,
    ) -> tuple[DbRow, int, int] | None:
        """Select one eligible file task while preserving backup scheduling policy.

        Respect discovery reservations and optionally rotate backup hosts by their
        last backup marker. Current-year preference and source-date ordering use
        explicit PostgreSQL NULL ordering. Host reservation is conditional; task
        claims remain the worker's responsibility through expected_status.

        Args:
            task_id (int | None): Explicit task ID bypassing queue filters/order.
            task_status (int | None): Queue state, or None for all states.
            task_type (int | None): Queue type, or None for all types.
            check_host_busy (bool): Require a free host and expire cooldowns.
            check_host_offline (bool): Require an online host.
            extension (str | None): Exact source extension filter.
            lock_host (bool): Reserve the host if busy checking is enabled.
            reserve_hosts_for_discovery (bool): Keep reserved discovery hosts out
                of backup selection until the configured reservation TTL expires.
            fair_by_host (bool): Select one candidate per backup host, then rotate
                hosts by DT_LAST_BACKUP, including never-backed-up hosts first.
            newest_first (bool): Prefer newest source date and task ID.
            prefer_current_year (bool): Prefer current-year files in fair backup mode.

        Returns:
            tuple[DbRow, int, int] | None: Prefixed task/host fields, host ID, and
            task ID, or None when no eligible candidate can reserve its host.

        Raises:
            psycopg.Error: If selection or conditional reservation fails.
        """
        if check_host_busy:
            self._host_release_expired_cooldowns()
        conditions: list[sql.Composable] = []
        params: list[object] = []
        fair_backup = (
            not task_id and task_type == k.FILE_TASK_BACKUP_TYPE and fair_by_host
        )
        current_year = fair_backup and prefer_current_year
        if task_id:
            conditions.append(sql.SQL('t."ID_FILE_TASK" = %s'))
            params.append(task_id)
        else:
            for name, value in (
                ("NU_STATUS", task_status),
                ("NU_TYPE", task_type),
                ("NA_EXTENSION_HOST", extension),
            ):
                if value is not None:
                    conditions.append(sql.SQL("t.{} = %s").format(sql.Identifier(name)))
                    params.append(value)
            if check_host_busy:
                conditions.append(sql.SQL('h."IS_BUSY" = 0'))
            if check_host_offline:
                conditions.append(sql.SQL('h."IS_OFFLINE" = 0'))
            if task_type == k.FILE_TASK_BACKUP_TYPE and reserve_hosts_for_discovery:
                conditions.append(
                    sql.SQL(
                        """
                        NOT EXISTS (
                            SELECT 1 FROM {} reservation
                            WHERE reservation."FK_HOST" = t."FK_HOST"
                              AND reservation."NU_TYPE" IN (%s, %s)
                              AND (
                                  reservation."NU_STATUS" = %s OR (
                                      reservation."NU_STATUS" = %s AND (
                                          reservation."DT_HOST_TASK" IS NULL OR
                                          reservation."DT_HOST_TASK" >=
                                              (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                                              - %s * INTERVAL '1 second'
                                      )
                                  )
                              )
                        )
                    """
                    ).format(sql.Identifier(self.schema, "HOST_TASK"))
                )
                params.extend(
                    (
                        k.HOST_TASK_CHECK_TYPE,
                        k.HOST_TASK_PROCESSING_TYPE,
                        k.TASK_RUNNING,
                        k.TASK_PENDING,
                        k.DISCOVERY_RESERVATION_TTL_SEC,
                    )
                )
            if fair_backup:
                conditions.append(
                    sql.SQL(
                        """
                        t."ID_FILE_TASK" = (
                            SELECT candidate."ID_FILE_TASK" FROM {} candidate
                            WHERE candidate."FK_HOST" = t."FK_HOST"
                              AND candidate."NU_TYPE" = t."NU_TYPE"
                              AND (%s OR candidate."NU_STATUS" = t."NU_STATUS")
                              AND (%s OR candidate."NA_EXTENSION_HOST" = t."NA_EXTENSION_HOST")
                            ORDER BY {} LIMIT 1
                        )
                    """
                    ).format(
                        sql.Identifier(self.schema, "FILE_TASK"),
                        self._file_task_build_order_by(
                            "candidate", newest_first, current_year
                        ),
                    )
                )
                params.extend((task_status is None, extension is None))
        file_order = self._file_task_build_order_by("t", newest_first, False)
        if fair_backup:
            parts = [sql.SQL('h."DT_LAST_BACKUP" ASC NULLS FIRST')]
            if current_year:
                # The year bucket must precede host rotation, as in the original scheduler.
                parts.insert(0, self._file_task_build_current_year_order("t"))
            order = sql.SQL(", ").join((*parts, file_order))
        else:
            order = (
                file_order
                if newest_first and not task_id
                else sql.SQL('t."ID_FILE_TASK" ASC')
            )
        row = self._select_task_with_host(
            table="FILE_TASK", conditions=conditions, params=params, order_by=order
        )
        if row is None:
            return None
        host_id = cast(int, row["HOST__ID_HOST"])
        if (
            lock_host
            and check_host_busy
            and not self._host_reserve(host_id, check_host_offline)
        ):
            return None
        return row, host_id, cast(int, row["FILE_TASK__ID_FILE_TASK"])

    def file_task_create(
        self,
        host_id: int,
        task_type: int,
        task_status: int,
        file_metadata: Sequence[FileMetadata],
    ) -> int:
        """Upsert homogeneous source-file records into the live queue atomically.

        Args:
            host_id (int): Parent host ID.
            task_type (int): Queue type assigned to each supplied artifact.
            task_status (int): Queue state assigned to each supplied artifact.
            file_metadata (Sequence[FileMetadata]): Source metadata; empty input
                returns zero without opening a connection.

        Returns:
            int: PostgreSQL inserted/updated row count across all chunks.

        Raises:
            psycopg.Error: If a queue write fails; standalone batches roll back.
        """
        if not file_metadata:
            return 0
        now = datetime.utcnow()
        rows = [
            self._merge_structured_error_fields(
                {
                    **self._file_metadata_payload(file),
                    "FK_HOST": host_id,
                    "NU_TYPE": task_type,
                    "NU_STATUS": task_status,
                    "NU_PID": os.getpid(),
                    "DT_FILE_TASK": now,
                    "NA_MESSAGE": tools.compose_message(
                        task_type, task_status, file.NA_PATH, file.NA_FILE
                    ),
                }
            )
            for file in file_metadata
        ]
        with self._unit_of_work():
            count = self._file_upsert_batch(table="FILE_TASK", data=rows)
            self._summary_publish_host_scope(host_id, reason="file_task_create")
            return count

    def file_task_update(
        self,
        *,
        task_id: int | None = None,
        host_id: int | None = None,
        host_file_path: str | None = None,
        host_file_name: str | None = None,
        server_file_name: str | None = None,
        expected_status: int | None = None,
        publish_summary: bool = True,
        **kwargs: object,
    ) -> DbRow:
        """Update identified file tasks with optional optimistic state matching.

        Args:
            task_id (int | None): Explicit queue primary key.
            host_id (int | None): Host used with a source identity or server name.
            host_file_path (str | None): Source path, paired with source filename.
            host_file_name (str | None): Source filename, paired with source path.
            server_file_name (str | None): Alternate name identity within a host.
            expected_status (int | None): Required current state for a safe claim.
            publish_summary (bool): Publish a host invalidation after a matched write.
            **kwargs (object): Supported replacement fields. None stores SQL NULL;
                PENDING/RUNNING set NU_PID only when it was not supplied explicitly.

        Returns:
            DbRow: success, rows_affected, updated_fields, and where_used; empty
            updates return success=False with zero affected rows. No timestamps
            are injected into update data.

        Raises:
            ValueError: If fields or the identity strategy are invalid.
            psycopg.Error: If the conditional update fails.
        """
        if not kwargs:
            return {"success": False, "rows_affected": 0, "updated_fields": {}}
        self._validate_fields(tuple(kwargs), self.VALID_FIELDS_FILE_TASK)
        payload = self._merge_structured_error_fields(kwargs)
        if payload.get("NU_STATUS") == k.TASK_PENDING:
            payload.setdefault("NU_PID", None)
        elif payload.get("NU_STATUS") == k.TASK_RUNNING:
            payload.setdefault("NU_PID", self.log.pid)
        where_conditions = self._file_identity_filter(
            "ID_FILE_TASK",
            task_id,
            host_id,
            host_file_path,
            host_file_name,
            server_file_name,
        )
        if expected_status is not None:
            where_conditions["NU_STATUS"] = expected_status
        with self._unit_of_work():
            affected = self._update_batch(
                table="FILE_TASK", data=payload, where=where_conditions
            )
            if affected and publish_summary:
                scope = (
                    host_id
                    if host_id is not None
                    else self._task_read_host_id(
                        table="FILE_TASK", task_id=cast(int, task_id)
                    )
                )
                self._summary_publish_host_scope(scope, reason="file_task_update")
            return {
                "success": True,
                "rows_affected": affected,
                "updated_fields": payload,
                "where_used": where_conditions,
            }

    def file_task_delete(self, task_id: int) -> int:
        """Delete one live queue row and invalidate its host when a row existed.

        Args:
            task_id (int): Queue primary key.

        Returns:
            int: Deleted row count, zero or one.

        Raises:
            psycopg.Error: If deletion fails. Caller transactions remain caller-owned.
        """
        with self._unit_of_work():
            host_id = self._task_read_host_id(table="FILE_TASK", task_id=task_id)
            where_conditions = {"ID_FILE_TASK": task_id}
            affected = self._delete_row(table="FILE_TASK", where=where_conditions)
            if affected:
                self._summary_publish_host_scope(host_id, reason="file_task_delete")
            return affected

    def file_task_list_for_backlog(
        self,
        *,
        where: Mapping[str, object],
        order_by: Sequence[tuple[str, str]],
        limit: int | None = None,
    ) -> list[DbRow]:
        """Lock a filtered, ordered queue slice without choosing its volume budget.

        Args:
            where (Mapping[str, object]): Prepared FILE_TASK filters, including host.
            order_by (Sequence[tuple[str, str]]): Validated column/direction pairs
                using the base's closed ordering vocabulary.
            limit (int | None): Optional maximum selected rows.

        Returns:
            list[DbRow]: ID_FILE_TASK and VL_FILE_SIZE_KB_HOST for each selected
            row, in requested order. Locks last until caller commit or rollback.

        Raises:
            ValueError: If filters, ordering, or limit are invalid.
            RuntimeError: If no explicit transaction is active.
            psycopg.Error: If reading or locking rows fails.
        """
        columns = ("ID_FILE_TASK", "VL_FILE_SIZE_KB_HOST")
        return self._select_batch(
            table="FILE_TASK",
            where=where,
            columns=columns,
            order_by=order_by,
            limit=limit,
            for_update=True,
        )

    def file_task_update_backlog(
        self,
        *,
        data: Mapping[str, object],
        where: Mapping[str, object],
        message_prefix: str,
    ) -> int:
        """Apply prepared queue values and compose each row's source-file message.

        Args:
            data (Mapping[str, object]): Nonempty literal FILE_TASK replacements;
                NA_MESSAGE must be absent because it is composed for each row.
            where (Mapping[str, object]): Nonempty original filters, optionally
                restricted to IDs selected and locked by the caller.
            message_prefix (str): Bound text preceding ' of file path/name'.

        Returns:
            int: PostgreSQL matched-row count. This method does not choose
            transitions, budgets, transaction boundaries, or Summary publication.

        Raises:
            ValueError: If fields/filters are invalid or NA_MESSAGE is supplied.
            psycopg.Error: If writing the selected rows fails.
        """
        if not where or not data or "NA_MESSAGE" in data:
            raise ValueError(
                "Backlog update requires filters and data without NA_MESSAGE."
            )
        target, fields = self._get_table_definition("FILE_TASK")
        self._validate_fields(tuple(data), fields)
        assignments = [sql.SQL("{} = %s").format(sql.Identifier(name)) for name in data]
        assignments.append(
            sql.SQL(
                """"NA_MESSAGE" = CONCAT(%s, ' of file ', "NA_HOST_FILE_PATH", '/', "NA_HOST_FILE_NAME")"""
            )
        )
        filters, values = self._build_where_clause(where, fields)
        query = (
            sql.SQL("UPDATE {} SET {}").format(target, sql.SQL(", ").join(assignments))
            + filters
        )
        params = (*data.values(), message_prefix, *values)
        return self._execute(query=query, params=params)

    def file_task_suspend_by_host(
        self, host_id: int, reason: str | None = None
    ) -> None:
        """Suspend discovery/backup tasks that require an unreachable host.

        Args:
            host_id (int): Parent host ID.
            reason (str | None): Optional message; the default describes host loss.

        Returns:
            None. Only PENDING/RUNNING host-dependent tasks are suspended.

        Raises:
            psycopg.Error: If suspension fails.
        """
        with self._unit_of_work():
            payload = self._merge_structured_error_fields(
                {
                    "NU_STATUS": k.TASK_SUSPENDED,
                    "NA_MESSAGE": reason
                    or "Host unreachable — HOST-dependent file task suspended by host_check service",
                }
            )
            where_conditions = {
                "FK_HOST": host_id,
                "NU_STATUS__in": (k.TASK_PENDING, k.TASK_RUNNING),
                "NU_TYPE__in": (k.FILE_TASK_DISCOVERY, k.FILE_TASK_BACKUP_TYPE),
            }
            affected = self._update_batch(
                table="FILE_TASK",
                data=payload,
                where=where_conditions,
            )
            if affected:
                self._summary_publish_host_scope(
                    host_id, reason="file_task_suspend_by_host"
                )

    def file_task_resume_by_host(
        self, host_id: int, busy_timeout_seconds: int = k.HOST_BUSY_TIMEOUT
    ) -> None:
        """Resume file tasks using the established host-recovery eligibility rules.

        Args:
            host_id (int): Recovered host ID.
            busy_timeout_seconds (int): Age threshold for stale RUNNING queue rows.

        Returns:
            None. Resume all SUSPENDED/stale RUNNING file tasks, plus ERROR tasks
            of discovery/backup types; preserve FROZEN and clear resumed PIDs.

        Raises:
            psycopg.Error: If any resumption update fails.
        """
        conditions = (
            {"NU_STATUS": k.TASK_SUSPENDED},
            {
                "NU_STATUS": k.TASK_ERROR,
                "NU_TYPE__in": (k.FILE_TASK_DISCOVERY, k.FILE_TASK_BACKUP_TYPE),
            },
            {
                "NU_STATUS": k.TASK_RUNNING,
                "DT_FILE_TASK__lt": datetime.utcnow()
                - timedelta(seconds=busy_timeout_seconds),
            },
        )
        data = self._merge_structured_error_fields(
            {
                "NU_STATUS": k.TASK_PENDING,
                "NU_PID": None,
                "NA_MESSAGE": "Host reachable again — task resumed automatically",
            }
        )
        with self._unit_of_work():
            affected = 0
            for condition in conditions:
                where_conditions = {"FK_HOST": host_id, **condition}
                affected += self._update_batch(
                    table="FILE_TASK", data=data, where=where_conditions
                )
            if affected:
                self._summary_publish_host_scope(
                    host_id, reason="file_task_resume_by_host"
                )

    @staticmethod
    def _file_task_build_current_year_order(alias: str) -> sql.Composed:
        """Build the UTC current-year bucket used by fair backup scheduling.

        Args:
            alias (str): Trusted query alias for the file-task table.

        Returns:
            sql.Composed: Descending boolean year bucket with undated files last.
        """
        return sql.SQL(
            "COALESCE(EXTRACT(YEAR FROM {}.\"DT_FILE_CREATED_HOST\") = EXTRACT(YEAR FROM (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')), FALSE) DESC"
        ).format(sql.Identifier(alias))

    def _file_task_build_order_by(
        self, alias: str, newest: bool, current_year: bool
    ) -> sql.Composed:
        """Compose deterministic candidate ordering with explicit NULL placement.

        Args:
            alias (str): Trusted file-task query alias.
            newest (bool): Order by newest creation date and descending ID.
            current_year (bool): Prepend the UTC current-year preference bucket.

        Returns:
            sql.Composed: Ordering suitable for outer and per-host candidates.
        """
        parts: list[sql.Composable] = []
        if current_year:
            parts.append(self._file_task_build_current_year_order(alias))
        if newest:
            parts.append(
                sql.SQL('{}."DT_FILE_CREATED_HOST" DESC NULLS LAST').format(
                    sql.Identifier(alias)
                )
            )
        parts.append(
            sql.SQL('{}."ID_FILE_TASK" {}').format(
                sql.Identifier(alias), sql.SQL("DESC" if newest else "ASC")
            )
        )
        return sql.SQL(", ").join(parts)

    # --- FILE_TASK_HISTORY ---

    def file_task_history_create(
        self,
        host_id: int,
        task_type: int,
        task_status: int,
        file_metadata: Sequence[FileMetadata],
    ) -> int:
        """Upsert discovery history by the schema's host/path/name identity.

        Args:
            host_id (int): Parent host ID.
            task_type (int): Type used to compose the discovery message.
            task_status (int): State used to compose the discovery message.
            file_metadata (Sequence[FileMetadata]): Homogeneous source artifacts.
                Empty input performs no SQL. Rediscovery replaces supplied fields.

        Returns:
            int: Inserted/updated rows. Discovery is DONE, later phases are PENDING,
            and their completion timestamps start unset, matching the legacy flow.

        Raises:
            psycopg.Error: If history persistence fails; chunks are atomic.
        """
        if not file_metadata:
            return 0
        now = datetime.utcnow()
        rows = [
            self._merge_structured_error_fields(
                {
                    **self._file_metadata_payload(file),
                    "FK_HOST": host_id,
                    "DT_DISCOVERED": now,
                    "DT_BACKUP": None,
                    "DT_PROCESSED": None,
                    "NU_STATUS_DISCOVERY": k.TASK_DONE,
                    "NU_STATUS_BACKUP": k.TASK_PENDING,
                    "NU_STATUS_PROCESSING": k.TASK_PENDING,
                    "NA_MESSAGE": tools.compose_message(
                        task_type, task_status, file.NA_PATH, file.NA_FILE
                    ),
                }
            )
            for file in file_metadata
        ]
        with self._unit_of_work():
            count = self._file_upsert_batch(table="FILE_TASK_HISTORY", data=rows)
            self._summary_publish_host_scope(
                host_id,
                reference_months=[file.DT_FILE_CREATED for file in file_metadata],
                reason="file_task_history_create",
            )
            return count

    def file_task_history_update(
        self,
        task_type: int | None = None,
        *,
        history_id: int | None = None,
        host_id: int | None = None,
        host_file_path: str | None = None,
        host_file_name: str | None = None,
        server_file_name: str | None = None,
        **kwargs: object,
    ) -> DbRow:
        """Persist explicit lifecycle changes and their host markers in one transaction.

        Preserve the established distinction between omitted/None values (no change)
        and constants.SET_NULL (write SQL NULL). Completion timestamps belong to the
        caller. Successful dated phases advance HOST markers monotonically.

        Args:
            task_type (int | None): Retained caller context; does not infer phase data.
            history_id (int | None): Explicit history primary key.
            host_id (int | None): Host paired with source identity or server name.
            host_file_path (str | None): Source path used with host_file_name.
            host_file_name (str | None): Source name used with host_file_path.
            server_file_name (str | None): Alternate server name within the host.
            **kwargs (object): Supported history replacements, None to omit a change,
                or constants.SET_NULL to clear a value explicitly.

        Returns:
            DbRow: success, rows_affected, updated_fields, and where_used. Empty
            effective changes succeed with zero rows and no SQL write.

        Raises:
            ValueError: If fields, empty input, or identity are invalid.
            psycopg.Error: If updating history or its host markers fails.
        """
        if not kwargs:
            raise ValueError("History update requires at least one field.")
        self._validate_fields(tuple(kwargs), self.VALID_FIELDS_FILE_TASK_HISTORY)
        where_conditions = self._file_identity_filter(
            "ID_HISTORY",
            history_id,
            host_id,
            host_file_path,
            host_file_name,
            server_file_name,
        )
        payload = {
            name: None if value is constants.SET_NULL else value
            for name, value in kwargs.items()
            if value is not None
        }
        if not payload:
            return {
                "success": True,
                "rows_affected": 0,
                "updated_fields": {},
                "where_used": where_conditions,
            }
        payload = self._merge_structured_error_fields(payload)
        if payload.get("IS_PAYLOAD_DELETED") is not None:
            payload["IS_PAYLOAD_DELETED"] = self._storage_flag(
                payload["IS_PAYLOAD_DELETED"]
            )
        with self._unit_of_work():
            columns = ("FK_HOST", "DT_FILE_CREATED_HOST")
            rows = self._select_batch(
                table="FILE_TASK_HISTORY", where=where_conditions, columns=columns
            )
            affected = self._update_batch(
                table="FILE_TASK_HISTORY", data=payload, where=where_conditions
            )
            hosts: set[int] = set()
            months: list[object] = [row["DT_FILE_CREATED_HOST"] for row in rows]
            if "DT_FILE_CREATED_HOST" in payload:
                months.append(payload["DT_FILE_CREATED_HOST"])
            for row in rows:
                scope = cast(int, row["FK_HOST"])
                if scope not in hosts:
                    self._host_update_phase_markers_from_history(
                        host_id=scope, history_update=payload
                    )
                    hosts.add(scope)
            for scope in hosts:
                self._summary_publish_host_scope(
                    scope, reference_months=months, reason="file_task_history_update"
                )
            return {
                "success": True,
                "rows_affected": affected,
                "updated_fields": payload,
                "where_used": where_conditions,
            }

    def file_task_history_delete(
        self,
        *,
        history_id: int | None = None,
        host_id: int | None = None,
        host_file_path: str | None = None,
        host_file_name: str | None = None,
    ) -> int:
        """Delete identified lifecycle rows and invalidate their former host/month.

        Args:
            history_id (int | None): Explicit history primary key.
            host_id (int | None): Host paired with source path and name otherwise.
            host_file_path (str | None): Exact source path.
            host_file_name (str | None): Exact source filename.

        Returns:
            int: Number of deleted history rows; zero when no identity matched.

        Raises:
            ValueError: If neither complete identity strategy is supplied.
            psycopg.Error: If reading the scope or deleting history fails.
        """
        where_conditions = self._file_identity_filter(
            "ID_HISTORY", history_id, host_id, host_file_path, host_file_name
        )
        with self._unit_of_work():
            columns = ("FK_HOST", "DT_FILE_CREATED_HOST")
            rows = self._select_batch(
                table="FILE_TASK_HISTORY", where=where_conditions, columns=columns
            )
            affected = self._delete_batch(
                table="FILE_TASK_HISTORY", where=where_conditions
            )
            for row in rows:
                self._summary_publish_host_scope(
                    cast(int, row["FK_HOST"]),
                    reference_months=[row["DT_FILE_CREATED_HOST"]],
                    reason="file_task_history_delete",
                )
            return affected

    def file_task_history_filter_existing_batch(
        self, host_id: int, batch: list[FileMetadata], *, batch_size: int
    ) -> list[FileMetadata]:
        """Keep new file identities while preserving path/name/size deduplication.

        Args:
            host_id (int): Parent history host.
            batch (list[FileMetadata]): Source metadata in enumeration order.
            batch_size (int): Positive maximum accepted input length.

        Returns:
            list[FileMetadata]: Original objects not present by path/name/size.
            ZIPs already processed to MAT in DONE/ERROR are also excluded by
            path/name, preserving the terminal conversion deduplication rule.

        Raises:
            ValueError: If the input exceeds the positive batch limit.
            psycopg.Error: If history lookup fails.
        """
        if batch_size < 1 or len(batch) > batch_size:
            raise ValueError("Discovery batch exceeds its positive size limit.")
        if not batch:
            return []
        values = sql.SQL(", ").join(
            sql.SQL("(%s::text, %s::text, %s::bigint)") for _ in batch
        )
        query = sql.SQL(
            """
                    SELECT f.path, f.name, f.size FROM (VALUES {}) AS f(path, name, size)
                                JOIN {} h ON h."FK_HOST" = %s AND h."NA_HOST_FILE_PATH" = f.path
                                 AND h."NA_HOST_FILE_NAME" = f.name AND h."VL_FILE_SIZE_KB_HOST" = f.size
                """
        ).format(values, sql.Identifier(self.schema, "FILE_TASK_HISTORY"))
        params = tuple(
            value
            for file in batch
            for value in (file.NA_PATH, file.NA_FILE, file.VL_FILE_SIZE_KB)
        )
        existing = {
            (row["path"], row["name"], row["size"])
            for row in self._select_raw(query=query, params=(*params, host_id))
        }
        zip_names = [
            file.NA_FILE for file in batch if file.NA_EXTENSION.lower() == ".zip"
        ]
        converted: set[tuple[object, object]] = set()
        if zip_names:
            where_conditions = {
                "FK_HOST": host_id,
                "NA_HOST_FILE_NAME__in": zip_names,
                "NA_EXTENSION_SERVER__ilike": ".mat",
                "NU_STATUS_PROCESSING__in": (k.TASK_DONE, k.TASK_ERROR),
            }
            columns = ("NA_HOST_FILE_PATH", "NA_HOST_FILE_NAME")
            rows = self._select_batch(
                table="FILE_TASK_HISTORY",
                where=where_conditions,
                columns=columns,
            )
            converted = {
                (row["NA_HOST_FILE_PATH"], row["NA_HOST_FILE_NAME"]) for row in rows
            }
        return [
            file
            for file in batch
            if (file.NA_PATH, file.NA_FILE, file.VL_FILE_SIZE_KB) not in existing
            and not (
                file.NA_EXTENSION.lower() == ".zip"
                and (file.NA_PATH, file.NA_FILE) in converted
            )
        ]

    def file_task_history_list_gc_candidates(
        self, batch_size: int, quarantine_days: int
    ) -> list[DbRow]:
        """List undeleted errored payloads past their processing-based quarantine.

        Args:
            batch_size (int): Positive maximum candidate count.
            quarantine_days (int): Nonnegative elapsed days since DT_PROCESSED.

        Returns:
            list[DbRow]: ID_HISTORY, NA_SERVER_FILE_PATH, and NA_SERVER_FILE_NAME,
            ordered by DT_PROCESSED and ID. Missing processing timestamps are excluded.

        Raises:
            ValueError: If the batch limit or quarantine is invalid.
            psycopg.Error: If reading retention candidates fails.
        """
        if batch_size < 1 or quarantine_days < 0:
            raise ValueError("GC requires a positive limit and nonnegative retention.")
        return self._select_raw(
            query=sql.SQL(
                """
                SELECT "ID_HISTORY", "NA_SERVER_FILE_PATH", "NA_SERVER_FILE_NAME" FROM {}
                WHERE "NU_STATUS_PROCESSING" = %s AND "IS_PAYLOAD_DELETED" = 0
                  AND "DT_PROCESSED" IS NOT NULL
                  AND "DT_PROCESSED" < (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - %s * INTERVAL '1 day'
                ORDER BY "DT_PROCESSED", "ID_HISTORY" LIMIT %s
            """
            ).format(sql.Identifier(self.schema, "FILE_TASK_HISTORY")),
            params=(k.TASK_ERROR, quarantine_days, batch_size),
        )

    def file_task_history_suspend_by_host(
        self, host_id: int, reason: str | None = None
    ) -> None:
        """Suspend live discovery/backup history phases after host connectivity loss.

        Args:
            host_id (int): Parent host ID.
            reason (str | None): Optional operational message.

        Returns:
            None. Processing phase and terminal phase states remain unchanged.

        Raises:
            psycopg.Error: If either phase update fails; standalone writes are atomic.
        """
        self._file_task_history_change_host_states(
            host_id,
            (k.TASK_PENDING, k.TASK_RUNNING),
            k.TASK_SUSPENDED,
            reason
            or "Host unreachable — host-dependent history suspended by host_check service",
        )

    def file_task_history_resume_by_host(self, host_id: int) -> None:
        """Return suspended/errored host-dependent history phases to PENDING.

        Args:
            host_id (int): Recovered host ID.

        Returns:
            None. Discovery/backup change together; processing and FROZEN are preserved.

        Raises:
            psycopg.Error: If either phase update fails.
        """
        self._file_task_history_change_host_states(
            host_id,
            (k.TASK_SUSPENDED, k.TASK_ERROR),
            k.TASK_PENDING,
            "Host reachable again — history phase resumed automatically",
        )

    def _file_task_history_change_host_states(
        self, host_id: int, old_states: Sequence[int], new_state: int, message: str
    ) -> None:
        """Change host-dependent history phases together without touching processing.

        Args:
            host_id (int): Parent host ID.
            old_states (Sequence[int]): Eligible phase states; excludes FROZEN.
            new_state (int): Target discovery/backup phase state.
            message (str): Message normalized into compact diagnostic fields.

        Returns:
            None. Publish one host invalidation if either phase changed.

        Raises:
            psycopg.Error: If any phase update fails.
        """
        with self._unit_of_work():
            affected = 0
            for phase in ("NU_STATUS_DISCOVERY", "NU_STATUS_BACKUP"):
                payload = self._merge_structured_error_fields(
                    {phase: new_state, "NA_MESSAGE": message}
                )
                where_conditions = {
                    "FK_HOST": host_id,
                    f"{phase}__in": tuple(old_states),
                }
                affected += self._update_batch(
                    table="FILE_TASK_HISTORY",
                    data=payload,
                    where=where_conditions,
                )
            if affected:
                self._summary_publish_host_scope(
                    host_id, reason="file_task_history_host_states"
                )

    # --- Shared persistence and Summary publication ---

    @contextmanager
    def _unit_of_work(self) -> Generator[None, None, None]:
        """Keep compound domain writes atomic without completing a caller transaction.

        Args:
            None.

        Returns:
            Generator[None, None, None]: Context owning a transaction only when
            none existed. Body failure rolls back only a self-owned transaction.
            Commit failures propagate without an automatic replay or rollback.

        Raises:
            Exception: Propagates the original operation or completion failure.
        """
        owns_transaction = not self.in_transaction
        if owns_transaction:
            self.begin_transaction()
        try:
            yield
        except Exception:
            if owns_transaction:
                try:
                    self.rollback()
                except psycopg.Error as error:
                    self._log_error("db_rollback_failed", "rollback", error)
            raise
        else:
            if owns_transaction:
                self.commit()

    @staticmethod
    def _storage_flag(value: object) -> int:
        """Convert an operational flag at the domain input boundary.

        Args:
            value (object): Boolean or integer 0/1 supplied for a smallint flag.

        Returns:
            int: Zero or one for PostgreSQL smallint storage.

        Raises:
            ValueError: If the value is not a boolean or 0/1 integer.
        """
        if not isinstance(value, (bool, int)) or value not in (0, 1):
            raise ValueError("Operational flags require a boolean or 0/1 integer.")
        return int(value)

    def _merge_structured_error_fields(self, payload: Mapping[str, object]) -> DbRow:
        """Normalize persisted task messages and their compact diagnostic fields.

        Args:
            payload (Mapping[str, object]): Operational fields with an optional
                NA_MESSAGE and explicit NA_ERROR_DETAIL. Legacy redundant error
                fields are removed rather than written into the current schema.

        Returns:
            DbRow: Copy with canonical message/code/detail/classifier fields.
            Explicit detail is retained if message classification provides none.
        """
        normalized = {
            name: value
            for name, value in payload.items()
            if name not in ("NA_ERROR_DOMAIN", "NA_ERROR_STAGE", "NA_ERROR_SUMMARY")
        }
        if "NA_MESSAGE" in normalized:
            compact = errors.build_operational_error_payload(
                cast(str | None, normalized["NA_MESSAGE"])
            )
            if compact.get("NA_ERROR_DETAIL") is None and normalized.get(
                "NA_ERROR_DETAIL"
            ):
                compact["NA_ERROR_DETAIL"] = normalized["NA_ERROR_DETAIL"]
            normalized.update(compact)
        return normalized

    @staticmethod
    def _file_identity_filter(
        id_column: str,
        record_id: int | None,
        host_id: int | None,
        path: str | None,
        name: str | None,
        server_name: str | None = None,
    ) -> DbRow:
        """Resolve the supported primary/natural/server identity strategies.

        Args:
            id_column (str): Trusted ID_HISTORY or ID_FILE_TASK column name.
            record_id (int | None): Preferred explicit primary key.
            host_id (int | None): Required parent for alternative identities.
            path (str | None): Source path paired with name.
            name (str | None): Source filename paired with path.
            server_name (str | None): Alternative server filename within the host.

        Returns:
            DbRow: Nonempty equality filters, in identity precedence order.

        Raises:
            ValueError: If no complete identity is supplied.
        """
        if record_id is not None:
            return {id_column: record_id}
        if host_id is not None and path is not None and name is not None:
            return {
                "FK_HOST": host_id,
                "NA_HOST_FILE_PATH": path,
                "NA_HOST_FILE_NAME": name,
            }
        if host_id is not None and server_name is not None:
            return {"FK_HOST": host_id, "NA_SERVER_FILE_NAME": server_name}
        raise ValueError("Provide a primary key, host/path/name, or host/server name.")

    @staticmethod
    def _file_metadata_payload(file: FileMetadata) -> DbRow:
        """Map one source metadata value object to shared queue/history columns.

        Args:
            file (FileMetadata): Source path, filename, extension, size in KB,
                and created/modified timestamps.

        Returns:
            DbRow: Source metadata only, without workflow state or inferred dates.
        """
        return {
            "NA_HOST_FILE_PATH": file.NA_PATH,
            "NA_HOST_FILE_NAME": file.NA_FILE,
            "NA_EXTENSION_HOST": file.NA_EXTENSION,
            "VL_FILE_SIZE_KB_HOST": file.VL_FILE_SIZE_KB,
            "DT_FILE_CREATED_HOST": file.DT_FILE_CREATED,
            "DT_FILE_MODIFIED_HOST": file.DT_FILE_MODIFIED,
        }

    def _file_upsert_batch(
        self, table: str, data: Sequence[Mapping[str, object]]
    ) -> int:
        """Upsert files against the existing expression index for natural identities.

        Args:
            table (str): FILE_TASK or FILE_TASK_HISTORY with declared fields.
            data (Sequence[Mapping[str, object]]): Homogeneous supplied columns;
                FK_HOST, NA_HOST_FILE_PATH, and NA_HOST_FILE_NAME are required.

        Returns:
            int: PostgreSQL affected-row count. Conflict updates use supplied
            values, including caller-owned timestamps, without MariaDB's count of two.

        Raises:
            ValueError: If the table, fields, identities, or batch shapes are invalid.
            psycopg.Error: If writing any chunk fails.
        """
        if table not in ("FILE_TASK", "FILE_TASK_HISTORY"):
            raise ValueError("Expression-based identities belong to file tables.")
        if not data:
            return 0
        target, fields = self._get_table_definition(table)
        columns = tuple(data[0])
        self._validate_fields(columns, fields)
        identity = ("FK_HOST", "NA_HOST_FILE_PATH", "NA_HOST_FILE_NAME")
        if not set(identity).issubset(columns):
            raise ValueError("File UPSERT requires the complete natural identity.")
        hash_function = sql.Identifier(self.schema, "file_identity_sha256")
        query = self._build_insert_query(target, columns)
        query += sql.SQL(
            ' ON CONFLICT ("FK_HOST", {}("NA_HOST_FILE_PATH"), {}("NA_HOST_FILE_NAME"))'
        ).format(hash_function, hash_function)
        updates = [
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(name), sql.Identifier(name)
            )
            for name in columns
            if name not in identity
        ]
        query += (
            sql.SQL(" DO UPDATE SET ") + sql.SQL(", ").join(updates)
            if updates
            else sql.SQL(" DO NOTHING")
        )
        return self._execute_batch(query=query, values=self._prepare_batch_values(data))

    def _task_read_host_id(self, table: str, task_id: int) -> int | None:
        """Resolve the parent host of a queue primary key before invalidation.

        Args:
            table (str): HOST_TASK or FILE_TASK, with its declared primary key.
            task_id (int): Task primary-key value.

        Returns:
            int | None: FK_HOST, or None when the queue row is absent/unassigned.

        Raises:
            psycopg.Error: If scope lookup fails.
        """
        key = getattr(self, f"PRIMARY_KEY_{table}")[0]
        where_conditions = {key: task_id}
        columns = ("FK_HOST",)
        row = self._select_row(table=table, where=where_conditions, columns=columns)
        return cast(int | None, row["FK_HOST"]) if row else None

    @staticmethod
    def _normalize_summary_reference_month(value: object) -> str | None:
        """Normalize supported date-like invalidation values to a first-of-month key.

        Args:
            value (object): datetime, date, YYYY-MM, or ISO date/timestamp text;
                None or unparseable values are omitted.

        Returns:
            str | None: YYYY-MM-01, or None when no valid month can be obtained.
        """
        if isinstance(value, (datetime, date)):
            return value.strftime("%Y-%m-01")
        if not isinstance(value, str):
            return None
        text = value.strip()[:10]
        try:
            parsed = date.fromisoformat(text + "-01" if len(text) == 7 else text)
        except ValueError:
            return None
        return parsed.strftime("%Y-%m-01")

    def _summary_publish_host_scope(
        self,
        host_id: int | None,
        *,
        reference_months: Sequence[object] | None = None,
        full_reconcile: bool = False,
        reason: str | None = None,
    ) -> None:
        """Coalesce host/month invalidations without aborting operational writes.

        Renew the outbox identity on conflict so checkpoint-based consumers see a
        re-dirtied scope. An active caller transaction retains the event until its
        own commit. A savepoint isolates publication failures; no SQL text, values,
        connection details, or raw driver messages are logged on failure.

        Args:
            host_id (int | None): Host scope, optional for full reconciliation.
            reference_months (Sequence[object] | None): Date-like month scopes.
            full_reconcile (bool): Add the configured full-reconcile scope.
            reason (str | None): Diagnostic label limited to the outbox column size.

        Returns:
            None. Driver failures are reported as sanitized warnings; periodic
            reconciliation remains the recovery path for missing invalidations.
        """
        if host_id is None and not full_reconcile:
            return
        scopes = {
            (k.SUMMARY_SCOPE_REFERENCE_MONTH, month)
            for value in (reference_months or ())
            if (month := self._normalize_summary_reference_month(value)) is not None
        }
        if host_id is not None:
            scopes.add((k.SUMMARY_SCOPE_HOST, str(host_id)))
        if full_reconcile:
            scopes.add(
                (k.SUMMARY_SCOPE_FULL_RECONCILE, k.SUMMARY_SCOPE_FULL_RECONCILE_KEY)
            )
        query = sql.SQL(
            """
                    INSERT INTO {} ("NA_SCOPE_TYPE", "NA_SCOPE_VALUE", "NA_SOURCE_HANDLER", "NA_REASON")
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT ("NA_SCOPE_TYPE", "NA_SCOPE_VALUE") DO UPDATE SET
                                  "ID_OUTBOX" = EXCLUDED."ID_OUTBOX", "NA_SOURCE_HANDLER" = EXCLUDED."NA_SOURCE_HANDLER",
                                  "NA_REASON" = EXCLUDED."NA_REASON", "DT_CREATED_AT" = EXCLUDED."DT_CREATED_AT"
                """
        ).format(sql.Identifier(k.SUMMARY_DATABASE_NAME, "SUMMARY_OUTBOX"))
        try:
            with self._cursor("summary_publish") as cursor:
                if (
                    self.in_transaction
                    and cursor.connection.info.transaction_status
                    == TransactionStatus.IDLE
                ):
                    # Start the caller transaction before creating a nested savepoint.
                    cursor.execute(sql.SQL("SELECT 1"))
                with cursor.connection.transaction():
                    cursor.executemany(
                        query,
                        [
                            (
                                kind,
                                value,
                                type(self).__name__,
                                reason[:64] if reason else None,
                            )
                            for kind, value in sorted(scopes)
                        ],
                    )
        except psycopg.Error as error:
            self.log.warning_event(
                "summary_scope_publish_failed",
                db_handler=type(self).__name__,
                operation="summary_publish",
                error=type(error).__name__,
                sqlstate=error.sqlstate,
            )

    def _select_task_with_host(
        self,
        table: str,
        conditions: Sequence[sql.Composable],
        params: Sequence[object],
        order_by: sql.Composable,
    ) -> DbRow | None:
        """Read one joined queue candidate with the established prefixed row keys.

        Args:
            table (str): Trusted HOST_TASK or FILE_TASK table name.
            conditions (Sequence[sql.Composable]): Trusted predicates using t/h aliases.
            params (Sequence[object]): Bound predicate values in SQL order.
            order_by (sql.Composable): Trusted queue-specific ordering expression.

        Returns:
            DbRow | None: One row keyed as TABLE__COLUMN and HOST__COLUMN, or None.

        Raises:
            ValueError: If a table has no declared fields.
            psycopg.Error: If candidate lookup fails.
        """
        target, fields = self._get_table_definition(table)
        projection = [
            sql.SQL("{}.{} AS {}").format(
                sql.Identifier(alias),
                sql.Identifier(name),
                sql.Identifier(f"{prefix}__{name}"),
            )
            for alias, prefix, names in (
                ("t", table, fields),
                ("h", "HOST", self.VALID_FIELDS_HOST),
            )
            for name in sorted(names)
        ]
        query = sql.SQL(
            'SELECT {} FROM {} t JOIN {} h ON h."ID_HOST" = t."FK_HOST"'
        ).format(
            sql.SQL(", ").join(projection), target, sql.Identifier(self.schema, "HOST")
        )
        if conditions:
            query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(conditions)
        query += sql.SQL(" ORDER BY {} LIMIT 1").format(order_by)
        rows = self._select_raw(query=query, params=params)
        return rows[0] if rows else None
