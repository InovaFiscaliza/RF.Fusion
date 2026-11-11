"""Persist operational hosts, queues, history, and Summary invalidations.

Keep PostgreSQL statements and workflow persistence in this handler. Workers
still own task claims and finalization; this module does not transfer files or
run discovery. The MariaDB implementation remains unchanged in backup/.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
import json
import os
from typing import TYPE_CHECKING, cast

from psycopg import sql
from psycopg.types.json import Json

import config as k
from . import constants as db_constants
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
    """

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

    def host_update_signals(self, host_id: int, values: Mapping[str, object]) -> int:
        """Persist evaluated discovery/SSH/GNSS evidence at its authoritative host.

        Args:
            host_id (int): Existing operational host identity.
            values (Mapping[str, object]): Nonempty subset of HOST_SIGNAL_FIELDS;
                nullable evidence timestamps/texts, discovery counts/KB and 0/1
                signal flags. Fields absent from this mapping remain unchanged.
        Returns:
            int: Matched host count, zero when the host does not exist.
        Raises:
            ValueError: Empty values or unauthorized signal fields.
        """
        if not values or set(values) - set(db_constants['hosts']['signal_fields']):
            raise ValueError(
                "A nonempty subset of operational signal fields is required."
            )
        data = dict(values)
        for field in ("IS_SSH_FAILURE", "IS_GPS_GNSS_UNAVAILABLE"):
            if field in data:
                if data[field] not in (0, 1):
                    raise ValueError("Operational signal flags must be 0 or 1.")
                data[field] = int(cast(int, data[field]))
        where_conditions = {"ID_HOST": host_id}
        return self._update_row(table="HOST", data=data, where=where_conditions)

    def host_upsert(self, **kwargs: object) -> None:
        """Create or refresh the supplied host fields by its explicit ID_HOST.

        Omitted columns remain unchanged on conflict. Convert operational flags
        to the existing smallint representation and invalidate the host snapshot.

        Args:
            **kwargs (object): Nonempty host column/value pairs;
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

    def host_list_busy(self) -> list[DbRow]:
        """Read host ownership marks for cleanup after all workers have stopped.

        Args:
            None.
        Returns:
            list[DbRow]: ID_HOST int and NU_PID nullable int for each busy host.
        Raises:
            psycopg.Error: If the ownership snapshot cannot be read.
        """
        where_conditions = {"IS_BUSY": 1}
        columns = ("ID_HOST", "NU_PID")
        return self._select_batch(
            table="HOST", where=where_conditions, columns=columns
        )

    def host_list_offline(self) -> list[DbRow]:
        """Read offline host identities for shutdown suspension reconciliation.

        Args:
            None.
        Returns:
            list[DbRow]: ID_HOST int for each offline host, including hosts
            without a configured address.
        Raises:
            psycopg.Error: If the offline snapshot cannot be read.
        """
        where_conditions = {"IS_OFFLINE": 1}
        columns = ("ID_HOST",)
        return self._select_batch(
            table="HOST", where=where_conditions, columns=columns
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
        data = {"IS_BUSY": 0, "NU_PID": db_constants['hosts']['unlocked_pid']}
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
        A timeout release clears DT_BUSY and uses the unlocked PID only when the
        observed reservation is unchanged; it then overrides supplied
        IS_BUSY, DT_BUSY, and NU_PID values.

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
        with self._unit_of_work():
            payload = self._host_storage_fields(kwargs)
            released = check_busy_timeout and self._host_release_timed_out(
                host_id, busy_timeout_seconds
            )
            if released:
                # The release already wrote the reservation fields.
                for name in ("IS_BUSY", "DT_BUSY", "NU_PID"):
                    payload.pop(name, None)
            data: DbRow = {}
            increments: dict[str, int | float] = {}
            for name, value in payload.items():
                if value is None:
                    continue
                if isinstance(value, tuple):
                    if (
                        len(value) != 2
                        or value[0] not in ("INC", "DEC")
                        or not isinstance(value[1], (int, float))
                    ):
                        raise ValueError(
                            "Arithmetic updates require (INC or DEC, number)."
                        )
                    increments[name] = value[1] if value[0] == "INC" else -value[1]
                elif (
                    not reset
                    and name not in ("IS_BUSY", "IS_OFFLINE")
                    and isinstance(value, (int, float))
                    and value != 0
                ):
                    increments[name] = value
                else:
                    data[name] = value
            if increments or data:
                where_conditions = {"ID_HOST": host_id}
                self._update_row(
                    table="HOST",
                    data=data,
                    where=where_conditions,
                    increments=increments,
                )
            elif not released:
                return
            self._summary_publish_host_scope(host_id, reason="host_update")

    def _host_release_timed_out(self, host_id: int, timeout_seconds: int) -> bool:
        """Release a reservation older than the timeout only if it is unchanged.

        The release compares the observed owner and DT_BUSY, so a reservation
        taken after the read is never cleared.

        Args:
            host_id (int): Host primary key.
            timeout_seconds (int): Minimum reservation age before release.

        Returns:
            bool: True when the observed reservation was released.

        Raises:
            psycopg.Error: If reading or releasing the reservation fails.
        """
        where_conditions = {"ID_HOST": host_id}
        columns = ("IS_BUSY", "DT_BUSY", "NU_PID")
        row = self._select_row(table="HOST", where=where_conditions, columns=columns)
        busy_since = cast(datetime | None, row["DT_BUSY"]) if row else None
        if (
            not row
            or not row["IS_BUSY"]
            or busy_since is None
            or (datetime.utcnow() - busy_since).total_seconds() <= timeout_seconds
        ):
            return False
        data = {"IS_BUSY": 0, "DT_BUSY": None, "NU_PID": db_constants['hosts']['unlocked_pid']}
        where_conditions = {
            "ID_HOST": host_id,
            "IS_BUSY": 1,
            "NU_PID": row["NU_PID"],
            "DT_BUSY": busy_since,
        }
        return self._update_batch(table="HOST", data=data, where=where_conditions) == 1

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
        if not row or not row["IS_BUSY"] or row["NU_PID"] == db_constants['hosts']['transient_busy_pid']:
            return
        pid = cast(int | None, row["NU_PID"])
        if (
            pid == current_pid
            or pid in (None, db_constants['hosts']['unlocked_pid'])
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
        """Read busy hosts with flags for the requested running work.

        Hosts are read before their tasks. A task that starts or ends between the
        reads only makes the flags more recent; releases remain conditional on
        the observed owner and DT_BUSY.

        Args:
            task_status (int): State matched in FILE_TASK and HOST_TASK.
            host_task_type (int): HOST_TASK type included in the flag.

        Returns:
            list[DbRow]: ID_HOST, DT_BUSY, NU_PID, FILE_RUNNING,
            and HOST_PROCESSING_RUNNING. Timestamps and nullable PIDs are native
            database values; flags are booleans. No liveness checks, timeout
            decisions, or writes are performed.

        Raises:
            psycopg.Error: If the maintenance snapshot cannot be read.
        """
        columns = ("ID_HOST", "DT_BUSY", "NU_PID")
        hosts = self._select_batch(table="HOST", where={"IS_BUSY": 1}, columns=columns)
        if not hosts:
            return []
        host_ids = tuple(row["ID_HOST"] for row in hosts)
        running: dict[str, set[object]] = {}
        for flag, table, where_conditions in (
            ("FILE_RUNNING", "FILE_TASK", {"NU_STATUS": task_status}),
            (
                "HOST_PROCESSING_RUNNING",
                "HOST_TASK",
                {"NU_STATUS": task_status, "NU_TYPE": host_task_type},
            ),
        ):
            rows = self._select_batch(
                table=table,
                where={**where_conditions, "FK_HOST__in": host_ids},
                columns=("FK_HOST",),
            )
            running[flag] = {row["FK_HOST"] for row in rows}
        for row in hosts:
            for flag, host_set in running.items():
                row[flag] = row["ID_HOST"] in host_set
        return hosts

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
        data = {"IS_BUSY": 0, "DT_BUSY": None, "NU_PID": db_constants['hosts']['unlocked_pid']}
        where_conditions = {
            "IS_BUSY": 1,
            "NU_PID": db_constants['hosts']['transient_busy_pid'],
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
            data = {"IS_BUSY": 0, "NU_PID": db_constants['hosts']['unlocked_pid']}
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
        """Occupy a host only when no other live worker runs host-dependent work.

        One transaction locks the host row, checks both queues, and writes the
        reservation. Competing reservers wait for that lock and then see the
        host busy, so the check and the reservation cannot interleave. A host
        already held by the current PID is reserved again, making retries
        idempotent. The task claim itself remains with the worker.

        Args:
            host_id (int): Candidate host primary key.
            require_online (bool): Also require IS_OFFLINE=0 when reserving.

        Returns:
            bool: True when the current PID holds the host; False when it is
            missing, offline when required, held by another owner, or occupied
            by live host-dependent work. False is an expected race loss.

        Raises:
            psycopg.Error: If locking, checking, or reserving the host fails.
        """
        pid = os.getpid()
        with self._unit_of_work():
            columns = ("IS_BUSY", "IS_OFFLINE", "NU_PID")
            rows = self._select_batch(
                table="HOST",
                where={"ID_HOST": host_id},
                columns=columns,
                for_update=True,
            )
            if (
                not rows
                or (rows[0]["IS_BUSY"] and rows[0]["NU_PID"] != pid)
                or (require_online and rows[0]["IS_OFFLINE"])
                or self._host_has_live_work(host_id, pid)
            ):
                return False
            data = {"IS_BUSY": 1, "DT_BUSY": datetime.utcnow(), "NU_PID": pid}
            self._update_row(table="HOST", data=data, where={"ID_HOST": host_id})
            return True

    def _host_has_live_work(self, host_id: int, current_pid: int) -> bool:
        """Report RUNNING host-occupying work owned by another live process.

        Only discovery (HOST_TASK processing) and discovery/backup FILE_TASK
        types occupy the remote host. Connectivity checks are lightweight and
        never block a reservation. Rows of dead, missing, or current PIDs are
        stale and do not occupy the host; recovery belongs to the resume paths.

        Args:
            host_id (int): Host to inspect.
            current_pid (int): PID asking for the host; its own rows are ignored.

        Returns:
            bool: True when at least one such task has a live owner.

        Raises:
            psycopg.Error: If reading either queue fails.
        """
        for table, types in (
            ("HOST_TASK", (db_constants['tasks']['host_types']['processing'],)),
            ("FILE_TASK", (db_constants['tasks']['file_types']['discovery'], db_constants['tasks']['file_types']['backup'])),
        ):
            where_conditions = {
                "FK_HOST": host_id,
                "NU_STATUS": db_constants['tasks']['states']['running'],
                "NU_TYPE__in": tuple(types),
                "NU_PID__ne": current_pid,
            }
            rows = self._select_batch(
                table=table, where=where_conditions, columns=("NU_PID",)
            )
            if any(tools.pid_exists(cast(int, row["NU_PID"])) for row in rows):
                return True
        return False

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
        maximums = {
            target: history_update[source]
            for phase, source, target in (
                ("NU_STATUS_BACKUP", "DT_BACKUP", "DT_LAST_BACKUP"),
                ("NU_STATUS_PROCESSING", "DT_PROCESSED", "DT_LAST_PROCESSING"),
            )
            if history_update.get(phase) == db_constants['tasks']['states']['done']
            and history_update.get(source) is not None
        }
        if maximums:
            where_conditions = {"ID_HOST": host_id}
            self._update_row(
                table="HOST", data={}, where=where_conditions, maximums=maximums
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
            psycopg.Error: If a column does not exist or task lookup fails.
        """
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

    def host_task_list_running(self) -> list[DbRow]:
        """Read running host-task ownership for cleanup after workers stop.

        Args:
            None.
        Returns:
            list[DbRow]: ID_HOST_TASK int, FK_HOST nullable int, and NU_PID
            nullable int. Includes every host-task type in RUNNING state.
        Raises:
            psycopg.Error: If the running-task snapshot cannot be read.
        """
        where_conditions = {"NU_STATUS": db_constants['tasks']['states']['running']}
        columns = ("ID_HOST_TASK", "FK_HOST", "NU_PID")
        return self._select_batch(
            table="HOST_TASK", where=where_conditions, columns=columns
        )

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
        payload = dict(kwargs)
        payload.setdefault("DT_HOST_TASK", datetime.utcnow())
        payload.setdefault("NU_STATUS", db_constants['tasks']['states']['pending'])
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
        task_status: int | None = db_constants['tasks']['states']['pending'],
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
        where_conditions: DbRow = {}
        host_conditions: DbRow = {}
        if task_id:
            where_conditions["ID_HOST_TASK"] = task_id
        else:
            where_conditions["NU_STATUS"] = task_status
            if isinstance(task_type, list):
                where_conditions["NU_TYPE__in"] = tuple(task_type)
            elif task_type is not None:
                where_conditions["NU_TYPE"] = task_type
            if check_host_busy:
                host_conditions["IS_BUSY"] = 0
            if check_host_offline:
                host_conditions["IS_OFFLINE"] = 0
        order_by = (("DT_HOST_TASK", "ASC NULLS FIRST"), ("ID_HOST_TASK", "ASC"))
        row = self._select_task_with_host(
            table="HOST_TASK",
            where=where_conditions,
            host_where=host_conditions,
            order_by=order_by,
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
        if payload.get("NU_STATUS") == db_constants['tasks']['states']['pending']:
            payload["NU_PID"] = None
        elif payload.get("NU_STATUS") == db_constants['tasks']['states']['running']:
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
                host_id = self._task_read_host_id(
                    table="HOST_TASK", where={"ID_HOST_TASK": task_id}
                )
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
        data = {
            "NU_STATUS": db_constants['tasks']['states']['suspended'],
            "NA_MESSAGE": "Host unreachable. Tasks suspended automatically.",
        }
        where_conditions = {
            "NU_STATUS__in": (db_constants['tasks']['states']['pending'], db_constants['tasks']['states']['running']),
            "NU_TYPE__in": tuple(
                db_constants['tasks']['host_types'][name]
                for name in db_constants['tasks']['connectivity_host_types']
            ),
        }
        self._task_update_by_host(
            table="HOST_TASK",
            host_id=host_id,
            updates=((data, where_conditions),),
            reason="host_task_suspend_by_host",
        )

    def host_task_resume_by_host(
        self, host_id: int, busy_timeout_seconds: int = k.HOST_BUSY_TIMEOUT
    ) -> None:
        """Resume eligible host tasks without changing their type or reviving FROZEN.

        Args:
            host_id (int): Recovered host identity.
            busy_timeout_seconds (int): Minimum age for reclaiming RUNNING tasks.

        Returns:
            None. SUSPENDED, ERROR, and stale RUNNING host-dependent tasks whose
            owner PID is gone become PENDING and clear NU_PID. Statistics tasks
            remain untouched.

        Raises:
            psycopg.Error: If any part of the resumption fails.
        """
        types = {
            "NU_TYPE__in": tuple(
                db_constants['tasks']['host_types'][name]
                for name in db_constants['tasks']['connectivity_host_types']
            )
        }
        stale = datetime.utcnow() - timedelta(seconds=busy_timeout_seconds)
        data = {
            "NU_STATUS": db_constants['tasks']['states']['pending'],
            "NU_PID": None,
            "NA_MESSAGE": "Host reachable again — task resumed automatically",
        }
        conditions = (
            {**types, "NU_STATUS__in": (db_constants['tasks']['states']['suspended'], db_constants['tasks']['states']['error'])},
            self._dead_owner_filter(
                "HOST_TASK",
                host_id,
                {**types, "NU_STATUS": db_constants['tasks']['states']['running'], "DT_HOST_TASK__lt": stale},
                id_column="ID_HOST_TASK",
            ),
        )
        self._task_update_by_host(
            table="HOST_TASK",
            host_id=host_id,
            updates=tuple((data, condition) for condition in conditions),
            reason="host_task_resume_by_host",
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
        columns = {
            "ID_HOST_TASK": ("HOST_TASK", "ID_HOST_TASK"),
            "FK_HOST": ("HOST_TASK", "FK_HOST"),
            "NU_STATUS": ("HOST_TASK", "NU_STATUS"),
            "DT_HOST_TASK": ("HOST_TASK", "DT_HOST_TASK"),
            "NU_PID": ("HOST_TASK", "NU_PID"),
            "IS_BUSY": ("HOST", "IS_BUSY"),
            "HOST_OWNER_PID": ("HOST", "NU_PID"),
            "DT_BUSY": ("HOST", "DT_BUSY"),
        }
        where_conditions = {
            "NU_TYPE__in": tuple(task_types),
            "NU_STATUS__in": tuple(task_statuses),
        }
        return self._select_batch(
            table="HOST_TASK",
            columns=columns,
            where=where_conditions,
            join=("HOST", "FK_HOST", "ID_HOST"),
        )

    def host_task_enqueue(
        self, host_id: int, task_type: int, task_status: int, filter_dict: DbRow
    ) -> DbRow:
        """Deterministically enqueue or refresh the singleton HOST_TASK row for one type.

        Queue contract:
            - one durable row per `FK_HOST + NU_TYPE`
            - RUNNING rows are preserved
            - PENDING or terminal rows are refreshed in place
            - a new row is created only when none exists for that type

        Listing and writing share one transaction. The create or update path
        publishes the host invalidation once; a preserved RUNNING row changes
        nothing and publishes nothing. Derived counters are never recalculated
        into ``BPDATA.HOST``.

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
        with self._unit_of_work():
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
                if status == db_constants['tasks']['states']['running']:
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
                        task_id=cast(int, existing["HOST_TASK__ID_HOST_TASK"]),
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
            normalized = dict(db_constants['tasks']['default_filter'])
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
        for status in (db_constants['tasks']['states']['pending'], db_constants['tasks']['states']['running']):
            for task in tasks:
                if task.get("HOST_TASK__NU_STATUS") == status:
                    return task
        return tasks[0] if tasks else None

    # --- FILE_TASK ---

    def file_task_list_running(self) -> list[DbRow]:
        """Read running file-task ownership for cleanup after workers stop.

        Args:
            None.
        Returns:
            list[DbRow]: ID_FILE_TASK int, FK_HOST nullable int, and NU_PID
            nullable int. Includes every file-task type in RUNNING state.
        Raises:
            psycopg.Error: If the running-task snapshot cannot be read.
        """
        where_conditions = {"NU_STATUS": db_constants['tasks']['states']['running']}
        columns = ("ID_FILE_TASK", "FK_HOST", "NU_PID")
        return self._select_batch(
            table="FILE_TASK", where=where_conditions, columns=columns
        )

    def file_task_read(
        self,
        task_id: int | None = None,
        task_status: int | None = db_constants['tasks']['states']['pending'],
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
        where_conditions: DbRow = {}
        host_conditions: DbRow = {}
        if task_id:
            where_conditions["ID_FILE_TASK"] = task_id
        else:
            where_conditions.update(
                {
                    name: value
                    for name, value in (
                        ("NU_STATUS", task_status),
                        ("NU_TYPE", task_type),
                        ("NA_EXTENSION_HOST", extension),
                    )
                    if value is not None
                }
            )
            if check_host_busy:
                host_conditions["IS_BUSY"] = 0
            if check_host_offline:
                host_conditions["IS_OFFLINE"] = 0
        fair_backup = (
            not task_id and task_type == db_constants['tasks']['file_types']['backup'] and fair_by_host
        )
        reserve_discovery = (
            not task_id
            and task_type == db_constants['tasks']['file_types']['backup']
            and reserve_hosts_for_discovery
        )
        if fair_backup or reserve_discovery:
            row = self._select_prioritized_file_task(
                where=where_conditions,
                host_where=host_conditions,
                fair_by_host=fair_backup,
                reserve_discovery=reserve_discovery,
                newest_first=newest_first,
                prefer_current_year=prefer_current_year,
            )
        else:
            order_by = (("ID_FILE_TASK", "ASC"),)
            if newest_first and not task_id:
                order_by = (
                    ("DT_FILE_CREATED_HOST", "DESC NULLS LAST"),
                    ("ID_FILE_TASK", "DESC"),
                )
            row = self._select_task_with_host(
                table="FILE_TASK",
                where=where_conditions,
                host_where=host_conditions,
                order_by=order_by,
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
        payload = self._merge_structured_error_fields(kwargs)
        if payload.get("NU_STATUS") == db_constants['tasks']['states']['pending']:
            payload.setdefault("NU_PID", None)
        elif payload.get("NU_STATUS") == db_constants['tasks']['states']['running']:
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
                        table="FILE_TASK", where={"ID_FILE_TASK": cast(int, task_id)}
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
            where_conditions = {"ID_FILE_TASK": task_id}
            host_id = self._task_read_host_id(
                table="FILE_TASK", where=where_conditions
            )
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
        target = self._table_identifier("FILE_TASK")
        self._validate_columns(tuple(data))
        assignments: list[sql.Composable] = [
            sql.SQL("{} = %s").format(sql.Identifier(name)) for name in data
        ]
        assignments.append(
            sql.SQL(
                """"NA_MESSAGE" = CONCAT(%s::text, ' of file ', "NA_HOST_FILE_PATH", '/', "NA_HOST_FILE_NAME")"""
            )
        )
        filters, values = self._build_where_clause(where)
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
        data = self._merge_structured_error_fields(
            {
                "NU_STATUS": db_constants['tasks']['states']['suspended'],
                "NA_MESSAGE": reason
                or "Host unreachable — HOST-dependent file task suspended by host_check service",
            }
        )
        where_conditions = {
            "NU_STATUS__in": (db_constants['tasks']['states']['pending'], db_constants['tasks']['states']['running']),
            "NU_TYPE__in": (db_constants['tasks']['file_types']['discovery'], db_constants['tasks']['file_types']['backup']),
        }
        self._task_update_by_host(
            table="FILE_TASK",
            host_id=host_id,
            updates=((data, where_conditions),),
            reason="file_task_suspend_by_host",
        )

    def file_task_resume_by_host(
        self, host_id: int, busy_timeout_seconds: int = k.HOST_BUSY_TIMEOUT
    ) -> None:
        """Resume file tasks using the established host-recovery eligibility rules.

        Args:
            host_id (int): Recovered host ID.
            busy_timeout_seconds (int): Age threshold for stale RUNNING queue rows.

        Returns:
            None. Resume all SUSPENDED file tasks, stale RUNNING tasks whose owner
            PID is gone, and ERROR tasks of discovery/backup types; preserve
            FROZEN and clear resumed PIDs.

        Raises:
            psycopg.Error: If any resumption update fails.
        """
        stale = datetime.utcnow() - timedelta(seconds=busy_timeout_seconds)
        conditions = (
            {"NU_STATUS": db_constants['tasks']['states']['suspended']},
            {
                "NU_STATUS": db_constants['tasks']['states']['error'],
                "NU_TYPE__in": (db_constants['tasks']['file_types']['discovery'], db_constants['tasks']['file_types']['backup']),
            },
            self._dead_owner_filter(
                "FILE_TASK",
                host_id,
                {"NU_STATUS": db_constants['tasks']['states']['running'], "DT_FILE_TASK__lt": stale},
                id_column="ID_FILE_TASK",
            ),
        )
        data = self._merge_structured_error_fields(
            {
                "NU_STATUS": db_constants['tasks']['states']['pending'],
                "NU_PID": None,
                "NA_MESSAGE": "Host reachable again — task resumed automatically",
            }
        )
        self._task_update_by_host(
            table="FILE_TASK",
            host_id=host_id,
            updates=tuple((data, condition) for condition in conditions),
            reason="file_task_resume_by_host",
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
                    "NU_STATUS_DISCOVERY": db_constants['tasks']['states']['done'],
                    "NU_STATUS_BACKUP": db_constants['tasks']['states']['pending'],
                    "NU_STATUS_PROCESSING": db_constants['tasks']['states']['pending'],
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
        # Path and name sets select a superset; exact identities are matched below.
        where_conditions = {
            "FK_HOST": host_id,
            "NA_HOST_FILE_PATH__in": tuple(sorted({file.NA_PATH for file in batch})),
            "NA_HOST_FILE_NAME__in": tuple(sorted({file.NA_FILE for file in batch})),
        }
        columns = (
            "NA_HOST_FILE_PATH",
            "NA_HOST_FILE_NAME",
            "VL_FILE_SIZE_KB_HOST",
            "NA_EXTENSION_SERVER",
            "NU_STATUS_PROCESSING",
        )
        rows = self._select_batch(
            table="FILE_TASK_HISTORY", where=where_conditions, columns=columns
        )
        existing = {
            (
                row["NA_HOST_FILE_PATH"],
                row["NA_HOST_FILE_NAME"],
                row["VL_FILE_SIZE_KB_HOST"],
            )
            for row in rows
        }
        converted = {
            (row["NA_HOST_FILE_PATH"], row["NA_HOST_FILE_NAME"])
            for row in rows
            if str(row["NA_EXTENSION_SERVER"] or "").lower() == ".mat"
            and row["NU_STATUS_PROCESSING"] in (db_constants['tasks']['states']['done'], db_constants['tasks']['states']['error'])
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
        where_conditions = {
            "NU_STATUS_PROCESSING": db_constants['tasks']['states']['error'],
            "IS_PAYLOAD_DELETED": 0,
            "DT_PROCESSED__lt": datetime.utcnow() - timedelta(days=quarantine_days),
        }
        columns = ("ID_HISTORY", "NA_SERVER_FILE_PATH", "NA_SERVER_FILE_NAME")
        order_by = (("DT_PROCESSED", "ASC"), ("ID_HISTORY", "ASC"))
        return self._select_batch(
            table="FILE_TASK_HISTORY",
            where=where_conditions,
            columns=columns,
            order_by=order_by,
            limit=batch_size,
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
            (db_constants['tasks']['states']['pending'], db_constants['tasks']['states']['running']),
            db_constants['tasks']['states']['suspended'],
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
            (db_constants['tasks']['states']['suspended'], db_constants['tasks']['states']['error']),
            db_constants['tasks']['states']['pending'],
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
        updates = tuple(
            (
                self._merge_structured_error_fields(
                    {phase: new_state, "NA_MESSAGE": message}
                ),
                {f"{phase}__in": tuple(old_states)},
            )
            for phase in ("NU_STATUS_DISCOVERY", "NU_STATUS_BACKUP")
        )
        self._task_update_by_host(
            table="FILE_TASK_HISTORY",
            host_id=host_id,
            updates=updates,
            reason="file_task_history_host_states",
        )

    # --- Shared persistence and Summary publication ---

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
            table (str): FILE_TASK or FILE_TASK_HISTORY for file persistence.
            data (Sequence[Mapping[str, object]]): Homogeneous supplied columns;
                FK_HOST, NA_HOST_FILE_PATH, and NA_HOST_FILE_NAME are required.

        Returns:
            int: PostgreSQL affected-row count. Conflict updates use supplied
            values, including caller-owned timestamps, without MariaDB's count of two.
            Existing rows with a RUNNING status field are left unchanged and
            are not counted.

        Raises:
            ValueError: If the table, fields, identities, or batch shapes are invalid.
            psycopg.Error: If writing any chunk fails.
        """
        if table not in ("FILE_TASK", "FILE_TASK_HISTORY"):
            raise ValueError("Expression-based identities belong to file tables.")
        if not data:
            return 0
        target = self._table_identifier(table)
        columns = tuple(data[0])
        self._validate_columns(columns)
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
        if not updates:
            query += sql.SQL(" DO NOTHING")
            return self._execute_batch(
                query=query, values=self._prepare_batch_values(data)
            )
        # Rediscovery must not reset work in progress; the skipped revision is
        # found again by the next discovery after that work finishes.
        guards = sql.SQL(" AND ").join(
            sql.SQL("{} IS DISTINCT FROM {}").format(
                sql.Identifier(table, name), sql.Literal(db_constants['tasks']['states']['running'])
            )
            for name in db_constants['tasks']['running_file_status_fields'][table]
        )
        query += sql.SQL(" DO UPDATE SET {} WHERE {}").format(
            sql.SQL(", ").join(updates), guards
        )
        return self._execute_batch(query=query, values=self._prepare_batch_values(data))

    def _task_update_by_host(
        self,
        *,
        table: str,
        host_id: int,
        updates: Sequence[tuple[Mapping[str, object], Mapping[str, object]]],
        reason: str,
    ) -> int:
        """Apply host-scoped state transitions atomically and invalidate the host once.

        Args:
            table (str): HOST_TASK, FILE_TASK, or FILE_TASK_HISTORY.
            host_id (int): Parent host; added to every filter.
            updates (Sequence[tuple[Mapping[str, object], Mapping[str, object]]]):
                Ordered (data, filters) pairs, each one set-based UPDATE.
            reason (str): Summary invalidation reason when any row changed.

        Returns:
            int: Total matched rows across all updates.

        Raises:
            ValueError: If fields or filters are invalid.
            psycopg.Error: If any update fails; standalone calls roll back together.
        """
        with self._unit_of_work():
            affected = sum(
                self._update_batch(
                    table=table, data=data, where={"FK_HOST": host_id, **where}
                )
                for data, where in updates
            )
            if affected:
                self._summary_publish_host_scope(host_id, reason=reason)
            return affected

    def _dead_owner_filter(
        self,
        table: str,
        host_id: int,
        where: Mapping[str, object],
        *,
        id_column: str,
    ) -> DbRow:
        """Restrict RUNNING-task filters to rows whose owner process is gone.

        Age alone does not prove abandonment: a long transfer may still run.
        The returned filter keeps the original conditions, so a row claimed
        again after this read no longer matches.

        Args:
            table (str): HOST_TASK or FILE_TASK.
            host_id (int): Parent host.
            where (Mapping[str, object]): Filters selecting stale RUNNING rows.
            id_column (str): Task identity column selected by the domain caller.

        Returns:
            DbRow: The original filters plus the primary keys of rows with a
            missing or dead NU_PID; an empty key set matches no row.

        Raises:
            psycopg.Error: If reading the candidate rows fails.
        """
        rows = self._select_batch(
            table=table,
            where={"FK_HOST": host_id, **where},
            columns=(id_column, "NU_PID"),
        )
        dead = tuple(
            row[id_column]
            for row in rows
            if row["NU_PID"] is None or not tools.pid_exists(cast(int, row["NU_PID"]))
        )
        return {**where, f"{id_column}__in": dead}

    def _task_read_host_id(
        self, table: str, where: Mapping[str, object]
    ) -> int | None:
        """Resolve the parent host of an identified queue row before invalidation.

        Args:
            table (str): HOST_TASK or FILE_TASK.
            where (Mapping[str, object]): Identifying filters from the domain caller.

        Returns:
            int | None: FK_HOST, or None when the queue row is absent/unassigned.

        Raises:
            psycopg.Error: If scope lookup fails.
        """
        columns = ("FK_HOST",)
        row = self._select_row(table=table, where=where, columns=columns)
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
        reason: str | None = None,
    ) -> None:
        """Build host/month invalidation scopes and publish them through the base.

        Publication shares the caller's transaction and is isolated by a savepoint.

        Args:
            host_id (int | None): Host scope; None publishes nothing.
            reference_months (Sequence[object] | None): Date-like month scopes.
            reason (str | None): Diagnostic label limited to the outbox column size.

        Returns:
            None. Driver failures are reported as sanitized warnings; periodic
            reconciliation remains the recovery path for missing invalidations.
        """
        if host_id is None:
            return
        scopes = {
            (db_constants['summary']['scopes']['reference_month'], month)
            for value in (reference_months or ())
            if (month := self._normalize_summary_reference_month(value)) is not None
        }
        scopes.add((db_constants['summary']['scopes']['host'], str(host_id)))
        self._publish_summary_scopes(scopes, reason=reason)

    def _select_task_with_host(
        self,
        *,
        table: str,
        where: Mapping[str, object],
        host_where: Mapping[str, object],
        order_by: Sequence[tuple[str, str]],
    ) -> DbRow | None:
        """Read one task and its host through the base's simple JOIN operation.

        Args:
            table (str): HOST_TASK or FILE_TASK root table.
            where (Mapping[str, object]): Root task filters supported by CRUD.
            host_where (Mapping[str, object]): Host filters supported by CRUD.
            order_by (Sequence[tuple[str, str]]): Root-column/direction pairs.

        Returns:
            DbRow | None: One row keyed as TABLE__COLUMN and HOST__COLUMN, or None.

        Raises:
            ValueError: If filter structure or ordering is invalid.
            psycopg.Error: If reading the task or host fails.
        """
        rows = self._select_batch(
            table=table,
            where=where,
            prefix_tables=True,
            join=("HOST", "FK_HOST", "ID_HOST"),
            joined_where=host_where,
            order_by=order_by,
            limit=1,
        )
        return rows[0] if rows else None

    def _select_prioritized_file_task(
        self,
        *,
        where: Mapping[str, object],
        host_where: Mapping[str, object],
        fair_by_host: bool,
        reserve_discovery: bool,
        newest_first: bool,
        prefer_current_year: bool,
    ) -> DbRow | None:
        """Read one backup candidate with correlated fairness and reservation rules.

        A single statement keeps host eligibility, discovery reservations, and
        per-host candidate priority in the same database snapshot. The query
        shape is fixed here; the base validates filters and binds all values.

        Args:
            where (Mapping[str, object]): Nonempty declared FILE_TASK filters;
                the caller includes NU_TYPE for backup work.
            host_where (Mapping[str, object]): Declared HOST eligibility filters.
            fair_by_host (bool): Pick one candidate per host and rotate hosts.
            reserve_discovery (bool): Exclude hosts reserved by discovery work.
            newest_first (bool): Prefer newest source timestamp and task ID.
            prefer_current_year (bool): Prefer the current UTC year in fair mode.

        Returns:
            DbRow | None: Prefixed FILE_TASK__* and HOST__* fields, or None.

        Raises:
            ValueError: If required filters or their structure are invalid.
            psycopg.Error: If candidate selection fails.
        """
        if not where:
            raise ValueError("Prioritized file selection requires task filters.")
        filters, params = self._build_where_clause(
            where,
            alias="t",
        )
        host_filters, host_params = self._build_where_clause(
            host_where,
            alias="h",
            conjunction=True,
        )
        params += host_params
        query = (
            sql.SQL(
                'SELECT t.*, h.* FROM {} t JOIN {} h ON h."ID_HOST" = t."FK_HOST"'
            ).format(
                sql.Identifier(self.schema, "FILE_TASK"),
                sql.Identifier(self.schema, "HOST"),
            )
            + filters
            + host_filters
        )
        if reserve_discovery:
            query += sql.SQL(
                """ AND NOT EXISTS (
                    SELECT 1 FROM {} reservation
                    WHERE reservation."FK_HOST" = t."FK_HOST"
                      AND reservation."NU_TYPE" = %s
                      AND (reservation."NU_STATUS" = %s OR (
                          reservation."NU_STATUS" = %s AND (
                              reservation."DT_HOST_TASK" IS NULL OR
                              reservation."DT_HOST_TASK" >=
                                  (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - %s * INTERVAL '1 second'
                          )
                      ))
                )"""
            ).format(sql.Identifier(self.schema, "HOST_TASK"))
            params += (
                db_constants['tasks']['host_types']['processing'],
                db_constants['tasks']['states']['running'],
                db_constants['tasks']['states']['pending'],
                k.DISCOVERY_RESERVATION_TTL_SEC,
            )
        current_year = fair_by_host and prefer_current_year
        if fair_by_host:
            candidate_filters = sql.SQL(
                'candidate."FK_HOST" = t."FK_HOST" AND candidate."NU_TYPE" = t."NU_TYPE"'
            )
            if "NU_STATUS" in where:
                candidate_filters += sql.SQL(
                    ' AND candidate."NU_STATUS" = t."NU_STATUS"'
                )
            if "NA_EXTENSION_HOST" in where:
                candidate_filters += sql.SQL(
                    ' AND candidate."NA_EXTENSION_HOST" = t."NA_EXTENSION_HOST"'
                )
            candidate_order = self._file_task_build_order_by(
                "candidate", newest_first, current_year
            )
            query += sql.SQL(
                ' AND t."ID_FILE_TASK" = (SELECT candidate."ID_FILE_TASK" FROM {} candidate WHERE {} ORDER BY {} LIMIT %s)'
            ).format(
                sql.Identifier(self.schema, "FILE_TASK"),
                candidate_filters,
                candidate_order,
            )
            params += (1,)
        file_order = self._file_task_build_order_by("t", newest_first, False)
        if fair_by_host:
            order_parts: list[sql.Composable] = [
                sql.SQL('h."DT_LAST_BACKUP" ASC NULLS FIRST')
            ]
            if current_year:
                order_parts.insert(0, self._file_task_build_current_year_order("t"))
            order = sql.SQL(", ").join((*order_parts, file_order))
        else:
            order = file_order if newest_first else sql.SQL('t."ID_FILE_TASK" ASC')
        query += sql.SQL(" ORDER BY {} LIMIT %s").format(order)
        rows = self._select_raw(
            query=query, params=(*params, 1), table_prefixes=("FILE_TASK", "HOST")
        )
        return rows[0] if rows else None
