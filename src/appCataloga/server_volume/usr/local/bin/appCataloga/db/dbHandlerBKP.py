
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BKP-domain database handler for host and file orchestration.

`dbHandlerBKP` owns the operational side of appCataloga: host registration,
host-level tasks, per-file tasks, immutable file history, backlog promotion,
and stale-lock recovery. It sits on top of `DBHandlerBase`, which provides the
generic SQL and connection helpers.
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime, timedelta
import json

# =================================================
# PROJECT ROOT (shared/, db/, stations/)
# =================================================
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# =================================================
# Project imports
# =================================================
import config as k
from .dbHandlerBase import DBHandlerBase
from shared import errors, filter, constants, tools
from shared.file_metadata import FileMetadata


class dbHandlerBKP(DBHandlerBase):
    """
    BKP-domain handler for hosts, host tasks, file tasks, and file history.

    Scope:
        - `HOST`: operational identity, access data, and counters
        - `HOST_TASK`: host-level orchestration and maintenance work
        - `FILE_TASK`: live per-file queue rows
        - `FILE_TASK_HISTORY`: durable per-file audit trail

    The class stays at the BKP-domain boundary and relies on
    `DBHandlerBase` for the generic SQL primitives.
    """
    # Table field definitions used both for validation and for automatic
    # column expansion in `DBHandlerBase._select_custom()`.
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
        # FILE TASK STATISTICS
        "VL_PENDING_BACKUP_KB",
        "VL_DONE_BACKUP_KB",
        "NU_PENDING_FILE_BACKUP_TASKS",
        "NU_DONE_FILE_BACKUP_TASKS",
        "NU_ERROR_FILE_BACKUP_TASKS",
        "NU_PENDING_FILE_PROCESS_TASKS",
        "NU_DONE_FILE_PROCESS_TASKS",
        "NU_ERROR_FILE_PROCESS_TASKS",
        "NU_DONE_FILE_DISCOVERY_TASKS",
        "NU_ERROR_FILE_DISCOVERY_TASKS",
        # HOST STATISTICS
        "NU_HOST_FILES",
        "NU_HOST_CHECK_ERROR",
    }


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
        "NA_EXTENSION",
        "VL_FILE_SIZE_KB",
        "DT_FILE_CREATED",
        "DT_FILE_MODIFIED",
        "NA_MESSAGE",
        "NA_ERROR_DOMAIN",
        "NA_ERROR_STAGE",
        "NA_ERROR_CODE",
        "NA_ERROR_SUMMARY",
        "NA_ERROR_DETAIL",
        "NU_ERROR_CLASSIFIER_VERSION",
    }

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
        "VL_FILE_SIZE_KB",
        "DT_FILE_CREATED",
        "DT_FILE_MODIFIED",
        "NA_EXTENSION",
        "NA_MESSAGE",
        "NA_ERROR_DOMAIN",
        "NA_ERROR_STAGE",
        "NA_ERROR_CODE",
        "NA_ERROR_SUMMARY",
        "NA_ERROR_DETAIL",
        "NU_ERROR_CLASSIFIER_VERSION",
        "IS_PAYLOAD_DELETED",
        "DT_PAYLOAD_DELETED",
    }
    
    # ======================================================================
    # Initialization
    # ======================================================================
    def __init__(self, database: str, log: Any):
        """Initialize the BKP handler with the target logical database and logger.

        Args:
            database (str): Logical database key. Resolved via `config.DB` mapping.
            log (Any): Logger implementing `.entry()`, `.warning()`, `.error()`.

        Raises:
            Exception: If initialization logic needs to validate inputs.
        """
        super().__init__(database=database, log=log)
        self.log.entry(f"[dbHandlerBKP] Initialized for DB '{database}'")

    def _merge_structured_error_fields(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Keep explicit error columns synchronized with `NA_MESSAGE`.

        FILE_TASK and FILE_TASK_HISTORY still persist the human-readable audit
        message, but downstream consumers should read the structured columns
        whenever possible instead of reparsing text repeatedly.
        """
        normalized = dict(payload)

        if "NA_MESSAGE" in normalized:
            normalized.update(
                errors.classify_persisted_error_message(normalized.get("NA_MESSAGE"))
            )
            return normalized

        if "NA_MESSAGE__expr" in normalized:
            normalized.update(errors.empty_persisted_error_fields(classified=True))
            return normalized

        return normalized

    # ======================================================================
    # HOST OPERATIONS
    # ======================================================================
    
    def host_upsert(self, **kwargs) -> None:
        """
        Create or refresh a `HOST` row with safe UPSERT semantics.

        This helper is intentionally conservative: it writes only the columns
        explicitly provided by the caller. Operational fields such as
        `IS_OFFLINE` must not be reset implicitly during routine access-data
        refreshes performed by the TCP entrypoint.

        MariaDB schema defaults remain responsible for first-insert
        initialization when the caller omits a column.
        """

        try:
            self._connect()

            # ------------------------------------------------------------------
            # Validation
            # ------------------------------------------------------------------
            valid_fields = self.VALID_FIELDS_HOST
            if valid_fields is None:
                raise ValueError("VALID_FIELDS_HOST not defined in DB handler.")

            for key in kwargs.keys():
                if key not in valid_fields:
                    raise ValueError(f"Invalid field '{key}' for HOST table.")

            # The caller decides exactly which fields should be refreshed.
            # This avoids resetting operational state like `IS_OFFLINE` every
            # time a new request arrives for an existing host.
            data = dict(kwargs)

            # ------------------------------------------------------------------
            # Perform UPSERT
            # ------------------------------------------------------------------
            self._upsert_row(
                table="HOST",
                data=data,
                unique_keys="ID_HOST",   # ensure this is your PK
                commit=True
            )

            self.log.entry(
                f"[DBHandlerBKP] HOST {data.get('ID_HOST')} ({data.get('NA_HOST_UID')}) "
                f"created or updated successfully."
            )

        except Exception as e:
            self.log.error(f"[DBHandlerBKP] Failed to upsert HOST record: {e}")
            raise

        finally:
            self._disconnect()
    
    def host_read_access(self, host_id: int) -> Dict[str, Any]:
        """Read connection credentials and network parameters for a host.

        Args:
            host_id (int): `HOST.ID_HOST` primary key.

        Returns:
            Dict[str, Any]: A dictionary with fields:
                - host_id (int)
                - host_uid (str)
                - host_addr (str): IP / DNS
                - port (int)
                - user (str)
                - password (str)
            Returns an empty dict if no row exists.

        Raises:
            mysql.connector.Error: On SELECT failure.
        """
        self._connect()
        try:
            rows = self._select_rows(
                table="HOST",
                where={"ID_HOST": host_id},
                limit=1,
                cols=[
                    "ID_HOST AS host_id",
                    "NA_HOST_NAME AS host_uid",
                    "NA_HOST_ADDRESS AS host_addr",
                    "NA_HOST_PORT AS port",
                    "NA_HOST_USER AS user",
                    "NA_HOST_PASSWORD AS password",
                ],
            )
            return rows[0] if rows else {}
        finally:
            self._disconnect()

    def host_read_status(self, host_id: int) -> Dict[str, Any]:
        """
        Read the full operational snapshot of a host.

        The column list is derived from `VALID_FIELDS_HOST` so the method stays
        aligned with the schema contract used elsewhere in the handler.

        Args:
            host_id (int): `HOST.ID_HOST`.

        Returns:
            Dict[str, Any]: Dictionary containing all available columns for the host.
                Includes a derived field `status` (1 if found, else 0).

        Raises:
            ValueError: If `valid_fields` for the HOST table are undefined.
            mysql.connector.Error: On SELECT failure.
        """
        self._connect()
        try:
            # Read the full HOST contract instead of maintaining a second
            # handwritten column list just for the status endpoint.
            valid_fields = self.VALID_FIELDS_HOST
            if not valid_fields:
                raise ValueError("Valid fields for HOST table not defined in handler.")

            rows = self._select_rows(
                table="HOST",
                where={"ID_HOST": host_id},
                limit=1,
                cols=list(valid_fields),
            )

            if not rows:
                return {"status": 0}

            row = rows[0]

            # Frontends and lightweight scripts consume this snapshot more
            # easily when all datetime fields are converted consistently.
            for key, val in row.items():
                if key.startswith("DT_") and val:
                    try:
                        row[key] = int(val.timestamp())
                    except Exception:
                        pass  # Keep as-is if conversion fails

            row["status"] = 1
            return row

        finally:
            self._disconnect()

    def host_list_for_connectivity_check(self) -> List[Dict[str, Any]]:
        """
        Return lightweight host rows ordered by the oldest connectivity snapshot.

        The background connectivity sweep reads the whole HOST table in memory
        because the dataset is small and the logic benefits from simple
        oldest-first scheduling.
        """
        self._connect()
        try:
            return self._select_rows(
                table="HOST",
                where={
                    "#CUSTOM#HOST_ADDRESS": (
                        "NA_HOST_ADDRESS IS NOT NULL "
                        "AND TRIM(NA_HOST_ADDRESS) <> ''"
                    ),
                },
                order_by="DT_LAST_CHECK IS NULL DESC, DT_LAST_CHECK ASC, ID_HOST ASC",
                cols=[
                    "ID_HOST",
                    "NA_HOST_NAME",
                    "NA_HOST_ADDRESS",
                    "NA_HOST_PORT",
                    "NA_HOST_USER",
                    "NA_HOST_PASSWORD",
                    "IS_BUSY",
                    "IS_OFFLINE",
                    "DT_LAST_CHECK",
                ],
            )
        finally:
            self._disconnect()

    def get_last_discovery(self, host_id: int) -> Optional[datetime]:
        """Return the last successful discovery timestamp recorded for a host."""

        self._connect()
        try:
            rows = self._select_rows(
                table="HOST",
                where={"ID_HOST": host_id},
                cols=["DT_LAST_DISCOVERY"],
                limit=1,
            )
        except Exception as e:
            self.log.error(f"[DB] Failed to read DT_LAST_DISCOVERY: {e}")
            return None

        if not rows:
            return None

        dt = rows[0].get("DT_LAST_DISCOVERY")
        return dt if dt else None

    
    def host_release_by_pid(self, pid: int) -> None:
        """
        Release all `HOST` locks owned by the given PID.

        This helper is intentionally idempotent and is mainly used during
        worker shutdown and crash recovery.
        """
        self._connect()
        self._update_row(
            table="HOST",
            data={
                "IS_BUSY": False,
                "NU_PID": k.HOST_UNLOCKED_PID,
            },
            where={
                "NU_PID": pid,
            },
        )
        
        self._disconnect()


    def host_update(
        self,
        host_id: int,
        reset: bool = False,
        check_busy_timeout: bool = False,
        busy_timeout_seconds: int = k.HOST_BUSY_TIMEOUT,
        **kwargs
    ) -> None:
        """
        Update a host row with optional arithmetic semantics.

        By default, numeric values are treated as deltas. This keeps counter
        maintenance concise at call sites. Pass `reset=True` to switch to
        direct assignment semantics. Explicit arithmetic tuples
        `("INC", x)` / `("DEC", x)` are also supported.

        Important:
            Passing `NU_HOST_CHECK_ERROR=1` with the default `reset=False`
            increments the stored counter by one; it does not assign the
            literal value `1`. Callers that already computed the absolute
            target value must use `reset=True`.
        """
        self._connect()

        # Optional stale-lock recovery can piggyback on the same update call.
        if check_busy_timeout:
            row = self._select_rows(
                table="HOST",
                where={"ID_HOST": host_id},
                cols=["IS_BUSY", "DT_BUSY"],
                limit=1
            )

            if row:
                row = row[0]
                if row["IS_BUSY"] and row["DT_BUSY"]:
                    elapsed = (datetime.now() - row["DT_BUSY"]).total_seconds()
                    if elapsed > busy_timeout_seconds:
                        self.log.entry(
                            f"[DBHandlerBKP] HOST {host_id} exceeded busy-timeout "
                            f"({elapsed:.1f}s > {busy_timeout_seconds}s). Forcing release."
                        )
                        kwargs["IS_BUSY"] = False
                        kwargs["DT_BUSY"] = None
                        kwargs["NU_PID"]  = k.HOST_UNLOCKED_PID

        if not kwargs:
            self.log.warning(f"No fields provided for host_update (ID={host_id}).")
            return

        for key in kwargs.keys():
            if key not in self.VALID_FIELDS_HOST:
                raise ValueError(f"Invalid field '{key}' for HOST table update.")

        # Split the update into direct assignments and arithmetic fragments so
        # we can preserve the legacy "increment by default" behavior safely.
        arithmetic_updates = []
        direct_updates = {}

        for field, value in kwargs.items():

            if value is None:
                continue

            if isinstance(value, tuple) and len(value) == 2:
                cmd, num = value

                if cmd == "INC":
                    arithmetic_updates.append(f"{field} = {field} + %s")
                    arithmetic_updates.append(("PARAM", num))
                    continue

                if cmd == "DEC":
                    arithmetic_updates.append(f"{field} = {field} - %s")
                    arithmetic_updates.append(("PARAM", num))
                    continue

                # Unknown tuple commands fall back to direct assignment.
                direct_updates[field] = value
                continue

            if reset or isinstance(value, (bool, datetime)):
                direct_updates[field] = value
                continue

            if isinstance(value, (int, float)):
                if reset:
                    direct_updates[field] = value
                elif value > 0:
                    arithmetic_updates.append(f"{field} = {field} + {value}")
                elif value < 0:
                    arithmetic_updates.append(f"{field} = {field} - {abs(value)}")
                else:
                    direct_updates[field] = 0
                continue

            direct_updates[field] = value

        try:
            if direct_updates:
                self._update_row(
                    table="HOST",
                    data=direct_updates,
                    where={"ID_HOST": host_id},
                    commit=False,
                )

            # Arithmetic updates stay explicit SQL because `_update_row`
            # intentionally models assignments, not expressions.
            if arithmetic_updates:
                set_parts = []
                params = []

                for item in arithmetic_updates:
                    if isinstance(item, tuple) and item[0] == "PARAM":
                        params.append(item[1])
                    else:
                        set_parts.append(item)

                sql = f"UPDATE HOST SET {', '.join(set_parts)} WHERE ID_HOST = %s"
                params.append(host_id)

                self.cursor.execute(sql, tuple(params))

            self.db_connection.commit()
            self.log.entry(f"[DBHandlerBKP] HOST {host_id} updated successfully.")
            self._disconnect()
        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBKP] host_update failed for HOST {host_id}: {e}")
            self._disconnect()
            raise

    
    
    def host_update_statistics(self, host_id: int) -> None:
        """
        Recalculate host statistics from `FILE_TASK_HISTORY` only.

        This method deliberately treats history as the authoritative source for
        counters and phase timestamps. Live task tables remain transient
        orchestration state; the durable aggregate belongs to history.
        """

        self._connect()
        try:
            # Counters and phase timestamps come directly from history.
            sql_hist = """
                SELECT
                    -- DONE
                    SUM(NU_STATUS_DISCOVERY  = 0) AS total_discovered,
                    SUM(NU_STATUS_BACKUP     = 0) AS total_backup,
                    SUM(NU_STATUS_PROCESSING = 0) AS total_processed,

                    -- PENDING
                    SUM(NU_STATUS_BACKUP     = 1) AS pending_backup,
                    SUM(NU_STATUS_PROCESSING = 1) AS pending_process,

                    -- ERROR
                    SUM(NU_STATUS_DISCOVERY  = -1) AS error_discovery,
                    SUM(NU_STATUS_BACKUP     = -1) AS error_backup,
                    SUM(NU_STATUS_PROCESSING = -1) AS error_process,

                    -- TIMESTAMPS
                    MAX(DT_DISCOVERED) AS last_discovered,
                    MAX(DT_BACKUP)     AS last_backup,
                    MAX(DT_PROCESSED)  AS last_processed
                FROM FILE_TASK_HISTORY
                WHERE FK_HOST = %s;
            """

            row = self._select_raw(sql_hist, (host_id,))
            hist = row[0] if row else {}

            # Aggregate queries return NULL when no history exists yet.
            total_discovered = hist.get("total_discovered") or 0
            total_backup     = hist.get("total_backup")     or 0
            total_processed  = hist.get("total_processed")  or 0

            pending_backup   = hist.get("pending_backup")   or 0
            pending_process  = hist.get("pending_process")  or 0

            error_discovery  = hist.get("error_discovery")  or 0
            error_backup     = hist.get("error_backup")     or 0
            error_process    = hist.get("error_process")    or 0

            last_discovered  = hist.get("last_discovered")
            last_backup      = hist.get("last_backup")
            last_processed   = hist.get("last_processed")

            # Volume counters are derived separately because pending and done
            # sizes follow different status predicates.
            sql_volume = """
                SELECT
                    SUM(
                        CASE
                            WHEN NU_STATUS_DISCOVERY = 0
                            AND NU_STATUS_BACKUP = 1
                            THEN VL_FILE_SIZE_KB
                            ELSE 0
                        END
                    ) AS pending_kb,

                    SUM(
                        CASE
                            WHEN NU_STATUS_BACKUP = 0
                            THEN VL_FILE_SIZE_KB
                            ELSE 0
                        END
                    ) AS done_kb
                FROM FILE_TASK_HISTORY
                WHERE FK_HOST = %s;
            """

            row2 = self._select_raw(sql_volume, (host_id,))
            vol = row2[0] if row2 else {}

            pending_kb = vol.get("pending_kb") or 0
            done_kb    = vol.get("done_kb")    or 0

            self.host_update(
                host_id=host_id,
                reset=False,

                # DONE
                NU_DONE_FILE_DISCOVERY_TASKS=total_discovered,
                NU_DONE_FILE_BACKUP_TASKS=total_backup,
                NU_DONE_FILE_PROCESS_TASKS=total_processed,

                # PENDING
                NU_PENDING_FILE_BACKUP_TASKS=pending_backup,
                NU_PENDING_FILE_PROCESS_TASKS=pending_process,

                # ERROR
                NU_ERROR_FILE_DISCOVERY_TASKS=error_discovery,
                NU_ERROR_FILE_BACKUP_TASKS=error_backup,
                NU_ERROR_FILE_PROCESS_TASKS=error_process,

                # TIMESTAMPS
                DT_LAST_DISCOVERY=last_discovered,
                DT_LAST_BACKUP=last_backup,
                DT_LAST_PROCESSING=last_processed,

                # VOLUME
                VL_PENDING_BACKUP_KB=pending_kb,
                VL_DONE_BACKUP_KB=done_kb,
            )

        finally:
            self._disconnect()
            
            
    def host_release_safe(self, host_id: int, current_pid: int):
        """
        Release a host lock only when it is safe to do so.

        Decision order:
            1. already free -> no-op
            2. transient cooldown -> preserve
            3. owned by current worker -> release
            4. stale PID -> release
            5. active foreign owner -> preserve
        """

        host_read = self.host_read_status(host_id=host_id)

        if not host_read:
            self.log.warning(f"[CLEANUP] Host not found (host_id={host_id})")
            return

        is_busy = host_read.get("IS_BUSY")
        pid = host_read.get("NU_PID")

        if not is_busy:
            self.log.entry(f"[CLEANUP] Host already released (host_id={host_id})")
            return

        # `HOST_TRANSIENT_BUSY_PID` means "temporarily quarantined after a
        # transient SFTP bootstrap error", not "free to release right now".
        if pid == k.HOST_TRANSIENT_BUSY_PID:
            self.log.entry(
                f"[CLEANUP] Preserving transient host cooldown "
                f"(host_id={host_id})"
            )
            return

        if pid == current_pid:
            self.log.warning(
                f"[CLEANUP] Releasing host lock owned by this worker "
                f"(host_id={host_id}, pid={current_pid})"
            )

            self.host_update(
                host_id=host_id,
                IS_BUSY=False,
                NU_PID=k.HOST_UNLOCKED_PID,
            )
            return

        # Stale ownership is safe to recover because no live process can still
        # legitimately hold the lock.
        if not tools.pid_exists(pid):
            self.log.warning(
                f"[CLEANUP] Stale PID detected (host_id={host_id}, pid={pid}). "
                f"Releasing lock."
            )

            self.host_update(
                host_id=host_id,
                IS_BUSY=False,
                NU_PID=k.HOST_UNLOCKED_PID,
            )
            return

        self.log.entry(
            f"[CLEANUP] Host lock owned by another worker "
            f"(host_id={host_id}, owner_pid={pid})"
        )

    def host_start_transient_busy_cooldown(
        self,
        host_id: int,
        *,
        owner_pid: Optional[int] = None,
        cooldown_seconds: int = k.SFTP_BUSY_COOLDOWN_SECONDS,
    ) -> bool:
        """
        Convert a worker-owned HOST lock into a short transient cooldown.

        This is used only after SSH/SFTP bootstrap errors such as
        `NoValidConnectionsError`. The host remains BUSY for a few seconds so
        discovery and backup do not ping-pong on the same remote endpoint.

        Args:
            host_id (int):
                Target HOST identifier.
            owner_pid (Optional[int]):
                PID expected to own the current BUSY lock. Defaults to the
                current process PID.
            cooldown_seconds (int):
                Cooldown duration used for observability only. The actual
                release is driven by `DT_BUSY`.

        Returns:
            bool: True when the lock was successfully converted to cooldown.
        """

        owner_pid = owner_pid or os.getpid()
        self._connect()

        try:
            affected = self._update_row(
                table="HOST",
                data={
                    "IS_BUSY": True,
                    "DT_BUSY": datetime.now(),
                    "NU_PID": k.HOST_TRANSIENT_BUSY_PID,
                },
                where={
                    "ID_HOST": host_id,
                    "IS_BUSY": True,
                    "NU_PID": owner_pid,
                },
                commit=True,
            )

            if affected == 1:
                self.log.entry(
                    f"[HOST_COOLDOWN] Started transient SFTP cooldown "
                    f"(host_id={host_id}, cooldown_seconds={cooldown_seconds})"
                )
                return True

            self.log.warning(
                f"[HOST_COOLDOWN] Failed to start cooldown because host lock "
                f"ownership changed (host_id={host_id}, owner_pid={owner_pid})"
            )
            return False

        finally:
            self._disconnect()

    def _release_expired_transient_busy_cooldowns(
        self,
        cooldown_seconds: int = k.SFTP_BUSY_COOLDOWN_SECONDS,
    ) -> int:
        """
        Release HOST rows whose transient SFTP cooldown window already expired.

        The method assumes an open DB connection and is intentionally called
        inline by the task-selection methods so successful hosts can be retried
        immediately after the short cooldown window ends.
        """

        threshold_time = datetime.now() - timedelta(seconds=cooldown_seconds)

        affected = self._update_row(
            table="HOST",
            data={
                "IS_BUSY": False,
                "DT_BUSY": None,
                "NU_PID": k.HOST_UNLOCKED_PID,
            },
            where={
                "IS_BUSY": True,
                "NU_PID": k.HOST_TRANSIENT_BUSY_PID,
                "DT_BUSY__lt": threshold_time,
            },
            commit=True,
        )

        if affected:
            self.log.entry(
                f"[HOST_COOLDOWN] Released {affected} expired transient "
                f"SFTP cooldown host lock(s)"
            )

        return affected

    def host_cleanup_stale_locks(self, threshold_seconds: int) -> None:
        """
        Release `HOST` rows that are stuck in BUSY state.

        This janitor focuses on BUSY hosts that no longer have matching live
        work. The two main recovery paths are:
            - no running file/processing work remains after a grace window
            - BUSY timeout elapsed and the owner PID is stale

        Recovered hosts are released and scheduled for a connection check so
        the orchestration layer can reconcile state safely.
        """
        now = datetime.now()
        self._connect()
        try:
            rows = self._select_raw("""
                SELECT
                    H.ID_HOST,
                    H.NA_HOST_NAME,
                    H.DT_BUSY,
                    H.NU_PID,
                    EXISTS(
                        SELECT 1
                        FROM FILE_TASK FT
                        WHERE FT.FK_HOST = H.ID_HOST
                        AND FT.NU_STATUS = %s
                    ) AS FILE_RUNNING,
                    EXISTS(
                        SELECT 1
                        FROM HOST_TASK HT
                        WHERE HT.FK_HOST = H.ID_HOST
                        AND HT.NU_STATUS = %s
                        AND HT.NU_TYPE = %s
                    ) AS HOST_PROCESSING_RUNNING
                FROM HOST H
                WHERE H.IS_BUSY = TRUE
            """, (
                k.TASK_RUNNING,
                k.TASK_RUNNING,
                k.HOST_TASK_PROCESSING_TYPE,
            ))
        finally:
            self._disconnect()

        if not rows:
            return

        self.log.entry(
            f"[HOST_CLEANUP] Evaluating {len(rows)} busy hosts"
        )

        for row in rows:

            host_id = row["ID_HOST"]
            host_name = row["NA_HOST_NAME"]
            busy_since = row["DT_BUSY"]
            pid = row["NU_PID"]

            file_running = row["FILE_RUNNING"]
            host_processing_running = row["HOST_PROCESSING_RUNNING"]

            if busy_since is None:
                elapsed = float("inf")
            else:
                elapsed = (now - busy_since).total_seconds()

            release = False
            reason = None

            # `HOST_TRANSIENT_BUSY_PID` is a short transient SFTP quarantine,
            # not a stale worker lock. Preserve it only for the configured
            # cooldown window.
            if pid == k.HOST_TRANSIENT_BUSY_PID and elapsed <= k.SFTP_BUSY_COOLDOWN_SECONDS:
                self.log.entry(
                    f"[HOST_CLEANUP] Preserving transient SFTP cooldown "
                    f"(host_id={host_id}, host={host_name}, busy_for={elapsed:.1f}s)"
                )
                continue

            if pid == k.HOST_TRANSIENT_BUSY_PID:
                release = True
                reason = "SFTP_BUSY_COOLDOWN_EXPIRED"

            elif (
                not file_running
                and not host_processing_running
                and elapsed > k.HOST_CLEANUP_NO_TASK_GRACE_SEC
            ):
                release = True
                reason = "NO_RUNNING_TASKS"

            elif not file_running and not host_processing_running:
                self.log.entry(
                    f"[HOST_CLEANUP] Preserving recently claimed busy host "
                    f"(host_id={host_id}, host={host_name}, pid={pid}, "
                    f"busy_for={elapsed:.1f}s, grace={k.HOST_CLEANUP_NO_TASK_GRACE_SEC}s)"
                )

            elif elapsed > threshold_seconds and (
                pid in (None, k.HOST_UNLOCKED_PID) or not tools.pid_exists(pid)
            ):
                release = True
                reason = "BUSY_TIMEOUT_STALE_PID"

            elif elapsed > threshold_seconds:
                self.log.warning(
                    f"[HOST_CLEANUP] Preserving long-running busy host "
                    f"(host_id={host_id}, host={host_name}, pid={pid}, "
                    f"busy_for={elapsed:.1f}s)"
                )

            if not release:
                continue

            self.log.warning(
                f"[HOST_CLEANUP] Releasing stale host "
                f"(host_id={host_id}, host={host_name}, "
                f"pid={pid}, busy_for={elapsed:.1f}s, reason={reason})"
            )

            try:
                self.host_update(
                    host_id=host_id,
                    IS_BUSY=False,
                    NU_PID=k.HOST_UNLOCKED_PID
                )
            except Exception as e:
                self.log.error(
                    f"[HOST_CLEANUP] Failed to release host "
                    f"(host_id={host_id}): {e}"
                )
                continue

            # A recovered host should go back through connectivity validation
            # before new host-dependent work is claimed.
            try:
                self._connect()
                self.queue_host_task(
                    host_id=host_id,
                    task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE,
                    task_status=k.TASK_PENDING,
                    filter_dict=k.NONE_FILTER,
                )
                self._disconnect()

                self.log.entry(
                    f"[HOST_CLEANUP] Connection check scheduled "
                    f"(host_id={host_id})"
                )

            except Exception as e:
                self._disconnect()
                self.log.error(
                    f"[HOST_CLEANUP] Failed to queue connection check "
                    f"(host_id={host_id}): {e}"
                )

    def host_task_cleanup_stale_operational_tasks(
        self,
        stale_after_seconds: int,
    ) -> None:
        """
        Recover stale queue-owned HOST_TASK rows without forcing SSH preemption.

        This janitor focuses on HOST_TASK state itself:
            - PENDING rows with missing `DT_HOST_TASK` get normalized so the
              discovery reservation TTL can expire deterministically.
            - RUNNING rows older than `stale_after_seconds` are reset to
              PENDING when execution ownership no longer matches reality
              (host unlocked, stale PID, or mismatched host/task owner).

        Host release remains conservative: only obviously stale host locks are
        cleared here. DB-only backlog tasks are also recovered by this janitor,
        but they never claim `HOST.IS_BUSY` in the first place.
        """
        now = datetime.now()
        operational_types = (
            k.HOST_TASK_CHECK_TYPE,
            k.HOST_TASK_PROCESSING_TYPE,
            k.HOST_TASK_CHECK_CONNECTION_TYPE,
            k.HOST_TASK_BACKLOG_CONTROL_TYPE,
            k.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
        )

        self._connect()
        try:
            rows = self._select_raw("""
                SELECT
                    HT.ID_HOST_TASK,
                    HT.FK_HOST,
                    HT.NU_TYPE,
                    HT.NU_STATUS,
                    HT.DT_HOST_TASK,
                    HT.NU_PID,
                    H.IS_BUSY,
                    H.NU_PID AS HOST_OWNER_PID
                FROM HOST_TASK HT
                JOIN HOST H ON H.ID_HOST = HT.FK_HOST
                WHERE HT.NU_TYPE IN (%s, %s, %s, %s, %s)
                  AND HT.NU_STATUS IN (%s, %s)
            """, (
                *operational_types,
                k.TASK_PENDING,
                k.TASK_RUNNING,
            ))
        finally:
            self._disconnect()

        if not rows:
            return

        self.log.entry(
            f"[HOST_TASK_CLEANUP] Evaluating {len(rows)} active operational HOST_TASK entries"
        )

        for row in rows:
            task_id = row["ID_HOST_TASK"]
            host_id = row["FK_HOST"]
            task_type = row["NU_TYPE"]
            status = row["NU_STATUS"]
            task_started_at = row["DT_HOST_TASK"]
            task_pid = row["NU_PID"]
            host_is_busy = bool(row["IS_BUSY"])
            host_owner_pid = row["HOST_OWNER_PID"]

            # Legacy PENDING rows without timestamps would otherwise never age
            # out of reservation logic deterministically.
            if status == k.TASK_PENDING and task_started_at is None:
                try:
                    self.host_task_update(
                        task_id=task_id,
                        DT_HOST_TASK=now,
                        NA_MESSAGE=(
                            "Operational HOST_TASK timestamp normalized by janitor "
                            "for reservation TTL control"
                        ),
                    )
                except Exception as e:
                    self.log.error(
                        f"[HOST_TASK_CLEANUP] Failed to normalize pending HOST_TASK "
                        f"(task_id={task_id}, host_id={host_id}, type={task_type}): {e}"
                    )
                continue

            if status != k.TASK_RUNNING:
                continue

            elapsed = (
                float("inf")
                if task_started_at is None
                else (now - task_started_at).total_seconds()
            )

            if elapsed <= stale_after_seconds:
                continue

            task_pid_alive = bool(task_pid) and tools.pid_exists(task_pid)
            host_owner_alive = bool(host_owner_pid) and tools.pid_exists(host_owner_pid)

            # Legitimate long-running execution still owns both the task and
            # the host with the same live PID.
            if (
                host_is_busy
                and task_pid_alive
                and host_owner_pid == task_pid
            ):
                self.log.entry(
                    f"[HOST_TASK_CLEANUP] Preserving active long-running HOST_TASK "
                    f"(task_id={task_id}, host_id={host_id}, type={task_type}, "
                    f"busy_for={elapsed:.1f}s)"
                )
                continue

            reasons = []
            if not host_is_busy:
                reasons.append("HOST_NOT_BUSY")
            if task_pid in (None, k.HOST_UNLOCKED_PID):
                reasons.append("TASK_PID_MISSING")
            elif not task_pid_alive:
                reasons.append("TASK_PID_STALE")
            if host_is_busy and host_owner_pid not in (None, k.HOST_UNLOCKED_PID):
                if not host_owner_alive:
                    reasons.append("HOST_PID_STALE")
                elif task_pid not in (None, k.HOST_UNLOCKED_PID) and host_owner_pid != task_pid:
                    reasons.append("HOST_PID_MISMATCH")
            elif host_is_busy:
                reasons.append("HOST_PID_MISSING")

            if not reasons:
                reasons.append("STALE_RUNNING_TTL_EXPIRED")

            self.log.warning(
                f"[HOST_TASK_CLEANUP] Recovering stale operational HOST_TASK "
                f"(task_id={task_id}, host_id={host_id}, type={task_type}, "
                f"busy_for={elapsed:.1f}s, reasons={','.join(reasons)})"
            )

            try:
                self.host_task_update(
                    task_id=task_id,
                    NU_STATUS=k.TASK_PENDING,
                    DT_HOST_TASK=now,
                    NA_MESSAGE=(
                        "Stale operational HOST_TASK recovered by janitor "
                        f"({', '.join(reasons)})"
                    ),
                )
            except Exception as e:
                self.log.error(
                    f"[HOST_TASK_CLEANUP] Failed to recover HOST_TASK "
                    f"(task_id={task_id}, host_id={host_id}): {e}"
                )
                continue

            should_release_host = (
                host_is_busy
                and (
                    host_owner_pid in (None, k.HOST_UNLOCKED_PID)
                    or not host_owner_alive
                    or (
                        task_pid not in (None, k.HOST_UNLOCKED_PID)
                        and host_owner_pid == task_pid
                        and not task_pid_alive
                    )
                )
            )

            if not should_release_host:
                continue

            try:
                self.host_update(
                    host_id=host_id,
                    IS_BUSY=False,
                    NU_PID=k.HOST_UNLOCKED_PID,
                )
                self.log.warning(
                    f"[HOST_TASK_CLEANUP] Released stale host lock while recovering "
                    f"HOST_TASK (task_id={task_id}, host_id={host_id})"
                )
            except Exception as e:
                self.log.error(
                    f"[HOST_TASK_CLEANUP] Failed to release stale host lock "
                    f"(task_id={task_id}, host_id={host_id}): {e}"
                )
        
    # ======================================================================
    # HOST_TASK OPERATIONS
    # ======================================================================
    def check_host_task(self, **kwargs) -> list[dict]:
        """
        Query HOST_TASK using dynamic filters.

        This helper is intentionally generic. Callers are responsible for
        deciding whether they care about active rows, terminal rows, or both.

        Returns:
            list[dict]: Matching HOST_TASK rows (may be empty).
        """

        valid_fields = self.VALID_FIELDS_HOST_TASK
        where_clause = {}

        for key, value in kwargs.items():

            if key not in valid_fields:
                raise ValueError(
                    f"Invalid field in check_host_task(): '{key}'"
                )

            if isinstance(value, (list, tuple, set)):
                where_clause[key] = ("IN", list(value))
            else:
                where_clause[key] = value

        rows = self._select_custom(
            table="HOST_TASK ht",
            where=where_clause,
            order_by="ht.ID_HOST_TASK DESC",
        )

        return rows or []

    def _serialize_host_task_filter(self, filter_value: Any) -> str:
        """
        Normalize and serialize a HOST_TASK filter for DB persistence.

        The operational queue treats FILTER as durable state, so inserts and
        updates must store it canonically to avoid accidental churn caused by
        JSON key ordering differences.
        """
        if filter_value is None:
            normalized = dict(k.NONE_FILTER)
        elif isinstance(filter_value, (bytes, bytearray)):
            normalized = json.loads(filter_value.decode("utf-8"))
        elif isinstance(filter_value, dict):
            normalized = dict(filter_value)
        elif isinstance(filter_value, str):
            try:
                normalized = json.loads(filter_value)
            except json.JSONDecodeError:
                raise ValueError("FILTER must be a valid JSON string or dict.")
        else:
            normalized = filter_value

        return json.dumps(normalized, sort_keys=True, ensure_ascii=False)

    def _find_reusable_singleton_host_task(
        self,
        tasks: list[dict],
    ) -> tuple[Optional[dict], int]:
        """
        Pick the best reusable row for the singleton-per-type HOST_TASK contract.

        Preference order:
            1. PENDING task
            2. RUNNING task
            3. Otherwise the newest terminal row
        """
        if not tasks:
            return None, 0

        pending = next(
            (
                task
                for task in tasks
                if task.get("HOST_TASK__NU_STATUS") == k.TASK_PENDING
            ),
            None,
        )
        if pending:
            return pending, len(tasks)

        running = next(
            (
                task
                for task in tasks
                if task.get("HOST_TASK__NU_STATUS") == k.TASK_RUNNING
            ),
            None,
        )
        if running:
            return running, len(tasks)

        return tasks[0], len(tasks)

    def queue_host_task(
        self,
        host_id: int,
        task_type: int,
        task_status: int,
        filter_dict: dict,
    ) -> dict:
        """
        Deterministically enqueue or refresh the singleton HOST_TASK row for one type.

        Queue contract:
            - one durable row per `FK_HOST + NU_TYPE`
            - RUNNING rows are preserved
            - PENDING or terminal rows are refreshed in place
            - a new row is created only when none exists for that type

        The method updates host statistics after the queue mutation and returns
        the refreshed host snapshot for callers that want immediate state.
        """
        tasks = self.check_host_task(
            FK_HOST=host_id,
            NU_TYPE=task_type,
        )

        existing, match_count = self._find_reusable_singleton_host_task(tasks)

        if match_count > 1:
            self.log.warning(
                "[DBHandlerBKP] Multiple HOST_TASK rows matched "
                f"host={host_id}, type={task_type}, matches={match_count}. "
                "Reusing one row and leaving cleanup to maintenance tooling."
            )

        if existing:
            status = existing["HOST_TASK__NU_STATUS"]

            # RUNNING task → preserve the live execution context. A new request
            # should not rewrite the filter of an in-flight worker.
            if status == k.TASK_RUNNING:
                self.log.warning(
                    "[DBHandlerBKP] HOST_TASK already RUNNING; "
                    f"preserving current execution (host={host_id}, "
                    f"task_id={existing['HOST_TASK__ID_HOST_TASK']}, "
                    f"type={task_type})."
                )

            # PENDING task or terminal row -> refresh in place and keep the
            # singleton-per-type identity stable.
            else:
                self.host_task_update(
                    task_id=existing["HOST_TASK__ID_HOST_TASK"],
                    FILTER=filter_dict,
                    NU_TYPE=task_type,
                    NU_STATUS=task_status,
                    DT_HOST_TASK=datetime.now(),
                    NA_MESSAGE=(
                        "HOST_TASK refreshed by queue_host_task "
                        f"(type={task_type}, previous status={status})"
                    ),
                )

        else:
            # No row exists yet for this exact type → create the singleton.
            self.host_task_create(
                NU_TYPE=task_type,
                NU_STATUS=task_status,
                FK_HOST=host_id,
                FILTER=filter_dict,
            )

        self.host_update_statistics(host_id)
        return self.host_read_status(host_id)

    
    def host_task_create(self, **kwargs) -> int:
        """
        Create a new `HOST_TASK` row and return its generated ID.

        The payload is validated against `VALID_FIELDS_HOST_TASK`, normalized,
        and written with sensible defaults for timestamp, status, filter, and
        message when callers omit them.
        """
        self._connect()
        try:
            valid_fields = getattr(self, "VALID_FIELDS_HOST_TASK", None)
            if not valid_fields:
                raise ValueError("VALID_FIELDS_HOST_TASK not defined in handler.")

            payload = {}
            for key, val in kwargs.items():
                if key not in valid_fields:
                    raise ValueError(f"Invalid field '{key}' for HOST_TASK table.")
                payload[key] = val

            if "DT_HOST_TASK" not in payload:
                payload["DT_HOST_TASK"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if "NU_STATUS" not in payload:
                payload["NU_STATUS"] = k.TASK_PENDING

            payload["FILTER"] = self._serialize_host_task_filter(
                payload.get("FILTER")
            )

            if "NA_MESSAGE" not in payload:
                host_id = payload.get("FK_HOST", "UNKNOWN")
                payload["NA_MESSAGE"] = f"New HOST_TASK created for host {host_id}"

            task_id = self._insert_row(
                table="HOST_TASK",
                data=payload,
                commit=True
            )

            self.log.entry(
                f"[DBHandlerBKP] HOST_TASK created (ID={task_id}, host={payload.get('FK_HOST')}, type={payload.get('NU_TYPE')})."
            )
            return int(task_id or 0)

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBKP] Failed to create HOST_TASK: {e}")
            raise

        finally:
            self._disconnect()

    def host_task_statistics_create(self, host_id: int) -> int:
        """
        Create or reactivate the singleton statistics task for a host.

        Behavior:
            - If no statistics task exists → INSERT (PENDING)
            - If a PENDING task already exists → return its ID
            - If an existing task is ERROR/SUSPENDED/etc → reactivate it to PENDING
        This keeps statistics work on one durable row instead of churning
        `HOST_TASK` with repeated inserts.
        """

        try:
            self._connect()

            dt_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload = {
                "FK_HOST": host_id,
                "NU_TYPE": k.HOST_TASK_UPDATE_STATISTICS_TYPE,
                "FILTER": k.NONE_FILTER,
                "NU_STATUS": k.TASK_PENDING,
                "DT_HOST_TASK": dt_now,
                "NA_MESSAGE": f"Update host statistics for host {host_id}",
            }

            # Keep FILTER canonical even when callers pass the shared dict.
            if isinstance(payload["FILTER"], dict):
                payload["FILTER"] = json.dumps(payload["FILTER"], ensure_ascii=False)

            rows = self.check_host_task(
                FK_HOST=host_id,
                NU_TYPE=k.HOST_TASK_UPDATE_STATISTICS_TYPE,
            )

            if rows:
                existing = rows[0]
                tid = int(existing["HOST_TASK__ID_HOST_TASK"])
                status = existing.get("HOST_TASK__NU_STATUS", k.TASK_PENDING)

                if status in (k.TASK_PENDING, k.TASK_RUNNING):
                    self.log.entry(
                        f"[DBHandlerBKP] Statistics HOST_TASK already active "
                        f"(host={host_id}, ID={tid}, status={status}). No action taken."
                    )
                    return tid

                # Reactivate the existing singleton row instead of creating
                # unbounded INSERT churn in HOST_TASK.
                self.host_task_update(
                    task_id=tid,
                    NU_STATUS=k.TASK_PENDING,
                    DT_HOST_TASK=dt_now,
                    NA_MESSAGE=payload["NA_MESSAGE"],
                )

                self.log.entry(
                    f"[DBHandlerBKP] Reactivated statistics HOST_TASK to PENDING "
                    f"(host={host_id}, ID={tid})."
                )
                return tid

            task_id = self.host_task_create(**payload)

            self.log.entry(
                f"[DBHandlerBKP] Created statistics HOST_TASK "
                f"(host={host_id}, ID={task_id})."
            )

            return int(task_id)

        except Exception as e:
            self.log.error(
                f"[DBHandlerBKP] Failed to create statistics HOST_TASK "
                f"(host={host_id}): {e}"
            )
            return -1


        finally:
            self._disconnect()


    def host_task_read(
        self,
        task_id: Optional[int] = None,
        task_status: Optional[int] = k.TASK_PENDING,
        task_type: Optional[Union[int, List[int]]] = None,
        check_host_busy: bool = False,
        check_host_offline: bool = False,
        lock_host: bool = False,
    ) -> Optional[tuple]:
        """
        Return one host task joined with its host metadata.

        This is the main polling helper used by workers that consume
        `HOST_TASK` entries.

        When `lock_host=True`, the method also attempts to atomically claim the
        host before returning the task to the caller.

        `check_host_offline=True` is opt-in because only host-dependent
        consumers such as discovery or connectivity checks should skip hosts
        already marked offline.
        """

        self._connect()
        try:
            if check_host_busy:
                self._release_expired_transient_busy_cooldowns()

            where = {}

            if task_id:
                where["HT.ID_HOST_TASK"] = task_id

            else:
                where["HT.NU_STATUS"] = task_status

                if task_type is not None:
                    if isinstance(task_type, list):
                        where["HT.NU_TYPE"] = ("IN", task_type)
                    else:
                        where["HT.NU_TYPE"] = task_type

                if check_host_busy:
                    where["H.IS_BUSY"] = False

                if check_host_offline:
                    where["H.IS_OFFLINE"] = False

            rows = self._select_custom(
                table="HOST_TASK HT",
                joins=["JOIN HOST H ON H.ID_HOST = HT.FK_HOST"],
                where=where,
                order_by="HT.DT_HOST_TASK ASC",
                limit=1,
            )

            if not rows:
                return None

            row = rows[0]

            host_id = row["HOST__ID_HOST"]

            # Atomically claim the host after the read so two workers cannot
            # race on the same task selection.
            if lock_host and check_host_busy:
                lock_query = """
                    UPDATE HOST
                    SET
                        IS_BUSY = 1,
                        DT_BUSY = NOW(),
                        NU_PID = %s
                    WHERE
                        ID_HOST = %s
                        AND IS_BUSY = 0
                """
                lock_params = [os.getpid(), host_id]

                # A host that flipped offline after SELECT should not be
                # claimed by the lock step.
                if check_host_offline:
                    lock_query += " AND IS_OFFLINE = 0"

                self.cursor.execute(lock_query, lock_params)

                if self.cursor.rowcount == 0:
                    return None

                self.db_connection.commit()

            # Consumers expect a parsed filter payload alongside the raw row.
            raw = row.get("HOST_TASK__FILTER") or "{}"
            try:
                row["host_filter"] = json.loads(raw)
            except:
                row["host_filter"] = {}

            return row


        finally:
            self._disconnect()


    def host_task_update(
        self,
        task_id: Optional[int] = None,
        where_dict: Optional[Dict[str, Any]] = None,
        expected_status: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Update `HOST_TASK` rows safely with validation and optional status guard.

        Callers may target a specific row by `task_id` or provide a broader
        `where_dict`. When `expected_status` is set, the update behaves like a
        lightweight optimistic lock.
        """

        if task_id is None and not where_dict:
            raise ValueError("host_task_update() requires either 'task_id' or 'where_dict' argument.")

        valid_fields = getattr(self, "VALID_FIELDS_HOST_TASK", set())
        set_dict: Dict[str, Any] = {}

        for key, value in kwargs.items():
            if key in valid_fields:
                set_dict[key] = value
            else:
                self.log.warning(f"[DB] Ignored invalid field '{key}' in host_task_update().")

        # Pending rows should not keep ownership PIDs; running rows should.
        if "NU_STATUS" in set_dict:
            status = set_dict["NU_STATUS"]
            if status == k.TASK_PENDING:
                set_dict["NU_PID"] = None
            elif status == k.TASK_RUNNING and "NU_PID" not in set_dict:
                set_dict["NU_PID"] = getattr(self.log, "pid", None)

        if "FILTER" in set_dict:
            set_dict["FILTER"] = self._serialize_host_task_filter(set_dict["FILTER"])

        if not set_dict:
            self.log.warning(f"[DB] host_task_update() called with no valid fields for update.")
            return {"success": False, "rows_affected": 0, "updated_fields": {}}

        where = where_dict if where_dict else {"ID_HOST_TASK": task_id}

        if expected_status is not None:
            where["NU_STATUS"] = expected_status

        try:
            rows_affected = self._update_row(
                table="HOST_TASK",
                data=set_dict,
                where=where,
                commit=True,
            )

            msg_prefix = f"[DB] HOST_TASK update"
            if task_id:
                msg_prefix += f" (ID={task_id})"
            elif where_dict:
                msg_prefix += f" (WHERE={where_dict})"

            if rows_affected == 0:
                self.log.warning(f"{msg_prefix}: no matching rows found.")
            else:
                self.log.entry(f"{msg_prefix}: {rows_affected} row(s) updated → {set_dict}")

            return {"success": True, "rows_affected": rows_affected, "updated_fields": set_dict}

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DB] Failed to update HOST_TASK ({task_id or where_dict}): {e}")
            raise

            
    def host_task_delete(self, task_id: int) -> bool:
        """Delete one `HOST_TASK` row by primary key and report success."""
        try:
            deleted = self._delete_row(
                table="HOST_TASK",
                where={"ID_HOST_TASK": task_id},
                commit=True,
            )

            if deleted > 0:
                self.log.entry(f"[DB] HOST_TASK {task_id} deleted successfully.")
                return True
            else:
                self.log.warning(f"[DB] HOST_TASK {task_id} not found for deletion.")
                return False

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DB] Error deleting HOST_TASK {task_id}: {e}")
            return False
        
    def host_task_suspend_by_host(self, host_id: int) -> None:
        """
        Suspend host-dependent HOST_TASK entries for a specific host.

        Only connectivity/operational tasks are suspended here. Statistics
        updates are DB-only maintenance work and intentionally remain outside
        host online/offline churn.

        Args:
            host_id (int): Unique identifier (ID_HOST) of the host whose tasks
                should be suspended.

        Returns:
            None

        Raises:
            Exception: Propagates any database access or SQL execution errors.
        """
        try:
            affected = 0
            host_dependent_types = (
                k.HOST_TASK_CHECK_TYPE,
                k.HOST_TASK_PROCESSING_TYPE,
                k.HOST_TASK_CHECK_CONNECTION_TYPE,
            )

            for status in (k.TASK_PENDING, k.TASK_RUNNING):
                affected += self._update_row(
                    table="HOST_TASK",
                    data={
                        "NU_STATUS": k.TASK_SUSPENDED,
                        "NA_MESSAGE": "Host unreachable. Tasks suspended automatically.",
                    },
                    where={
                        "FK_HOST": host_id,
                        "NU_STATUS": status,
                        "NU_TYPE__in": host_dependent_types,
                    },
                    commit=True
                )

            if affected:
                self.log.entry(f"[DBHandlerBKP] Suspended {affected} HOST_TASK entries for host {host_id}.")
        except Exception as e:
            self.log.error(f"[DBHandlerBKP] Failed to suspend HOST_TASK entries for host {host_id}: {e}")


    def host_task_resume_by_host(
        self,
        host_id: int,
        busy_timeout_seconds: int = k.HOST_BUSY_TIMEOUT
    ) -> None:
        """
        Resume host-dependent suspended, errored, or stale HOST_TASK entries.

        Tasks are reactivated under three conditions:
            1. NU_STATUS = TASK_SUSPENDED → host became reachable again.
            2. NU_STATUS = TASK_ERROR     → retry.
            3. NU_STATUS = TASK_RUNNING   → considered stale if DT_HOST_TASK is older
            than (now - busy_timeout_seconds), assuming the worker crashed.

        Statistics HOST_TASK rows are intentionally excluded because they do
        not depend on host connectivity and should keep their own lifecycle.

        Statistics tasks are intentionally excluded because they do not depend
        on host connectivity and keep their own lifecycle.
        """
        try:
            total_resumed = 0
            threshold_time = datetime.now() - timedelta(seconds=busy_timeout_seconds)
            host_dependent_types = (
                k.HOST_TASK_CHECK_TYPE,
                k.HOST_TASK_PROCESSING_TYPE,
                k.HOST_TASK_CHECK_CONNECTION_TYPE,
            )

            # -----------------------------------------------------------------
            # 1. Reactivate suspended tasks
            # -----------------------------------------------------------------
            resumed_suspended = self._update_row(
                table="HOST_TASK",
                data={
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        "Host reachable again — suspended task resumed automatically"
                    ),
                },
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS": k.TASK_SUSPENDED,
                    "NU_TYPE__in": host_dependent_types,
                },
                commit=True,
            )

            # -----------------------------------------------------------------
            # 2. Reactivate tasks previously marked as error
            # -----------------------------------------------------------------
            resumed_error = self._update_row(
                table="HOST_TASK",
                data={
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        "Host reachable again — previously failed task resubmitted"
                    ),
                },
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS": k.TASK_ERROR,
                    "NU_TYPE__in": host_dependent_types,
                },
                commit=True,
            )

            # -----------------------------------------------------------------
            # 3. Reactivate stale tasks stuck in TASK_RUNNING past timeout
            # -----------------------------------------------------------------
            resumed_stale_running = self._update_row(
                table="HOST_TASK",
                data={
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        f"Detected stale running task (> {busy_timeout_seconds}s) — "
                        f"resubmitted automatically"
                    ),
                },
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS": k.TASK_RUNNING,
                    "DT_HOST_TASK__lt": threshold_time,
                    "NU_TYPE__in": host_dependent_types,
                },
                commit=True,
            )

            # -----------------------------------------------------------------
            # Result logging
            # -----------------------------------------------------------------
            total_resumed = resumed_suspended + resumed_error + resumed_stale_running

            if total_resumed > 0:
                self.log.entry(
                    f"[DBHandlerBKP] Resumed {total_resumed} HOST_TASK entries for host {host_id} "
                    f"(stale if > {busy_timeout_seconds}s)."
                )
            else:
                self.log.entry(
                    f"[DBHandlerBKP] No HOST_TASK entries required resumption for host {host_id}."
                )

        except Exception as e:
            self.log.error(
                f"[DBHandlerBKP] Failed to resume HOST_TASK entries for host {host_id}: {e}"
            )


    # ======================================================================
    # FILE_TASK OPERATIONS
    # ======================================================================
    def read_file_task(
        self,
        task_id: Optional[int] = None,
        task_status: Optional[int] = k.TASK_PENDING,
        task_type: Optional[int] = None,
        check_host_busy: bool = True,
        check_host_offline: bool = False,
        extension: Optional[str] = None,
        lock_host: bool = False,
        reserve_hosts_for_discovery: bool = False,
        fair_by_host: bool = False,
    ) -> Optional[tuple]:
        """
        Return one file task joined with host metadata.

        When `lock_host=True`, the method also attempts to atomically claim the
        host before returning the task to the caller.

        The helper supports the two queue-selection refinements used by backup:
            - discovery reservation windows
            - host-fair round-robin ordering
        """

        self._connect()

        if check_host_busy:
            self._release_expired_transient_busy_cooldowns()

        where = {}
        custom_clauses = {}
        fair_backup_mode = (
            not task_id
            and task_type == k.FILE_TASK_BACKUP_TYPE
            and fair_by_host
        )
        discovery_host_reservation = (
            not task_id
            and task_type == k.FILE_TASK_BACKUP_TYPE
            and reserve_hosts_for_discovery
        )

        if task_id:
            where["FT.ID_FILE_TASK"] = task_id

        else:
            if task_status is not None:
                where["FT.NU_STATUS"] = task_status

            if task_type is not None:
                where["FT.NU_TYPE"] = task_type

            if extension is not None:
                where["FT.NA_EXTENSION"] = extension

            if check_host_busy:
                where["H.IS_BUSY"] = False

            if check_host_offline:
                where["H.IS_OFFLINE"] = False

            if discovery_host_reservation:
                custom_clauses["#CUSTOM#host_discovery_reservation"] = (
                    "NOT EXISTS ("
                    "SELECT 1 FROM HOST_TASK HT_BLOCK "
                    "WHERE HT_BLOCK.FK_HOST = FT.FK_HOST "
                    f"AND HT_BLOCK.NU_TYPE IN ({k.HOST_TASK_CHECK_TYPE}, {k.HOST_TASK_PROCESSING_TYPE}) "
                    "AND ("
                    f"HT_BLOCK.NU_STATUS = {k.TASK_RUNNING} "
                    "OR ("
                    f"HT_BLOCK.NU_STATUS = {k.TASK_PENDING} "
                    "AND ("
                    "HT_BLOCK.DT_HOST_TASK IS NULL "
                    f"OR HT_BLOCK.DT_HOST_TASK >= DATE_SUB(NOW(), INTERVAL {k.DISCOVERY_RESERVATION_TTL_SEC} SECOND)"
                    ")"
                    ")"
                    ")"
                    ")"
                )

            if fair_backup_mode:
                custom_clauses["#CUSTOM#backup_host_round_robin"] = (
                    "FT.ID_FILE_TASK = ("
                    "SELECT MIN(FT_HOST.ID_FILE_TASK) FROM FILE_TASK FT_HOST "
                    "WHERE FT_HOST.FK_HOST = FT.FK_HOST "
                    f"AND FT_HOST.NU_STATUS = {task_status} "
                    f"AND FT_HOST.NU_TYPE = {task_type}"
                    ")"
                )

            where.update(custom_clauses)

        if fair_backup_mode:
            order_by = (
                "CASE WHEN H.DT_LAST_BACKUP IS NULL THEN 0 ELSE 1 END ASC, "
                "H.DT_LAST_BACKUP ASC, FT.ID_FILE_TASK ASC"
            )
        else:
            order_by = "FT.ID_FILE_TASK ASC" if not task_id else None

        rows = self._select_custom(
            table="FILE_TASK FT",
            joins=["JOIN HOST H ON H.ID_HOST = FT.FK_HOST"],
            where=where,
            order_by=order_by,
            limit=1,
        )

        if not rows:
            self._disconnect()
            return None

        row = rows[0]

        file_task_id = row["FILE_TASK__ID_FILE_TASK"]
        host_id = row["HOST__ID_HOST"]

        # Optional atomic host claim prevents a second worker from racing the
        # selected task after the read step.
        if lock_host and check_host_busy:
            lock_query = """
                UPDATE HOST
                SET
                    IS_BUSY = 1,
                    DT_BUSY = NOW(),
                    NU_PID = %s
                WHERE
                    ID_HOST = %s
                    AND IS_BUSY = 0
            """
            lock_params = [os.getpid(), host_id]

            # Backup can opt into the same offline guard so a host already
            # marked unreachable is not locked and retried needlessly.
            if check_host_offline:
                lock_query += " AND IS_OFFLINE = 0"

            self.cursor.execute(lock_query, lock_params)

            if self.cursor.rowcount == 0:
                self._disconnect()
                return None

            self.db_connection.commit()

        self._disconnect()

        return row, host_id, file_task_id


    # -------------------------- Public APIs --------------------------------
    def file_task_create(
        self,
        host_id: int,
        task_type: int,
        task_status: int,
        file_metadata: list,
    ) -> int:
        """
        Create or refresh `FILE_TASK` rows from `FileMetadata` objects.

        The method accepts one or many `FileMetadata` objects and uses UPSERT
        semantics so repeated discovery/backlog promotion refreshes the same
        logical queue row instead of inserting duplicates.
        """

        if not file_metadata:
            return 0

        self._connect()
        processed = 0

        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rows: list[dict] = []

            for file in file_metadata:
                row = {
                    "FK_HOST": host_id,
                    "NA_HOST_FILE_PATH": file.NA_PATH,
                    "NA_HOST_FILE_NAME": file.NA_FILE,
                    "NA_EXTENSION": file.NA_EXTENSION,
                    "VL_FILE_SIZE_KB": file.VL_FILE_SIZE_KB,
                    "DT_FILE_CREATED": file.DT_FILE_CREATED,
                    "DT_FILE_MODIFIED": file.DT_FILE_MODIFIED,
                    "NU_PID": os.getpid(),
                    "NU_TYPE": task_type,
                    "NU_STATUS": task_status,
                    "DT_FILE_TASK": now,
                    "NA_MESSAGE": tools.compose_message(
                        task_type=task_type,
                        task_status=task_status,
                        path=file.NA_PATH,
                        name=file.NA_FILE,
                    ),
                }
                rows.append(self._merge_structured_error_fields(row))

            if len(rows) == 1:
                self._upsert_row(
                    table="FILE_TASK",
                    data=rows[0],
                    unique_keys=["FK_HOST", "NA_HOST_FILE_PATH","NA_HOST_FILE_NAME"],
                    commit=False,
                    touch_field="DT_FILE_TASK",
                    log_each=False,
                )
                processed = 1

            else:
                processed = self._upsert_batch(
                    table="FILE_TASK",
                    rows=rows,
                    unique_keys=["FK_HOST", "NA_HOST_FILE_PATH","NA_HOST_FILE_NAME"],
                    touch_field="DT_FILE_TASK",
                    batch_size=1000,
                    commit=False,
                )

            self.db_connection.commit()

            self.log.entry(
                f"[file_task_create] Upserted {processed} FILE_TASK entries "
                f"for host {host_id}"
            )

            return processed

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[file_task_create] failed: {e}")
            raise

        finally:
            self._disconnect()


    def file_task_update(
        self,
        *,
        task_id: Optional[int] = None,
        host_id: Optional[int] = None,
        host_file_path: Optional[str] = None,
        host_file_name: Optional[str] = None,
        server_file_name: Optional[str] = None,
        expected_status: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Update a `FILE_TASK` row using deterministic identification rules.

        Identification strategies (exactly one required):

            1) task_id (ID_FILE_TASK)
            2) (host_id, host_file_path, host_file_name)
            3) (host_id, server_file_name)

        Optional optimistic lock:
            `expected_status` restricts the update to rows still in the
            caller-expected lifecycle state.

        Update semantics:
            - Field omitted  -> not updated
            - Field=None     -> not updated
            - Field=value    -> updated

        Timestamp ownership:
            `DT_FILE_TASK` is caller-owned. This method never injects implicit
            task timestamps, so workers and maintenance scripts must pass the
            transition time explicitly when they want that audit field updated.
        """

        self._connect()

        # -------------------------------------------------
        # Validate update fields
        # -------------------------------------------------
        if not kwargs:
            self.log.warning("[DBHandlerBKP] No fields provided for file_task_update.")
            self._disconnect()
            return {
                "success": False,
                "rows_affected": 0,
                "updated_fields": {},
            }

        valid_fields = getattr(self, "VALID_FIELDS_FILE_TASK", set())

        for key in kwargs.keys():
            if key not in valid_fields:
                raise ValueError(f"Invalid field '{key}' for FILE_TASK table update.")

        # Mirror HOST_TASK semantics: pending rows should not advertise worker
        # ownership, while running rows should.
        if "NU_STATUS" in kwargs:
            status = kwargs["NU_STATUS"]
            if status == k.TASK_PENDING and "NU_PID" not in kwargs:
                kwargs["NU_PID"] = None
            elif status == k.TASK_RUNNING and "NU_PID" not in kwargs:
                kwargs["NU_PID"] = getattr(self.log, "pid", None)

        kwargs = self._merge_structured_error_fields(kwargs)

        # -------------------------------------------------
        # Build deterministic WHERE clause
        # -------------------------------------------------
        where_dict: Dict[str, Any] = {}

        # OPTION 1 — Primary Key
        if task_id is not None:
            where_dict = {"ID_FILE_TASK": task_id}

        # OPTION 2 — Unique host composite key
        elif (
            host_id is not None
            and host_file_path is not None
            and host_file_name is not None
        ):
            where_dict = {
                "FK_HOST": host_id,
                "NA_HOST_FILE_PATH": host_file_path,
                "NA_HOST_FILE_NAME": host_file_name,
            }

        # OPTION 3 — Post-backup unique key
        elif (
            host_id is not None
            and server_file_name is not None
        ):
            where_dict = {
                "FK_HOST": host_id,
                "NA_SERVER_FILE_NAME": server_file_name,
            }

        else:
            raise ValueError(
                "Invalid identification strategy for FILE_TASK update. "
                "Use one of:\n"
                "1) task_id\n"
                "2) (host_id, host_file_path, host_file_name)\n"
                "3) (host_id, server_file_name)"
            )

        # -------------------------------------------------
        # Optional optimistic lock
        # -------------------------------------------------
        if expected_status is not None:
            where_dict["NU_STATUS"] = expected_status

        # -------------------------------------------------
        # Execute UPDATE
        # -------------------------------------------------
        try:
            affected = self._update_row(
                table="FILE_TASK",
                data=kwargs,
                where=where_dict,
                commit=True,
            )

            if affected != 1:
                self.log.warning(
                    f"[DBHandlerBKP] FILE_TASK update affected {affected} rows "
                    f"(expected 1). WHERE={where_dict}"
                )
            else:
                self.log.entry(
                    f"[DBHandlerBKP] FILE_TASK updated successfully "
                    f"WHERE={where_dict} | fields={list(kwargs.keys())}"
                )

            return {
                "success": True,
                "rows_affected": affected,
                "updated_fields": kwargs,
                "where_used": where_dict,
            }

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(
                f"[DBHandlerBKP] Failed to update FILE_TASK "
                f"(WHERE={where_dict}): {e}"
            )
            raise

        finally:
            self._disconnect()


    def file_task_delete(self, task_id: int) -> int:
        """Delete a `FILE_TASK` by primary key and return the row count."""
        self._connect()
        try:
            where = {"ID_FILE_TASK": task_id}
            return self._delete_row("FILE_TASK", where=where, commit=True)
        finally:
            self._disconnect()


    def file_history_delete(
        self,
        *,
        history_id: Optional[int] = None,
        host_id: Optional[int] = None,
        host_file_path: Optional[str] = None,
        host_file_name: Optional[str] = None,
    ) -> int:
        """Delete a `FILE_TASK_HISTORY` row using a deterministic identity.

        Supported identification strategies:
            1) `history_id`
            2) (`host_id`, `host_file_path`, `host_file_name`)

        Used mainly by backup cleanup when a discovered file vanished from the
        remote host and must not remain resumable in history.

        Returns:
            int: Number of deleted rows.
        """
        self._connect()
        try:
            if history_id is not None:
                where = {"ID_HISTORY": history_id}
            elif (
                host_id is not None
                and host_file_path is not None
                and host_file_name is not None
            ):
                where = {
                    "FK_HOST": host_id,
                    "NA_HOST_FILE_PATH": host_file_path,
                    "NA_HOST_FILE_NAME": host_file_name,
                }
            else:
                raise ValueError(
                    "Invalid identification strategy for FILE_TASK_HISTORY delete. "
                    "Use either history_id or (host_id, host_file_path, host_file_name)."
                )

            return self._delete_row("FILE_TASK_HISTORY", where=where, commit=True)
        finally:
            self._disconnect()
            
    
    def file_task_suspend_by_host(self, host_id: int, reason: str = None) -> None:
        """
        Suspend host-dependent file tasks for a host.

        Only DISCOVERY and BACKUP tasks are suspended, since PROCESS tasks
        operate exclusively on files already stored on the server and do
        not depend on host connectivity.
        """

        try:
            message = (
                reason
                or "Host unreachable — HOST-dependent file task suspended by host_check service"
            )

            affected = 0

            for status in (k.TASK_PENDING, k.TASK_RUNNING):
                affected += self._update_row(
                    table="FILE_TASK",
                    data=self._merge_structured_error_fields({
                        "NU_STATUS": k.TASK_SUSPENDED,
                        "NA_MESSAGE": message,
                    }),
                    where={
                        "FK_HOST": host_id,
                        "NU_STATUS": status,
                        "NU_TYPE__in": (
                            k.FILE_TASK_DISCOVERY,
                            k.FILE_TASK_BACKUP_TYPE,
                        ),
                    },
                    commit=True,
                )

            if affected:
                self.log.entry(
                    f"[DBHandlerBKP] Suspended {affected} HOST-dependent FILE_TASK entries "
                    f"for host {host_id}."
                )

        except Exception as e:
            self.log.error(
                f"[DBHandlerBKP] Failed to suspend FILE_TASK entries for host {host_id}: {e}"
            )

 

    def file_task_resume_by_host(
        self,
        host_id: int,
        busy_timeout_seconds: int = k.HOST_BUSY_TIMEOUT
    ) -> None:
        """
        Resume suspended, errored, or stale host-dependent file tasks.

        Tasks are reactivated under three conditions:
            1. NU_STATUS = TASK_SUSPENDED → host became reachable again.
            2. NU_STATUS = TASK_ERROR     → retry.
            3. NU_STATUS = TASK_RUNNING   → considered stale if DT_FILE_TASK is older than
            (now - busy_timeout_seconds), assuming the worker crashed or was interrupted.

        Args:
            host_id (int):
                Host identifier (FK_HOST) whose file-level tasks should be resumed.
            busy_timeout_seconds (int):
                Maximum allowed time (in seconds) for a running task before it is
                considered stale. Defaults to k.HOST_BUSY_TIMEOUT.

        Returns:
            None
        """
        try:
            total_resumed = 0
            threshold_time = datetime.now() - timedelta(seconds=busy_timeout_seconds)

            # -----------------------------------------------------------------
            # 1. Reactivate suspended file tasks
            # -----------------------------------------------------------------
            resumed_suspended = self._update_row(
                table="FILE_TASK",
                data=self._merge_structured_error_fields({
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        "Host reachable again — suspended file task resumed automatically"
                    ),
                }),
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS": k.TASK_SUSPENDED,
                },
                commit=True,
            )

            # -----------------------------------------------------------------
            # 2) Reactivate ERROR tasks (DISCOVERY and BACKUP only)
            # -----------------------------------------------------------------
            resumed_error = self._update_row(
                table="FILE_TASK",
                data=self._merge_structured_error_fields({
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        "Host reachable again — previously failed file task resubmitted"
                    ),
                }),
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS": k.TASK_ERROR,
                    "NU_TYPE__in": (
                        k.FILE_TASK_DISCOVERY,
                        k.FILE_TASK_BACKUP_TYPE,
                    ),
                },
                commit=True,
            )

            # -----------------------------------------------------------------
            # 3. Reactivate stale running file tasks (> busy_timeout_seconds)
            # -----------------------------------------------------------------
            resumed_stale_running = self._update_row(
                table="FILE_TASK",
                data=self._merge_structured_error_fields({
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        f"Detected stale running file task (> {busy_timeout_seconds}s) — "
                        f"resubmitted automatically"
                    ),
                }),
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS": k.TASK_RUNNING,
                    "DT_FILE_TASK__lt": threshold_time,
                },
                commit=True,
            )

            # -----------------------------------------------------------------
            # 4. Final log
            # -----------------------------------------------------------------
            total_resumed = resumed_suspended + resumed_error + resumed_stale_running

            if total_resumed > 0:
                self.log.entry(
                    f"[DBHandlerBKP] Resumed {total_resumed} FILE_TASK entries for host {host_id} "
                    f"(stale if > {busy_timeout_seconds}s)."
                )
            else:
                self.log.entry(
                    f"[DBHandlerBKP] No FILE_TASK entries required resumption for host {host_id}."
                )

        except Exception as e:
            self.log.error(
                f"[DBHandlerBKP] Failed to resume FILE_TASK entries for host {host_id}: {e}"
            )

    def file_history_suspend_by_host(self, host_id: int, reason: str = None) -> None:
        """
        Mirror host-driven suspension into FILE_TASK_HISTORY.

        Only host-dependent phases are touched:
            - DISCOVERY status
            - BACKUP status

        Processing history is intentionally left alone because it does not
        depend on the remote host once the file is already on the server.
        """
        try:
            message = (
                reason
                or "Host unreachable — host-dependent history suspended by host_check service"
            )

            suspended_discovery = self._update_row(
                table="FILE_TASK_HISTORY",
                data=self._merge_structured_error_fields({
                    "NU_STATUS_DISCOVERY": k.TASK_SUSPENDED,
                    "NA_MESSAGE": message,
                }),
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS_DISCOVERY__in": (
                        k.TASK_PENDING,
                        k.TASK_RUNNING,
                    ),
                },
                commit=True,
            )

            suspended_backup = self._update_row(
                table="FILE_TASK_HISTORY",
                data=self._merge_structured_error_fields({
                    "NU_STATUS_BACKUP": k.TASK_SUSPENDED,
                    "NA_MESSAGE": message,
                }),
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS_BACKUP__in": (
                        k.TASK_PENDING,
                        k.TASK_RUNNING,
                    ),
                },
                commit=True,
            )

            total_suspended = suspended_discovery + suspended_backup

            if total_suspended:
                self.log.entry(
                    f"[DBHandlerBKP] Suspended {total_suspended} FILE_TASK_HISTORY phases "
                    f"(discovery + backup) for host {host_id}."
                )

        except Exception as e:
            self.log.error(
                f"[DBHandlerBKP] Failed to suspend FILE_TASK_HISTORY entries for host {host_id}: {e}"
            )

    def file_history_resume_by_host(
        self,
        host_id: int,
    ) -> None:
        """
        Resume DISCOVERY and BACKUP phases in file history when a host recovers.

        Processing phase is NOT resumed by design. Both TASK_SUSPENDED and
        TASK_ERROR are reactivated here because both states represent
        host-dependent work that can continue once connectivity returns.
        """
        try:
            total_resumed = 0

            # -------------------------------------------------------------
            # 1) Resume DISCOVERY phase (host-dependent)
            # -------------------------------------------------------------
            resumed_discovery = self._update_row(
                table="FILE_TASK_HISTORY",
                data=self._merge_structured_error_fields({
                    "NU_STATUS_DISCOVERY": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        "Host reachable again — discovery resumed automatically"
                    ),
                }),
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS_DISCOVERY__in": (
                        k.TASK_SUSPENDED,
                        k.TASK_ERROR,
                    ),
                },
                commit=True,
            )

            # -------------------------------------------------------------
            # 2) Resume BACKUP phase (host-dependent)
            # -------------------------------------------------------------
            resumed_backup = self._update_row(
                table="FILE_TASK_HISTORY",
                data=self._merge_structured_error_fields({
                    "NU_STATUS_BACKUP": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        "Host reachable again — backup resumed automatically"
                    ),
                }),
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS_BACKUP__in": (
                        k.TASK_SUSPENDED,
                        k.TASK_ERROR,
                    ),
                },
                commit=True,
            )

            total_resumed = resumed_discovery + resumed_backup

            # -------------------------------------------------------------
            # 3) Final log
            # -------------------------------------------------------------
            if total_resumed > 0:
                self.log.entry(
                    f"[DBHandlerBKP] Resumed {total_resumed} FILE_TASK_HISTORY entries "
                    f"(discovery + backup) for host {host_id}"
                )
            else:
                self.log.entry(
                    f"[DBHandlerBKP] No FILE_TASK_HISTORY entries required resumption for host {host_id}."
                )

        except Exception as e:
            self.log.error(
                f"[DBHandlerBKP] Failed to resume FILE_TASK_HISTORY entries for host {host_id}: {e}"
            )


            
    def check_file_task(self, **kwargs) -> list[dict]:
        """
        Query `FILE_TASK` with dynamic equality filters.

        This helper still exists for maintenance scripts that need a lightweight
        FILE_TASK lookup without the heavier worker-oriented joins.
        """
        valid_fields = self.VALID_FIELDS_FILE_TASK
        
        self._connect()

        where_clause = {}
        for key, value in kwargs.items():
            if key not in valid_fields:
                raise ValueError(f"Invalid field in _check_file_history(): '{key}' is not a valid column.")
            where_clause[key] = value

        rows = self._select_rows(
            table="FILE_TASK",
            where=where_clause,
            order_by="ID_FILE_TASK DESC",
            cols=[
                "ID_FILE_TASK",
                "FK_HOST",
                "DT_FILE_TASK",
                "NA_HOST_FILE_PATH",
                "NA_HOST_FILE_NAME",
                "NU_TYPE",
                "NU_STATUS",
                "VL_FILE_SIZE_KB",
                "NA_SERVER_FILE_PATH",
                "NA_SERVER_FILE_NAME",
                "NA_MESSAGE",
            ],
        )

        return rows or None
    
    def update_backlog_by_filter(
        self,
        host_id: int,
        task_filter: Dict[str, Any],
        *,
        search_type: int,
        search_status: Union[int, List[int]],
        new_type: int,
        new_status: int,
    ) -> Dict[str, int]:
        """
        Promote backlog entries in `FILE_TASK` based on a logical filter.

        This method performs a single SQL UPDATE on FILE_TASK, transitioning
        tasks from one type/status to another (e.g. DISCOVERY → BACKUP).

        Architectural contract:
            - DB-driven only
            - no filesystem inspection
            - no per-file Python loops
            - safe for large CelPlan-style backlogs

        Returns:
            dict with counters:
                {
                    "rows_updated": int,
                    "moved_to_backup": int,
                    "moved_to_discovery": int,
                }
        """

        summary = {
            "rows_updated": 0,
            "moved_to_backup": 0,
            "moved_to_discovery": 0,
            "selected_total_kb": 0,
        }

        self._connect()

        try:
            filter_obj = filter.Filter(task_filter, log=self.log)

            meta = filter_obj.evaluate_database(
                host_id=host_id,
                search_type=search_type,
                search_status=search_status,
            )

            where = meta.get("where")
            extra_sql = meta.get("extra_sql", "")
            order_by = meta.get("order_by")
            limit = meta.get("limit")
            max_total_kb = meta.get("max_total_kb")
            msg_prefix = tools.compose_message(
                new_type,
                new_status,
                prefix_only=True,
            )

            # Some filter combinations intentionally resolve to "no-op".
            if not where:
                self.log.entry(
                    "[update_backlog_by_filter] Filter resolved to no-op. Skipping UPDATE."
                )
                return summary

            sql_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            update_payload = {
                "NU_TYPE": new_type,
                "NU_STATUS": new_status,
                "DT_FILE_TASK": sql_now,
                "NA_MESSAGE__expr": (
                    f"CONCAT('{msg_prefix} of file ', "
                    f"NA_HOST_FILE_PATH, '/', NA_HOST_FILE_NAME)"
                ),
            }
            update_payload = self._merge_structured_error_fields(update_payload)

            if new_type == k.FILE_TASK_BACKUP_TYPE and max_total_kb is not None:
                candidate_rows = self._select_rows(
                    table="FILE_TASK",
                    where=where,
                    order_by=order_by,
                    limit=limit,
                    cols=[
                        "ID_FILE_TASK",
                        "VL_FILE_SIZE_KB",
                    ],
                )

                selected_ids: list[int] = []
                selected_total_kb = 0

                for row in candidate_rows:
                    file_id = int(row["ID_FILE_TASK"])
                    file_size_kb = max(0, int(row.get("VL_FILE_SIZE_KB") or 0))

                    if selected_total_kb + file_size_kb > max_total_kb:
                        break

                    selected_ids.append(file_id)
                    selected_total_kb += file_size_kb

                summary["selected_total_kb"] = selected_total_kb

                if not selected_ids:
                    self.log.entry(
                        "[update_backlog_by_filter] Budget-limited promotion selected no rows "
                        f"(host_id={host_id}, max_total_kb={max_total_kb})"
                    )
                    return summary

                update_where = dict(where)
                update_where["ID_FILE_TASK__in"] = selected_ids

                rows_updated = self._update_row(
                    table="FILE_TASK",
                    data=update_payload,
                    where=update_where,
                    commit=True,
                )
            else:
                rows_updated = self._update_row(
                    table="FILE_TASK",
                    data=update_payload,
                    where=where,
                    extra_sql=extra_sql,
                    commit=True,
                )

            summary["rows_updated"] = rows_updated

            if new_type == k.FILE_TASK_BACKUP_TYPE:
                summary["moved_to_backup"] = rows_updated
            elif new_type == k.FILE_TASK_DISCOVERY:
                summary["moved_to_discovery"] = rows_updated

            self.log.entry(
                f"[update_backlog_by_filter] Updated {rows_updated} FILE_TASK rows "
                f"(new_type={new_type}, new_status={new_status}, "
                f"selected_total_kb={summary['selected_total_kb']}) for host {host_id}"
            )

            return summary

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[update_backlog_by_filter] Failed: {e}")
            raise

        finally:
            self._disconnect()
    def file_history_create(
        self,
        host_id: int,
        task_type: int,
        task_status: int,
        file_metadata: list,
    ) -> int:
        """
        Create or refresh `FILE_TASK_HISTORY` rows from `FileMetadata` objects.

        This is the durable audit-table counterpart of `file_task_create()`.
        Discovery creates the history row once and later phases mutate its
        timestamps and statuses as the file advances through backup and
        processing.
        """

        if not file_metadata:
            return 0

        self._connect()
        processed = 0

        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rows: list[dict] = []

            for file in file_metadata:
                msg = tools.compose_message(
                    task_type=task_type,
                    task_status=task_status,
                    path=file.NA_PATH,
                    name=file.NA_FILE,
                )

                row = {
                    "FK_HOST": host_id,
                    "NA_HOST_FILE_PATH": file.NA_PATH,
                    "NA_HOST_FILE_NAME": file.NA_FILE,
                    "NA_EXTENSION": file.NA_EXTENSION,
                    "VL_FILE_SIZE_KB": file.VL_FILE_SIZE_KB,
                    "DT_FILE_CREATED": file.DT_FILE_CREATED,
                    "DT_FILE_MODIFIED": file.DT_FILE_MODIFIED,

                    # Discovery is creating the durable history row, so the
                    # first phase is immediately DONE while the later phases
                    # remain pending.
                    "DT_DISCOVERED": now,
                    "DT_BACKUP": None,
                    "DT_PROCESSED": None,
                    "NU_STATUS_DISCOVERY": k.TASK_DONE,
                    "NU_STATUS_BACKUP": k.TASK_PENDING,
                    "NU_STATUS_PROCESSING": k.TASK_PENDING,

                    "NA_MESSAGE": msg,
                }
                rows.append(self._merge_structured_error_fields(row))

            if len(rows) == 1:
                self._upsert_row(
                    table="FILE_TASK_HISTORY",
                    data=rows[0],
                    unique_keys=["FK_HOST", "NA_HOST_FILE_NAME", "NA_HOST_FILE_PATH"],
                    commit=False,
                    touch_field="DT_DISCOVERED",
                    log_each=False,
                )
                processed = 1

            else:
                processed = self._upsert_batch(
                    table="FILE_TASK_HISTORY",
                    rows=rows,
                    unique_keys=["FK_HOST", "NA_HOST_FILE_NAME", "NA_HOST_FILE_PATH"],
                    touch_field="DT_DISCOVERED",
                    batch_size=1000,
                    commit=False,
                )

            self.db_connection.commit()

            self.log.entry(
                f"[file_history_create] Upserted {processed} FILE_TASK_HISTORY entries "
                f"for host {host_id}"
            )

            return processed

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[file_history_create] failed: {e}")
            raise

        finally:
            self._disconnect()



    def file_history_update(
        self,
        task_type: Optional[int] = None,
        *,
        history_id: Optional[int] = None,
        host_id: Optional[int] = None,
        host_file_path: Optional[str] = None,
        host_file_name: Optional[str] = None,
        server_file_name: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Update `FILE_TASK_HISTORY` using deterministic identification rules.

        Identification strategies (exactly one required):

            1) history_id
            2) (host_id, host_file_path, host_file_name)
            3) (host_id, server_file_name)  -> only when server_file_name IS NOT NULL

        Update semantics:
            - omitted field   -> not updated
            - `None`          -> not updated
            - `SET_NULL`      -> explicit SQL NULL
            - any other value -> updated

        Timestamp ownership:
            `DT_BACKUP` and `DT_PROCESSED` are caller-owned. The function keeps
            `task_type` only for signature compatibility and call-site clarity;
            it no longer injects implicit timestamps.

        Artifact-deletion semantics:
            `IS_PAYLOAD_DELETED` / `DT_PAYLOAD_DELETED` refer only to the
            artifact currently referenced by `FILE_TASK_HISTORY`
            (`NA_SERVER_FILE_PATH` + `NA_SERVER_FILE_NAME`). They do not
            represent deletion of superseded leftovers parked in
            `trash/resolved_files`, because those files are no longer owned by
            the history row.
        """

        self._connect()
        valid_fields = getattr(self, "VALID_FIELDS_FILE_TASK_HISTORY", set())

        # -------------------------------------------------
        # Validate update fields
        # -------------------------------------------------
        if not kwargs:
            raise ValueError("No fields provided for FILE_TASK_HISTORY update.")

        for key in kwargs:
            if key not in valid_fields:
                raise ValueError(f"Invalid field '{key}' for FILE_TASK_HISTORY.")

        # -------------------------------------------------
        # Build deterministic WHERE clause
        # -------------------------------------------------
        where_dict: Dict[str, Any] = {}

        # OPTION 1 — Primary Key
        if history_id is not None:
            where_dict = {"ID_HISTORY": history_id}

        # OPTION 2 — Unique host composite key
        elif (
            host_id is not None
            and host_file_path is not None
            and host_file_name is not None
        ):
            where_dict = {
                "FK_HOST": host_id,
                "NA_HOST_FILE_PATH": host_file_path,
                "NA_HOST_FILE_NAME": host_file_name,
            }

        # OPTION 3 — Post-backup unique key
        elif (
            host_id is not None
            and server_file_name is not None
        ):
            where_dict = {
                "FK_HOST": host_id,
                "NA_SERVER_FILE_NAME": server_file_name,
            }

        else:
            raise ValueError(
                "Invalid identification strategy for FILE_TASK_HISTORY update. "
                "Use one of:\n"
                "1) history_id\n"
                "2) (host_id, host_file_path, host_file_name)\n"
                "3) (host_id, server_file_name)"
            )

        # -------------------------------------------------
        # Build UPDATE payload with explicit NULL semantics
        # -------------------------------------------------
        update_data: Dict[str, Any] = {}

        for key, value in kwargs.items():
            if value is None:
                continue

            if value is constants.SET_NULL:
                update_data[key] = None
            else:
                update_data[key] = value

        if not update_data:
            self._disconnect()
            return {
                "success": True,
                "rows_affected": 0,
                "updated_fields": {},
                "where_used": where_dict,
            }

        update_data = self._merge_structured_error_fields(update_data)

        # -------------------------------------------------
        # Execute UPDATE
        # -------------------------------------------------
        try:
            affected_rows = self._update_row(
                table="FILE_TASK_HISTORY",
                data=update_data,
                where=where_dict,
                commit=True,
            )

            if affected_rows != 1:
                self.log.warning(
                    f"[DBHandlerBKP] FILE_TASK_HISTORY update affected {affected_rows} rows "
                    f"(expected 1). WHERE={where_dict}"
                )

            return {
                "success": True,
                "rows_affected": affected_rows,
                "updated_fields": update_data,
                "where_used": where_dict,
            }

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(
                f"[DBHandlerBKP] Failed to update FILE_TASK_HISTORY "
                f"(WHERE={where_dict}): {e}"
            )
            raise

        finally:
            self._disconnect()
            
    def filter_existing_file_batch(
        self,
        host_id: int,
        batch: List["FileMetadata"],
        *,
        batch_size: int,
    ) -> List["FileMetadata"]:
        """
        Deduplicate a batch of `FileMetadata` objects against file history.

        Identity definition (logical key):
            (FK_HOST, NA_HOST_FILE_NAME, VL_FILE_SIZE_KB, minute(DT_FILE_CREATED))

        IMPORTANT:
            Timestamp comparison is intentionally performed at MINUTE precision.
            Seconds and microseconds are considered unstable and irrelevant
            for CelPlan DONE.zip identity semantics.

        Why minute precision:
            - NTFS may preserve sub-second precision
            - MySQL may truncate microseconds
            - exact datetime equality is therefore unreliable across layers

        The normalized comparison keeps deduplication deterministic during
        rediscovery without requiring schema changes.

        Returns:
            List[FileMetadata] containing only files not already
            present in FILE_TASK_HISTORY under the defined identity.
        """

        if not batch:
            return []

        if len(batch) > batch_size:
            raise ValueError(
                f"Batch size exceeded: {len(batch)} > {batch_size}"
            )

        # Build one derived table from the in-memory batch and let SQL perform
        # the minute-level identity comparison in one pass.
        row_sql = "SELECT %s AS name, %s AS created, %s AS size"
        union_sql = " UNION ALL ".join([row_sql] * len(batch))

        sql = f"""
            SELECT f.name, f.created, f.size
            FROM (
                {union_sql}
            ) AS f
            JOIN FILE_TASK_HISTORY h
                ON h.FK_HOST = %s
                AND h.NA_HOST_FILE_NAME = f.name
                AND h.VL_FILE_SIZE_KB = f.size
                AND TIMESTAMPDIFF(MINUTE, h.DT_FILE_CREATED, f.created) = 0
        """

        params: list[object] = []

        for m in batch:
            if not isinstance(m.DT_FILE_CREATED, datetime):
                raise TypeError(
                    f"DT_FILE_CREATED must be datetime, got {type(m.DT_FILE_CREATED)}"
                )

            params.extend([
                m.NA_FILE,
                m.DT_FILE_CREATED,
                m.VL_FILE_SIZE_KB,
            ])

        params.append(host_id)

        self._connect()
        rows = self._select_raw(sql, tuple(params))

        # SQL already matched by minute; normalize again here so the in-memory
        # comparison uses the exact same identity semantics.
        existing_keys = set()

        for row in rows:
            created = row["created"]

            if not isinstance(created, datetime):
                created = datetime.fromisoformat(str(created))

            created_minute = created.replace(second=0, microsecond=0)

            existing_keys.add((
                row["name"],
                created_minute,
                row["size"],
            ))

        # ------------------------------------------------------------
        # Filter original batch using minute-level identity
        # ------------------------------------------------------------
        result = []

        for m in batch:
            created_minute = m.DT_FILE_CREATED.replace(second=0, microsecond=0)

            key = (
                m.NA_FILE,
                created_minute,
                m.VL_FILE_SIZE_KB,
            )

            if key not in existing_keys:
                result.append(m)

        return result
    
        
    # ======================================================================
    # GARBAGE COLLECTION
    # ======================================================================
    def file_history_get_gc_candidates(self, batch_size: int, quarantine_days: int):
        """
        Return history rows eligible for garbage collection of the tracked artifact.

        Criteria:
            - NU_STATUS_PROCESSING = -1
            - IS_PAYLOAD_DELETED = 0
            - quarantine anchor older than quarantine_days

        The quarantine anchor is `DT_PROCESSED` when available because GC
        should start counting from the moment the worker retired the artifact
        into trash, not from the original payload creation time. Older history
        rows may still lack `DT_PROCESSED`, so `DT_FILE_CREATED` remains a
        fallback to avoid leaking legacy trash entries forever.

        Scope:
            This query covers only the artifact still referenced by
            `FILE_TASK_HISTORY`. Superseded artifacts in `trash/resolved_files`
            are intentionally excluded and are collected directly from the
            filesystem by the garbage-collector worker.

        Args:
            batch_size (int):
                Maximum number of records returned.

            quarantine_days (int):
                Minimum file age in days.

        Returns rows containing only the identifiers and server paths required
        by the garbage-collector worker.
        """
        self._connect()
        where = {
            "NU_STATUS_PROCESSING": -1,
            "IS_PAYLOAD_DELETED": 0,
            "#CUSTOM#QUARANTINE": (
                f"(COALESCE(DT_PROCESSED, DT_FILE_CREATED) IS NULL OR "
                f"COALESCE(DT_PROCESSED, DT_FILE_CREATED) "
                f"< NOW() - INTERVAL {quarantine_days} DAY)"
            )
        }

        return self._select_rows(
            table="FILE_TASK_HISTORY",
            where=where,
            order_by="COALESCE(DT_PROCESSED, DT_FILE_CREATED), ID_HISTORY",
            limit=batch_size,
            cols=[
                "ID_HISTORY",
                "NA_SERVER_FILE_PATH",
                "NA_SERVER_FILE_NAME"
            ]
        )
        
    
