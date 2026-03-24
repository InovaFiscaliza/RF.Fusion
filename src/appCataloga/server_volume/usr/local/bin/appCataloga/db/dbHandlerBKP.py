
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
# Imports internos do projeto
# =================================================
import config as k
from .dbHandlerBase import DBHandlerBase
from shared import filter, constants, tools
from shared.file_metadata import FileMetadata


class dbHandlerBKP(DBHandlerBase):
    """BKP-domain handler for hosts, host tasks, file tasks, and file history.

    This class centralizes all database interactions related to:
    - `HOST` (connection data and counters)
    - `HOST_TASK` (scheduled actions against a host)
    - `FILE_TASK` (per-file work items: discovery, backup, processing)

    It **never** issues raw SQL directly; instead it delegates to `DBHandlerBase`
    helpers (`_select_rows`, `_insert_row`, `_update_row`, `_delete_row`,
    `_select_custom`, `_execute_custom`, `_execute_many_custom`).

    All constants are referenced directly from `config as k` to avoid shadowing.
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

    # ======================================================================
    # HOST OPERATIONS
    # ======================================================================
    
    def host_upsert(self, **kwargs) -> None:
        """
        Create or refresh a `HOST` row using UPSERT semantics.

        Missing statistical fields are initialized only for new rows, while
        explicit caller values always win.

        Args:
            **kwargs: Fields for the HOST table, e.g.:
                ID_HOST=1001,
                NA_HOST_UID="host-xyz",
                NA_HOST_ADDRESS="192.168.1.10",
                NA_HOST_PORT=22,
                NA_HOST_USER="root",
                NA_HOST_PASSWORD="pass123"

        Raises:
            ValueError: Invalid fields.
            Exception: Database or SQL errors.
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

            # ------------------------------------------------------------------
            # Default initialization (only used if record is NEW)
            # ------------------------------------------------------------------
            defaults = {
                "IS_OFFLINE": False,
                "NU_HOST_CHECK_ERROR": 0,
                "NU_HOST_FILES": 0
            }

            # Merge defaults with provided arguments (kwargs overwrite defaults)
            data = {**defaults, **kwargs}

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
            # ------------------------------------------------------------------
            # Dynamically get valid columns for HOST table
            # ------------------------------------------------------------------
            valid_fields = self.VALID_FIELDS_HOST
            if not valid_fields:
                raise ValueError("Valid fields for HOST table not defined in handler.")

            # Perform dynamic SELECT using all valid fields
            rows = self._select_rows(
                table="HOST",
                where={"ID_HOST": host_id},
                limit=1,
                cols=list(valid_fields),
            )

            if not rows:
                return {"status": 0}

            row = rows[0]

            # Convert any DATETIME fields into UNIX timestamps
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

        Numeric values are additive by default, which is convenient for
        counters. Pass `reset=True` to switch to direct assignment semantics.
        `("INC", x)` and `("DEC", x)` are also supported for explicit updates.
        """

        # Connect to DB
        self._connect()
        # ==========================================================
        # 1. Optional busy-timeout check
        # ==========================================================
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

        # ==========================================================
        # 2. Validation
        # ==========================================================
        if not kwargs:
            self.log.warning(f"No fields provided for host_update (ID={host_id}).")
            return

        for key in kwargs.keys():
            if key not in self.VALID_FIELDS_HOST:
                raise ValueError(f"Invalid field '{key}' for HOST table update.")

        # ==========================================================
        # 3. Prepare direct + arithmetic updates
        # ==========================================================
        arithmetic_updates = []
        direct_updates = {}

        for field, value in kwargs.items():

            if value is None:
                continue

            # ---------------------------------------------
            # NEW FEATURE: ("INC", x)
            # ---------------------------------------------
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

                # fallback → treat tuple as direct assignment
                direct_updates[field] = value
                continue

            # ---------------------------------------------
            # Original behavior
            # ---------------------------------------------
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

        # ==========================================================
        # 4. Execute SQL
        # ==========================================================
        try:
            # Direct updates
            if direct_updates:
                self._update_row(
                    table="HOST",
                    data=direct_updates,
                    where={"ID_HOST": host_id},
                    commit=False,
                )

            # Arithmetic updates (with params)
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
        progress counters and timestamps, instead of trying to infer state from
        the transient task tables.
        """

        self._connect()
        try:
            # ==============================================================
            # 1) Counters + timestamps
            # ==============================================================
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

            # Safe defaults
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

            # ==============================================================
            # 2) Backup volume (KB)
            # ==============================================================
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

            # ==============================================================
            # 3) Update HOST table
            # ==============================================================
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

        Rules:
            • Release if owned by current PID
            • Preserve short-lived transient SFTP cooldowns
            • Release if PID is stale
            • Ignore if owned by another active process
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

        # stale pid detection
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

        A host lock is considered stale when:
            • No FILE_TASK is running for the host AND
            • No HOST_TASK of type PROCESSING is running for the host

        OR when the BUSY duration exceeds `threshold_seconds` and the owner PID
        is no longer alive.

        In both cases the host is released and a connection-check task is
        scheduled so the orchestration layer can reconcile state safely.
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

            # -------------------------------------------------
            # Safe elapsed computation
            # -------------------------------------------------
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

            # -------------------------------------------------
            # Release host lock
            # -------------------------------------------------
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

            # -------------------------------------------------
            # Schedule connectivity check
            # -------------------------------------------------
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
        Recover stale host-dependent HOST_TASK rows without forcing SSH preemption.

        The janitor focuses on the host-task state itself:
            - PENDING rows with missing `DT_HOST_TASK` get normalized so the
              discovery reservation TTL can expire deterministically.
            - RUNNING rows older than `stale_after_seconds` are reset to
              PENDING when execution ownership no longer matches reality
              (host unlocked, stale PID, or mismatched host/task owner).

        Host release remains conservative: only obviously stale host locks are
        cleared here. Normal connectivity reconciliation still belongs to the
        existing host cleanup / host_check paths.
        """
        now = datetime.now()
        operational_types = (
            k.HOST_TASK_CHECK_TYPE,
            k.HOST_TASK_PROCESSING_TYPE,
            k.HOST_TASK_CHECK_CONNECTION_TYPE,
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
                WHERE HT.NU_TYPE IN (%s, %s, %s)
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

            # Normalize legacy/residual PENDING rows without timestamps so the
            # discovery reservation can age out deterministically.
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

            # Legitimate long-running execution still owns the host and keeps
            # the same live PID on both HOST and HOST_TASK.
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

        IMPORTANT:
        This function is intentionally generic. Callers MUST explicitly
        decide which task states are relevant (e.g. ACTIVE vs TERMINAL).

        Returns:
            list[dict]: Matching HOST_TASK rows (may be empty).
        """

        valid_fields = self.VALID_FIELDS_HOST_TASK
        where_clause = {}

        for key, value in kwargs.items():

            # Validate field name
            if key not in valid_fields:
                raise ValueError(
                    f"Invalid field in check_host_task(): '{key}'"
                )

            # List → IN operator
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

    def _normalize_host_task_filter(self, filter_value: Any) -> Any:
        """
        Return a semantic representation of a HOST_TASK filter payload.

        `HOST_TASK.FILTER` may arrive as dict, JSON string, or raw bytes from
        the DB connector. Queue deduplication must compare filters by meaning,
        not by textual key order.
        """
        if filter_value is None:
            return dict(k.NONE_FILTER)

        if isinstance(filter_value, (bytes, bytearray)):
            filter_value = filter_value.decode("utf-8")

        if isinstance(filter_value, str):
            try:
                return json.loads(filter_value)
            except json.JSONDecodeError:
                return filter_value

        if isinstance(filter_value, dict):
            return dict(filter_value)

        return filter_value

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

    def _canonicalize_host_task_filter(self, filter_value: Any) -> str:
        """
        Build a deterministic string key for semantic HOST_TASK filter matching.
        """
        normalized = self._normalize_host_task_filter(filter_value)
        return json.dumps(normalized, sort_keys=True, ensure_ascii=False)

    def _find_reusable_operational_host_task(
        self,
        tasks: list[dict],
    ) -> tuple[Optional[dict], int]:
        """
        Pick the best reusable CHECK/PROCESSING row for a host.

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

    def _find_matching_host_task(
        self,
        tasks: list[dict],
        *,
        filter_value: Any,
    ) -> tuple[Optional[dict], int]:
        """
        Return the best semantic filter match among already selected HOST_TASK rows.

        Preference order:
            1. Any ACTIVE row (PENDING / RUNNING)
            2. Otherwise the newest matching row
        """
        target_filter = self._canonicalize_host_task_filter(filter_value)
        matches = [
            task
            for task in tasks
            if self._canonicalize_host_task_filter(
                task.get("HOST_TASK__FILTER")
            ) == target_filter
        ]

        if not matches:
            return None, 0

        active = next(
            (
                task
                for task in matches
                if task.get("HOST_TASK__NU_STATUS")
                in (k.TASK_PENDING, k.TASK_RUNNING)
            ),
            None,
        )

        return active or matches[0], len(matches)

    
    def queue_host_task(
        self,
        host_id: int,
        task_type: int,
        task_status: int,
        filter_dict: dict,
    ) -> dict:
        """
        Deterministically enqueue or refresh an operational host task.

        Guarantees:
            - At most ONE CHECK/PROCESSING task row is reused per host
            - No statistics task is created here
        """
        operational_types = (
            k.HOST_TASK_CHECK_TYPE,
            k.HOST_TASK_PROCESSING_TYPE,
        )
        lookup_types: Union[int, list[int]]

        if task_type in operational_types:
            lookup_types = list(operational_types)
        else:
            lookup_types = task_type

        tasks = self.check_host_task(
            FK_HOST=host_id,
            NU_TYPE=lookup_types,
        )

        if task_type in operational_types:
            existing, match_count = self._find_reusable_operational_host_task(tasks)
        else:
            existing, match_count = self._find_matching_host_task(
                tasks,
                filter_value=filter_dict,
            )

        if match_count > 1:
            if task_type in operational_types:
                warning_message = (
                    "[DBHandlerBKP] Multiple operational HOST_TASK rows matched "
                    f"host={host_id}, type={task_type}, matches={match_count}. "
                    "Reusing one row and leaving cleanup to maintenance tooling."
                )
            else:
                warning_message = (
                    "[DBHandlerBKP] Multiple matching HOST_TASK rows found "
                    f"host={host_id}, type={task_type}, matches={match_count}. "
                    "Reusing one row and leaving cleanup to maintenance tooling."
                )

            self.log.warning(warning_message)

        if existing:
            status = existing["HOST_TASK__NU_STATUS"]
            existing_type = existing.get("HOST_TASK__NU_TYPE")

            # RUNNING task → preserve the live execution context. A new request
            # should not rewrite the filter of an in-flight worker.
            if status == k.TASK_RUNNING and task_type in operational_types:
                self.log.warning(
                    "[DBHandlerBKP] Operational HOST_TASK already RUNNING; "
                    f"preserving current execution (host={host_id}, "
                    f"task_id={existing['HOST_TASK__ID_HOST_TASK']}, "
                    f"type={existing_type})."
                )

            # PENDING task or terminal row -> refresh in place.
            else:
                self.host_task_update(
                    task_id=existing["HOST_TASK__ID_HOST_TASK"],
                    FILTER=filter_dict,
                    NU_TYPE=task_type,
                    NU_STATUS=task_status,
                    DT_HOST_TASK=datetime.now(),
                    NA_MESSAGE=(
                        "Operational HOST_TASK refreshed by queue_host_task "
                        f"(previous type={existing_type}, status={status})"
                    ),
                )

        else:
            # No operational task exists → create new
            self.host_task_create(
                NU_TYPE=task_type,
                NU_STATUS=task_status,
                FK_HOST=host_id,
                FILTER=filter_dict,
            )

        # Update statistics
        self.host_update_statistics(host_id)
        
        # Return current host status snapshot
        return self.host_read_status(host_id)

    
    def host_task_create(self, **kwargs) -> int:
        """
        Create a new `HOST_TASK` row and return its generated ID.

        Dynamically builds the INSERT statement using only fields defined in
        `VALID_FIELDS_HOST_TASK`. Automatically serializes FILTER (if dict),
        sets defaults for missing critical fields, and performs validation.

        Args:
            **kwargs: Arbitrary keyword arguments corresponding to HOST_TASK columns.
                Typical examples:
                    FK_HOST (int): Foreign key to HOST table.
                    NU_TYPE (int): Task type (e.g., discovery, backup).
                    FILTER (dict | str): JSON filter defining selection parameters.
                    NU_STATUS (int): Task status (defaults to k.TASK_PENDING).
                    NA_MESSAGE (str): Optional message (auto-filled if missing).

        Returns:
            int: The newly created HOST_TASK.ID_HOST_TASK.

        Raises:
            ValueError: If invalid fields are passed.
            mysql.connector.Error: On SQL execution or commit failure.
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

            # --- Default timestamps and statuses ---
            if "DT_HOST_TASK" not in payload:
                payload["DT_HOST_TASK"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if "NU_STATUS" not in payload:
                payload["NU_STATUS"] = k.TASK_PENDING

            # --- Handle FILTER field properly ---
            payload["FILTER"] = self._serialize_host_task_filter(
                payload.get("FILTER")
            )

            # --- Default message ---
            if "NA_MESSAGE" not in payload:
                host_id = payload.get("FK_HOST", "UNKNOWN")
                payload["NA_MESSAGE"] = f"New HOST_TASK created for host {host_id}"

            # --- Execute safe INSERT ---
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

        Ensures that each host has at most one pending statistics task.

        Args:
            host_id (int): ID of the host to which the statistics task belongs.

        Returns:
            int: The ID_HOST_TASK of the created or reactivated task.
        """

        try:
            self._connect()

            # Base payload
            dt_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            payload = {
                "FK_HOST": host_id,
                "NU_TYPE": k.HOST_TASK_UPDATE_STATISTICS_TYPE,
                "FILTER": k.NONE_FILTER,
                "NU_STATUS": k.TASK_PENDING,
                "DT_HOST_TASK": dt_now,
                "NA_MESSAGE": f"Update host statistics for host {host_id}",
            }

            # Ensure FILTER is always serialized as string or JSON
            if isinstance(payload["FILTER"], dict):
                payload["FILTER"] = json.dumps(payload["FILTER"], ensure_ascii=False)

            # Check if a statistics entry already exists
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
                # unbounded INSERT/DELETE churn in HOST_TASK.
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

            # No task found — create new
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
        consumers such as discovery should skip hosts already marked offline.
        """

        self._connect()
        try:
            if check_host_busy:
                self._release_expired_transient_busy_cooldowns()

            where = {}

            # 1 — Lookup direto
            if task_id:
                where["HT.ID_HOST_TASK"] = task_id

            else:
                where["HT.NU_STATUS"] = task_status

                # 2 — Suporte a 1 tipo OU lista de tipos
                if task_type is not None:
                    if isinstance(task_type, list):
                        # use IN
                        where["HT.NU_TYPE"] = ("IN", task_type)
                    else:
                        # single equality
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

            # --------------------------------------------------------------
            # Optional atomic host lock
            # --------------------------------------------------------------
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

                # Discovery can opt into this guard so a host that flipped
                # offline after SELECT is not claimed by the atomic lock.
                if check_host_offline:
                    lock_query += " AND IS_OFFLINE = 0"

                self.cursor.execute(lock_query, lock_params)

                if self.cursor.rowcount == 0:
                    return None

                self.db_connection.commit()

            # Parse JSON filter
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
        """Safely update HOST_TASK fields with validation, supporting task_id or custom where_dict.

        Args:
            task_id (Optional[int]): Task ID (ID_HOST_TASK). Required unless where_dict is provided.
            where_dict (Optional[Dict[str, Any]]): Optional WHERE condition for bulk updates.
            **kwargs: Fields to update. Only keys in VALID_FIELDS_HOST_TASK are applied.

        Returns:
            Dict[str, Any]: {
                "success": bool,
                "rows_affected": int,
                "updated_fields": Dict[str, Any]
            }

        Raises:
            ValueError: If neither task_id nor where_dict is provided.
            Exception: On SQL execution or commit failure.
        """

        # --- Validation of target criteria ---
        if task_id is None and not where_dict:
            raise ValueError("host_task_update() requires either 'task_id' or 'where_dict' argument.")

        valid_fields = getattr(self, "VALID_FIELDS_HOST_TASK", set())
        set_dict: Dict[str, Any] = {}

        # --- Build update dictionary (only valid fields) ---
        for key, value in kwargs.items():
            if key in valid_fields:
                set_dict[key] = value
            else:
                self.log.warning(f"[DB] Ignored invalid field '{key}' in host_task_update().")

        # --- Apply business logic if applicable ---
        if "NU_STATUS" in set_dict:
            status = set_dict["NU_STATUS"]
            if status == k.TASK_PENDING:
                set_dict["NU_PID"] = None
            elif status == k.TASK_RUNNING and "NU_PID" not in set_dict:
                set_dict["NU_PID"] = getattr(self.log, "pid", None)

        if "FILTER" in set_dict:
            set_dict["FILTER"] = self._serialize_host_task_filter(set_dict["FILTER"])

        # --- Nothing to update ---
        if not set_dict:
            self.log.warning(f"[DB] host_task_update() called with no valid fields for update.")
            return {"success": False, "rows_affected": 0, "updated_fields": {}}

        # --- Determine WHERE condition ---
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
        """Delete a host task from HOST_TASK table by its ID.

        Args:
            task_id (int): ID of the task to delete.

        Returns:
            bool: True if deletion succeeded, False otherwise.
        """
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

        Args:
            host_id (int): Foreign key of the host whose tasks should be resumed.
            busy_timeout_seconds (int): Timeout interval defining when a running
                task is considered stale. Defaults to k.HOST_BUSY_TIMEOUT.

        Returns:
            None
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

        Args:
            task_id (Optional[int]):
                Direct lookup by FILE_TASK ID.
            task_status (Optional[int]):
                Task status filter.
            task_type (Optional[int]):
                Task type filter.
            check_host_busy (bool):
                Exclude BUSY hosts if True.
            check_host_offline (bool):
                Exclude hosts already marked OFFLINE if True.
            extension (Optional[str]):
                File extension filter.
            lock_host (bool):
                If True, attempts to atomically lock the host
                (IS_BUSY=1) before returning the task.
            reserve_hosts_for_discovery (bool):
                When True for backup selection, treats active CHECK/PROCESSING
                HOST_TASK rows as a reservation for the next host window. This
                does not preempt current SSH work; it only prevents backup from
                claiming a fresh FILE_TASK on that host.
            fair_by_host (bool):
                When True for backup selection, chooses at most one candidate
                FILE_TASK per host and orders hosts by least recent successful
                backup before task age.

        Returns:
            Optional[tuple]:
                (row_dict, host_id, file_task_id) or None.
        """

        # --------------------------------------------------------------
        # 0) Connect
        # --------------------------------------------------------------
        self._connect()

        if check_host_busy:
            self._release_expired_transient_busy_cooldowns()

        # --------------------------------------------------------------
        # 1) WHERE clause
        # --------------------------------------------------------------
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

        # --------------------------------------------------------------
        # 2) Select candidate task
        # --------------------------------------------------------------
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

        # --------------------------------------------------------------
        # 3) Optional atomic host lock
        # --------------------------------------------------------------
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
                # Another worker locked it
                self._disconnect()
                return None

            self.db_connection.commit()

        # --------------------------------------------------------------
        # 4) Return task
        # --------------------------------------------------------------
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

        This method supports both single-file and batch ingestion and automatically
        selects the optimal UPSERT strategy.

        IMPORTANT:
            • file_metadata MUST be a list of FileMetadata objects.
            • This method intentionally performs full materialization of rows
            before database interaction.
        """

        if not file_metadata:
            return 0

        self._connect()
        processed = 0

        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rows: list[dict] = []

            for file in file_metadata:
                rows.append({
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
                })

            # --------------------------------------------------------------
            # Single-row UPSERT (preserves legacy behavior)
            # --------------------------------------------------------------
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

            # --------------------------------------------------------------
            # Batch UPSERT path
            # --------------------------------------------------------------
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

            expected_status:
                If provided, the update will only occur when
                FILE_TASK.NU_STATUS matches this value.

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

        # Mirror HOST_TASK semantics: when a FILE_TASK returns to PENDING, it
        # should no longer advertise ownership by a worker PID.
        if "NU_STATUS" in kwargs:
            status = kwargs["NU_STATUS"]
            if status == k.TASK_PENDING and "NU_PID" not in kwargs:
                kwargs["NU_PID"] = None
            elif status == k.TASK_RUNNING and "NU_PID" not in kwargs:
                kwargs["NU_PID"] = getattr(self.log, "pid", None)

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
        """Delete a `FILE_TASK` by its primary key.

        Args:
            task_id (int): Value of `FILE_TASK.ID_FILE_TASK`.

        Returns:
            int: Number of deleted rows (0 or 1).

        Raises:
            mysql.connector.Error: On DELETE/COMMIT failure.
        """
        self._connect()
        try:
            # Adapted to new _delete_row signature using 'where' dict
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

        This is used by backup cleanup when a file vanished from the remote
        host after discovery and must not remain resumable in history.

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
                    data={
                        "NU_STATUS": k.TASK_SUSPENDED,
                        "NA_MESSAGE": message,
                    },
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
                data={
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        "Host reachable again — suspended file task resumed automatically"
                    ),
                },
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
                data={
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        "Host reachable again — previously failed file task resubmitted"
                    ),
                },
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
                data={
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        f"Detected stale running file task (> {busy_timeout_seconds}s) — "
                        f"resubmitted automatically"
                    ),
                },
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
                data={
                    "NU_STATUS_DISCOVERY": k.TASK_SUSPENDED,
                    "NA_MESSAGE": message,
                },
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
                data={
                    "NU_STATUS_BACKUP": k.TASK_SUSPENDED,
                    "NA_MESSAGE": message,
                },
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
                data={
                    "NU_STATUS_DISCOVERY": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        "Host reachable again — discovery resumed automatically"
                    ),
                },
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
                data={
                    "NU_STATUS_BACKUP": k.TASK_PENDING,
                    "NA_MESSAGE": (
                        "Host reachable again — backup resumed automatically"
                    ),
                },
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

        # Validate and build WHERE clause
        where_clause = {}
        for key, value in kwargs.items():
            if key not in valid_fields:
                raise ValueError(f"Invalid field in _check_file_history(): '{key}' is not a valid column.")
            where_clause[key] = value

        # Execute the SELECT query
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
            • This method is DB-driven only.
            • No filesystem inspection.
            • No file lists or per-file decisions.
            • MODE_FILE is resolved via SQL patterns (LIKE), not explicit names.
            • Safe for very large backlogs (Celplan-scale).

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
        }

        self._connect()

        try:
            # -----------------------------------------------------
            # Build SQL filtering metadata (WHERE / ORDER / LIMIT)
            # -----------------------------------------------------
            filter_obj = filter.Filter(task_filter, log=self.log)

            meta = filter_obj.evaluate_database(
                host_id=host_id,
                search_type=search_type,
                search_status=search_status,
            )

            where = meta.get("where")
            extra_sql = meta.get("extra_sql", "")
            msg_prefix = meta.get("msg_prefix")

            # Filter resolved to a no-op
            if not where:
                self.log.entry(
                    "[update_backlog_by_filter] Filter resolved to no-op. Skipping UPDATE."
                )
                return summary

            # -----------------------------------------------------
            # Execute UPDATE on FILE_TASK
            # -----------------------------------------------------
            sql_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            rows_updated = self._update_row(
                table="FILE_TASK",
                data={
                    "NU_TYPE": new_type,
                    "NU_STATUS": new_status,
                    "DT_FILE_TASK": sql_now,
                    "NA_MESSAGE__expr": (
                        f"CONCAT('{msg_prefix} of file ', "
                        f"NA_HOST_FILE_PATH, '/', NA_HOST_FILE_NAME)"
                    ),
                },
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
                f"(new_type={new_type}, new_status={new_status}) for host {host_id}"
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

        Unique keys:
            (FK_HOST, NA_HOST_FILE_NAME, NU_TYPE)

        IMPORTANT:
            • file_metadata MUST be a list of FileMetadata objects.
            • NU_TYPE is part of the UNIQUE INDEX but is not explicitly
            stored in the payload (legacy behavior preserved).
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

                rows.append({
                    "FK_HOST": host_id,
                    "NA_HOST_FILE_PATH": file.NA_PATH,
                    "NA_HOST_FILE_NAME": file.NA_FILE,
                    "NA_EXTENSION": file.NA_EXTENSION,
                    "VL_FILE_SIZE_KB": file.VL_FILE_SIZE_KB,
                    "DT_FILE_CREATED": file.DT_FILE_CREATED,
                    "DT_FILE_MODIFIED": file.DT_FILE_MODIFIED,

                    # lifecycle timestamps
                    "DT_DISCOVERED": now,
                    "DT_BACKUP": None,
                    "DT_PROCESSED": None,
                    "NU_STATUS_DISCOVERY": k.TASK_DONE,
                    "NU_STATUS_BACKUP": k.TASK_PENDING,
                    "NU_STATUS_PROCESSING": k.TASK_PENDING,

                    "NA_MESSAGE": msg,
                })

            # --------------------------------------------------------------
            # Single-row UPSERT
            # --------------------------------------------------------------
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

            # --------------------------------------------------------------
            # Batch UPSERT path
            # --------------------------------------------------------------
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
            - Field omitted      -> not updated
            - Field=None         -> not updated
            - Field=SET_NULL     -> explicitly updated to NULL
            - Field=value        -> updated

        Timestamp ownership:
            `DT_BACKUP` and `DT_PROCESSED` are caller-owned. The function keeps
            `task_type` only for signature compatibility and call-site clarity;
            it no longer injects implicit timestamps.
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

        Rationale:
            • NTFS may provide sub-second precision.
            • MySQL DATETIME may truncate microseconds.
            • Different drivers may alter timestamp precision.
            • Exact datetime equality is therefore unsafe.

            By normalizing to minute precision we guarantee:
                - Deterministic deduplication
                - Stability across re-discovery cycles
                - Compatibility with historical records
                - Independence from filesystem precision differences

        Architectural guarantees:
            • No modification of existing DB records required.
            • No dependency on DB column precision.
            • Idempotent behavior under REDISCOVERY mode.
            • Batch memory bounded by `batch_size`.

        Constraints:
            • Assumes CelPlan does not generate two distinct files
            with same name, size and creation minute.
            • If that assumption changes, identity model must be revisited.

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

        # ------------------------------------------------------------
        # Build a derived table from the in-memory batch
        #
        # Each row contains:
        #   - file name
        #   - creation timestamp (raw)
        #   - file size
        #
        # Deduplication is delegated to SQL using minute-level comparison.
        # ------------------------------------------------------------
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

        # ------------------------------------------------------------
        # Bind parameters (no normalization here — SQL handles minute logic)
        # ------------------------------------------------------------
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

        # ------------------------------------------------------------
        # Execute deduplication query
        # ------------------------------------------------------------
        self._connect()
        rows = self._select_raw(sql, tuple(params))

        # ------------------------------------------------------------
        # Build set of existing identity keys (normalized to minute)
        #
        # Even though SQL already matches by minute, we normalize again
        # here for deterministic in-memory comparison.
        # ------------------------------------------------------------
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
        Return history rows eligible for payload garbage collection.

        Criteria:
            - NU_STATUS_PROCESSING = -1
            - IS_PAYLOAD_DELETED = 0
            - DT_FILE_CREATED older than quarantine_days
            (NULL timestamps are also considered eligible)

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
                f"(DT_FILE_CREATED IS NULL OR "
                f"DT_FILE_CREATED < NOW() - INTERVAL {quarantine_days} DAY)"
            )
        }

        return self._select_rows(
            table="FILE_TASK_HISTORY",
            where=where,
            order_by="DT_FILE_CREATED, ID_HISTORY",
            limit=batch_size,
            cols=[
                "ID_HISTORY",
                "NA_SERVER_FILE_PATH",
                "NA_SERVER_FILE_NAME"
            ]
        )
        
    
