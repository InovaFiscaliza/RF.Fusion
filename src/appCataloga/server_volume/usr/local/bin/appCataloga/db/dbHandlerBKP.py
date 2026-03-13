
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dbHandlerBKP
---------------------------------

High-level handler for the *BKP* domain of appCataloga. This class exposes
operations for `HOST`, `HOST_TASK`, and `FILE_TASK`, reusing the generic CRUD
and execution helpers provided by `DBHandlerBase`.

This version contains **complete Google-Style docstrings** and **commentary**
to make intent and reasoning explicit, while keeping behavior identical to the
refactored versions.

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
from shared import filter, legacy, constants, tools
from shared.file_metadata import FileMetadata
from datetime import datetime as _dt


class dbHandlerBKP(DBHandlerBase):
    """BKP domain handler (hosts, host tasks, file tasks).

    This class centralizes all database interactions related to:
    - `HOST` (connection data and counters)
    - `HOST_TASK` (scheduled actions against a host)
    - `FILE_TASK` (per-file work items: discovery, backup, processing)

    It **never** issues raw SQL directly; instead it delegates to `DBHandlerBase`
    helpers (`_select_rows`, `_insert_row`, `_update_row`, `_delete_row`,
    `_select_custom`, `_execute_custom`, `_execute_many_custom`).

    All constants are referenced directly from `config as k` to avoid shadowing.
    """
    # ------------------------------------------------------------------
    # Table field definitions (for validation and consistency)
    # ------------------------------------------------------------------
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
        """Initialize the handler with the target logical database and logger.

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
    
    def host_exists(self, host_id: int) -> bool:
        """Check whether a host_id exists in the HOST table.

        Args:
            host_id (int): Primary key (ID_HOST) to check.

        Returns:
            bool: True if the host exists, False otherwise.

        Raises:
            mysql.connector.Error: If a database error occurs.
        """
        try:
            self._connect()
            sql = "SELECT COUNT(*) FROM HOST WHERE ID_HOST = %s;"
            self.cursor.execute(sql, (host_id,))
            count = self.cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            self.log.error(f"[DBHandlerBKP] Failed to check host existence for ID {host_id}: {e}")
            raise
        finally:
            self._disconnect()
            
    def host_upsert(self, **kwargs) -> None:
        """
        Create or update a HOST record using UPSERT semantics.

        Any provided field in kwargs will be inserted if the host does not exist,
        or updated if it already exists. Missing numeric/statistical fields are
        initialized with defaults.

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

    
    def host_create(self, **kwargs) -> None:
        """
        Insert a new host record into the HOST table if it does not exist.

        This method accepts dynamic keyword arguments and validates them against
        the predefined list of valid fields for the HOST table. Missing fields
        are initialized with zero or NULL defaults as appropriate.

        If the host already exists, the operation is ignored due to
        INSERT IGNORE semantics.

        Args:
            **kwargs: Key-value pairs corresponding to columns in the HOST table.
                Example:
                    ID_HOST=1001,
                    NA_HOST_UID="host-xyz",
                    NA_HOST_ADDRESS="192.168.1.10",
                    NA_HOST_PORT=22,
                    NA_HOST_USER="root",
                    NA_HOST_PASSWORD="pass123"

        Returns:
            None

        Raises:
            ValueError: If any provided field is invalid.
            Exception: On SQL or database errors.
        """
        try:
            # Ensure connection
            self._connect()

            # ------------------------------------------------------------------
            # Validation
            # ------------------------------------------------------------------
            valid_fields = self.VALID_FIELDS_HOST
            if valid_fields is None:
                raise ValueError("Valid fields for HOST table not defined in handler.")

            for key in kwargs.keys():
                if key not in valid_fields:
                    raise ValueError(f"Invalid field '{key}' for HOST table.")

            # ------------------------------------------------------------------
            # Default initialization for numeric/statistical fields
            # ------------------------------------------------------------------
            defaults = {
                "IS_OFFLINE": False,
                "NU_HOST_CHECK_ERROR": 0,
                "NU_HOST_FILES": 0
            }

            # Merge defaults with provided arguments (kwargs overwrite defaults)
            data = {**defaults, **kwargs}

            # ------------------------------------------------------------------
            # Perform safe insertion (INSERT IGNORE)
            # ------------------------------------------------------------------
            self._insert_row(
                table="HOST",
                data=data,
                ignore=True,   # Enables INSERT IGNORE semantics
                commit=True,
            )

            self.log.entry(f"[DBHandlerBKP] HOST {data.get('ID_HOST')} ({data.get('NA_HOST_UID')}) created or already exists.")

        except Exception as e:
            self.log.error(f"[DBHandlerBKP] Failed to create HOST record: {e}")
            raise

        finally:
            self._disconnect()
            
    def host_check_free(self, host_id: int, task_type: int) -> bool:
        """
        Check if a host has NO RUNNING FILE_TASK of a given type.

        Returns:
            True  -> host is free (can be released)
            False -> host still has running tasks
        """
        self._connect()
        try:
            rows = self._select_rows(
                table="FILE_TASK",
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS": k.TASK_RUNNING,
                    "NU_TYPE": task_type,
                },
                limit=1,
                cols=["ID_FILE_TASK"],
            )

            # If no RUNNING task exists, host is free
            return len(rows) == 0

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
        Read all valid host counters and timestamps dynamically from the HOST table.

        This method automatically retrieves all columns listed in `self.valid_fields["HOST"]`
        and returns them as a dictionary. No static column list is needed, ensuring
        schema flexibility and consistency with future updates.

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


    def host_read_all(self) -> list[dict]:
        """
        Retrieve all registered hosts dynamically using `valid_fields`.

        This method reads every row from the HOST table, automatically
        including only the columns defined in `self.valid_fields["HOST"]`.
        The result is returned as a list of dictionaries.

        Returns:
            list[dict]: List of all hosts, each represented as a dictionary.
                        Returns an empty list if no hosts are found.

        Raises:
            ValueError: If `valid_fields` for HOST table is not defined.
            mysql.connector.Error: On query execution or connection failure.
        """
        self._connect()
        try:
            # ------------------------------------------------------------------
            # Load valid fields dynamically
            # ------------------------------------------------------------------
            valid_fields = self.VALID_FIELDS_HOST
            if not valid_fields:
                raise ValueError("Valid fields for HOST table not defined in handler.")

            # ------------------------------------------------------------------
            # Perform dynamic SELECT
            # ------------------------------------------------------------------
            rows = self._select_rows(
                table="HOST",
                cols=list(valid_fields),
                order_by="ID_HOST ASC"
            )

            # ------------------------------------------------------------------
            # Handle empty results
            # ------------------------------------------------------------------
            if not rows:
                self.log.entry("[DBHandlerBKP] No hosts found in HOST table.")
                return []

            self.log.entry(f"[DBHandlerBKP] Retrieved {len(rows)} host record(s).")
            return rows

        except Exception as e:
            self.log.error(f"[DBHandlerBKP] Failed to read HOST table: {e}")
            return []

        finally:
            self._disconnect()
    
    def get_last_discovery(self, host_id: int) -> Optional[datetime]:
        """
        Return HOST.DT_LAST_DISCOVERY for the given host_id.
        Uses _select_rows(), which returns a list of dicts.
        """

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
        Release all HOST rows locked by the given PID.
        Safe to call multiple times.
        """
        self._connect()
        self._update_row(
            table="HOST",
            data={
                "IS_BUSY": False,
                "NU_PID": 0,
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
                        kwargs["NU_PID"]  = 0

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
        Recalculate and update HOST statistics based ONLY on FILE_TASK_HISTORY.

        Status semantics:
            1  → Pending
            0  → Done
        -1  → Error
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
        Safely release a HOST lock.

        Rules:
            • Release if owned by current PID
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

        if pid == current_pid:
            self.log.warning(
                f"[CLEANUP] Releasing host lock owned by this worker "
                f"(host_id={host_id}, pid={current_pid})"
            )

            self.host_update(
                host_id=host_id,
                IS_BUSY=False,
                NU_PID=0,
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
                NU_PID=0,
            )
            return

        self.log.entry(
            f"[CLEANUP] Host lock owned by another worker "
            f"(host_id={host_id}, owner_pid={pid})"
        )

    def host_cleanup_stale_locks(self, threshold_seconds: int) -> None:
        """
        Release HOST records stuck in BUSY state.

        A host lock is considered stale when:
            • No FILE_TASK is running for the host AND
            • No HOST_TASK is running for the host

        OR when the BUSY duration exceeds `threshold_seconds`.

        In these cases the host is released and a connection check
        is scheduled.
        """

        now = datetime.now()

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
                ) AS HOST_RUNNING
            FROM HOST H
            WHERE H.IS_BUSY = TRUE
        """, (k.TASK_RUNNING, k.TASK_RUNNING))

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
            host_running = row["HOST_RUNNING"]

            # -------------------------------------------------
            # Safe elapsed computation
            # -------------------------------------------------
            if busy_since is None:
                elapsed = float("inf")
            else:
                elapsed = (now - busy_since).total_seconds()

            release = False
            reason = None

            if not file_running and not host_running:
                release = True
                reason = "NO_RUNNING_TASKS"

            elif elapsed > threshold_seconds:
                release = True
                reason = "BUSY_TIMEOUT"

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
                    NU_PID=0
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
                self.queue_host_task(
                    host_id=host_id,
                    task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE,
                    task_status=k.TASK_PENDING,
                    filter_dict=k.NONE_FILTER,
                )

                self.log.entry(
                    f"[HOST_CLEANUP] Connection check scheduled "
                    f"(host_id={host_id})"
                )

            except Exception as e:
                self.log.error(
                    f"[HOST_CLEANUP] Failed to queue connection check "
                    f"(host_id={host_id}): {e}"
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

    
    def queue_host_task(
        self,
        host_id: int,
        task_type: int,
        task_status: int,
        filter_dict: dict,
    ) -> dict:
        """
        Deterministically enqueue or refresh an operational HOST_TASK
        (CHECK or PROCESSING) for a given host.

        Guarantees:
            - At most ONE CHECK/PROCESSING task per host
            - No statistics task is created here
        """

        filter_json = json.dumps(filter_dict)

        # Fetch existing operational task (CHECK / PROCESSING)
        tasks = self.check_host_task(
            FK_HOST=host_id,
            NU_TYPE=task_type,
            FILTER=filter_json,
        )

        existing = tasks[0] if tasks else None

        if existing:
            status = existing["HOST_TASK__NU_STATUS"]

            # ACTIVE task → do nothing
            if status in (k.TASK_PENDING, k.TASK_RUNNING):
                pass

            # TERMINAL task → refresh
            # Have to be reseted to HOST_CHECK because first
                self.host_task_update(
                    task_id=existing["HOST_TASK__ID_HOST_TASK"],
                    NU_TYPE=task_type,
                    NU_STATUS=k.TASK_PENDING,
                    DT_HOST_TASK=datetime.now(),
                    NA_MESSAGE=(
                        "Operational HOST_TASK refreshed by queue_host_task "
                        f"(previous status={status})"
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
        Create a new HOST_TASK entry and return its generated ID.

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
            if "FILTER" not in payload or payload["FILTER"] is None:
                payload["FILTER"] = k.NONE_FILTER
            elif isinstance(payload["FILTER"], dict):
                payload["FILTER"] = json.dumps(payload["FILTER"], ensure_ascii=False)
            elif isinstance(payload["FILTER"], str):
                # Validate JSON string
                try:
                    json.loads(payload["FILTER"])
                except json.JSONDecodeError:
                    raise ValueError("FILTER must be a valid JSON string or dict.")

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
        Create or reactivate a HOST_TASK of type HOST_TASK_UPDATE_STATISTICS_TYPE
        for the specified host.

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
                status = existing.get("NU_STATUS", k.TASK_PENDING)

                if status == k.TASK_PENDING:
                    self.log.entry(
                        f"[DBHandlerBKP] Statistics HOST_TASK already pending "
                        f"(host={host_id}, ID={tid}). No action taken."
                    )
                    return tid

                # Reactivate the existing task
                self.host_task_update(
                    task_id=tid,
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
    ) -> Optional[tuple]:

        self._connect()
        try:
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

        # --- Nothing to update ---
        if not set_dict:
            self.log.warning(f"[DB] host_task_update() called with no valid fields for update.")
            return {"success": False, "rows_affected": 0, "updated_fields": {}}

        # --- Determine WHERE condition ---
        where = where_dict if where_dict else {"ID_HOST_TASK": task_id}

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
        Suspend all HOST_TASK entries for a specific host.

        This method updates all pending (TASK_PENDING) or running (TASK_RUNNING)
        HOST_TASK records related to the specified host, setting their status
        to TASK_SUSPENDED and updating the NA_MESSAGE field with an explanatory
        reason.

        Args:
            host_id (int): Unique identifier (ID_HOST) of the host whose tasks
                should be suspended.
            reason (str, optional): Optional text reason to store in NA_MESSAGE.
                If not provided, a default system message is used.

        Returns:
            None

        Raises:
            Exception: Propagates any database access or SQL execution errors.
        """
        try:
            affected = 0

            for status in (k.TASK_PENDING, k.TASK_RUNNING):
                affected += self._update_row(
                    table="HOST_TASK",
                    data={"NU_STATUS": k.TASK_SUSPENDED, "NA_MESSAGE": "Host unreachable. Tasks suspended automatically."},
                    where={"FK_HOST": host_id, "NU_STATUS": status},
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
        Resume previously suspended, errored, or stale HOST_TASK entries for a given host.

        Tasks are reactivated under three conditions:
            1. NU_STATUS = TASK_SUSPENDED → host became reachable again.
            2. NU_STATUS = TASK_ERROR     → retry.
            3. NU_STATUS = TASK_RUNNING   → considered stale if DT_HOST_TASK is older
            than (now - busy_timeout_seconds), assuming the worker crashed.

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

            # -----------------------------------------------------------------
            # 1. Reactivate suspended tasks
            # -----------------------------------------------------------------
            resumed_suspended = self._update_row(
                table="HOST_TASK",
                data={
                    "NU_STATUS": k.TASK_PENDING,
                    "NU_TYPE": k.HOST_TASK_PROCESSING_TYPE,
                    "NA_MESSAGE": (
                        "Host reachable again — suspended task resumed automatically"
                    ),
                },
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS": k.TASK_SUSPENDED,
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
                    "NU_TYPE": k.HOST_TASK_PROCESSING_TYPE,
                    "NA_MESSAGE": (
                        "Host reachable again — previously failed task resubmitted"
                    ),
                },
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS": k.TASK_ERROR,
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
                    "NU_TYPE": k.HOST_TASK_PROCESSING_TYPE,
                    "NA_MESSAGE": (
                        f"Detected stale running task (> {busy_timeout_seconds}s) — "
                        f"resubmitted automatically"
                    ),
                },
                where={
                    "FK_HOST": host_id,
                    "NU_STATUS": k.TASK_RUNNING,
                    "DT_HOST_TASK__lt": threshold_time,
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
        extension: Optional[str] = None,
        lock_host: bool = False,
    ) -> Optional[tuple]:
        """
        Return a single FILE_TASK record joined with HOST metadata.

        Optionally performs an atomic HOST lock to prevent multiple workers
        from processing the same host simultaneously.

        Args:
            task_id (Optional[int]):
                Direct lookup by FILE_TASK ID.
            task_status (Optional[int]):
                Task status filter.
            task_type (Optional[int]):
                Task type filter.
            check_host_busy (bool):
                Exclude BUSY hosts if True.
            extension (Optional[str]):
                File extension filter.
            lock_host (bool):
                If True, attempts to atomically lock the host
                (IS_BUSY=1) before returning the task.

        Returns:
            Optional[tuple]:
                (row_dict, host_id, file_task_id) or None.
        """

        # --------------------------------------------------------------
        # 0) Connect
        # --------------------------------------------------------------
        self._connect()

        # --------------------------------------------------------------
        # 1) WHERE clause
        # --------------------------------------------------------------
        where = {}

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

        # --------------------------------------------------------------
        # 2) Select candidate task
        # --------------------------------------------------------------
        rows = self._select_custom(
            table="FILE_TASK FT",
            joins=["JOIN HOST H ON H.ID_HOST = FT.FK_HOST"],
            where=where,
            order_by="FT.ID_FILE_TASK ASC" if not task_id else None,
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

            self.cursor.execute(
                """
                UPDATE HOST
                SET
                    IS_BUSY = 1,
                    DT_BUSY = NOW(),
                    NU_PID = %s
                WHERE
                    ID_HOST = %s
                    AND IS_BUSY = 0
                """,
                (os.getpid(), host_id),
            )

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
        Create or update FILE_TASK entries using FileMetadata objects.

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
        Safe and deterministic FILE_TASK update.

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
            
    
    def file_task_suspend_by_host(self, host_id: int, reason: str = None) -> None:
        """
        Suspend HOST-dependent FILE_TASK entries for a specific host.

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
        Resume previously suspended, errored, or stale FILE_TASK entries for a given host.

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

    def file_history_resume_by_host(
        self,
        host_id: int,
    ) -> None:
        """
        Resume suspended or errored DISCOVERY and BACKUP phases
        for FILE_TASK_HISTORY entries when a host becomes reachable again.

        Processing phase is NOT resumed by design.
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
                    "NU_STATUS_DISCOVERY": k.TASK_ERROR,
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
                    "NU_STATUS_BACKUP": k.TASK_ERROR,
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
        Query FILE_TASK with dynamic filters based on provided keyword arguments.

        Only valid column names defined in `valid_fields` are allowed to form the WHERE clause.
        Each key in kwargs must match a valid field name in the table.

        Args:
            **kwargs: Dynamic filter arguments (e.g., FK_HOST=123, NU_STATUS=1, NA_HOST_FILE_NAME='file.bin').

        Returns:
            list[dict]: Matching rows from FILE_TASK_HISTORY. Returns an empty list if none found.

        Raises:
            ValueError: If any provided keyword does not match a valid field.
            mysql.connector.Error: On query failure.
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
    
    def get_all_filetask_names(self, host_id: int) -> set:
        """
        Return a set containing all filenames already present in FILE_TASK
        for the given host.
        """
        rows = self._select_rows(
            table="FILE_TASK",
            where={"FK_HOST": host_id},
            cols=["NA_HOST_FILE_NAME"]
        )
        return {r["NA_HOST_FILE_NAME"] for r in rows}


    # ======================================================================
    # BACKLOG MANAGEMENT
    # ======================================================================


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
        Update FILE_TASK backlog entries based on a Filter definition.

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


    # ======================================================================
    # BACKLOG MANAGEMENT
    # ======================================================================
    def get_all_filetaskhistory_names(self, host_id: int) -> set:
        """
        Return a set containing all filenames already present in FILE_TASK_HISTORY
        for the given host.
        """
        rows = self._select_rows(
            table="FILE_TASK_HISTORY",
            where={"FK_HOST": host_id},
            cols=["NA_HOST_FILE_NAME"]
        )
        return {r["NA_HOST_FILE_NAME"] for r in rows}

    

    def check_file_history(self, **kwargs) -> list[dict]:
        """
        Query FILE_TASK_HISTORY with dynamic filters based on provided keyword arguments.

        Only valid column names defined in `valid_fields` are allowed to form the WHERE clause.
        Each key in kwargs must match a valid field name in the table.

        Args:
            **kwargs: Dynamic filter arguments (e.g., FK_HOST=123, NU_STATUS=1, NA_HOST_FILE_NAME='file.bin').

        Returns:
            list[dict]: Matching rows from FILE_TASK_HISTORY. Returns an empty list if none found.

        Raises:
            ValueError: If any provided keyword does not match a valid field.
            mysql.connector.Error: On query failure.
        """
        valid_fields = self.VALID_FIELDS_FILE_TASK_HISTORY
        
        self._connect()

        # Validate and build WHERE clause
        where_clause = {}
        for key, value in kwargs.items():
            if key not in valid_fields:
                raise ValueError(f"Invalid field in _check_file_history(): '{key}' is not a valid column.")
            where_clause[key] = value

        # Execute the SELECT query
        rows = self._select_rows(
            table="FILE_TASK_HISTORY",
            where=where_clause,
            order_by="ID_HISTORY DESC",
            cols=[
                "ID_HISTORY",
                "FK_HOST",
                "DT_BACKUP",
                "DT_PROCESSED",
                "NA_HOST_FILE_PATH",
                "NA_HOST_FILE_NAME",
                "VL_FILE_SIZE_KB",
                "NA_SERVER_FILE_PATH",
                "NA_SERVER_FILE_NAME",
                "NA_MESSAGE",
            ],
        )

        return rows or None

        
    def file_history_create(
        self,
        host_id: int,
        task_type: int,
        task_status: int,
        file_metadata: list,
    ) -> int:
        """
        Insert or update FILE_TASK_HISTORY entries using FileMetadata objects.

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
        Safe and deterministic FILE_TASK_HISTORY update.

        Identification strategies (exactly one required):

            1) history_id
            2) (host_id, host_file_path, host_file_name)
            3) (host_id, server_file_name)  -> only when server_file_name IS NOT NULL

        Update semantics:
            - Field omitted      -> not updated
            - Field=None         -> not updated
            - Field=SET_NULL     -> explicitly updated to NULL
            - Field=value        -> updated
        """

        self._connect()
        valid_fields = getattr(self, "VALID_FIELDS_FILE_TASK_HISTORY", set())

        # -------------------------------------------------
        # Automatic timestamps
        # -------------------------------------------------
        if task_type:
            if task_type == k.FILE_TASK_BACKUP_TYPE and not kwargs.get("DT_BACKUP"):
                kwargs["DT_BACKUP"] = datetime.now()

            elif task_type == k.FILE_TASK_PROCESS_TYPE and not kwargs.get("DT_PROCESSED"):
                kwargs["DT_PROCESSED"] = datetime.now()

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
        Deduplicate a batch of FileMetadata objects against FILE_TASK_HISTORY.

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
    
        
    def file_task_insert_batch(
        self,
        rows: list[dict],
        batch_size: int = 4000,
    ) -> int:

        if not rows:
            return 0

        self._connect()
        processed = 0

        try:
            cursor = self.db_connection.cursor()

            columns = list(rows[0].keys())
            cols_sql = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            sql = f"""
                INSERT INTO FILE_TASK ({cols_sql})
                VALUES ({placeholders})
            """

            batch = []

            for row in rows:
                batch.append(tuple(row[col] for col in columns))

                if len(batch) >= batch_size:
                    cursor.executemany(sql, batch)
                    processed += len(batch)
                    batch.clear()

            if batch:
                cursor.executemany(sql, batch)
                processed += len(batch)

            self.db_connection.commit()

            self.log.entry(
                f"[DB] Batch INSERT FILE_TASK | rows={processed}"
            )

            return processed

        except Exception:
            self.db_connection.rollback()
            raise

        finally:
            self._disconnect()
            
    def file_history_insert_batch(
        self,
        rows: list[dict],
        batch_size: int = 4000,
    ) -> int:

        if not rows:
            return 0

        self._connect()
        processed = 0

        try:
            cursor = self.db_connection.cursor()

            columns = list(rows[0].keys())
            cols_sql = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            sql = f"""
                INSERT INTO FILE_TASK_HISTORY ({cols_sql})
                VALUES ({placeholders})
            """

            batch = []

            for row in rows:
                batch.append(tuple(row[col] for col in columns))

                if len(batch) >= batch_size:
                    cursor.executemany(sql, batch)
                    processed += len(batch)
                    batch.clear()

            if batch:
                cursor.executemany(sql, batch)
                processed += len(batch)

            self.db_connection.commit()

            self.log.entry(
                f"[DB] Batch INSERT FILE_TASK_HISTORY | rows={processed}"
            )

            return processed

        except Exception:
            self.db_connection.rollback()
            raise

        finally:
            self._disconnect()
            
    # ======================================================================
    # GARBAGE COLLECTION
    # ======================================================================
    def file_history_get_gc_candidates(self, batch_size: int, quarantine_days: int):
        """
        Retrieve FILE_TASK_HISTORY records eligible for garbage collection.

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

        Returns:
            List[Dict[str, Any]]:
                Rows containing ID_HISTORY, NA_SERVER_FILE_PATH,
                and NA_SERVER_FILE_NAME.
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
        

    def file_history_mark_payload_deleted(self, history_id: int) -> None:
        """
        Mark payload as deleted in FILE_TASK_HISTORY.

        Args:
            history_id (int):
                FILE_TASK_HISTORY primary key.
        """
        self._connect()
        self._update_rows(
            table="FILE_TASK_HISTORY",
            where={"ID_HISTORY": history_id},
            data={
                "IS_PAYLOAD_DELETED": 1,
                "DT_PAYLOAD_DELETED": datetime.now()
            }
        )
    