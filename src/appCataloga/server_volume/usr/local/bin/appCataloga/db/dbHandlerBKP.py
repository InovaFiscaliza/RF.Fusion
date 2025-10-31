
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
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime
import json
import config as k
from .dbHandlerBase import DBHandlerBase
from datetime import datetime as _dt
from shared import Filter


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
        "ID_HOST",
        "NA_HOST_NAME",
        "NA_HOST_ADDRESS",
        "NA_HOST_PORT",
        "NA_HOST_USER",
        "NA_HOST_PASSWORD",
        "IS_OFFLINE",
        "DT_LAST_FAIL",
        "DT_LAST_CHECK",
        "DT_LAST_BACKUP",
        "DT_LAST_PROCESSING",
        "NU_HOST_FILES",
        "NU_HOST_CHECK_ERROR",
    }

    VALID_FIELDS_FILE_TASK = {
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
        "FK_HOST",
        "NU_TYPE",
        "DT_HOST_TASK",
        "NU_STATUS",
        "NU_PID",
        "FILTER",
        "NA_MESSAGE",
    }

    VALID_FIELDS_FILE_HISTORY = {
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


    def host_update(self, host_id: int, reset: bool = False, **kwargs) -> None:
        """
        Safely update one or more fields in the HOST table.

        This method performs dynamic updates to any valid column of the HOST table.
        It supports both **direct assignments** (e.g., setting a boolean or datetime value)
        and **arithmetic adjustments** (increment/decrement of numeric counters),
        according to the data type and the `reset` flag.

        The set of allowed columns is validated against `VALID_FIELDS_HOST`, preventing
        SQL misuse or schema inconsistencies.

        Examples:
            >>> db.host_update(10367, NU_PENDING_BACKUP=+1)
            # Increments NU_PENDING_BACKUP by 1

            >>> db.host_update(10367, IS_OFFLINE=True, NU_FAILS=+1)
            # Marks host as offline and increases failure counter

            >>> db.host_update(10367, reset=True, NU_FAILS=0, IS_OFFLINE=False)
            # Resets failure counter and restores host as online

        Behavior:
            - When `reset=False` (default):
                • Integer or float values are treated as relative deltas:
                positive values increment, negative values decrement.
                • Boolean, string, and datetime values are always assigned directly.
            - When `reset=True`:
                • All provided values are assigned directly (no arithmetic logic).
                • Zero values explicitly overwrite the current database value.
            - Null (`None`) values are ignored.

        Args:
            host_id (int):
                Unique identifier of the host to update (`HOST.ID_HOST`).
            reset (bool, optional):
                If True, overwrites existing field values instead of applying
                arithmetic deltas. Defaults to False.
            **kwargs:
                Arbitrary field-value pairs corresponding to valid columns of the
                `HOST` table. Examples include:
                - NU_PENDING_BACKUP (int)
                - NU_BACKUP_ERROR (int)
                - IS_OFFLINE (bool)
                - DT_LAST_CHECK (datetime)

        Raises:
            ValueError:
                If one or more fields in `kwargs` are not listed in `VALID_FIELDS_HOST`.
            Exception:
                If database execution or transaction commit fails.

        Returns:
            None

        Notes:
            • Updates are executed within a single transaction and committed
            automatically upon success.
            • In case of failure, all changes are rolled back.
            • Parameterized SQL is used for safety; arithmetic operations are
            handled via controlled inline SQL expressions.
        """
        if not kwargs:
            self.log.warning(f"[DBHandlerBKP] No fields provided for host_update (ID={host_id}).")
            return

        # --- Validate provided fields ---
        for key in kwargs.keys():
            if key not in self.VALID_FIELDS_HOST:
                raise ValueError(f"Invalid field '{key}' for HOST table update.")

        arithmetic_updates = []
        direct_updates = {}

        for field, value in kwargs.items():
            if value is None:
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
                elif value == 0 and reset:
                    direct_updates[field] = 0
                continue

            direct_updates[field] = value

        try:
            # 1. Parameterized direct updates
            if direct_updates:
                self._update_row(
                    table="HOST",
                    data=direct_updates,
                    where={"ID_HOST": host_id},
                    commit=False,
                )

            # 2. Arithmetic (increment/decrement) updates
            if arithmetic_updates:
                sql = f"UPDATE HOST SET {', '.join(arithmetic_updates)} WHERE ID_HOST = %s;"
                self.cursor.execute(sql, (host_id,))

            # 3. Commit transaction
            self.db_connection.commit()
            self.log.entry(f"[DBHandlerBKP] HOST {host_id} updated successfully (reset={reset}).")

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBKP] Failed to update HOST {host_id}: {e}")
            raise


    # ======================================================================
    # HOST_TASK OPERATIONS
    # ======================================================================
    def queue_host_task(
        self,
        host_id: int,
        task_type: int,
        filter_dict: dict,
    ) -> dict:
        """Queue a new backup discovery task for a given host.

        If the host does not exist, create it in the HOST table.
        Then enqueue a discovery-type task in HOST_TASK, and
        return the current host status summary.

        Args:
            host_id (int): Host primary key ID.
            host_uid (str): Unique host UID string.
            filter_dict (dict): JSON-style dictionary for filtering.

        Returns:
            dict: Host status dictionary from host_read_status().
        """
        self.host_task_create(
            NU_TYPE=task_type,
            FK_HOST=host_id,
            FILTER=filter_dict,
        )

        host_statistics = self.host_read_status(host_id)
        return host_statistics
    
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



    def host_task_read(
        self,
        task_id: Optional[int] = None,
        task_status: Optional[int] = k.TASK_PENDING,
        task_type: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve one HOST_TASK entry (by ID or first pending one) joined with HOST data.

        This method reads a single HOST_TASK, prioritizing tasks with status `PENDING`.
        It ensures that suspended or errored tasks are not returned, preventing workers
        from processing hosts that are unavailable or already failed.

        Args:
            task_id (Optional[int]):
                Specific HOST_TASK ID to fetch. If provided, overrides other filters.
            task_status (Optional[int], default=k.TASK_PENDING):
                Task status filter (e.g., PENDING). Suspended or errored tasks are ignored.
            task_type (Optional[int], optional):
                Optional filter for task type (`NU_TYPE`), e.g., BACKUP, PROCESSING, etc.

        Returns:
            Optional[Dict[str, Any]]:
                Dictionary containing HOST_TASK fields and joined HOST info.
                Returns `None` if no matching or eligible task is found.

        Raises:
            mysql.connector.Error:
                If query execution or database communication fails.
        """
        self._connect()
        try:
            params = []
            where_clauses = []

            # If a specific task ID is requested, it overrides all filters
            if task_id:
                where_clauses.append("T.ID_HOST_TASK = %s")
                params.append(task_id)
            else:
                # Default: get only pending tasks
                where_clauses.append("T.NU_STATUS = %s")
                params.append(task_status)

                # Exclude suspended and error states
                where_clauses.append("T.NU_STATUS NOT IN (%s, %s)")
                params.extend([k.TASK_SUSPENDED, k.TASK_ERROR])

                # Optional filter by type
                if task_type is not None:
                    where_clauses.append("T.NU_TYPE = %s")
                    params.append(task_type)

            where_clause = " AND ".join(where_clauses)
            order_clause = "ORDER BY T.DT_HOST_TASK ASC LIMIT 1" if not task_id else ""

            sql = f"""
                SELECT
                    T.ID_HOST_TASK AS task_id,
                    T.NU_TYPE AS task_nu_type,
                    T.FILTER AS host_filter,
                    T.NU_STATUS AS nu_status,
                    H.ID_HOST AS host_id,
                    H.NA_HOST_NAME AS host_uid,
                    H.NA_HOST_ADDRESS AS host_addr,
                    H.NA_HOST_PORT AS port,
                    H.NA_HOST_USER AS user,
                    H.NA_HOST_PASSWORD AS password
                FROM HOST_TASK T
                JOIN HOST H ON H.ID_HOST = T.FK_HOST
                WHERE {where_clause}
                {order_clause};
            """

            rows = self._select_custom(sql, tuple(params))
            row = rows[0] if rows else None
            if not row:
                return None

            # Parse FILTER JSON (fault-tolerant)
            try:
                row["host_filter"] = json.loads(row.get("host_filter") or "{}")
            except Exception:
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

    def host_task_resume_by_host(self, host_id: int) -> None:
        """
        Resume previously suspended or errored HOST_TASK entries for a host.

        This method reactivates tasks whose status is TASK_SUSPENDED or TASK_ERROR,
        setting their status back to TASK_PENDING and updating NA_MESSAGE to reflect
        the reason for resumption.

        Args:
            host_id (int): Unique identifier (ID_HOST) of the host whose host-level
                tasks should be reactivated.

        Returns:
            None

        Raises:
            Exception: Propagates any database access or SQL execution errors.
        """
        try:
            total_resumed = 0

            resumed_suspended = self._update_row(
                table="HOST_TASK",
                data={
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": "Host reachable again — suspended task resumed automatically"
                },
                where={"FK_HOST": host_id, "NU_STATUS": k.TASK_SUSPENDED},
                commit=True
            )

            resumed_error = self._update_row(
                table="HOST_TASK",
                data={
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": "Host reachable again — previously failed task resubmitted"
                },
                where={"FK_HOST": host_id, "NU_STATUS": k.TASK_ERROR},
                commit=True
            )

            total_resumed = resumed_suspended + resumed_error
            if total_resumed:
                self.log.entry(f"[DBHandlerBKP] Resumed {total_resumed} HOST_TASK entries for host {host_id}.")
        except Exception as e:
            self.log.error(f"[DBHandlerBKP] Failed to resume HOST_TASK entries for host {host_id}: {e}")
    
    # ======================================================================
    # FILE_TASK OPERATIONS
    # ======================================================================
    def read_file_tasks(
        self,
        host_id: Optional[int] = None,
        *,
        task_type: Optional[int] = None,
        task_status: Optional[Union[int, List[int], Tuple[int, ...]]] = None,
        limit: Optional[int] = None,
        group_by_host: bool = False,
        single_task: bool = False,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Unified FILE_TASK reader with consistent output type (list of dicts).

        Regardless of mode (grouped, single, or host-based),
        this method always returns a *list of FILE_TASK rows*.

        Args:
            host_id (Optional[int]): Target host ID. If None and group_by_host=True,
                the first host with eligible tasks is selected automatically.
            task_type (Optional[int]): NU_TYPE filter.
            task_status (Optional[Union[int, List[int], Tuple[int, ...]]]): NU_STATUS filter.
            limit (Optional[int]): Optional LIMIT clause.
            group_by_host (bool): If True, auto-select the first eligible host.
            single_task (bool): If True, only the oldest matching FILE_TASK is returned.

        Returns:
            Optional[List[Dict[str, Any]]]: Always a list of FILE_TASK rows, even if only one host is matched.
            Returns None if no rows are found.
        """
        self._connect()
        try:
            # 1) Determine host dynamically if needed
            if host_id is None and group_by_host:
                if not task_type or not task_status:
                    raise ValueError("Both task_type and task_status are required when group_by_host=True.")

                status_list = (task_status,) if isinstance(task_status, int) else tuple(task_status)
                placeholders = ", ".join(["%s"] * len(status_list))
                sql_host = (
                    f"SELECT FK_HOST AS host_id "
                    f"FROM FILE_TASK "
                    f"WHERE NU_TYPE=%s AND NU_STATUS IN ({placeholders}) "
                    f"GROUP BY FK_HOST "
                    f"ORDER BY MIN(DT_FILE_TASK) ASC LIMIT 1;"
                )

                rows = self._select_custom(sql_host, (task_type, *status_list))
                if not rows:
                    return None
                host_id = rows[0]["host_id"]

            # 2️) WHERE clause
            where_clause = {}
            if host_id is not None:
                where_clause["FK_HOST"] = int(host_id)
            if task_type is not None:
                where_clause["NU_TYPE"] = int(task_type)
            if task_status is not None:
                if isinstance(task_status, int):
                    where_clause["NU_STATUS"] = int(task_status)
                elif isinstance(task_status, (list, tuple)) and len(task_status) > 0:
                    conditions = " OR ".join([f"NU_STATUS = {int(v)}" for v in task_status])
                    where_clause["#CUSTOM#NU_STATUS"] = f"({conditions})"

            # 3️) Query
            rows = self._select_rows(
                table="FILE_TASK",
                where=where_clause,
                order_by="DT_FILE_TASK ASC",
                limit=limit or (1 if single_task else None),
                cols=[
                    "ID_FILE_TASK",
                    "FK_HOST",
                    "NA_HOST_FILE_PATH",
                    "NA_HOST_FILE_NAME",
                    "NA_EXTENSION",
                    "VL_FILE_SIZE_KB",
                    "DT_FILE_CREATED",
                    "DT_FILE_MODIFIED",
                    "NA_SERVER_FILE_PATH",
                    "NA_SERVER_FILE_NAME",
                    "NU_TYPE",
                    "NU_STATUS",
                    "NU_PID",
                ],
            )

            if not rows:
                return None
            
            if single_task:
                return rows[0]

            return rows

        finally:
            self._disconnect()

    def _route_from_flag(
        self,
        flag: int,
        *,
        search_type: int,
        search_status: Union[int, List[int]],
        new_type: int,
        new_status: int,
    ) -> Tuple[int, int]:
        """Determine the new (NU_TYPE, NU_STATUS) based on the selection flag.

        Map a selection flag to new (NU_TYPE, NU_STATUS).

        Args:
            flag (int): Selection flag. 1 = selected (should be routed to new type),
                0 = non-selected (remains with search type/status).
            search_type (int): Original NU_TYPE of records being searched.
            search_status (Union[int, List[int]]): Allowed status or list of statuses
                that define the current searchable state.
            new_type (int): NU_TYPE to assign when record is routed.
            new_status (int): NU_STATUS to assign when record is routed.

        Returns:
            Tuple[int, int]: (NU_TYPE, NU_STATUS) after routing decision.
        """
        
        if int(flag) == 1:
            return new_type, new_status
        return search_type, search_status[0] if isinstance(search_status, list) else search_status


    def _compose_message(self, task_type: int, task_status: int, path: str, name: str) -> str:
        """Build a normalized `NA_MESSAGE` for FILE_TASK transitions.

        Args:
            task_type (int): Destination `NU_TYPE`.
            task_status (int): Destination `NU_STATUS`.
            path (str): Host-side path.
            name (str): Host-side filename.

        Returns:
            str: Message like "Backup Pending of file /path/file.ext".
        """
        if task_type == k.FILE_TASK_BACKUP_TYPE:
            type_msg = "Backup"
        elif task_type == k.FILE_TASK_DISCOVERY:
            type_msg = "Discovery"
        else:
            type_msg = "Processing"

        status_msg = (
            "Pending"
            if task_status == k.TASK_PENDING
            else "Done"
            if task_status == k.TASK_DONE
            else "Running"
            if task_status == k.TASK_RUNNING
            else "Refresh"
            if task_status == k.TASK_ERROR
            else "Error"
        )
        return f"{type_msg} {status_msg} of file {path}/{name}"

    def _reset_error_tasks(
        self,
        task_ids: List[int],
        *,
        reset_to_status: Optional[int] = None,
        clear_pid: bool = True,
        clear_message: bool = False,
        commit: bool = False,
    ) -> int:
        """Batch-reset error tasks to a safe pending state.

        Also touches `DT_FILE_TASK` with `NOW()` so schedulers see a fresh row.

        Args:
            task_ids (List[int]): IDs to reset.
            reset_to_status (Optional[int]): Target status. Defaults to `k.TASK_PENDING`.
            clear_pid (bool): If True, set `NU_PID = NULL`.
            clear_message (bool): If True, set `NA_MESSAGE = NULL`.
            commit (bool): Commit transaction at the end.

        Returns:
            int: Number of affected rows.

        Raises:
            mysql.connector.Error: On UPDATE failure.
        """
        if not task_ids:
            return 0
        if reset_to_status is None:
            reset_to_status = k.TASK_PENDING

        parts = ["NU_STATUS=%s", "DT_FILE_TASK=NOW()"]
        if clear_pid:
            parts.append("NU_PID=NULL")
        if clear_message:
            parts.append("NA_MESSAGE=NULL")

        sql = f"UPDATE FILE_TASK SET {', '.join(parts)} WHERE ID_FILE_TASK=%s;"
        params = [(reset_to_status, tid) for tid in task_ids]
        return self._execute_many_custom(sql, params, commit=commit)

    def _apply_updates(self, updates: List[Dict[str, Any]]) -> Dict[str, int]:
        """Apply multiple per-row updates to FILE_TASK and collect routing statistics.

        Each row in `updates` must include:
            - ID_FILE_TASK (int)
            - NU_TYPE (int)
            - NU_STATUS (int)
            - NA_MESSAGE (str)

        The method performs batched updates, stamping each modified record with the
        current timestamp in `DT_FILE_TASK`. All changes are committed in a single
        transaction for performance and consistency.

        Args:
            updates (List[Dict[str, Any]]): List of update dictionaries.

        Returns:
            Dict[str, int]: {
                "rows_updated": int,
                "moved_to_backup": int,
                "moved_to_discovery": int
            }

        Raises:
            mysql.connector.Error: On SQL execution or commit failure.
        """
        summary = {"rows_updated": 0, "moved_to_backup": 0, "moved_to_discovery": 0}
        if not updates:
            return summary

        moved_backup = 0
        moved_disc = 0

        try:
            for row in updates:
                ttype = row["NU_TYPE"]
                tstatus = row["NU_STATUS"]

                if ttype == k.FILE_TASK_BACKUP_TYPE:
                    moved_backup += 1
                elif ttype == k.FILE_TASK_DISCOVERY:
                    moved_disc += 1

                # ✅ Usa o novo padrão do _update_row (sem id_field/id_value)
                self._update_row(
                    "FILE_TASK",
                    {
                        "NU_TYPE": ttype,
                        "NU_STATUS": tstatus,
                        "NA_MESSAGE": row.get("NA_MESSAGE", None),
                    },
                    where={"ID_FILE_TASK": row["ID_FILE_TASK"]},
                    commit=False,
                    touch_field="DT_FILE_TASK",
                )

            # ✅ Um único commit para todo o batch (melhor desempenho)
            self.db_connection.commit()

            summary["rows_updated"] = len(updates)
            summary["moved_to_backup"] = moved_backup
            summary["moved_to_discovery"] = moved_disc
            return summary

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[dbHandlerBKP] _apply_updates failed: {e}")
            raise


    # -------------------------- Public APIs --------------------------------
    def file_task_create(
        self,
        host_id: int,
        task_type: int,
        task_status: int,
        files: List[str],
        file_metadata: List[Dict[str, Any]],
    ) -> int:
        """
        Create or update FILE_TASK entries for the given files (transactional upsert).

        For each file:
        - If a FILE_TASK with the same (FK_HOST, NA_HOST_FILE_NAME) exists, it is updated.
        - Otherwise, a new record is inserted.
        All operations occur within a single transaction; failures trigger rollback.

        Args:
            host_id (int): Host identifier.
            task_type (int): Task type (e.g., discovery, backup).
            task_status (int): Task status code.
            files (List[str]): Absolute remote file paths.
            file_metadata (List[Dict[str, Any]]): Metadata list for each file, expected keys:
                - "NA_FILE", "NA_EXTENSION", "VL_FILE_SIZE_KB",
                "DT_FILE_CREATED", "DT_FILE_MODIFIED"

        Returns:
            int: Number of records inserted or updated.
        """
        self._connect()
        processed = 0

        try:
            file_pairs = [(os.path.dirname(f), os.path.basename(f)) for f in files]

            for path, name in file_pairs:
                meta = next((m for m in file_metadata if m.get("NA_FILE") == name), {})
                if not meta:
                    self.log.warning(f"[file_task_create] Missing metadata for {name}")
                    continue

                msg = self._compose_message(
                    task_type=task_type,
                    task_status=task_status,
                    path=path,
                    name=name,
                )

                payload = {
                    "FK_HOST": host_id,
                    "NA_HOST_FILE_PATH": path,
                    "NA_HOST_FILE_NAME": name,
                    "NA_EXTENSION": meta.get("NA_EXTENSION"),
                    "VL_FILE_SIZE_KB": meta.get("VL_FILE_SIZE_KB"),
                    "DT_FILE_CREATED": meta.get("DT_FILE_CREATED"),
                    "DT_FILE_MODIFIED": meta.get("DT_FILE_MODIFIED"),
                    "NU_TYPE": task_type,
                    "NU_STATUS": task_status,
                    "DT_FILE_TASK": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "NA_MESSAGE": msg,
                }

                # --- Tenta atualizar primeiro ---
                try:
                    rows_affected = self._update_row(
                        "FILE_TASK",
                        data={k: v for k, v in payload.items() if k not in ("FK_HOST", "NA_HOST_FILE_NAME")},
                        where={"FK_HOST": host_id, "NA_HOST_FILE_NAME": name},
                        commit=False,
                    )
                except Exception as e:
                    self.log.warning(f"[file_task_create] Update failed for {name}: {e}")
                    rows_affected = 0

                # --- Se não atualizou nenhuma linha, faz INSERT ---
                if not rows_affected:
                    try:
                        self._insert_row("FILE_TASK", payload, commit=False)
                    except Exception as e:
                        self.log.error(f"[file_task_create] Insert failed for {name}: {e}")
                        continue

                processed += 1

            self.db_connection.commit()
            return processed

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[dbHandlerBKP] file_task_create failed: {e}")
            raise

        finally:
            self._disconnect()


    def file_task_update(self, task_id: int, **kwargs) -> None:
        """
        Update a specific FILE_TASK entry in the database.

        Dynamically updates one or more columns in the FILE_TASK table using the
        internal `_update_rows()` helper. Only valid fields (as defined in
        `VALID_FIELDS_FILE_TASK`) are accepted.

        Args:
            task_id (int):
                The ID of the FILE_TASK record to update.
            **kwargs:
                Arbitrary column-value pairs corresponding to FILE_TASK fields.
                Example:
                    >>> db.file_task_update(
                    ...     task_id=123,
                    ...     NU_STATUS=k.TASK_DONE,
                    ...     NA_MESSAGE="Backup completed successfully."
                    ... )

        Raises:
            ValueError:
                If invalid fields are passed.
            Exception:
                If SQL execution or commit fails.

        Returns:
            None
        """
        if not kwargs:
            self.log.warning(f"[DBHandlerBKP] No fields provided for file_task_update (ID={task_id}).")
            return

        # --- Validate fields ---
        valid_fields = getattr(self, "VALID_FIELDS_FILE_TASK", set())
        for key in kwargs.keys():
            if key not in valid_fields:
                raise ValueError(f"Invalid field '{key}' for FILE_TASK table update.")

        try:
            affected = self._update_row(
                table="FILE_TASK",
                data=kwargs,
                where={"ID_FILE_TASK": task_id},
                commit=True,
            )

            if affected:
                self.log.entry(
                    f"[DBHandlerBKP] FILE_TASK {task_id} updated successfully with fields: "
                    f"{', '.join(kwargs.keys())}."
                )
            else:
                self.log.warning(f"[DBHandlerBKP] FILE_TASK {task_id} not found or not updated.")

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBKP] Failed to update FILE_TASK {task_id}: {e}")
            raise

    def file_task_update_many(self, updates: List[Dict[str, Any]]) -> Dict[str, int]:
        """Batch update of FILE_TASK rows in a single transaction.

        This method replaces the legacy `_apply_updates()` by using the
        higher-level `file_task_update()` for each update, while performing
        all changes in a single transaction for efficiency.

        Args:
            updates (List[Dict[str, Any]]): Sequence of dictionaries with keys:
                - ID_FILE_TASK (int)
                - NU_TYPE (int, optional)
                - NU_STATUS (int, optional)
                - NA_MESSAGE (str, optional)
                - (any other column name is accepted by file_task_update)

        Returns:
            Dict[str, int]: {
                "rows_updated": int,
                "moved_to_backup": int,
                "moved_to_discovery": int
            }

        Raises:
            mysql.connector.Error: On UPDATE/COMMIT failure.
        """
        summary = {"rows_updated": 0, "moved_to_backup": 0, "moved_to_discovery": 0}
        if not updates:
            return summary

        try:
            for row in updates:
                self.file_task_update(
                    row["ID_FILE_TASK"],
                    NU_TYPE=row.get("NU_TYPE"),
                    NU_STATUS=row.get("NU_STATUS"),
                    NA_MESSAGE=row.get("NA_MESSAGE"),
                    NA_HOST_FILE_PATH=row.get("NA_HOST_FILE_PATH"),
                    NA_HOST_FILE_NAME=row.get("NA_HOST_FILE_NAME"),
                    NA_SERVER_FILE_PATH=row.get("NA_SERVER_FILE_PATH"),
                    NA_SERVER_FILE_NAME=row.get("NA_SERVER_FILE_NAME"),
                    VL_FILE_SIZE_KB=row.get("VL_FILE_SIZE_KB"),
                    DT_FILE_MODIFIED=row.get("DT_FILE_MODIFIED"),
                )

                # Count routing
                if row.get("NU_TYPE") == k.FILE_TASK_BACKUP_TYPE:
                    summary["moved_to_backup"] += 1
                elif row.get("NU_TYPE") == k.FILE_TASK_DISCOVERY:
                    summary["moved_to_discovery"] += 1

            # Perform one commit for all updates
            self.db_connection.commit()
            summary["rows_updated"] = len(updates)
            return summary

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBKP] file_task_update_many failed: {e}")
            raise


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
        Suspend all FILE_TASK entries for a specific host.

        This method updates all pending (TASK_PENDING) or running (TASK_RUNNING)
        FILE_TASK records related to the specified host, setting their status
        to TASK_SUSPENDED and updating NA_MESSAGE with a contextual description.

        Args:
            host_id (int): Unique identifier (ID_HOST) of the host whose file
                tasks should be suspended.
            reason (str, optional): Optional text message describing the reason
                for suspension. If not provided, a default message is used.

        Returns:
            None

        Raises:
            Exception: Propagates any database access or SQL execution errors.
        """
        try:
            message = reason or "Host unreachable — file task suspended by host_check service"
            affected = 0

            for status in (k.TASK_PENDING, k.TASK_RUNNING):
                affected += self._update_row(
                    table="FILE_TASK",
                    data={"NU_STATUS": k.TASK_SUSPENDED, "NA_MESSAGE": message},
                    where={"FK_HOST": host_id, "NU_STATUS": status},
                    commit=True
                )

            if affected:
                self.log.entry(f"[DBHandlerBKP] Suspended {affected} FILE_TASK entries for host {host_id}.")
        except Exception as e:
            self.log.error(f"[DBHandlerBKP] Failed to suspend FILE_TASK entries for host {host_id}: {e}")

    
    def file_task_resume_by_host(self, host_id: int) -> None:
        """
        Resume previously suspended or errored FILE_TASK entries for a host.

        This method reactivates tasks whose status is TASK_SUSPENDED or TASK_ERROR,
        setting their status back to TASK_PENDING and updating NA_MESSAGE to record
        the reason for reactivation.

        Args:
            host_id (int): Unique identifier (ID_HOST) of the host whose file-level
                tasks should be reactivated.

        Returns:
            None

        Raises:
            Exception: Propagates any database access or SQL execution errors.
        """
        try:
            total_resumed = 0

            resumed_suspended = self._update_row(
                table="FILE_TASK",
                data={
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": "Host reachable again — suspended file task resumed automatically"
                },
                where={"FK_HOST": host_id, "NU_STATUS": k.TASK_SUSPENDED},
                commit=True
            )

            resumed_error = self._update_row(
                table="FILE_TASK",
                data={
                    "NU_STATUS": k.TASK_PENDING,
                    "NA_MESSAGE": "Host reachable again — previously failed file task resubmitted"
                },
                where={"FK_HOST": host_id, "NU_STATUS": k.TASK_ERROR},
                commit=True
            )

            total_resumed = resumed_suspended + resumed_error
            if total_resumed:
                self.log.entry(f"[DBHandlerBKP] Resumed {total_resumed} FILE_TASK entries for host {host_id}.")
        except Exception as e:
            self.log.error(f"[DBHandlerBKP] Failed to resume FILE_TASK entries for host {host_id}: {e}")
            
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
                "VL_FILE_SIZE_KB",
                "NA_SERVER_FILE_PATH",
                "NA_SERVER_FILE_NAME",
                "NA_MESSAGE",
            ],
        )

        return rows or None

    # ======================================================================
    # BACKLOG MANAGEMENT
    # ======================================================================

    def update_backlog_by_filter(
        self,
        host_id: int,
        task_filter: Dict[str, Any],
        limit: Optional[int] = None,
        *,
        search_type: int = k.FILE_TASK_DISCOVERY,
        search_status: List[int] = [k.TASK_DONE, k.TASK_ERROR, k.TASK_SUSPENDED],
        new_type: int = k.BACKUP_QUERY_TAG,
        new_status: int = k.TASK_PENDING,
        candidate_paths: Optional[List[str]] = None,
    ) -> Dict[str, int]:
        """
        Re-routes FILE_TASK entries for a given host based on logical filter evaluation.

        Integrates filtering (Filter.apply) and routing (_route_from_flag) into a single
        efficient pipeline. Updates are applied directly using file_task_update() for
        atomic consistency.

        Args:
            host_id (int): Target host ID.
            task_filter (Dict[str, Any]): Filter configuration (mode, date, etc.).
            limit (Optional[int]): Optional row limit for efficiency.
            search_type (int): NU_TYPE eligible for routing (default: discovery).
            search_status (List[int]): NU_STATUS values considered for routing.
            new_type (int): NU_TYPE assigned when routed.
            new_status (int): NU_STATUS assigned when routed.
            candidate_paths (Optional[List[str]]): Absolute remote paths defining
                the subset of interest (optional).

        Returns:
            Dict[str, int]: Summary of affected records and routing actions:
                {
                    "rows_read": int,
                    "rows_updated": int,
                    "moved_to_backup": int,
                    "moved_to_discovery": int,
                }
        """
        summary = {
            "rows_read": 0,
            "rows_updated": 0,
            "moved_to_backup": 0,
            "moved_to_discovery": 0,
        }

        if isinstance(search_status, int):
            search_status = [search_status]

        self._connect()
        try:
            # 1) Retrieve eligible backlog tasks directly from DB
            tasks = self.read_file_tasks(
                host_id=host_id,
                limit=limit,
                task_type=search_type,
                task_status=search_status,
                group_by_host=True,
            )

            if not tasks:
                self.log.entry("[DBHandlerBKP] No eligible FILE_TASKs found for routing.")
                return summary
            
            summary["rows_read"] = len(tasks)
            # 2) Build filterable inputs (from backlog)
            tuples, metadata = Filter.build_inputs_from_tasks(tasks)

            # 3) Apply filtering logic (optionally restricted by candidate_paths)
            flags = Filter.apply(
                files_tuple_list=tuples,
                file_metadata=metadata,
                filter_cfg=task_filter,
                candidate_paths=candidate_paths,
                log=self.log,
            )

            # 4) Process each task
            for i, task in enumerate(tasks):
                flag = int(flags[i]) if i < len(flags) else 0
                
                routed_type, routed_status = self._route_from_flag(
                    flag,
                    search_type=search_type,
                    search_status=search_status,
                    new_type=new_type,
                    new_status=new_status,
                )

                # Skip if no change
                if (
                    routed_type == task.get("NU_TYPE")
                    and routed_status == task.get("NU_STATUS")
                ):
                    continue

                msg = self._compose_message(
                    routed_type,
                    routed_status,
                    task.get("NA_HOST_FILE_PATH", ""),
                    task.get("NA_HOST_FILE_NAME", ""),
                )

                # Update FILE_TASK record
                self.file_task_update(
                    task["ID_FILE_TASK"],
                    NU_TYPE=routed_type,
                    NU_STATUS=routed_status,
                    NA_MESSAGE=msg,
                )

                summary["rows_updated"] += 1
                if routed_type == k.FILE_TASK_BACKUP_TYPE:
                    summary["moved_to_backup"] += 1
                elif routed_type == k.FILE_TASK_DISCOVERY:
                    summary["moved_to_discovery"] += 1

            self.log.entry(
                f"[DBHandlerBKP] Host {host_id} — Routed {summary['rows_updated']} FILE_TASK(s): "
                f"{summary['moved_to_backup']} → backup, {summary['moved_to_discovery']} → discovery."
            )
            return summary

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBKP] update_backlog_by_filter failed: {e}")
            raise

        finally:
            self._disconnect()


    # ======================================================================
    # BACKLOG MANAGEMENT
    # ======================================================================
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
        valid_fields = self.VALID_FIELDS_FILE_HISTORY

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

        
    def file_history_create(self, task_type: int, **kwargs) -> None:
        """
        Create a new entry in FILE_TASK_HISTORY.

        This method always performs an INSERT operation, creating a new
        record in FILE_TASK_HISTORY based on the provided field values.

        Args:
            task_type (int): Type of task (e.g., FILE_TASK_BACKUP_TYPE or FILE_TASK_PROCESS_TYPE).
            **kwargs: Column values to insert.

        Returns:
            None

        Raises:
            ValueError: If invalid fields are provided in kwargs.
        """
        valid_fields = {
            "FK_HOST", "DT_BACKUP", "DT_PROCESSED",
            "NA_HOST_FILE_PATH", "NA_HOST_FILE_NAME",
            "VL_FILE_SIZE_KB", "NA_SERVER_FILE_PATH",
            "NA_SERVER_FILE_NAME", "NA_MESSAGE"
        }

        # --- Validate provided fields ---
        for key in kwargs.keys():
            if key not in valid_fields:
                raise ValueError(f"Invalid field '{key}' for FILE_TASK_HISTORY.")

        # --- Assign timestamp based on task type ---
        if task_type == k.FILE_TASK_BACKUP_TYPE:
            kwargs["DT_BACKUP"] = datetime.now()
            kwargs["DT_PROCESSED"] = None
        elif task_type == k.FILE_TASK_PROCESS_TYPE:
            kwargs["DT_BACKUP"] = None
            kwargs["DT_PROCESSED"] = datetime.now()

        # --- Insert new record ---
        last_id = self._insert_row(
            table="FILE_TASK_HISTORY",
            data=kwargs,
            ignore=False,
            commit=True,
        )

        self.log.entry(
            f"[DBHandlerBKP] Created new FILE_TASK_HISTORY record (ID={last_id}) "
            f"for file '{kwargs.get('NA_HOST_FILE_NAME')}'."
        )
        
    def file_history_update(
        self,
        task_type: int,
        file_name: Optional[str] = None,
        *,
        task_id: Optional[int] = None,
        host_id: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Update an existing entry in FILE_TASK_HISTORY using NA_HOST_FILE_NAME as the primary key.

        This method updates the specified FILE_TASK_HISTORY record identified by its
        NA_HOST_FILE_NAME (or ID_HISTORY as a fallback) with the provided field values.

        Args:
            task_type (int): The FILE_TASK_* constant indicating task type (e.g., BACKUP or PROCESS).
            file_name (Optional[str]): The name of the host file to match in FILE_TASK_HISTORY.
            task_id (Optional[int]): Fallback unique ID (ID_HISTORY) if file_name is not provided.
            host_id (Optional[int]): Optional host filter for disambiguation (FK_HOST).
            **kwargs: Column values to update.

        Returns:
            Dict[str, Any]: {
                "success": bool,
                "rows_affected": int,
                "updated_fields": Dict[str, Any],
                "where_used": Dict[str, Any]
            }

        Raises:
            ValueError: If no update fields are provided or if invalid fields are detected.
        """

        valid_fields = getattr(self, "VALID_FIELDS_FILE_HISTORY", set())

        # --- Automatic timestamps based on task type ---
        if task_type == k.FILE_TASK_BACKUP_TYPE and not kwargs.get("DT_BACKUP"):
            kwargs["DT_BACKUP"] = datetime.now()
        elif task_type == k.FILE_TASK_PROCESS_TYPE and not kwargs.get("DT_PROCESSED"):
            kwargs["DT_PROCESSED"] = datetime.now()

        # --- Validate fields ---
        if not kwargs:
            raise ValueError("No fields provided for update in FILE_TASK_HISTORY.")
        for key in kwargs.keys():
            if key not in valid_fields:
                raise ValueError(f"Invalid field '{key}' for FILE_TASK_HISTORY.")

        # --- Determine WHERE condition ---
        if file_name:
            where_dict = {"NA_HOST_FILE_NAME": file_name}
            if host_id:
                where_dict["FK_HOST"] = host_id
        elif task_id is not None:
            where_dict = {"ID_HISTORY": task_id}
        else:
            raise ValueError("Either 'file_name' or 'task_id' must be provided for update.")

        # --- Perform update ---
        try:
            affected_rows = self._update_row(
                table="FILE_TASK_HISTORY",
                data=kwargs,
                where=where_dict,
                commit=True,
            )

            if affected_rows:
                self.log.entry(
                    f"[DBHandlerBKP] Updated FILE_TASK_HISTORY ({where_dict}) "
                    f"with fields: {', '.join(kwargs.keys())}."
                )
            else:
                self.log.warning(
                    f"[DBHandlerBKP] No FILE_TASK_HISTORY entry found for {where_dict}. Nothing updated."
                )

            return {
                "success": True,
                "rows_affected": affected_rows,
                "updated_fields": kwargs,
                "where_used": where_dict,
            }

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBKP] Failed to update FILE_TASK_HISTORY ({where_dict}): {e}")
            raise





        
    