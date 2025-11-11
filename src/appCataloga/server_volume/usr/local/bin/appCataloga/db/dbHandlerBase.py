
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dbHandlerBase_refactored_doc_v2.py
----------------------------------

Base MySQL handler for appCataloga system, providing all reusable database
operations for subclasses such as `dbHandlerBKP`.

Implements connection management, CRUD operations, and safe SQL execution
patterns with centralized logging and transaction control.

"""

from typing import Any, Dict, List, Optional, Tuple
import mysql.connector
from mysql.connector import Error
import config as k


class DBHandlerBase:
    """Base class providing MySQL connection management and CRUD utilities.

    This class is inherited by higher-level handlers that manipulate specific
    database domains such as FILE_TASK, HOST, and HOST_TASK.
    """

    # ======================================================================
    # Initialization
    # ======================================================================
    def __init__(self, database: str, log: Any) -> None:
        """Initialize the base handler.

        Args:
            database (str): Logical key for database credentials (config.DB).
            log (Any): Logger instance implementing .entry(), .warning(), .error().
        """
        self.database = database
        self.log = log
        self.db_connection = None
        self.cursor = None

    # ======================================================================
    # Connection Management
    # ======================================================================
    def _get_db_config(self) -> Dict[str, Any]:
        """Retrieve database credentials from `config`.

        Returns:
            Dict[str, Any]: Dictionary containing MySQL connection parameters.
        """
        config = {
            "user": k.DB_USER_NAME,
            "password": k.DB_PASSWORD,
            "host": k.SERVER_NAME,
            "database": self.database,
        }
        
        return config

    def _connect(self) -> None:
        """Establish or reuse a database connection efficiently.

        This method validates whether an existing connection is still active
        and reuses it to minimize connection overhead. It also ensures that
        any pending (unread) results from prior operations are flushed before
        reuse to avoid 'Unread result found' errors.

        Raises:
            Error: If a connection to the database cannot be established.
        """
        try:
            # ==========================================================
            # 1) Reuse existing connection if still alive
            # ==========================================================
            if hasattr(self, "db_connection") and self.db_connection:
                if self.db_connection.is_connected():
                    # Ensure cursor exists and is valid
                    if hasattr(self, "cursor") and self.cursor:
                        try:
                            self.cursor.execute("SELECT 1;")

                            # --------------------------------------------------
                            # Clean any unread results from previous operations
                            # --------------------------------------------------
                            try:
                                while True:
                                    if self.cursor.nextset():
                                        try:
                                            self.cursor.fetchall()
                                        except Exception:
                                            pass
                                    else:
                                        break
                            except Exception:
                                pass

                            return  # Connection and cursor valid and clean

                        except Error:
                            # Cursor invalid → recreate it
                            self.cursor = self.db_connection.cursor()

                            # Clean any potential unread results
                            try:
                                while True:
                                    if self.cursor.nextset():
                                        try:
                                            self.cursor.fetchall()
                                        except Exception:
                                            pass
                                    else:
                                        break
                            except Exception:
                                pass

                            return

                    # Cursor missing → create a new one
                    self.cursor = self.db_connection.cursor()

                    # Clean any potential unread results
                    try:
                        while True:
                            if self.cursor.nextset():
                                try:
                                    self.cursor.fetchall()
                                except Exception:
                                    pass
                            else:
                                break
                    except Exception:
                        pass

                    return

                # ======================================================
                # 2) Try to reconnect if connection dropped
                # ======================================================
                try:
                    self.db_connection.reconnect(attempts=3, delay=2)
                    self.cursor = self.db_connection.cursor()
                    self.log.entry("Database reconnected successfully.")

                    # Clean any residual results just in case
                    try:
                        while True:
                            if self.cursor.nextset():
                                try:
                                    self.cursor.fetchall()
                                except Exception:
                                    pass
                            else:
                                break
                    except Exception:
                        pass

                    return
                except Error:
                    self.log.warning("Database reconnect failed, creating a new session.")

            # ==========================================================
            # 3) Create a new connection if none exists
            # ==========================================================
            cfg = self._get_db_config()
            self.db_connection = mysql.connector.connect(**cfg)
            self.cursor = self.db_connection.cursor()
            self.log.entry("Database connection established successfully.")

            # ==========================================================
            # 4) Final cleanup — consume any unread results
            # ==========================================================
            try:
                while True:
                    if self.cursor.nextset():
                        try:
                            self.cursor.fetchall()
                        except Exception:
                            pass
                    else:
                        break
            except Exception:
                pass

        except Error as e:
            self.log.error(f"Error connecting to database: {e}")
            raise


    def _disconnect(self, force: bool = False) -> None:
        """Safely close the database connection.

        This method closes the connection only when explicitly requested
        or when the current session is no longer valid.

        Args:
            force (bool, optional): If True, forces disconnection regardless
                of connection state. Defaults to False.

        Returns:
            None
        """
        try:
            # Check if the connection object exists
            if hasattr(self, "db_connection") and self.db_connection:
                # Close only if forced or if connection is no longer alive
                if force or not self.db_connection.is_connected():
                    self.db_connection.close()
                    self.db_connection = None
                    self.cursor = None
                    self.log.entry("Database connection closed.")
                else:
                    self.log.entry("Database connection kept alive (reuse enabled).")

        except Exception as e:
            self.log.warning(f"Error while closing database connection: {e}")

    # ======================================================================
    # CRUD Operations
    # ======================================================================
    def _insert_row(
        self,
        table: str,
        data: Dict[str, Any],
        *,
        ignore: bool = False,
        commit: bool = True
    ) -> int:
        """Insert a new record into a table with optional IGNORE behavior.

        This method automatically builds a parameterized INSERT statement using
        the provided dictionary. If `ignore=True`, it will use `INSERT IGNORE`
        to suppress duplicate key errors.

        Args:
            table (str): Target table name.
            data (Dict[str, Any]): Mapping of column names to values.
            ignore (bool, optional): If True, uses `INSERT IGNORE` instead of `INSERT`.
                Defaults to False.
            commit (bool, optional): Whether to commit immediately after insert.
                Defaults to True.

        Returns:
            int: Last inserted row ID, or 0 if unavailable.

        Raises:
            mysql.connector.Error: If SQL execution or commit fails.
        """
        
        # Validate input dictionary
        if not data:
            self.log.warning(f"[DBHandlerBase] Empty data dictionary for table '{table}'. Skipping insert.")
            return 0

        # Compose SQL statement dynamically
        cols = ", ".join(data.keys())
        vals = ", ".join(["%s"] * len(data))
        insert_kw = "INSERT IGNORE" if ignore else "INSERT"
        sql = f"{insert_kw} INTO {table} ({cols}) VALUES ({vals});"

        try:
            # Execute parameterized query safely
            self.cursor.execute(sql, tuple(data.values()))

            if commit:
                self.db_connection.commit()

            last_id = int(self.cursor.lastrowid or 0)
            self.log.entry(f"[DBHandlerBase] {insert_kw} executed successfully on {table} (ID={last_id}).")
            return last_id

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBase] {insert_kw} failed on {table}: {e}")
            raise

    def _update_row(
        self,
        table: str,
        data: Dict[str, Any],
        where: Optional[Dict[str, Any]] = None,
        *,
        commit: bool = True,
        touch_field: Optional[str] = None,
    ) -> int:
        """Update rows in a table with safe, parameterized SQL (supports operators __lt, __gt, __lte, __gte, __like)."""

        if not data and not touch_field:
            self.log.warning(f"[DBHandlerBase] UPDATE skipped: no data for {table}")
            return 0

        # --- Build SET clause ---
        set_parts = [f"{col}=%s" for col in data.keys()]
        params = list(data.values())

        if touch_field:
            set_parts.append(f"{touch_field}=NOW()")

        sql = f"UPDATE {table} SET {', '.join(set_parts)}"

        # --- Build WHERE clause ---
        if where:
            where_parts = []
            for key, value in where.items():
                if "__" in key:
                    col, op = key.split("__", 1)
                    if op == "lt":
                        where_parts.append(f"{col} < %s")
                    elif op == "gt":
                        where_parts.append(f"{col} > %s")
                    elif op == "lte":
                        where_parts.append(f"{col} <= %s")
                    elif op == "gte":
                        where_parts.append(f"{col} >= %s")
                    elif op == "like":
                        where_parts.append(f"{col} LIKE %s")
                    else:
                        raise ValueError(f"Unsupported operator __{op}")
                else:
                    where_parts.append(f"{key}=%s")
                params.append(value)

            sql += " WHERE " + " AND ".join(where_parts)

        sql += ";"

        try:
            self.cursor.execute(sql, tuple(params))
            affected = int(self.cursor.rowcount or 0)
            if commit:
                self.db_connection.commit()

            self.log.entry(f"[DBHandlerBase] UPDATE executed successfully on {table} ({affected} rows affected).")
            return affected

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBase] UPDATE failed on {table}: {e}")
            raise


    def _delete_row(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        *,
        commit: bool = True,
    ) -> int:
        """Delete rows from a table with flexible WHERE filtering.

        This method deletes one or more rows using a parameterized DELETE query.
        The filtering conditions are passed as a dictionary, where each key-value
        pair corresponds to a field and its matching value. If no filter is provided,
        no deletion occurs (to avoid truncating the table accidentally).

        Args:
            table (str): Table name.
            where (Optional[Dict[str, Any]]): Dictionary of field filters.
            commit (bool, optional): Whether to commit after deletion. Defaults to True.

        Returns:
            int: Number of deleted rows (0 if no match or no WHERE provided).

        Raises:
            mysql.connector.Error: On execution or commit failure.
        """
        # Ensure safe deletion (require WHERE clause)
        if not where:
            self.log.warning(f"[DBHandlerBase] DELETE skipped: no WHERE provided for {table}")
            return 0

        # Build SQL and parameterized conditions
        sql = f"DELETE FROM {table} WHERE " + " AND ".join([f"{k}=%s" for k in where])
        params = tuple(where.values())

        try:
            self.cursor.execute(sql, params)
            deleted_count = int(self.cursor.rowcount or 0)

            if commit:
                self.db_connection.commit()

            return deleted_count

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBase] DELETE failed on {table}: {e}")
            raise


    def _select_rows(
        self,
        table: str,
        where: Optional[Dict[str, Any]] = None,
        *,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        cols: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Select rows from a table and return as list of dictionaries.

        Supports both normal parameterized filters (key=value)
        and custom SQL snippets via keys starting with '#CUSTOM#'.
        """
        c = ", ".join(cols) if cols else "*"
        sql = f"SELECT {c} FROM {table}"
        params: List[Any] = []

        if where:
            conditions = []
            for key, value in where.items():
                if key.startswith("#CUSTOM#"):
                    # Inject preformatted SQL fragment (e.g. "(NU_STATUS = 0 OR NU_STATUS = -1)")
                    conditions.append(value)
                else:
                    conditions.append(f"{key}=%s")
                    params.append(value)
            sql += " WHERE " + " AND ".join(conditions)

        if order_by:
            sql += f" ORDER BY {order_by}"

        if limit:
            sql += f" LIMIT {limit}"

        sql += ";"

        try:
            self.cursor.execute(sql, tuple(params))
            rows = self.cursor.fetchall() or []
            if not rows:
                return []

            columns = [col[0] for col in self.cursor.description]
            results = [dict(zip(columns, row)) for row in rows]
            return results

        except Exception as e:
            self.log.error(f"[DBHandlerBase] SELECT failed on {table}: {e}")
            raise



    # ======================================================================
    # Custom Execution Helpers
    # ======================================================================
    def _select_custom(self, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a custom SELECT query and return results as list of dicts.

        Args:
            sql (str): Full SQL SELECT statement (may include JOINs, aliases, etc).
            params (Optional[tuple]): Optional parameter tuple for placeholders.

        Returns:
            List[Dict[str, Any]]: Query results as dictionaries (column_name → value).
        """
        try:
            self.cursor.execute(sql, params or ())
            rows = self.cursor.fetchall() or []
            if not rows:
                return []

            # Convert to list of dicts using cursor.description for column names
            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            self.log.error(f"[DBHandlerBase] SELECT_CUSTOM failed: {e}")
            raise

    def _execute_custom(self, sql: str, params: Tuple[Any, ...] = (), *, commit: bool = True) -> int:
        """Execute an arbitrary SQL command (INSERT, UPDATE, DELETE).

        Args:
            sql (str): SQL command.
            params (Tuple[Any, ...], optional): Query parameters. Defaults to an empty tuple.
            commit (bool, optional): Whether to commit after execution. Defaults to True.

        Returns:
            int: Number of affected rows.

        Raises:
            mysql.connector.Error: On execution or commit failure.
        """
        try:
            self.cursor.execute(sql, params)
            affected = int(self.cursor.rowcount or 0)
            if commit:
                self.db_connection.commit()
            return affected
        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBase] execute_custom failed: {e}")
            raise

    def _execute_many_custom(self, sql: str, values: List[Tuple[Any, ...]], *, commit: bool = True) -> int:
        """Execute batch SQL commands efficiently.

        Args:
            sql (str): SQL command with placeholders.
            values (List[Tuple[Any, ...]]): List of parameter tuples.
            commit (bool, optional): Commit after execution. Defaults to True.

        Returns:
            int: Total number of affected rows.

        Raises:
            mysql.connector.Error: On execution or commit failure.
        """
        try:
            self.cursor.executemany(sql, values)
            affected = int(self.cursor.rowcount or 0)
            if commit:
                self.db_connection.commit()
            return affected
        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBase] executemany failed: {e}")
            raise
