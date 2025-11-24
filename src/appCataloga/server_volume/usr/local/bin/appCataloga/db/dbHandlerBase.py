
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
            "port": k.DB_PORT,
            "database": self.database,
        }
        
        return config

    def _connect(self) -> None:
        """
        Establish or reuse a MySQL/MariaDB connection and cursor.

        This method centralizes all connection handling for the RF.Fusion
        database layer and guarantees the following:

        - Reuse of an existing live connection when possible
        - Automatic reconnection if the connection has dropped
        - Cleanup of any unread result sets (“Unread result found” protection)
        - Creation of a fresh connection if none is valid
        - Enforcement of AUTOCOMMIT=True to ensure that all SELECT operations
        always see the latest committed state of the database, including changes
        applied externally (e.g., via DBeaver/MySQL Workbench)

        Rationale:
            Without autocommit, MariaDB/MySQL may keep the cursor inside an
            implicit transaction, causing SELECT to return stale snapshots and
            ignore updates performed outside the current Python session.

            Explicit commit/rollback logic inside write operations remains intact.
            Autocommit only disables implicit transaction snapshots for reads.

        Raises:
            Error: If the database connection cannot be established or reused.
        """

        try:
            # ==========================================================
            # 1) Reuse an existing connection if still alive
            # ==========================================================
            if hasattr(self, "db_connection") and self.db_connection:
                if self.db_connection.is_connected():

                    # Ensure autocommit is always active (avoids stale snapshots)
                    try:
                        self.db_connection.autocommit = True
                    except Exception:
                        pass

                    # ----------------------------------------------
                    # Validate the existing cursor with 'SELECT 1'
                    # ----------------------------------------------
                    if hasattr(self, "cursor") and self.cursor:
                        try:
                            self.cursor.execute("SELECT 1;")

                            # Consume any pending unread results
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

                            return  # Valid connection and cursor ready

                        except Error:
                            # Existing cursor is invalid → recreate it
                            self.cursor = self.db_connection.cursor()

                            # Cleanup any leftover result sets
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

                    # If no cursor exists, create a new one
                    self.cursor = self.db_connection.cursor()

                    # Cleanup for safety
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

                    return  # Reuse path complete

                # ======================================================
                # 2) Attempt reconnection if connection is down
                # ======================================================
                try:
                    self.db_connection.reconnect(attempts=3, delay=2)

                    # Re-assert autocommit
                    try:
                        self.db_connection.autocommit = True
                    except Exception:
                        pass

                    self.cursor = self.db_connection.cursor()
                    self.log.entry("Database reconnected successfully.")

                    # Cleanup unread results post-reconnect
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
                    self.log.warning(
                        "Database reconnect failed, creating a new session."
                    )

            # ==========================================================
            # 3) Create a brand new connection when none exists
            # ==========================================================
            cfg = self._get_db_config()
            self.db_connection = mysql.connector.connect(**cfg)

            # Always operate in autocommit mode
            self.db_connection.autocommit = True

            self.cursor = self.db_connection.cursor()
            self.log.entry("Database connection established successfully.")

            # Final cleanup for safety
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


    def _disconnect(self, force: bool = False, verbose: bool = False) -> None:
        """
        Safely close the database connection.

        This method closes the connection only when explicitly requested
        or when the current session is no longer valid.

        Args:
            force (bool, optional): If True, forces disconnection regardless
                of connection state. Defaults to False.
            verbose (bool, optional): If True, logs kept-alive connections
                for debugging. Defaults to False.

        Returns:
            None
        """
        try:
            if hasattr(self, "db_connection") and self.db_connection:
                if force or not self.db_connection.is_connected():
                    self.db_connection.close()
                    self.db_connection = None
                    self.cursor = None
                    self.log.entry("Database connection closed.")
                else:
                    # Only log if explicitly requested (e.g., debugging mode)
                    if verbose:
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
        extra_sql: str = "",
        commit: bool = True,
        touch_field: Optional[str] = None,
    ) -> int:
        """
        Generic SQL UPDATE builder with support for:
        __lt, __gt, __lte, __gte, __like, __between, __in, __expr
        
        Extra SQL (ORDER BY, LIMIT...) may be appended via extra_sql.
        """

        if not data and not touch_field:
            return 0

        set_parts = []
        params = []

        # ---------------------------------------------------------
        # SET clause
        # ---------------------------------------------------------
        for col, val in data.items():
            if col.endswith("__expr"):
                real_col = col.replace("__expr", "")
                set_parts.append(f"{real_col}={val}")
            else:
                set_parts.append(f"{col}=%s")
                params.append(val)

        if touch_field:
            set_parts.append(f"{touch_field}=NOW()")

        sql = f"UPDATE {table} SET {', '.join(set_parts)}"

        # ---------------------------------------------------------
        # WHERE clause
        # ---------------------------------------------------------
        if where:
            where_parts = []

            for key, value in where.items():
                if "__" in key:
                    col, op = key.split("__", 1)

                    if op == "lt":
                        where_parts.append(f"{col} < %s")
                        params.append(value)

                    elif op == "gt":
                        where_parts.append(f"{col} > %s")
                        params.append(value)

                    elif op == "lte":
                        where_parts.append(f"{col} <= %s")
                        params.append(value)

                    elif op == "gte":
                        where_parts.append(f"{col} >= %s")
                        params.append(value)

                    elif op == "like":
                        where_parts.append(f"{col} LIKE %s")
                        params.append(value)

                    elif op == "between":
                        if not isinstance(value, (list, tuple)) or len(value) != 2:
                            raise ValueError("BETWEEN operator requires (start, end)")
                        where_parts.append(f"{col} BETWEEN %s AND %s")
                        params.extend([value[0], value[1]])

                    elif op == "in":
                        if not isinstance(value, (list, tuple)):
                            raise ValueError("IN operator requires list/tuple")
                        placeholders = ", ".join(["%s"] * len(value))
                        where_parts.append(f"{col} IN ({placeholders})")
                        params.extend(list(value))

                    else:
                        raise ValueError(f"Unsupported operator '__{op}'")

                else:
                    where_parts.append(f"{key}=%s")
                    params.append(value)

            sql += " WHERE " + " AND ".join(where_parts)

        # Extra SQL segment
        if extra_sql:
            sql += f" {extra_sql}"

        sql += ";"

        # ---------------------------------------------------------
        # Execute SQL
        # ---------------------------------------------------------
        try:
            self.cursor.execute(sql, params)
            affected = int(self.cursor.rowcount or 0)
            if commit:
                self.db_connection.commit()
            return affected

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DB] UPDATE failed: {e}")
            raise


    def _upsert_row(
        self,
        table: str,
        data: Dict[str, Any],
        unique_keys: List[str],
        *,
        commit: bool = True,
        touch_field: Optional[str] = None,
    ) -> int:
        """
        Perform an atomic UPSERT operation (INSERT or UPDATE) using
        'INSERT ... ON DUPLICATE KEY UPDATE' in MariaDB/MySQL.

        Args:
            table (str): Target table name.
            data (Dict[str, Any]): Column-value mapping to insert or update.
            unique_keys (List[str]): List of columns that define the unique constraint.
            commit (bool, optional): Whether to commit immediately. Defaults to True.
            touch_field (str, optional): Optional field to auto-update with NOW() when updated.

        Returns:
            int: Number of affected rows (1 = inserted or updated).

        Raises:
            Exception: If the query fails (rolled back automatically).
        """
        if not data:
            self.log.warning(f"[DBHandlerBase] UPSERT skipped: no data for {table}")
            return 0

        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))

        update_parts = []
        for col in data.keys():
            if col not in unique_keys:
                update_parts.append(f"{col}=VALUES({col})")

        if touch_field:
            update_parts.append(f"{touch_field}=NOW()")

        update_clause = ", ".join(update_parts)

        sql = f"""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause};
        """

        try:
            self.cursor.execute(sql, tuple(data.values()))
            affected = int(self.cursor.rowcount or 0)
            if commit:
                self.db_connection.commit()

            self.log.entry(f"[DBHandlerBase] UPSERT executed successfully on {table} ({affected} row affected).")
            return affected

        except Exception as e:
            self.db_connection.rollback()
            self.log.error(f"[DBHandlerBase] UPSERT failed on {table}: {e}")
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
    
    def _select_raw(self, sql: str, params: tuple = ()):
        """
        Execute a raw SQL SELECT query with parameter binding.

        This method bypasses the automatic SELECT generator used by
        `_select_custom()`, allowing execution of aggregated queries,
        complex JOINs, window functions, GROUP BY, subqueries, and any
        SQL construct not supported by the automatic VALID_FIELDS_* engine.

        Args:
            sql (str):
                Complete SQL query string, including SELECT ... FROM ...
                and any JOIN/WHERE/GROUP BY clauses.

            params (tuple):
                Tuple of parameters for the SQL query (safe binding).
                Defaults to empty tuple.

        Returns:
            List[Dict[str, Any]]:
                A list of dictionaries where each key corresponds to a
                column name returned by the query.

                Example:
                    [
                        {"count": 42, "last_updated": datetime(...)}
                    ]

        Raises:
            Exception:
                Re-raises any SQL execution error after logging the failure.
        """
        try:
            self.cursor.execute(sql, params)
            rows = self.cursor.fetchall() or []

            if not rows:
                return []

            columns = [col[0] for col in self.cursor.description]
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            self.log.error(
                f"[DB][SELECT_RAW] {e}\n"
                f"SQL:\n{sql}\n"
                f"PARAMS: {params}"
            )
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
    def _select_custom(
        self,
        table: str,
        *,
        joins: Optional[List[str]] = None,
        where: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
        order_by: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        PREMIUM SELECT builder supporting:
            - Automatic column expansion using VALID_FIELDS_*
            - JOIN resolution with alias discovery
            - WHERE operators:
                =, IN, >=, <=, >, <, LIKE, BETWEEN, IS NULL, IS NOT NULL
            - Raw SQL overrides with "#CUSTOM#"
            - Safe parameter binding
            - Fully normalized result columns: TABLE__COL

        Returns:
            List[Dict[str, Any]]
        """

        # ------------------------------------------------------------------
        # 1) Parse base table + alias
        # ------------------------------------------------------------------
        base_table, base_alias = table.split()
        tables = {base_alias: base_table}

        # ------------------------------------------------------------------
        # 2) Detect table aliases from JOINs
        # ------------------------------------------------------------------
        if joins:
            for join in joins:
                parts = join.replace(",", " ").split()
                if parts[0].upper() == "JOIN":
                    tbl = parts[1]
                    alias = parts[2]
                    tables[alias] = tbl

        # ------------------------------------------------------------------
        # 3) Build SELECT column list
        # ------------------------------------------------------------------
        select_cols = []
        for alias, tbl in tables.items():
            valid_fields = getattr(self, f"VALID_FIELDS_{tbl}", None)
            if not valid_fields:
                raise ValueError(f"VALID_FIELDS definition missing for table {tbl}")

            for col in valid_fields:
                select_cols.append(f"{alias}.{col} AS {tbl}__{col}")

        select_sql = ",\n    ".join(select_cols)

        # ------------------------------------------------------------------
        # 4) Base SQL
        # ------------------------------------------------------------------
        sql = f"SELECT\n    {select_sql}\nFROM {table}\n"
        if joins:
            sql += "\n".join(joins) + "\n"

        # ------------------------------------------------------------------
        # 5) WHERE clause builder
        # ------------------------------------------------------------------
        params = []
        if where:
            clauses = []

            for key, val in where.items():

                # ----------------------------------------------------------
                # Raw SQL override
                # ----------------------------------------------------------
                if key.startswith("#CUSTOM#"):
                    clauses.append(val)
                    continue

                # ----------------------------------------------------------
                # IN operator
                # ----------------------------------------------------------
                if isinstance(val, tuple) and val[0].upper() == "IN":
                    _, seq = val
                    placeholders = ",".join(["%s"] * len(seq))
                    clauses.append(f"{key} IN ({placeholders})")
                    params.extend(seq)
                    continue

                # ----------------------------------------------------------
                # BETWEEN operator → ("BETWEEN", (low, high))
                # ----------------------------------------------------------
                if isinstance(val, tuple) and val[0].upper() == "BETWEEN":
                    _, (low, high) = val
                    clauses.append(f"{key} BETWEEN %s AND %s")
                    params.extend([low, high])
                    continue

                # ----------------------------------------------------------
                # LIKE operator → ("LIKE", pattern)
                # ----------------------------------------------------------
                if isinstance(val, tuple) and val[0].upper() == "LIKE":
                    _, pattern = val
                    clauses.append(f"{key} LIKE %s")
                    params.append(pattern)
                    continue

                # ----------------------------------------------------------
                # Null tests → "IS_NULL" / "NOT_NULL"
                # ----------------------------------------------------------
                if val == "IS_NULL":
                    clauses.append(f"{key} IS NULL")
                    continue

                if val == "NOT_NULL":
                    clauses.append(f"{key} IS NOT NULL")
                    continue

                # ----------------------------------------------------------
                # Comparison operators → (">", x), ("<=", y), etc
                # ----------------------------------------------------------
                if isinstance(val, tuple) and val[0] in (">", "<", ">=", "<="):
                    op, number = val
                    clauses.append(f"{key} {op} %s")
                    params.append(number)
                    continue

                # ----------------------------------------------------------
                # Default: equality
                # ----------------------------------------------------------
                clauses.append(f"{key} = %s")
                params.append(val)

            sql += "WHERE " + " AND ".join(clauses) + "\n"

        # ------------------------------------------------------------------
        # ORDER BY / LIMIT
        # ------------------------------------------------------------------
        if order_by:
            sql += f"ORDER BY {order_by}\n"
        if limit:
            sql += f"LIMIT {limit}\n"

        # ------------------------------------------------------------------
        # 6) EXECUTE QUERY
        # ------------------------------------------------------------------
        try:
            self.cursor.execute(sql, tuple(params))
            rows = self.cursor.fetchall() or []
            if not rows:
                return []

            columns = [c[0] for c in self.cursor.description]
            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            self.log.error(
                f"[DB][SELECT_CUSTOM] {e}\nSQL:\n{sql}\nPARAMS: {params}"
            )
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
