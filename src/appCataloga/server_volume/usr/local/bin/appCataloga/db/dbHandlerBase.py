
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shared MySQL foundation for the appCataloga database layer.

`DBHandlerBase` owns connection lifecycle, generic CRUD helpers, and a small
set of safe SQL builders reused by the domain handlers. It is intentionally
thin on business rules: subclasses such as `dbHandlerBKP` and `dbHandlerRFM`
define table semantics, while this module provides the reusable execution
machinery.
"""

from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime
import json
import mysql.connector
from mysql.connector import Error
import config as k


class DBHandlerBase:
    """
    Shared MySQL foundation for all appCataloga database handlers.

    Provides:
        - connection lifecycle (connect, reconnect, reuse, disconnect)
        - generic parameterized CRUD helpers (_insert_row, _update_row, etc.)
        - a multi-table JOIN builder (_select_custom) driven by VALID_FIELDS_*
          metadata declared in subclasses
        - the SUMMARY_OUTBOX publisher used by both BKP and RFM subclasses

    Subclasses:
        - `dbHandlerBKP`: owns HOST, HOST_TASK, FILE_TASK, FILE_TASK_HISTORY
        - `dbHandlerRFM`: owns FACT_SPECTRUM, DIM_SPECTRUM_SITE, DIM_SPECTRUM_FILE

    Connection policy:
        Long-lived workers pass `reuse_connection=True` (default) so the same
        session is kept open across thousands of queue iterations. One-shot
        callers (e.g., startup helpers) pass `False` to ensure the connection
        is torn down immediately after use.
    """

    # ======================================================================
    # Initialization
    # ======================================================================
    def __init__(
        self,
        database: str,
        log: Any,
        *,
        reuse_connection: bool = True,
    ) -> None:
        """Initialize the base handler.

        Args:
            database (str): Logical key for database credentials (config.DB).
            log (Any): Logger instance implementing .entry(), .warning(), .error().
            reuse_connection (bool, optional): Keep the session open across
                helper calls on long-lived handlers. One-shot callers can pass
                `False` to make `_disconnect()` tear the session down
                immediately. Defaults to True.
        """
        self.database = database
        self.log = log
        self.reuse_connection = reuse_connection
        self.log_connection_lifecycle = bool(
            getattr(k, "DB_LOG_CONNECTION_LIFECYCLE", False)
        )
        self.db_connection = None
        self.cursor = None

    # ======================================================================
    # Connection Management
    # ======================================================================
    def _log_connection_lifecycle(self, message: str, *, force: bool = False) -> None:
        """Write low-level DB session lifecycle logs only when explicitly enabled."""
        if force or self.log_connection_lifecycle:
            self._log_db_event(
                "db_connection_lifecycle",
                operation="connection_lifecycle",
                detail=message,
            )

    def _log_db_event(self, event: str, **fields: Any) -> None:
        """Emit one structured informational DB event."""
        payload = {
            "db_handler": self.__class__.__name__,
            "database": getattr(self, "database", None),
            **fields,
        }
        if hasattr(self.log, "event"):
            self.log.event(event, **payload)
            return
        self.log.entry(f"{event} {payload}")

    def _log_db_warning(self, event: str, **fields: Any) -> None:
        """Emit one structured non-fatal DB warning."""
        payload = {
            "db_handler": self.__class__.__name__,
            "database": getattr(self, "database", None),
            **fields,
        }
        if hasattr(self.log, "warning_event"):
            self.log.warning_event(event, **payload)
            return
        self.log.warning(f"{event} {payload}")

    def _log_db_error(self, event: str, **fields: Any) -> None:
        """Emit one structured DB failure event."""
        payload = {
            "db_handler": self.__class__.__name__,
            "database": getattr(self, "database", None),
            **fields,
        }
        if hasattr(self.log, "error_event"):
            self.log.error_event(event, **payload)
            return
        self.log.error(f"{event} {payload}")

    def _drain_cursor(self) -> None:
        """
        Discard any pending result sets on the current cursor.

        mysql.connector raises 'Unread result found' if a new query is issued
        while a previous result set is still unconsumed. This can happen after
        a failed mid-batch query, after cursor recreation on reconnect, or
        after the health-probe `SELECT 1` in the reuse path.

        Draining is always safe here because we never rely on leftover results;
        all consumed data is fetched explicitly by the callers above.
        """
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

        Guarantees provided by this method:

        - Reuse of an existing live connection when possible
        - Automatic reconnection if the connection has dropped
        - Cleanup of any unread result sets (“Unread result found” protection)
        - Creation of a fresh connection if none is valid
        - `autocommit=True` for non-transactional callers, unless an explicit
          transaction is already active in the subclass
        """

        try:
            # ==========================================================
            # 1) Reuse an existing connection if still alive
            # ==========================================================
            if hasattr(self, "db_connection") and self.db_connection:
                if self.db_connection.is_connected():

                    # --------------------------------------------------
                    # Enforce autocommit ONLY if not in explicit TX
                    # --------------------------------------------------
                    if not getattr(self, "in_transaction", False):
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

                            # Drain any unconsumed result sets left over from
                            # the SELECT 1 probe or a previous partial query.
                            self._drain_cursor()

                            return  # Valid connection and cursor ready

                        except Error:
                            # Existing cursor is invalid → recreate it
                            self.cursor = self.db_connection.cursor()

                            self._drain_cursor()

                            return

                    # If no cursor exists, create a new one
                    self.cursor = self.db_connection.cursor()
                    self._drain_cursor()

                    return  # Reuse path complete

                # ======================================================
                # 2) Attempt reconnection if connection is down
                # ======================================================
                try:
                    self.db_connection.reconnect(attempts=3, delay=2)

                    # Re-assert autocommit ONLY if not in TX
                    if not getattr(self, "in_transaction", False):
                        try:
                            self.db_connection.autocommit = True
                        except Exception:
                            pass

                    self.cursor = self.db_connection.cursor()
                    self._log_connection_lifecycle(
                        "Database reconnected successfully."
                    )
                    self._drain_cursor()

                    return

                except Error:
                    self._log_db_warning(
                        "db_reconnect_failed",
                        operation="reconnect",
                        error="Database reconnect failed, creating a new session.",
                    )

            # ==========================================================
            # 3) Create a brand new connection when none exists
            # ==========================================================
            cfg = self._get_db_config()
            self.db_connection = mysql.connector.connect(**cfg)

            # Autocommit default ONLY if not in TX
            if not getattr(self, "in_transaction", False):
                try:
                    self.db_connection.autocommit = True
                except Exception:
                    pass

            self.cursor = self.db_connection.cursor()
            self._log_connection_lifecycle(
                "Database connection established successfully."
            )
            self._drain_cursor()

        except Error as e:
            self._log_db_error(
                "db_connect_failed",
                operation="connect",
                error=repr(e),
            )
            raise



    def _disconnect(self, force: bool = False, verbose: bool = False) -> None:
        """
        Close the current database connection.

        Args:
            force (bool, optional): If True, always tears the session down
                regardless of the handler reuse policy. Defaults to False.
            verbose (bool, optional): If True, logs whether the connection was
                closed or intentionally kept alive for reuse. Defaults to False.

        Returns:
            None
        """
        try:
            if hasattr(self, "db_connection") and self.db_connection:
                was_connected = False
                should_close = force or not self.reuse_connection

                try:
                    was_connected = self.db_connection.is_connected()
                except Exception:
                    was_connected = False

                if not should_close and not was_connected:
                    should_close = True

                if should_close:
                    try:
                        self.db_connection.close()
                    finally:
                        self.db_connection = None
                        self.cursor = None

                    if was_connected or force:
                        self._log_connection_lifecycle(
                            "Database connection closed.",
                            force=verbose,
                        )
                    elif verbose:
                        self._log_connection_lifecycle(
                            "Database connection already closed.",
                            force=True,
                        )
                elif verbose:
                    self._log_connection_lifecycle(
                        "Database connection kept alive (reuse enabled).",
                        force=True,
                    )

        except Exception as e:
            self._log_db_warning(
                "db_disconnect_failed",
                operation="disconnect",
                error=repr(e),
            )


    # ======================================================================
    # CRUD Operations
    # ======================================================================
    @staticmethod
    def _normalize_summary_reference_month(value: Any) -> Optional[str]:
        """
        Normalize one month-like value into `YYYY-MM-01`.

        BKP and RFM publish monthly dirty scopes through the same outbox
        contract, so the worker always receives first-of-month buckets.
        """
        if value is None:
            return None

        if isinstance(value, datetime):
            return value.strftime("%Y-%m-01")

        if isinstance(value, date):
            return value.strftime("%Y-%m-01")

        text = str(value).strip()
        if not text:
            return None

        if len(text) >= 10:
            text = text[:10]

        try:
            parsed = datetime.strptime(text, "%Y-%m-%d")
            return parsed.strftime("%Y-%m-01")
        except ValueError:
            pass

        try:
            parsed = datetime.strptime(text, "%Y-%m")
            return parsed.strftime("%Y-%m-01")
        except ValueError:
            return None

    def summary_enqueue_refresh(
        self,
        *,
        host_ids: Optional[List[int]] = None,
        site_ids: Optional[List[int]] = None,
        equipment_ids: Optional[List[int]] = None,
        reference_months: Optional[List[Any]] = None,
        full_reconcile: bool = False,
        reason: Optional[str] = None,
        source_handler: Optional[str] = None,
        commit: Optional[bool] = None,
    ) -> int:
        """
        Append one dirty-scope event into `BPDATA.SUMMARY_OUTBOX`.

        Callers describe which operational or analytical scope changed; the
        summary worker later coalesces many such events into one refresh cycle.
        An event is only written when at least one non-empty scope is provided.

        Args:
            host_ids: IDs of HOST rows whose file counts or errors changed.
            site_ids: IDs of DIM_SPECTRUM_SITE rows affected by new spectra.
            equipment_ids: IDs of DIM_SPECTRUM_EQUIPMENT rows affected.
            reference_months: Month values (datetime, date, or ``"YYYY-MM"``
                string) that may now have stale aggregate statistics. Each is
                normalised to ``YYYY-MM-01`` before storage.
            full_reconcile: When True, the worker re-aggregates everything
                regardless of listed scopes. Use after bulk data imports.
            reason: Free-text label for diagnostics, e.g. ``"host_upsert"``.
            source_handler: Name of the calling handler class, embedded in the
                outbox row for tracing. Defaults to ``type(self).__name__``.
            commit: Override the auto-commit default. ``None`` commits when the
                caller is not inside an explicit ``begin_transaction()`` block.

        Returns:
            int: ``lastrowid`` of the inserted outbox row, or 0 when all
                provided scopes were empty and no event was written.
        """
        normalized_host_ids = sorted(
            {
                int(value)
                for value in (host_ids or [])
                if value is not None
            }
        )
        normalized_site_ids = sorted(
            {
                int(value)
                for value in (site_ids or [])
                if value is not None
            }
        )
        normalized_equipment_ids = sorted(
            {
                int(value)
                for value in (equipment_ids or [])
                if value is not None
            }
        )
        normalized_months = sorted(
            {
                month
                for month in (
                    self._normalize_summary_reference_month(value)
                    for value in (reference_months or [])
                )
                if month is not None
            }
        )

        if (
            not normalized_host_ids
            and not normalized_site_ids
            and not normalized_equipment_ids
            and not normalized_months
            and not full_reconcile
        ):
            return 0

        # The outbox stores invalidation scope, not precomputed summary rows.
        payload = {
            "host_ids": normalized_host_ids,
            "site_ids": normalized_site_ids,
            "equipment_ids": normalized_equipment_ids,
            "reference_months": normalized_months,
            "full_reconcile": bool(full_reconcile),
            "reason": reason,
        }
        row = {
            "NA_EVENT_TYPE": "summary_dirty",
            "NA_SOURCE_HANDLER": (
                source_handler
                or getattr(self, "__class__", type(self)).__name__
            ),
            "JS_PAYLOAD": json.dumps(
                payload,
                ensure_ascii=True,
                sort_keys=True,
            ),
        }

        self._connect()
        try:
            auto_commit = (
                not getattr(self, "in_transaction", False)
                if commit is None
                else bool(commit)
            )
            return self._insert_row(
                table="BPDATA.SUMMARY_OUTBOX",
                data=row,
                commit=auto_commit,
                log_success=False,
            )
        finally:
            self._disconnect()

    # ------------------------------------------------------------------
    # Internal SQL building helpers (shared by CRUD methods below)
    # ------------------------------------------------------------------

    def _build_where_clause(
        self,
        where: Dict[str, Any],
        params: List[Any],
        *,
        allow_custom_fragments: bool = False,
    ) -> str:
        """
        Build a parameterized WHERE clause from a filter dict.

        Single implementation of the ``__``-suffix operator DSL shared by
        ``_update_row`` and ``_select_rows``. Appends bound values to *params*
        in-place so they stay aligned with any SET or column-list parameters
        that precede them in the final statement.

        Supported key suffixes:
            ``__lt``      → ``col < %s``
            ``__gt``      → ``col > %s``
            ``__lte``     → ``col <= %s``
            ``__gte``     → ``col >= %s``
            ``__like``    → ``col LIKE %s``
            ``__between`` → ``col BETWEEN %s AND %s``  (value: ``[low, high]``)
            ``__in``      → ``col IN (%s, …)``         (value: list/tuple)
            no suffix     → ``col = %s``

        Args:
            where: Filter dict following the operator DSL above.
            params: Mutable list extended in-place with bound parameters.
            allow_custom_fragments: When True, keys prefixed with ``#CUSTOM#``
                inject a raw SQL fragment with no parameter binding. Only safe
                for fixed, developer-controlled expressions — never for
                user-supplied strings.

        Returns:
            str: A ``" WHERE …"`` fragment (leading space included), or ``""``
                when *where* is empty.

        Raises:
            ValueError: On unsupported operator suffix or malformed BETWEEN/IN.
        """
        if not where:
            return ""

        parts: List[str] = []
        for key, value in where.items():

            if allow_custom_fragments and key.startswith("#CUSTOM#"):
                parts.append(value)  # raw fragment — no parameter binding
                continue

            if "__" in key:
                col, op = key.split("__", 1)
                if op == "lt":
                    parts.append(f"{col} < %s")
                    params.append(value)
                elif op == "gt":
                    parts.append(f"{col} > %s")
                    params.append(value)
                elif op == "lte":
                    parts.append(f"{col} <= %s")
                    params.append(value)
                elif op == "gte":
                    parts.append(f"{col} >= %s")
                    params.append(value)
                elif op == "like":
                    parts.append(f"{col} LIKE %s")
                    params.append(value)
                elif op == "between":
                    if not isinstance(value, (list, tuple)) or len(value) != 2:
                        raise ValueError("BETWEEN operator requires [low, high]")
                    parts.append(f"{col} BETWEEN %s AND %s")
                    params.extend([value[0], value[1]])
                elif op == "in":
                    if not isinstance(value, (list, tuple)):
                        raise ValueError("IN operator requires list/tuple")
                    placeholders = ", ".join(["%s"] * len(value))
                    parts.append(f"{col} IN ({placeholders})")
                    params.extend(list(value))
                else:
                    raise ValueError(f"Unsupported operator '__{op}'")
            else:
                parts.append(f"{key}=%s")
                params.append(value)

        return " WHERE " + " AND ".join(parts)

    def _map_cursor_rows(self, rows: list) -> List[Dict[str, Any]]:
        """
        Convert fetched cursor result tuples into column-keyed dicts.

        Must be called immediately after ``cursor.execute()`` while
        ``cursor.description`` is still populated.

        Args:
            rows: Raw result set from ``cursor.fetchall()``.

        Returns:
            List[Dict[str, Any]]: One dict per row keyed by column name.
                Empty list when *rows* is empty.
        """
        if not rows:
            return []
        columns = [col[0] for col in self.cursor.description]
        return [dict(zip(columns, row)) for row in rows]

    def _insert_row(
        self,
        table: str,
        data: Dict[str, Any],
        *,
        ignore: bool = False,
        commit: bool = True,
        log_success: bool = True,
    ) -> int:
        """
        Insert one row using a fully parameterized SQL statement.

        Args:
            table (str): Target table name.
            data (Dict[str, Any]): Column-to-value mapping for the new row.
            ignore (bool): If True, uses INSERT IGNORE to suppress duplicate-key
                errors silently. Useful for idempotent inserts of dimension rows.
            commit (bool): Commit immediately after the insert. Callers inside an
                explicit transaction must pass `commit=False`.
            log_success (bool): Emit an entry-level log on success. Disable for
                high-frequency inserts (e.g., spectrum batch) to reduce noise.

        Returns:
            int: `lastrowid` of the inserted row, or 0 when unavailable
                (e.g., INSERT IGNORE on a duplicate).

        Raises:
            Exception: Re-raises any SQL failure after logging and rollback.
        """
        if not data:
            self._log_db_warning(
                "db_invalid_input",
                operation="insert",
                table=table,
                error="Empty data dictionary. Insert skipped.",
            )
            return 0

        cols = ", ".join(data.keys())
        vals = ", ".join(["%s"] * len(data))
        insert_kw = "INSERT IGNORE" if ignore else "INSERT"
        sql = f"{insert_kw} INTO {table} ({cols}) VALUES ({vals});"
        # True when this call owns its own transaction boundary. When False we
        # are inside an explicit begin_transaction() block managed by the caller,
        # so rollback on failure is also the caller's responsibility.
        manage_own_transaction = commit or not getattr(self, "in_transaction", False)

        try:
            # Execute parameterized query safely
            self.cursor.execute(sql, tuple(data.values()))

            if commit:
                self.db_connection.commit()

            last_id = int(self.cursor.lastrowid or 0)
            return last_id

        except Exception as e:
            if manage_own_transaction:
                try:
                    self.db_connection.rollback()
                except Exception:
                    pass
            self._log_db_error(
                "db_insert_failed",
                operation=insert_kw.lower().replace(" ", "_"),
                table=table,
                error=repr(e),
                commit=commit,
            )
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
        Update rows using a dictionary-driven parameterized SQL builder.

        SET clause:
            Plain keys map to parameterized `col=%s`.
            Keys ending in `__expr` inject a trusted raw SQL expression:
            ``{"DT_PROCESSED__expr": "NOW()"}`` → ``SET DT_PROCESSED=NOW()``.
            Use `__expr` only for server-side functions; never for user input.

        WHERE clause operators (key suffixes):
            ``__lt``  ``__gt``  ``__lte``  ``__gte``  ``__like``
            ``__between`` (value must be ``[low, high]``)
            ``__in``  (value must be a list/tuple)
            No suffix → equality (``col=%s``).

        Args:
            extra_sql: Raw SQL appended after WHERE (e.g. ``"LIMIT 1"``). Only
                used in call sites where ambiguity is impossible and ordering
                matters (e.g. atomic claim-one-row patterns).
            touch_field: Column name set to ``NOW()`` on every update, typically
                a last-modified timestamp.

        Returns:
            int: Number of affected rows.

        Raises:
            Exception: Re-raises after logging and unconditional rollback.

        Warning:
            The rollback in the except clause is unconditional. If this method
            is called from within an explicit `begin_transaction()` block and
            raises, the entire outer transaction will be rolled back. Callers
            inside explicit transactions should catch exceptions themselves and
            call `self.rollback()` at the transaction boundary instead.
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
            sql += self._build_where_clause(where, params)

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
            if not getattr(self, "in_transaction", False):
                self.db_connection.rollback()
            self._log_db_error(
                "db_update_failed",
                operation="update",
                table=table,
                error=repr(e),
                commit=commit,
            )
            raise


    def _upsert_row(
        self,
        table: str,
        data: Dict[str, Any],
        unique_keys: List[str],
        *,
        commit: bool = True,
        touch_field: Optional[str] = None,
        log_each: bool = False,
    ) -> int:
        """
        Atomic INSERT … ON DUPLICATE KEY UPDATE (MariaDB/MySQL UPSERT).

        The method builds the UPDATE clause by excluding `unique_keys` columns
        from the SET list — those are the constraint-defining columns whose
        values must not change on conflict.

        Args:
            data: Column-to-value mapping for the row.
            unique_keys: Column name(s) that define the unique/PK constraint.
                Accepts either a single string (``"ID_HOST"``) or a list of
                strings. When passed as a plain string, membership testing uses
                Python's ``in`` operator on the string, which works correctly
                for column names that do not appear as sub-strings of the key
                string — but callers should prefer passing a list to be safe.
            touch_field: Column name touched with ``NOW()`` on every update,
                typically a last-modified timestamp.
            log_each: Emit a debug entry per UPSERT. Leave False on hot paths
                such as host statistics updates to avoid log flooding.

        Returns:
            int: Rows affected (1 = insert, 2 = update, 0 = no-op).

        Raises:
            Exception: Re-raises after logging and unconditional rollback.
        """

        if not data:
            self._log_db_warning(
                "db_invalid_input",
                operation="upsert",
                table=table,
                error="No data provided. Upsert skipped.",
            )
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

            return affected

        except Exception as e:
            self.db_connection.rollback()
            self._log_db_error(
                "db_update_failed",
                operation="upsert",
                table=table,
                error=repr(e),
                commit=commit,
            )
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
            self._log_db_warning(
                "db_invalid_input",
                operation="delete",
                table=table,
                error="No WHERE provided. Delete skipped.",
            )
            return 0

        # `col` is used as the loop variable intentionally to avoid shadowing
        # the `config as k` module import that is in scope at the class level.
        sql = f"DELETE FROM {table} WHERE " + " AND ".join([f"{col}=%s" for col in where])
        params = tuple(where.values())

        try:
            self.cursor.execute(sql, params)
            deleted_count = int(self.cursor.rowcount or 0)

            if commit:
                self.db_connection.commit()

            return deleted_count

        except Exception as e:
            if not getattr(self, "in_transaction", False):
                self.db_connection.rollback()
            self._log_db_error(
                "db_delete_failed",
                operation="delete",
                table=table,
                error=repr(e),
                commit=commit,
            )
            raise
    
    def _select_raw(self, sql: str, params: tuple = ()):
        """
        Execute a raw parameterized SELECT query.

        This escape hatch is used for aggregate queries and shapes that do not
        fit the structured builders, while still preserving parameter binding.

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
            return self._map_cursor_rows(self.cursor.fetchall() or [])

        except Exception as e:
            self._log_db_error(
                "db_select_failed",
                operation="select_raw",
                table="raw_sql",
                error=repr(e),
                sql=sql,
                params=params,
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
        Select rows from a single table and return a list of dicts.

        WHERE clause operators (key suffixes):
            ``__lt``  ``__gt``  ``__lte``  ``__gte``  ``__like``
            ``__between`` (value must be ``[low, high]``)
            ``__in``  (value must be a list/tuple)
            No suffix → equality.

        The ``#CUSTOM#`` key prefix injects a pre-formatted SQL fragment
        directly into the WHERE clause without parameter binding::

            where={"#CUSTOM#status": "(NU_STATUS = 0 OR NU_STATUS = -1)"}

        Use this escape hatch only for fixed expressions that cannot be
        expressed with the operator suffixes above. Never use it with
        user-supplied strings — it bypasses parameter binding entirely.

        Args:
            cols: Explicit column list for SELECT. Supports aliases
                (``"ID_HOST AS host_id"``). Defaults to ``*``.

        Returns:
            List[Dict[str, Any]]: Each row as a column-keyed dict.
                Empty list when no rows match.
        """
        c = ", ".join(cols) if cols else "*"
        sql = f"SELECT {c} FROM {table}"
        params: List[Any] = []

        if where:
            sql += self._build_where_clause(where, params, allow_custom_fragments=True)

        if order_by:
            sql += f" ORDER BY {order_by}"

        if limit:
            sql += f" LIMIT {limit}"

        sql += ";"

        try:
            self.cursor.execute(sql, tuple(params))
            return self._map_cursor_rows(self.cursor.fetchall() or [])

        except Exception as e:
            self._log_db_error(
                "db_select_failed",
                operation="select",
                table=table,
                error=repr(e),
            )
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
        Build a multi-table SELECT driven by ``VALID_FIELDS_*`` class attributes.

        Every table referenced in the query must have a corresponding
        ``VALID_FIELDS_TABLENAME`` set attribute on the handler class. All
        columns from that set are projected; result columns are named
        ``TABLE__column`` to avoid ambiguity across joined tables.

        Args:
            table: Base table as ``"TABLE_NAME ALIAS"`` — e.g.
                ``"FILE_TASK ft"``. The alias is mandatory because WHERE and
                JOIN clauses must qualify column names.
            joins: Additional JOIN strings in plain SQL — e.g.
                ``"JOIN HOST h ON h.ID_HOST = ft.FK_HOST"``. The alias in each
                JOIN is used to look up the corresponding ``VALID_FIELDS_*``
                attribute for automatic column projection.
            where: Filter dict using a **tuple-based** DSL, distinct from the
                ``__suffix`` DSL used by ``_select_rows`` and ``_update_row``::

                    ("IN",      [1, 2, 3])      → col IN (%s, %s, %s)
                    ("BETWEEN", (low, high))     → col BETWEEN %s AND %s
                    ("LIKE",    "pat%")          → col LIKE %s
                    (">=", value)               → col >= %s  (any cmp op)
                    "IS_NULL"                   → col IS NULL
                    "NOT_NULL"                  → col IS NOT NULL
                    plain value                 → col = %s
                    "#CUSTOM#key": "raw_sql"    → injected verbatim

                Column names must include the table alias when the same name
                exists in more than one joined table.

        Returns:
            List[Dict[str, Any]]: Rows with ``TABLE__column``-keyed dicts.
                Empty list when no rows match.

        Raises:
            ValueError: When a ``VALID_FIELDS_TABLE`` attribute is missing for
                any table referenced via the base table or joins.
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
            return self._map_cursor_rows(self.cursor.fetchall() or [])

        except Exception as e:
            self._log_db_error(
                "db_select_failed",
                operation="select_custom",
                table=table,
                error=repr(e),
                sql=sql,
                params=params,
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
            self._log_db_error(
                "db_execute_failed",
                operation="execute_custom",
                table="custom_sql",
                error=repr(e),
                commit=commit,
            )
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
            self._log_db_error(
                "db_executemany_failed",
                operation="execute_many_custom",
                table="custom_sql",
                error=repr(e),
                commit=commit,
                batch_size=len(values),
            )
            raise
        
    def _upsert_batch(
        self,
        *,
        table: str,
        rows: list[dict],
        unique_keys: list[str],
        touch_field: str | None = None,
        batch_size: int = 1000,
        commit: bool = True,
    ) -> int:
        """
        High-throughput batch UPSERT via ``INSERT … ON DUPLICATE KEY UPDATE``.

        Rows are submitted in ``batch_size`` chunks via ``cursor.executemany``,
        which is significantly faster than repeated single-row ``_upsert_row``
        calls for bulk imports or full-table reconciles.

        Args:
            table: Target table name.
            rows: Column-value dicts to upsert. All dicts must share identical
                keys (homogeneous schema); raises ``ValueError`` otherwise.
            unique_keys: Column name(s) defining the unique/PK constraint.
                These columns are excluded from the ``ON DUPLICATE KEY UPDATE``
                clause so their values are never overwritten on conflict.
            touch_field: Column name set to ``NOW()`` on every update, typically
                a last-modified timestamp.
            batch_size: Number of rows per ``executemany`` call. Tune based on
                row width and server memory; default of 1000 is conservative.
            commit: Commit after all batches complete. Pass ``False`` to fold
                this operation into an outer ``begin_transaction()`` block.

        Returns:
            int: Total rows submitted (not necessarily changed — ``executemany``
                does not expose per-row affected counts).

        Raises:
            ValueError: When rows have inconsistent column schemas.
            Exception: Re-raises any SQL failure after rollback.
        """

        if not rows:
            return 0

        self._connect()
        processed = 0

        try:
            # self.cursor is already initialised by _connect() above;
            # no need to open a separate local cursor.

            # ---------------------------------------------------------
            # Validate homogeneous schema
            # ---------------------------------------------------------
            columns = list(rows[0].keys())
            for r in rows:
                if list(r.keys()) != columns:
                    raise ValueError(
                        "[DBHandlerBase] _upsert_batch requires homogeneous rows"
                    )

            cols_sql = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            update_parts = [
                f"{col}=VALUES({col})"
                for col in columns
                if col not in unique_keys
            ]

            if touch_field:
                update_parts.append(f"{touch_field}=NOW()")

            update_sql = ", ".join(update_parts)

            sql = f"""
                INSERT INTO {table} ({cols_sql})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_sql}
            """

            batch: list[tuple] = []

            for row in rows:
                batch.append(tuple(row[col] for col in columns))

                if len(batch) >= batch_size:
                    self.cursor.executemany(sql, batch)
                    processed += len(batch)
                    batch.clear()

            if batch:
                self.cursor.executemany(sql, batch)
                processed += len(batch)

            if commit:
                self.db_connection.commit()

            return processed

        except Exception as e:
            self.db_connection.rollback()
            self._log_db_error(
                "db_executemany_failed",
                operation="upsert_batch",
                table=table,
                error=repr(e),
                commit=commit,
                batch_size=batch_size,
            )
            raise

        finally:
            self._disconnect()
