"""Implement the PostgreSQL foundation for appCataloga domain handlers.

Keep connection ownership, transaction control, SQL composition, and CRUD in
one base. Concrete handlers supply table metadata and domain operations;
workers call those handlers instead of constructing SQL themselves.
"""

from __future__ import annotations

from collections.abc import Generator, Mapping, Sequence, Set
from contextlib import contextmanager, nullcontext
from typing import TYPE_CHECKING

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.pq import TransactionStatus
from psycopg.rows import dict_row

import config as k

if TYPE_CHECKING:
    from shared import logging_utils


DbRow = dict[str, object]


class DbHandler:
    """Provide PostgreSQL persistence for handlers that own a data domain.

    The base manages sessions, transaction boundaries, and parameterized CRUD.
    Subclasses declare VALID_FIELDS_<TABLE> and PRIMARY_KEY_<TABLE>, then expose
    methods named after domain operations. Task transitions, filesystem work,
    and SUMMARY publication do not belong to this class.

    Each instance owns one sequential session in one process. Connections are
    opened on demand and may be retained between operations. Single statements
    normally use autocommit; related writes use a caller-owned transaction.
    Batch execution keeps all chunks in one transaction and also serves UPSERT
    for a single row. Failed SQL and uncertain commits are never replayed.

    Public methods control session lifecycle and transactions. Protected CRUD
    supports subclasses; SQL builders and cursor management are implementation
    details. Logging uses the caller's shared logger without managing its files.

    Attributes:
        schema (str): Explicit, case-sensitive schema for structured CRUD.
        log (logging_utils.log): Existing shared logger supplied by the caller.
        reuse_connection (bool): Whether to retain idle sessions; initially True
            unless overridden by the caller.
        db_connection (psycopg.Connection[DbRow] | None): Owned session, initially
            None. Do not replace it or control its transactions outside this API.
        _conninfo (str): Private connection settings supplied by the caller;
            never logged or persisted by this class.
        _transaction_active (bool): Caller transaction ownership flag, initially
            False; remains True after failure until explicit cleanup.
    """

    # Public session lifecycle and transaction control.

    def __init__(
        self,
        schema: str,
        log: logging_utils.log,
        *,
        conninfo: str,
        reuse_connection: bool = True,
    ) -> None:
        """Configure a handler for one schema without connecting to PostgreSQL.

        Keep the database selected by conninfo separate from the schema used to
        qualify tables. Validate settings before retaining them; opening a session
        and checking database privileges are deferred to connect or the first SQL
        operation. Field and primary-key declarations are supplied by subclasses.

        Args:
            schema (str): Unqualified schema name with exact case.
            log (logging_utils.log): Logger from shared.logging_utils.
            conninfo (str): Libpq connection string with an explicit dbname.
                Authentication may be supplied externally; this class never
                creates credential files or logs the connection string.
            reuse_connection (bool): Retain idle sessions between operations.

        Returns:
            None.

        Raises:
            TypeError: If schema or conninfo is not a string at this API boundary.
            ValueError: If the schema or connection string is invalid or dbname
                is missing. Invalid connection details are not included in errors.
        """
        if not isinstance(schema, str) or not isinstance(conninfo, str):
            raise TypeError("Schema and conninfo must be strings.")
        if not schema:
            raise ValueError("A schema name is required.")
        try:
            settings = conninfo_to_dict(conninfo)
        except psycopg.Error:
            raise ValueError("Invalid PostgreSQL connection settings.") from None
        if not settings.get("dbname"):
            raise ValueError("An explicit PostgreSQL dbname is required.")
        self.schema = schema
        self.log = log
        self.reuse_connection = reuse_connection
        self._conninfo = conninfo
        self.db_connection: psycopg.Connection[DbRow] | None = None
        self._transaction_active = False

    @property
    def in_transaction(self) -> bool:
        """Report whether transaction completion belongs to the caller.

        This flag tracks handler ownership, not the server's current transaction
        status. It becomes true before the first statement starts the transaction
        and remains true after SQL failure so the caller still has to resolve it.

        Args:
            None.

        Returns:
            bool: True from begin_transaction until commit, rollback, or close,
            including failed transactions that still require cleanup.
        """
        return self._transaction_active

    def connect(self) -> psycopg.Connection[DbRow]:
        """Establish the owned session or reuse it without replacing its state.

        Call explicitly to establish database access at startup, or let persistence
        operations connect on demand. New sessions return dictionary rows and use
        autocommit until begin_transaction changes their mode. Reuse checks local
        closed/broken flags without running a health query. A lost session requires
        explicit close; reconnecting here could discard locks or transaction state.

        Args:
            None.

        Returns:
            psycopg.Connection[DbRow]: New or locally open session with dict rows, still owned
            by the handler. Callers must not close it, execute SQL on it, or
            change its transaction state directly. Reuse checks local connection
            state without issuing a health query or reconnecting.

        Raises:
            psycopg.Error: If connection fails or the owned session was lost.
                No automatic reconnection occurs; close explicitly to discard
                lost session state before starting a new unit of work.
        """
        if self.db_connection is not None:
            if self.db_connection.closed or self.db_connection.broken:
                raise psycopg.OperationalError(
                    "Database session lost; explicit close is required."
                )
            return self.db_connection
        try:
            self.db_connection = psycopg.connect(
                self._conninfo,
                autocommit=True,
                row_factory=dict_row,
            )
        except psycopg.Error as error:
            self._log_error("db_connect_failed", "connect", error)
            raise
        return self.db_connection

    def close(self) -> None:
        """Release the owned session and discard work that was not committed.

        Use when a handler is no longer needed or a failed session must be discarded.
        Closing an open transaction abandons its pending writes. Local session and
        ownership state are cleared even if closing raises. Calling close before
        connecting, or more than once, has no effect.

        Args:
            None.

        Returns:
            None. Clears local session and transaction state even on failure.

        Raises:
            psycopg.Error: If releasing the session fails.
        """
        try:
            if self.db_connection is not None:
                self.db_connection.close()
        finally:
            self.db_connection = None
            self._transaction_active = False

    def begin_transaction(self) -> None:
        """Reserve one session for a group of writes completed by the caller.

        Disable autocommit and retain the connection even when connection reuse is
        disabled. The driver sends BEGIN with the first SQL statement, rather than
        here. Subsequent persistence helpers must leave commit and rollback to the
        caller. Nested or unmanaged transactions are rejected.

        Args:
            None.

        Returns:
            None. Keeps the session open regardless of the reuse policy.

        Raises:
            RuntimeError: If a transaction is already owned or the session is busy.
            psycopg.Error: If opening or configuring the connection fails.
        """
        if self.in_transaction:
            raise RuntimeError("Nested explicit transactions are not supported.")
        connection = self.connect()
        if connection.info.transaction_status != TransactionStatus.IDLE:
            raise RuntimeError("The session already has an unmanaged transaction.")
        connection.autocommit = False
        self._transaction_active = True

    def commit(self) -> None:
        """Persist the caller's transaction and return the session to idle use.

        Reject an already aborted transaction instead of treating its rollback as
        a successful commit. After success, restore autocommit and close the session
        when reuse is disabled. On failure, retain ownership state and propagate
        the error; a lost reply does not establish whether PostgreSQL committed.

        Args:
            None.

        Returns:
            None. Restores autocommit and applies the session reuse policy.

        Raises:
            RuntimeError: If no transaction is owned or it is aborted; rollback
                is required instead. State is retained when commit fails.
            psycopg.Error: If the session was lost or commit fails. The caller
                must resolve an uncertain commit before repeating writes.
        """
        if not self.in_transaction:
            raise RuntimeError("No explicit transaction to commit.")
        connection = self.connect()
        if connection.info.transaction_status == TransactionStatus.INERROR:
            raise RuntimeError("An aborted transaction requires rollback.")
        try:
            connection.commit()
        except psycopg.Error as error:
            self._log_error("db_execute_failed", "commit", error)
            raise
        connection.autocommit = True
        self._transaction_active = False
        if not self.reuse_connection:
            self.close()

    def rollback(self) -> None:
        """Discard the caller's pending writes and restore standalone execution.

        Use after a failed operation or when the entire unit of work must be
        abandoned. A successful rollback restores autocommit and applies the reuse
        policy. It does not reopen a lost connection or resolve whether an earlier
        uncertain commit succeeded.

        Args:
            None.

        Returns:
            None. Restores autocommit and applies the session reuse policy.

        Raises:
            RuntimeError: If no explicit transaction is owned.
            psycopg.Error: If rollback fails or the session is lost. Use close
                to discard a lost session; do not assume a previous commit failed.
        """
        if not self.in_transaction:
            raise RuntimeError("No explicit transaction to roll back.")
        connection = self.connect()
        connection.rollback()
        connection.autocommit = True
        self._transaction_active = False
        if not self.reuse_connection:
            self.close()

    # Protected persistence operations for domain subclasses.

    def _select_row(
        self,
        table: str,
        where: Mapping[str, object],
        *,
        columns: Sequence[str] | None = None,
    ) -> DbRow | None:
        """Look up one entity using the handler's declared primary key.

        Require equality values for the complete key so a broad predicate cannot
        silently become a single-row lookup. Return the selected fields directly,
        with None for a missing entity. The handler declaration must match the
        actual PostgreSQL primary key; this method does not inspect constraints.

        Args:
            table (str): Unqualified declared table name.
            where (Mapping[str, object]): Exactly the PRIMARY_KEY_<TABLE> fields
                and their non-null values, without filter operator suffixes.
            columns (Sequence[str] | None): Ordered projection; None selects all
                supported fields in alphabetical order.

        Returns:
            DbRow | None: Matching row with native driver values, or None when
            the key does not exist. The declared key must match the real PK.

        Raises:
            ValueError: On invalid table, where, or projection.
            psycopg.Error: On query or session failure.
        """
        where = self._get_primary_key_filter(table, where)
        rows = self._select_batch(table, where, columns=columns)
        return rows[0] if rows else None

    def _select_batch(
        self,
        table: str,
        where: Mapping[str, object] | None = None,
        *,
        columns: Sequence[str] | None = None,
        order_by: Sequence[tuple[str, str]] = (),
        limit: int | None = None,
        for_update: bool = False,
    ) -> list[DbRow]:
        """Read a filtered set of rows using declared table and column names.

        Use for searches and lists within one table. Combine filters with AND and
        apply explicit ordering and an optional result limit. Without order_by,
        result order is unspecified. The whole result is collected into memory;
        this method does not stream rows or split the read into execution chunks.
        Use _select_raw for JOINs, aggregates, or other explicit SQL shapes.

        Args:
            table (str): Unqualified declared table name.
            where (Mapping[str, object] | None): AND filters documented by
                self._build_where_clause; None reads without filters.
            columns (Sequence[str] | None): Ordered projection; None uses all
                declared fields sorted by name, without relying on set order.
            order_by (Sequence[tuple[str, str]]): Column/direction pairs; ASC or
                DESC may include NULLS FIRST or NULLS LAST.
            limit (int | None): Nonnegative maximum rows; zero returns no rows.
            for_update (bool): Lock selected rows until the explicit caller
                transaction ends. False performs an ordinary read.

        Returns:
            list[DbRow]: Matching rows with exact column names and native values;
            an empty list when no rows match.

        Raises:
            ValueError: On invalid names, filters, ordering, or limit.
            RuntimeError: If for_update is requested without an explicit transaction.
            psycopg.Error: On query or session failure.
        """
        if for_update and not self.in_transaction:
            raise RuntimeError("FOR UPDATE requires an explicit transaction.")
        target, fields = self._get_table_definition(table)
        columns = sorted(fields) if columns is None else columns
        self._validate_fields(columns, fields)
        projection = sql.SQL(", ").join(sql.Identifier(name) for name in columns)
        filters, params = self._build_where_clause(
            {} if where is None else where, fields
        )
        query = sql.SQL("SELECT {} FROM {}").format(projection, target) + filters
        query += self._build_order_by_clause(order_by, fields)
        if limit is not None:
            if limit < 0:
                raise ValueError("Limit must be a nonnegative integer.")
            query += sql.SQL(" LIMIT %s")
            params += (limit,)
        if for_update:
            query += sql.SQL(" FOR UPDATE")
        return self._select_raw(query, params)

    def _insert_row(
        self,
        table: str,
        data: Mapping[str, object],
        *,
        skip_duplicates: bool = False,
        returning: Sequence[str] = (),
    ) -> DbRow | None:
        """Persist one supplied record and optionally fetch its stored fields.

        Bind values separately from column names and let PostgreSQL apply defaults
        for omitted fields. Use returning to obtain generated IDs or other stored
        values in the same statement. skip_duplicates suppresses only uniqueness
        conflicts; it does not hide invalid data. An existing caller transaction
        remains open until the caller commits or rolls back.

        Args:
            table (str): Unqualified declared table name.
            data (Mapping[str, object]): Nonempty column/value mapping.
            skip_duplicates (bool): Use ON CONFLICT DO NOTHING for uniqueness conflicts;
                other constraint violations still raise.
            returning (Sequence[str]): Ordered fields for RETURNING; empty
                disables fetching. Never infer an ID from a sequence counter.

        Returns:
            DbRow | None: Requested fields for the inserted row, or None when
            no RETURNING was requested or a conflict skipped the insert.

        Raises:
            ValueError: On an undeclared table, invalid fields, or empty data.
            psycopg.Error: On insert or session failure.
        """
        target, fields = self._get_table_definition(table)
        columns = tuple(data)
        self._validate_fields(columns, fields)
        query = self._build_insert_query(target, columns)
        if skip_duplicates:
            query += sql.SQL(" ON CONFLICT DO NOTHING")
        if returning:
            self._validate_fields(returning, fields)
            query += sql.SQL(" RETURNING ") + sql.SQL(", ").join(
                sql.Identifier(name) for name in returning
            )
        with self._cursor("insert") as cursor:
            cursor.execute(query, tuple(data[name] for name in columns))
            return cursor.fetchone() if returning else None

    def _insert_batch(
        self,
        table: str,
        data: Sequence[Mapping[str, object]],
        *,
        skip_duplicates: bool = False,
        batch_size: int = k.DB_BATCH_SIZE,
    ) -> int:
        """Persist homogeneous records as one atomic operation across all chunks.

        Use when records share a column set, even if their mapping order differs.
        Align values to the first record before sending any writes. batch_size
        bounds each executemany call; it does not introduce intermediate commits.
        When the caller owns no transaction, the executor creates one for all chunks.
        Otherwise, the caller remains responsible for commit or rollback.

        Args:
            table (str): Unqualified declared table name.
            data (Sequence[Mapping[str, object]]): Nonempty column/value mappings
                with identical column sets; mapping insertion order may differ.
            skip_duplicates (bool): Skip uniqueness conflicts with ON CONFLICT DO NOTHING.
            batch_size (int): Positive parameter-row count per executemany call.

        Returns:
            int: Inserted row count, excluding skipped conflicts; zero for empty
            input. Does not infer or return generated IDs from sequences.

        Raises:
            ValueError: On an undeclared table, invalid columns, heterogeneous
                data, or chunk size.
            psycopg.Error: On SQL, session, or self-owned commit failure.
        """
        if not data:
            return 0
        target, fields = self._get_table_definition(table)
        columns = tuple(data[0])
        self._validate_fields(columns, fields)
        query = self._build_insert_query(target, columns)
        if skip_duplicates:
            query += sql.SQL(" ON CONFLICT DO NOTHING")
        return self._execute_batch(
            query,
            self._prepare_batch_values(data),
            batch_size=batch_size,
        )

    def _update_row(
        self,
        table: str,
        data: Mapping[str, object],
        where: Mapping[str, object],
        *,
        touch_field: str | None = None,
    ) -> int:
        """Apply replacement values to the entity identified by its primary key.

        Require the complete declared key in where and reuse the set-update path
        with that equality filter. Missing entities return zero; unchanged values
        still count as a matched row. Use touch_field for an explicit server-side
        timestamp update. This method never completes a caller-owned transaction.

        Args:
            table (str): Unqualified declared table name.
            data (Mapping[str, object]): Replacement fields and values; may be
                empty only when touch_field is supplied.
            where (Mapping[str, object]): Exactly the PRIMARY_KEY_<TABLE> fields
                and their non-null values. The declaration must match the real PK.
            touch_field (str | None): Timestamp field set to CURRENT_TIMESTAMP.

        Returns:
            int: One for a matched row, including unchanged values, or zero
            when the primary key does not exist.

        Raises:
            ValueError: On invalid table, where, fields, or assignments.
            psycopg.Error: On update or session failure.
        """
        where = self._get_primary_key_filter(table, where)
        return self._update_batch(table, data, where, touch_field=touch_field)

    def _update_batch(
        self,
        table: str,
        data: Mapping[str, object],
        where: Mapping[str, object],
        *,
        touch_field: str | None = None,
    ) -> int:
        """Apply one set of replacement values to every row matching a predicate.

        This is a set-based UPDATE, not a list of different updates. Bind data as
        literal replacement values and optionally set touch_field to the transaction
        timestamp. A nonempty filter is required, but the caller must still choose
        its selectivity. All matched rows belong to one SQL statement and share
        the caller's transaction when one is active.

        Args:
            table (str): Unqualified declared table name.
            data (Mapping[str, object]): Columns and replacement values; may be
                empty only when touch_field is supplied. SQL expressions rejected.
            where (Mapping[str, object]): Required, nonempty AND filters using
                the shared suffix syntax; matching all rows is caller-owned.
            touch_field (str | None): Declared column set to CURRENT_TIMESTAMP;
                must not also appear in data.

        Returns:
            int: Rows matched by PostgreSQL, including unchanged values.

        Raises:
            ValueError: On an undeclared table, missing filters, or invalid fields.
            psycopg.Error: On update or session failure.
        """
        target, fields = self._get_table_definition(table)
        if not where or (not data and touch_field is None):
            raise ValueError("Update requires filters and at least one assignment.")
        if data:
            self._validate_fields(tuple(data), fields)
        assignments = [sql.SQL("{} = %s").format(sql.Identifier(name)) for name in data]
        if touch_field is not None:
            self._validate_fields((touch_field,), fields)
            if touch_field in data:
                raise ValueError("Touch column must not also appear in data.")
            assignments.append(
                sql.SQL("{} = CURRENT_TIMESTAMP").format(sql.Identifier(touch_field))
            )
        filters, params = self._build_where_clause(where, fields)
        query = (
            sql.SQL("UPDATE {} SET {}").format(target, sql.SQL(", ").join(assignments))
            + filters
        )
        return self._execute(query, tuple(data.values()) + params)

    def _delete_row(
        self,
        table: str,
        where: Mapping[str, object],
    ) -> int:
        """Remove the entity identified by the complete declared primary key.

        Reject missing key fields, extra predicates, and null key values before
        executing SQL. Delegate the actual DELETE using equality on that key, so
        a nonexistent entity is a normal zero-row result. The database's real
        primary-key constraint provides the single-row guarantee.

        Args:
            table (str): Unqualified declared table name.
            where (Mapping[str, object]): Exactly the PRIMARY_KEY_<TABLE> fields
                and their non-null values. The declaration must match the real PK.

        Returns:
            int: One for the deleted row, or zero when its key does not exist.

        Raises:
            ValueError: On invalid table or where.
            psycopg.Error: On delete or session failure.
        """
        where = self._get_primary_key_filter(table, where)
        return self._delete_batch(table, where)

    def _delete_batch(
        self,
        table: str,
        where: Mapping[str, object],
    ) -> int:
        """Remove all rows matching an explicit set of supported filters.

        Use for deliberate set deletion rather than primary-key entity removal.
        An empty filter is rejected, but nonempty filters can still match every row;
        the caller owns that selection. PostgreSQL executes one DELETE statement,
        with no per-row commits or implicit completion of an outer transaction.

        Args:
            table (str): Unqualified declared table name.
            where (Mapping[str, object]): Required nonempty AND filters; the
                caller remains responsible for their selectivity.

        Returns:
            int: Number of deleted rows; zero if no rows match.

        Raises:
            ValueError: On an undeclared table or missing/invalid filters.
            psycopg.Error: On delete or session failure.
        """
        if not where:
            raise ValueError("Delete requires explicit filters.")
        target, fields = self._get_table_definition(table)
        filters, params = self._build_where_clause(where, fields)
        return self._execute(sql.SQL("DELETE FROM {}").format(target) + filters, params)

    def _upsert_row(
        self,
        table: str,
        data: Mapping[str, object],
        *,
        conflict_columns: Sequence[str],
        touch_field: str | None = None,
    ) -> int:
        """Insert one record or update the record matching its conflict columns.

        Use when an entity may already exist and its unique identity is known.
        Non-key supplied fields are replaced on conflict; omitted fields are left
        unchanged in that update. Delegate one record to _upsert_batch so conflict
        rules and transaction handling stay identical to the multi-record path.
        A key-only conflict performs no update unless touch_field is requested.

        Args:
            table (str): Unqualified declared table name.
            data (Mapping[str, object]): Nonempty inserted column/value mapping.
            conflict_columns (Sequence[str]): Explicit inserted conflict columns backed
                by a unique constraint or inferable unique index.
            touch_field (str | None): Non-key timestamp column updated on conflict.

        Returns:
            int: One row inserted/updated, or zero when an existing key-only
            record is skipped by DO NOTHING.
            Does not use MariaDB's two-affected-rows convention.

        Raises:
            ValueError: On an undeclared table, invalid data, or keys.
            psycopg.Error: On conflict-target, constraint, SQL, or session failure.
        """
        return self._upsert_batch(
            table=table,
            data=[data],
            conflict_columns=conflict_columns,
            touch_field=touch_field,
        )

    def _upsert_batch(
        self,
        table: str,
        data: Sequence[Mapping[str, object]],
        *,
        conflict_columns: Sequence[str],
        touch_field: str | None = None,
        batch_size: int = k.DB_BATCH_SIZE,
    ) -> int:
        """Insert records and resolve declared uniqueness conflicts in one unit.

        Build ON CONFLICT from conflict_columns, which must be inserted columns
        backed by an actual PostgreSQL unique key or inferable index. On conflict,
        replace supplied non-key fields and optionally update touch_field. If no
        update assignments exist, use DO NOTHING. Repeated identities in the input
        are processed in input order. All execution chunks share one transaction;
        failures do not produce a partial success count.

        Args:
            table (str): Unqualified declared table name.
            data (Sequence[Mapping[str, object]]): Rows with identical column
                sets. Mapping insertion order may differ; values are aligned
                using the first row's explicit column order.
            conflict_columns (Sequence[str]): Nonempty ordered columns backed by
                a unique constraint or inferable index; all must appear in data.
            touch_field (str | None): Non-key timestamp column updated on conflict.
            batch_size (int): Positive executemany chunk size.

        Returns:
            int: Total rows affected, excluding conflicts skipped by DO NOTHING;
            zero for empty input. All chunks share one transaction.

        Raises:
            ValueError: On an undeclared table, heterogeneous/invalid data, keys,
                or chunk size.
            psycopg.Error: On SQL, session, or self-owned commit failure.
        """
        if not data:
            return 0
        target, fields = self._get_table_definition(table)
        columns = tuple(data[0])
        self._validate_fields(columns, fields)
        query = self._build_insert_query(target, columns)
        self._validate_fields(conflict_columns, fields)
        if not set(conflict_columns).issubset(columns):
            raise ValueError("Conflict keys must be included in inserted columns.")
        updates = [
            sql.SQL("{} = EXCLUDED.{}").format(
                sql.Identifier(name), sql.Identifier(name)
            )
            for name in columns
            if name not in conflict_columns and name != touch_field
        ]
        if touch_field is not None:
            self._validate_fields((touch_field,), fields)
            if touch_field in conflict_columns:
                raise ValueError("Conflict keys cannot be touched.")
            updates.append(
                sql.SQL("{} = CURRENT_TIMESTAMP").format(sql.Identifier(touch_field))
            )
        query += sql.SQL(" ON CONFLICT ({})").format(
            sql.SQL(", ").join(sql.Identifier(name) for name in conflict_columns)
        )
        if updates:
            query += sql.SQL(" DO UPDATE SET ") + sql.SQL(", ").join(updates)
        else:
            query += sql.SQL(" DO NOTHING")
        values = self._prepare_batch_values(data)
        return self._execute_batch(query, values, batch_size=batch_size)

    def _select_raw(
        self,
        query: sql.Composable,
        params: Sequence[object] = (),
    ) -> list[DbRow]:
        """Read a developer-owned query that does not fit single-table selection.

        Use from domain handlers for JOINs, aggregates, CTEs, or explicit projections.
        The handler is responsible for composing identifiers and choosing aliases;
        this path does not apply VALID_FIELDS validation. Bind external values
        through params. Fetch all rows and close the cursor without changing the
        caller's transaction ownership.

        Args:
            query (sql.Composable): Developer-owned SELECT, including explicit
                JOINs or aggregates. Never compose SQL from untrusted strings.
            params (Sequence[object]): Values bound to placeholders, in order.

        Returns:
            list[DbRow]: Rows keyed by exact column names or aliases; empty
            when no rows match. Values retain the driver's Python types.

        Raises:
            psycopg.Error: On execution or session failure.
        """
        with self._cursor("select") as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()

    def _execute(
        self,
        query: sql.Composable,
        params: Sequence[object] = (),
    ) -> int:
        """Run one developer-owned statement and report PostgreSQL's row count.

        Provide the execution path for structured writes and domain-specific SQL.
        Identifier validation belongs to its builder or handler; values remain bound
        through params. Outside an explicit transaction, the statement uses the
        session's autocommit mode. Inside one, both success and failure leave
        transaction completion to the caller.

        Args:
            query (sql.Composable): Developer-owned statement. Do not send
                BEGIN/COMMIT/ROLLBACK or change session transaction settings here.
            params (Sequence[object]): Placeholder values in order.

        Returns:
            int: Driver rowcount, or -1 if the statement has no row count.

        Raises:
            psycopg.Error: On execution or session failure.
        """
        with self._cursor("execute") as cursor:
            cursor.execute(query, params)
            return cursor.rowcount

    def _execute_batch(
        self,
        query: sql.Composable,
        values: Sequence[Sequence[object]],
        *,
        batch_size: int = k.DB_BATCH_SIZE,
    ) -> int:
        """Run parameter sets in bounded chunks with one transaction boundary.

        Create one transaction around all chunks when the caller owns none, so a
        later failure rolls back earlier chunks. If a caller transaction is active,
        use it directly without a separate commit, rollback, or savepoint. A failure
        then propagates for the caller to resolve. batch_size controls transmission
        chunks, not durability boundaries. Empty input performs no SQL.

        Args:
            query (sql.Composable): Trusted statement with ordered placeholders.
            values (Sequence[Sequence[object]]): One parameter sequence per row.
            batch_size (int): Positive number of parameter rows per executemany.

        Returns:
            int: Sum of affected rows; zero for an empty batch. A failed batch
            returns no count and rolls back all batches only when self-owned.

        Raises:
            ValueError: On invalid batch size.
            psycopg.Error: On execution, session, or self-owned commit failure.
        """
        if batch_size < 1:
            raise ValueError("Batch size must be a positive integer.")
        if not values:
            return 0
        with self._cursor("executemany") as cursor:
            transaction = (
                nullcontext()
                if self.in_transaction
                else cursor.connection.transaction()
            )
            with transaction:
                affected = 0
                for start in range(0, len(values), batch_size):
                    cursor.executemany(query, values[start : start + batch_size])
                    affected += cursor.rowcount
                return affected

    # Internal SQL construction and validation.

    def _get_primary_key_filter(
        self,
        table: str,
        key: Mapping[str, object],
    ) -> dict[str, object]:
        """Turn an exact entity identity into ordered equality predicates.

        Read PRIMARY_KEY_<TABLE> from the handler, validate its supported fields,
        and require precisely those non-null values from the caller. Validate key
        names as literal columns before the batch path interprets filter operators;
        a declaration such as ID__gt must not turn an entity lookup into a range.
        The declaration is trusted metadata; the database catalog is not queried.

        Args:
            table (str): Table with VALID_FIELDS and PRIMARY_KEY declarations.
            key (Mapping[str, object]): Exactly the primary-key fields and
                non-null values; no extra fields or operator suffixes.

        Returns:
            dict[str, object]: Equality filters in declared primary-key order.

        Raises:
            ValueError: On absent/invalid key declaration, missing or extra fields,
                or null key values. The database PK is not discovered here.
        """
        _, fields = self._get_table_definition(table)
        primary_key: tuple[str, ...] = getattr(self, f"PRIMARY_KEY_{table}", ())
        if not primary_key:
            raise ValueError("Row operations require a PRIMARY_KEY declaration.")
        self._validate_fields(primary_key, fields)
        if set(key) != set(primary_key) or any(value is None for value in key.values()):
            raise ValueError(
                "Row operations require the complete non-null primary key."
            )
        return {name: key[name] for name in primary_key}

    def _prepare_batch_values(
        self,
        rows: Sequence[Mapping[str, object]],
    ) -> list[tuple[object, ...]]:
        """Align each record to the first record's INSERT placeholder order.

        Check every record's column set before execution so differing dictionary
        order cannot associate a value with the wrong column. Values are preserved
        without conversion. Callers handle empty input before invoking this helper;
        column authorization is performed when the shared statement is built.

        Args:
            rows (Sequence[Mapping[str, object]]): Nonempty row sequence with
                identical column sets and potentially different mapping order.

        Returns:
            list[tuple[object, ...]]: Parameter tuples aligned to the first row.

        Raises:
            ValueError: If any row has a different column set.
        """
        columns = tuple(rows[0])
        column_set = set(columns)
        if any(set(row) != column_set for row in rows):
            raise ValueError("Batch rows must have identical column sets.")
        return [tuple(row[name] for name in columns) for row in rows]

    def _get_table_definition(self, table: str) -> tuple[sql.Identifier, Set[str]]:
        """Resolve table metadata declared by the domain handler.

        Require a nonempty VALID_FIELDS_<TABLE> declaration, then qualify the table
        with this handler's schema. Names are quoted as literal identifiers by
        Psycopg. This reads Python metadata only: it does not discover tables or
        verify that the live database schema matches the declaration.

        Args:
            table (str): Table name declared by the handler.

        Returns:
            tuple[sql.Identifier, Set[str]]: Qualified table and allowed fields.

        Raises:
            ValueError: If no fields are declared for the table.
        """
        fields: Set[str] | None = getattr(self, f"VALID_FIELDS_{table}", None)
        if not fields:
            raise ValueError("Table has no declared fields.")
        return sql.Identifier(self.schema, table), fields

    def _validate_fields(self, fields: Sequence[str], valid_fields: Set[str]) -> None:
        """Check a field selection without changing its order or contents.

        Require at least one field, reject duplicates, and enforce membership in
        the handler's allowed set. This is a semantic check for typed internal
        callers; it neither discovers columns in PostgreSQL nor quotes identifiers.

        Args:
            fields (Sequence[str]): Selected field names in SQL order.
            valid_fields (Set[str]): Allowed names declared by the handler.

        Returns:
            None.

        Raises:
            ValueError: If fields are empty, repeated, or not allowed.
        """
        selected = set(fields)
        if not selected or len(selected) != len(fields):
            raise ValueError("Fields must be nonempty and unique.")
        if not selected.issubset(valid_fields):
            raise ValueError("Field is not declared in VALID_FIELDS.")

    def _build_where_clause(
        self, where: Mapping[str, object], allowed: Set[str]
    ) -> tuple[sql.Composed, tuple[object, ...]]:
        """Translate supported filter mappings into SQL and aligned bound values.

        Share one interpretation of filters across SELECT, UPDATE, and DELETE.
        Treat unsuffixed names as equality, combine predicates with AND, and keep
        values out of the SQL text. Operator-specific operand checks are needed
        because filter values are typed as object. The helper only builds a fragment;
        it does not execute SQL or choose which rows a domain operation may affect.

        Args:
            where (Mapping[str, object]): Column or column__operator keys. Operators
                are eq, ne, lt, gt, lte, gte, like, ilike, trim_ne, between, in,
                and not_in. trim_ne compares TRIM(column) to a non-null string;
                ilike is PostgreSQL's case-insensitive pattern match. Plain
                keys mean eq. None supports eq/ne; between takes two non-null
                values; in/not_in take a list or tuple. Empty in is false and
                empty not_in is true. SQL expressions and legacy #CUSTOM# are rejected.
            allowed (Set[str]): Columns declared by the domain handler.

        Returns:
            tuple[sql.Composed, tuple[object, ...]]: WHERE fragment (empty when no
            filters) and bound values in placeholder order.

        Raises:
            ValueError: If a field, operator, or operand is invalid.
        """
        parts: list[sql.Composable] = []
        params: list[object] = []
        for key, value in where.items():
            name, separator, operator = key.partition("__")
            if not separator:
                operator = "eq"
            self._validate_fields((name,), allowed)
            field = sql.Identifier(name)
            match operator:
                case "eq" | "ne" if value is None:
                    test = (
                        sql.SQL("IS NULL")
                        if operator == "eq"
                        else sql.SQL("IS NOT NULL")
                    )
                    parts.append(sql.SQL("{} {}").format(field, test))
                case "trim_ne":
                    if not isinstance(value, str):
                        raise ValueError("TRIM comparison requires a string.")
                    parts.append(sql.SQL("TRIM({}) <> %s").format(field))
                    params.append(value)
                case "eq" | "ne" | "lt" | "gt" | "lte" | "gte" | "like" | "ilike":
                    if value is None:
                        raise ValueError("None is only supported with eq or ne.")
                    symbol = {
                        "eq": "=",
                        "ne": "<>",
                        "lt": "<",
                        "gt": ">",
                        "lte": "<=",
                        "gte": ">=",
                        "like": "LIKE",
                        "ilike": "ILIKE",
                    }[operator]
                    parts.append(sql.SQL("{} {} %s").format(field, sql.SQL(symbol)))
                    params.append(value)
                case "between":
                    if not isinstance(value, (list, tuple)) or len(value) != 2:
                        raise ValueError("BETWEEN requires two values.")
                    if any(item is None for item in value):
                        raise ValueError("BETWEEN bounds cannot be None.")
                    parts.append(sql.SQL("{} BETWEEN %s AND %s").format(field))
                    params.extend(value)
                case "in" | "not_in":
                    if not isinstance(value, (list, tuple)):
                        raise ValueError("IN requires a list or tuple.")
                    if not value:
                        parts.append(sql.SQL("FALSE" if operator == "in" else "TRUE"))
                        continue
                    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in value)
                    test = sql.SQL("IN" if operator == "in" else "NOT IN")
                    parts.append(
                        sql.SQL("{} {} ({})").format(field, test, placeholders)
                    )
                    params.extend(value)
                case _:
                    raise ValueError("Unsupported filter operator.")
        fragment = (
            sql.SQL(" WHERE ") + sql.SQL(" AND ").join(parts)
            if parts
            else sql.Composed([])
        )
        return fragment, tuple(params)

    def _build_order_by_clause(
        self, order_by: Sequence[tuple[str, str]], allowed: Set[str]
    ) -> sql.Composed:
        """Build explicit ordering from supported field and direction pairs.

        Preserve pair order as sort priority, accept ASC or DESC case insensitively,
        with optional NULLS FIRST/LAST, and validate fields against the handler
        declaration. Directions come from
        this fixed vocabulary, while column names are quoted as identifiers.
        An empty sequence leaves result ordering to PostgreSQL.

        Args:
            order_by (Sequence[tuple[str, str]]): Column/direction pairs in priority
                order. Directions are ASC or DESC, optionally followed by NULLS
                FIRST or NULLS LAST, case insensitive. Empty means no ORDER BY.
            allowed (Set[str]): Columns declared by the domain handler.

        Returns:
            sql.Composed: ORDER BY fragment, or an empty fragment.

        Raises:
            ValueError: If ordering is malformed or a column/direction is invalid.
        """
        parts: list[sql.Composable] = []
        for name, direction in order_by:
            if direction.upper() not in (
                "ASC",
                "DESC",
                "ASC NULLS FIRST",
                "ASC NULLS LAST",
                "DESC NULLS FIRST",
                "DESC NULLS LAST",
            ):
                raise ValueError("Invalid ordering direction or NULL placement.")
            self._validate_fields((name,), allowed)
            parts.append(
                sql.SQL("{} {}").format(
                    sql.Identifier(name), sql.SQL(direction.upper())
                )
            )
        return (
            sql.SQL(" ORDER BY ") + sql.SQL(", ").join(parts)
            if parts
            else sql.Composed([])
        )

    @staticmethod
    def _build_insert_query(
        table: sql.Identifier,
        columns: Sequence[str],
    ) -> sql.Composed:
        """Construct the INSERT shared by individual, batch, and UPSERT writes.

        Quote each column and create one placeholder per field in the supplied
        order. The calling operation has already resolved the qualified table and
        validated the columns; this helper only composes SQL. Callers align bound
        values to that order and add RETURNING or conflict handling as needed.
        No values, cursors, connections, or transaction state are handled here.

        Args:
            table (sql.Identifier): Already resolved schema-qualified table.
            columns (Sequence[str]): Nonempty, distinct, authorized column names
                in the order used for both placeholders and bound values.

        Returns:
            sql.Composed: INSERT statement with quoted identifiers and placeholders,
            without conflict/RETURNING clauses or embedded values.
        """
        return sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            table,
            sql.SQL(", ").join(sql.Identifier(name) for name in columns),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
        )

    # Internal execution lifecycle and failure reporting.

    @contextmanager
    def _cursor(self, operation: str) -> Generator[psycopg.Cursor[DbRow], None, None]:
        """Manage one cursor scope without taking ownership of the caller's transaction.

        Use as `with self._cursor(operation) as cursor` inside persistence helpers.
        Open or reuse the session, close the cursor on every exit, and report driver
        failures through the shared logger before propagating them. Close an idle
        session only when reuse is disabled; retain it while the caller owns a
        transaction so the caller can still commit or roll back.

        Args:
            operation (str): Operation name for sanitized failure logging.

        Returns:
            Generator[psycopg.Cursor[DbRow], None, None]: Yields one dict-row
            cursor to the with block, accepts no sent values, and returns None.
            The decorator exposes this generator as a context manager.

        Raises:
            psycopg.Error: Propagates session and SQL failures after logging.
        """
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                yield cursor
        except psycopg.Error as error:
            self._log_error(f"db_{operation}_failed", operation, error)
            raise
        finally:
            if not self.reuse_connection and not self.in_transaction:
                self.close()

    def _log_error(self, event: str, operation: str, error: psycopg.Error) -> None:
        """Report database failure classification without exposing query contents.

        Send the operation, handler class, exception class, and SQLSTATE through
        the injected shared logger. Exclude SQL text, bound values, connection
        settings, and exception messages, which may contain sensitive data.
        Formatting and output destinations remain the logger's responsibility.

        Args:
            event (str): Canonical database event name.
            operation (str): Operation being attempted.
            error (psycopg.Error): Driver exception; only type and SQLSTATE are used.

        Returns:
            None.
        """
        self.log.error_event(
            event,
            db_handler=type(self).__name__,
            operation=operation,
            error=type(error).__name__,
            sqlstate=error.sqlstate,
        )
