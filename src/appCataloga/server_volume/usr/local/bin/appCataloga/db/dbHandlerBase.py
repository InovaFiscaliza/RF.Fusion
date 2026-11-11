"""Implement the PostgreSQL foundation for appCataloga domain handlers.

Keep connection ownership, transaction control, SQL composition, and CRUD in
one base. Concrete handlers supply table metadata and domain operations;
workers call those handlers instead of constructing SQL themselves.
"""

from __future__ import annotations

from collections.abc import Generator, Mapping, Sequence, Set
from contextlib import contextmanager, nullcontext
from hashlib import sha256
from typing import TYPE_CHECKING, cast

from typing_extensions import LiteralString

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.pq import TransactionStatus
from psycopg.rows import RowMaker, dict_row

import config as k
from . import constants as db_constants

if TYPE_CHECKING:
    from shared import logging_utils


DbRow = dict[str, object]


class DbHandler:
    """Provide PostgreSQL persistence for handlers that own a data domain.

    The base manages sessions, transaction boundaries, and parameterized CRUD.
    Subclasses expose methods named after domain operations and supply the
    filters that identify each entity. Primary keys belong to the SQL schema.
    Task transitions, filesystem work, and SUMMARY publication do not belong to this class.

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
        operation. SQL constraints are defined only in PostgreSQL.

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
            connection = psycopg.Connection[DbRow].connect(
                self._conninfo,
                autocommit=True,
                row_factory=dict_row,
            )
        except psycopg.Error as error:
            self._log_error("db_connect_failed", "connect", error)
            raise
        self.db_connection = connection
        return connection

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

    @contextmanager
    def _savepoint(self) -> Generator[None, None, None]:
        """Isolate optional writes so their failure keeps the transaction usable.

        Inside a caller transaction the block runs under a savepoint and joins
        the caller's commit. Without one, _unit_of_work owns a short transaction,
        which also keeps non-reusable sessions open until the block ends.
        Failures roll back only the block and propagate to the caller.

        Args:
            None.

        Returns:
            Generator[None, None, None]: Context for the isolated block.

        Raises:
            Exception: Propagates any failure raised inside the block.
        """
        with self._unit_of_work():
            with self._cursor("savepoint") as cursor:
                if cursor.connection.info.transaction_status == TransactionStatus.IDLE:
                    # Start the server transaction so transaction() nests a
                    # savepoint instead of committing on exit.
                    cursor.execute(sql.SQL("SELECT 1"))
            with self.connect().transaction():
                yield

    def _publish_summary_scopes(
        self, scopes: Set[tuple[str, str]], *, reason: str | None
    ) -> None:
        """Coalesce Summary invalidations without aborting the caller's writes.

        Renew the outbox identity on conflict so checkpoint-based consumers see
        a re-dirtied scope. An active caller transaction retains the events
        until its own commit. A savepoint isolates publication failures; no SQL
        text, values, connection details, or raw driver messages are logged.
        Domain handlers choose the scopes; this method only persists them.

        Args:
            scopes (Set[tuple[str, str]]): (scope type, scope value) pairs;
                empty performs no SQL.
            reason (str | None): Diagnostic label truncated to the column size.

        Returns:
            None. Driver failures are reported as sanitized warnings; periodic
            reconciliation remains the recovery path for missing invalidations.
        """
        if not scopes:
            return
        query = sql.SQL(
            'INSERT INTO {} ("NA_SCOPE_TYPE", "NA_SCOPE_VALUE", "NA_SOURCE_HANDLER",'
            ' "NA_REASON") VALUES (%s, %s, %s, %s)'
            ' ON CONFLICT ("NA_SCOPE_TYPE", "NA_SCOPE_VALUE") DO UPDATE SET'
            ' "ID_OUTBOX" = EXCLUDED."ID_OUTBOX",'
            ' "NA_SOURCE_HANDLER" = EXCLUDED."NA_SOURCE_HANDLER",'
            ' "NA_REASON" = EXCLUDED."NA_REASON",'
            ' "DT_CREATED_AT" = EXCLUDED."DT_CREATED_AT"'
        ).format(sql.Identifier(k.SUMMARY_DATABASE_NAME, "SUMMARY_OUTBOX"))
        handler = type(self).__name__
        label = reason[:64] if reason else None
        rows = [(kind, value, handler, label) for kind, value in sorted(scopes)]
        try:
            with self._savepoint():
                self._execute_batch(query, rows)
        except psycopg.Error as error:
            self.log.warning_event(
                "summary_scope_publish_failed",
                db_handler=handler,
                operation="summary_publish",
                error=type(error).__name__,
                sqlstate=error.sqlstate,
            )

    def _setting_read(self, name: str) -> str:
        """Read one PostgreSQL run-time parameter of the current session.

        Args:
            name (str): Parameter name, bound as a value.
        Returns:
            str: Current textual setting.
        Raises:
            psycopg.Error: If the parameter is unknown or the read fails.
        """
        rows = self._select_raw(sql.SQL("SELECT current_setting(%s) AS value"), (name,))
        return cast(str, rows[0]["value"])

    def _setting_write(self, name: str, value: str, *, local: bool = False) -> None:
        """Set one PostgreSQL run-time parameter for the session or transaction.

        Args:
            name (str): Parameter name, bound as a value.
            value (str): New textual setting.
            local (bool): Limit the change to the current transaction.
        Returns:
            None.
        Raises:
            psycopg.Error: If the parameter or value is rejected.
        """
        self._select_raw(sql.SQL("SELECT set_config(%s, %s, %s)"), (name, value, local))

    @contextmanager
    def _local_setting(self, name: str, value: str) -> Generator[None, None, None]:
        """Apply a transaction-local parameter to one block and restore it after.

        A local setting lasts until the transaction ends, so callers sharing an
        outer transaction get the previous value back after a successful block.
        After a failure the transaction is aborted and must be rolled back,
        which already discards the setting; no restore is attempted then.

        Args:
            name (str): Parameter name.
            value (str): Value used inside the block.
        Returns:
            Generator[None, None, None]: Context for the bounded block.
        Raises:
            RuntimeError: If no explicit transaction is active.
            psycopg.Error: If reading or changing the parameter fails.
        """
        if not self.in_transaction:
            raise RuntimeError("Local settings require an explicit transaction.")
        previous = self._setting_read(name)
        self._setting_write(name, value, local=True)
        yield
        self._setting_write(name, previous, local=True)

    @staticmethod
    def _advisory_key(name: str) -> int:
        """Derive the signed 64-bit advisory lock key of a stable name.

        Args:
            name (str): Lock identity.
        Returns:
            int: First eight SHA-256 bytes as a signed big-endian integer.
        """
        return int.from_bytes(sha256(name.encode()).digest()[:8], "big", signed=True)

    def _advisory_lock_try(self, name: str) -> bool:
        """Try to take a session advisory lock without waiting.

        Args:
            name (str): Lock identity shared by competing sessions.
        Returns:
            bool: True when acquired by this session.
        Raises:
            psycopg.Error: If the lock call fails.
        """
        rows = self._select_raw(
            sql.SQL("SELECT pg_try_advisory_lock(%s) AS acquired"),
            (self._advisory_key(name),),
        )
        return bool(rows[0]["acquired"])

    def _advisory_unlock(self, name: str) -> bool:
        """Release a session advisory lock held by this session.

        Args:
            name (str): Lock identity used to acquire it.
        Returns:
            bool: False when this session did not hold the lock.
        Raises:
            psycopg.Error: If the unlock call fails.
        """
        rows = self._select_raw(
            sql.SQL("SELECT pg_advisory_unlock(%s) AS released"),
            (self._advisory_key(name),),
        )
        return bool(rows[0]["released"])

    # Protected persistence operations for domain subclasses.

    def _select_row(
        self,
        table: str,
        where: Mapping[str, object],
        *,
        columns: Sequence[str] | None = None,
    ) -> DbRow | None:
        """Read one entity using the domain method's identifying filters.

        The caller must identify at most one entity, by ID, composite key, or
        another unique identity. The base neither duplicates nor discovers SQL
        constraints. LIMIT 1 bounds the read; it does not verify uniqueness.

        Args:
            table (str): Unqualified table name.
            where (Mapping[str, object]): Nonempty identifying filters, using
                the same operators as _select_batch. The caller owns uniqueness.
            columns (Sequence[str] | None): Ordered projection; None selects all
                columns through SELECT *.

        Returns:
            DbRow | None: Matching row with native values, or None if absent.

        Raises:
            ValueError: On empty or malformed filters, or invalid projection.
            psycopg.Error: On query or session failure.
        """
        if not where:
            raise ValueError("Individual reads require explicit filters.")
        rows = self._select_batch(table, where, columns=columns, limit=1)
        return rows[0] if rows else None

    def _select_batch(
        self,
        table: str,
        where: Mapping[str, object] | None = None,
        *,
        columns: Sequence[str] | Mapping[str, tuple[str, str]] | None = None,
        order_by: Sequence[tuple[str, str]] = (),
        limit: int | None = None,
        for_update: bool = False,
        join: tuple[str, str, str] | None = None,
        joined_where: Mapping[str, object] | None = None,
        distinct: bool = False,
        prefix_tables: bool = False,
    ) -> list[DbRow]:
        """Read a filtered set of rows with quoted SQL identifiers.

        Read columns from one table, optionally joined to one related
        table. Without order_by, result order is unspecified. The whole result is
        collected into memory; this method does not stream rows or split the read
        into execution chunks.

        Args:
            table (str): Unqualified table name.
            where (Mapping[str, object] | None): AND filters documented by
                self._build_where_clause; None reads without filters.
            columns (Sequence[str] | Mapping[str, tuple[str, str]] | None): Root
                columns, or output names mapped to (query table, column).
                None selects all root columns through SELECT *.
            order_by (Sequence[tuple[str, str]]): Root column/direction pairs; ASC
                or DESC may include NULLS FIRST or NULLS LAST.
            limit (int | None): Nonnegative maximum rows; zero returns no rows.
            for_update (bool): Lock selected rows until the explicit caller
                transaction ends. False performs an ordinary read.
            join (tuple[str, str, str] | None): Optional INNER JOIN described as
                (other table, root column, other column); PostgreSQL checks existence.
            joined_where (Mapping[str, object] | None): AND filters on the joined
                table; requires join. Filter syntax matches where.
            distinct (bool): Remove duplicate projected rows in PostgreSQL;
                cannot be combined with for_update.
            prefix_tables (bool): With join and columns=None, return both table
                stars using TABLE__COLUMN keys from result metadata.

        Returns:
            list[DbRow]: Matching rows keyed by column or output name, with native
            values; an empty list when no rows match.

        Raises:
            ValueError: On invalid names, filters, ordering, join, or limit.
            RuntimeError: If for_update is requested without an explicit transaction.
            psycopg.Error: On query or session failure.
        """
        if for_update and not self.in_transaction:
            raise RuntimeError("FOR UPDATE requires an explicit transaction.")
        if for_update and distinct:
            raise ValueError("FOR UPDATE cannot lock DISTINCT rows.")
        if joined_where and not join:
            raise ValueError("Joined filters require a joined table.")
        target = self._table_identifier(table)
        source: sql.Composable = target
        # Aliases exist only with a JOIN, so single-table SQL stays unqualified.
        tables: dict[str, str | None] = {table: None}
        if join:
            joined_table, root_column, joined_column = join
            if joined_table == table:
                raise ValueError("A joined table must differ from the root table.")
            joined_target = self._table_identifier(joined_table)
            self._validate_columns((root_column,))
            self._validate_columns((joined_column,))
            tables = {table: "base", joined_table: "joined"}
            source = sql.SQL('{} AS "base" JOIN {} AS "joined" ON {} = {}').format(
                target, joined_target,
                sql.Identifier("base", root_column),
                sql.Identifier("joined", joined_column),
            )
        if prefix_tables and (not join or columns is not None):
            raise ValueError("Prefixed table reads require a JOIN and no projection.")
        projection: list[sql.Composable] = []
        if columns is None:
            aliases = tables.values() if prefix_tables else (tables[table],)
            projection = [
                sql.SQL("{}.*").format(sql.Identifier(alias)) if alias else sql.SQL("*")
                for alias in aliases
            ]
        else:
            if not isinstance(columns, Mapping):
                self._validate_columns(columns)
                columns = {name: (table, name) for name in columns}
            if not columns:
                raise ValueError("At least one field is required.")
            for output, (source_table, name) in columns.items():
                if not output or source_table not in tables:
                    raise ValueError("Projection must name a table of the query.")
                alias = tables[source_table]
                self._validate_columns((name,))
                field = sql.Identifier(alias, name) if alias else sql.Identifier(name)
                projection.append(
                    field if output == name
                    else sql.SQL("{} AS {}").format(field, sql.Identifier(output))
                )
        root_alias = tables[table]
        filters, params = self._build_where_clause(
            {} if where is None else where, alias=root_alias
        )
        if joined_where:
            joined_filters, values = self._build_where_clause(
                joined_where, alias="joined", conjunction=bool(where)
            )
            filters += joined_filters
            params += values
        query = (
            sql.SQL("SELECT DISTINCT {} FROM {}" if distinct else "SELECT {} FROM {}").format(
                sql.SQL(", ").join(projection), source
            )
            + filters
        )
        query += self._build_order_by_clause(order_by, alias=root_alias)
        if limit is not None:
            if limit < 0:
                raise ValueError("Limit must be a nonnegative integer.")
            query += sql.SQL(" LIMIT %s")
            params += (limit,)
        if for_update:
            query += sql.SQL(" FOR UPDATE")
        return self._select_raw(
            query, params, table_prefixes=tuple(tables) if prefix_tables else ()
        )

    def _select_nearest_point(
        self,
        table: str,
        *,
        point_column: str,
        identity_column: str,
        longitude: float,
        latitude: float,
    ) -> tuple[object, float] | None:
        """Read the nearest PostGIS point using spherical distance in meters.

        Args:
            table (str): Unqualified table name, resolved by the handler schema.
            point_column (str): Non-null geometry point column in decimal degrees.
            identity_column (str): Unique column used to identify and break ties.
            longitude (float): Query longitude in decimal degrees.
            latitude (float): Query latitude in decimal degrees.

        Returns:
            tuple[object, float] | None: Identity and distance in meters, or None
            when the table is empty. The query point uses the stored point SRID.

        Raises:
            ValueError: If column names are empty or repeated.
            psycopg.Error: If spatial calculation or the database operation fails.
        """
        self._validate_columns((point_column, identity_column))
        query = sql.SQL(
            'SELECT {identity} AS "IDENTITY", '
            'public.ST_DistanceSphere({point}, public.ST_SetSRID('
            'public.ST_MakePoint(%s, %s), public.ST_SRID({point}))) AS "DISTANCE" '
            'FROM {table} ORDER BY "DISTANCE" ASC, {identity} ASC LIMIT %s'
        ).format(
            identity=sql.Identifier(identity_column),
            point=sql.Identifier(point_column),
            table=self._table_identifier(table),
        )
        rows = self._select_raw(query=query, params=(longitude, latitude, 1))
        if not rows:
            return None
        return rows[0]["IDENTITY"], cast(float, rows[0]["DISTANCE"])

    _AGGREGATE_FUNCTIONS: dict[str, LiteralString] = {
        "count": "COUNT", "sum": "SUM", "min": "MIN", "max": "MAX"
    }

    def _select_aggregate(
        self,
        table: str,
        aggregates: Mapping[str, tuple[str, str | None, Mapping[str, object] | None]],
        *,
        group_by: Sequence[str] = (),
        month_buckets: Mapping[str, str] | None = None,
        where: Mapping[str, object] | None = None,
    ) -> list[DbRow]:
        """Aggregate columns with a closed function vocabulary.

        Each aggregate may carry its own filter, written with the same mapping
        syntax as where, so one scan can return several conditional counts and
        sums. Month buckets are the only computed grouping key. This is not an
        expression language: functions and output structure are checked before
        SQL execution.

        Args:
            table (str): Unqualified table name.
            aggregates (Mapping[str, tuple[str, str | None, Mapping[str, object] | None]]):
                Output name mapped to (function, column, filter). Functions are
                count, sum, min and max; column None is only valid for count(*).
                An empty or None filter aggregates every selected row.
            group_by (Sequence[str]): Grouping columns, returned by name.
            month_buckets (Mapping[str, str] | None): Output name mapped to a
                timestamp column grouped by the first day of its month (date).
            where (Mapping[str, object] | None): Row filters applied before grouping.

        Returns:
            list[DbRow]: One row per group, or one row when nothing is grouped.
            SUM of integer columns is returned by PostgreSQL as Decimal.

        Raises:
            ValueError: On unknown functions, empty columns or repeated outputs.
            psycopg.Error: On query or session failure.
        """
        target = self._table_identifier(table)
        buckets = month_buckets or {}
        outputs = [*group_by, *buckets, *aggregates]
        if not aggregates or not all(outputs) or len(set(outputs)) != len(outputs):
            raise ValueError("Aggregate outputs must be nonempty and unique.")
        if group_by:
            self._validate_columns(group_by)
        keys: list[sql.Composable] = [sql.Identifier(name) for name in group_by]
        projection = list(keys)
        for output, column in buckets.items():
            self._validate_columns((column,))
            bucket = sql.SQL("date_trunc('month', {})::date").format(sql.Identifier(column))
            keys.append(bucket)
            projection.append(sql.SQL("{} AS {}").format(bucket, sql.Identifier(output)))
        params: list[object] = []
        for output, (function, column, condition) in aggregates.items():
            name = self._AGGREGATE_FUNCTIONS.get(function)
            if name is None or (column is None and function != "count"):
                raise ValueError("Unsupported aggregate.")
            if column is not None:
                self._validate_columns((column,))
            operand = sql.SQL("*") if column is None else sql.Identifier(column)
            expression = sql.SQL("{}({})").format(sql.SQL(name), operand)
            if condition:
                # The builder emits " AND ..." predicates; TRUE anchors them.
                predicate, values = self._build_where_clause(
                    condition, conjunction=True
                )
                expression += sql.SQL(" FILTER (WHERE TRUE{})").format(predicate)
                params.extend(values)
            projection.append(sql.SQL("{} AS {}").format(expression, sql.Identifier(output)))
        filters, values = self._build_where_clause(where or {})
        params.extend(values)
        query = sql.SQL("SELECT {} FROM {}").format(sql.SQL(", ").join(projection), target)
        query += filters
        if keys:
            query += sql.SQL(" GROUP BY {}").format(sql.SQL(", ").join(keys))
        return self._select_raw(query, tuple(params))

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
            table (str): Unqualified table name.
            data (Mapping[str, object]): Nonempty column/value mapping.
            skip_duplicates (bool): Use ON CONFLICT DO NOTHING for uniqueness conflicts;
                other constraint violations still raise.
            returning (Sequence[str]): Ordered fields for RETURNING; empty
                disables fetching. Never infer an ID from a sequence counter.

        Returns:
            DbRow | None: Requested fields for the inserted row, or None when
            no RETURNING was requested or a conflict skipped the insert.

        Raises:
            ValueError: On empty data or empty/repeated column names.
            psycopg.Error: On insert or session failure.
        """
        target = self._table_identifier(table)
        columns = tuple(data)
        self._validate_columns(columns)
        query = self._build_insert_query(target, columns)
        if skip_duplicates:
            query += sql.SQL(" ON CONFLICT DO NOTHING")
        if returning:
            self._validate_columns(returning)
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
        batch_size: int = db_constants['persistence']['batch_size'],
    ) -> int:
        """Persist homogeneous records as one atomic operation across all chunks.

        Use when records share a column set, even if their mapping order differs.
        Align values to the first record before sending any writes. batch_size
        bounds each executemany call; it does not introduce intermediate commits.
        When the caller owns no transaction, the executor creates one for all chunks.
        Otherwise, the caller remains responsible for commit or rollback.

        Args:
            table (str): Unqualified table name.
            data (Sequence[Mapping[str, object]]): Nonempty column/value mappings
                with identical column sets; mapping insertion order may differ.
            skip_duplicates (bool): Skip uniqueness conflicts with ON CONFLICT DO NOTHING.
            batch_size (int): Positive parameter-row count per executemany call.

        Returns:
            int: Inserted row count, excluding skipped conflicts; zero for empty
            input. Does not infer or return generated IDs from sequences.

        Raises:
            ValueError: On empty/repeated columns, heterogeneous data, or chunk size.
            psycopg.Error: On SQL, session, or self-owned commit failure.
        """
        if not data:
            return 0
        target = self._table_identifier(table)
        columns = tuple(data[0])
        self._validate_columns(columns)
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
        increments: Mapping[str, int | float] | None = None,
        maximums: Mapping[str, object] | None = None,
    ) -> int:
        """Update the entity identified by the domain method's explicit filters.

        The caller must identify at most one entity and may include additional
        conditions such as its expected status. The base does not inspect SQL
        constraints or limit affected rows. Reuse the filtered set-update path
        without completing a caller-owned transaction.

        Args:
            table (str): Unqualified table name.
            data (Mapping[str, object]): Replacement fields and values; may be
                empty when another argument supplies an assignment.
            where (Mapping[str, object]): Nonempty identifying filters. The caller
                owns uniqueness; operators match _update_batch.
            touch_field (str | None): Timestamp field set to CURRENT_TIMESTAMP.
            increments (Mapping[str, int | float] | None): Numeric deltas, as in
                self._update_batch.
            maximums (Mapping[str, object] | None): Monotonic lower bounds, as in
                self._update_batch.

        Returns:
            int: PostgreSQL's affected count, normally one or zero when absent
                or when an additional condition does not match.

        Raises:
            ValueError: On missing/malformed filters or invalid assignments.
            psycopg.Error: On update or session failure.
        """
        return self._update_batch(
            table,
            data,
            where,
            touch_field=touch_field,
            increments=increments,
            maximums=maximums,
        )

    def _update_batch(
        self,
        table: str,
        data: Mapping[str, object],
        where: Mapping[str, object],
        *,
        touch_field: str | None = None,
        increments: Mapping[str, int | float] | None = None,
        maximums: Mapping[str, object] | None = None,
    ) -> int:
        """Apply one set of assignments to every row matching a predicate.

        This is a set-based UPDATE, not a list of different updates. Every column
        receives at most one assignment kind; all values are bound parameters.
        A nonempty filter is required, but the caller must still choose its
        selectivity. All matched rows belong to one SQL statement and share the
        caller's transaction when one is active.

        Args:
            table (str): Unqualified table name.
            data (Mapping[str, object]): Columns and literal replacement values;
                may be empty when another argument supplies an assignment.
            where (Mapping[str, object]): Required, nonempty AND filters using
                the shared suffix syntax; matching all rows is caller-owned.
            touch_field (str | None): Declared column set to CURRENT_TIMESTAMP.
            increments (Mapping[str, int | float] | None): Signed deltas added
                atomically to numeric fields. Null database values remain null.
            maximums (Mapping[str, object] | None): Values applied with GREATEST,
                so a stored value only advances. PostgreSQL ignores NULL in
                GREATEST: a null column takes the value and a null value keeps it.

        Returns:
            int: Rows matched by PostgreSQL, including unchanged values.

        Raises:
            ValueError: On missing filters, empty column names,
                or a column assigned more than once.
            psycopg.Error: On update or session failure.
        """
        target = self._table_identifier(table)
        increments = increments or {}
        maximums = maximums or {}
        touched = () if touch_field is None else (touch_field,)
        assigned = (*increments, *maximums, *data, *touched)
        if not where or not assigned:
            raise ValueError("Update requires filters and at least one assignment.")
        self._validate_columns(assigned)
        templates: tuple[
            tuple[Mapping[str, object] | Sequence[str], LiteralString], ...
        ] = (
            (increments, "{0} = {0} + %s"),
            (maximums, "{0} = GREATEST({0}, %s)"),
            (data, "{0} = %s"),
            (touched, "{0} = CURRENT_TIMESTAMP"),
        )
        assignments = [
            sql.SQL(template).format(sql.Identifier(name))
            for names, template in templates
            for name in names
        ]
        filters, params = self._build_where_clause(where)
        query = (
            sql.SQL("UPDATE {} SET {}").format(target, sql.SQL(", ").join(assignments))
            + filters
        )
        values = (*increments.values(), *maximums.values(), *data.values())
        return self._execute(query, (*values, *params))

    def _delete_row(
        self,
        table: str,
        where: Mapping[str, object],
    ) -> int:
        """Delete the entity identified by the domain method's explicit filters.

        The caller must identify at most one entity, optionally with additional
        conditions. The base does not inspect constraints or limit affected rows;
        it delegates to the filtered set-delete path.

        Args:
            table (str): Unqualified table name.
            where (Mapping[str, object]): Nonempty identifying filters. The caller
                owns uniqueness; operators match _delete_batch.

        Returns:
            int: PostgreSQL's affected count, normally one or zero when absent
                or when an additional condition does not match.

        Raises:
            ValueError: On missing or malformed filters.
            psycopg.Error: On delete or session failure.
        """
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
            table (str): Unqualified table name.
            where (Mapping[str, object]): Required nonempty AND filters; the
                caller remains responsible for their selectivity.

        Returns:
            int: Number of deleted rows; zero if no rows match.

        Raises:
            ValueError: On missing filters or invalid filter structure.
            psycopg.Error: On delete or session failure.
        """
        if not where:
            raise ValueError("Delete requires explicit filters.")
        target = self._table_identifier(table)
        filters, params = self._build_where_clause(where)
        return self._execute(sql.SQL("DELETE FROM {}").format(target) + filters, params)

    def _delete_all(self, table: str) -> int:
        """Delete every row of a table as an explicit full rebuild step.

        Separate from _delete_batch so an empty filter can never become a global
        delete by accident. Uses DELETE rather than TRUNCATE so concurrent
        readers are not blocked and the caller's transaction can still roll back.

        Args:
            table (str): Unqualified table name.

        Returns:
            int: Number of deleted rows.

        Raises:
            psycopg.Error: On delete or session failure.
        """
        target = self._table_identifier(table)
        return self._execute(sql.SQL("DELETE FROM {}").format(target))

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
            table (str): Unqualified table name.
            data (Mapping[str, object]): Nonempty inserted column/value mapping.
            conflict_columns (Sequence[str]): Explicit inserted conflict columns backed
                by a unique constraint or inferable unique index.
            touch_field (str | None): Non-key timestamp column updated on conflict.

        Returns:
            int: One row inserted/updated, or zero when an existing key-only
            record is skipped by DO NOTHING.
            Does not use MariaDB's two-affected-rows convention.

        Raises:
            ValueError: On invalid data structure or conflict keys.
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
        batch_size: int = db_constants['persistence']['batch_size'],
    ) -> int:
        """Insert records and resolve declared uniqueness conflicts in one unit.

        Build ON CONFLICT from conflict_columns, which must be inserted columns
        backed by an actual PostgreSQL unique key or inferable index. On conflict,
        replace supplied non-key fields and optionally update touch_field. If no
        update assignments exist, use DO NOTHING. Repeated identities in the input
        are processed in input order. All execution chunks share one transaction;
        failures do not produce a partial success count.

        Args:
            table (str): Unqualified table name.
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
            ValueError: On heterogeneous/invalid data structure, conflict keys,
                or chunk size.
            psycopg.Error: On SQL, session, or self-owned commit failure.
        """
        if not data:
            return 0
        target = self._table_identifier(table)
        columns = tuple(data[0])
        self._validate_columns(columns)
        query = self._build_insert_query(target, columns)
        self._validate_columns(conflict_columns)
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
            self._validate_columns((touch_field,))
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
        query: sql.SQL | sql.Composed,
        params: Sequence[object] = (),
        *,
        table_prefixes: Sequence[str] = (),
    ) -> list[DbRow]:
        """Read a developer-owned query that does not fit single-table selection.

        Use from domain handlers for JOINs, aggregates, CTEs, or explicit projections.
        The handler is responsible for composing identifiers and choosing aliases.
        PostgreSQL validates table and column existence. Bind external values
        through params. Fetch all rows and close the cursor without changing the
        caller's transaction ownership.

        Args:
            query (sql.SQL | sql.Composed): Developer-owned SELECT, including explicit
                JOINs or aggregates. Never compose SQL from untrusted strings.
            params (Sequence[object]): Values bound to placeholders, in order.
            table_prefixes (Sequence[str]): Optional output prefixes for ordered
                table-star groups. Names come from result column origins, without
                a catalog query; values retain native driver types.

        Returns:
            list[DbRow]: Rows keyed by exact column names or aliases; empty
            when no rows match. Values retain the driver's Python types.

        Raises:
            ValueError: If prefixed output groups do not match result metadata.
            psycopg.Error: On execution or session failure.
        """
        with self._cursor("select") as cursor:
            if table_prefixes:
                def prefixed_row(cursor: psycopg.Cursor[DbRow]) -> RowMaker[DbRow]:
                    """Name contiguous table-star groups from result metadata.

                    Args:
                        cursor (psycopg.Cursor[DbRow]): Executed SELECT cursor.
                    Returns:
                        RowMaker[DbRow]: Native values keyed by TABLE__COLUMN.
                    Raises:
                        ValueError: If the result differs from the requested groups.
                    """
                    if cursor.pgresult is None or cursor.description is None:
                        raise ValueError("Prefixed reads require column metadata.")
                    origins = [
                        cursor.pgresult.ftable(i)
                        for i in range(len(cursor.description))
                    ]
                    sources = dict.fromkeys(origins)
                    prefixes = dict(zip(sources, table_prefixes, strict=True))
                    names = [
                        f"{prefixes[origin]}__{column.name}"
                        for origin, column in zip(origins, cursor.description, strict=True)
                    ]
                    return lambda values: dict(zip(names, values, strict=True))

                cursor.row_factory = prefixed_row
            cursor.execute(query, params)
            return cursor.fetchall()

    def _execute(
        self,
        query: sql.SQL | sql.Composed,
        params: Sequence[object] = (),
    ) -> int:
        """Run one developer-owned statement and report PostgreSQL's row count.

        Provide the execution path for structured writes and domain-specific SQL.
        Identifier validation belongs to its builder or handler; values remain bound
        through params. Outside an explicit transaction, the statement uses the
        session's autocommit mode. Inside one, both success and failure leave
        transaction completion to the caller.

        Args:
            query (sql.SQL | sql.Composed): Developer-owned statement. Do not send
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
        query: sql.SQL | sql.Composed,
        values: Sequence[Sequence[object]],
        *,
        batch_size: int = db_constants['persistence']['batch_size'],
    ) -> int:
        """Run parameter sets in bounded chunks with one transaction boundary.

        Create one transaction around all chunks when the caller owns none, so a
        later failure rolls back earlier chunks. If a caller transaction is active,
        use it directly without a separate commit, rollback, or savepoint. A failure
        then propagates for the caller to resolve. batch_size controls transmission
        chunks, not durability boundaries. Empty input performs no SQL.

        Args:
            query (sql.SQL | sql.Composed): Trusted statement with ordered placeholders.
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

    def _prepare_batch_values(
        self,
        rows: Sequence[Mapping[str, object]],
    ) -> list[tuple[object, ...]]:
        """Align each record to the first record's INSERT placeholder order.

        Check every record's column set before execution so differing dictionary
        order cannot associate a value with the wrong column. Values are preserved
        without conversion. Callers handle empty input before invoking this helper;
        column structure is checked when the shared statement is built.

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

    def _table_identifier(self, table: str) -> sql.Identifier:
        """Qualify a literal table name without duplicating the SQL schema.

        Args:
            table (str): Unqualified table name. SCHEMA_<TABLE> overrides the
                handler's default schema for cross-schema source reads.
        Returns:
            sql.Identifier: Quoted schema and table; PostgreSQL checks existence.
        """
        return sql.Identifier(getattr(self, f"SCHEMA_{table}", self.schema), table)

    @staticmethod
    def _validate_columns(columns: Sequence[str]) -> None:
        """Require a nonempty column sequence without repeated assignments.

        Args:
            columns (Sequence[str]): Column names in SQL order.
        Returns:
            None.
        Raises:
            ValueError: If the sequence or any name is empty, or names repeat.
        """
        if not columns or not all(columns) or len(set(columns)) != len(columns):
            raise ValueError("Columns must be nonempty and unique.")

    def _build_where_clause(
        self,
        where: Mapping[str, object],
        *,
        alias: str | None = None,
        conjunction: bool = False,
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
                empty not_in is true. SQL values cannot be expressions; every name is
                quoted literally, without a legacy raw-SQL escape hatch.
            alias (str | None): Optional literal table alias for qualified fields.
            conjunction (bool): Prefix nonempty predicates with AND instead of WHERE.

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
            self._validate_columns((name,))
            if isinstance(value, sql.Composable):
                raise ValueError("SQL fragments are not filter values.")
            field = sql.Identifier(alias, name) if alias else sql.Identifier(name)
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
                    symbols: dict[str, LiteralString] = {
                        "eq": "=",
                        "ne": "<>",
                        "lt": "<",
                        "gt": ">",
                        "lte": "<=",
                        "gte": ">=",
                        "like": "LIKE",
                        "ilike": "ILIKE",
                    }
                    symbol = symbols[operator]
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
            sql.SQL(" AND " if conjunction else " WHERE ")
            + sql.SQL(" AND ").join(parts)
            if parts
            else sql.Composed([])
        )
        return fragment, tuple(params)

    def _build_order_by_clause(
        self,
        order_by: Sequence[tuple[str, str]],
        *,
        alias: str | None = None,
    ) -> sql.Composed:
        """Build explicit ordering from supported field and direction pairs.

        Preserve pair order as sort priority, accept ASC or DESC case insensitively,
        with optional NULLS FIRST/LAST. Directions come from
        this fixed vocabulary, while column names are quoted as identifiers.
        An empty sequence leaves result ordering to PostgreSQL.

        Args:
            order_by (Sequence[tuple[str, str]]): Column/direction pairs in priority
                order. Directions are ASC or DESC, optionally followed by NULLS
                FIRST or NULLS LAST, case insensitive. Empty means no ORDER BY.
            alias (str | None): Optional literal table alias for qualified fields.

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
            self._validate_columns((name,))
            parts.append(
                sql.SQL("{} {}").format(
                    sql.Identifier(alias, name) if alias else sql.Identifier(name),
                    sql.SQL(cast(LiteralString, direction.upper())),
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
            columns (Sequence[str]): Nonempty, distinct column names
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
