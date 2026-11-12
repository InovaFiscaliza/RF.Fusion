"""Migrate a MariaDB logical dump into a new RF.Fusion PostgreSQL installation.

The gzip dump is streamed once and never expanded on disk. ``inspect`` converts
every migrated row without connecting to PostgreSQL and reports counts, control
totals, conversion problems and duplicates rejected by PostgreSQL-only unique
keys. ``load`` requires a database without the four RF.Fusion schemas, applies
the bootstrap DDL, replaces its seed rows with the dump rows and copies all data
in one transaction. Foreign keys are dropped during the copy and recreated
before commit, which validates referential integrity for the whole load.

Reports carry counts, identifiers and catalog labels only. File paths, e-mails
and other row values are never printed.
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
import gzip
import hashlib
import importlib.util
import json
import logging
import os
from pathlib import Path
import re
import shutil
import struct
import sys
import time

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo


def _load_config():
    """Load the sibling `config.py` without clashing with other `config` modules.

    Returns:
        module: Migration configuration module.
    """
    spec = importlib.util.spec_from_file_location("postgres_migration_config", Path(__file__).with_name("config.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


k = _load_config()
LOGGER = logging.getLogger("mariadb_dump_to_postgres")

_VALUE = r"'[^'\\]*(?:\\.[^'\\]*)*'|NULL|0x[0-9A-Fa-f]*|[-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?"
_ROW_RE = re.compile(rf"\((?:{_VALUE})(?:,(?:{_VALUE}))*\)")
_TOKEN_RE = re.compile(
    r"(')([^'\\]*(?:\\.[^'\\]*)*)'|(NULL)|(0x)([0-9A-Fa-f]*)"
    r"|([-+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][-+]?\d+)?)"
)
_INSERT_RE = re.compile(r"INSERT INTO `([^`]+)`(?: \(([^)]*)\))? VALUES\s*")
_COLUMN_RE = re.compile(r"^\s+`([^`]+)` ([A-Za-z]+)")
_AUTO_INCREMENT_RE = re.compile(r"AUTO_INCREMENT=(\d+)")
_MYSQL_ESCAPE_RE = re.compile(r"\\(.)", re.S)
_COPY_COMMAND_RE = re.compile(r"^\\copy\s+(.+?)\s+FROM\s+'([^']+)'\s+WITH\s+(\(.*\))\s*$", re.I)

# MySQL keeps the backslash for \% and \_; every other unknown escape drops it.
_MYSQL_ESCAPES = {
    "0": "\0", "b": "\b", "n": "\n", "r": "\r", "t": "\t", "Z": "\x1a",
    "%": "\\%", "_": "\\_",
}
_DUMP_TYPE_KINDS = {
    "point": "geometry", "geometry": "geometry",
    "blob": "bytea", "tinyblob": "bytea", "mediumblob": "bytea", "longblob": "bytea",
    "binary": "bytea", "varbinary": "bytea",
    "json": "json",
    "date": "temporal", "datetime": "temporal", "timestamp": "temporal", "time": "temporal",
    "tinyint": "number", "smallint": "number", "mediumint": "number", "int": "number",
    "integer": "number", "bigint": "number", "decimal": "number", "numeric": "number",
    "float": "number", "double": "number",
    "char": "text", "varchar": "text", "tinytext": "text", "text": "text",
    "mediumtext": "text", "longtext": "text", "enum": "text", "set": "text",
}
_EWKB_SRID_FLAG = 0x20000000
_WKB_POINT = 1


class MigrationError(RuntimeError):
    """Signal a dump, catalog or validation problem that stops the migration."""


@dataclass
class DumpTable:
    """Describe one table structure declared in the dump.

    Attributes:
        database (str): MariaDB database that maps to a PostgreSQL schema.
        name (str): Table name with its original case.
        columns (list[str]): Column names in dump order.
        kinds (list[str]): Value kind per column: number, text, temporal, json,
            bytea, geometry or `unsupported <type>`.
        auto_increment (int | None): Next MariaDB identity value, if declared.
    """

    database: str
    name: str
    columns: list[str]
    kinds: list[str]
    auto_increment: int | None


@dataclass
class InsertStatement:
    """Carry one complete INSERT statement from the dump.

    Attributes:
        database (str): Database selected by the preceding USE statement.
        table (str): Target table name.
        columns (list[str] | None): Explicit column list, or None for all columns.
        values (str): Text after `VALUES`, including the final semicolon.
        line (int): Line where the statement starts, for error messages.
    """

    database: str
    table: str
    columns: list[str] | None
    values: str
    line: int


@dataclass
class TableStats:
    """Accumulate per-table counts and controls while rows are converted.

    Attributes:
        rows (int): Converted rows, starting at 0.
        first_column_sum (int | None): Sum of integer values in the first column;
            None when the column is not an integer key.
        issues (Counter[str]): Conversion problems by type.
        issue_samples (list[str]): First row locations of conversion problems.
        srids (Counter[int]): SRIDs found in geometry values.
        bounds (list[float] | None): Minimum and maximum X and Y of geometries.
        duplicates (int): Rows rejected by a PostgreSQL-only unique key.
        duplicate_samples (list[dict[str, object]]): First duplicated rows, with
            the identifiers of both rows and, for labels, the repeated value.
        seconds (float): Conversion and copy time.
    """

    rows: int = 0
    first_column_sum: int | None = 0
    issues: Counter = field(default_factory=Counter)
    issue_samples: list[str] = field(default_factory=list)
    srids: Counter = field(default_factory=Counter)
    bounds: list[float] | None = None
    duplicates: int = 0
    duplicate_samples: list[dict[str, object]] = field(default_factory=list)
    seconds: float = 0.0


def _mysql_unescape(text: str) -> str:
    """Decode the backslash escapes written by mysqldump inside a string literal.

    Args:
        text (str): Literal content without the surrounding quotes.

    Returns:
        str: Original string value.
    """
    if "\\" not in text:
        return text
    return _MYSQL_ESCAPE_RE.sub(lambda match: _MYSQL_ESCAPES.get(match.group(1), match.group(1)), text)


def _copy_escape(text: str) -> str:
    """Escape one decoded value for the PostgreSQL COPY text format.

    Args:
        text (str): Decoded value.

    Returns:
        str: Value with backslash, tab, newline and carriage return escaped.
    """
    return (
        text.replace("\\", "\\\\").replace("\t", "\\t")
        .replace("\n", "\\n").replace("\r", "\\r")
    )


def mysql_geometry_to_ewkb(raw: bytes) -> tuple[str, int, float, float]:
    """Convert a MariaDB internal point to PostGIS hexadecimal EWKB.

    MariaDB stores a little-endian 4-byte SRID followed by standard WKB. The
    SRID is preserved in EWKB only when it is not 0, which PostGIS treats as
    unknown.

    Args:
        raw (bytes): Internal MariaDB geometry value.

    Returns:
        tuple[str, int, float, float]: Uppercase hexadecimal EWKB, SRID, X and Y.

    Raises:
        MigrationError: If the value is not a 2D point.
    """
    if len(raw) != 25:
        raise MigrationError(f"geometry with {len(raw)} bytes is not a 2D point")
    srid = int.from_bytes(raw[:4], "little")
    wkb = raw[4:]
    if wkb[0] not in (0, 1):
        raise MigrationError("geometry has an invalid WKB byte order")
    endian = "little" if wkb[0] == 1 else "big"
    geometry_type = int.from_bytes(wkb[1:5], endian)
    if geometry_type != _WKB_POINT:
        raise MigrationError(f"geometry type {geometry_type} is not a point")
    x, y = struct.unpack(("<" if endian == "little" else ">") + "dd", wkb[5:21])
    if srid:
        wkb = (
            wkb[:1] + (geometry_type | _EWKB_SRID_FLAG).to_bytes(4, endian)
            + srid.to_bytes(4, endian) + wkb[5:]
        )
    return wkb.hex().upper(), srid, x, y


def _parse_create_table(database: str, lines: list[str]) -> DumpTable:
    """Read the column list and identity state of one dumped table.

    Args:
        database (str): Database selected by the preceding USE statement.
        lines (list[str]): CREATE TABLE lines up to the closing ENGINE line.

    Returns:
        DumpTable: Columns in dump order with their value kinds. Unsupported
            types receive the kind `unsupported <type>`.
    """
    name = lines[0].split("`")[1]
    columns: list[str] = []
    kinds: list[str] = []
    for line in lines[1:-1]:
        match = _COLUMN_RE.match(line)
        if not match:
            continue
        column, dump_type = match.group(1), match.group(2).lower()
        # Unsupported types only matter for migrated tables; see `plan_table`.
        kind = "json" if f"json_valid(`{column}`)" in line else _DUMP_TYPE_KINDS.get(dump_type, f"unsupported {dump_type}")
        columns.append(column)
        kinds.append(kind)
    auto_increment = _AUTO_INCREMENT_RE.search(lines[-1])
    return DumpTable(database, name, columns, kinds, int(auto_increment.group(1)) if auto_increment else None)


def iter_dump(path: Path) -> Iterator[DumpTable | InsertStatement]:
    """Stream table structures and INSERT statements from a gzip dump.

    Args:
        path (Path): `mysqldump` output compressed with gzip.

    Returns:
        Iterator[DumpTable | InsertStatement]: Items in dump order. A table is
            yielded before its INSERT statements.

    Raises:
        MigrationError: If an INSERT appears outside a database.
        UnicodeDecodeError: If the dump is not valid UTF-8.
    """
    database: str | None = None
    table_lines: list[str] | None = None
    insert_lines: list[str] | None = None
    insert_start = 0
    with gzip.open(path, "rt", encoding="utf-8", newline="\n") as stream:
        for number, line in enumerate(stream, 1):
            if insert_lines is not None:
                insert_lines.append(line)
                if line.rstrip().endswith(";"):
                    yield _insert_statement(database, "".join(insert_lines), insert_start)
                    insert_lines = None
                continue
            if table_lines is not None:
                table_lines.append(line)
                if line.startswith(")"):
                    yield _parse_create_table(database or "", table_lines)
                    table_lines = None
                continue
            if line.startswith("USE `"):
                database = line.split("`")[1]
            elif line.startswith("CREATE TABLE `"):
                table_lines = [line]
            elif line.startswith("INSERT INTO `"):
                if database is None:
                    raise MigrationError(f"line {number}: INSERT outside a database")
                if line.rstrip().endswith(";"):
                    yield _insert_statement(database, line, number)
                else:
                    insert_lines, insert_start = [line], number


def _insert_statement(database: str | None, text: str, line: int) -> InsertStatement:
    """Split an INSERT statement into table, optional columns and values text.

    Args:
        database (str | None): Database selected by USE.
        text (str): Complete INSERT statement.
        line (int): Starting line number.

    Returns:
        InsertStatement: Parsed statement header and its VALUES text.

    Raises:
        MigrationError: If the statement header is not recognized.
    """
    match = _INSERT_RE.match(text)
    if not match or database is None:
        raise MigrationError(f"line {line}: unsupported INSERT statement")
    columns = [name.strip().strip("`") for name in match.group(2).split(",")] if match.group(2) else None
    return InsertStatement(database, match.group(1), columns, text[match.end():], line)


def iter_rows(statement: InsertStatement) -> Iterator[list[tuple[str, ...]]]:
    """Yield the value tokens of each row in one INSERT statement.

    Every character between rows is checked, so a malformed statement fails
    instead of silently losing rows.

    Args:
        statement (InsertStatement): Statement produced by `iter_dump`.

    Returns:
        Iterator[list[tuple[str, ...]]]: One token list per row. Each token has
            six groups: quote, string, NULL, hex marker, hex digits and number.

    Raises:
        MigrationError: If text outside the rows is not a separator.
    """
    text = statement.values
    position = 0
    separator = ""
    for match in _ROW_RE.finditer(text):
        if text[position:match.start()].strip() != separator:
            raise MigrationError(f"line {statement.line}: unexpected text between rows")
        yield _TOKEN_RE.findall(match.group(0), 1, len(match.group(0)) - 1)
        position = match.end()
        separator = ","
    if text[position:].strip() != ";":
        raise MigrationError(f"line {statement.line}: INSERT does not end after its rows")


class RowConverter:
    """Convert dump tokens of one table into PostgreSQL COPY text lines.

    The converter also maintains the table statistics and, in inspection mode,
    validates JSON and PostgreSQL-only unique keys.

    Attributes:
        table (DumpTable): Dumped table structure.
        kinds (list[str]): Value kind for each column in dump order.
        stats (TableStats): Counts and controls for the converted rows.
        validate_json (bool): Parse JSON locally instead of leaving it to PostgreSQL.
        unique_key (tuple[list[int], bool] | None): Key column positions and whether
            duplicated values may be reported.
        seen_keys (dict[bytes, str]): Digest of each unique key and its first row ID.
        keep_rows (list[tuple[str | None, ...]] | None): Decoded rows retained
            for comparison with bootstrap seed rows.
        logged_intervals (int): Progress intervals already logged, starting at 0.
    """

    def __init__(
        self,
        table: DumpTable,
        *,
        validate_json: bool,
        unique_key: tuple[tuple[str, ...], bool] | None,
        keep_rows: bool,
    ) -> None:
        """Prepare one table conversion.

        Args:
            table (DumpTable): Dumped table structure.
            validate_json (bool): Parse JSON values locally.
            unique_key (tuple[tuple[str, ...], bool] | None): PostgreSQL-only
                unique columns and whether their values may be reported.
            keep_rows (bool): Retain decoded rows for seed comparison.

        Raises:
            MigrationError: If a unique key column is missing from the dump.
        """
        self.table = table
        self.kinds = table.kinds
        self.stats = TableStats(first_column_sum=0 if table.kinds and table.kinds[0] == "number" else None)
        self.validate_json = validate_json
        self.unique_key = None
        if unique_key:
            missing = [column for column in unique_key[0] if column not in table.columns]
            if missing:
                raise MigrationError(f"{table.database}.{table.name}: unique key columns {missing} not dumped")
            self.unique_key = ([table.columns.index(column) for column in unique_key[0]], unique_key[1])
        self.seen_keys: dict[bytes, str] = {}
        self.keep_rows: list[tuple[str | None, ...]] | None = [] if keep_rows else None
        self.logged_intervals = 0

    def convert(self, tokens: list[tuple[str, ...]], line: int) -> str | None:
        """Convert one row and update the table statistics.

        Args:
            tokens (list[tuple[str, ...]]): Row tokens from `iter_rows`.
            line (int): Statement line, used only in issue locations.

        Returns:
            str | None: COPY text line ending with a newline; None when the row
                has a conversion problem recorded in the statistics.
        """
        if len(tokens) != len(self.kinds):
            return self._issue("column count", line)
        fields: list[str] = []
        for index, (kind, (quote, text, null, hex_marker, hex_digits, number)) in enumerate(zip(self.kinds, tokens)):
            if null:
                fields.append("\\N")
            elif number:
                if kind not in ("number", "text"):
                    return self._issue(f"number in {kind} column", line)
                fields.append(number)
            elif quote:
                if kind in ("bytea", "geometry"):
                    return self._issue(f"quoted value in {kind} column", line)
                if "\\" in text or "\t" in text:
                    value = _mysql_unescape(text)
                    if "\0" in value:
                        return self._issue("NUL character", line)
                    text = _copy_escape(value)
                if kind == "temporal" and text.startswith("0000-00-00"):
                    return self._issue("zero date", line)
                if kind == "json" and self.validate_json:
                    try:
                        json.loads(_mysql_unescape(tokens[index][1]))
                    except ValueError:
                        return self._issue("invalid JSON", line)
                fields.append(text)
            elif hex_marker:
                field_value = self._hex_field(kind, hex_digits, line)
                if field_value is None:
                    return None
                fields.append(field_value)
            else:
                return self._issue("unrecognized value", line)

        self._track(tokens)
        return "\t".join(fields) + "\n"

    def _hex_field(self, kind: str, digits: str, line: int) -> str | None:
        """Convert a hexadecimal literal for bytea or geometry columns.

        Args:
            kind (str): Column kind.
            digits (str): Hexadecimal digits without the `0x` prefix.
            line (int): Statement line for issue locations.

        Returns:
            str | None: COPY field, or None after recording an issue.
        """
        if kind == "bytea":
            return "\\\\x" + digits
        if kind != "geometry":
            return self._issue(f"hexadecimal value in {kind} column", line)
        try:
            ewkb, srid, x, y = mysql_geometry_to_ewkb(bytes.fromhex(digits))
        except (MigrationError, ValueError):
            return self._issue("invalid geometry", line)
        self.stats.srids[srid] += 1
        bounds = self.stats.bounds
        self.stats.bounds = (
            [min(bounds[0], x), max(bounds[1], x), min(bounds[2], y), max(bounds[3], y)]
            if bounds else [x, x, y, y]
        )
        return ewkb

    def _track(self, tokens: list[tuple[str, ...]]) -> None:
        """Update counts, control totals, retained rows and unique keys.

        Args:
            tokens (list[tuple[str, ...]]): Tokens of a converted row.

        Returns:
            None.
        """
        stats = self.stats
        stats.rows += 1
        if stats.first_column_sum is not None:
            first = tokens[0][5]
            if first and first.lstrip("-+").isdigit():
                stats.first_column_sum += int(first)
            elif not tokens[0][2]:
                stats.first_column_sum = None
        if self.keep_rows is not None:
            self.keep_rows.append(tuple(_decoded(token) for token in tokens))
        if self.unique_key:
            positions, reportable = self.unique_key
            values = [_decoded(tokens[position]) for position in positions]
            if any(value is None for value in values):
                return
            digest = hashlib.sha256("\x1f".join(values).encode("utf-8")).digest()
            row_id = _decoded(tokens[0]) or ""
            first_id = self.seen_keys.setdefault(digest, row_id)
            if first_id != row_id:
                stats.duplicates += 1
                if len(stats.duplicate_samples) < k.REPORT_SAMPLE_LIMIT:
                    sample: dict[str, object] = {"id": row_id, "first_id": first_id}
                    if reportable:
                        sample["value"] = values
                    stats.duplicate_samples.append(sample)

    def _issue(self, reason: str, line: int) -> None:
        """Record one conversion problem without exposing the row value.

        Args:
            reason (str): Problem category.
            line (int): Statement line.

        Returns:
            None: The caller skips the row.
        """
        self.stats.issues[reason] += 1
        if len(self.stats.issue_samples) < k.REPORT_SAMPLE_LIMIT:
            self.stats.issue_samples.append(f"{reason} at INSERT line {line}")
        return None


def _decoded(token: tuple[str, ...]) -> str | None:
    """Return the textual value of one token, or None for NULL.

    Args:
        token (tuple[str, ...]): Token groups from `_TOKEN_RE`.

    Returns:
        str | None: Decoded string, number text, `0x`-prefixed hex or None.
    """
    quote, text, null, hex_marker, hex_digits, number = token
    if null:
        return None
    if quote:
        return _mysql_unescape(text)
    if hex_marker:
        return "0x" + hex_digits
    return number


def plan_table(table: DumpTable) -> str | None:
    """Decide whether a dumped table belongs to the migration.

    Args:
        table (DumpTable): Dumped table structure.

    Returns:
        str | None: None for migrated tables; otherwise the reason for skipping.

    Raises:
        MigrationError: If a migrated table has a column type without conversion.
    """
    if table.database not in k.TARGET_SCHEMAS:
        return "database outside the RF.Fusion schemas"
    if table.database == k.SUMMARY_SCHEMA and table.name not in k.SUMMARY_RESTORED_TABLES:
        return "Summary table rebuilt by full reconcile"
    unsupported = [f"{column} ({kind})" for column, kind in zip(table.columns, table.kinds)
                   if kind.startswith("unsupported")]
    if unsupported:
        raise MigrationError(f"{table.database}.{table.name}: unsupported columns {unsupported}")
    return None


def verify_checksum(path: Path) -> str:
    """Compare the dump with its `.sha256` sidecar file when present.

    Args:
        path (Path): Dump path.

    Returns:
        str: `verified` or `no sidecar`.

    Raises:
        MigrationError: If the checksum differs.
    """
    sidecar = path.with_name(path.name + ".sha256")
    if not sidecar.exists():
        return "no sidecar"
    expected = sidecar.read_text(encoding="utf-8").split()[0].lower()
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise MigrationError("dump SHA-256 differs from its sidecar file")
    return "verified"


def _stats_report(stats: TableStats) -> dict[str, object]:
    """Serialize table statistics for the JSON report.

    Args:
        stats (TableStats): Table statistics.

    Returns:
        dict[str, object]: Rows, controls, issues, geometry and duplicate data.
    """
    report: dict[str, object] = {"rows": stats.rows, "seconds": round(stats.seconds, 1)}
    if stats.first_column_sum is not None:
        report["first_column_sum"] = str(stats.first_column_sum)
    if stats.issues:
        report["issues"] = dict(stats.issues)
        report["issue_samples"] = stats.issue_samples
    if stats.srids:
        report["srids"] = {str(srid): count for srid, count in stats.srids.items()}
        report["bounds_x_min_max_y_min_max"] = stats.bounds
    if stats.duplicates:
        report["postgres_unique_duplicates"] = stats.duplicates
        report["duplicate_samples"] = stats.duplicate_samples
    return report


def inspect_dump(path: Path) -> dict[str, object]:
    """Convert every migrated row without writing and report blocking problems.

    Args:
        path (Path): Gzip dump.

    Returns:
        dict[str, object]: Report with `checksum`, `tables`, `skipped` and
            `blocking` (bool) keys. Tables are keyed by `SCHEMA.TABLE`.
    """
    started = time.monotonic()
    report: dict[str, object] = {"mode": "inspect", "dump": str(path), "checksum": verify_checksum(path)}
    converters: dict[tuple[str, str], RowConverter] = {}
    skipped: dict[str, dict[str, object]] = {}
    for item in iter_dump(path):
        if isinstance(item, DumpTable):
            reason = plan_table(item)
            key = (item.database, item.name)
            if reason:
                skipped[f"{item.database}.{item.name}"] = {"reason": reason, "insert_statements": 0}
            else:
                converters[key] = RowConverter(
                    item, validate_json=True, unique_key=k.POSTGRES_ONLY_UNIQUE_KEYS.get(key), keep_rows=False,
                )
            continue
        converter = converters.get((item.database, item.table))
        if converter is None:
            # Skipped data is not parsed; system databases use literals outside this tool.
            entry = skipped.setdefault(f"{item.database}.{item.table}",
                                       {"reason": "no table structure", "insert_statements": 0})
            entry["insert_statements"] += 1
            continue
        _require_dump_columns(converter.table, item)
        table_started = time.monotonic()
        for tokens in iter_rows(item):
            converter.convert(tokens, item.line)
        converter.stats.seconds += time.monotonic() - table_started
        _log_progress(converter)

    tables = {f"{schema}.{name}": _stats_report(c.stats) for (schema, name), c in converters.items()}
    report.update(
        tables=tables,
        skipped=skipped,
        blocking=any(c.stats.issues or c.stats.duplicates for c in converters.values()),
        seconds=round(time.monotonic() - started, 1),
    )
    return report


def _require_dump_columns(table: DumpTable, statement: InsertStatement) -> None:
    """Reject INSERT statements whose column list differs from the table.

    Args:
        table (DumpTable): Dumped table structure.
        statement (InsertStatement): INSERT statement for that table.

    Returns:
        None.

    Raises:
        MigrationError: If an explicit column list does not match the table order.
    """
    if statement.columns is not None and statement.columns != table.columns:
        raise MigrationError(f"line {statement.line}: column list differs from {table.name}")


def _log_progress(converter: RowConverter) -> None:
    """Log progress when a table crosses another progress interval.

    Args:
        converter (RowConverter): Table converter.

    Returns:
        None.
    """
    stats = converter.stats
    if stats.rows // k.PROGRESS_ROWS > converter.logged_intervals:
        converter.logged_intervals = stats.rows // k.PROGRESS_ROWS
        LOGGER.info("%s.%s: %d rows in %.0fs", converter.table.database, converter.table.name,
                    stats.rows, stats.seconds)


def _target_kind(pg_type: str) -> str:
    """Map a PostgreSQL column type to the converter value kind.

    Args:
        pg_type (str): Output of `format_type`.

    Returns:
        str: number, text, temporal, json, bytea, geometry or `unknown`.
    """
    if pg_type.startswith("geometry"):
        return "geometry"
    if pg_type == "bytea":
        return "bytea"
    if pg_type in ("json", "jsonb"):
        return "json"
    if pg_type.startswith(("timestamp", "date", "time")):
        return "temporal"
    if pg_type.startswith(("integer", "smallint", "bigint", "numeric", "real", "double")):
        return "number"
    if pg_type.startswith(("character", "text")):
        return "text"
    return "unknown"


class ResourceGuard:
    """Stop the load before the database or its filesystem exceeds set limits.

    Attributes:
        connection (psycopg.Connection): Autocommit session used only for probes.
        max_database_bytes (int | None): Largest accepted database size.
        disk_path (Path | None): Local path on the filesystem of the PostgreSQL data.
        min_free_bytes (int | None): Smallest accepted free space on `disk_path`.
        peak_database_bytes (int): Largest observed database size.
        min_observed_free_bytes (int | None): Smallest observed free space.
    """

    def __init__(
        self,
        conninfo: str,
        max_database_gb: float | None,
        disk_path: Path | None,
        min_free_gb: float | None,
    ) -> None:
        """Open the probe session.

        Args:
            conninfo (str): Target database connection string.
            max_database_gb (float | None): Database size limit in GiB.
            disk_path (Path | None): Path whose filesystem is checked.
            min_free_gb (float | None): Free space limit in GiB.
        """
        self.connection = psycopg.connect(conninfo, autocommit=True)
        self.max_database_bytes = int(max_database_gb * 1024 ** 3) if max_database_gb else None
        self.disk_path = disk_path
        self.min_free_bytes = int(min_free_gb * 1024 ** 3) if min_free_gb else None
        self.peak_database_bytes = 0
        self.min_observed_free_bytes: int | None = None

    def check(self) -> None:
        """Measure the database and filesystem and enforce the limits.

        Returns:
            None.

        Raises:
            MigrationError: If a limit is exceeded.
        """
        size = self.connection.execute("SELECT pg_database_size(current_database())").fetchone()[0]
        self.peak_database_bytes = max(self.peak_database_bytes, size)
        if self.max_database_bytes and size > self.max_database_bytes:
            raise MigrationError(f"database reached {size / 1024 ** 3:.2f} GiB, above the limit")
        if self.disk_path and self.min_free_bytes:
            free = shutil.disk_usage(self.disk_path).free
            self.min_observed_free_bytes = min(free, self.min_observed_free_bytes or free)
            if free < self.min_free_bytes:
                raise MigrationError(f"only {free / 1024 ** 3:.2f} GiB free on {self.disk_path}")

    def close(self) -> None:
        """Close the probe session.

        Returns:
            None.
        """
        self.connection.close()


def _prepare_database(admin_conninfo: str, conninfo: str, database: str, create: bool) -> None:
    """Create the target database if requested and apply the bootstrap DDL.

    Args:
        admin_conninfo (str): Connection used to create the database.
        conninfo (str): Connection to the target database.
        database (str): Target database name.
        create (bool): Create the database; it must not exist yet.

    Returns:
        None. Each DDL script commits independently, like `psql --single-transaction`.

    Raises:
        MigrationError: If the database state does not allow a fresh migration.
    """
    with psycopg.connect(admin_conninfo, autocommit=True) as admin:
        exists = admin.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,)).fetchone()
        if create and exists:
            raise MigrationError(f"database {database} already exists")
        if not create and not exists:
            raise MigrationError(f"database {database} does not exist; use --create-database")
        if create:
            admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'").format(sql.Identifier(database)))
            LOGGER.info("created database %s", database)

    with psycopg.connect(conninfo) as connection:
        existing = connection.execute(
            "SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s)", (list(k.TARGET_SCHEMAS),),
        ).fetchall()
        if existing:
            raise MigrationError(f"schemas already exist: {sorted(row[0] for row in existing)}")
        for schema, script in k.DDL_SCRIPTS:
            _apply_ddl(connection, k.DDL_DIR / script)
            connection.commit()
            LOGGER.info("applied %s for %s", script, schema)
        connection.execute("RESET search_path")
        connection.commit()


def _apply_ddl(connection: psycopg.Connection, script: Path) -> None:
    """Run one bootstrap script, translating `psql` `\\copy` lines to COPY.

    Args:
        connection (psycopg.Connection): Target session inside a transaction.
        script (Path): DDL script from `src/postgres/scripts`.

    Returns:
        None. The caller commits.

    Raises:
        MigrationError: If the script uses another `psql` meta-command.
    """
    pending: list[str] = []
    for line in script.read_text(encoding="utf-8").splitlines(keepends=True):
        if not line.startswith("\\"):
            pending.append(line)
            continue
        match = _COPY_COMMAND_RE.match(line.strip())
        if not match:
            raise MigrationError(f"{script.name}: unsupported psql command")
        if "".join(pending).strip():
            connection.execute("".join(pending))
        pending = []
        # The bootstrap CSV files live next to the DDL scripts.
        source = k.DDL_DIR / Path(match.group(2)).name
        with connection.cursor().copy(f"COPY {match.group(1)} FROM STDIN WITH {match.group(3)}") as copy:
            copy.write(source.read_bytes())
    if "".join(pending).strip():
        connection.execute("".join(pending))


def _read_catalog(connection: psycopg.Connection) -> dict[tuple[str, str], dict[str, object]]:
    """Read target columns, identity columns and row presence per table.

    Args:
        connection (psycopg.Connection): Target session.

    Returns:
        dict[tuple[str, str], dict[str, object]]: Per table, `columns` maps names
            to PostgreSQL types and `identities` lists identity columns.
    """
    catalog: dict[tuple[str, str], dict[str, object]] = {}
    rows = connection.execute(
        """
        SELECT n.nspname, c.relname, a.attname, format_type(a.atttypid, a.atttypmod), a.attidentity <> ''
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        WHERE c.relkind = 'r' AND n.nspname = ANY(%s)
        ORDER BY n.nspname, c.relname, a.attnum
        """,
        (list(k.TARGET_SCHEMAS),),
    ).fetchall()
    for schema, table, column, pg_type, identity in rows:
        entry = catalog.setdefault((schema, table), {"columns": {}, "identities": []})
        entry["columns"][column] = pg_type
        if identity:
            entry["identities"].append(column)
    return catalog


def _drop_foreign_keys(connection: psycopg.Connection) -> list[tuple[str, str, str, str]]:
    """Drop every foreign key of the RF.Fusion schemas inside the load transaction.

    Args:
        connection (psycopg.Connection): Target session inside the load transaction.

    Returns:
        list[tuple[str, str, str, str]]: Schema, table, constraint name and
            definition of each dropped foreign key.
    """
    keys = connection.execute(
        """
        SELECT n.nspname, c.relname, con.conname, pg_get_constraintdef(con.oid)
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE con.contype = 'f' AND n.nspname = ANY(%s)
        ORDER BY n.nspname, c.relname, con.conname
        """,
        (list(k.TARGET_SCHEMAS),),
    ).fetchall()
    for schema, table, name, _ in keys:
        connection.execute(sql.SQL("ALTER TABLE {}.{} DROP CONSTRAINT {}").format(
            sql.Identifier(schema), sql.Identifier(table), sql.Identifier(name)))
    return keys


def _check_target_columns(table: DumpTable, catalog: dict[tuple[str, str], dict[str, object]]) -> list[str]:
    """Check that each dumped column exists in PostgreSQL with a compatible kind.

    Args:
        table (DumpTable): Dumped table structure.
        catalog (dict[tuple[str, str], dict[str, object]]): Output of `_read_catalog`.

    Returns:
        list[str]: Target columns absent from the dump; they receive defaults.

    Raises:
        MigrationError: If the table or a column is missing or incompatible.
    """
    entry = catalog.get((table.database, table.name))
    if entry is None:
        raise MigrationError(f"{table.database}.{table.name} does not exist in PostgreSQL")
    target_columns: dict[str, str] = entry["columns"]
    for column, kind in zip(table.columns, table.kinds):
        if column not in target_columns:
            raise MigrationError(f"{table.database}.{table.name}.{column} does not exist in PostgreSQL")
        target_kind = _target_kind(target_columns[column])
        if target_kind != kind:
            raise MigrationError(
                f"{table.database}.{table.name}.{column}: dump kind {kind} differs from {target_columns[column]}"
            )
    return [column for column in target_columns if column not in table.columns]


def _read_seed_rows(connection: psycopg.Connection, table: DumpTable) -> list[tuple[str | None, ...]]:
    """Read bootstrap rows of one table as text, in dump column order.

    Args:
        connection (psycopg.Connection): Target session.
        table (DumpTable): Dumped table structure.

    Returns:
        list[tuple[str | None, ...]]: Seed rows; empty when the DDL inserted none.
    """
    columns = sql.SQL(", ").join(sql.SQL("{}::text").format(sql.Identifier(column)) for column in table.columns)
    return connection.execute(sql.SQL("SELECT {} FROM {}.{}").format(
        columns, sql.Identifier(table.database), sql.Identifier(table.name))).fetchall()


def _compare_seeds(seed_rows: list[tuple[str | None, ...]], dump_rows: list[tuple[str | None, ...]]) -> dict[str, int]:
    """Compare bootstrap seeds with dump rows by their first column.

    Args:
        seed_rows (list[tuple[str | None, ...]]): Rows inserted by the DDL.
        dump_rows (list[tuple[str | None, ...]]): Decoded dump rows.

    Returns:
        dict[str, int]: Counts of `identical`, `changed`, `only_seed` and `only_dump` IDs.
    """
    seeds = {row[0]: row for row in seed_rows}
    dumped = {row[0]: row for row in dump_rows}
    return {
        "identical": sum(1 for key in seeds.keys() & dumped.keys() if seeds[key] == dumped[key]),
        "changed": sum(1 for key in seeds.keys() & dumped.keys() if seeds[key] != dumped[key]),
        "only_seed": len(seeds.keys() - dumped.keys()),
        "only_dump": len(dumped.keys() - seeds.keys()),
    }


def load_dump(
    path: Path,
    base_conninfo: str,
    database: str,
    *,
    create_database: bool,
    max_database_gb: float | None,
    disk_guard_path: Path | None,
    min_free_gb: float | None,
    report: dict[str, object],
) -> None:
    """Load the dump into a fresh RF.Fusion PostgreSQL database.

    Args:
        path (Path): Gzip dump.
        base_conninfo (str): Connection string; its database is used to create the target.
        database (str): Target database name.
        create_database (bool): Create the database before applying the DDL.
        max_database_gb (float | None): Abort above this database size.
        disk_guard_path (Path | None): Local path on the PostgreSQL data filesystem.
        min_free_gb (float | None): Abort below this free space on `disk_guard_path`.
        report (dict[str, object]): Report filled in place, also on failure, with
            tables, skipped tables, seeds, defaulted columns, sequences,
            durations and observed sizes.

    Returns:
        None.

    Raises:
        MigrationError: If the target, conversion or validation fails. The load
            transaction is rolled back; the created database and DDL remain.
    """
    started = time.monotonic()
    report.update(mode="load", dump=str(path), database=database, checksum=verify_checksum(path))
    conninfo = make_conninfo(base_conninfo, dbname=database)
    _prepare_database(base_conninfo, conninfo, database, create_database)

    guard = ResourceGuard(conninfo, max_database_gb, disk_guard_path, min_free_gb)
    converters: dict[tuple[str, str], RowConverter] = {}
    copy_sql: dict[tuple[str, str], sql.Composed] = {}
    seeds: dict[tuple[str, str], list[tuple[str | None, ...]]] = {}
    skipped: dict[str, str] = {}
    defaulted: dict[str, list[str]] = {}
    try:
        with psycopg.connect(conninfo) as connection:
            connection.execute("SET statement_timeout = 0")
            catalog = _read_catalog(connection)
            foreign_keys = _drop_foreign_keys(connection)
            rows_since_check = 0
            for item in iter_dump(path):
                if isinstance(item, DumpTable):
                    reason = plan_table(item)
                    if reason:
                        skipped[f"{item.database}.{item.name}"] = reason
                        continue
                    key = (item.database, item.name)
                    missing = _check_target_columns(item, catalog)
                    if missing:
                        defaulted[f"{item.database}.{item.name}"] = missing
                    seed_rows = _read_seed_rows(connection, item)
                    if seed_rows:
                        # Dump IDs are referenced by the dumped facts, so they replace the seeds.
                        seeds[key] = seed_rows
                        connection.execute(sql.SQL("TRUNCATE {}.{}").format(
                            sql.Identifier(item.database), sql.Identifier(item.name)))
                    converters[key] = RowConverter(item, validate_json=False, unique_key=None,
                                                   keep_rows=bool(seed_rows))
                    copy_sql[key] = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
                        sql.Identifier(item.database), sql.Identifier(item.name),
                        sql.SQL(", ").join(sql.Identifier(column) for column in item.columns))
                    continue

                key = (item.database, item.table)
                converter = converters.get(key)
                if converter is None:
                    continue
                _require_dump_columns(converter.table, item)
                table_started = time.monotonic()
                buffer: list[str] = []
                buffered = 0
                with connection.cursor().copy(copy_sql[key]) as copy:
                    for tokens in iter_rows(item):
                        line = converter.convert(tokens, item.line)
                        if line is None:
                            raise MigrationError(f"{item.database}.{item.table}: {converter.stats.issue_samples[-1]}")
                        buffer.append(line)
                        buffered += len(line)
                        rows_since_check += 1
                        if buffered >= k.COPY_BUFFER_CHARS:
                            copy.write("".join(buffer))
                            buffer, buffered = [], 0
                        if rows_since_check >= k.GUARD_CHECK_ROWS:
                            guard.check()
                            rows_since_check = 0
                    if buffer:
                        copy.write("".join(buffer))
                converter.stats.seconds += time.monotonic() - table_started
                _log_progress(converter)

            step = time.monotonic()
            for schema, table, name, definition in foreign_keys:
                connection.execute(sql.SQL("ALTER TABLE {}.{} ADD CONSTRAINT {} {}").format(
                    sql.Identifier(schema), sql.Identifier(table), sql.Identifier(name), sql.SQL(definition)))
            report["foreign_keys"] = {"recreated": len(foreign_keys), "seconds": round(time.monotonic() - step, 1)}
            LOGGER.info("recreated %d foreign keys", len(foreign_keys))
            guard.check()

            report["sequences"] = _sync_sequences(connection, converters, catalog)
            _validate_counts(connection, converters)
            connection.commit()
            LOGGER.info("load committed")

        with psycopg.connect(conninfo, autocommit=True) as maintenance:
            step = time.monotonic()
            for schema, table in converters:
                maintenance.execute(sql.SQL("ANALYZE {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))
            report["analyze_seconds"] = round(time.monotonic() - step, 1)
            report["database_bytes"] = maintenance.execute(
                "SELECT pg_database_size(current_database())").fetchone()[0]
    finally:
        guard.close()
        report.update(
            peak_database_bytes=guard.peak_database_bytes,
            min_free_bytes_observed=guard.min_observed_free_bytes,
            tables={f"{schema}.{name}": _stats_report(c.stats) for (schema, name), c in converters.items()},
            seeds={f"{schema}.{name}": _compare_seeds(rows, converters[(schema, name)].keep_rows or [])
                   for (schema, name), rows in seeds.items()},
            defaulted_columns=defaulted,
            skipped=skipped,
            seconds=round(time.monotonic() - started, 1),
        )


def _sync_sequences(
    connection: psycopg.Connection,
    converters: dict[tuple[str, str], RowConverter],
    catalog: dict[tuple[str, str], dict[str, object]],
) -> dict[str, int]:
    """Continue each identity after the loaded IDs and the MariaDB counter.

    Args:
        connection (psycopg.Connection): Target session inside the load transaction.
        converters (dict[tuple[str, str], RowConverter]): Loaded tables.
        catalog (dict[tuple[str, str], dict[str, object]]): Target catalog.

    Returns:
        dict[str, int]: Last value assigned to each identity, keyed by
            `SCHEMA.TABLE.COLUMN`; 0 means the next value is 1.
    """
    result: dict[str, int] = {}
    for (schema, table), converter in converters.items():
        for column in catalog[(schema, table)]["identities"]:
            maximum = connection.execute(sql.SQL("SELECT COALESCE(MAX({}), 0) FROM {}.{}").format(
                sql.Identifier(column), sql.Identifier(schema), sql.Identifier(table))).fetchone()[0]
            last = max(int(maximum), (converter.table.auto_increment or 1) - 1)
            sequence = connection.execute("SELECT pg_get_serial_sequence(%s, %s)", (
                sql.Identifier(schema, table).as_string(connection), column)).fetchone()[0]
            if last > 0:
                connection.execute("SELECT setval(%s::regclass, %s, true)", (sequence, last))
            else:
                connection.execute("SELECT setval(%s::regclass, 1, false)", (sequence,))
            result[f"{schema}.{table}.{column}"] = last
    return result


def _validate_counts(connection: psycopg.Connection, converters: dict[tuple[str, str], RowConverter]) -> None:
    """Compare loaded row counts and first-column sums with the dump.

    Args:
        connection (psycopg.Connection): Target session inside the load transaction.
        converters (dict[tuple[str, str], RowConverter]): Loaded tables.

    Returns:
        None.

    Raises:
        MigrationError: If any control total differs.
    """
    for (schema, table), converter in converters.items():
        stats = converter.stats
        total_sql = (sql.SQL("SUM({})::text").format(sql.Identifier(converter.table.columns[0]))
                     if stats.first_column_sum is not None else sql.SQL("NULL"))
        count, total = connection.execute(sql.SQL("SELECT count(*), {} FROM {}.{}").format(
            total_sql, sql.Identifier(schema), sql.Identifier(table))).fetchone()
        if count != stats.rows:
            raise MigrationError(f"{schema}.{table}: {count} rows loaded, {stats.rows} in dump")
        if stats.first_column_sum is not None and stats.rows and total != str(stats.first_column_sum):
            raise MigrationError(f"{schema}.{table}: first-column sum differs from the dump")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    """Parse command-line options.

    Args:
        argv (list[str]): Arguments without the program name.

    Returns:
        argparse.Namespace: Parsed options.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect", help="convert all rows without writing")
    inspect.add_argument("dump", type=Path)
    inspect.add_argument("--report", type=Path)
    load = commands.add_parser("load", help="load into a database without RF.Fusion schemas")
    load.add_argument("dump", type=Path)
    load.add_argument("--database", required=True)
    load.add_argument("--create-database", action="store_true")
    load.add_argument("--max-database-gb", type=float)
    load.add_argument("--disk-guard-path", type=Path)
    load.add_argument("--min-free-gb", type=float)
    load.add_argument("--report", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the requested migration command.

    Args:
        argv (list[str] | None): Arguments without the program name.

    Returns:
        int: 0 on success, 1 on failure and 2 when inspection finds blocking problems.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    report: dict[str, object] = {}
    status = 0
    try:
        if args.command == "inspect":
            report = inspect_dump(args.dump)
            status = 2 if report["blocking"] else 0
        else:
            if bool(args.disk_guard_path) != bool(args.min_free_gb):
                raise MigrationError("--disk-guard-path and --min-free-gb must be used together")
            base_conninfo = os.environ.get(k.CONNINFO_ENV)
            if not base_conninfo:
                raise MigrationError(f"set {k.CONNINFO_ENV} with the PostgreSQL destination")
            conninfo_to_dict(base_conninfo)
            load_dump(
                args.dump, base_conninfo, args.database,
                create_database=args.create_database,
                max_database_gb=args.max_database_gb,
                disk_guard_path=args.disk_guard_path,
                min_free_gb=args.min_free_gb,
                report=report,
            )
    except (MigrationError, psycopg.Error, OSError, UnicodeDecodeError) as error:
        LOGGER.error("migration failed: %s", error)
        report["error"] = str(error)
        status = 1

    output = json.dumps(report, indent=2, ensure_ascii=False)
    if args.report:
        args.report.write_text(output + "\n", encoding="utf-8")
        LOGGER.info("report written to %s", args.report)
    else:
        print(output)
    return status


if __name__ == "__main__":
    sys.exit(main())
