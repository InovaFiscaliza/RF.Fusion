"""Own the read-only SQL ported from the external MATLAB DBHandler.m.

Keep query differences from the WebFusion UI intentional: these methods form
the compatibility boundary for the external client, including empty fallbacks.
"""

from dataclasses import dataclass, replace

from pymysql.cursors import SSDictCursor

from db import get_connection_bpdata, get_connection_rfdata, get_connection_summary

from . import config as k
from .filters import Filters


LOCALITY_SQL = """COALESCE(NULLIF(s.NA_SITE, ''), NULLIF(d.NA_DISTRICT, ''),
    c.NA_COUNTY, CONCAT('Site ', s.ID_SITE))"""
FILE_JOINS = """FROM RFDATA.FACT_SPECTRUM f
JOIN RFDATA.BRIDGE_SPECTRUM_FILE b ON b.FK_SPECTRUM = f.ID_SPECTRUM
JOIN RFDATA.DIM_SPECTRUM_FILE repos ON repos.ID_FILE = b.FK_FILE
"""
GEO_JOINS = """JOIN RFDATA.DIM_SPECTRUM_SITE s ON s.ID_SITE = f.FK_SITE
LEFT JOIN RFDATA.DIM_SITE_DISTRICT d ON d.ID_DISTRICT = s.FK_DISTRICT
LEFT JOIN RFDATA.DIM_SITE_COUNTY c ON c.ID_COUNTY = s.FK_COUNTY
LEFT JOIN RFDATA.DIM_SITE_STATE st ON st.ID_STATE = s.FK_STATE
"""
EQUIPMENT_JOIN = "JOIN RFDATA.DIM_SPECTRUM_EQUIPMENT e ON e.ID_EQUIPMENT = f.FK_EQUIPMENT"


@dataclass
class TableResult:
    """Carry query rows and column order, including empty result sets.

    Attributes:
        columns: Ordered public column names (list[str]).
        rows: Rows keyed by every column name (list[dict[str, object]]).
            Values are native database scalars, including None for SQL NULL.
    """

    columns: list[str]
    rows: list[dict[str, object]]


def _conditions(filters: Filters, fields: dict[str, str]) -> tuple[str, list[object]]:
    """Build equality/IN clauses from server-owned column mappings.

    Args:
        filters: Validated filter values (Filters).
        fields: Filter attribute to SQL column mapping (dict[str, str]); keys
            are equipment_id, site_id, state_code and/or district_ids.

    Returns:
        SQL suffix and ordered bound values (tuple[str, list[object]]).
    """
    clauses = []
    params = []
    for name, column in fields.items():
        value = getattr(filters, name)
        if value is None or value == ():
            continue
        if name == "district_ids":
            placeholders = ", ".join(["%s"] * len(value))
            clauses.append(f"{column} IN ({placeholders})")
            params.extend(value)
        else:
            clauses.append(f"{column} = %s")
            params.append(value)
    return "".join(f" AND {clause}" for clause in clauses), params


def _spectrum_conditions(filters: Filters) -> tuple[str, list[object]]:
    """Preserve MATLAB overlap, containment and description matching rules.

    Args:
        filters: Validated spectrum search filters (Filters).

    Returns:
        AND-prefixed SQL and ordered bound values (tuple[str, list[object]]).
        Requires fact alias f, site alias s when district_ids are supplied,
        and state alias st when state_code is supplied.
    """
    sql, params = _conditions(filters, {
        "equipment_id": "f.FK_EQUIPMENT", "site_id": "f.FK_SITE",
        "state_code": "st.LC_STATE", "district_ids": "s.FK_DISTRICT",
    })
    for value, expression in (
        (filters.start_date, "f.DT_TIME_END >= %s"),
        (filters.end_date, "f.DT_TIME_START <= %s"),
    ):
        if value is not None:
            sql += f" AND {expression}"
            params.append(value)
    for value, expression in (
        (filters.freq_start, "f.NU_FREQ_START >= %s"),
        (filters.freq_end, "f.NU_FREQ_END <= %s"),
    ):
        # Negative bounds mean absent in MATLAB, but still select the fact path.
        if value is not None and value >= 0:
            sql += f" AND {expression}"
            params.append(value)
    if filters.description:
        pattern = filters.description
        if "%" not in pattern and "_" not in pattern:
            pattern = f"%{pattern}%"
        sql += " AND UPPER(COALESCE(f.NA_DESCRIPTION, '')) LIKE UPPER(%s)"
        params.append(pattern)
    return sql, params


class AppAnaliseDB:
    """Read appAnalise datasets through existing WebFusion connection factories.

    Instances are stateless. Every query opens and closes its own connection;
    no connection or cursor is shared between HTTP requests. Only named public
    methods are called by the HTTP and service layers.
    """

    def _fetch(self, schema: str, sql: str, params: list[object] | None = None) -> TableResult:
        """Execute internal parameterized SQL and always release resources.

        Args:
            schema: Server-owned schema key from config (str).
            sql: Server-owned SELECT statement (str).
            params: Ordered driver-bound scalar values (list[object] | None).

        Returns:
            Column names and dictionary rows (TableResult).

        Raises:
            Exception: Database failures propagate; empty data never hides errors.
        """
        factories = {
            k.SUMMARY_SCHEMA: get_connection_summary,
            k.SPECTRUM_SCHEMA: get_connection_rfdata,
            k.HOST_SCHEMA: get_connection_bpdata,
        }
        connection = factories[schema]()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params or [])
                columns = [column[0] for column in cursor.description]
                return TableResult(columns, list(cursor.fetchall()))
        finally:
            connection.close()

    def get_station_summary(self) -> TableResult:
        """Read located stations in the original MATLAB ordering.

        Args:
            None.

        Returns:
            HOST_LOCATION_SUMMARY columns and rows (TableResult).
        """
        return self._fetch(k.SUMMARY_SCHEMA, """
            SELECT * FROM HOST_LOCATION_SUMMARY
            WHERE VL_LATITUDE IS NOT NULL AND VL_LONGITUDE IS NOT NULL
            ORDER BY IS_CURRENT_LOCATION DESC, IS_OFFLINE_SNAPSHOT ASC,
                DT_LAST_SEEN_AT DESC, NA_STATE_CODE ASC,
                NA_LOCALITY_LABEL ASC, NA_HOST_NAME ASC
        """)

    def get_host_stats(self, host_id: int) -> TableResult:
        """Reconstruct removed HOST counters from their original history source.

        Args:
            host_id: Selected host identifier (int).

        Returns:
            Host identity, connectivity and backup statistics (TableResult).
            An unknown host returns no rows, preserving fetch semantics.
            Counts use the legacy status predicates; pending volume remains KB.
        """
        # HOST no longer stores these counters. Rounded summary GB cannot recover exact KB.
        return self._fetch(k.HOST_SCHEMA, """
            SELECT h.ID_HOST, h.NA_HOST_NAME, h.NA_HOST_ADDRESS, h.NA_HOST_PORT,
                h.IS_OFFLINE, h.IS_BUSY, h.DT_LAST_CHECK, h.DT_LAST_DISCOVERY,
                h.DT_LAST_BACKUP, h.DT_LAST_PROCESSING,
                COALESCE(stats.NU_HOST_FILES, 0) AS NU_HOST_FILES,
                COALESCE(stats.NU_PENDING_FILE_BACKUP_TASKS, 0) AS NU_PENDING_FILE_BACKUP_TASKS,
                COALESCE(stats.NU_DONE_FILE_BACKUP_TASKS, 0) AS NU_DONE_FILE_BACKUP_TASKS,
                COALESCE(stats.VL_PENDING_BACKUP_KB, 0) AS VL_PENDING_BACKUP_KB
            FROM BPDATA.HOST h
            LEFT JOIN (
                SELECT FK_HOST,
                    SUM(NU_STATUS_DISCOVERY = %s) AS NU_HOST_FILES,
                    SUM(NU_STATUS_BACKUP = %s) AS NU_PENDING_FILE_BACKUP_TASKS,
                    SUM(NU_STATUS_BACKUP = %s) AS NU_DONE_FILE_BACKUP_TASKS,
                    SUM(CASE WHEN NU_STATUS_DISCOVERY = %s AND NU_STATUS_BACKUP = %s
                        THEN VL_FILE_SIZE_KB_HOST ELSE 0 END) AS VL_PENDING_BACKUP_KB
                FROM BPDATA.FILE_TASK_HISTORY WHERE FK_HOST = %s GROUP BY FK_HOST
            ) stats ON stats.FK_HOST = h.ID_HOST
            WHERE h.ID_HOST = %s
        """, [k.TASK_DONE, k.TASK_PENDING, k.TASK_DONE, k.TASK_DONE,
              k.TASK_PENDING, host_id, host_id])

    def get_spectrum_equipments(self, filters: Filters) -> TableResult:
        """Read equipment options with the original empty-summary fallback.

        Args:
            filters: Query values (Filters). Uses state_code, site_id and
                district_ids; the legacy fact fallback ignores district_ids.

        Returns:
            ID_EQUIPMENT and NA_EQUIPMENT columns and rows (TableResult).
        """
        where, params = _conditions(filters, {
            "state_code": "NA_STATE_CODE", "site_id": "FK_SITE",
            "district_ids": "FK_DISTRICT",
        })
        result = self._fetch(k.SUMMARY_SCHEMA, f"""
            SELECT DISTINCT FK_EQUIPMENT AS ID_EQUIPMENT, NA_EQUIPMENT
            FROM SITE_EQUIPMENT_OBS_SUMMARY
            WHERE NA_EQUIPMENT IS NOT NULL AND NA_EQUIPMENT <> '' {where}
            ORDER BY NA_EQUIPMENT
        """, params)
        if result.rows:
            return result
        # Keep the legacy fallback scope; fixing its missing district filter is separate.
        where, params = _conditions(filters, {
            "state_code": "steq.LC_STATE", "site_id": "f.FK_SITE",
        })
        state_join = """
            JOIN RFDATA.DIM_SPECTRUM_SITE seq ON seq.ID_SITE = f.FK_SITE
            LEFT JOIN RFDATA.DIM_SITE_STATE steq ON steq.ID_STATE = seq.FK_STATE
        """ if filters.state_code else ""
        # Membership needs one matching spectrum, not all rows followed by DISTINCT.
        return self._fetch(k.SPECTRUM_SCHEMA, f"""
            SELECT e.ID_EQUIPMENT, e.NA_EQUIPMENT
            FROM RFDATA.DIM_SPECTRUM_EQUIPMENT e
            WHERE EXISTS (
                SELECT 1 FROM RFDATA.FACT_SPECTRUM f {state_join}
                WHERE f.FK_EQUIPMENT = e.ID_EQUIPMENT {where}
            ) ORDER BY e.NA_EQUIPMENT
        """, params)

    def get_spectrum_states(self, filters: Filters) -> TableResult:
        """Read observed states, falling back only when the summary is empty.

        Args:
            filters: Query values (Filters); uses equipment_id and site_id.

        Returns:
            LC_STATE column and rows (TableResult).
        """
        where, params = _conditions(filters, {
            "equipment_id": "FK_EQUIPMENT", "site_id": "FK_SITE",
        })
        result = self._fetch(k.SUMMARY_SCHEMA, f"""
            SELECT DISTINCT NA_STATE_CODE AS LC_STATE FROM SITE_EQUIPMENT_OBS_SUMMARY
            WHERE NA_STATE_CODE IS NOT NULL AND NA_STATE_CODE <> '' {where}
            ORDER BY NA_STATE_CODE
        """, params)
        if result.rows:
            return result
        where, params = _conditions(filters, {
            "equipment_id": "f.FK_EQUIPMENT", "site_id": "f.FK_SITE",
        })
        return self._fetch(k.SPECTRUM_SCHEMA, f"""
            SELECT DISTINCT st.LC_STATE FROM RFDATA.FACT_SPECTRUM f
            JOIN RFDATA.DIM_SPECTRUM_SITE s ON s.ID_SITE = f.FK_SITE
            LEFT JOIN RFDATA.DIM_SITE_STATE st ON st.ID_STATE = s.FK_STATE
            WHERE st.LC_STATE IS NOT NULL {where} ORDER BY st.LC_STATE
        """, params)

    def get_spectrum_localities(self, filters: Filters) -> TableResult:
        """Read district options using the MATLAB summary/fact selection rules.

        Args:
            filters: Query values (Filters). Requires equipment_id or state_code;
                summary uses only these two. Fact queries ignore site_id and
                honor district_ids, date, frequency and description filters.

        Returns:
            District labels, site/spectrum counts and availability dates
            (TableResult). Without equipment or state, returns an empty table.
        """
        if filters.equipment_id is None and filters.state_code is None:
            return TableResult(list(k.LOCALITY_COLUMNS), [])
        spectrum_filters = any(value is not None for value in (
            filters.start_date, filters.end_date, filters.freq_start,
            filters.freq_end, filters.description,
        ))
        if not spectrum_filters:
            where, params = _conditions(filters, {
                "equipment_id": "FK_EQUIPMENT", "state_code": "NA_STATE_CODE",
            })
            result = self._fetch(k.SUMMARY_SCHEMA, f"""
                SELECT sm.FK_DISTRICT AS ID_DISTRICT,
                    sm.NA_DISTRICT_NAME AS LOCALITY_LABEL,
                    sm.NA_COUNTY_NAME AS COUNTY_NAME, sm.NA_STATE_CODE AS STATE_CODE,
                    COUNT(DISTINCT sm.FK_SITE) AS SITE_COUNT,
                    SUM(sm.NU_SPECTRUM_COUNT) AS SPECTRUM_COUNT,
                    MIN(sm.DT_FIRST_SEEN_AT) AS DATE_START,
                    MAX(sm.DT_LAST_SEEN_AT) AS DATE_END
                FROM SITE_EQUIPMENT_OBS_SUMMARY sm
                WHERE sm.FK_DISTRICT IS NOT NULL {where}
                GROUP BY sm.FK_DISTRICT, sm.NA_DISTRICT_NAME,
                    sm.NA_COUNTY_NAME, sm.NA_STATE_CODE
                ORDER BY sm.NA_DISTRICT_NAME ASC, sm.NA_COUNTY_NAME ASC, sm.NA_STATE_CODE ASC
            """, params)
            if result.rows:
                return result
        where, params = _spectrum_conditions(replace(filters, site_id=None))
        return self._fetch(k.SPECTRUM_SCHEMA, f"""
            SELECT d.ID_DISTRICT, d.NA_DISTRICT AS LOCALITY_LABEL,
                c.NA_COUNTY AS COUNTY_NAME, st.LC_STATE AS STATE_CODE,
                COUNT(DISTINCT s.ID_SITE) AS SITE_COUNT, COUNT(*) AS SPECTRUM_COUNT,
                MIN(f.DT_TIME_START) AS DATE_START, MAX(f.DT_TIME_END) AS DATE_END
            FROM RFDATA.FACT_SPECTRUM f
            JOIN RFDATA.DIM_SPECTRUM_SITE s ON s.ID_SITE = f.FK_SITE
            JOIN RFDATA.DIM_SITE_DISTRICT d ON d.ID_DISTRICT = s.FK_DISTRICT
            LEFT JOIN RFDATA.DIM_SITE_COUNTY c ON c.ID_COUNTY = s.FK_COUNTY
            LEFT JOIN RFDATA.DIM_SITE_STATE st ON st.ID_STATE = s.FK_STATE
            WHERE s.FK_DISTRICT IS NOT NULL {where}
            GROUP BY d.ID_DISTRICT, d.NA_DISTRICT, c.NA_COUNTY, st.LC_STATE
            ORDER BY d.NA_DISTRICT ASC, c.NA_COUNTY ASC, st.LC_STATE ASC
        """, params)

    def get_spectrum_file_data(self, filters: Filters) -> TableResult:
        """Read an uncached page using the same index and metadata stages.

        Args:
            filters: Spectrum filters plus page and page_size (Filters).

        Returns:
            Complete metadata with up to page_size + 1 rows (TableResult).
            HTTP services reuse the index; direct callers rebuild it per call.
        """
        file_ids = self.get_spectrum_file_index(filters)
        offset = (filters.page - 1) * filters.page_size
        return self.get_spectrum_files_by_ids(file_ids[offset:offset + filters.page_size + 1])

    def get_spectrum_file_index(self, filters: Filters) -> tuple[int, ...]:
        """Select matching file IDs ordered by complete-file start and ID.

        Args:
            filters: Spectrum search values (Filters); pagination is ignored.

        Returns:
            All matching IDs in descending full-file start/ID order
            (tuple[int, ...]). Streams IDs without retaining database row dicts.

        Raises:
            Exception: Database failures propagate after closing the connection
                and its temporary table. No persistent catalog data is changed.
        """
        where, params = _spectrum_conditions(filters)
        connection = get_connection_rfdata()
        try:
            with connection.cursor(SSDictCursor) as cursor:
                # Evaluate spectrum filters once, independently of file joins.
                # The table and cursor belong exclusively to this connection.
                cursor.execute(f"""
                    CREATE TEMPORARY TABLE {k.FILE_FILTER_TEMP_TABLE}
                    (PRIMARY KEY (ID_SPECTRUM)) ENGINE=InnoDB AS
                    SELECT f.ID_SPECTRUM
                    FROM RFDATA.FACT_SPECTRUM f {EQUIPMENT_JOIN} {GEO_JOINS}
                    WHERE 1 = 1 {where}
                """, params)
                cursor.execute(f"""
                    SELECT STRAIGHT_JOIN matched_files.ID_FILE
                    FROM (
                        SELECT DISTINCT repos.ID_FILE
                        FROM {k.FILE_FILTER_TEMP_TABLE} filtered
                        JOIN RFDATA.BRIDGE_SPECTRUM_FILE bf
                            ON bf.FK_SPECTRUM = filtered.ID_SPECTRUM
                        JOIN RFDATA.DIM_SPECTRUM_FILE repos ON repos.ID_FILE = bf.FK_FILE
                        WHERE repos.NA_VOLUME = %s
                    ) matched_files
                    JOIN RFDATA.BRIDGE_SPECTRUM_FILE b ON b.FK_FILE = matched_files.ID_FILE
                    JOIN RFDATA.FACT_SPECTRUM f ON f.ID_SPECTRUM = b.FK_SPECTRUM
                    {EQUIPMENT_JOIN}
                    JOIN RFDATA.DIM_SPECTRUM_SITE s ON s.ID_SITE = f.FK_SITE
                    GROUP BY matched_files.ID_FILE
                    ORDER BY MIN(f.DT_TIME_START) DESC, matched_files.ID_FILE DESC
                """, [k.REPOSITORY_VOLUME])
                return tuple(row["ID_FILE"] for row in cursor)
        finally:
            connection.close()

    def get_spectrum_files_by_ids(self, file_ids: tuple[int, ...]) -> TableResult:
        """Read complete metadata for a bounded, already ordered file page.

        Args:
            file_ids: Ordered IDs selected by the service or direct page
                method, including the lookahead row (tuple[int, ...]).

        Returns:
            Complete metadata in the supplied index order (TableResult).
            Missing files are omitted. Empty IDs retain columns without SQL.

        Raises:
            Exception: Database failures propagate; no empty success fallback.
        """
        if not file_ids:
            return TableResult(list(k.FILE_COLUMNS), [])
        placeholders = ", ".join(["%s"] * len(file_ids))
        result = self._fetch(k.SPECTRUM_SCHEMA, f"""
            SELECT STRAIGHT_JOIN repos.ID_FILE, repos.NA_PATH, repos.NA_FILE, repos.NA_EXTENSION,
                repos.VL_FILE_SIZE_KB, MIN(f.DT_TIME_START) AS DT_TIME_START,
                MAX(f.DT_TIME_END) AS DT_TIME_END, MIN(f.NU_FREQ_START) AS NU_FREQ_START,
                MAX(f.NU_FREQ_END) AS NU_FREQ_END, COUNT(DISTINCT f.ID_SPECTRUM) AS NU_SPECTRA,
                COUNT(DISTINCT s.ID_SITE) AS LOCALITY_COUNT,
                GROUP_CONCAT(DISTINCT {LOCALITY_SQL} ORDER BY {LOCALITY_SQL}
                    SEPARATOR ' | ') AS LOCALITY_LABELS,
                GROUP_CONCAT(DISTINCT e.NA_EQUIPMENT ORDER BY e.NA_EQUIPMENT
                    SEPARATOR ' | ') AS EQUIPMENT_LABELS
            FROM RFDATA.DIM_SPECTRUM_FILE repos
            JOIN RFDATA.BRIDGE_SPECTRUM_FILE b ON b.FK_FILE = repos.ID_FILE
            JOIN RFDATA.FACT_SPECTRUM f ON f.ID_SPECTRUM = b.FK_SPECTRUM
            {EQUIPMENT_JOIN} {GEO_JOINS}
            WHERE repos.ID_FILE IN ({placeholders})
            GROUP BY repos.ID_FILE, repos.NA_PATH, repos.NA_FILE,
                repos.NA_EXTENSION, repos.VL_FILE_SIZE_KB
        """, list(file_ids))
        positions = {file_id: index for index, file_id in enumerate(file_ids)}
        result.rows.sort(key=lambda row: positions[row["ID_FILE"]])
        return result

    def get_spectra_by_file_id(self, file_id: int) -> TableResult:
        """Read all spectra linked to one reposfi file, without search filters.

        Args:
            file_id: Repository file identifier (int).

        Returns:
            Spectrum identifiers, descriptions, ranges, geography and equipment
            names (TableResult). Unknown files return no rows.
        """
        return self._fetch(k.SPECTRUM_SCHEMA, f"""
            SELECT f.ID_SPECTRUM, f.NA_DESCRIPTION, f.NU_FREQ_START, f.NU_FREQ_END,
                f.DT_TIME_START, f.DT_TIME_END, f.FK_SITE AS ID_SITE,
                {LOCALITY_SQL} AS LOCALITY_LABEL, c.NA_COUNTY AS COUNTY_NAME,
                st.LC_STATE AS STATE_CODE, e.NA_EQUIPMENT
            {FILE_JOINS} {EQUIPMENT_JOIN} {GEO_JOINS}
            WHERE repos.NA_VOLUME = %s AND repos.ID_FILE = %s
            ORDER BY f.DT_TIME_START DESC, f.ID_SPECTRUM DESC
        """, [k.REPOSITORY_VOLUME, file_id])

    def get_spectrum_file_data_count(self, filters: Filters) -> int:
        """Count files using the original count query's join scope.

        Args:
            filters: Spectrum search filters (Filters); pagination is ignored.

        Returns:
            Number of matching file groups (int).
        """
        where, params = _spectrum_conditions(filters)
        # Add only joins required by active geography filters.
        geo_join = ""
        if filters.state_code or filters.district_ids:
            geo_join = "JOIN RFDATA.DIM_SPECTRUM_SITE s ON s.ID_SITE = f.FK_SITE"
        if filters.state_code:
            geo_join += " LEFT JOIN RFDATA.DIM_SITE_STATE st ON st.ID_STATE = s.FK_STATE"
        result = self._fetch(k.SPECTRUM_SCHEMA, f"""
            SELECT COUNT(DISTINCT repos.ID_FILE) AS TOTAL_COUNT
            {FILE_JOINS} {geo_join}
            WHERE repos.NA_VOLUME = %s {where}
        """, [k.REPOSITORY_VOLUME, *params])
        return int(result.rows[0]["TOTAL_COUNT"])
