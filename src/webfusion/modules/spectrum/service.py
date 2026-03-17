import os
import time
from db import get_connection_rfdata as get_connection

ALLOWED_SORT_FIELDS = {
    "date_start": "f.DT_TIME_START",
    "date_end": "f.DT_TIME_END",
    "freq_start": "f.NU_FREQ_START",
    "freq_end": "f.NU_FREQ_END",
    "rbw": "f.NU_RBW",
    "trace_count": "f.NU_TRACE_COUNT"
}

ALLOWED_SORT_ORDERS = ["ASC", "DESC"]

ALLOWED_FILE_SORT_FIELDS = {
    "date_start": "MIN(f.DT_TIME_START)",
    "date_end": "MAX(f.DT_TIME_END)",
    "file_name": "repos.NA_FILE",
    "spectrum_count": "COUNT(*)",
}

SPECTRUM_QUERY_CACHE_TTL_SECONDS = 30
EQUIPMENT_CACHE_TTL_SECONDS = 300
FILE_PATH_CACHE_TTL_SECONDS = 300

_EQUIPMENT_CACHE = {"expires_at": 0.0, "value": None}
_SPECTRUM_QUERY_CACHE = {}
_FILE_PATH_CACHE = {}


def _get_cached_query(cache_key):
    """
    Return a cached query result when it is still fresh.
    """
    cached = _SPECTRUM_QUERY_CACHE.get(cache_key)

    if not cached:
        return None

    if cached["expires_at"] <= time.time():
        _SPECTRUM_QUERY_CACHE.pop(cache_key, None)
        return None

    return cached["value"]


def _set_cached_query(cache_key, value):
    """
    Cache a query result for a short TTL.
    """
    _SPECTRUM_QUERY_CACHE[cache_key] = {
        "expires_at": time.time() + SPECTRUM_QUERY_CACHE_TTL_SECONDS,
        "value": value,
    }


def _get_cached_file_path(cache_key):
    """
    Return a cached repository file path when still fresh.
    """
    cached = _FILE_PATH_CACHE.get(cache_key)

    if not cached:
        return None

    if cached["expires_at"] <= time.time():
        _FILE_PATH_CACHE.pop(cache_key, None)
        return None

    return cached["value"]


def _set_cached_file_path(cache_key, value):
    """
    Cache repository file resolution for repeated download clicks.
    """
    _FILE_PATH_CACHE[cache_key] = {
        "expires_at": time.time() + FILE_PATH_CACHE_TTL_SECONDS,
        "value": value,
    }


def get_equipments():
    now = time.time()

    if (
        _EQUIPMENT_CACHE["value"] is not None
        and _EQUIPMENT_CACHE["expires_at"] > now
    ):
        return _EQUIPMENT_CACHE["value"]

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT
            e.ID_EQUIPMENT,
            e.NA_EQUIPMENT
        FROM DIM_SPECTRUM_EQUIPMENT e
        JOIN FACT_SPECTRUM f
            ON f.FK_EQUIPMENT = e.ID_EQUIPMENT
        ORDER BY e.NA_EQUIPMENT
    """)

    result = cur.fetchall()
    conn.close()
    _EQUIPMENT_CACHE["value"] = result
    _EQUIPMENT_CACHE["expires_at"] = now + EQUIPMENT_CACHE_TTL_SECONDS
    return result


def get_spectrum_data(
    equipment_id=None,
    start_date=None,
    end_date=None,
    freq_start=None,
    freq_end=None,
    description=None,
    sort_by="date_start",
    sort_order="DESC",
    page=1,
    page_size=50
):
    if sort_by not in ALLOWED_SORT_FIELDS:
        sort_by = "date_start"

    if sort_order not in ALLOWED_SORT_ORDERS:
        sort_order = "DESC"

    try:
        page = int(page)
        if page < 1:
            page = 1
    except Exception:
        page = 1

    cache_key = (
        "spectrum",
        equipment_id,
        start_date,
        end_date,
        freq_start,
        freq_end,
        description,
        sort_by,
        sort_order,
        page,
        page_size,
    )
    cached = _get_cached_query(cache_key)

    if cached is not None:
        return cached

    # ---------------------------
    # Construção dinâmica do WHERE
    # ---------------------------

    where_clauses = []
    params = []

    if equipment_id:
        where_clauses.append("f.FK_EQUIPMENT = %s")
        params.append(equipment_id)

    if start_date:
        where_clauses.append("f.DT_TIME_END >= %s")
        params.append(start_date)

    if end_date:
        where_clauses.append("f.DT_TIME_START <= %s")
        params.append(end_date + " 23:59:59")

    if freq_start is not None:
        where_clauses.append("f.NU_FREQ_END >= %s")
        params.append(freq_start)

    if freq_end is not None:
        where_clauses.append("f.NU_FREQ_START <= %s")
        params.append(freq_end)

    if description:
        where_clauses.append("f.NA_DESCRIPTION LIKE %s")
        params.append(f"%{description}%")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    # ---------------------------
    # ORDER BY seguro
    # ---------------------------

    order_sql = f"""
        ORDER BY {ALLOWED_SORT_FIELDS[sort_by]} {sort_order},
                 f.ID_SPECTRUM DESC
    """

    # ---------------------------
    # Paginação
    # ---------------------------

    offset = (page - 1) * page_size
    limit_sql = "LIMIT %s OFFSET %s"
    data_params = params + [page_size, offset]

    # ---------------------------
    # Query principal
    # ---------------------------

    data_query = f"""
        SELECT
            f.ID_SPECTRUM,
            f.NA_DESCRIPTION,
            f.NU_FREQ_START,
            f.NU_FREQ_END,
            f.DT_TIME_START,
            f.DT_TIME_END,
            f.NU_TRACE_COUNT,
            f.NU_TRACE_LENGTH,
            f.NU_RBW,
            f.NU_VBW,
            f.NU_ATT_GAIN,
            e.NA_EQUIPMENT,
            repos.NA_PATH,
            repos.NA_FILE,
            repos.NA_EXTENSION,
            repos.VL_FILE_SIZE_KB
        FROM FACT_SPECTRUM f
        JOIN DIM_SPECTRUM_EQUIPMENT e
            ON e.ID_EQUIPMENT = f.FK_EQUIPMENT

        LEFT JOIN (
            SELECT
                b.FK_SPECTRUM,
                MAX(d.ID_FILE) AS ID_FILE
            FROM BRIDGE_SPECTRUM_FILE b
            JOIN DIM_SPECTRUM_FILE d
                ON d.ID_FILE = b.FK_FILE
            WHERE d.NA_VOLUME = 'reposfi'
            GROUP BY b.FK_SPECTRUM
        ) latest
            ON latest.FK_SPECTRUM = f.ID_SPECTRUM

        LEFT JOIN DIM_SPECTRUM_FILE repos
            ON repos.ID_FILE = latest.ID_FILE

        {where_sql}
        {order_sql}
        {limit_sql}
    """

    # ---------------------------
    # Query de contagem
    # ---------------------------

    count_query = f"""
        SELECT COUNT(*) AS total
        FROM FACT_SPECTRUM f
        LEFT JOIN (
            SELECT
                b.FK_SPECTRUM,
                MAX(d.ID_FILE) AS ID_FILE
            FROM BRIDGE_SPECTRUM_FILE b
            JOIN DIM_SPECTRUM_FILE d
                ON d.ID_FILE = b.FK_FILE
            WHERE d.NA_VOLUME = 'reposfi'
            GROUP BY b.FK_SPECTRUM
        ) latest
            ON latest.FK_SPECTRUM = f.ID_SPECTRUM

        LEFT JOIN DIM_SPECTRUM_FILE repos
            ON repos.ID_FILE = latest.ID_FILE
        {where_sql}
    """

    # ---------------------------
    # Execução
    # ---------------------------

    conn = get_connection()
    cur = conn.cursor()

    # Dados
    cur.execute(data_query, data_params)
    rows = cur.fetchall()

    # Total
    cur.execute(count_query, params)
    result = cur.fetchone()
    total = result["total"] if result else 0

    conn.close()

    result = (rows, total)
    _set_cached_query(cache_key, result)
    return result


def get_spectrum_file_data(
    equipment_id=None,
    start_date=None,
    end_date=None,
    sort_by="date_start",
    sort_order="DESC",
    page=1,
    page_size=50
):
    if sort_by not in ALLOWED_FILE_SORT_FIELDS:
        sort_by = "date_start"

    if sort_order not in ALLOWED_SORT_ORDERS:
        sort_order = "DESC"

    try:
        page = int(page)
        if page < 1:
            page = 1
    except Exception:
        page = 1

    cache_key = (
        "file",
        equipment_id,
        start_date,
        end_date,
        sort_by,
        sort_order,
        page,
        page_size,
    )
    cached = _get_cached_query(cache_key)

    if cached is not None:
        return cached

    where_clauses = ["repos.NA_VOLUME = 'reposfi'"]
    params = []

    where_sql = "WHERE " + " AND ".join(where_clauses)

    order_sql = f"""
        ORDER BY {ALLOWED_FILE_SORT_FIELDS[sort_by]} {sort_order},
                 repos.ID_FILE DESC
    """

    offset = (page - 1) * page_size
    limit_sql = "LIMIT %s OFFSET %s"
    data_params = [page_size, offset]

    data_query = f"""
        SELECT
            repos.ID_FILE,
            repos.NA_PATH,
            repos.NA_FILE,
            repos.NA_EXTENSION,
            repos.VL_FILE_SIZE_KB,
            MIN(f.DT_TIME_START) AS DT_TIME_START,
            MAX(f.DT_TIME_END) AS DT_TIME_END,
            COUNT(*) AS NU_SPECTRA
        FROM (
            SELECT
                ID_SPECTRUM,
                DT_TIME_START,
                DT_TIME_END
            FROM FACT_SPECTRUM
            WHERE FK_EQUIPMENT = %s
            {"AND DT_TIME_END >= %s" if start_date else ""}
            {"AND DT_TIME_START <= %s" if end_date else ""}
        ) f
        JOIN BRIDGE_SPECTRUM_FILE b
            ON b.FK_SPECTRUM = f.ID_SPECTRUM
        JOIN DIM_SPECTRUM_FILE repos
            ON repos.ID_FILE = b.FK_FILE
        {where_sql}
        GROUP BY
            repos.ID_FILE,
            repos.NA_PATH,
            repos.NA_FILE,
            repos.NA_EXTENSION,
            repos.VL_FILE_SIZE_KB
        {order_sql}
        {limit_sql}
    """

    count_query = f"""
        SELECT COUNT(*) AS total
        FROM (
            SELECT repos.ID_FILE
            FROM (
                SELECT
                    ID_SPECTRUM,
                    DT_TIME_START,
                    DT_TIME_END
                FROM FACT_SPECTRUM
                WHERE FK_EQUIPMENT = %s
                {"AND DT_TIME_END >= %s" if start_date else ""}
                {"AND DT_TIME_START <= %s" if end_date else ""}
            ) f
            JOIN BRIDGE_SPECTRUM_FILE b
                ON b.FK_SPECTRUM = f.ID_SPECTRUM
            JOIN DIM_SPECTRUM_FILE repos
                ON repos.ID_FILE = b.FK_FILE
            {where_sql}
            GROUP BY repos.ID_FILE
        ) grouped_files
    """

    file_query_params = [equipment_id]
    if start_date:
        file_query_params.append(start_date)
    if end_date:
        file_query_params.append(end_date + " 23:59:59")

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(data_query, file_query_params + data_params)
    rows = cur.fetchall()

    cur.execute(count_query, file_query_params + params)
    result = cur.fetchone()
    total = result["total"] if result else 0

    conn.close()

    result = (rows, total)
    _set_cached_query(cache_key, result)
    return result

def get_file_by_spectrum_id(spectrum_id):
    cache_key = ("spectrum_file_path", spectrum_id)
    cached = _get_cached_file_path(cache_key)

    if cached is not None:
        return cached
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT
            repos.NA_PATH,
            repos.NA_FILE,
            repos.NA_EXTENSION
        FROM FACT_SPECTRUM f

        LEFT JOIN (
            SELECT
                b.FK_SPECTRUM,
                MAX(d.ID_FILE) AS ID_FILE
            FROM BRIDGE_SPECTRUM_FILE b
            JOIN DIM_SPECTRUM_FILE d
                ON d.ID_FILE = b.FK_FILE
            WHERE d.NA_VOLUME = 'reposfi'
            GROUP BY b.FK_SPECTRUM
        ) latest
            ON latest.FK_SPECTRUM = f.ID_SPECTRUM

        LEFT JOIN DIM_SPECTRUM_FILE repos
            ON repos.ID_FILE = latest.ID_FILE

        WHERE f.ID_SPECTRUM = %s
    """

    cur.execute(query, (spectrum_id,))
    result = cur.fetchone()

    conn.close()

    if not result:
        return None

    na_path = result["NA_PATH"]
    na_file = result["NA_FILE"]

    if not na_path or not na_file:
        return None

    file_path = os.path.join(na_path, na_file)
    _set_cached_file_path(cache_key, file_path)
    return file_path


def get_file_by_file_id(file_id):
    cache_key = ("file_id_path", file_id)
    cached = _get_cached_file_path(cache_key)

    if cached is not None:
        return cached
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            NA_PATH,
            NA_FILE
        FROM DIM_SPECTRUM_FILE
        WHERE ID_FILE = %s
          AND NA_VOLUME = 'reposfi'
        """,
        (file_id,),
    )
    result = cur.fetchone()

    conn.close()

    if not result:
        return None

    na_path = result["NA_PATH"]
    na_file = result["NA_FILE"]

    if not na_path or not na_file:
        return None

    file_path = os.path.join(na_path, na_file)
    _set_cached_file_path(cache_key, file_path)
    return file_path


def get_spectra_by_file_id(file_id):
    """
    Return the spectra linked to a single repository file.

    This supports the expandable "file mode" view without forcing the main
    query to repeat one line per spectrum.
    """
    cache_key = ("file_spectra", file_id)
    cached = _get_cached_query(cache_key)

    if cached is not None:
        return cached

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            f.ID_SPECTRUM,
            f.NA_DESCRIPTION,
            f.NU_FREQ_START,
            f.NU_FREQ_END,
            f.DT_TIME_START,
            f.DT_TIME_END,
            f.NU_RBW,
            f.NU_TRACE_COUNT,
            e.NA_EQUIPMENT
        FROM BRIDGE_SPECTRUM_FILE b
        JOIN FACT_SPECTRUM f
            ON f.ID_SPECTRUM = b.FK_SPECTRUM
        JOIN DIM_SPECTRUM_EQUIPMENT e
            ON e.ID_EQUIPMENT = f.FK_EQUIPMENT
        WHERE b.FK_FILE = %s
        ORDER BY f.DT_TIME_START DESC, f.ID_SPECTRUM DESC
        """,
        (file_id,),
    )

    rows = cur.fetchall()
    conn.close()

    _set_cached_query(cache_key, rows)
    return rows
