# modules/spectrum/service.py
import os
from db import get_connection_rfdata as get_connection

ALLOWED_SORT_FIELDS = {
    "date_start": "f.DT_TIME_START",
    "freq_start": "f.NU_FREQ_START",
    "trace_count": "f.NU_TRACE_COUNT"
}

ALLOWED_SORT_ORDERS = ["ASC", "DESC"]


def get_equipments():
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
    return result


def get_spectrum_data(
    equipment_id=None,
    start_date=None,
    end_date=None,
    sort_by="date_start",
    sort_order="DESC",
    page=1,
    page_size=50
):

    # ---------------------------
    # Sanitização de parâmetros
    # ---------------------------

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

    return rows, total

def get_file_by_spectrum_id(spectrum_id):

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

    return os.path.join(na_path, na_file)