from db import get_connection_rfdata as get_connection


def get_all_hosts(online_only=False, search=None):

    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT ID_HOST, NA_HOST_NAME, IS_OFFLINE
        FROM BPDATA.HOST
    """

    where_clauses = []
    params = []

    if online_only:
        where_clauses.append("IS_OFFLINE = 0")

    if search:
        where_clauses.append("NA_HOST_NAME LIKE %s")
        params.append(f"%{search}%")

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " ORDER BY NA_HOST_NAME"

    cur.execute(query, params)
    rows = cur.fetchall()

    conn.close()

    return rows


def get_host_statistics(host_id):

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            ID_HOST,
            NA_HOST_NAME,
            NA_HOST_ADDRESS,
            NA_HOST_PORT,
            IS_OFFLINE,
            IS_BUSY,
            NU_PID,
            DT_BUSY,
            DT_LAST_FAIL,
            DT_LAST_CHECK,
            NU_HOST_CHECK_ERROR,
            DT_LAST_DISCOVERY,
            NU_DONE_FILE_DISCOVERY_TASKS,
            NU_ERROR_FILE_DISCOVERY_TASKS,
            DT_LAST_BACKUP,
            NU_PENDING_FILE_BACKUP_TASKS,
            NU_DONE_FILE_BACKUP_TASKS,
            NU_ERROR_FILE_BACKUP_TASKS,
            VL_PENDING_BACKUP_KB,
            VL_DONE_BACKUP_KB,
            DT_LAST_PROCESSING,
            NU_PENDING_FILE_PROCESS_TASKS,
            NU_DONE_FILE_PROCESS_TASKS,
            NU_ERROR_FILE_PROCESS_TASKS,
            NU_HOST_FILES
        FROM BPDATA.HOST
        WHERE ID_HOST = %s
    """, (host_id,))

    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    # Conversões
    row["PENDING_GB"] = round((row["VL_PENDING_BACKUP_KB"] or 0) / 1024 /1024, 2)
    row["DONE_GB"] = round((row["VL_DONE_BACKUP_KB"] or 0) / 1024 / 1024, 2)

    return row


def get_hosts(search=None):

    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT
            ID_HOST,
            NA_HOST_NAME,
            NA_HOST_ADDRESS,
            NA_HOST_PORT,
            IS_OFFLINE,
            IS_BUSY,
            DT_LAST_CHECK,
            DT_LAST_DISCOVERY,
            DT_LAST_BACKUP,
            DT_LAST_PROCESSING,
            NU_PENDING_FILE_BACKUP_TASKS,
            NU_PENDING_FILE_PROCESS_TASKS,
            VL_PENDING_BACKUP_KB
        FROM BPDATA.HOST
    """

    params = []

    if search:
        query += " WHERE NA_HOST_NAME LIKE %s"
        params.append(f"%{search}%")

    query += " ORDER BY NA_HOST_NAME"

    cur.execute(query, params)
    rows = cur.fetchall()

    conn.close()

    # Enriquecimento leve (sem mexer no banco)
    for r in rows:
        r["STATUS_LABEL"] = "Offline" if r["IS_OFFLINE"] else "Online"
        r["BUSY_LABEL"] = "Busy" if r["IS_BUSY"] else "Idle"

        if r["VL_PENDING_BACKUP_KB"]:
            r["PENDING_BACKUP_MB"] = round(r["VL_PENDING_BACKUP_KB"] / 1024, 2)
        else:
            r["PENDING_BACKUP_MB"] = 0

    return rows
