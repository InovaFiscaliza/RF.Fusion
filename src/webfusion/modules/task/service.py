import json
from datetime import datetime
from .builder import build_filter, NONE_FILTER


HOST_TASK_CHECK_TYPE = 1
HOST_TASK_UPDATE_STATISTICS_TYPE = 3

TASK_PENDING = 1
TASK_RUNNING = 2
TASK_ERROR = -1
TASK_SUSPENDED = 3


def queue_host_task_safe(db, host_id, task_type, filter_dict, message):
    """
    Deterministic host task creation.
    """

    cursor = db.cursor()
    filter_json = json.dumps(filter_dict)

    cursor.execute("""
        SELECT ID_HOST_TASK, NU_STATUS
        FROM HOST_TASK
        WHERE FK_HOST = %s
        AND NU_TYPE = %s
        ORDER BY DT_HOST_TASK DESC
        LIMIT 1
    """, (host_id, task_type))

    row = cursor.fetchone()

    if row:

        task_id = row["ID_HOST_TASK"]
        status = row["NU_STATUS"]

        # Already active → do nothing
        if status in (TASK_PENDING, TASK_RUNNING):
            return

        # Terminal → refresh
        if status in (TASK_ERROR, TASK_SUSPENDED):
            cursor.execute("""
                UPDATE HOST_TASK
                SET NU_STATUS = %s,
                    DT_HOST_TASK = NOW(),
                    FILTER = %s,
                    NA_MESSAGE = %s
                WHERE ID_HOST_TASK = %s
            """, (
                TASK_PENDING,
                filter_json,
                f"Refreshed by WebFusion | {message}",
                task_id
            ))

            db.commit()
            return

    # No existing task → create new
    cursor.execute("""
        INSERT INTO HOST_TASK
        (FK_HOST, NU_TYPE, DT_HOST_TASK, NU_STATUS, FILTER, NA_MESSAGE)
        VALUES (%s, %s, NOW(), %s, %s, %s)
    """, (
        host_id,
        task_type,
        TASK_PENDING,
        filter_json,
        message
    ))

    db.commit()


def create_task(db, hosts, task_type, mode, filter_data):

    if task_type not in (
        HOST_TASK_CHECK_TYPE,
        HOST_TASK_UPDATE_STATISTICS_TYPE,
    ):
        raise ValueError("Tipo de task inválido")

    collective = len(hosts) > 1

    for host_id in hosts:

        if task_type == HOST_TASK_UPDATE_STATISTICS_TYPE:
            filter_dict = NONE_FILTER.copy()
            action_name = "Update Statistics"

        else:
            filter_dict = build_filter(
                mode=mode,
                start_date=filter_data.get("start_date"),
                end_date=filter_data.get("end_date"),
                last_n_files=filter_data.get("last_n_files"),
                extension=filter_data.get("extension"),
                file_path=filter_data.get("file_path"),
                file_name=filter_data.get("file_name"),
            )
            action_name = f"Host Check ({mode})"

        scope = "Collective" if collective else "Individual"

        message = f"Created by WebFusion | {action_name} | {scope}"

        queue_host_task_safe(
            db=db,
            host_id=host_id,
            task_type=task_type,
            filter_dict=filter_dict,
            message=message
        )