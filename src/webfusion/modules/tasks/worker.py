"""Legacy HOST_TASK socket-dispatch example.

This file is excluded from the Flask request lifecycle and is not started by
the current deployment. It documents the former bridge from a BPDATA task row
to an external socket client; appCataloga owns the supported queue runtime.
"""

import time
import json
from db import get_connection
from socket_client import send_socket_payload  # Provided only by legacy deployments.


def run_worker():
    """Poll pending tasks and forward them to the socket layer.

    This legacy loop has no claim-race protection and must not be used as a
    production queue worker. It remains only as a payload-shape reference.
    """

    while True:
        db = get_connection()
        cursor = db.cursor()

        cursor.execute("""
            SELECT ht.*, h.*
            FROM HOST_TASK ht
            JOIN HOST h ON h.ID_HOST = ht.FK_HOST
            WHERE ht.NU_STATUS = 1
            LIMIT 1
        """)

        task = cursor.fetchone()

        if not task:
            time.sleep(2)
            continue

        cursor.execute(
            "UPDATE HOST_TASK SET NU_STATUS = 2 WHERE ID_HOST_TASK = %s",
            (task["ID_HOST_TASK"],),
        )
        db.commit()

        try:
            # Translate the database task row into the socket payload expected
            # by the remote processing side.
            payload = {
                "query_tag": task["NU_TYPE"],
                "host_id": task["ID_HOST"],
                "host_uid": task["NA_HOST_NAME"],
                "host_add": task["NA_HOST_ADDRESS"],
                "host_port": task["NA_HOST_PORT"],
                "user": task["NA_HOST_USER"],
                "passwd": task["NA_HOST_PASSWORD"],
                "filter": json.loads(task["FILTER"]),
            }

            response = send_socket_payload(
                task["NA_HOST_ADDRESS"],
                task["NA_HOST_PORT"],
                payload,
                timeout=30,
            )

            cursor.execute(
                "UPDATE HOST_TASK SET NU_STATUS = 0, NA_MESSAGE=%s WHERE ID_HOST_TASK=%s",
                (response, task["ID_HOST_TASK"]),
            )

        except Exception as e:
            cursor.execute(
                "UPDATE HOST_TASK SET NU_STATUS = -1, NA_MESSAGE=%s WHERE ID_HOST_TASK=%s",
                (str(e), task["ID_HOST_TASK"]),
            )

        db.commit()
