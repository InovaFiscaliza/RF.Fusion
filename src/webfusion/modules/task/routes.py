from flask import Blueprint, Response, redirect, render_template, request
from modules.task.service import create_task
from db import get_connection_bpdata as get_connection


task_bp = Blueprint("task", __name__, url_prefix="/task")

TASK_AUTH_USERNAME = "admin"
TASK_AUTH_PASSWORD = "admin"
TASK_AUTH_REALM = "RF.Fusion Task"


def _task_auth_failed():
    """
    Trigger a browser basic-auth challenge for the task module.
    """
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": f'Basic realm="{TASK_AUTH_REALM}"'},
    )


def _has_valid_task_credentials():
    """
    Validate the simple bootstrap credentials for the task module.

    This is intentionally minimal for the first protection layer. If the
    module graduates to broader use, these credentials should move to a proper
    configuration source and session-backed authentication.
    """
    auth = request.authorization

    if not auth:
        return False

    return (
        str(auth.username or "") == TASK_AUTH_USERNAME
        and str(auth.password or "") == TASK_AUTH_PASSWORD
    )


@task_bp.before_request
def require_task_auth():
    """
    Protect the task builder and task list behind a basic-auth prompt.
    """
    if not _has_valid_task_credentials():
        return _task_auth_failed()


@task_bp.route("/", methods=["GET", "POST"])
def task_builder():
    """
    WebFusion Task Builder.

    This view allows the creation of HOST_TASK entries either:
        • Individually (single host)
        • Collectively (multiple hosts)

    Collective execution can optionally be filtered by host prefix.
    The prefix is automatically detected from the HOST table using
    the alphabetical portion of NA_HOST_NAME.

    Example hostnames:
        RFEye002264  → prefix "RFEye"
        CWSM211006   → prefix "CWSM"

    Workflow:
        1. Discover host prefixes dynamically from database
        2. Load host list (optionally only online hosts)
        3. Render builder interface
        4. Process POST submission
        5. Create tasks via create_task()

    Returns:
        HTML page or redirect to task list.
    """

    db = get_connection()
    cursor = db.cursor()
    selected_host = request.form.get("host_id") if request.method == "POST" else request.args.get("host_id")

    # --------------------------------------------------
    # Discover host prefixes dynamically
    # --------------------------------------------------
    cursor.execute("""
        SELECT
            REGEXP_SUBSTR(NA_HOST_NAME, '^[A-Za-z]+') AS PREFIX,
            COUNT(*) AS HOSTS
        FROM HOST
        GROUP BY PREFIX
        ORDER BY PREFIX
    """)
    host_prefixes = cursor.fetchall()

    # --------------------------------------------------
    # Determine checkbox state (online-only filter)
    # --------------------------------------------------
    if request.method == "POST":
        online_only = request.form.get("online_only") == "1"
    else:
        # Default behavior: show only online hosts, but allow the page
        # filter to explicitly request the full HOST list.
        online_only = request.args.get("online_only", "1") == "1"

    # --------------------------------------------------
    # Load hosts for individual selection
    # --------------------------------------------------
    query = """
        SELECT ID_HOST, NA_HOST_NAME
        FROM HOST
    """

    if online_only:
        query += " WHERE IS_OFFLINE = 0"

    query += " ORDER BY NA_HOST_NAME"

    cursor.execute(query)
    hosts = cursor.fetchall()

    # --------------------------------------------------
    # Handle POST submission
    # --------------------------------------------------
    if request.method == "POST":

        task_type = int(request.form.get("task_type"))
        execution_type = request.form.get("execution_type")
        mode = request.form.get("mode")

        # Task filter payload
        filter_data = {
            "start_date": request.form.get("start_date") or None,
            "end_date": request.form.get("end_date") or None,
            "last_n_files": request.form.get("last_n_files") or None,
            "extension": request.form.get("extension") or None,
            "file_path": request.form.get("file_path") or None,
            "file_name": request.form.get("file_name") or None,
        }

        # ==================================================
        # Collective execution
        # ==================================================
        if execution_type == "collective":

            host_filter = request.form.get("host_filter", "ALL")

            query = """
                SELECT ID_HOST
                FROM HOST
                WHERE 1 = 1
            """

            if online_only:
                query += " AND IS_OFFLINE = 0"

            # Apply prefix filter dynamically
            if host_filter != "ALL":
                query += f" AND NA_HOST_NAME LIKE '{host_filter}%'"

            query += " ORDER BY NA_HOST_NAME"

            cursor.execute(query)

            all_hosts = [h["ID_HOST"] for h in cursor.fetchall()]

            if all_hosts:
                create_task(
                    db=db,
                    hosts=all_hosts,
                    task_type=task_type,
                    mode=mode,
                    filter_data=filter_data,
                )

        # ==================================================
        # Individual execution
        # ==================================================
        else:

            host_id = request.form.get("host_id")

            if host_id:
                create_task(
                    db=db,
                    hosts=[int(host_id)],
                    task_type=task_type,
                    mode=mode,
                    filter_data=filter_data,
                )

        return redirect("/task/list")

    # --------------------------------------------------
    # Render page
    # --------------------------------------------------
    return render_template(
        "task/task_builder.html",
        hosts=hosts,
        host_prefixes=host_prefixes,
        online_only=online_only,
        selected_host=selected_host,
    )


@task_bp.route("/list")
def task_list():

    db = get_connection()
    cursor = db.cursor()

    cursor.execute("""
        SELECT
            ht.ID_HOST_TASK,
            ht.FK_HOST,
            ht.NU_TYPE,
            ht.NU_STATUS,
            ht.DT_HOST_TASK,
            ht.NA_MESSAGE,
            h.NA_HOST_NAME
        FROM HOST_TASK ht
        JOIN HOST h ON h.ID_HOST = ht.FK_HOST
        ORDER BY ht.DT_HOST_TASK DESC
        LIMIT 100
    """)

    tasks = cursor.fetchall()

    return render_template("task/task_list.html", tasks=tasks)
