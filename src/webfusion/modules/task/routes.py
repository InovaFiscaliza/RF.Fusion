from flask import Blueprint, render_template, request, redirect
from modules.task.service import create_task
from db import get_connection_bpdata as get_connection


task_bp = Blueprint("task", __name__, url_prefix="/task")


@task_bp.route("/", methods=["GET", "POST"])
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
        online_only = request.form.get("online_only") is not None
    else:
        # Default behavior: show only online hosts
        online_only = True

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
                WHERE IS_OFFLINE = 0
            """

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
        online_only=online_only
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