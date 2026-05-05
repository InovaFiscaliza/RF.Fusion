"""Routes for the task builder and recent task list.

This is the write-oriented corner of WebFusion. Instead of only reading the
current state, these routes collect operator intent and translate it into
durable ``HOST_TASK`` rows that appCataloga will later consume.

The route layer keeps three concerns local:

- lightweight HTTP auth for this module
- form normalization and UI defaults
- batching rules for individual versus collective task creation
"""

import re
from flask import Blueprint, Response, redirect, render_template, request, url_for
from modules.task.service import (
    EXPOSED_TASK_TYPES,
    HOST_TASK_BACKLOG_ROLLBACK_TYPE,
    HOST_TASK_CHECK_TYPE,
    create_task,
)
from db import get_connection_bpdata as get_connection


task_bp = Blueprint("task", __name__, url_prefix="/task")

TASK_AUTH_USERNAME = "admin"
TASK_AUTH_PASSWORD = "admin"
TASK_AUTH_REALM = "RF.Fusion Task"
DEFAULT_LINUX_FILE_PATH = "/mnt/internal/data"
DEFAULT_LINUX_EXTENSION = ".bin"
DEFAULT_CWSM_FILE_PATH = "C:/CelPlan/CellWireless RU/Spectrum/Completed"
DEFAULT_CWSM_EXTENSION = ".zip"

# Different station families do not always share the same path/extension
# conventions. These defaults let the UI suggest sensible values before the
# operator customizes them.


def _task_auth_failed():
    """Trigger the browser basic-auth challenge used by the task module."""
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


def _safe_int_arg(name):
    """Parse an optional integer query parameter without breaking the page."""
    raw_value = request.args.get(name)

    if raw_value in (None, ""):
        return None

    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def _normalize_selected_task_type(raw_value):
    """
    Keep task-type selection pinned to the small set exposed by WebFusion.
    """
    try:
        parsed = int(raw_value)
    except (TypeError, ValueError):
        return HOST_TASK_CHECK_TYPE

    if parsed in EXPOSED_TASK_TYPES:
        return parsed

    return HOST_TASK_CHECK_TYPE


def _normalize_filter_mode(raw_value):
    """Map legacy UI aliases to the canonical filter mode understood downstream."""
    normalized = str(raw_value or "NONE").strip().upper()
    if normalized in {"LAST_N", "LAST_N_FILES"}:
        return "LAST"
    return normalized or "NONE"


def _task_type_supports_backlog_budget(task_type):
    """Return whether the UI should expose backlog budget fields."""

    return int(task_type) == HOST_TASK_CHECK_TYPE


def _filter_mode_supports_backlog_budget(raw_value):
    """Return whether backlog budget fields make sense for the selected mode."""

    normalized = _normalize_filter_mode(raw_value)
    return normalized not in {"NONE", "REDISCOVERY"}


def _selection_supports_backlog_budget(task_type, raw_mode):
    """Return whether the current task/mode combination should expose budget UI."""

    return (
        _task_type_supports_backlog_budget(task_type)
        and _filter_mode_supports_backlog_budget(raw_mode)
    )


def _normalize_filter_mode_for_task_type(raw_value, task_type):
    """Coerce unsupported mode/task combinations to a safe visible fallback.

    The route tolerates bookmarked or hand-edited URLs, but the visible builder
    should stay within the smaller set of combinations that the UI explicitly
    supports.
    """

    normalized = _normalize_filter_mode(raw_value)

    if int(task_type) == HOST_TASK_BACKLOG_ROLLBACK_TYPE and normalized in {"NONE", "REDISCOVERY"}:
        return "ALL"

    return normalized


def _extract_host_prefix(host_name):
    """Return the leading alphabetical station family marker from a host name."""
    match = re.match(r"^[A-Za-z]+", str(host_name or "").strip())
    return match.group(0).upper() if match else ""


def _station_profile_field_key(host_prefix):
    """Build a stable HTML/form key for one station-family override row."""
    return re.sub(r"[^A-Z0-9]+", "_", str(host_prefix or "").upper()).strip("_")


def _resolve_filter_defaults_for_prefix(host_prefix):
    """Return the default path/extension pair for a station family."""
    if str(host_prefix or "").upper() == "CWSM":
        return {
            "file_path": DEFAULT_CWSM_FILE_PATH,
            "extension": DEFAULT_CWSM_EXTENSION,
        }

    return {
        "file_path": DEFAULT_LINUX_FILE_PATH,
        "extension": DEFAULT_LINUX_EXTENSION,
    }


def _build_station_profile_rows(host_prefix_rows, selected_values=None):
    """
    Build the per-family rows rendered by the collective task builder.

    Known families start prefilled with their operational defaults; the rest
    inherit the generic Linux-like fallback until a dedicated family profile is
    introduced.
    """
    selected_values = selected_values or {}
    rows = []

    for row in host_prefix_rows or []:
        prefix = str(row.get("PREFIX") or "").strip()
        if not prefix:
            continue

        field_key = _station_profile_field_key(prefix)
        defaults = _resolve_filter_defaults_for_prefix(prefix)

        rows.append(
            {
                "prefix": prefix,
                "field_key": field_key,
                "hosts": int(row.get("HOSTS") or 0),
                "file_path": selected_values.get(
                    f"profile_file_path__{field_key}",
                    defaults["file_path"],
                ),
                "extension": selected_values.get(
                    f"profile_extension__{field_key}",
                    defaults["extension"],
                ),
            }
        )

    return rows


def _extract_station_profile_overrides(form_data, station_profile_rows):
    """Read per-family file-path and extension overrides from the submitted form."""
    overrides = {}

    for row in station_profile_rows:
        field_key = row["field_key"]
        overrides[row["prefix"].upper()] = {
            "file_path": (form_data.get(f"profile_file_path__{field_key}") or "").strip(),
            "extension": (form_data.get(f"profile_extension__{field_key}") or "").strip(),
        }

    return overrides


def _looks_like_auto_filter_defaults(filter_data):
    """
    Detect whether the current filter fields still look auto-suggested.

    When collective execution mixes station families, a single shared
    `.bin`/Linux default is conceptually wrong. We only auto-split by family
    when the operator still appears to be relying on the builder defaults
    instead of having typed an explicit custom path/extension.
    """
    file_path = str(filter_data.get("file_path") or "").strip()
    extension = str(filter_data.get("extension") or "").strip().lower()

    auto_paths = {
        "",
        DEFAULT_LINUX_FILE_PATH,
        DEFAULT_CWSM_FILE_PATH,
    }
    auto_extensions = {
        "",
        DEFAULT_LINUX_EXTENSION,
        DEFAULT_CWSM_EXTENSION,
    }

    return file_path in auto_paths and extension in auto_extensions


def _build_collective_task_batches(host_rows, filter_data, profile_overrides=None):
    """
    Split collective requests by station family when defaults are still implicit.

    Mixed-family collective runs cannot safely reuse one shared path/extension
    pair. In that case we fan out the request into one batch per family, each
    with the correct default path/extension.
    """
    if not host_rows:
        return []

    if profile_overrides:
        grouped_hosts = {}
        for row in host_rows:
            prefix = _extract_host_prefix(row.get("NA_HOST_NAME"))
            grouped_hosts.setdefault(prefix, []).append(row["ID_HOST"])

        batches = []
        for prefix, host_ids in grouped_hosts.items():
            merged_filter = dict(filter_data)
            defaults = _resolve_filter_defaults_for_prefix(prefix)
            override = profile_overrides.get(prefix, {})
            merged_filter["file_path"] = override.get("file_path") or defaults["file_path"]
            merged_filter["extension"] = override.get("extension") or defaults["extension"]
            batches.append({"hosts": host_ids, "filter_data": merged_filter})

        return batches

    if not _looks_like_auto_filter_defaults(filter_data):
        return [
            {
                "hosts": [row["ID_HOST"] for row in host_rows],
                "filter_data": dict(filter_data),
            }
        ]

    grouped_hosts = {}
    for row in host_rows:
        prefix = _extract_host_prefix(row.get("NA_HOST_NAME"))
        grouped_hosts.setdefault(prefix, []).append(row["ID_HOST"])

    batches = []
    for prefix, host_ids in grouped_hosts.items():
        merged_filter = dict(filter_data)
        merged_filter.update(_resolve_filter_defaults_for_prefix(prefix))
        batches.append({"hosts": host_ids, "filter_data": merged_filter})

    return batches


@task_bp.before_request
def require_task_auth():
    """
    Protect the task builder and task list behind a basic-auth prompt.
    """
    if not _has_valid_task_credentials():
        return _task_auth_failed()


@task_bp.route("/", methods=["GET", "POST"])
def task_builder():
    """Render and process the task-builder form.

    The builder supports two execution styles:

    - individual execution for one selected host
    - collective execution across many hosts, optionally split by station family

    A key responsibility here is keeping the submitted filter payload aligned
    with the station family defaults that appCataloga expects downstream,
    especially when one collective action spans both Linux-like and CWSM hosts.
    """

    db = get_connection()
    cursor = db.cursor()
    selected_host = request.args.get("host_id")
    selected_task_type = str(
        _normalize_selected_task_type(request.args.get("task_type"))
    )
    selected_execution_type = request.args.get("execution_type", "individual")
    selected_host_filter = request.args.get("host_filter", "ALL")
    selected_mode = _normalize_filter_mode_for_task_type(
        request.args.get("mode", "NONE"),
        selected_task_type,
    )
    selected_start_date = request.args.get("start_date", "")
    selected_end_date = request.args.get("end_date", "")
    selected_last_n_files = request.args.get("last_n_files", "")
    selected_extension = request.args.get("extension", "")
    selected_file_path = request.args.get("file_path", "/mnt/internal/data")
    selected_file_name = request.args.get("file_name", "")
    selected_max_total_gb = request.args.get("max_total_gb", "")
    selected_sort_order = request.args.get("sort_order", "newest_first")
    selected_collective_host_ids = [
        value
        for value in request.args.getlist("collective_host_ids")
        if str(value).strip()
    ]
    selected_collective_host_search = request.args.get("collective_host_search", "")

    if not _selection_supports_backlog_budget(selected_task_type, selected_mode):
        selected_max_total_gb = ""
        selected_sort_order = "newest_first"

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
    station_profile_rows = _build_station_profile_rows(
        host_prefix_rows=host_prefixes,
        selected_values=request.args,
    )

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
        SELECT ID_HOST, NA_HOST_NAME, DT_LAST_DISCOVERY
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

        task_type = _normalize_selected_task_type(request.form.get("task_type"))
        execution_type = request.form.get("execution_type")
        mode = _normalize_filter_mode_for_task_type(
            request.form.get("mode"),
            task_type,
        )

        # Task filter payload
        filter_data = {
            "start_date": request.form.get("start_date") or None,
            "end_date": request.form.get("end_date") or None,
            "last_n_files": request.form.get("last_n_files") or None,
            "extension": request.form.get("extension") or None,
            "file_path": request.form.get("file_path") or None,
            "file_name": request.form.get("file_name") or None,
            "max_total_gb": request.form.get("max_total_gb") or None,
            "sort_order": request.form.get("sort_order") or "newest_first",
        }

        if not _selection_supports_backlog_budget(task_type, mode):
            filter_data["max_total_gb"] = None
            filter_data["sort_order"] = None

        # ==================================================
        # Collective execution
        # ==================================================
        if execution_type == "collective":

            host_filter = request.form.get("host_filter", "ALL")
            selected_collective_host_ids = {
                int(value)
                for value in request.form.getlist("collective_host_ids")
                if str(value).strip()
            }

            query = """
                SELECT ID_HOST, NA_HOST_NAME
                FROM HOST
                WHERE 1 = 1
            """
            params = []

            if online_only:
                query += " AND IS_OFFLINE = 0"

            # Apply prefix filter dynamically
            if host_filter != "ALL":
                query += " AND NA_HOST_NAME LIKE %s"
                params.append(f"{host_filter}%")

            query += " ORDER BY NA_HOST_NAME"

            cursor.execute(query, tuple(params))

            candidate_hosts = cursor.fetchall()
            if selected_collective_host_ids:
                selected_hosts = [
                    host_row
                    for host_row in candidate_hosts
                    if host_row["ID_HOST"] in selected_collective_host_ids
                ]
            else:
                selected_hosts = candidate_hosts

            creation_summary = {"queued_count": 0, "skipped_count": 0}
            if selected_hosts:
                profile_overrides = None
                if host_filter == "ALL":
                    profile_overrides = _extract_station_profile_overrides(
                        request.form,
                        station_profile_rows,
                    )

                for batch in _build_collective_task_batches(
                    host_rows=selected_hosts,
                    filter_data=filter_data,
                    profile_overrides=profile_overrides,
                ):
                    batch_summary = create_task(
                        db=db,
                        hosts=batch["hosts"],
                        task_type=task_type,
                        mode=mode,
                        filter_data=batch["filter_data"],
                    )
                    creation_summary["queued_count"] += batch_summary["queued_count"]
                    creation_summary["skipped_count"] += batch_summary["skipped_count"]

        # ==================================================
        # Individual execution
        # ==================================================
        else:

            host_id = request.form.get("host_id")
            creation_summary = {"queued_count": 0, "skipped_count": 0}

            if host_id:
                creation_summary = create_task(
                    db=db,
                    hosts=[int(host_id)],
                    task_type=task_type,
                    mode=mode,
                    filter_data=filter_data,
                )

        return redirect(
            url_for(
                "task.task_list",
                queued_count=creation_summary["queued_count"],
                skipped_count=creation_summary["skipped_count"],
            )
        )

    # --------------------------------------------------
    # Render page
    # --------------------------------------------------
    return render_template(
        "task/task_builder.html",
        hosts=hosts,
        host_prefixes=host_prefixes,
        online_only=online_only,
        selected_host=selected_host,
        selected_task_type=selected_task_type,
        selected_execution_type=selected_execution_type,
        selected_host_filter=selected_host_filter,
        selected_mode=selected_mode,
        selected_start_date=selected_start_date,
        selected_end_date=selected_end_date,
        selected_last_n_files=selected_last_n_files,
        selected_extension=selected_extension,
        selected_file_path=selected_file_path,
        selected_file_name=selected_file_name,
        selected_max_total_gb=selected_max_total_gb,
        selected_sort_order=selected_sort_order,
        selected_collective_host_ids=selected_collective_host_ids,
        selected_collective_host_search=selected_collective_host_search,
        station_profile_rows=station_profile_rows,
        exposed_task_types=EXPOSED_TASK_TYPES,
        stop_task_type=HOST_TASK_BACKLOG_ROLLBACK_TYPE,
    )


@task_bp.route("/list")
def task_list():
    """Render the latest ``HOST_TASK`` rows and any creation-result summary.

    The redirect from the builder includes ``queued_count`` and
    ``skipped_count`` so the page can confirm how many logical tasks were
    actually created or refreshed.
    """

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

    queued_count = _safe_int_arg("queued_count")
    skipped_count = _safe_int_arg("skipped_count")

    return render_template(
        "task/task_list.html",
        tasks=tasks,
        queued_count=queued_count,
        skipped_count=skipped_count,
        show_creation_summary=queued_count is not None or skipped_count is not None,
    )
