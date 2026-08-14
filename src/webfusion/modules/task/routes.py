"""Routes for the task builder and recent task list.

This is the write-oriented corner of WebFusion. Instead of only reading the
current state, these routes collect operator intent and translate it into
durable ``HOST_TASK`` rows that appCataloga will later consume.

The route layer keeps three concerns local:

- lightweight HTTP auth for this module
- form normalization and UI defaults
- batching rules for individual versus collective task creation
"""

import json
import re
from typing import Any
from flask import (
    Blueprint,
    Response,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from modules.task.service import (
    EXPOSED_TASK_TYPES,
    HOST_TASK_BACKLOG_ROLLBACK_TYPE,
    HOST_TASK_CHECK_TYPE,
    HOST_TASK_INTERACTIVE_CHECK_TYPE,
    create_task,
    queue_interactive_connectivity_test,
)
from modules.server.usage_metrics import record_page_view
from modules.zabbix_configuration.service import (
    TARGET_KIND_HOST,
    ZabbixApiError,
    ZabbixConfigurationError,
    get_configuration,
    get_hosts_backup_defaults,
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
DEFAULT_UMS300_FILE_PATH = "C:/Users/NUC/Downloads"
DEFAULT_UMS300_EXTENSION = ".bin"
ZABBIX_BACKUP_PATH_MACRO = "{$BACKUP_PATH}"
ZABBIX_BACKUP_EXTENSION_MACRO = "{$BACKUP_EXTENSION}"
MAX_COLLECTIVE_ZABBIX_DEFAULT_HOSTS = 500

TASK_ACTION_BACKUP = "backup"
TASK_ACTION_BACKLOG_ROLLBACK = "backlog_rollback"
TASK_ACTION_DISCOVER = "discover"
TASK_ACTION_REDISCOVER = "rediscover"
TASK_ACTION_CONNECTIVITY_TEST = "connectivity_test"
TASK_ACTION_DEFAULT = TASK_ACTION_DISCOVER

TASK_ACTIONS = (
    {
        "value": TASK_ACTION_BACKUP,
        "label": "Enviar para Fila de Backup",
        "task_type": HOST_TASK_CHECK_TYPE,
        "fixed_mode": None,
    },
    {
        "value": TASK_ACTION_BACKLOG_ROLLBACK,
        "label": "Remover da Fila de Backup",
        "task_type": HOST_TASK_BACKLOG_ROLLBACK_TYPE,
        "fixed_mode": None,
    },
    {
        "value": TASK_ACTION_DISCOVER,
        "label": "Executar Descoberta Incremental",
        "task_type": HOST_TASK_CHECK_TYPE,
        "fixed_mode": "NONE",
    },
    {
        "value": TASK_ACTION_REDISCOVER,
        "label": "Executar Descoberta Completa",
        "task_type": HOST_TASK_CHECK_TYPE,
        "fixed_mode": "REDISCOVERY",
    },
    {
        "value": TASK_ACTION_CONNECTIVITY_TEST,
        "label": "Testar Conectividade da Estação",
        "task_type": HOST_TASK_INTERACTIVE_CHECK_TYPE,
        "fixed_mode": None,
    },
)

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


def _normalize_task_action(raw_value: object) -> str:
    """Keep the submitted action within the small task-builder vocabulary."""
    normalized = str(raw_value or "").strip().lower()
    available_actions = {action["value"] for action in TASK_ACTIONS}

    if normalized in available_actions:
        return normalized

    return TASK_ACTION_DEFAULT


def _resolve_task_action(action_value: str) -> dict[str, Any]:
    """Return the queue type and optional fixed mode for one visible action."""
    normalized_action = _normalize_task_action(action_value)
    return next(
        action
        for action in TASK_ACTIONS
        if action["value"] == normalized_action
    )


def _action_from_legacy_selection(task_type: int, mode: str) -> str:
    """Preserve direct URLs created before the action selector existed."""
    if task_type == HOST_TASK_BACKLOG_ROLLBACK_TYPE:
        return TASK_ACTION_BACKLOG_ROLLBACK
    if mode == "REDISCOVERY":
        return TASK_ACTION_REDISCOVER
    if mode == "NONE":
        return TASK_ACTION_DISCOVER
    return TASK_ACTION_BACKUP


def _resolve_action_mode(action: dict[str, Any], raw_mode: object) -> str:
    """Apply an action's fixed filter or validate its selectable mode."""
    fixed_mode = action["fixed_mode"]
    if fixed_mode:
        return str(fixed_mode)

    task_type = int(action["task_type"])
    mode = _normalize_filter_mode_for_task_type(raw_mode, task_type)

    # Discovery and rediscovery are explicit actions, not generic backup modes.
    if action["value"] == TASK_ACTION_BACKUP and mode in {"NONE", "REDISCOVERY"}:
        return "ALL"

    return mode


def _extract_host_prefix(host_name: object) -> str:
    """Return the leading alphabetical station family marker from a host name."""
    normalized_name = str(host_name or "").strip().upper()

    if normalized_name.startswith("UMS"):
        return "UMS300"

    if normalized_name.startswith("ERMX"):
        return "ERMX"

    match = re.match(r"^[A-Z]+", normalized_name)
    return match.group(0) if match else ""


def _station_profile_field_key(host_prefix):
    """Build a stable HTML/form key for one station-family override row."""
    return re.sub(r"[^A-Z0-9]+", "_", str(host_prefix or "").upper()).strip("_")


def _resolve_filter_defaults_for_prefix(host_prefix):
    """Return the default path/extension pair for a station family."""
    normalized_prefix = str(host_prefix or "").upper()

    if normalized_prefix == "CWSM":
        return {
            "file_path": DEFAULT_CWSM_FILE_PATH,
            "extension": DEFAULT_CWSM_EXTENSION,
        }

    if normalized_prefix == "UMS300" or normalized_prefix.startswith("UMS"):
        return {
            "file_path": DEFAULT_UMS300_FILE_PATH,
            "extension": DEFAULT_UMS300_EXTENSION,
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
        file_path_key = f"profile_file_path__{field_key}"
        extension_key = f"profile_extension__{field_key}"
        selected_file_path = selected_values.get(file_path_key)
        selected_extension = selected_values.get(extension_key)

        rows.append(
            {
                "prefix": prefix,
                "field_key": field_key,
                "hosts": int(row.get("HOSTS") or 0),
                "file_path": selected_file_path or defaults["file_path"],
                "extension": selected_extension or defaults["extension"],
                "has_custom_value": bool(
                    (selected_file_path and selected_file_path != defaults["file_path"])
                    or (selected_extension and selected_extension != defaults["extension"])
                ),
            }
        )

    return rows


def _extract_zabbix_backup_defaults(configuration):
    """Return plain-text backup defaults from one resolved Zabbix host."""
    macro_fields = {
        ZABBIX_BACKUP_PATH_MACRO: "file_path",
        ZABBIX_BACKUP_EXTENSION_MACRO: "extension",
    }
    defaults = {field_name: None for field_name in macro_fields.values()}

    for macro in configuration.get("macros", []):
        field_name = macro_fields.get(str(macro.get("name") or ""))
        if not field_name:
            continue

        # Never use a display value here: the endpoint only accepts macros
        # whose text value is explicitly available to the configuration module.
        if str(macro.get("type")) != "0" or not macro.get("accepts_value"):
            continue

        value = str(macro.get("editable_value") or "").strip()
        if value:
            defaults[field_name] = value

    return defaults


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


def _parse_collective_zabbix_defaults(raw_value, allowed_host_ids):
    """Accept a bounded client snapshot only for the selected host scope."""
    if not raw_value:
        return {}

    try:
        payload = json.loads(raw_value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}

    if not isinstance(payload, dict):
        return {}

    allowed_ids = {str(host_id) for host_id in allowed_host_ids}
    defaults_by_host = {}

    for host_id, values in payload.items():
        normalized_host_id = str(host_id)
        if normalized_host_id not in allowed_ids or not isinstance(values, dict):
            continue

        file_path = str(values.get("file_path") or "").strip()
        extension = str(values.get("extension") or "").strip()
        if not file_path or not extension:
            continue
        if len(file_path) > 2048 or len(extension) > 255:
            continue

        defaults_by_host[normalized_host_id] = {
            "file_path": file_path,
            "extension": extension,
        }

    return defaults_by_host


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
        DEFAULT_UMS300_FILE_PATH,
    }
    auto_extensions = {
        "",
        DEFAULT_LINUX_EXTENSION,
        DEFAULT_CWSM_EXTENSION,
        DEFAULT_UMS300_EXTENSION,
    }

    return file_path in auto_paths and extension in auto_extensions


def _build_collective_task_batches(
    host_rows,
    filter_data,
    profile_overrides=None,
    zabbix_defaults_by_host=None,
):
    """
    Split collective requests by station family when defaults are still implicit.

    Mixed-family collective runs cannot safely reuse one shared path/extension
    pair. In that case we fan out the request into one batch per family, each
    with the correct default path/extension.
    """
    if not host_rows:
        return []

    if zabbix_defaults_by_host:
        batches_by_backup_defaults = {}
        fallback_rows = []

        for row in host_rows:
            defaults = zabbix_defaults_by_host.get(str(row["ID_HOST"])) or {}
            file_path = str(defaults.get("file_path") or "").strip()
            extension = str(defaults.get("extension") or "").strip()

            if not file_path or not extension:
                fallback_rows.append(row)
                continue

            batches_by_backup_defaults.setdefault(
                (file_path, extension),
                [],
            ).append(row["ID_HOST"])

        batches = []
        for (file_path, extension), host_ids in batches_by_backup_defaults.items():
            merged_filter = dict(filter_data)
            merged_filter.update(
                {
                    "file_path": file_path,
                    "extension": extension,
                }
            )
            batches.append({"hosts": host_ids, "filter_data": merged_filter})

        if not fallback_rows:
            return batches

        return batches + _build_collective_task_batches(
            host_rows=fallback_rows,
            filter_data=filter_data,
            profile_overrides=profile_overrides,
        )

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
    legacy_task_type = _normalize_selected_task_type(request.args.get("task_type"))
    legacy_mode = _normalize_filter_mode(request.args.get("mode", "NONE"))
    selected_action = _normalize_task_action(
        request.args.get("action")
        or _action_from_legacy_selection(legacy_task_type, legacy_mode)
    )
    selected_action_definition = _resolve_task_action(selected_action)
    selected_task_type = str(selected_action_definition["task_type"])
    selected_execution_type = request.args.get("execution_type", "individual")
    selected_host_filter = request.args.get("host_filter", "ALL")
    selected_mode = _resolve_action_mode(
        selected_action_definition,
        legacy_mode,
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

    selected_filter_defaults_custom = not _looks_like_auto_filter_defaults(
        {
            "file_path": selected_file_path,
            "extension": selected_extension,
        }
    )

    # --------------------------------------------------
    # Discover host prefixes dynamically
    # --------------------------------------------------
    cursor.execute("""
        SELECT
            CASE
                WHEN UPPER(NA_HOST_NAME) LIKE 'UMS%' THEN 'UMS300'
                WHEN UPPER(NA_HOST_NAME) LIKE 'ERMX%' THEN 'ERMX'
                ELSE REGEXP_SUBSTR(UPPER(NA_HOST_NAME), '^[A-Z]+')
            END AS PREFIX,
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

        selected_action = _normalize_task_action(request.form.get("action"))
        action_definition = _resolve_task_action(selected_action)
        task_type = int(action_definition["task_type"])
        execution_type = request.form.get("execution_type")

        if selected_action == TASK_ACTION_CONNECTIVITY_TEST:
            raw_host_id = request.form.get("host_id")
            try:
                host_id = int(raw_host_id)
            except (TypeError, ValueError):
                host_id = None

            if execution_type != "individual" or not host_id:
                return redirect(url_for("task.task_builder", action=selected_action))

            queue_interactive_connectivity_test(db, host_id)
            return redirect(
                url_for(
                    "task.task_list",
                    queued_count=1,
                    skipped_count=0,
                )
            )

        mode = _resolve_action_mode(
            action_definition,
            request.form.get("mode"),
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
                if str(host_filter).upper() == "UMS300":
                    query += " AND UPPER(NA_HOST_NAME) LIKE %s"
                    params.append("UMS%")
                else:
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

                zabbix_defaults_by_host = {}
                if task_type == HOST_TASK_CHECK_TYPE:
                    zabbix_defaults_by_host = _parse_collective_zabbix_defaults(
                        request.form.get("collective_zabbix_defaults"),
                        [host_row["ID_HOST"] for host_row in selected_hosts],
                    )

                for batch in _build_collective_task_batches(
                    host_rows=selected_hosts,
                    filter_data=filter_data,
                    profile_overrides=profile_overrides,
                    zabbix_defaults_by_host=zabbix_defaults_by_host,
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
    record_page_view()
    return render_template(
        "task/task_builder.html",
        hosts=hosts,
        host_prefixes=host_prefixes,
        online_only=online_only,
        selected_host=selected_host,
        selected_action=selected_action,
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
        selected_filter_defaults_custom=selected_filter_defaults_custom,
        selected_collective_host_ids=selected_collective_host_ids,
        selected_collective_host_search=selected_collective_host_search,
        station_profile_rows=station_profile_rows,
        task_actions=TASK_ACTIONS,
        backup_task_type=HOST_TASK_CHECK_TYPE,
        stop_task_type=HOST_TASK_BACKLOG_ROLLBACK_TYPE,
    )


@task_bp.route("/api/host/<int:host_id>/backup-defaults", methods=["GET"])
def task_zabbix_backup_defaults(host_id):
    """Provide one station's effective backup path and extension on demand."""
    defaults = {"file_path": None, "extension": None}

    try:
        configuration = get_configuration(TARGET_KIND_HOST, str(host_id))
        defaults = _extract_zabbix_backup_defaults(configuration)
    except (ZabbixApiError, ZabbixConfigurationError) as error:
        # Task creation must remain usable when the configuration service is
        # unavailable, so the browser falls back to its existing family hints.
        current_app.logger.warning("task_zabbix_defaults_unavailable: %s", error)

    return jsonify(
        {
            **defaults,
            "source": "zabbix" if any(defaults.values()) else "fallback",
        }
    )


@task_bp.route("/api/hosts/backup-defaults", methods=["GET"])
def task_zabbix_collective_backup_defaults():
    """Provide effective backup defaults for a bounded collective selection."""
    host_ids = sorted(
        {
            str(host_id)
            for host_id in request.args.getlist("host_id")
            if str(host_id).isdigit() and int(str(host_id)) > 0
        },
        key=int,
    )

    if len(host_ids) > MAX_COLLECTIVE_ZABBIX_DEFAULT_HOSTS:
        return jsonify(
            {
                "defaults": {},
                "source": "fallback",
                "message": "A seleção coletiva excede o limite de consulta ao Zabbix.",
            }
        ), 400

    try:
        defaults = get_hosts_backup_defaults(host_ids)
    except (ZabbixApiError, ZabbixConfigurationError) as error:
        current_app.logger.warning(
            "task_collective_zabbix_defaults_unavailable: %s",
            error,
        )
        return jsonify(
            {
                "defaults": {},
                "source": "fallback",
                "message": "Não foi possível consultar a configuração coletiva no Zabbix.",
            }
        )

    return jsonify(
        {
            "defaults": defaults,
            "source": "zabbix",
            "message": "Configuração coletiva consultada no Zabbix.",
        }
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

    record_page_view()
    return render_template(
        "task/task_list.html",
        tasks=tasks,
        queued_count=queued_count,
        skipped_count=skipped_count,
        show_creation_summary=queued_count is not None or skipped_count is not None,
    )
