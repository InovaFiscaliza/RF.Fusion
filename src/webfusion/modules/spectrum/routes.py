"""Routes for spectrum browsing, file mode, and repository downloads."""

import os
import time
from urllib.parse import quote
from urllib.parse import urlencode
from flask import Blueprint, Response, current_app, jsonify, render_template, request
from werkzeug.wsgi import wrap_file
from modules.spectrum.service import (
    get_spectrum_data,
    get_spectrum_file_data,
    get_equipments,
    get_spectrum_locality_options,
    get_spectrum_site_option,
    get_spectrum_site_availability_range,
    get_file_by_file_id,
    get_file_by_spectrum_id,
    get_spectra_by_file_id,
)

spectrum_bp = Blueprint("spectrum", __name__)


def _build_visible_page_slots(page, total_pages, max_slots=5):
    """Return a stable page-slot window for the numeric paginator.

    The UI feels jumpy when the amount of visible page buttons changes near the
    beginning or the end of the result set. Returning a fixed-length list keeps
    the paginator footprint stable while still centering the current page when
    possible.
    """

    if total_pages <= 0:
        return []

    max_slots = max(1, int(max_slots))

    if total_pages <= max_slots:
        return list(range(1, total_pages + 1))

    half_window = max_slots // 2
    start = max(1, page - half_window)
    end = start + max_slots - 1

    if end > total_pages:
        end = total_pages
        start = end - max_slots + 1

    return list(range(start, end + 1))


def _build_public_download_url(file_path):
    """
    Build a direct nginx-served download URL for repository-backed files.

    When the spectrum table already knows the repository path, the fastest
    option is to let the browser request the file straight from nginx instead
    of going through Flask again just to resolve the same path.
    """
    if not file_path:
        return None

    public_prefix = os.getenv("WEBFUSION_PUBLIC_DOWNLOAD_PREFIX", "/downloads").strip()
    repo_root = os.getenv("WEBFUSION_ACCEL_REDIRECT_ROOT", "/mnt/reposfi").strip()

    normalized_root = os.path.abspath(repo_root)
    normalized_path = os.path.abspath(file_path)

    try:
        relative_path = os.path.relpath(normalized_path, normalized_root)
    except ValueError:
        return None

    if relative_path.startswith(".."):
        return None

    quoted_relative_path = quote(relative_path.replace(os.sep, "/"), safe="/")
    return f"{public_prefix.rstrip('/')}/{quoted_relative_path.lstrip('/')}"


def _build_accel_redirect_response(file_path):
    """
    Offload repository downloads to nginx when internal redirects are enabled.

    This avoids opening or stat'ing the CIFS-backed file in Python before the
    first byte is sent. When the container is fronted by nginx, it is a much
    better fit to return only the download headers plus ``X-Accel-Redirect``.
    """
    accel_prefix = os.getenv("WEBFUSION_ACCEL_REDIRECT_PREFIX", "").strip()

    if not accel_prefix:
        return None

    repo_root = os.getenv("WEBFUSION_ACCEL_REDIRECT_ROOT", "/mnt/reposfi").strip()
    normalized_root = os.path.abspath(repo_root)
    normalized_path = os.path.abspath(file_path)

    try:
        relative_path = os.path.relpath(normalized_path, normalized_root)
    except ValueError:
        return None

    if relative_path.startswith(".."):
        return None

    accel_uri = quote(relative_path.replace(os.sep, "/"), safe="/")
    filename = os.path.basename(file_path)
    quoted_filename = quote(filename)

    response = Response(status=200)
    response.headers["Content-Type"] = "application/octet-stream"
    response.headers["Content-Disposition"] = (
        f"attachment; filename*=UTF-8''{quoted_filename}"
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Accel-Buffering"] = "no"
    response.headers["X-Accel-Redirect"] = (
        f"{accel_prefix.rstrip('/')}/{accel_uri.lstrip('/')}"
    )
    return response


def _stream_repository_file(file_path):
    """
    Stream a repository file with minimal filesystem metadata overhead.

    The previous path-based `send_file(..., conditional=True)` flow is
    convenient, but it tends to perform extra stat/conditional work against the
    CIFS-mounted repository. Opening the file once and streaming it directly is
    a better fit for this deployment shape.
    """
    accel_response = _build_accel_redirect_response(file_path)
    if accel_response is not None:
        return accel_response

    try:
        file_handle = open(file_path, "rb")
    except OSError:
        return "Arquivo não encontrado", 404

    try:
        file_size = os.fstat(file_handle.fileno()).st_size
    except OSError:
        file_size = None

    filename = os.path.basename(file_path)
    quoted_filename = quote(filename)

    response = Response(
        wrap_file(request.environ, file_handle),
        mimetype="application/octet-stream",
        direct_passthrough=True,
    )
    response.call_on_close(file_handle.close)
    response.headers["Content-Disposition"] = (
        f"attachment; filename*=UTF-8''{quoted_filename}"
    )
    response.headers["Cache-Control"] = "no-store"
    response.headers["X-Accel-Buffering"] = "no"

    if file_size is not None:
        response.headers["Content-Length"] = str(file_size)

    return response


def _annotate_download_urls(rows):
    """
    Add direct nginx download URLs to query rows when the repository path is known.
    """
    for row in rows:
        file_path = None

        if row.get("NA_PATH") and row.get("NA_FILE"):
            file_path = os.path.join(row["NA_PATH"], row["NA_FILE"])

        row["DOWNLOAD_URL"] = _build_public_download_url(file_path)

    return rows


def _normalize_optional_arg(value):
    """
    Normalize optional query-string values coming from HTML forms.

    Browsers or templates can leak literal strings such as "None" or "null"
    when an optional field is left blank. Those values must not become SQL
    filters like `LIKE '%None%'`.
    """
    if value is None:
        return None

    normalized = value.strip()

    if not normalized:
        return None

    if normalized.lower() in {"none", "null"}:
        return None

    return normalized


@spectrum_bp.route("/spectrum", methods=["GET"])
def spectrum():
    """Render the spectrum query page.

    The page supports two different grains:

    - ``spectrum``: one row per spectrum
    - ``file``: one row per repository file, with expandable linked spectra
    """
    query_mode = request.args.get("query_mode", "spectrum")
    if query_mode not in {"spectrum", "file"}:
        query_mode = "spectrum"

    equipment_id = _normalize_optional_arg(request.args.get("equipment_id"))
    site_id = _normalize_optional_arg(request.args.get("site_id"))
    start_date = _normalize_optional_arg(request.args.get("start_date"))
    end_date = _normalize_optional_arg(request.args.get("end_date"))
    freq_start = _normalize_optional_arg(request.args.get("freq_start"))
    freq_end = _normalize_optional_arg(request.args.get("freq_end"))
    description = _normalize_optional_arg(request.args.get("description"))

    if query_mode == "file":
        freq_start = None
        freq_end = None
        description = None

    sort_by = request.args.get("sort_by", "date_start")
    sort_order = request.args.get("sort_order", "DESC")

    # ---------------------------
    # Sanitização da página
    # ---------------------------
    try:
        page = int(request.args.get("page", 1))
        if page < 1:
            page = 1
    except Exception:
        page = 1

    page_size = 50
    freq_start_value = None
    freq_end_value = None

    try:
        if freq_start:
            freq_start_value = float(freq_start)
    except Exception:
        freq_start = None

    try:
        if freq_end:
            freq_end_value = float(freq_end)
    except Exception:
        freq_end = None

    query_error_message = None

    try:
        equipments = get_equipments()
    except Exception:
        current_app.logger.exception("failed_to_load_spectrum_equipments")
        equipments = []
        query_error_message = "Nao foi possivel carregar o catalogo de estacoes agora."

    equipment_name_by_id = {
        str(item["ID_EQUIPMENT"]): item["NA_EQUIPMENT"]
        for item in equipments
    }

    rows = []
    total = 0
    total_pages = 0
    visible_pages = []
    site_availability_hint = None
    # ---------------------------
    # Consulta por equipamento
    # ---------------------------
    # Once an equipment is selected, empty optional filters should mean
    # "return all spectra for this equipment", not "skip the query".
    if equipment_id:
        query_started_at = time.perf_counter()
        try:
            if query_mode == "file":
                rows, total = get_spectrum_file_data(
                    equipment_id=equipment_id,
                    site_id=site_id,
                    start_date=start_date,
                    end_date=end_date,
                    sort_by=sort_by,
                    sort_order=sort_order,
                    page=page,
                    page_size=page_size
                )
            else:
                rows, total = get_spectrum_data(
                    equipment_id=equipment_id,
                    site_id=site_id,
                    start_date=start_date,
                    end_date=end_date,
                    freq_start=freq_start_value,
                    freq_end=freq_end_value,
                    description=description,
                    sort_by=sort_by,
                    sort_order=sort_order,
                    page=page,
                    page_size=page_size
                )

            total_pages = ((total + page_size - 1) // page_size) if total > 0 else 0

            # Ajuste caso usuário tente acessar página maior que total_pages
            if total_pages > 0 and page > total_pages:
                page = total_pages
                if query_mode == "file":
                    rows, total = get_spectrum_file_data(
                        equipment_id=equipment_id,
                        site_id=site_id,
                        start_date=start_date,
                        end_date=end_date,
                        sort_by=sort_by,
                        sort_order=sort_order,
                        page=page,
                        page_size=page_size
                    )
                else:
                    rows, total = get_spectrum_data(
                        equipment_id=equipment_id,
                        site_id=site_id,
                        start_date=start_date,
                        end_date=end_date,
                        freq_start=freq_start_value,
                        freq_end=freq_end_value,
                        description=description,
                        sort_by=sort_by,
                        sort_order=sort_order,
                        page=page,
                        page_size=page_size
                    )

            # ---------------------------
            # Paginação numérica inteligente
            # ---------------------------
            if total_pages > 0:
                visible_pages = _build_visible_page_slots(page, total_pages)

            if query_mode == "file":
                selected_equipment_name = equipment_name_by_id.get(str(equipment_id))

                for row in rows:
                    row["NA_EQUIPMENT"] = selected_equipment_name

            _annotate_download_urls(rows)

            current_app.logger.info(
                "spectrum_query_completed query_mode=%s equipment_id=%s site_id=%s "
                "start_date=%s end_date=%s rows=%s total=%s elapsed_ms=%.1f",
                query_mode,
                equipment_id,
                site_id,
                start_date,
                end_date,
                len(rows),
                total,
                (time.perf_counter() - query_started_at) * 1000.0,
            )
        except Exception:
            current_app.logger.exception(
                "failed_to_query_spectrum_page query_mode=%s equipment_id=%s site_id=%s "
                "start_date=%s end_date=%s freq_start=%s freq_end=%s description=%s "
                "sort_by=%s sort_order=%s page=%s",
                query_mode,
                equipment_id,
                site_id,
                start_date,
                end_date,
                freq_start,
                freq_end,
                description,
                sort_by,
                sort_order,
                page,
            )
            rows = []
            total = 0
            total_pages = 0
            visible_pages = []
            query_error_message = (
                "Nao foi possivel consultar os registros com esses filtros agora."
            )

    # Preserve the active filters while the user moves through pagination links.
    localities = []

    if site_id:
        try:
            selected_locality = get_spectrum_site_option(site_id)
            if selected_locality:
                localities = [selected_locality]
        except Exception:
            current_app.logger.exception(
                "failed_to_load_selected_spectrum_locality site_id=%s equipment_id=%s query_mode=%s",
                site_id,
                equipment_id,
                query_mode,
            )

    if (
        equipment_id
        and site_id
        and not rows
        and not query_error_message
    ):
        try:
            site_availability_hint = get_spectrum_site_availability_range(
                equipment_id=equipment_id,
                site_id=site_id,
            )
        except Exception:
            current_app.logger.exception(
                "failed_to_load_spectrum_site_availability equipment_id=%s site_id=%s",
                equipment_id,
                site_id,
            )

    query_base = urlencode(
        {
            key: value
            for key, value in {
                "equipment_id": equipment_id,
                "site_id": site_id,
                "query_mode": query_mode,
                "start_date": start_date,
                "end_date": end_date,
                "freq_start": freq_start,
                "freq_end": freq_end,
                "description": description,
                "sort_by": sort_by,
                "sort_order": sort_order,
            }.items()
            if value not in (None, "")
        }
    )
    page_query_prefix = f"{query_base}&" if query_base else ""

    return render_template(
        "spectrum/spectrum.html",
        equipments=equipments,
        localities=localities,
        rows=rows,
        query_mode=query_mode,
        equipment_id=equipment_id,
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        freq_start=freq_start,
        freq_end=freq_end,
        description=description,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        total_pages=total_pages,
        total=total,
        query_error_message=query_error_message,
        site_availability_hint=site_availability_hint,
        visible_pages=visible_pages,
        page_query_prefix=page_query_prefix,
    )


@spectrum_bp.route("/api/spectrum/localities")
def spectrum_localities():
    """Return known localities for one equipment."""

    query_mode = request.args.get("query_mode", "spectrum")
    if query_mode not in {"spectrum", "file"}:
        query_mode = "spectrum"

    equipment_id = _normalize_optional_arg(request.args.get("equipment_id"))

    if not equipment_id:
        return jsonify({"rows": []})

    started_at = time.perf_counter()

    try:
        rows = get_spectrum_locality_options(
            equipment_id=equipment_id,
            query_mode=query_mode,
        )
        current_app.logger.info(
            "spectrum_localities_loaded query_mode=%s equipment_id=%s rows=%s elapsed_ms=%.1f",
            query_mode,
            equipment_id,
            len(rows),
            (time.perf_counter() - started_at) * 1000.0,
        )
        return jsonify({"rows": rows})
    except Exception:
        current_app.logger.exception(
            "failed_to_load_spectrum_localities query_mode=%s equipment_id=%s",
            query_mode,
            equipment_id,
        )
        return jsonify({"rows": []})


@spectrum_bp.route("/spectrum/download/<int:spectrum_id>")
def download_spectrum(spectrum_id):
    """Download the repository file linked to one spectrum row."""

    file_path = get_file_by_spectrum_id(spectrum_id)

    if not file_path:
        return "Arquivo não encontrado", 404

    return _stream_repository_file(file_path)


@spectrum_bp.route("/spectrum/download-file/<int:file_id>")
def download_spectrum_file(file_id):
    """Download a repository file directly from file-mode results."""

    file_path = get_file_by_file_id(file_id)

    if not file_path:
        return "Arquivo não encontrado", 404

    return _stream_repository_file(file_path)


@spectrum_bp.route("/api/spectrum/file/<int:file_id>/spectra")
def spectrum_file_spectra(file_id):
    """Return the spectra listed in the expandable file-mode detail row."""

    try:
        return jsonify({"rows": get_spectra_by_file_id(file_id)})
    except Exception:
        current_app.logger.exception(
            "failed_to_load_spectrum_file_detail file_id=%s",
            file_id,
        )
        return jsonify({"rows": []})
