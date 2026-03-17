import os
from urllib.parse import quote
from urllib.parse import urlencode
from flask import Blueprint, Response, jsonify, render_template, request
from werkzeug.wsgi import wrap_file
from modules.spectrum.service import (
    get_spectrum_data,
    get_spectrum_file_data,
    get_equipments,
    get_file_by_file_id,
    get_file_by_spectrum_id,
    get_spectra_by_file_id,
)

spectrum_bp = Blueprint("spectrum", __name__)


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
    query_mode = request.args.get("query_mode", "spectrum")
    if query_mode not in {"spectrum", "file"}:
        query_mode = "spectrum"

    equipment_id = _normalize_optional_arg(request.args.get("equipment_id"))
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

    equipments = get_equipments()
    equipment_name_by_id = {
        str(item["ID_EQUIPMENT"]): item["NA_EQUIPMENT"]
        for item in equipments
    }

    rows = []
    total = 0
    total_pages = 0
    visible_pages = []

    # ---------------------------
    # Consulta por equipamento
    # ---------------------------
    # Once an equipment is selected, empty optional filters should mean
    # "return all spectra for this equipment", not "skip the query".
    if equipment_id:
        if query_mode == "file":
            rows, total = get_spectrum_file_data(
                equipment_id=equipment_id,
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

        total_pages = (total + page_size - 1) // page_size

        # Ajuste caso usuário tente acessar página maior que total_pages
        if total_pages > 0 and page > total_pages:
            page = total_pages
            if query_mode == "file":
                rows, total = get_spectrum_file_data(
                    equipment_id=equipment_id,
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
            start = max(1, page - 2)
            end = min(total_pages, page + 2)
            visible_pages = list(range(start, end + 1))

        if query_mode == "file":
            selected_equipment_name = equipment_name_by_id.get(str(equipment_id))

            for row in rows:
                row["NA_EQUIPMENT"] = selected_equipment_name

        _annotate_download_urls(rows)

    query_base = urlencode(
        {
            key: value
            for key, value in {
                "equipment_id": equipment_id,
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
        rows=rows,
        query_mode=query_mode,
        equipment_id=equipment_id,
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
        visible_pages=visible_pages,
        page_query_prefix=page_query_prefix,
    )


@spectrum_bp.route("/spectrum/download/<int:spectrum_id>")
def download_spectrum(spectrum_id):

    file_path = get_file_by_spectrum_id(spectrum_id)

    if not file_path:
        return "Arquivo não encontrado", 404

    return _stream_repository_file(file_path)


@spectrum_bp.route("/spectrum/download-file/<int:file_id>")
def download_spectrum_file(file_id):

    file_path = get_file_by_file_id(file_id)

    if not file_path:
        return "Arquivo não encontrado", 404

    return _stream_repository_file(file_path)


@spectrum_bp.route("/api/spectrum/file/<int:file_id>/spectra")
def spectrum_file_spectra(file_id):

    return jsonify({"rows": get_spectra_by_file_id(file_id)})
