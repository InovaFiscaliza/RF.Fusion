"""Expose the authenticated upload page and single-file API."""

import errno
from http import HTTPStatus

from flask import Blueprint, Response, current_app, g, jsonify, render_template, request
from werkzeug.exceptions import RequestEntityTooLarge

from modules.upload import config as k, service

upload_bp = Blueprint("upload", __name__)


@upload_bp.before_request
def _require_identity() -> Response | tuple[Response, int] | None:
    """Reject anonymous requests before reading multipart data.

    Args:
        None. Reads the normalized proxy identity from flask.g.

    Returns:
        None for identified users, or a JSON response and HTTP status (int).
    """
    if not getattr(g, "webfusion_user", {}).get("email"):
        return jsonify(error="Identifique-se para enviar arquivos."), HTTPStatus.UNAUTHORIZED
    if request.method == "POST":
        # A custom header requires a CORS preflight for cross-origin browser uploads.
        if request.headers.get(k.REQUEST_HEADER) != k.REQUEST_HEADER_VALUE:
            return jsonify(error="Origem do envio não autorizada."), HTTPStatus.FORBIDDEN
        request.max_content_length = k.MAX_REQUEST_BYTES
        request.max_form_parts = k.MAX_FORM_PARTS
    return None


@upload_bp.get("/upload")
def upload_page() -> str:
    """Render the upload workspace using the shared application shell.

    Args:
        None.

    Returns:
        str: Rendered HTML with the file limit and destination.
    """
    stations = []
    station_error = ""
    try:
        stations = service.get_stations()
    except service.StationListUnavailable as exc:
        current_app.logger.exception("upload_station_catalog_failed")
        station_error = str(exc)
    return render_template("upload/upload.html", max_file_bytes=k.MAX_FILE_BYTES,
                           stations=stations, station_error=station_error,
                           drive_test_types=k.DRIVE_TEST_TYPES,
                           upload_folder=str(k.UPLOAD_FOLDER),
                           request_header=k.REQUEST_HEADER,
                           request_header_value=k.REQUEST_HEADER_VALUE)


@upload_bp.errorhandler(RequestEntityTooLarge)
def _too_large(error: RequestEntityTooLarge) -> tuple[Response, int]:
    """Translate body, multipart and stream limits into a JSON error.

    Args:
        error: Raised request size exception. Type: RequestEntityTooLarge.

    Returns:
        tuple: JSON error response and HTTP 413 status (int).
    """
    limit_mib = k.MAX_FILE_BYTES // (1024 * 1024)
    return jsonify(error=f"O envio excede o limite permitido de {limit_mib} MiB por arquivo."), HTTPStatus.REQUEST_ENTITY_TOO_LARGE


@upload_bp.post("/api/upload")
def upload_file() -> tuple[Response, int]:
    """Validate multipart input and save one file for the browser upload queue.

    Args:
        None. Reads one multipart `file` plus category and its optional selector.

    Returns:
        tuple: JSON response and HTTP status (int). Success contains name (str),
        size (int), folder (str) and elapsed_sec (float); failures contain error (str).
    """
    files = list(request.files.items(multi=True))
    if len(files) != 1 or files[0][0] != "file":
        return jsonify(error="Envie um arquivo por requisição."), HTTPStatus.BAD_REQUEST
    if set(request.form) - k.FORM_FIELDS or any(len(request.form.getlist(key)) != 1 for key in request.form):
        return jsonify(error="Classificação do upload inválida."), HTTPStatus.BAD_REQUEST
    try:
        result = service.save_upload(files[0][1], request.form.get("category", ""),
                                     request.form.get("station", ""), request.form.get("drive_type", ""))
    except service.StationListUnavailable as exc:
        current_app.logger.exception("upload_station_catalog_failed")
        return jsonify(error=str(exc)), HTTPStatus.SERVICE_UNAVAILABLE
    except ValueError as exc:
        return jsonify(error=str(exc)), HTTPStatus.BAD_REQUEST
    except FileExistsError:
        return jsonify(error="Já existe um arquivo com esse nome. Renomeie-o antes de enviar."), HTTPStatus.CONFLICT
    except OSError as exc:
        current_app.logger.exception("upload_storage_failed")
        if exc.errno in {errno.ENOSPC, errno.EDQUOT}:
            return jsonify(error="O servidor está sem espaço para receber o arquivo."), HTTPStatus.INSUFFICIENT_STORAGE
        return jsonify(error="A pasta de upload está indisponível para gravação."), HTTPStatus.SERVICE_UNAVAILABLE
    current_app.logger.info("upload_saved", extra={"upload_name": result["name"],
                                                "upload_size": result["size"],
                                                "upload_folder": result["folder"],
                                                "upload_user": g.webfusion_user["email"]})
    return jsonify(result), HTTPStatus.CREATED
