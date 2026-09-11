"""Expose named appAnalise queries over HTTP; raw SQL is never accepted."""

from flask import Blueprint, Response, current_app, jsonify, request
from werkzeug.exceptions import BadRequest, HTTPException, UnsupportedMediaType

from . import config as k
from .db_handler import AppAnaliseDB
from .filters import Filters, parse_filters
from .service import table_payload


appanalise_api_bp = Blueprint("appanalise_api", __name__, url_prefix=k.API_PREFIX)


def _request_filters(*, allow_filters: bool = True) -> Filters:
    """Read either GET parameters or a POST JSON filter object.

    Args:
        allow_filters: Whether this endpoint accepts search fields (bool).

    Returns:
        Validated request filters (Filters). Repeated GET districtId values
        become a list; other repeated fields and mixed body/query are rejected.

    Raises:
        BadRequest: On invalid filter values, JSON shapes or ambiguous inputs.
        UnsupportedMediaType: When POST does not contain JSON.
    """
    if request.method == "POST":
        if request.args:
            raise BadRequest("Use apenas o corpo JSON no POST.")
        if not request.is_json:
            raise UnsupportedMediaType("Envie Content-Type: application/json.")
        values = request.get_json()
        if not isinstance(values, dict):
            raise BadRequest("O corpo deve ser um objeto JSON de filtros.")
    else:
        if request.content_length or request.headers.get("Transfer-Encoding"):
            raise BadRequest("GET aceita filtros somente na URL.")
        values = {}
        for key in request.args:
            items = request.args.getlist(key)
            if len(items) > 1 and key != "districtId":
                raise BadRequest(f"O parâmetro {key} não pode ser repetido.")
            values[key] = items if len(items) > 1 else items[0]
    if not allow_filters and values:
        raise BadRequest("Esta rota não aceita filtros.")
    try:
        return parse_filters(values)
    except ValueError as exc:
        raise BadRequest(str(exc)) from exc


@appanalise_api_bp.before_request
def limit_request_body() -> None:
    """Bound filter payloads before reading a POST body.

    Args:
        None. Uses the current Flask request.

    Returns:
        None. Assigns a request-local body limit without changing other blueprints.
    """
    request.max_content_length = k.MAX_BODY_BYTES


@appanalise_api_bp.errorhandler(Exception)
def api_error(error: Exception) -> tuple[Response, int]:
    """Return JSON errors without leaking SQL, connection details or credentials.

    Args:
        error: Validation, HTTP or unexpected execution failure (Exception).

    Returns:
        JSON response and HTTP status (tuple[Response, int]). The response has
        error.code (str) and error.message (str); database failures return 500.
    """
    if isinstance(error, HTTPException):
        status = error.code or 500
        return jsonify({"error": {"code": f"http_{status}", "message": error.description}}), status
    current_app.logger.exception("appanalise_api_request_failed")
    return jsonify({"error": {"code": "query_failed", "message": "Não foi possível consultar os dados."}}), 500


@appanalise_api_bp.get("/stations/summary")
def station_summary() -> Response:
    """Expose DBHandler.getStationSummary.

    Args:
        None. No filters are accepted.

    Returns:
        JSON table with columns and rows from HOST_LOCATION_SUMMARY (Response).
    """
    _request_filters(allow_filters=False)
    return jsonify(table_payload(AppAnaliseDB().get_station_summary()))


@appanalise_api_bp.get("/hosts/<int(min=1):host_id>/stats")
def host_stats(host_id: int) -> Response:
    """Expose DBHandler.getHostStats.

    Args:
        host_id: Positive host identifier from the URL (int).

    Returns:
        JSON table with columns and zero or one host statistics row (Response).
    """
    _request_filters(allow_filters=False)
    return jsonify(table_payload(AppAnaliseDB().get_host_stats(host_id)))


@appanalise_api_bp.route("/equipments", methods=["GET", "POST"])
def equipments() -> Response:
    """Expose DBHandler.getSpectrumEquipments with optional search filters.

    Args:
        None. Reads stateCode, siteId and districtId from URL or JSON filters.

    Returns:
        JSON table with columns and equipment option rows (Response).
    """
    return jsonify(table_payload(AppAnaliseDB().get_spectrum_equipments(_request_filters())))


@appanalise_api_bp.route("/states", methods=["GET", "POST"])
def states() -> Response:
    """Expose DBHandler.getSpectrumStates with optional search filters.

    Args:
        None. Reads equipmentId and siteId from URL or JSON filters.

    Returns:
        JSON table with columns and LC_STATE rows (Response).
    """
    return jsonify(table_payload(AppAnaliseDB().get_spectrum_states(_request_filters())))


@appanalise_api_bp.route("/localities", methods=["GET", "POST"])
def localities() -> Response:
    """Expose DBHandler.getSpectrumLocalities with its required scope rule.

    Args:
        None. Reads URL or JSON spectrum filters; equipmentId or stateCode
            must be present to query the database.

    Returns:
        JSON table with columns and district option rows (Response).
    """
    return jsonify(table_payload(AppAnaliseDB().get_spectrum_localities(_request_filters())))


@appanalise_api_bp.route("/files", methods=["GET", "POST"])
def files() -> Response:
    """Expose DBHandler.getSpectrumFileData, preserving its extra lookahead row.

    Args:
        None. Reads spectrum filters, page and pageSize from URL or JSON.

    Returns:
        JSON table plus pagination (Response). pagination contains page (int),
        page_size (int) and has_more (bool). rows retains up to page_size + 1
        entries for direct compatibility with the MATLAB query.
    """
    filters = _request_filters()
    result = AppAnaliseDB().get_spectrum_file_data(filters)
    payload = table_payload(result)
    payload["pagination"] = {
        "page": filters.page, "page_size": filters.page_size,
        "has_more": len(result.rows) > filters.page_size,
    }
    return jsonify(payload)


@appanalise_api_bp.route("/files/count", methods=["GET", "POST"])
def file_count() -> Response:
    """Expose DBHandler.getSpectrumFileDataCount.

    Args:
        None. Reads spectrum filters from URL or JSON; pagination is ignored.

    Returns:
        JSON object with count (int), the number of matching files (Response).
    """
    return jsonify({"count": AppAnaliseDB().get_spectrum_file_data_count(_request_filters())})


@appanalise_api_bp.get("/files/<int(min=1):file_id>/spectra")
def file_spectra(file_id: int) -> Response:
    """Expose DBHandler.getSpectraByFileId.

    Args:
        file_id: Positive repository file identifier from the URL (int).

    Returns:
        JSON table with columns and all spectra linked to the file (Response).
    """
    _request_filters(allow_filters=False)
    return jsonify(table_payload(AppAnaliseDB().get_spectra_by_file_id(file_id)))
