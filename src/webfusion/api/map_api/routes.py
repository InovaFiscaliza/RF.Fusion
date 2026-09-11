"""Serve the single map contract used by browsers and external clients."""

from flask import Blueprint, Response, current_app, jsonify, request

from . import config as k
from .service import get_station_map_dataset, get_station_map_site_detail


map_api_bp = Blueprint("map_api", __name__, url_prefix=k.API_PREFIX)


@map_api_bp.get("/stations")
def map_stations() -> Response:
    """Return map points and optionally the matching site details in one read.

    Args:
        None. Reads optional start_date and end_date (YYYY-MM-DD strings) and
        include_details=true (str) from the current Flask query parameters.

    Returns:
        JSON response (Response) with points (list[dict[str, object]]). When
        requested, also includes site_details (list[dict[str, object]]).
        Database failures preserve the existing empty-map degradation behavior.
    """
    include_details = request.args.get("include_details", "").lower() == "true"
    try:
        payload = get_station_map_dataset(
            start_date=request.args.get("start_date") or None,
            end_date=request.args.get("end_date") or None,
        )
        if not include_details:
            payload.pop("site_details")
        return jsonify(payload)
    except Exception:
        current_app.logger.exception("failed_to_build_station_map")
        payload = {"points": []}
        if include_details:
            payload["site_details"] = []
        return jsonify(payload)


@map_api_bp.get("/stations/<int:site_id>")
def map_station_detail(site_id: int) -> Response:
    """Return the popup data for one site using the shared map rules.

    Args:
        site_id: Selected site identifier (int). The Flask request also accepts
            optional start_date and end_date strings (YYYY-MM-DD).

    Returns:
        JSON detail (Response) with site_id, stations and availability flags.
        Failures retain the original route's empty popup response.
    """
    try:
        return jsonify(get_station_map_site_detail(
            site_id,
            start_date=request.args.get("start_date") or None,
            end_date=request.args.get("end_date") or None,
        ))
    except Exception:
        current_app.logger.exception("failed_to_build_station_map_site_detail", extra={"site_id": site_id})
        return jsonify({
            "site_id": site_id,
            "stations": [],
            "has_online_host": False,
            "has_known_host": False,
        })
