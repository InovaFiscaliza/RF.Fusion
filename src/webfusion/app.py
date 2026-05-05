"""Application entrypoint for the WebFusion web interface.

This module owns only the routes that do not fit cleanly inside one feature
package:

- the landing page shell
- the summary-backed station-map APIs used by that page
- the small legacy popup helper used by older task flows
- the container health endpoint

All feature-specific pages live in blueprints under ``modules/``.
"""

import os
import logging

from flask import Flask, request, render_template, jsonify
from waitress import serve
from modules.spectrum.routes import spectrum_bp
from modules.host.routes import host_bp
from modules.server.routes import server_bp
from modules.task.routes import task_bp
from modules.map.service import (
    get_station_map_points,
    get_station_map_site_detail,
)


app = Flask(__name__)
app.logger.setLevel(logging.INFO)

# Register feature blueprints first; the app-level routes below are kept only
# for the landing page and a few small cross-module helpers.

app.register_blueprint(spectrum_bp)
app.register_blueprint(host_bp)
app.register_blueprint(server_bp)
app.register_blueprint(task_bp)

@app.route("/")
def index():
    """Render the landing page shell.

    The station data is loaded asynchronously by the browser so the page can
    appear quickly while the map API resolves in parallel.
    """
    return render_template("index.html")


@app.route("/api/map/stations")
def map_stations():
    """Return the cached summary-backed station map payload.

    The page intentionally renders before this endpoint resolves, so failures
    should degrade to an empty map instead of breaking the whole landing page.
    """
    start_date = request.args.get("start_date") or None
    end_date = request.args.get("end_date") or None

    try:
        return jsonify(
            {
                "points": get_station_map_points(
                    start_date=start_date,
                    end_date=end_date,
                )
            }
        )
    except Exception:
        app.logger.exception("failed_to_build_station_map")
        # The UI treats an empty dataset as a degraded-but-usable state.
        return jsonify({"points": []})


@app.route("/api/map/stations/<int:site_id>")
def map_station_detail(site_id):
    """Return popup metadata for one map point.

    The popup is loaded on demand after the operator focuses a site, which
    keeps the initial map payload smaller than embedding every station detail
    into the first HTML response.
    """
    start_date = request.args.get("start_date") or None
    end_date = request.args.get("end_date") or None

    try:
        return jsonify(
            get_station_map_site_detail(
                site_id,
                start_date=start_date,
                end_date=end_date,
            )
        )
    except Exception:
        app.logger.exception("failed_to_build_station_map_site_detail", extra={"site_id": site_id})
        return jsonify(
            {
                "site_id": site_id,
                "stations": [],
                "has_online_host": False,
                "has_known_host": False,
            }
        )

@app.route("/popup", methods=["GET", "POST"])
def popup():
    """Render the legacy popup used to preview host-task filter payloads.

    This helper is still useful while the task builder and older operational
    flows coexist. It stays in the app module because it does not belong to a
    single WebFusion feature page.
    """

    hostid = request.args.get("hostid")
    hostname = request.args.get("hostname")

    filter_json = None

    if request.method == "POST":
        mode = request.form.get("mode")

        # Mirror the filter contract expected by appCataloga workers.
        filter_data = {
            "mode": mode,
            "start_date": None,
            "end_date": None,
            "last_n_files": None,
            "extension": request.form.get("extension"),
            "file_path": request.form.get("file_path"),
            "file_name": None,
        }

        if mode == "RANGE":
            filter_data["start_date"] = request.form.get("start_date") or None
            filter_data["end_date"] = request.form.get("end_date") or None

        if mode == "LAST":
            filter_data["last_n_files"] = (
                int(request.form.get("last_n_files"))
                if request.form.get("last_n_files") else None
            )

        if mode == "FILE":
            filter_data["file_name"] = request.form.get("file_name") or None

        filter_json = jsonify(filter_data).get_data(as_text=True)

    return render_template(
        "popup/popup.html",
        hostid=hostid,
        hostname=hostname,
        filter_json=filter_json
    )

@app.route("/health")
def health():
    """Return the minimal liveness response used by container health checks."""
    return {"status": "ok"}

if __name__ == "__main__":
    serve(
        app,
        host=os.getenv("WEBFUSION_HOST", "127.0.0.1"),
        port=int(os.getenv("WEBFUSION_PORT", "8000")),
        threads=int(os.getenv("WEBFUSION_THREADS", "8")),
        channel_timeout=int(os.getenv("WEBFUSION_CHANNEL_TIMEOUT", "300")),
        ident="RF.Fusion Web"
    )
