"""Application entrypoint for the WebFusion web interface.

This module creates the Flask app, registers the feature blueprints, and keeps
only a few small routes that are truly cross-module:

- the landing page
- the station-map JSON APIs
- a small legacy popup helper
- the health endpoint used by the container
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

# ----------------------------------------------------------
# Registro de Blueprints
# ----------------------------------------------------------

app.register_blueprint(spectrum_bp)
app.register_blueprint(host_bp)
app.register_blueprint(server_bp)
app.register_blueprint(task_bp)

@app.route("/")
def index():
    """Render the landing page shell.

    The heavy station data is loaded asynchronously by the browser so the page
    can appear quickly even when the map data takes longer to prepare.
    """
    return render_template("index.html")


@app.route("/api/map/stations")
def map_stations():
    """
    Return station-map points without blocking the landing page render.
    """
    try:
        return jsonify({"points": get_station_map_points()})
    except Exception:
        app.logger.exception("failed_to_build_station_map")
        # The UI treats an empty dataset as a degraded-but-usable state.
        return jsonify({"points": []})


@app.route("/api/map/stations/<int:site_id>")
def map_station_detail(site_id):
    """
    Return popup actions for a single station point.
    """
    try:
        return jsonify(get_station_map_site_detail(site_id))
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
    """Return a tiny response used by container health checks."""
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
