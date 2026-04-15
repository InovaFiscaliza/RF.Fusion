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
import threading
import time

from flask import Flask, request, render_template, jsonify
from waitress import serve
from modules.spectrum.routes import spectrum_bp
from modules.host.routes import host_bp
from modules.server.routes import server_bp
from modules.task.routes import task_bp
from modules.host.service import (
    get_server_backup_error_overview,
    get_server_processing_error_overview,
    get_server_summary_metrics,
)
from modules.map.service import (
    get_station_map_points,
    get_station_map_site_detail,
    start_station_map_background_refresh,
)


app = Flask(__name__)
app.logger.setLevel(logging.INFO)
_STARTUP_PREWARM_LOCK = threading.Lock()
_STARTUP_PREWARM_STARTED = False
SERVER_PREWARM_POLL_SECONDS = 30.0

# ----------------------------------------------------------
# Registro de Blueprints
# ----------------------------------------------------------

app.register_blueprint(spectrum_bp)
app.register_blueprint(host_bp)
app.register_blueprint(server_bp)
app.register_blueprint(task_bp)


def _prime_server_dashboard_caches():
    """Warm heavy server-side dashboard caches when they are due."""

    cache_loaders = [
        ("server_summary_metrics", get_server_summary_metrics),
        ("server_processing_error_overview", get_server_processing_error_overview),
        ("server_backup_error_overview", get_server_backup_error_overview),
    ]

    for cache_name, loader in cache_loaders:
        try:
            loader()
        except Exception:
            app.logger.exception("failed_to_prewarm_%s", cache_name)


def _startup_prewarm_worker():
    """Warm the most expensive shared UI caches in the background."""

    try:
        start_station_map_background_refresh()
    except Exception:
        app.logger.exception("failed_to_start_station_map_background_refresh")

    while True:
        _prime_server_dashboard_caches()
        time.sleep(SERVER_PREWARM_POLL_SECONDS)


def start_background_prewarm_services():
    """Start one per-process background warm-up worker."""

    global _STARTUP_PREWARM_STARTED

    with _STARTUP_PREWARM_LOCK:
        if _STARTUP_PREWARM_STARTED:
            return False

        _STARTUP_PREWARM_STARTED = True

    worker = threading.Thread(
        target=_startup_prewarm_worker,
        name="webfusion-startup-prewarm",
        daemon=True,
    )
    worker.start()
    return True

@app.route("/")
def index():
    """Render the landing page shell.

    The heavy station data is loaded asynchronously by the browser so the page
    can appear quickly even when the map data takes longer to prepare.
    """
    start_background_prewarm_services()
    return render_template("index.html")


@app.route("/api/map/stations")
def map_stations():
    """
    Return station-map points without blocking the landing page render.
    """
    try:
        start_background_prewarm_services()
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
    start_background_prewarm_services()
    serve(
        app,
        host=os.getenv("WEBFUSION_HOST", "127.0.0.1"),
        port=int(os.getenv("WEBFUSION_PORT", "8000")),
        threads=int(os.getenv("WEBFUSION_THREADS", "8")),
        channel_timeout=int(os.getenv("WEBFUSION_CHANNEL_TIMEOUT", "300")),
        ident="RF.Fusion Web"
    )
