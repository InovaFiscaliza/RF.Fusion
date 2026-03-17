import os

from flask import Flask, request, render_template, jsonify
from waitress import serve
from modules.spectrum.routes import spectrum_bp  # ← IMPORT CORRETO
from modules.host.routes import host_bp
from modules.task.routes import task_bp
from modules.map.service import (
    get_station_map_points,
    get_station_map_site_detail,
)


app = Flask(__name__)

# ----------------------------------------------------------
# Registro de Blueprints
# ----------------------------------------------------------

app.register_blueprint(spectrum_bp)
app.register_blueprint(host_bp)
app.register_blueprint(task_bp)

# ----------------------------------------------------------
# Página inicial institucional
# ----------------------------------------------------------

@app.route("/")
def index():
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

# ----------------------------------------------------------
# Popup de filtro (temporariamente mantido aqui)
# ----------------------------------------------------------

@app.route("/popup", methods=["GET", "POST"])
def popup():

    hostid = request.args.get("hostid")
    hostname = request.args.get("hostname")

    filter_json = None

    if request.method == "POST":
        mode = request.form.get("mode")

        filter_data = {
            "mode": mode,
            "start_date": None,
            "end_date": None,
            "last_n_files": None,
            "extension": request.form.get("extension"),
            "file_path": request.form.get("file_path"),
            "file_name": None,
            "agent": "local"
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

# ----------------------------------------------------------
# Healthcheck
# ----------------------------------------------------------

@app.route("/health")
def health():
    return {"status": "ok"}

# ----------------------------------------------------------
# Execução
# ----------------------------------------------------------

if __name__ == "__main__":
    serve(
        app,
        host=os.getenv("WEBFUSION_HOST", "127.0.0.1"),
        port=int(os.getenv("WEBFUSION_PORT", "8000")),
        threads=int(os.getenv("WEBFUSION_THREADS", "8")),
        channel_timeout=int(os.getenv("WEBFUSION_CHANNEL_TIMEOUT", "300")),
        ident="RF.Fusion Web"
    )
