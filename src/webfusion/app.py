from flask import Flask, request, render_template, jsonify
from modules.spectrum.routes import spectrum_bp  # ← IMPORT CORRETO
from modules.host.routes import host_bp
from modules.task.routes import task_bp


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
    app.run(
        host="0.0.0.0",
        port=80
    )
