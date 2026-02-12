from flask import Flask, request, render_template_string, jsonify
from spectrum import register_spectrum

app = Flask(__name__)

# ======================================================================
# HTML do popup de filtro
# ======================================================================

HTML_POPUP = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>RF.Fusion – Filtro de Arquivos</title>

<style>
body { font-family: Arial; margin: 20px; }
label { display: block; margin-top: 10px; }
fieldset { margin-top: 15px; padding: 10px; }
pre { background: #f4f4f4; padding: 10px; }
</style>

<script>
function updateMode() {
    const mode = document.getElementById("mode").value;

    document.getElementById("range_fields").style.display =
        (mode === "RANGE") ? "block" : "none";

    document.getElementById("last_field").style.display =
        (mode === "LAST") ? "block" : "none";

    document.getElementById("file_field").style.display =
        (mode === "FILE") ? "block" : "none";
}
</script>
</head>

<body onload="updateMode()">

<h2>RF.Fusion – Criar Filtro</h2>

<p><b>Host:</b> {{ hostname }} (ID {{ hostid }})</p>

<form method="post">

<label>
Modo:
<select name="mode" id="mode" onchange="updateMode()">
    <option value="NONE">NONE</option>
    <option value="ALL">ALL</option>
    <option value="RANGE">RANGE</option>
    <option value="LAST">LAST</option>
    <option value="FILE">FILE</option>
</select>
</label>

<fieldset id="range_fields">
<legend>Período</legend>
<label>
Data inicial:
<input type="date" name="start_date">
</label>

<label>
Data final:
<input type="date" name="end_date">
</label>
</fieldset>

<div id="last_field">
<label>
Últimos N arquivos:
<input type="number" name="last_n_files" min="1">
</label>
</div>

<div id="file_field">
<label>
Nome do arquivo:
<input type="text" name="file_name">
</label>
</div>

<label>
Caminho do arquivo:
<select name="file_path">
    <option value="/mnt/internal">Linux: /mnt/internal</option>
    <option value="C:/CelPlan/CellWireless RU/Spectrum/Completed">
        Windows: C:/CelPlan/CellWireless RU/Spectrum/Completed
    </option>
</select>
</label>

<label>
Extensão:
<select name="extension">
    <option value=".bin">Linux: .bin</option>
    <option value=".dbm">Windows: .dbm</option>
</select>
</label>

<input type="hidden" name="agent" value="local">

<button type="submit">Gerar filtro</button>

</form>

{% if filter_json %}
<hr>
<h3>Filtro gerado</h3>
<pre>{{ filter_json }}</pre>
{% endif %}

</body>
</html>
"""

# ======================================================================
# Rota /
# ======================================================================

@app.route("/")
def index():
    return "RF.Fusion Web OK"

# ======================================================================
# Rota /popup
# ======================================================================

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

    return render_template_string(
        HTML_POPUP,
        hostid=hostid,
        hostname=hostname,
        filter_json=filter_json
    )

# ======================================================================
# Registro da página /spectrum
# ======================================================================

register_spectrum(app)

# ======================================================================
# Execução (APENAS PARA TESTE)
# ======================================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=80,
        debug=True
    )
