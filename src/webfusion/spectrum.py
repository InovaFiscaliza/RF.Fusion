from flask import request, render_template_string
import pymysql

# ======================================================================
# Configuração do banco
# ======================================================================

DB_CFG = {
    "host": "10.88.0.33",
    "port": 3306,
    "user": "root",
    "password": "changeme",
    "database": "RFDATA",
    "cursorclass": pymysql.cursors.DictCursor,
    "connect_timeout": 5
}

# ======================================================================
# HTML
# ======================================================================

HTML_SPECTRUM = """
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>RF.Fusion – Spectrum Query</title>

<style>
body { font-family: Arial; margin: 20px; }
label { margin-right: 10px; }
table { border-collapse: collapse; margin-top: 20px; width: 100%; font-size: 12px; }
th, td { border: 1px solid #ccc; padding: 4px; text-align: center; }
th { background: #f0f0f0; position: sticky; top: 0; }
.msg { margin-top: 15px; color: #555; }
</style>
</head>
<body>

<h2>Consulta Spectrum</h2>

<form method="get">
<label>
Equipamento:
<select name="equipment_id" required>
<option value="">-- selecione --</option>
{% for e in equipments %}
<option value="{{ e.ID_EQUIPMENT }}"
{% if e.ID_EQUIPMENT|string == equipment_id %}selected{% endif %}>
{{ e.NA_EQUIPMENT }}
</option>
{% endfor %}
</select>
</label>

<label>
Data início:
<input type="date" name="start_date" value="{{ start_date }}" required>
</label>

<label>
Data fim:
<input type="date" name="end_date" value="{{ end_date }}">
</label>

<button type="submit">Consultar</button>
</form>

{% if message %}
<div class="msg">{{ message }}</div>
{% endif %}

{% if rows %}
<table>
<thead>
<tr>
<th>ID_SPECTRUM</th>
<th>FK_SITE</th>
<th>FK_DETECTOR</th>
<th>FK_TRACE_TYPE</th>
<th>FK_MEASURE_UNIT</th>
<th>FK_PROCEDURE</th>
<th>NA_DESCRIPTION</th>
<th>FREQ_START</th>
<th>FREQ_END</th>
<th>DT_START</th>
<th>DT_END</th>
<th>SAMPLE_DURATION</th>
<th>TRACE_COUNT</th>
<th>TRACE_LENGTH</th>
<th>RBW</th>
<th>VBW</th>
<th>ATT_GAIN</th>
<th>EQUIPMENT</th>
</tr>
</thead>
<tbody>
{% for r in rows %}
<tr>
<td>{{ r.ID_SPECTRUM }}</td>
<td>{{ r.FK_SITE }}</td>
<td>{{ r.FK_DETECTOR }}</td>
<td>{{ r.FK_TRACE_TYPE }}</td>
<td>{{ r.FK_MEASURE_UNIT }}</td>
<td>{{ r.FK_PROCEDURE }}</td>
<td>{{ r.NA_DESCRIPTION }}</td>
<td>{{ r.NU_FREQ_START }}</td>
<td>{{ r.NU_FREQ_END }}</td>
<td>{{ r.DT_TIME_START }}</td>
<td>{{ r.DT_TIME_END }}</td>
<td>{{ r.NU_SAMPLE_DURATION }}</td>
<td>{{ r.NU_TRACE_COUNT }}</td>
<td>{{ r.NU_TRACE_LENGTH }}</td>
<td>{{ r.NU_RBW }}</td>
<td>{{ r.NU_VBW }}</td>
<td>{{ r.NU_ATT_GAIN }}</td>
<td>{{ r.NA_EQUIPMENT }}</td>
</tr>
{% endfor %}
</tbody>
</table>
{% endif %}

</body>
</html>
"""

# ======================================================================
# Rota /spectrum
# ======================================================================

def register_spectrum(app):

    @app.route("/spectrum", methods=["GET"])
    def spectrum():

        equipment_id = request.args.get("equipment_id")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")

        conn = pymysql.connect(**DB_CFG)

        with conn.cursor() as cur:

            # ------------------------------------------------------------
            # Combobox: somente equipamentos com spectrum associado
            # ------------------------------------------------------------
            cur.execute("""
                SELECT DISTINCT
                    e.ID_EQUIPMENT,
                    e.NA_EQUIPMENT
                FROM DIM_SPECTRUM_EQUIPMENT e
                JOIN BRIDGE_SPECTRUM_EQUIPMENT b
                  ON b.FK_EQUIPMENT = e.ID_EQUIPMENT
                JOIN FACT_SPECTRUM f
                  ON f.ID_SPECTRUM = b.FK_SPECTRUM
                ORDER BY e.NA_EQUIPMENT
            """)
            equipments = cur.fetchall()

            rows = []
            message = None

            # ------------------------------------------------------------
            # Consulta principal (overlap temporal)
            # ------------------------------------------------------------
            if equipment_id and start_date:

                params = [equipment_id, start_date]

                sql = """
                    SELECT
                        f.ID_SPECTRUM,
                        f.FK_SITE,
                        f.FK_DETECTOR,
                        f.FK_TRACE_TYPE,
                        f.FK_MEASURE_UNIT,
                        f.FK_PROCEDURE,
                        f.NA_DESCRIPTION,
                        f.NU_FREQ_START,
                        f.NU_FREQ_END,
                        f.DT_TIME_START,
                        f.DT_TIME_END,
                        f.NU_SAMPLE_DURATION,
                        f.NU_TRACE_COUNT,
                        f.NU_TRACE_LENGTH,
                        f.NU_RBW,
                        f.NU_VBW,
                        f.NU_ATT_GAIN,
                        e.NA_EQUIPMENT
                    FROM FACT_SPECTRUM f
                    JOIN BRIDGE_SPECTRUM_EQUIPMENT b
                      ON b.FK_SPECTRUM = f.ID_SPECTRUM
                    JOIN DIM_SPECTRUM_EQUIPMENT e
                      ON e.ID_EQUIPMENT = b.FK_EQUIPMENT
                    WHERE e.ID_EQUIPMENT = %s
                      AND f.DT_TIME_END >= %s
                """

                if end_date:
                    sql += " AND f.DT_TIME_START <= %s"
                    params.append(end_date + " 23:59:59")

                sql += " ORDER BY f.DT_TIME_START DESC LIMIT 500"

                cur.execute(sql, params)
                rows = cur.fetchall()

                if not rows:
                    message = "Nenhum registro encontrado para os filtros informados."

        conn.close()

        return render_template_string(
            HTML_SPECTRUM,
            equipments=equipments,
            rows=rows,
            message=message,
            equipment_id=equipment_id,
            start_date=start_date,
            end_date=end_date
        )
