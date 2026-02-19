from flask import Blueprint, render_template, request
from db import get_connection

spectrum_bp = Blueprint("spectrum", __name__)

@spectrum_bp.route("/spectrum", methods=["GET"])
def spectrum():

    equipment_id = request.args.get("equipment_id")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    conn = get_connection()
    cur = conn.cursor()

    # ---------------------------------------------------------
    # Combobox de equipamentos
    # ---------------------------------------------------------
    cur.execute("""
        SELECT DISTINCT
            e.ID_EQUIPMENT,
            e.NA_EQUIPMENT
        FROM DIM_SPECTRUM_EQUIPMENT e
        JOIN FACT_SPECTRUM f
          ON f.FK_EQUIPMENT = e.ID_EQUIPMENT
        ORDER BY e.NA_EQUIPMENT
    """)
    equipments = cur.fetchall()

    rows = []

        # ---------------------------------------------------------
    # Consulta principal
    # ---------------------------------------------------------
    if equipment_id and start_date:

        sql = """
            SELECT
                f.ID_SPECTRUM,
                f.NA_DESCRIPTION,
                f.NU_FREQ_START,
                f.NU_FREQ_END,
                f.DT_TIME_START,
                f.DT_TIME_END,
                f.NU_TRACE_COUNT,
                f.NU_TRACE_LENGTH,
                f.NU_RBW,
                f.NU_VBW,
                f.NU_ATT_GAIN,
                e.NA_EQUIPMENT,

                repos.NA_PATH,
                repos.NA_FILE,
                repos.NA_EXTENSION,
                repos.VL_FILE_SIZE_KB

            FROM FACT_SPECTRUM f

            JOIN DIM_SPECTRUM_EQUIPMENT e
                ON e.ID_EQUIPMENT = f.FK_EQUIPMENT

            /* Subquery para evitar duplicação */
            LEFT JOIN (
                SELECT
                    b.FK_SPECTRUM,
                    MAX(d.ID_FILE) AS ID_FILE
                FROM BRIDGE_SPECTRUM_FILE b
                JOIN DIM_SPECTRUM_FILE d
                    ON d.ID_FILE = b.FK_FILE
                WHERE d.NA_VOLUME = 'reposfi'
                GROUP BY b.FK_SPECTRUM
            ) latest
                ON latest.FK_SPECTRUM = f.ID_SPECTRUM

            LEFT JOIN DIM_SPECTRUM_FILE repos
                ON repos.ID_FILE = latest.ID_FILE

            WHERE f.FK_EQUIPMENT = %s
              AND f.DT_TIME_END >= %s
        """

        params = [equipment_id, start_date]

        if end_date:
            sql += " AND f.DT_TIME_START <= %s"
            params.append(end_date + " 23:59:59")

        sql += " ORDER BY f.DT_TIME_START DESC LIMIT 500"

        cur.execute(sql, params)
        rows = cur.fetchall()


    conn.close()

    return render_template(
        "spectrum/spectrum.html",
        equipments=equipments,
        rows=rows,
        equipment_id=equipment_id,
        start_date=start_date,
        end_date=end_date
    )
