import os
from flask import Blueprint, render_template, request, send_file, abort
from modules.spectrum.service import (
    get_spectrum_data,
    get_equipments,
    get_file_by_spectrum_id   # <<< IMPORTANTE
)

spectrum_bp = Blueprint("spectrum", __name__)


@spectrum_bp.route("/spectrum", methods=["GET"])
def spectrum():

    equipment_id = request.args.get("equipment_id")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    sort_by = request.args.get("sort_by", "date_start")
    sort_order = request.args.get("sort_order", "DESC")

    # ---------------------------
    # Sanitização da página
    # ---------------------------
    try:
        page = int(request.args.get("page", 1))
        if page < 1:
            page = 1
    except Exception:
        page = 1

    page_size = 50

    equipments = get_equipments()

    rows = []
    total = 0
    total_pages = 0
    visible_pages = []

    # ---------------------------
    # Só consulta se filtro mínimo for informado
    # ---------------------------
    if equipment_id and start_date:

        rows, total = get_spectrum_data(
            equipment_id=equipment_id,
            start_date=start_date,
            end_date=end_date,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            page_size=page_size
        )

        total_pages = (total + page_size - 1) // page_size

        # Ajuste caso usuário tente acessar página maior que total_pages
        if total_pages > 0 and page > total_pages:
            page = total_pages

        # ---------------------------
        # Paginação numérica inteligente
        # ---------------------------
        if total_pages > 0:
            start = max(1, page - 2)
            end = min(total_pages, page + 2)
            visible_pages = list(range(start, end + 1))

    return render_template(
        "spectrum/spectrum.html",
        equipments=equipments,
        rows=rows,
        equipment_id=equipment_id,
        start_date=start_date,
        end_date=end_date,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        total_pages=total_pages,
        total=total,
        visible_pages=visible_pages
    )
    
@spectrum_bp.route("/spectrum/download/<int:spectrum_id>")
def download_spectrum(spectrum_id):

    file_path = get_file_by_spectrum_id(spectrum_id)

    if not file_path or not os.path.exists(file_path):
        return "Arquivo não encontrado", 404

    return send_file(
        file_path,
        as_attachment=True,
        download_name=os.path.basename(file_path),
        conditional=True
    )