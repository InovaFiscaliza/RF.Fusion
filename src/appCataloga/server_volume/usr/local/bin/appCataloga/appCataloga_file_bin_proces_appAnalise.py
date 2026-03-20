#!/usr/bin/python3
"""
appAnalise-backed processing worker for heterogeneous station files.

This worker delegates spectral parsing to the external MATLAB-based
`appAnalise` service and keeps the rest of the lifecycle explicit: semantic
validation first, persistence second, and final file resolution last. The flow
stays linear so transient service failures can be distinguished cleanly from
definitive payload failures.
"""

import json
import inspect
import os
import signal
import sys
import time
from datetime import datetime

# ---------------------------------------------------------------
# Configuration path
# ---------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# =================================================
# Config directory
# =================================================
_CFG_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga")
)

if _CFG_DIR not in sys.path and os.path.isdir(_CFG_DIR):
    sys.path.append(_CFG_DIR)

# =================================================
# DB directory
# =================================================
_DB_DIR = os.path.join(PROJECT_ROOT, "db")

if _DB_DIR not in sys.path and os.path.isdir(_DB_DIR):
    sys.path.append(_DB_DIR)

# ---------------------------------------------------------------
# External libraries
# ---------------------------------------------------------------
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# ---------------------------------------------------------------
# Internal modules
# ---------------------------------------------------------------
import config as k
from shared import errors, legacy, logging_utils, tools

from db.dbHandlerBKP import dbHandlerBKP
from db.dbHandlerRFM import dbHandlerRFM
from stations.appAnaliseConnection import AppAnaliseConnection


# ===============================================================
# GLOBAL STATE
# ===============================================================

log = logging_utils.log(target_screen=False)
process_status = {"running": True}


# ===============================================================
# Signal handling
# ===============================================================
def release_busy_hosts_on_exit() -> None:
    """
    Release BUSY hosts held by this worker PID.
    """
    try:
        pid = os.getpid()
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
        db.host_release_by_pid(pid)
    except Exception as e:
        log.error(f"event=cleanup_busy_hosts_failed error={e}")


def _signal_handler(signal_name: str) -> None:
    """
    Register shutdown intent and release BUSY resources.
    """
    fn = inspect.currentframe().f_back.f_code.co_name
    log.signal_received(signal_name, handler=fn)
    process_status["running"] = False
    release_busy_hosts_on_exit()

def sigterm_handler(signal=None, frame=None) -> None:
    """
    Handle SIGTERM by requesting a graceful shutdown.
    """
    _signal_handler("SIGTERM")


def sigint_handler(signal=None, frame=None) -> None:
    """
    Handle SIGINT by requesting a graceful shutdown.
    """
    _signal_handler("SIGINT")


signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


# ===============================================================
# Geolocation helpers
# ===============================================================

def do_reverse_geocode(data, attempt=1, max_attempts=10):
    """
    Perform reverse geocoding using Nominatim with retry logic.

    This function must only be executed after payload semantic validation.
    """
    point = (data["latitude"], data["longitude"])
    geocoding = Nominatim(user_agent=k.NOMINATIM_USER, timeout=5)

    try:
        return geocoding.reverse(point, timeout=5 + attempt, language="pt")
    except GeocoderTimedOut:
        if attempt < max_attempts:
            time.sleep(2)
            return do_reverse_geocode(data, attempt + 1)
        raise


def map_location_to_data(location, data):
    """
    Map Nominatim address fields into internal SITE structure.
    """
    address = location.raw.get("address", {})

    for field, candidates in k.REQUIRED_ADDRESS_FIELD.items():
        data[field] = None
        for c in candidates:
            if c in address:
                data[field] = address[c]
                break

    if not data.get("state"):
        iso = address.get("ISO3166-2-lvl4")

        if iso and iso.startswith("BR-"):
            uf_to_state = {
                "RO": "Rondônia",
                "AC": "Acre",
                "AM": "Amazonas",
                "RR": "Roraima",
                "PA": "Pará",
                "AP": "Amapá",
                "TO": "Tocantins",
                "MA": "Maranhão",
                "PI": "Piauí",
                "CE": "Ceará",
                "RN": "Rio Grande do Norte",
                "PB": "Paraíba",
                "PE": "Pernambuco",
                "AL": "Alagoas",
                "SE": "Sergipe",
                "BA": "Bahia",
                "MG": "Minas Gerais",
                "ES": "Espírito Santo",
                "RJ": "Rio de Janeiro",
                "SP": "São Paulo",
                "PR": "Paraná",
                "SC": "Santa Catarina",
                "RS": "Rio Grande do Sul",
                "MS": "Mato Grosso do Sul",
                "MT": "Mato Grosso",
                "GO": "Goiás",
                "DF": "Distrito Federal",
            }

            data["state"] = uf_to_state.get(iso[3:])

    return data


# ===============================================================
# File operations
# ===============================================================

def file_move(filename, path, new_path):
    """
    Move a file from (path/filename) to (new_path/filename),
    creating intermediate directories if necessary.

    Unlike os.renames(), this function NEVER removes source
    directories, preventing side effects on shared worker folders.
    """
    source = f"{path}/{filename}"
    target = f"{new_path}/{filename}"

    os.makedirs(new_path, exist_ok=True)
    os.rename(source, target)

    return {"filename": filename, "path": new_path}


def should_export(hostname: str) -> bool:
    """
    Decide whether appAnalise should export a .mat artifact.
    """
    normalized = (hostname or "").lower()

    if "rfeye" in normalized:
        return False

    if "cw" in normalized:
        return True

    return True


def resolve_history_file_metadata(
    file_was_processed,
    file_meta,
    server_name,
    extension,
    vl_file_size_kb,
    dt_created,
    dt_modified,
):
    """
    Resolve which file metadata should be persisted to FILE_TASK_HISTORY.
    """
    if file_was_processed and file_meta:
        return {
            "name": file_meta["file_name"],
            "extension": file_meta["extension"],
            "size_kb": file_meta["size_kb"],
            "dt_created": file_meta["dt_created"],
            "dt_modified": file_meta["dt_modified"],
        }

    return {
        "name": server_name,
        "extension": extension,
        "size_kb": vl_file_size_kb,
        "dt_created": dt_created,
        "dt_modified": dt_modified,
    }


def build_file_metadata(
    *,
    file_path,
    file_name,
    extension,
    size_kb,
    dt_created,
    dt_modified,
):
    """
    Build the normalized file metadata structure used by this worker.
    """
    return {
        "file_path": file_path,
        "file_name": file_name,
        "extension": extension,
        "size_kb": size_kb,
        "dt_created": dt_created,
        "dt_modified": dt_modified,
        "full_path": os.path.join(file_path, file_name),
    }


def build_site_data(gps):
    """
    Convert normalized GPS data into the SITE payload used by RFDATA.
    """
    return {
        "longitude": gps.longitude,
        "latitude": gps.latitude,
        "altitude": gps.altitude,
        "nu_gnss_measurements": len(gps._longitude),
    }


def upsert_site(db_rfm, bin_data):
    """
    Resolve or create the SITE referenced by the processed spectrum batch.
    """
    gps = bin_data["gps"]
    site_data = build_site_data(gps)
    site_id = db_rfm.get_site_id(site_data)

    if site_id:
        # Existing sites are identified from the persisted GNSS centroid.
        # In this path we deliberately avoid reverse geocoding so a new
        # Nominatim answer cannot degrade the geographic labels already stored.
        db_rfm.update_site(
            site=site_id,
            longitude_raw=gps._longitude,
            latitude_raw=gps._latitude,
            altitude_raw=gps._altitude,
        )
        return site_id

    location = do_reverse_geocode(site_data)
    site_data = map_location_to_data(location, site_data)
    return db_rfm.insert_site(site_data)


def insert_spectra_batch(
    db_rfm,
    bin_data,
    site_id,
    hostname_bin,
    hostname_db,
    host_path,
    host_file_name,
    extension,
    vl_file_size_kb,
    dt_created,
    dt_modified,
):
    """
    Persist host file metadata and all normalized spectra in a single batch.
    """
    host_file_id = db_rfm.insert_file(
        hostname=hostname_bin,
        NA_VOLUME=hostname_db,
        NA_PATH=host_path,
        NA_FILE=host_file_name,
        NA_EXTENSION=extension,
        VL_FILE_SIZE_KB=vl_file_size_kb,
        DT_FILE_CREATED=dt_created,
        DT_FILE_MODIFIED=dt_modified,
    )

    procedure_id = db_rfm.insert_procedure(bin_data["method"])
    dim_eq = db_rfm.get_or_create_spectrum_equipment(hostname_bin.lower())
    spectrum_ids = []

    for spectrum in bin_data["spectrum"]:
        metadata = spectrum.metadata if hasattr(spectrum, "metadata") else {}

        spectrum_ids.append(
            db_rfm.insert_spectrum(
                {
                    "id_site": site_id,
                    "id_procedure": procedure_id,
                    "id_detector_type": db_rfm.insert_detector_type(
                        k.DEFAULT_DETECTOR
                    ),
                    "id_trace_type": db_rfm.insert_trace_type(
                        spectrum.processing
                    ),
                    "id_equipment": dim_eq,
                    "id_measure_unit": db_rfm.insert_measure_unit(
                        spectrum.level_unit
                    ),
                    "na_description": getattr(spectrum, "description", None),
                    "nu_freq_start": spectrum.start_mega,
                    "nu_freq_end": spectrum.stop_mega,
                    "dt_time_start": spectrum.start_dateidx,
                    "dt_time_end": spectrum.stop_dateidx,
                    "nu_sample_duration": k.DEFAULT_SAMPLE_DURATION,
                    "nu_trace_count": spectrum.trace_length,
                    "nu_trace_length": spectrum.ndata,
                    "nu_rbw": getattr(spectrum, "bw", None),
                    "nu_att_gain": k.DEFAULT_ATTENUATION_GAIN,
                    "js_metadata": json.dumps(metadata),
                }
            )
        )

    db_rfm.insert_bridge_spectrum_file(spectrum_ids, [host_file_id])
    return spectrum_ids


def return_task_to_pending(db_bp, file_task_id, err):
    """
    Requeue the current FILE_TASK after a transient appAnalise failure.
    """
    db_bp.file_task_update(
        task_id=file_task_id,
        NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
        NU_STATUS=k.TASK_PENDING,
        DT_FILE_TASK=datetime.now(),
        NA_MESSAGE=tools.compose_message(
            task_type=k.FILE_TASK_PROCESS_TYPE,
            task_status=k.TASK_PENDING,
            detail="APP_ANALISE transient failure, task returned for retry",
            error=err.format_error(),
        ),
    )


def is_same_file(file_a, file_b):
    """
    Check whether two metadata dictionaries point to the same filesystem path.
    """
    if not file_a or not file_b:
        return False

    path_a = os.path.normpath(file_a["full_path"])
    path_b = os.path.normpath(file_b["full_path"])
    return path_a == path_b


def move_file_if_present(file_meta, destination_path):
    """
    Move a file when it still exists and return its new metadata.
    """
    if not file_meta or not os.path.exists(file_meta["full_path"]):
        return None

    file_move(
        filename=file_meta["file_name"],
        path=file_meta["file_path"],
        new_path=destination_path,
    )

    moved_meta = dict(file_meta)
    moved_meta["file_path"] = destination_path
    moved_meta["full_path"] = os.path.join(
        destination_path,
        file_meta["file_name"],
    )
    return moved_meta


def build_resolved_files_trash_path():
    """
    Return the dedicated quarantine for export-resolved leftovers.

    Files moved here are intentionally outside the normal FILE_TASK_HISTORY
    garbage-collection path because the canonical artifact may now be a renamed
    `.mat` stored elsewhere. The garbage collector sweeps this folder directly
    by filesystem age.
    """
    return (
        f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}/"
        f"{k.RESOLVED_FILES_TRASH_SUBDIR}"
    )


def finalize_successful_processing(
    db_rfm,
    spectrum_ids,
    bin_data,
    site_id,
    hostname_bin,
    file_meta,
    source_file_meta,
    export,
    filename,
):
    """
    Move the final artifact, register it in RFDATA, and retire superseded input.

    This helper keeps the success-side filesystem semantics explicit:
    when `export=True`, the exported `.mat` becomes the final artifact and the
    original input is relegated to `trash/resolved_files`.
    """
    year = bin_data["spectrum"][0].start_dateidx.year
    new_path = f"{k.REPO_FOLDER}/{year}/{db_rfm.build_path(site_id)}"
    final_file_meta = move_file_if_present(file_meta, new_path)

    if final_file_meta is None:
        raise FileNotFoundError(
            f"Final output file unavailable: {file_meta}"
        )

    server_file_id = db_rfm.insert_file(
        hostname=hostname_bin,
        NA_VOLUME=k.REPO_VOLUME_NAME,
        NA_PATH=new_path,
        NA_FILE=final_file_meta["file_name"],
        NA_EXTENSION=final_file_meta["extension"],
        VL_FILE_SIZE_KB=final_file_meta["size_kb"],
        DT_FILE_CREATED=final_file_meta["dt_created"],
        DT_FILE_MODIFIED=final_file_meta["dt_modified"],
    )

    db_rfm.insert_bridge_spectrum_file(spectrum_ids, [server_file_id])

    if export and not is_same_file(source_file_meta, final_file_meta):
        move_file_if_present(
            source_file_meta,
            build_resolved_files_trash_path(),
        )

    log.event(
        "processing_completed",
        file=filename,
        export=export,
        final_file=final_file_meta["full_path"],
    )
    return new_path, final_file_meta


def finalize_task_resolution(
    db_bp,
    *,
    file_task_id,
    host_id,
    host_file_name,
    host_path,
    server_name,
    extension,
    vl_file_size_kb,
    dt_created,
    dt_modified,
    file_was_processed,
    new_path,
    file_meta,
    source_file_meta,
    export,
    err,
):
    """
    Apply the final FILE_TASK resolution once retry is no longer an option.

    Definitive failures follow the normal trash/history path. Successful runs
    persist the exported artifact metadata as the server-side result. Any
    export-only leftovers that no longer participate in lineage are moved into
    the dedicated `resolved_files` quarantine for filesystem-based garbage
    cleanup.
    """
    if not file_was_processed and new_path is None:
        try:
            trashed_source_meta = move_file_if_present(
                source_file_meta,
                f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}",
            )

            if trashed_source_meta:
                new_path = trashed_source_meta["file_path"]
        except Exception as fs_err:
            log.error(f"event=trash_move_failed error={fs_err}")

        if (
            export
            and file_meta
            and not is_same_file(file_meta, source_file_meta)
        ):
            try:
                move_file_if_present(
                    file_meta,
                    build_resolved_files_trash_path(),
                )
            except Exception as artifact_err:
                log.error(
                    "event=resolved_artifact_trash_move_failed "
                    f"error={artifact_err}"
                )

    db_bp.file_task_delete(task_id=file_task_id)

    status = k.TASK_DONE if file_was_processed else k.TASK_ERROR

    history_meta = resolve_history_file_metadata(
        file_was_processed=file_was_processed,
        file_meta=file_meta,
        server_name=server_name,
        extension=extension,
        vl_file_size_kb=vl_file_size_kb,
        dt_created=dt_created,
        dt_modified=dt_modified,
    )
    history_server_path = new_path

    if (
        not file_was_processed
        and history_server_path is None
        and source_file_meta
    ):
        # Error finalization should keep a deterministic last-known repository
        # location even if the move to trash could not be completed.
        history_server_path = source_file_meta["file_path"]

    na_message = tools.compose_message(
        task_type=k.FILE_TASK_PROCESS_TYPE,
        task_status=status,
        path=new_path if file_was_processed else None,
        name=history_meta["name"] if file_was_processed else None,
        error=err.format_error() if err.triggered else None,
    )
    processed_at = datetime.now()

    db_bp.file_history_update(
        host_id=host_id,
        task_type=k.FILE_TASK_PROCESS_TYPE,
        host_file_name=host_file_name,
        host_file_path=host_path,
        DT_PROCESSED=processed_at,
        NA_SERVER_FILE_NAME=history_meta["name"],
        NA_SERVER_FILE_PATH=history_server_path,
        NA_EXTENSION=history_meta["extension"],
        VL_FILE_SIZE_KB=history_meta["size_kb"],
        DT_FILE_CREATED=history_meta["dt_created"],
        DT_FILE_MODIFIED=history_meta["dt_modified"],
        NU_STATUS_PROCESSING=status,
        NA_MESSAGE=na_message,
    )

    db_bp.host_task_statistics_create(host_id=host_id)
    return {
        "status": status,
        "new_path": history_server_path,
        "history_meta": history_meta,
        "final_file": (
            os.path.join(history_server_path, history_meta["name"])
            if history_server_path else None
        ),
    }


def resolve_task_after_attempt(
    db_bp,
    *,
    file_task_id,
    host_id,
    host_file_name,
    host_path,
    server_name,
    extension,
    vl_file_size_kb,
    dt_created,
    dt_modified,
    file_was_processed,
    new_path,
    file_meta,
    source_file_meta,
    export,
    retry_later,
    err,
):
    """
    Resolve the claimed task after a processing attempt.

    Only transient appAnalise connectivity/service failures keep the task alive
    for a later retry. All other outcomes flow through the definitive
    history/trash resolution path, because reprocessing the same payload would
    produce the same definitive result again.
    """
    if retry_later:
        return_task_to_pending(db_bp, file_task_id, err)
        return {"action": "retry"}

    result = finalize_task_resolution(
        db_bp,
        file_task_id=file_task_id,
        host_id=host_id,
        host_file_name=host_file_name,
        host_path=host_path,
        server_name=server_name,
        extension=extension,
        vl_file_size_kb=vl_file_size_kb,
        dt_created=dt_created,
        dt_modified=dt_modified,
        file_was_processed=file_was_processed,
        new_path=new_path,
        file_meta=file_meta,
        source_file_meta=source_file_meta,
        export=export,
        err=err,
    )
    result["action"] = "finalized"
    return result


# ===============================================================
# MAIN LOOP
# ===============================================================

def main():
    """
    Run the production worker loop backed by appAnalise.

    The worker only commits side effects after the external processing
    result has been normalized and validated. Transient dependency
    failures are requeued for retry; definitive validation failures
    follow the normal trash/history finalization path.
    """
    log.service_start("appCataloga_file_bin_proces_appAnalise")

    db_bp = dbHandlerBKP(database="BPDATA", log=log)
    db_rfm = dbHandlerRFM(database="RFDATA", log=log)
    app_analise = AppAnaliseConnection()

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        file_task_id = None
        file_was_processed = False
        new_path = None
        host_id = None
        file_meta = None
        retry_later = False
        should_sleep = False
        export = None
        source_file_meta = None

        try:
            # ===================================================
            # ACT I — Fetch FILE_TASK
            # ===================================================
            result = db_bp.read_file_task(
                task_type=k.FILE_TASK_PROCESS_TYPE,
                task_status=k.TASK_PENDING,
                check_host_busy=False,
            )

            if not result:
                should_sleep = True
                continue

            # From this point on, we have a concrete FILE_TASK candidate.
            row, host_id, _ = result
            file_task_id    = row["FILE_TASK__ID_FILE_TASK"]
            server_path     = row["FILE_TASK__NA_SERVER_FILE_PATH"]
            server_name     = row["FILE_TASK__NA_SERVER_FILE_NAME"]
            host_path       = row["FILE_TASK__NA_HOST_FILE_PATH"]
            host_file_name  = row["FILE_TASK__NA_HOST_FILE_NAME"]
            hostname_db     = row["HOST__NA_HOST_NAME"]
            extension       = row["FILE_TASK__NA_EXTENSION"]
            dt_created      = row["FILE_TASK__DT_FILE_CREATED"]
            dt_modified     = row["FILE_TASK__DT_FILE_MODIFIED"]
            vl_file_size_kb = row["FILE_TASK__VL_FILE_SIZE_KB"]
            filename        = f"{server_path}/{server_name}"
            source_file_meta = build_file_metadata(
                file_path=server_path,
                file_name=server_name,
                extension=extension,
                size_kb=vl_file_size_kb,
                dt_created=dt_created,
                dt_modified=dt_modified,
            )

            # ===================================================
            # ACT II — Preflight appAnalise connectivity
            # ===================================================
            try:
                app_analise.check_connection()
            except errors.ExternalServiceTransientError as e:
                log.warning(
                    f"event=appanalise_unavailable_retry error={e}"
                )
                should_sleep = True
                continue

            # ===================================================
            # ACT III — Mark task as RUNNING
            # ===================================================
            try:
                db_bp.file_task_update(
                    task_id=file_task_id,
                    NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
                    DT_FILE_TASK=datetime.now(),
                    NU_STATUS=k.TASK_RUNNING,
                    NU_PID=os.getpid(),
                    NA_MESSAGE=tools.compose_message(
                        task_type=k.FILE_TASK_PROCESS_TYPE,
                        task_status=k.TASK_RUNNING,
                        path=server_path,
                        name=server_name,
                        detail=k.APP_ANALISE_WORKER_DETAIL,
                    ),
                )
            except Exception as e:
                err.set(reason=str(e), stage="PROCESS", exc=e)
                raise

            # ===================================================
            # ACT IV — Process and validate via appAnalise
            # ===================================================
            try:
                export = should_export(hostname_db)
                log.event(
                    "processing_started",
                    file=filename,
                    export=export,
                )

                bin_data, file_meta = app_analise.process(
                    file_path=server_path,
                    file_name=server_name,
                    export=export,
                )

                hostname_bin = bin_data["hostname"]
            except errors.ExternalServiceTransientError as e:
                # Connection/service failures are the only processing errors
                # that should requeue the FILE_TASK instead of resolving it as
                # definitive error.
                retry_later = True
                err.set(reason=str(e), stage="PROCESS", exc=e)
                raise
            except errors.BinValidationError as e:
                err.set(reason=str(e), stage="PROCESS", exc=e)
                raise
            except Exception as e:
                err.set(reason=str(e), stage="PROCESS", exc=e)
                raise

            try:
                # SITE resolution is intentionally outside the DB transaction,
                # matching the existing BIN worker behavior.
                site_id = upsert_site(db_rfm, bin_data)
            except Exception as e:
                err.set(reason=str(e), stage="SITE", exc=e)
                raise

            # ===================================================
            # ACT VI — Begin DB transaction
            # ===================================================
            try:
                db_rfm.begin_transaction()
            except Exception as e:
                err.set(reason=str(e), stage="DB", exc=e)
                raise

            # ===================================================
            # ACT VII — Insert spectrum and metadata (DB)
            # ===================================================
            try:
                # The host-side source file and all derived spectra must be
                # committed as one unit for consistent lineage.
                spectrum_ids = insert_spectra_batch(
                    db_rfm=db_rfm,
                    bin_data=bin_data,
                    site_id=site_id,
                    hostname_bin=hostname_bin,
                    hostname_db=hostname_db,
                    host_path=host_path,
                    host_file_name=host_file_name,
                    extension=extension,
                    vl_file_size_kb=vl_file_size_kb,
                    dt_created=dt_created,
                    dt_modified=dt_modified,
                )
                db_rfm.commit()
            except Exception as e:
                err.set(reason=str(e), stage="DB", exc=e)
                raise

            # ===================================================
            # ACT VIII — Filesystem move + server file registration
            # ===================================================
            try:
                # Success resolution decides which artifact becomes canonical
                # (`.mat` export or original file) and retires superseded input.
                new_path, file_meta = finalize_successful_processing(
                    db_rfm=db_rfm,
                    spectrum_ids=spectrum_ids,
                    bin_data=bin_data,
                    site_id=site_id,
                    hostname_bin=hostname_bin,
                    file_meta=file_meta,
                    source_file_meta=source_file_meta,
                    export=export,
                    filename=filename,
                )
                file_was_processed = True
            except Exception as e:
                err.set(reason=str(e), stage="FS", exc=e)
                raise

        except Exception as e:
            if not err.triggered:
                err.capture(
                    reason="Unexpected processing loop failure",
                    stage="PROCESS",
                    exc=e,
                    host_id=host_id,
                    task_id=file_task_id,
                )
            err.log_error(host_id=host_id, task_id=file_task_id)
            if db_rfm.in_transaction:
                db_rfm.rollback()

        finally:
            if should_sleep:
                legacy._random_jitter_sleep()

            if not file_task_id:
                continue

            # ---------------------------------------------------
            # Global resolution (single exit point)
            # ---------------------------------------------------
            if retry_later:
                try:
                    # Transient service failures keep the task alive and push
                    # the whole resolution decision into the shared helper.
                    resolve_task_after_attempt(
                        db_bp,
                        file_task_id=file_task_id,
                        host_id=host_id,
                        host_file_name=host_file_name,
                        host_path=host_path,
                        server_name=server_name,
                        extension=extension,
                        vl_file_size_kb=vl_file_size_kb,
                        dt_created=dt_created,
                        dt_modified=dt_modified,
                        file_was_processed=file_was_processed,
                        new_path=new_path,
                        file_meta=file_meta,
                        source_file_meta=source_file_meta,
                        export=export,
                        retry_later=True,
                        err=err,
                    )
                except Exception as update_err:
                    log.error(
                        f"event=retry_requeue_failed error={update_err}"
                    )

                continue

            # Definitive outcomes (success or fatal payload error) are closed
            # here so task deletion, trash handling, and history stay aligned.
            resolution = resolve_task_after_attempt(
                db_bp,
                file_task_id=file_task_id,
                host_id=host_id,
                host_file_name=host_file_name,
                host_path=host_path,
                server_name=server_name,
                extension=extension,
                vl_file_size_kb=vl_file_size_kb,
                dt_created=dt_created,
                dt_modified=dt_modified,
                file_was_processed=file_was_processed,
                new_path=new_path,
                file_meta=file_meta,
                source_file_meta=source_file_meta,
                export=export,
                retry_later=False,
                err=err,
            )

            if resolution["status"] == k.TASK_ERROR:
                log.error_event(
                    "processing_error",
                    file=filename,
                    export=export,
                    final_file=resolution["final_file"],
                    error=err.format_error() or "Processing failed",
                )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal appAnalise processing worker crash",
            stage="MAIN",
            exc=e,
        )
        err.log_error()
        release_busy_hosts_on_exit()
        raise
