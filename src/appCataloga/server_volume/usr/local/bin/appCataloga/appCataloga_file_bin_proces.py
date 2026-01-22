#!/usr/bin/python3
"""
appCataloga_file_bin_process

Worker responsible for processing BIN files produced by monitoring stations
(e.g., RFeye).

Pipeline (deterministic):

    ACT I   - Fetch FILE_TASK
    ACT II  - Mark task as RUNNING
    ACT III - Parse and semantically validate BIN (fatal)
    ACT IV  - Begin DB transaction
    ACT V   - SITE / GEO enrichment (only for validated BINs)
    ACT VI  - Insert spectrum and metadata into RFDATA
    ACT VII - Filesystem move + server file registration
    FINALLY - Global resolution (trash on error, history update, cleanup)

Design principles:
- BIN semantic validation is exclusive responsibility of Station classes
- No DB operation occurs before successful BIN validation
- FILE_TASK is always transient
- FILE_TASK_HISTORY is the single source of truth
- Error handling is centralized via ErrorHandler + finally
"""

import sys
import os
import time
import signal
import inspect
import json
from datetime import datetime

# ---------------------------------------------------------------
# Configuration path (shared config and handlers)
# ---------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# =================================================
# Config directory (etc/appCataloga)
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
from rfpye.parser import parse_bin
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut

# ---------------------------------------------------------------
# Internal modules
# ---------------------------------------------------------------
import config as k
from shared import errors, legacy, logging_utils, tools

from db.dbHandlerBKP import dbHandlerBKP
from db.dbHandlerRFM import dbHandlerRFM
from stations import station_factory


# ===============================================================
# GLOBAL STATE
# ===============================================================
log = logging_utils.log(target_screen=False)
process_status = {"running": True}


# ===============================================================
# SIGNAL HANDLING
# ===============================================================

def release_busy_hosts_on_exit():
    """
    Release BUSY hosts held by this worker PID.
    """
    try:
        pid = os.getpid()
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
        db.host_release_by_pid(pid)
    except Exception as e:
        log.error(f"[CLEANUP] Failed to release BUSY hosts: {e}")


def _signal_handler(signal=None, frame=None):
    """
    Handle SIGTERM / SIGINT gracefully.
    """
    fn = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"SIGNAL received at {fn}()")
    process_status["running"] = False
    release_busy_hosts_on_exit()


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


# ===============================================================
# GEOLOCATION HELPERS
# ===============================================================

def do_reverse_geocode(data, attempt=1, max_attempts=10):
    """
    Perform reverse geocoding using Nominatim with retry logic.

    This function must only be executed after BIN semantic validation.
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
    for field, candidates in k.REQUIRED_ADDRESS_FIELD.items():
        data[field] = None
        for c in candidates:
            if c in location.raw.get("address", {}):
                data[field] = location.raw["address"][c]
                break
    return data


# ===============================================================
# FILE OPERATIONS
# ===============================================================

def file_move(filename, path, new_path):
    """
    Move a file from (path/filename) to (new_path/filename),
    creating intermediate directories if necessary.
    """
    source = f"{path}/{filename}"
    target = f"{new_path}/{filename}"
    os.renames(source, target)
    return {"filename": filename, "path": new_path}


# ===============================================================
# MAIN LOOP
# ===============================================================

def main():
    """
    Main worker loop.
    """
    log.entry("[INIT] appCataloga_file_bin_process started")

    db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    db_rfm = dbHandlerRFM(database=k.RFM_DATABASE_NAME, log=log)

    while process_status["running"]:
        err = errors.ErrorHandler(log)

        file_task_id = None
        file_was_processed = False
        new_path = None
        host_id = None

        try:
            # ===================================================
            # ACT I — Fetch FILE_TASK
            # ===================================================
            result = db_bp.read_file_task(
                task_type=k.FILE_TASK_PROCESS_TYPE,
                task_status=k.TASK_PENDING,
                extension=".bin",
                check_host_busy=False,
            )

            if not result:
                legacy._random_jitter_sleep()
                continue

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

            filename = f"{server_path}/{server_name}"

            # ===================================================
            # ACT II — Mark task as RUNNING
            # ===================================================
            try:
                db_bp.file_task_update(
                    task_id=file_task_id,
                    NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
                    NU_STATUS=k.TASK_RUNNING,
                    NU_PID=os.getpid(),
                    NA_MESSAGE=f"Processing BIN: {filename}",
                )
            except Exception as e:
                err.set(reason=str(e), stage="PROCESS", exc=e)
                raise

            # ===================================================
            # ACT III — Parse and validate BIN (semantic, fatal)
            # ===================================================
            try:
                bin_data = parse_bin(filename)
                bin_data = station_factory(
                    bin_data=bin_data,
                    host_uid=hostname_db
                ).process()
                
                hostname_bin = bin_data["hostname"]
            except errors.BinValidationError as e:
                err.set(reason=str(e), stage="PROCESS", exc=e)
                raise
            except Exception as e:
                err.set(reason=str(e), stage="PROCESS", exc=e)
                raise

            # ===================================================
            # ACT IV — Begin DB transaction
            # ===================================================
            try:
                db_rfm.begin_transaction()
            except Exception as e:
                err.set(reason=str(e), stage="DB", exc=e)
                raise

            # ===================================================
            # ACT V — SITE / GEO enrichment
            # ===================================================
            try:
                gps = bin_data["gps"]

                site_data = {
                    "longitude": gps.longitude,
                    "latitude": gps.latitude,
                    "altitude": gps.altitude,
                    "nu_gnss_measurements": len(gps._longitude),
                }

                site_id = db_rfm.get_site_id(site_data)

                if site_id:
                    db_rfm.update_site(
                        site=site_id,
                        longitude_raw=gps._longitude,
                        latitude_raw=gps._latitude,
                        altitude_raw=gps._altitude,
                    )
                else:
                    location = do_reverse_geocode(site_data)
                    site_data = map_location_to_data(location, site_data)
                    site_id = db_rfm.insert_site(site_data)

            except Exception as e:
                err.set(reason=str(e), stage="SITE", exc=e)
                raise

            # ===================================================
            # ACT VI — Insert spectrum and metadata (DB)
            # ===================================================
            try:
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

                for s in bin_data["spectrum"]:
                    spectrum_ids.append(
                        db_rfm.insert_spectrum(
                            {
                                "id_site": site_id,
                                "id_procedure": procedure_id,
                                "id_detector_type": db_rfm.insert_detector_type(k.DEFAULT_DETECTOR),
                                "id_trace_type": db_rfm.insert_trace_type(s.processing),
                                "id_equipment": dim_eq,
                                "id_measure_unit": db_rfm.insert_measure_unit(s.level_unit),
                                "na_description": getattr(s, "description", None),
                                "nu_freq_start": s.start_mega,
                                "nu_freq_end": s.stop_mega,
                                "dt_time_start": s.start_dateidx,
                                "dt_time_end": s.stop_dateidx,
                                "nu_sample_duration": k.DEFAULT_SAMPLE_DURATION,
                                "nu_trace_count": len(s.timestamp),
                                "nu_trace_length": s.ndata,
                                "nu_rbw": getattr(s, "bw", None),
                                "nu_att_gain": k.DEFAULT_ATTENUATION_GAIN,
                                "js_metadata": json.dumps(
                                    s.metadata if hasattr(s, "metadata") else {"antuid": s.antuid}
                                ),
                            }
                        )
                    )

                db_rfm.insert_bridge_spectrum_file(spectrum_ids, [host_file_id])
                db_rfm.commit()

            except Exception as e:
                err.set(reason=str(e), stage="DB", exc=e)
                raise

            # ===================================================
            # ACT VII — Filesystem move + server file registration
            # ===================================================
            try:
                year = bin_data["spectrum"][0].start_dateidx.year
                new_path = f"{k.REPO_FOLDER}/{year}/{db_rfm.build_path(site_id)}"

                file_move(server_name, server_path, new_path)

                server_file_id = db_rfm.insert_file(
                    hostname=hostname_bin,
                    NA_VOLUME="reposfi",
                    NA_PATH=new_path,
                    NA_FILE=server_name,
                    NA_EXTENSION=extension,
                    VL_FILE_SIZE_KB=vl_file_size_kb,
                    DT_FILE_CREATED=dt_created,
                    DT_FILE_MODIFIED=dt_modified,
                )

                db_rfm.insert_bridge_spectrum_file(spectrum_ids, [server_file_id])

                file_was_processed = True
                log.entry(f"[DONE] {filename}")

            except Exception as e:
                err.set(reason=str(e), stage="FS", exc=e)
                raise

        except Exception:
            if db_rfm.in_transaction:
                db_rfm.rollback()

        finally:
            if not file_task_id:
                continue

            # ---------------------------------------------------
            # Global resolution (single exit point)
            # ---------------------------------------------------

            # Move file to trash if processing failed
            if not file_was_processed and new_path is None:
                try:
                    file_data = file_move(
                        filename=server_name,
                        path=server_path,
                        new_path=f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}",
                    )
                    new_path = file_data["path"]
                except Exception as fs_err:
                    log.error(f"[TRASH] Failed to move file to trash: {fs_err}")

            # Remove transient FILE_TASK
            db_bp.file_task_delete(task_id=file_task_id)

            # Update FILE_TASK_HISTORY
            status = k.TASK_DONE if file_was_processed else k.TASK_ERROR

            # Build NA_MESSAGE
            NA_MESSAGE = tools.compose_message(
                task_type=k.FILE_TASK_PROCESS_TYPE,
                task_status=status,
                path=new_path if file_was_processed else None,
                name=server_name if file_was_processed else None,
                error=err.format_error() if err.triggered else None,
            )

            # Update File History
            # Internally insert DT_PROCESSED timestamp
            db_bp.file_history_update(
                task_type=k.FILE_TASK_PROCESS_TYPE,
                host_file_name=host_file_name,
                server_file_name=server_name,
                host_id=host_id,
                NA_SERVER_FILE_NAME=server_name,
                NA_SERVER_FILE_PATH=new_path,   
                NU_STATUS_PROCESSING=status,
                NA_MESSAGE=NA_MESSAGE,
            )


            db_bp.host_task_statistics_create(host_id=host_id)


if __name__ == "__main__":
    main()
