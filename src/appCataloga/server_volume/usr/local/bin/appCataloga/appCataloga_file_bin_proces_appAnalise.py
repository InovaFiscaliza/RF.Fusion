#!/usr/bin/python3
"""
appCataloga_file_bin_process_matlab_test

TEST WORKER for RF.Fusion using MATLAB (appAnalise) as the processing engine.

Differences from production worker:

• Uses BPDATA_TEST and RFDATA_TEST
• All spectrum parsing delegated to appAnalise
• Filesystem operations disabled
• Trash disabled
• Safe to run alongside production

Purpose
-------

Validate MATLAB processing pipeline without affecting production data.
"""

import sys
import os
import time
import signal
import inspect
import json
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
from stations import AppAnaliseConnection

# ===============================================================
# GLOBAL STATE
# ===============================================================

log = logging_utils.log(target_screen=True)
process_status = {"running": True}


# ===============================================================
# SIGNAL HANDLING
# ===============================================================

def release_busy_hosts_on_exit():

    try:

        pid = os.getpid()
        db = dbHandlerBKP(database="BPDATA_TEST", log=log)
        db.host_release_by_pid(pid)

    except Exception as e:
        log.error(f"[CLEANUP] Failed to release BUSY hosts: {e}")


def _signal_handler(signal=None, frame=None):

    fn = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"SIGNAL received at {fn}()")
    process_status["running"] = False
    release_busy_hosts_on_exit()


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


# ===============================================================
# GEOLOCATION
# ===============================================================

def do_reverse_geocode(data, attempt=1, max_attempts=10):

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

    address = location.raw.get("address", {})
    for field, candidates in k.REQUIRED_ADDRESS_FIELD.items():
        data[field] = None
        for c in candidates:
            if c in address:
                data[field] = address[c]
                break

    return data


# ===============================================================
# FILE MOVE (DISABLED FOR TEST)
# ===============================================================

def file_move(filename, path, new_path):

    log.warning("[TEST MODE] File move skipped")
    return {"filename": filename, "path": new_path}


# ===============================================================
# MAIN LOOP
# ===============================================================

def main():

    log.entry("[INIT] MATLAB TEST WORKER STARTED")

    # -----------------------------------------
    # TEST DATABASES
    # -----------------------------------------
    db_bp = dbHandlerBKP(database="BPDATA_TEST", log=log)
    db_rfm = dbHandlerRFM(database="RFDATA_TEST", log=log)

    # -----------------------------------------
    # MATLAB CLIENT
    # -----------------------------------------
    app_analise = AppAnaliseConnection()

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
                check_host_busy=False,
            )

            if not result:
                legacy._random_jitter_sleep()
                continue

            row, host_id, _ = result
            file_task_id = row["FILE_TASK__ID_FILE_TASK"]
            server_path = row["FILE_TASK__NA_SERVER_FILE_PATH"]
            server_name = row["FILE_TASK__NA_SERVER_FILE_NAME"]
            host_path = row["FILE_TASK__NA_HOST_FILE_PATH"]
            host_file_name = row["FILE_TASK__NA_HOST_FILE_NAME"]
            hostname_db = row["HOST__NA_HOST_NAME"]
            extension = row["FILE_TASK__NA_EXTENSION"]
            dt_created = row["FILE_TASK__DT_FILE_CREATED"]
            dt_modified = row["FILE_TASK__DT_FILE_MODIFIED"]
            vl_file_size_kb = row["FILE_TASK__VL_FILE_SIZE_KB"]
            filename = f"{server_path}/{server_name}"

            # ===================================================
            # ACT II — Mark task RUNNING
            # ===================================================

            db_bp.file_task_update(
                task_id=file_task_id,
                NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
                DT_FILE_TASK=datetime.now(),
                NU_STATUS=k.TASK_RUNNING,
                NU_PID=os.getpid(),
                NA_MESSAGE=f"[MATLAB TEST] Processing: {filename}",
            )

            # ===================================================
            # ACT III — MATLAB PROCESSING
            # ===================================================
            log.entry(f"[APP_ANALISE] Processing {filename}")

            # Check if have to export ".mat" file
            if "cw" in hostname_db.lower():
                export = True
            else:                
                export = False 
            
            # Process file with appAnalise and get structured 
            # data ready for DB insertion
            bin_data = app_analise.process(
                    file_path=server_path,
                    file_name=server_name,
                    export=export
            )               

            hostname_bin = bin_data["hostname"]
            # ===================================================
            # ACT IV — SITE / GEO
            # ===================================================

            gps = bin_data["gps"]
            site_data = {
                "longitude": gps.longitude,
                "latitude": gps.latitude,
                "altitude": gps.altitude,
                "nu_gnss_measurements": len(gps._longitude),
            }

            site_id = db_rfm.get_site_id(site_data)

            if not site_id:
                location = do_reverse_geocode(site_data)
                site_data = map_location_to_data(location, site_data)
                site_id = db_rfm.insert_site(site_data)

            # ===================================================
            # ACT V — DB TRANSACTION
            # ===================================================
            db_rfm.begin_transaction()

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
            dim_eq = db_rfm.get_or_create_spectrum_equipment( hostname_bin.lower() )

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
                            "nu_trace_count": s.trace_length,
                            "nu_trace_length": s.ndata,
                            "nu_rbw": getattr(s, "bw", None),
                            "nu_att_gain": k.DEFAULT_ATTENUATION_GAIN,
                            "js_metadata": json.dumps(s.metadata if hasattr(s, "metadata") else {} ),
                        }
                    )
                )

            db_rfm.insert_bridge_spectrum_file(
                spectrum_ids,
                [host_file_id]
            )

            db_rfm.commit()
            file_was_processed = True
            log.entry(f"[DONE TEST] {filename}")

        except Exception:
            log.error(f"[PROCESS ERROR] {err.format_error()}")
            if db_rfm.in_transaction:
                db_rfm.rollback()
        finally:
            if not file_task_id:
                continue

            # -------------------------------------------
            # TEST MODE - NO TRASH
            # -------------------------------------------
            if not file_was_processed:
                log.warning("[TEST MODE] File not moved to trash")
            db_bp.file_task_delete(task_id=file_task_id)

            status = k.TASK_DONE if file_was_processed else k.TASK_ERROR

            NA_MESSAGE = tools.compose_message(
                task_type=k.FILE_TASK_PROCESS_TYPE,
                task_status=status,
                path=None,
                name=server_name,
                error=err.format_error() if err.triggered else None,
            )

            db_bp.file_history_update(
                host_id=host_id,
                task_type=k.FILE_TASK_PROCESS_TYPE,
                host_file_name=host_file_name,
                host_file_path=host_path,
                NA_SERVER_FILE_NAME=server_name,
                NA_SERVER_FILE_PATH=None,
                NU_STATUS_PROCESSING=status,
                NA_MESSAGE=NA_MESSAGE,
            )

            db_bp.host_task_statistics_create(host_id=host_id)


if __name__ == "__main__":
    main()