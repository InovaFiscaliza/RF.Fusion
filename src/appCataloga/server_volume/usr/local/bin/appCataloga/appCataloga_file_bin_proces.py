#!/usr/bin/python3
"""
appCataloga_file_bin_process

Process BIN files produced by monitoring stations.

Design rules:
- All BIN validation is delegated to Station classes.
- No database operation occurs before validation succeeds.
- FILE_TASK is transient and always removed.
- FILE_TASK_HISTORY is the source of truth.
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
CONFIG_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga")
)
sys.path.append(CONFIG_PATH)

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
import shared as sh

from db.dbHandlerBKP import dbHandlerBKP
from db.dbHandlerRFM import dbHandlerRFM
from stations import station_factory


# ===============================================================
# GLOBAL STATE
# ===============================================================
log = sh.log(target_screen=False)
process_status = {"running": True}


# ===============================================================
# SIGNAL HANDLING
# ===============================================================

def release_busy_hosts_on_exit():
    try:
        pid = os.getpid()
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
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
# GEOLOCATION HELPERS
# ===============================================================

def do_revese_geocode(data, attempt=1, max_attempts=10):
    point = (data["latitude"], data["longitude"])
    geocoding = Nominatim(user_agent=k.NOMINATIM_USER, timeout=5)

    try:
        return geocoding.reverse(point, timeout=5 + attempt, language="pt")
    except GeocoderTimedOut:
        if attempt < max_attempts:
            time.sleep(2)
            return do_revese_geocode(data, attempt + 1)
        raise


def map_location_to_data(location, data):
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
    source = f"{path}/{filename}"
    target = f"{new_path}/{filename}"
    os.renames(source, target)
    return {"filename": filename, "path": new_path}


# ===============================================================
# MAIN LOOP
# ===============================================================

def main():
    log.entry("[INIT] appCataloga_file_bin_process started")

    db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    db_rfm = dbHandlerRFM(database=k.RFM_DATABASE_NAME, log=log)

    while process_status["running"]:
        err = sh.ErrorHandler(log)

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
                sh._random_jitter_sleep()
                continue
            
            # Get data from FILE_TASK
            row, host_id, _     = result
            file_task_id        = row["FILE_TASK__ID_FILE_TASK"]
            server_path         = row["FILE_TASK__NA_SERVER_FILE_PATH"]
            server_name         = row["FILE_TASK__NA_SERVER_FILE_NAME"]
            host_path           = row["FILE_TASK__NA_HOST_FILE_PATH"]
            host_file_name      = row["FILE_TASK__NA_HOST_FILE_NAME"]
            hostname            = row["HOST__NA_HOST_NAME"]
            extension           = row["FILE_TASK__NA_EXTENSION"]
            dt_created          = row["FILE_TASK__DT_FILE_CREATED"]
            dt_modified         = row["FILE_TASK__DT_FILE_MODIFIED"]
            vl_file_size_kb     = row["FILE_TASK__VL_FILE_SIZE_KB"]
            filename            = f"{server_path}/{server_name}"

            # ===================================================
            # ACT II — Mark RUNNING
            # ===================================================
            db_bp.file_task_update(
                task_id=file_task_id,
                NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
                NU_STATUS=k.TASK_RUNNING,
                NU_PID=os.getpid(),
                NA_MESSAGE=f"Processing BIN: {filename}",
            )

            # ===================================================
            # ACT III — Parse + Validate (NO DATABASE ACCESS)
            # ===================================================
            try:
                bin_data = parse_bin(filename)

                bin_data = station_factory(
                    bin_data=bin_data,
                    host_uid=hostname
                ).process()

            except sh.BinValidationError as e:
                err.set(reason=str(e), stage="PROCESS", exc=e)

                file_data = file_move(
                    filename=server_name,
                    path=server_path,
                    new_path=f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}",
                )
                new_path = file_data["path"]

                log.error(
                    f"[TRASH] Validation failed. File moved to trash: "
                    f"{new_path}/{server_name}"
                )
                raise

            # ===================================================
            # ACT IV — BEGIN TRANSACTION
            # ===================================================
            db_rfm.begin_transaction()

            # ===================================================
            # ACT V — SITE
            # ===================================================
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
                location = do_revese_geocode(site_data)
                site_data = map_location_to_data(location, site_data)
                site_id = db_rfm.insert_site(site_data)

            # ===================================================
            # ACT VI — INSERT DATA (HOST FILE)
            # ===================================================
            host_file_id = db_rfm.insert_file(
                hostname=hostname,
                NA_VOLUME=hostname,
                NA_PATH=host_path,
                NA_FILE=host_file_name,
                NA_EXTENSION=extension,
                VL_FILE_SIZE_KB=vl_file_size_kb,
                DT_FILE_CREATED=dt_created,
                DT_FILE_MODIFIED=dt_modified,
            )

            procedure_id = db_rfm.insert_procedure(bin_data["method"])
            dim_eq = db_rfm.get_or_create_spectrum_equipment(hostname.lower())

            spectrum_ids = []

            # Inser Spectrum into FACT_SPECTRUM:
            for s in bin_data["spectrum"]:
                spectrum_id = db_rfm.insert_spectrum(
                    {
                        "id_site"           : site_id,
                        "id_procedure"      : procedure_id,
                        "id_detector_type"  : db_rfm.insert_detector_type(k.DEFAULT_DETECTOR),
                        "id_trace_type"     : db_rfm.insert_trace_type(s.processing),
                        "id_equipment"      : dim_eq,
                        "id_measure_unit"   : db_rfm.insert_measure_unit(s.dtype),
                        "na_description"    : getattr(s, "description", None),
                        "nu_freq_start"     : s.start_mega,
                        "nu_freq_end"       : s.stop_mega,
                        "dt_time_start"     : s.start_dateidx,
                        "dt_time_end"       : s.stop_dateidx,
                        "nu_sample_duration": k.DEFAULT_SAMPLE_DURATION,
                        "nu_trace_count"    : len(s.timestamp),
                        "nu_trace_length"   : s.ndata,
                        "nu_rbw"            : getattr(s, "bw", None),
                        "nu_att_gain"       : k.DEFAULT_ATTENUATION_GAIN,
                        "js_metadata"       : json.dumps(
                                s.metadata if hasattr(s, "metadata") else {"antuid": s.antuid}
                            ),
                    }
                )
                spectrum_ids.append(spectrum_id)

            # Bridge spectrum server file
            db_rfm.insert_bridge_spectrum_file(spectrum_ids, [host_file_id])
            db_rfm.commit()

            # ===================================================
            # ACT VII — MOVE FILE + SERVER FILE REGISTRATION
            # ===================================================
            year = bin_data["spectrum"][0].start_dateidx.year
            new_path = f"{k.REPO_FOLDER}/{year}/{db_rfm.build_path(site_id)}"
            file_move(server_name, server_path, new_path)

            # Insert Server File record
            server_file_id = db_rfm.insert_file(
                hostname=hostname,
                NA_VOLUME="reposfi",
                NA_PATH=new_path,
                NA_FILE=server_name,
                NA_EXTENSION=extension,
                VL_FILE_SIZE_KB=vl_file_size_kb,
                DT_FILE_CREATED=dt_created,
                DT_FILE_MODIFIED=dt_modified,
            )

            # Bridge spectrum server file
            db_rfm.insert_bridge_spectrum_file(spectrum_ids, [server_file_id])

            file_was_processed = True
            log.entry(f"[DONE] {filename}")

        except Exception:
            if db_rfm.in_transaction:
                db_rfm.rollback()

        finally:
            if file_task_id:
                # Always remove FILE_TASK - transitory data
                db_bp.file_task_delete(task_id=file_task_id)

                # Status update in FILE_TASK_HISTORY
                status = k.TASK_DONE if file_was_processed else k.TASK_ERROR
                NA_MESSAGE = sh._compose_message(
                    task_type=k.FILE_TASK_PROCESS_TYPE,
                    task_status=status,
                    path=new_path if file_was_processed else None,
                    name=server_name if file_was_processed else None,
                )

                if err.triggered:
                    NA_MESSAGE = f"{NA_MESSAGE} | {err.format_error()}"

                # Status Transaction
                db_bp.file_history_update(
                    task_type=k.FILE_TASK_PROCESS_TYPE,
                    file_name=server_name,
                    NA_SERVER_FILE_NAME=server_name,
                    NA_SERVER_FILE_PATH=new_path,
                    NU_STATUS_PROCESSING=status,
                    NA_MESSAGE=NA_MESSAGE,
                )

                db_bp.host_task_statistics_create(host_id=host_id)


if __name__ == "__main__":
    main()
