#!/usr/bin/python3
"""
appCataloga_file_bin_process

Process BIN files produced by RFeye stations:
- Parse BIN
- Validate structure and semantics
- Resolve site and location
- Insert spectrum data and metadata into RFDATA
- Maintain FILE_TASK as transient
- Persist final state into FILE_TASK_HISTORY

Transactional model:
• 1 BIN file = 1 RFDATA transaction
• FILE_TASK is transient (runtime control only)
• FILE_TASK_HISTORY is the source of truth
"""

import sys
import os
import time
import signal
import inspect
from datetime import datetime
from collections.abc import Iterable
import json

# ---------------------------------------------------------------
# Configuration path (shared modules, handlers, config)
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
from db.dbHandlerBKP import dbHandlerBKP
from db.dbHandlerRFM import dbHandlerRFM
import shared as sh


# ===============================================================
# GLOBAL STATE
# ===============================================================

# Centralized logger
log = sh.log(target_screen=False)

# Runtime control flag (used by signal handlers)
process_status = {"running": True}


# ===============================================================
# SIGNAL HANDLING
# ===============================================================

def release_busy_hosts_on_exit() -> None:
    """
    Release all HOST records marked as BUSY by this process PID.

    This function is called on SIGINT/SIGTERM to avoid leaving
    HOSTs locked after abnormal termination.
    """
    try:
        pid = os.getpid()
        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
        db.host_release_by_pid(pid)
    except Exception as e:
        log.error(f"[CLEANUP] Failed to release BUSY hosts: {e}")


def _signal_handler(signal=None, frame=None) -> None:
    """
    Unified signal handler for SIGINT and SIGTERM.

    Stops the main loop and triggers cleanup logic.
    """
    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"SIGNAL received at {current_function}()")
    process_status["running"] = False
    release_busy_hosts_on_exit()


signal.signal(signal.SIGTERM, _signal_handler)
signal.signal(signal.SIGINT, _signal_handler)


# ===============================================================
# GEOLOCATION HELPERS
# ===============================================================

def do_revese_geocode(data: dict, attempt: int = 1, max_attempts: int = 10):
    """
    Perform reverse geocoding using Nominatim with retry logic.

    Args:
        data (dict): Must contain latitude and longitude.
        attempt (int): Current attempt counter.
        max_attempts (int): Maximum retry attempts.

    Returns:
        geopy.location.Location

    Raises:
        Exception: On timeout exhaustion or unexpected geocoding error.
    """
    point = (data["latitude"], data["longitude"])
    geocoding = Nominatim(user_agent=k.NOMINATIM_USER, timeout=5)

    try:
        return geocoding.reverse(point, timeout=5 + attempt, language="pt")
    except GeocoderTimedOut:
        if attempt < max_attempts:
            time.sleep(2)
            return do_revese_geocode(data, attempt + 1)
        raise
    except Exception:
        raise


def map_location_to_data(location, data: dict) -> dict:
    """
    Map reverse-geocoded address fields into site data dictionary.

    Uses semantic field mappings defined in config.REQUIRED_ADDRESS_FIELD.
    """
    for field, candidates in k.REQUIRED_ADDRESS_FIELD.items():
        data[field] = None
        for c in candidates:
            if c in location.raw.get("address", {}):
                data[field] = location.raw["address"][c]
                break
    return data


# ===============================================================
# VALIDATION
# ===============================================================

def validate_bin_data(bin_data: dict) -> None:
    """
    Validate structural and semantic integrity of parsed BIN data.

    This is a *fatal validation*:
    - Any inconsistency raises an exception
    - No database operations must occur before this step

    Validation scope:
    • Required top-level fields
    • Hostname format
    • GPS attributes and coordinate ranges
    • Spectrum iterable and per-spectrum semantic checks
    • Acquisition method
    """
    if not isinstance(bin_data, dict):
        raise ValueError("bin_data must be dict")

    # Required top-level keys
    for key in ("hostname", "gps", "spectrum", "method"):
        if key not in bin_data:
            raise ValueError(f"Missing required field: {key}")

    # Hostname
    if not isinstance(bin_data["hostname"], str) or not bin_data["hostname"].strip():
        raise ValueError("Invalid hostname")

    # GPS
    gps = bin_data["gps"]
    for attr in ("latitude", "longitude", "altitude"):
        if not hasattr(gps, attr):
            raise ValueError(f"GPS missing attribute: {attr}")

    if not (-90 <= gps.latitude <= 90):
        raise ValueError("Invalid latitude")

    if not (-180 <= gps.longitude <= 180):
        raise ValueError("Invalid longitude")

    # Spectrum iterable
    spectra = bin_data["spectrum"]
    if not isinstance(spectra, Iterable) or not spectra:
        raise ValueError("Spectrum iterable is empty or invalid")

    for idx, s in enumerate(spectra, start=1):
        ctx = f"spectrum[{idx}]"

        if s.start_mega >= s.stop_mega:
            raise ValueError(f"{ctx}: invalid frequency range")

        if not isinstance(s.ndata, int) or s.ndata <= 0:
            raise ValueError(f"{ctx}: invalid ndata")

        if not isinstance(s.antuid, int) or s.antuid < 0:
            raise ValueError(f"{ctx}: invalid antuid")

        if not isinstance(s.processing, str) or not s.processing:
            raise ValueError(f"{ctx}: invalid processing")

        if not isinstance(s.dtype, str) or not s.dtype:
            raise ValueError(f"{ctx}: invalid dtype")

        if s.start_dateidx >= s.stop_dateidx:
            raise ValueError(f"{ctx}: invalid time window")

    # Acquisition method
    if not isinstance(bin_data["method"], str) or not bin_data["method"].strip():
        raise ValueError("Invalid acquisition method")


# ===============================================================
# FILE OPERATIONS
# ===============================================================

def file_move(filename: str, path: str, new_path: str) -> dict:
    """
    Move a file from its source directory to the final repository path.

    Returns a dictionary compatible with insert_file().
    """
    source = f"{path}/{filename}"
    target = f"{new_path}/{filename}"
    os.renames(source, target)
    return {"filename": filename, "path": new_path, "volume": k.REPO_UID}


# ===============================================================
# MAIN LOOP
# ===============================================================

def main():
    """
    Main worker loop for BIN processing.
    """
    log.entry("[INIT] appCataloga_file_bin_process started")

    db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    db_rfm = dbHandlerRFM(database=k.RFM_DATABASE_NAME, log=log)

    while process_status["running"]:
        err = sh.ErrorHandler(log)

        file_task_id = None
        file_was_processed = False
        new_path = None
        site_id = None
        file_id = None

        try:
            # =======================================================
            # ACT I — Fetch FILE_TASK (transient)
            # =======================================================
            result = db_bp.read_file_task(
                task_type=k.FILE_TASK_PROCESS_TYPE,
                task_status=k.TASK_PENDING,
                extension=".bin",
                check_host_busy=False,
            )

            if not result:
                sh._random_jitter_sleep()
                continue
            
            # Get file data
            row, host_id, task_id = result
            file_task_id = row["FILE_TASK__ID_FILE_TASK"]
            server_path = row["FILE_TASK__NA_SERVER_FILE_PATH"]
            server_name = row["FILE_TASK__NA_SERVER_FILE_NAME"]
            host_path = row["FILE_TASK__NA_HOST_FILE_PATH"]
            host_name = row["FILE_TASK__NA_HOST_FILE_NAME"]
            extension = row["FILE_TASK__NA_EXTENSION"]
            dt_created = row["FILE_TASK__DT_FILE_CREATED"]
            dt_modified = row["FILE_TASK__DT_FILE_MODIFIED"]
            vl_file_size_kb = row["FILE_TASK__VL_FILE_SIZE_KB"]    
            filename = f"{server_path}/{server_name}"

            # =======================================================
            # ACT II — Mark FILE_TASK RUNNING
            # =======================================================
            db_bp.file_task_update(
                task_id=file_task_id,
                NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
                NU_STATUS=k.TASK_RUNNING,
                NU_PID=os.getpid(),
                NA_MESSAGE=f"Processing BIN: {filename}",
            )

            # =======================================================
            # ACT III — Parse + Validate
            # =======================================================
            bin_data = parse_bin(filename)
            validate_bin_data(bin_data)

            # =======================================================
            # ACT IV — BEGIN TRANSACTION (RFDATA)
            # =======================================================
            db_rfm.begin_transaction()

            # =======================================================
            # ACT V — SITE
            # =======================================================
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

            # =======================================================
            # ACT VI — Insert common data (BIN file, procedure, spectrum)
            # =======================================================
            
            # Insert FILE record
            file_id = db_rfm.insert_file(
                hostname=bin_data["hostname"],
                NA_VOLUME=bin_data["hostname"],
                NA_PATH=host_path,
                NA_FILE=host_name,
                NA_EXTENSION=extension,
                VL_FILE_SIZE_KB=vl_file_size_kb,
                DT_FILE_CREATED=dt_created,
                DT_FILE_MODIFIED=dt_modified,
            )

            # Insert procedure
            procedure_id = db_rfm.insert_procedure(bin_data["method"])

            # Insert equipment
            receiver = bin_data["hostname"].lower()
            dim_eq = db_rfm.get_or_create_spectrum_equipment(receiver)
            spectrum_ids = []

            # Check each spectrum inside the BIN File
            for s in bin_data["spectrum"]:

                # Build Spectrum data to be inserted in FACT_SPECTRUM
                spectrum_id = db_rfm.insert_spectrum(
                    {
                        "id_site": site_id,
                        "id_procedure": procedure_id,
                        "id_detector_type": db_rfm.insert_detector_type(k.DEFAULT_DETECTOR),
                        "id_trace_type": db_rfm.insert_trace_type(s.processing),
                        "id_equipment": dim_eq,
                        "id_measure_unit": db_rfm.insert_measure_unit(s.dtype),
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
                            s.metadata
                            if hasattr(s, "metadata") and isinstance(s.metadata, dict)
                            else {"antuid": s.antuid}
                        ),
                    }
                )

                # Insert spectrum in list for bridging
                spectrum_ids.append(spectrum_id)

            # Bridge spectrum ↔ file (single ID_FILE)
            db_rfm.insert_bridge_spectrum_file(spectrum_ids, [file_id])

            # =======================================================
            # ACT VII — COMMIT
            # =======================================================
            db_rfm.commit()

            # =======================================================
            # ACT VIII — MOVE FILE + UPDATE FILE RECORD
            # =======================================================
            new_path = db_rfm.build_path(site_id)
            new_path = f"{k.REPO_FOLDER}/{s.start_dateidx.year}/{new_path}"
            file_move(server_name, server_path, new_path)

            # Update same file record with server-side info
            # update server location
            db_rfm.insert_file(
                hostname=bin_data["hostname"],
                NA_VOLUME="reposfi",
                NA_PATH=new_path,
                NA_FILE=host_name,
                NA_EXTENSION=extension,
                VL_FILE_SIZE_KB=vl_file_size_kb,
                DT_FILE_CREATED=dt_created,
                DT_FILE_MODIFIED=dt_modified,
            )

            file_was_processed = True
            log.entry(f"[DONE] {filename}")

        except Exception as e:
            # ---------------------------------------------------
            # Rollback any partial RFDATA transaction
            # ---------------------------------------------------
            if db_rfm.in_transaction:
                db_rfm.rollback()

            # ---------------------------------------------------
            # Register error
            # ---------------------------------------------------
            err.set("Processing failed", "PROCESS", e)
            err.log_error(host_id=host_id, task_id=file_task_id)

            # ---------------------------------------------------
            # Move BIN file to TRASH_FOLDER
            # ---------------------------------------------------
            try:
                file_data = file_move(
                    filename=server_name,
                    path=server_path,
                    new_path=f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}",
                )

                # update runtime path for history
                new_path = file_data["path"]

                log.error(
                    f"[TRASH] File moved to trash after error: "
                    f"{new_path}/{server_name}"
                )

            except Exception as move_err:
                # File could not be moved → log but keep original error
                log.error(
                    f"[TRASH-FAILED] Unable to move file to trash: {move_err}"
                )

        finally:
            if file_task_id:
                # FILE_TASK is transient → always removed
                db_bp.file_task_delete(task_id=file_task_id)

                # =======================================================
                # UPDATE FILE_TASK_HISTORY
                # =======================================================
                status = k.TASK_DONE if file_was_processed else k.TASK_ERROR
                NA_MESSAGE = sh._compose_message(
                    task_type=k.FILE_TASK_PROCESS_TYPE,
                    task_status=status,
                    path=new_path if file_was_processed else None,
                    name=server_name if file_was_processed else None,
                    error_msg=err.msg if err.triggered else None,
                )
                db_bp.file_history_update(
                    task_type=k.FILE_TASK_PROCESS_TYPE,
                    file_name=server_name,
                    NA_SERVER_FILE_NAME=server_name if file_was_processed else None,
                    NA_SERVER_FILE_PATH=new_path if file_was_processed else None,
                    NU_STATUS_PROCESSING=status,
                    NA_MESSAGE=NA_MESSAGE,
                )
                
                # Create statistics task
                db_bp.host_task_statistics_create(host_id=host_id)


if __name__ == "__main__":
    main()