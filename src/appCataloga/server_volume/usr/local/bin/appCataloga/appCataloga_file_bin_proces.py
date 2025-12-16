#!/usr/bin/python3
"""Get file tasks in the control database and perform processing and cataloging.

Args:   Arguments passed from the command line should present in the format: "key=value"

        Where the possible keys are:

            "worker": int, Serial index of the worker process. Default is 0.

        (stdin): ctrl+c will soft stop the process similar to kill or systemd stop <service>. kill -9 will hard stop.

Returns (stdout): As log messages, if target_screen in log is set to True.

Raises:
    Exception: If any error occurs, the exception is raised with a message describing the error.
"""

# Set system path to include modules from /etc/appCataloga
import sys,os

CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga"))
sys.path.append(CONFIG_PATH)

from rfpye.parser import parse_bin

# Import libraries for file processing
import time
import random

from geopy.geocoders import Nominatim  #  Processing of geographic data
from geopy.exc import GeocoderTimedOut

# Import modules for file processing
import config as k
from db.dbHandlerBKP import dbHandlerBKP
from db.dbHandlerRFM import dbHandlerRFM
import shared as sh
import os

import signal
import inspect
from datetime import datetime

# create a warning message object
log = sh.log(target_screen=False)

process_status = {"running": True}

def release_busy_hosts_on_exit() -> None:
    """
    Release all HOST records marked BUSY by this process PID.
    """
    try:
        pid = os.getpid()
        log.entry(f"[CLEANUP] Releasing BUSY hosts for PID={pid}")

        db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
        db.host_release_by_pid(pid)

    except Exception as e:
        # Nunca deixar o handler quebrar o shutdown
        log.error(f"[CLEANUP] Failed to release BUSY hosts: {e}")

def sigterm_handler(signal=None, frame=None) -> None:
    global process_status, log

    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"SIGTERM received at: {current_function}()")

    process_status["running"] = False

    # Libera HOSTs presos por este PID
    release_busy_hosts_on_exit()


def sigint_handler(signal=None, frame=None) -> None:
    global process_status, log

    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"SIGINT received at: {current_function}()")

    process_status["running"] = False

    # Libera HOSTs presos por este PID
    release_busy_hosts_on_exit()


# Register the signal handler function, to handle system kill commands
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


# recursive function to perform several tries in geocoding before final time out.
def do_revese_geocode(data: dict, attempt=1, max_attempts=10) -> dict:
    """Perform reverse geocoding using Nominatim service with timeout and attempts

    Args:
        data (dict): {"latitude":0,"longitude":0}
        attempt (int, optional): Number of attempts. Defaults to 1.
        max_attempts (int, optional): _description_. Defaults to 10.
        log (obj): Log object.

    Raises:
        Exception: Geocoder Timed Out
        Exception: Error in geocoding

    Returns:
        location: nominatim location object
    """
    global log

    point = (data["latitude"], data["longitude"])

    geocodingService = Nominatim(user_agent=k.NOMINATIM_USER, timeout=5)

    attempt = 1
    not_geocoded = True
    while not_geocoded:
        try:
            location = geocodingService.reverse(
                point, timeout=5 + attempt, language="pt"
            )
            not_geocoded = False
        except GeocoderTimedOut:
            if attempt <= max_attempts:
                time.sleep(2)
                location = do_revese_geocode(data, attempt=attempt + 1)
                not_geocoded = False
            else:
                message = f"Geocoder timed out: {point}"
                log.error(message)
                raise Exception(message)
        except Exception as e:
            message = f"Error in geocoding: {e}"
            log.error(message)
            raise Exception(message)

    return location


def map_location_to_data(location: dict, data: dict) -> dict:
    """Map location data to data dictionary

    Args:
        location (dict): location data dictionary
        data (dict): data dictionary
        log (obj): Log object.

    Returns:
        dict: data dictionary
    """
    global log

    # TODO: #8 Insert site name
    for field_name, nominatim_semantic_lst in k.REQUIRED_ADDRESS_FIELD.items():
        data[field_name] = None
        unfilled_field = True
        for nominatimField in nominatim_semantic_lst:
            try:
                data[field_name] = location.raw["address"][nominatimField]
                unfilled_field = False
            except KeyError:
                pass
        if unfilled_field:
            message = f"Field {nominatimField} not found in: {location.raw['address']}"
            log.warning(message)

    return data


# function that performs the file processing
def file_move(filename: str, path: str, new_path: str) -> dict:
    """Move file to new path

    Args:
        file (str): source file name
        path (str): source file path
        new_path (str): target file path

    Raises:
        Exception: Error moving file

    Returns:
        dict: Dict with target {'file':str,'path':str,'volume':str}
    """

    # Construct the source file path
    source_file = f"{path}/{filename}"

    # Construct the target file path
    target_file = f"{new_path}/{filename}"

    # Move the file to the new path
    try:
        os.renames(source_file, target_file)
    except Exception as e:
        raise Exception(f"Error moving file {source_file} to {target_file}: {e}")

    # Return the target file information
    return {"filename": filename, "path": new_path, "volume": k.REPO_UID}


def main():
    global process_status
    global log

    log.entry("[INIT] Starting appCataloga_file_bin_process")

    # ===============================================================
    # DATABASE INIT
    # ===============================================================
    try:
        db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
        db_rfm = dbHandlerRFM(database=k.RFM_DATABASE_NAME, log=log)
    except Exception as e:
        log.error(f"[INIT] Failed to initialize database: {e}")
        raise

    # ===============================================================
    # MAIN LOOP
    # ===============================================================
    while process_status["running"]:

        # Runtime state & handler
        err = sh.ErrorHandler(log)

        # Runtime objects
        row = None
        host_info = None
        file_task_id = None
        server_path = None
        server_name = None
        file_was_processed = False
        host_id = None

        try:
            # ===========================================================
            # ACT I — Fetch task
            # ===========================================================
            try:
                row, host_id, task_id = db_bp.read_file_task(
                    task_type=k.FILE_TASK_BACKUP_TYPE,
                    task_status=k.TASK_DONE,
                    check_host_busy=False,
                )
            except Exception as e:
                err.set("Failed reading FILE_TASK", "READ_TASK", e)

            if err.triggered:
                continue

            if not row:
                sh._random_jitter_sleep()
                continue

            # Shortcuts
            file_task_id = row["FILE_TASK__ID_FILE_TASK"]
            server_path = row["FILE_TASK__NA_SERVER_FILE_PATH"]
            server_name = row["FILE_TASK__NA_SERVER_FILE_NAME"]
            host_path = row["FILE_TASK__NA_HOST_FILE_PATH"]
            host_name = row["FILE_TASK__NA_HOST_FILE_NAME"]
            filename = f"{server_path}/{server_name}"

            # ===========================================================
            # ACT II — Load HOST metadata + lock HOST + set FILE_TASK RUNNING
            # ===========================================================
            try:
                host_info = db_bp.host_read_access(host_id)
                if not host_info:
                    raise RuntimeError("host_read_access returned None")

                db_bp.file_task_update(
                    task_id=file_task_id,
                    DT_FILE_TASK = datetime.now(),
                    NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
                    NU_STATUS=k.TASK_RUNNING,
                    NA_MESSAGE=f"Processing BIN: {filename}",
                )
            except Exception as e:
                err.set("Failed loading host or marking task running", "MARK_RUNNING", e)

            if err.triggered:
                continue

            host_uid = host_info["host_uid"]
            log.entry(f"[PROCESS] Starting processing '{filename}'")

            # ===========================================================
            # ACT III — Parse BIN file
            # ===========================================================
            try:
                bin_data = parse_bin(filename)
            except FileNotFoundError:
                # Return to pending backup instead of error
                db_bp.file_task_update(
                    task_id=file_task_id,
                    NU_STATUS=k.TASK_PENDING,
                    NU_TYPE=k.FILE_TASK_BACKUP_TYPE,
                    NA_MESSAGE=f"File missing: {filename} — Retry backup",
                )
                continue
            except Exception as e:
                err.set(f"Error parsing file {filename}", "PARSE", e)

            if err.triggered:
                continue

            # ===========================================================
            # ACT IV — SITE PROCESSING
            # ===========================================================
            try:
                gps = bin_data["gps"]
                data = {
                    "longitude": gps.longitude,
                    "latitude": gps.latitude,
                    "altitude": gps.altitude,
                    "nu_gnss_measurements": len(gps._longitude),
                }

                site = db_rfm.get_site_id(data)

                if site:
                    data["id_site"] = site
                    db_rfm.update_site(
                        site=site,
                        longitude_raw=gps._longitude,
                        latitude_raw=gps._latitude,
                        altitude_raw=gps._altitude,
                    )
                else:
                    location = do_revese_geocode(data)
                    data = map_location_to_data(location, data)
                    site = db_rfm.insert_site(data)

            except Exception as e:
                # Insert coordinates info in error message
                try:
                    coord_info = (
                        f"lat={data.get('latitude')}, "
                        f"lon={data.get('longitude')}, "
                        f"alt={data.get('altitude')}"
                    )
                except Exception:
                    coord_info = "coordinates unavailable"

                err.set(
                    f"Site/location processing failed [{coord_info}]",
                    "SITE",
                    e,
                )

            if err.triggered:
                continue

            # ===========================================================
            # ACT V — RAW FILE INSERT
            # ===========================================================
            try:
                first_file_id = db_rfm.insert_file(
                    filename=host_name,
                    path=host_path,
                    volume=host_uid,
                )
                data["id_procedure"] = db_rfm.insert_procedure(bin_data["method"])
            except Exception as e:
                err.set("Failed inserting raw file", "INSERT_FILE", e)

            if err.triggered:
                continue

            # ===========================================================
            # ACT VI — SPECTRUM PROCESSING
            # ===========================================================
            try:
                receiver = bin_data["hostname"].lower()
                db_rfm.insert_equipment(receiver)

                spectrum_lst = []

                # Contexto mínimo para rastreabilidade de erro
                current_spectrum_ctx = None

                for spectrum in bin_data["spectrum"]:

                    # Atualiza contexto ANTES de qualquer operação sensível
                    current_spectrum_ctx = {
                        "receiver": receiver,
                        "processing": spectrum.processing,
                        "dtype": spectrum.dtype,
                        "freq_start": spectrum.start_mega,
                        "freq_end": spectrum.stop_mega,
                        "trace_len": spectrum.ndata,
                    }

                    na_description = getattr(
                        spectrum,
                        "description",
                        f"{spectrum.processing.upper()} — "
                        f"{spectrum.start_mega}–{spectrum.stop_mega} MHz ({spectrum.dtype})"
                    )

                    data.update({
                        "id_detector_type": db_rfm.insert_detector_type(k.DEFAULT_DETECTOR),
                        "id_trace_type": db_rfm.insert_trace_type(spectrum.processing),
                        "id_measure_unit": db_rfm.insert_measure_unit(spectrum.dtype),
                        "na_description": na_description,
                        "nu_freq_start": spectrum.start_mega,
                        "nu_freq_end": spectrum.stop_mega,
                        "dt_time_start": spectrum.start_dateidx.strftime("%Y-%m-%d %H:%M:%S"),
                        "dt_time_end": spectrum.stop_dateidx.strftime("%Y-%m-%d %H:%M:%S"),
                        "nu_sample_duration": k.DEFAULT_SAMPLE_DURATION,
                        "nu_trace_count": len(spectrum.timestamp),
                        "nu_trace_length": spectrum.ndata,
                        "nu_att_gain": k.DEFAULT_ATTENUATION_GAIN,
                    })

                    try:
                        data["nu_rbw"] = spectrum.bw
                    except AttributeError:
                        data["nu_rbw"] = (
                            (data["nu_freq_end"] - data["nu_freq_start"])
                            / data["nu_trace_length"]
                        )

                    spectrum_lst.append({
                        "spectrum": db_rfm.insert_spectrum(data),
                        "equipment": receiver,
                    })

                db_rfm.insert_bridge_spectrum_equipment(spectrum_lst)

            except Exception as e:
                # Enriquecimento do erro com contexto do espectro
                try:
                    spectrum_info = (
                        f"receiver={current_spectrum_ctx.get('receiver')}, "
                        f"processing={current_spectrum_ctx.get('processing')}, "
                        f"dtype={current_spectrum_ctx.get('dtype')}, "
                        f"freq={current_spectrum_ctx.get('freq_start')}–"
                        f"{current_spectrum_ctx.get('freq_end')} MHz, "
                        f"trace_len={current_spectrum_ctx.get('trace_len')}"
                    ) if current_spectrum_ctx else "spectrum context unavailable"
                except Exception:
                    spectrum_info = "spectrum context unavailable"

                err.set(
                    f"Spectrum processing failed [{spectrum_info}]",
                    "SPECTRUM",
                    e,
                )

            if err.triggered:
                continue


            # ===========================================================
            # ACT VII — MOVE FILE TO FINAL DESTINATION
            # ===========================================================
            try:
                new_path = db_rfm.build_path(site_id=data["id_site"])
                new_path = f"{k.REPO_FOLDER}/{spectrum.stop_dateidx.year}/{new_path}"

                file_data = file_move(
                    filename=server_name,
                    path=server_path,
                    new_path=new_path,
                )
            except Exception as e:
                err.set("Failed moving file to final path", "MOVE", e)

            if err.triggered:
                continue

            # ===========================================================
            # ACT VIII — SECOND FILE + BRIDGES
            # ===========================================================
            try:
                second_file_id = db_rfm.insert_file(**file_data)
                db_rfm.insert_bridge_spectrum_file(
                    spectrum_lst,
                    [first_file_id, second_file_id],
                )
            except Exception as e:
                err.set("Failed inserting final file", "FILE2", e)

            if err.triggered:
                continue

            # ===========================================================
            # ACT IX — Mark success (ONLY SET FLAG)
            # ===========================================================
            file_was_processed = True
            log.entry(f"[DONE] Finished processing '{filename}'")

        # ===============================================================
        # FATAL ERRORS
        # ===============================================================
        except Exception as e:
            err.set("Unexpected fatal error", "UNEXPECTED", e)

        # ===============================================================
        # FINALLY — CENTRALIZED UPDATES
        # ===============================================================
        finally:

            # SUCCESS FLOW
            if file_was_processed:
                try:
                    db_bp.file_task_delete(task_id=file_task_id)
                    db_bp.file_history_update(
                        task_type=k.FILE_TASK_PROCESS_TYPE,
                        file_name=server_name,
                        NA_MESSAGE=sh._compose_message(k.FILE_TASK_PROCESS_TYPE, k.TASK_DONE),
                    )
                    db_bp.host_task_statistics_create(host_id=host_id)
                except Exception as e:
                    log.error(f"[FINALIZE] Error finalizing successful task: {e}")

            # ERROR FLOW
            if err.triggered and file_task_id:
                try:
                    try:
                        file_move(
                            filename=server_name,
                            path=server_path,
                            new_path=f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}",
                        )
                    except Exception as e2:
                        log.warning(f"[CLEANUP] Failed moving file to trash: {e2}")

                    db_bp.file_task_update(
                        task_id=file_task_id,
                        NU_STATUS=k.TASK_ERROR,
                        NA_SERVER_FILE_PATH = f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}",
                        NA_MESSAGE=err.msg,
                    )
                    
                    db_bp.file_history_update(
                        task_type=k.FILE_TASK_PROCESS_TYPE,
                        file_name=server_name,
                        NA_SERVER_FILE_PATH = f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}",
                        NA_MESSAGE=err.msg,
                    )
                except Exception as e3:
                    fatal = f"Fatal during error flow: Main={err.exc}; Cleanup={e3}"
                    log.error(fatal)
                    raise Exception(fatal)




if __name__ == "__main__":
    main()
