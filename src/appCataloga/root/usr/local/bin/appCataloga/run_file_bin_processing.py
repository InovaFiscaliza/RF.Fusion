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
import sys
sys.path.append('/etc/appCataloga')

from rfpye.parser import parse_bin

# Import libraries for file processing
import time
import random

from geopy.geocoders import Nominatim   #  Processing of geographic data
from geopy.exc import GeocoderTimedOut

# Import modules for file processing 
import config as k
import db_handler as dbh
import shared as sh
import os

import signal
import inspect

# create a warning message object
log = sh.log(target_screen=True)

process_status = {"running": True}

# Define a signal handler for SIGTERM (kill command )
def sigterm_handler(signal=None, frame=None) -> None:
    global process_status
    global log
      
    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Kill signal received at: {current_function}()")
    process_status["running"] = False

# Define a signal handler for SIGINT (Ctrl+C)
def sigint_handler(signal=None, frame=None) -> None:
    global process_status
    global log
    
    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Ctrl+C received at: {current_function}()")
    process_status['running'] = False

# Register the signal handler function, to handle system kill commands
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)

# recursive function to perform several tries in geocoding before final time out.
def do_revese_geocode(data:dict,
                      attempt=1,
                      max_attempts=10) -> dict:
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
    
    point = (data['latitude'],data['longitude'])
    
    geocodingService = Nominatim(user_agent=k.NOMINATIM_USER, timeout = 5)

    attempt = 1
    not_geocoded = True
    while not_geocoded:
        try:
            location = geocodingService.reverse(point, timeout = 5+attempt, language="pt")
            not_geocoded = False
        except GeocoderTimedOut:
            if attempt <= max_attempts:
                time.sleep(2)
                location = do_revese_geocode(data, attempt=attempt+1)
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

def map_location_to_data(location:dict,
                         data:dict) -> dict:
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
                data[field_name] = location.raw['address'][nominatimField]
                unfilled_field = False
            except KeyError:
                pass
        if unfilled_field:
            message = f"Field {nominatimField} not found in: {location.raw['address']}"
            log.warning(message)

    return data

# function that performs the file processing
def file_move(  filename: str,
                path: str,
                new_path: str) -> dict:
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
    return {'filename': filename, 'path': new_path, 'volume': k.REPO_UID}

def main():
    global process_status
    global log

    log.entry("Starting....")
    
    try:
        # create db object using databaseHandler class for the backup and processing database
        db_bp = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)
        db_rfm = dbh.dbHandler(database=k.RFM_DATABASE_NAME, log=log)
    except Exception as e:  
        log.error("Error initializing database: {e}")
        raise Exception(f"Error initializing database: {e}")

    while process_status["running"]:
        try:
            # Get one backup task from the queue in the database
            task = None
                    
            task = db_bp.get_next_file_task(task_type=db_bp.PROCESS_TASK_TYPE)

            # if there is a task in the database
            if task:
                # get metadata from bin file
                filename = f"{task['server path']}/{task['server file']}"
                
                log.entry(f"Start processing '{filename}'.")
                
                # store reference information to the file        
                try:
                    bin_data = parse_bin(filename)
                except Exception as e:
                    raise Exception(f"Error parsing file {filename}: {e}")

                # TODO: #9 check site type processing the raw gps data and set the data dictionary used by get_site_id method
                # start arranging the site data
                data = {    "longitude":bin_data["gps"].longitude,
                            "latitude":bin_data["gps"].latitude,
                            "altitude":bin_data["gps"].altitude,
                            "nu_gnss_measurements":len(bin_data["gps"]._longitude)}
                
                site = db_rfm.get_site_id(data)
                
                if site:
                    data['id_site'] = site
                    db_rfm.update_site(site = site,
                                    longitude_raw = bin_data["gps"]._longitude,
                                    latitude_raw = bin_data["gps"]._latitude,
                                    altitude_raw = bin_data["gps"]._altitude)
                else:
                    location = do_revese_geocode(data=data)
                    data = map_location_to_data(location=location,data=data)
                    site = db_rfm.insert_site(data)
                
                # update data dictionary with data associated with the entire file scope
                file_id = db_rfm.insert_file(filename=task['host file'],
                                             path=task['host path'],
                                             volume=task['host_uid'])
                
                data['id_procedure'] = db_rfm.insert_procedure(bin_data["method"])
                
                # ! WORK ONLY FOR RFEYE######  TODO: #10 refactor to a more generic solution that works for all equipment
                # Create a list of the equipment that may be present in the file, the four antennas and the receiver
                receiver = bin_data["hostname"].lower()
                rec_serial = receiver[5:]
                equipment_lst = [   f"acc_ant[0]_{rec_serial}",
                                    f"acc_ant[1]_{rec_serial}",
                                    f"acc_ant[2]_{rec_serial}",
                                    f"acc_ant[3]_{rec_serial}",
                                    receiver]
                
                # insert the equipment in the database and/or get the ids if the equipment already exists
                equipment_ids = db_rfm.insert_equipment(equipment_lst)
                
                
                spectrum_lst = []
                for spectrum in bin_data["spectrum"]:

                    data['id_detector_type'] = db_rfm.insert_detector_type(k.DEFAULT_DETECTOR)
                    data['id_trace_type'] = db_rfm.insert_trace_type(spectrum.processing)
                    data['id_measure_unit'] = db_rfm.insert_measure_unit(spectrum.dtype)
                    data['na_description'] = spectrum.description
                    data['nu_freq_start'] = spectrum.start_mega
                    data['nu_freq_end'] = spectrum.stop_mega
                    data['dt_time_start'] = spectrum.start_dateidx.strftime("%Y-%m-%d %H:%M:%S")
                    data['dt_time_end'] = spectrum.stop_dateidx.strftime("%Y-%m-%d %H:%M:%S")
                    data['nu_sample_duration'] = k.DEFAULT_SAMPLE_DURATION
                    data['nu_trace_count'] = len(spectrum.timestamp)
                    data['nu_trace_length'] = spectrum.ndata
                    try:
                        data['nu_rbw'] = spectrum.bw
                    except AttributeError:
                        data['nu_rbw'] = (data['nu_freq_end'] - data['nu_freq_start'])/data['nu_trace_length']
                        
                    data['nu_att_gain'] = k.DEFAULT_ATTENUATION_GAIN
                    
                    # create a list of equipment associated with the spectrum measurement
                    equipment = [equipment_ids[receiver],equipment_ids[f"acc_ant[{spectrum.antuid}]_{rec_serial}"]]
                    spectrum_lst.append({   "spectrum":db_rfm.insert_spectrum(data),
                                            "equipment":equipment})
                
                
                db_rfm.insert_bridge_spectrum_equipment(spectrum_lst)

                new_path = db_rfm.build_path(site_id=data['id_site'])
                new_path = f"{k.REPO_FOLDER}/{spectrum.stop_dateidx.year}/{new_path}"
                
                file_data = file_move(  filename=task['server file'],
                                        path=task['server path'],
                                        new_path=new_path)
                
                new_file_id = db_rfm.insert_file(**file_data)
                db_rfm.insert_bridge_spectrum_file(  spectrum_lst,
                                                        [file_id,new_file_id])
                
                db_bp.delete_file_task(task_id=task['task_id'])
                
                log.entry(f"Finished processing '{filename}'.")
                
            else:
                time_to_wait = int((k.MAX_FILE_TASK_WAIT_TIME+k.MAX_FILE_TASK_WAIT_TIME*random.random())/2)
                
                log.entry(f"Waiting {time_to_wait} seconds for new tasks.")
                # wait for a task to be posted
                time.sleep(time_to_wait)
        
        except Exception as e:
            
            if not task:
                log.error(f"Error in run_file_bin_processing: {e}")
                continue
            else:
                try:
                    file_data = file_move(  filename=task["server file"],
                                            path=task["server path"],
                                            new_path=f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}")
                    
                    task['server path'] = file_data['path']
                    
                    message = f"Error processing task: {e}"
                    
                    log.error(message)
                except Exception as second_e:
                    message = f"Error moving file to trash: First: {e}; raised another exception: {second_e}"
                    log.error(message)
                    pass
            
                try:
                    task['message'] = message
                    
                    db_bp.file_task_error(task_id=task['task_id'],
                                        message=message)
                    
                    db_bp.update_host_status(   host_id=task['host_id'],
                                                pending_processing=-1,
                                                processing_error=1)
                except Exception as second_e:
                    log.error(f"Error removing processing task: First: {e}; raised another exception: {second_e}")
                    
                    # raise a fatal error excpetion to stop the program
                    raise Exception(f"Exception: {e}; raised another exception: {second_e}")
            
            pass
            
    log.entry("Shutting down....")
        
if __name__ == "__main__":
    main()