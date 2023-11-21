#!/usr/bin/env python
"""
Access the processing list from BKPDATA database and perform the processing task.
    
    Usage:
        processing_control
            
    Returns:
        (json) =  { 'Total Files': (int),
                    'Files to backup': (int),
                    'Last Backup data': (str)
                    'Days since last backup': (int),
                    'Status': (int), 
                    'Message': (str)}

        Status may be 1=valid data or 0=error in the script
        All keys except "Message" are suppresed when Status=0
        Message describe the error or warning information
        
        
"""
# Set system path to include modules from /etc/appCataloga
import sys
sys.path.append('/etc/appCataloga')

from fastcore.xtras import Path
from rfpye.utils import get_files
from rfpye.parser import parse_bin

from rich import print

# Import libraries for file processing
import pandas as pd
import time

from geopy.geocoders import Nominatim   #  Processing of geographic data
from geopy.exc import GeocoderTimedOut

import concurrent.futures

# Import modules for file processing 
import config as k
import db_handler as dbh
import shared as sh
import shutil
import shutil

# recursive function to perform several tries in geocoding before final time out.
def do_revese_geocode(data = {"latitude":0,"longitude":0}, attempt=1, max_attempts=10, log=sh.log()):

    # find location data references using the open free service of Nominatim - https://nominatim.org/ 
    point = (data['latitude'],data['longitude'])
    
    geocodingService = Nominatim(user_agent=k.NOMINATIM_USER, timeout = 5)

    # try using service with extended timeout and increasing up to 15 seconds and delays of 2 seconds between consecutive attempts
    attempt = 1
    while attempt <= k.MAX_NOMINATIN_ATTEMPTS:
        try:
            location = geocodingService.reverse(point, timeout = 5+attempt, language="pt")
        except GeocoderTimedOut:
            if attempt <= max_attempts:
                time.sleep(2)
                location = do_revese_geocode(data, attempt=attempt+1,log=log)
            else:
                message = f"GeocoderTimedOut: {point}"
                log.error(message)
                raise Exception(message)

    # populate location data with information from the geocoding result.
    # Loop through all required fields as defined in the constat, and get data from dictionary using associated nominatim semantic translations
    for field_name, nominatim_semantic_lst in k.REQUIRED_ADDRESS_FIELD.items():
        data[field_name] = None
        unfilled_field = True
        for nominatimField in nominatim_semantic_lst:
            try:
                data[field_name] = location.raw['address'][nominatimField]
                unfilled_field = False
            except:
                pass
        if unfilled_field:
            message = f"Field {nominatimField} not found in: {location.raw['address']}"
            log.warning(message)

    return data

# function that performs the file processing

def file_move(  filename: str,
                path: str,
                volume=None,
                new_path: str) -> dict:
    """Move file to new path

    Args:
        file (str): source file name
        path (str): source file path
        volume (str): source volume name (default: k.DEFAULT_VOLUME)
        new_path (str): target file path

    Returns:
        dict: Dict with target {'file':str,'path':str,'volume':str}
    """

    if volume is None:
        volume = k.DEFAULT_VOLUME
        
    # Construct the source file path
    source_file = f"{path}/{filename}"

    # Construct the target file path
    target_file = f"{new_path}/{filename}"

    # Move the file to the new path
    shutil.move(source_file, target_file)

    # Return the target file information
    return {'file': filename, 'path': new_path, 'volume': volume}

def main():
    # Call the file_move function
    result = file_move('example.txt', '/path/to/source', 'Volume1', '/path/to/target')

    # Print the result
    print(result)

if __name__ == "__main__":
    main()

def main():

    # create a warning message object
    log = sh.log(verbose=True, target_screen=True, target_file=True)

    try:
        # create db object using databaseHandler class for the backup and processing database
        db_bkp = dbh.dbHandler(database=k.BKP_DATABASE_NAME)
        db_rfm = dbh.dbHandler(database=k.RFM_DATABASE_NAME)
    except Exception as e:
        log.error("Error initializing database: {e}")
        raise

    while True:
        try:
            # Get one backup task from the queue in the database
            task = {"task_id": 0,
                    "host_id": 0,
                    "host_uid": "none",
                    "host path": "none",
                    "host file": "none",
                    "server path": "none",
                    "server file": "none"}
                    
            task = db_bkp.next_processing_task()

            # if there is a task in the database
            if task:
                # check if there is a task already running for the same host and remove it if it is the case, avoiding the creation of multiple tasks for the same host
                # get metadata from bin file
                filename = f"{task['server path']}\{task['server file']}"
                
                # store reference infortion to the file        
                try:
                    bin_data = parse_bin(filename)
                except:
                    log.error(f"Error parsing file {filename}")

                # start arranging the site data
                data={'longitude':bin_data["gps"].longitude,
                    'latitude':bin_data["gps"].latitude}
                
                site = db_rfm.get_site_id(data)
                
                if site:
                    data['id_site'] = site
                    db_rfm.update_site(site = site,
                                    longitude_raw = bin_data["gps"]._longitude,
                                    latitude_raw = bin_data["gps"]._latitude,
                                    altitude_raw = bin_data["gps"]._altitude)
                else:
                    data = do_revese_geocode(data=data,log=log)
                    site = db_rfm.insert_site(data)
                
                # update data dictionary with data associated with the entire file scope
                file_id = db_rfm.insert_file(file=task['host file'],path=task['host path'],volume=task['host_uid'])
                data['id_procedure'] = db_rfm.insert_procedure(bin_data["method"])
                
                equipment_id = []
                receiver = bin_data["hostname"]
                equipment_id.append(db_rfm.insert_equipment(receiver))
                
                for spectrum in bin_data["spectrum"]:

                    data['id_detector_type'] = db_rfm.insert_detector_type(k.DEFAULT_DETECTOR)
                    data['id_trace_type'] = db_rfm.insert_trace_time(spectrum.processing)
                    data['id_measure_unit'] = db_rfm.insert_measure_unit(spectrum.dtype)

                    data['na_description'] = spectrum.description
                    data['nu_freq_start'] = spectrum.start_mega
                    data['nu_freq_end'] = spectrum.stop_mega
                    data['dt_time_start'] = spectrum.start_dateidx
                    data['dt_time_end'] = spectrum.stop_dateidx
                    data['nu_sample_duration'] = k.DEFAULT_SAMPLE_DURATION
                    data['nu_trace_count'] = len(bin_data["spectrum"][0].timestamp)
                    data['nu_trace_length'] = spectrum.ndata
                    data['nu_rbw'] = spectrum.bw
                    data['nu_vbw'] = k.DEFAULT_VBW,
                    data['nu_att_gain'] = k.DEFAULT_ATTENUATOR,
                    
                    spectrun_id = db_rfm.insert_spectrun(data)
                    
                    antenna = f"{receiver}_ant[{spectrum.antuid}]"
                    equipment_id.append(db_rfm.insert_equipment(antenna))
                    
                    db_rfm.store_bridge_spectrun_equipment(spectrun_id,equipment_id)
                    db_rfm.store_bridge_spectrun_file(spectrun_id,file_id)

                # test if task['server path'] includes the "tmp" directory and move the file to the "data" directory
                if task['server path'].find(k.TMP_DIR) >= 0:
                    new_path = db_rfm.build_path(site_id=data['id_site'])
                    new_path = f"{data['dt_time_end'].year}\{new_path}"
                    
                    try:
                        file_data = file_move(  file=task['server file'],
                                                path=task['server path'],
                                                new_path=new_path)
                        file_id = db_rfm.insert_file(*file_data)
                        db_rfm.store_bridge_spectrun_file(spectrun_id,file_id)
                    except:
                        log.error(f"Error moving file {task['server path']}\{task['server file']} to {file_data['volume']}\{file_data['path']}\{file_data['file']}")
                        pass
                
                db_bkp.remove_processing_task(task['task_id'])
                
                                                
            else:
                log.entry(f"No processing task. Waiting for {k.BKP_TASK_REQUEST_WAIT_TIME/k.SECONDS_IN_MINUTE} minutes.")
                # wait for a task to be posted
                time.sleep(k.BKP_TASK_REQUEST_WAIT_TIME)
        except Exception as e:
            log.error("Error processing task: {e}")
            pass
        except KeyboardInterrupt:
            log.entry("Keyboard interrupt. Exiting.")
            break
        
if __name__ == "__main__":
    main()