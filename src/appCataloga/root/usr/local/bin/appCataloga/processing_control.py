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

from rfpye.parser import parse_bin

from rich import print

# Import libraries for file processing
import time

from geopy.geocoders import Nominatim   #  Processing of geographic data
from geopy.exc import GeocoderTimedOut

# Import modules for file processing 
import config as k
import db_handler as dbh
import shared as sh
import os

# recursive function to perform several tries in geocoding before final time out.
def do_revese_geocode(data:dict,
                      attempt=1,
                      max_attempts=10,
                      log=sh.log()) -> dict:
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
                location = do_revese_geocode(data, attempt=attempt+1,log=log)
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
                         data:dict,
                         log:sh.log()) -> dict:
    """Map location data to data dictionary

    Args:
        location (dict): location data dictionary
        data (dict): data dictionary
        log (obj): Log object.

    Returns:
        dict: data dictionary
    """
    # TODO: #8 Insert site name
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
                new_path: str,
                volume=None) -> dict:
    """Move file to new path

    Args:
        file (str): source file name
        path (str): source file path
        volume (str): source volume name (default: k.DEFAULT_VOLUME)
        new_path (str): target file path

    Raises:
        Exception: Error moving file
        
    Returns:
        dict: Dict with target {'file':str,'path':str,'volume':str}
    """

    if not volume:
        volume = k.DEFAULT_VOLUME_NAME
        
    # Construct the source file path
    source_file = f"{path}/{filename}"

    # Construct the target file path
    target_file = f"{k.DEFAULT_VOLUME_MOUNT}/{new_path}/{filename}"
    
    # Move the file to the new path
    try:
        os.renames(source_file, target_file)
    except Exception as e:
        raise Exception(f"Error moving file {source_file} to {target_file}: {e}")
    
    # Return the target file information
    return {'filename': filename, 'path': new_path, 'volume': volume}

def main():

    # create a warning message object
    log = sh.log()

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
                filename = f"{task['server path']}/{task['server file']}"
                
                log.entry(f"Starting processing of {filename}")
                
                # store reference infortion to the file        
                try:
                    bin_data = parse_bin(filename)
                except:
                    log.error(f"Error parsing file {filename}")

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
                    location = do_revese_geocode(data=data,log=log)
                    data = map_location_to_data(location=location,data=data,log=log)
                    site = db_rfm.insert_site(data)
                
                # update data dictionary with data associated with the entire file scope
                file_id = db_rfm.insert_file(filename=task['host file'],
                                             path=task['host path'],
                                             volume=task['host_uid'])
                
                data['id_procedure'] = db_rfm.insert_procedure(bin_data["method"])
                
                # ! WORK ONLY FOR RFEYE######  TODO: #10 refactor to a more generic solution that works for all equipment
                # Create a list of the equipment that may be present in the file
                equipment_id = []
                receiver = bin_data["hostname"]
                equipment_lst = [   f"acc_ant[0]_{receiver[5:]}",
                                    f"acc_ant[1]_{receiver[5:]}",
                                    f"acc_ant[2]_{receiver[5:]}",
                                    f"acc_ant[3]_{receiver[5:]}",
                                    receiver]
                
                # insert the equipment in the database and get the ids
                equipment_ids = db_rfm.insert_equipment(equipment_lst)
                #TODO #11 get only the receiver in this list
                
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
                    except:
                        data['nu_rbw'] = (data['nu_freq_end'] - data['nu_freq_start'])/data['nu_trace_length']
                        
                    data['nu_att_gain'] = k.DEFAULT_ATTENUATION_GAIN
                    
                    equipment = [equipment_ids[-1],equipment_ids[spectrum.antuid]]
                    spectrum_lst.append({   "spectrum":db_rfm.insert_spectrum(data),
                                            "equipment":equipment})
                
                
                db_rfm.insert_bridge_spectrum_equipment(spectrum_lst)

                # test if task['server path'] includes the "tmp" directory and move the file to the "data" directory
                if task['server path'].find(k.TARGET_TMP_FOLDER) >= 0:
                    new_path = db_rfm.build_path(site_id=data['id_site'])
                    new_path = f"{spectrum.stop_dateidx.year}/{new_path}"
                    
                    file_data = file_move(  filename=task['server file'],
                                            path=task['server path'],
                                            new_path=new_path)
                    new_file_id = db_rfm.insert_file(**file_data)
                    db_rfm.insert_bridge_spectrum_file(  spectrum_lst,
                                                        [file_id,new_file_id])
                
                db_bkp.remove_processing_task(task_id=task['task_id'],
                                              host_id=task['host_id'])
                
                                                
            else:
                log.entry(f"No processing task. Waiting for {k.BKP_TASK_REQUEST_WAIT_TIME/k.SECONDS_IN_MINUTE} minutes.")
                # wait for a task to be posted
                time.sleep(k.BKP_TASK_REQUEST_WAIT_TIME)
        except Exception as e:
            log.error(f"Error processing task: {e}")
            pass
        except KeyboardInterrupt:
            log.entry("Keyboard interrupt. Exiting.")
            break
        
if __name__ == "__main__":
    main()