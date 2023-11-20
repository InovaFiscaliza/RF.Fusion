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

        self.rfdata = {   
                        'bridge_spectrun_emitter': {
                            'id_bridge_emitter':0,
                            'fk_emitter':0,
                            'fk_spectrun':0
                            },
                        'bridge_spectrun_equipment': {
                            'id_bridge_equipment':0,
                            'fk_equipment':0,
                            'fk_spectrun':0
                            },
                        'dim_equipment_type': {
                            'id_equipment_type':0,
                            'na_equipment_type':""
                            },
                        'dim_file_type': {
                            'id_type_file':0,
                            'na_type_file':""
                            },
                        'dim_measurement_procedure': {
                            'id_procedure':0,
                            'na_procedure':""
                            },
                        'dim_site_county': {
                            'id_county_code':0,
                            'fk_state_code':0,
                            'na_county':""
                            },
                        'dim_site_district': {
                            'id_district':0,
                            'fk_county_code':0,
                            'na_district':""
                            },
                        'dim_site_state': {
                            'id_state_code':0,
                            'na_state':"",
                            'lc_state':""},
                        'dim_spectrun_detector': {
                            'id_detector_type':0,
                            'na_detector_type':""},
                        'dim_spectrun_emitter': {
                            'id_emitter':0,
                            'na_emitter':""},
                        'dim_spectrun_equipment': {
                            'id_equipment':0,
                            'na_equipment':"",
                            'fk_equipment_type':0},
                        'dim_spectrun_file': {
                            'id_file':0,
                            'id_type_file':0,
                            'na_file':"",
                            'na_dir_e_file':"",
                            'na_url':""},
                        'dim_spectrun_site': {
                            'id_site':0,
                            'fk_site_district':0,
                            'fk_county_code':0,
                            'fk_state_code':0,
                            'na_site':"",
                            'geolocation':(0,0),
                            'nu_altutude':0,
                            'nu_gnss_measurements':0},
                        'dim_spectrun_traco': {
                            'id_trace_time':0,
                            'na_trace_time':""},
                        'dim_spectrun_unidade': {
                            'id_measure_unit':0,
                            'na_measure_unit':""},
                        'fact_spectrun': {
                            'id_fact_spectrun':0,
                            'fk_file':0,
                            'fk_site':0,
                            'fk_detector_type':0,
                            'fk_trace_time':0,
                            'fk_measure_unit':0,
                            'fk_procedure':0,
                            'na_description':"",
                            'nu_freq_start':0,
                            'nu_freq_end':0,
                            'dt_time_start':0,
                            'dt_time_end':0,
                            'nu_sample_duration':0,
                            'nu_trace_count':0,
                            'nu_trace_length':0,
                            'nu_rbw':0,
                            'nu_vbw':0,
                            'nu_att_gain':0
                            }
                        }

sample = {'filename': 'rfeye002073_230331_T142000.bin',
          'file_version': 23,
          'string': 'CRFS DATA FILE V023',
          'hostname': 'RFeye002073',
          'method': 'Script2022_v2_Logger_Fixed.cfg',
          'unit_info': 'Fixed',
          'file_number': 0,
          'identifier': 'MESSAGE',
          'gps': '.latitude, .longitude, .altitude',
          'spectrum': [
              Spectrum(type=67, thread_id=290, description='PMEF 2022 (Faixa 10 de 10).', start_mega=3290, stop_mega=3799, dtype='dBm', ndata=13056, bw=73828, processing='peak', antuid=0), Spectrum(type=67, thread_id=310, description='PMEC 2022 (Faixa 2 de 10).', start_mega=105, stop_mega=140, dtype='dBm', ndata=3584, bw=18457, processing='peak', antuid=0), Spectrum(type=67, thread_id=320, description='PMEC 2022 (Faixa 3 de 10).', start_mega=155, stop_mega=165, dtype='dBm', ndata=1024, bw=18457, processing='peak', antuid=0), Spectrum(type=67, thread_id=340, description='PMEC 2022 (Faixa 5 de 10).', start_mega=405, stop_mega=410, dtype='dBm', ndata=512, bw=18457, processing='peak', antuid=0), Spectrum(type=67, thread_id=100, description='PRD 2022 (Faixa 1 de 4).', start_mega=50, stop_mega=90, dtype='dBμV/m', ndata=1024, bw=73828, processing='peak', antuid=0), Spectrum(type=67, thread_id=110, description='PRD 2022 (Faixa 2 de 4).', start_mega=70, stop_mega=110, dtype='dBμV/m', ndata=2048, bw=36914, processing='peak', antuid=0), Spectrum(type=67, thread_id=120, description='PRD 2022 (Faixa 3 de 4).', start_mega=170, stop_mega=220, dtype='dBμV/m', ndata=1280, bw=73828, processing='peak', antuid=0), Spectrum(type=67, thread_id=130, description='PRD 2022 (Faixa 4 de 4).', start_mega=470, stop_mega=700, dtype='dBμV/m', ndata=5888, bw=73828, processing='peak', antuid=0), Spectrum(type=67, thread_id=300, description='PMEC 2022 (Faixa 1 de 10).', start_mega=70, stop_mega=80, dtype='dBm', ndata=1024, bw=18457, processing='peak', antuid=0), Spectrum(type=67, thread_id=330, description='PMEC 2022 (Faixa 4 de 10).', start_mega=325, stop_mega=340, dtype='dBm', ndata=1536, bw=18457, processing='peak', antuid=0), Spectrum(type=67, thread_id=350, description='PMEC 2022 (Faixa 6 de 10).', start_mega=960, stop_mega=1429, dtype='dBm', ndata=12032, bw=73828, processing='peak', antuid=0), Spectrum(type=67, thread_id=360, description='PMEC 2022 (Faixa 7 de 10).', start_mega=1530, stop_mega=1649, dtype='dBm', ndata=3072, bw=73828, processing='peak', antuid=0), Spectrum(type=67, thread_id=370, description='PMEC 2022 (Faixa 8 de 10).', start_mega=2690, stop_mega=2899, dtype='dBm', ndata=5376, bw=73828, processing='peak', antuid=0), Spectrum(type=67, thread_id=380, description='PMEC 2022 (Faixa 9 de 10).', start_mega=5000, stop_mega=5160, dtype='dBm', ndata=4096, bw=73828, processing='peak', antuid=0), Spectrum(type=67, thread_id=390, description='PMEC 2022 (Faixa 10 de 10).', start_mega=5339, stop_mega=5459, dtype='dBm', ndata=3328, bw=73828, processing='peak', antuid=0)]}


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
def file_move(task):
    # TODO: move file from tmp to data directory
    print("nada")

def main():

    # create a warning message object
    log = sh.log(verbose=True, target_screen=True, target_file=True)

    # create db object using databaseHandler class for the backup and processing database
    db_bkp = dbh.dbHandler(database=k.BKP_DATABASE_NAME)
    db_rfm = dbh.dbHandler(database=k.RFM_DATABASE_NAME)

    while True:
        # Get one backup task from the queue in the database
        task = {"task_id": 0,
                "host_id": 0,
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
                # TODO: handle error
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
            
            data['id_site'] = site



                    "host_id": int(task[1]),
                    "host path": str(task[2]),
                    "host file": str(task[3]),
                    "server path": str(task[4]),
                    "server file": str(task[5])}
                                
            # update data dictionary with data associated with the entire file scope
            data['id_file'] = db_rfm.insert_file(file=task['host file'],path=task['host path'],volume=task['host_id'])
            data['id_procedure'] = db_rfm.insert_procedure(bin_data["method"])
            equipment = db_rfm.insert_equipment(bin_data["hostname"])
            
            data['id_spectrum'] = []
            for spectrum in bin_data["spectrum"]:
 
                data['id_detector_type'] = db_rfm.store_detector_type(k.DEFAULT_DETECTOR)
                data['id_trace_type'] = db_rfm.store_trace_time(spectrum.processing)
                data['id_measure_unit'] = db_rfm.store_measure_unit(task)

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
                
                spectrun_id = db_rfm.store_fact_spectrun(data)
                
                db_rfm.store_bridge_spectrun_equipment(spectrun_id,equipment)

            # TODO: Implement the following code to update the database with the spectrum information               
            # test if task['server path'] includes the "tmp" directory and move the file to the "data" directory
            if task['server path'].find(k.TMP_DIR) >= 0:
                data = file_move(task,data)
                    
                    log.warning(f"Backup task killed due to timeout for host {task['host_add']} after {execution_time/60} minutes.")
            
            log.entry(f"Wainting for {len(tasks)} backup tasks to finish. Next check in {k.BKP_TASK_EXECUTION_WAIT_TIME} seconds.")
            # wait for some task to finish or be posted
            time.sleep(k.BKP_TASK_EXECUTION_WAIT_TIME)
            
        else:
            log.entry(f"No backup task. Waiting for {k.BKP_TASK_REQUEST_WAIT_TIME/k.SECONDS_IN_MINUTE} minutes.")
            # wait for a task to be posted
            time.sleep(k.BKP_TASK_REQUEST_WAIT_TIME)

if __name__ == "__main__":
    main()