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

# create a warning message object
log = sh.log(verbose=True, target_screen=True, target_file=True)

# create db object using databaseHandler class for the backup and processing database
db_bkp = dbh.dbHandler(database=k.BKP_DATABASE_NAME)
db_rfm = dbh.dbHandler(database=k.RFM_DATABASE_NAME)

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
class BinProcessing:

    def __init__(self, task):

        filename = f"{task['server path']}\{task['server file']}"
        # store reference infortion to the file        
        try:
            raw_bin_data = parse_bin(filename)
        except:
            # TODO: handle error
            log.error(f"Error parsing file {filename}")
        
        

        

    # recursive function to perform several tries in geocoding before final time out.
    def doReveseGeocode(self, attempt=1, max_attempts=10):

        # try to find the location data in the database
        
        
        # find location data references using the open free service of Nominatim - https://nominatim.org/ 
        point = (self.metadata['Latitude'],self.metadata['Longitude'])
        geocodingService = Nominatim(user_agent=k.NOMINATIM_USER, timeout = 5)

        # try using service with extended timeout and increasing up to 15 seconds and delays of 2 seconds between consecutive attempts
        attempt = 1
        while attempt <= k.MAX_NOMINATIN_ATTEMPTS:
            try:
                location = geocodingService.reverse(point, timeout = 5+attempt, language="pt")
            except GeocoderTimedOut:
                if attempt <= max_attempts:
                    time.sleep(2)
                    location = self.doReveseGeocode(point, attempt=attempt+1)
                raise

        # populate location data with information from the geocoding result.
        # Loop through all required fields as defined in the constat, and get data from dictionary using associated nominatim semantic translations
        for fieldName, nominatimSemanticList in k.REQUIRED_ADDRESS_FIELD.items():
            self.metadata[fieldName] = None
            for nominatimField in nominatimSemanticList:
                try:
                    self.metadata[fieldName] = location.raw['address'][nominatimField]  
                except:
                    pass

        # get the state code for the state name retrived from the geocoding service
        self.metadata['State_Code'] = k.STATE_CODES[self.metadata['State']]

    def computeFinaltime(self, row):
        return row['Timestamp'][-1]

    def exportMetadata(self,exportFilename):

        # convert multiple thread dictionary into dataframe adjusting columns to correspond to the desired CSV format
        df = pd.DataFrame(self.metadata['Fluxos'])
        df = df.transpose()

        # compute the measurement final time to each data thread
        df['Final_Time'] = df.apply (lambda row: self.computeFinaltime(row), axis=1)

        # drop detailed  
        df.drop(columns=['Timestamp'], inplace=True)

        # add data that is common to all data threads
        df['Script_Version'] = self.metadata['Script_Version']
        df['Equipment_ID'] = self.metadata['Equipment_ID']
        df['Latitude'] = self.metadata['Sum_Latitude'] / self.metadata['Count_GPS']
        df['Longitude'] = self.metadata['Sum_Longitude'] / self.metadata['Count_GPS']
        df['Altitude'] = self.metadata['Altitude']
        df['Count_GPS'] =  self.metadata['Count_GPS']
        df['State'] = self.metadata['State']
        df['State_Code'] = self.metadata['State_Code']
        df['County'] = self.metadata['County']
        df['District'] = self.metadata['District']
        df['File_Name'] = self.metadata['File_Name']
        df['URL'] = self.metadata['URL']

        #reorder dataframe according to reference list
        df = df[k.CSV_COLUMN_ORDER]
#TODO: handle write errors
        #export dataframe
        df.to_csv(path_or_buf=exportFilename, index=False)
        
        if k.VERBOSE: print(f"     Output metadata as CSV to {exportFilename}")

# function that performs the file processing
def file_processing(task):
    print("nada")

def main():
        
    while True:
        # Get one backup task from the queue in the database
        task = db_bkp.next_processing_task()

        # if there is a task in the database
        if task:
            # check if there is a task already running for the same host and remove it if it is the case, avoiding the creation of multiple tasks for the same host
            # get metadata from bin file
            bin_data = BinProcessing(task)
            
            bin_data.do_revese_geocode()

            # update database with metadata
            db.update_processing_status(BinFile.metadata)
        
            # TODO: Implement error handling to rollback partial database updates                
            dbHandle.updateDatabase(BinFile)

            self.fileMove(event, BinFileMetadata.newFilePath, BinFileMetadata.FileName)
                    
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