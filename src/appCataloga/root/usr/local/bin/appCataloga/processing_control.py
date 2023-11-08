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

from rfpye import parse_bin
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

class BinProcessing:

    def __init__(self, task):

        filename = f"{task['server path']}\{task['server file']}"
        # store reference infortion to the file        
        try:
            self.bin_metadata = parse_bin(filename)
        except:
            # TODO: handle error
            log.error(f"Error parsing file {filename}")
        
        # merge the two dictionaries
        self.metadata = {**task, **self.bin_metadata}

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
            
            bin_data.do_reveseGeocode()

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