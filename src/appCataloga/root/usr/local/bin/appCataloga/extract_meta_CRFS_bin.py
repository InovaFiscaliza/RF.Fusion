#!/usr/bin/env python

# # File processor
# This script perform the following tasks
# - Use whatdog to monitor folder
# -
# Set system path to include modules from /etc/appCataloga
import sys
sys.path.append('/etc/appCataloga')

from rfpye.main import extract_bin_data


# Import libraries for file processing
import pandas as pd
import time

from geopy.geocoders import Nominatim   #  Processing of geographic data
from geopy.exc import GeocoderTimedOut


import os                               #  file processing

# Import file with constants used in this code that are relevant to the operation
import config as k
import db_handler as dbh

# Class used to process bin files based on PFPye 
class BinFileHandler:

    def __init__(self, fileFullPath):

        # store reference infortion to the file        
        filename = os.path.basename(fileFullPath)

        # use rfpye to extract metadata from the file
        rfpyeDict = extract_bin_data(path = fileFullPath)

        # clean out the outer dictionary form rfpye 
        # ! This script process only single files
        self.metadata = rfpyeDict[filename]
        self.metadata['fileFullPath'] = fileFullPath

        # perform adjustments from the file path to the desired URL
        targetPath = fileFullPath.replace(k.FOLDER_TO_WATCH,k.TARGET_ROOT_URL)
        targetPath = targetPath.replace('\\','/')
        self.metadata['URL'] = targetPath

    # recursive function to perform several tries in geocoding before final time out.
    def doReveseGeocode(self, attempt=1, max_attempts=10):

        # find location data references using the open free service of Nominatim - https://nominatim.org/ 
        point = (self.metadata['Latitude'],self.metadata['Longitude'])
        geocodingService = Nominatim(user_agent=k.NOMINATIM_USER, timeout = 5)

        # try using service with extended timeout and increasing up to 15 seconds and delays of 2 seconds between consecutive attempts
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
        
        
def main():
    
    FAILED_TASK = { 'host_id': running_task['host_id'],
                    'nu_pending_processing': 0, 
                    'nu_processing_error': 1}

    # Connect to the database
    # create db object using databaseHandler class
    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME)

    # Get one backup task to start
    task = db.nextBackup()

    # create a list to hold the future objects
    tasks = []

    # Use ThreadPoolExecutor to limit the number of concurrent threads
    with concurrent.futures.ThreadPoolExecutor(k.MAX_THREADS) as executor:
        
        while True:
            
            print(f"Starting backup for {task['host']}.")
            
            # test if len(tasks) < k.MAX_THREADS
            # if true, add task to tasks list
            # else, wait for a task to finish and remove it from the list
            if len(tasks) < k.MAX_THREADS:
                # add task to tasks list
                task["map_itarator"] = executor.map(host_backup, task)
                
                tasks.append(task)
                
            else:
                # loop through tasks list and remove completed tasks
                for running_task in tasks:
                    # test if the runnning_task is completed
                    if running_task["map_itarator"].done():

                        try:
                            # get the result from the map_itarator
                            task_status = running_task["map_itarator"].result()
                            
                            # remove task from tasks list
                            tasks.remove(running_task)
                            
                            # If running task was successful (result not empty or False)
                            if task_status:
                                
                                # remove task from database
                                db.remove_backup_task(task)
                                                                
                                print(f"Completed backup from {task['host']}")
                            else:
                                task_status = FAILED_TASK

                                print(f"Error in backup from {task['host']}. Will try again later.")

                        # except error in running_task
                        except running_task["map_itarator"].exception() as e:
                            # if the running task has an error, set task_status to False
                            task_status = FAILED_TASK
                            print(f"Error in backup from {task['host']}. Will try again later. {str(e)}")
                        
                        # any other exception
                        except Exception as e:
                            # if the running task has an error, set task_status to False
                            task_status = FAILED_TASK
                            print(f"Error in backup from {task['host']}. Will try again later. {str(e)}")
                        finally:
                            # update backup summary status for the host_id
                            db.update_host_backup_status(task_status)
            
            # Get the next backup task
            task = db.nextBackup()
            
            while not task:
                print("No backup task. Waiting for 5 minutes.")
                # wait for 5 minutes
                time.sleep(k.FIVE_MINUTES)
                
                # try again to get a task
                task = db.nextBackup()

if __name__ == "__main__":
    main()