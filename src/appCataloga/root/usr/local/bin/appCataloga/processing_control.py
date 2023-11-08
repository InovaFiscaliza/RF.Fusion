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

class BinFileHandler:

    def __init__(self, fileFullPath):

        # store reference infortion to the file

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

# function that performs the file processing
def file_processing(task):
    

def main():
    
    # create a warning message object
    log = sh.log(verbose=True, target_screen=True, target_file=True)
    
    # create db object using databaseHandler class for the backup and processing database
    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME)

    while True:
        # Get one backup task from the queue in the database
        task = db.next_backup_task()

        """	
            task={  "task_id": str,
                    "host_id": str,
                    "host_add": str,
                    "port": str,
                    "user": str,
                    "password": str}"""        
        
        # if there is a task in the database
        if task:
            # check if there is a task already running for the same host and remove it if it is the case, avoiding the creation of multiple tasks for the same host
            new_task = True
            for running_task in tasks:
                if running_task["host_id"] == task["host_id"]:
                    db.remove_backup_task(task)
                    new_task = False
            
            # if it really is a new task and the total number of tasks running didn't top the capacity alloted
            if new_task and (task_counter < k.BKP_MAX_PROCESS):
                log.entry(f"Adding backup task for {task['host_add']}.")

                command = ( f'bash -c '
                            f'"source ~/miniconda3/etc/profile.d/conda.sh; '
                            f'conda activate appdata; '
                            f'python3 {k.BACKUP_SINGLE_HOST_MODULE} '
                            f'host_id={task["host_id"]} '
                            f'host_add={task["host_add"]} '
                            f'port={task["port"]} '
                            f'user={task["user"]} '
                            f'pass={task["password"]}"')
                
                # add task to tasks list
                task["process_handle"] = subprocess.Popen([command],
                                                        stdout=subprocess.PIPE,
                                                        stderr=subprocess.PIPE,
                                                        text=True,
                                                        shell=True)
                
                task["birth_time"] = time.time()
                task["nu_backup_error"] = 0
                tasks.append(task)                
                task_counter += 1 
                          
                # remove task from database
                db.remove_backup_task(task)

        # if there are tasks running
        if len(tasks) > 0:
            # loop through tasks list and remove completed tasks
            for running_task in tasks:
                # Get output from the running task, wainting 5 seconds for timeout
                try:
                    task_output, task_error = running_task["process_handle"].communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    task_output = False
                    task_error = False
                    # in case of timeout, check if the task is running for more than the alotted time and remove it if it is the case, otherwise, pass
                    if time.time() - running_task["birth_time"] > k.BKP_TASK_EXECUTION_TIMEOUT:
                        running_task["process_handle"].kill()
                        tasks.remove(running_task)
                        log.warning(f"Backup task canceled due to timeout {task['host_add']}")
                    pass
                
                # if there is output from the task, process it
                if task_output:
                    try:
                        task_dict_output = json.loads(task_output)
                        
                        # remove task from tasks list
                        tasks.remove(running_task)
                            
                        # add the list of files to the processing task list TODO: if len(task_dict_output['done_backup_list']) > 0:
                        db.add_processing_task(hostid=task_dict_output['host_id'],
                                            done_backup_list=task_dict_output['done_backup_list'])
            
                        # update backup summary status for the host_id in case of previous errors
                        task_dict_output['nu_backup_error'] = task_dict_output['nu_backup_error'] + running_task['nu_backup_error']
                        
                        # update backup summary status for the host_id
                        db.update_backup_status(task_dict_output)
                        
                        log.entry(f"Completed backup from {running_task['host_add']}")
                        
                    except json.JSONDecodeError as e:
                        log.error(f"Malformed JSON received. Dumped\n***Output: {task_output}\n***Error: {e}")
                    
                elif task_error:
                    running_task["nu_backup_error"] += 1

                    log.entry(f"Error in backup from {task['host_add']}. Will try again later. Error: {task_error}")
                    
                # if birth time is more than alowed time, remove task from tasks list
                execution_time = time.time() - running_task["birth_time"]
                if  execution_time > k.BKP_TASK_EXECUTION_TIMEOUT:
                    
                    # kill the process
                    running_task["process_handle"].kill()
                    
                    # remove task from tasks list
                    tasks.remove(running_task)
                        
                    # remove task from database. If there are pending backup, it will be consider in the next cycle.
                    db.remove_backup_task(running_task)

                    failed_task["nu_backup_error"] = running_task["nu_backup_error"] + 1

                    # update backup summary status for the host_id
                    db.update_backup_status(failed_task)
                    
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