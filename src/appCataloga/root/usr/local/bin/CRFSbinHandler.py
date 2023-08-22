#! /root/miniconda3/envs/rflook/bin/python

# # File processor
# This script perform the following tasks
# - Use whatdog to monitor folder
# -

from rfpye.main import extract_bin_data


# Import libraries for file processing
import pandas as pd
import time

from geopy.geocoders import Nominatim   #  Processing of geographic data
from geopy.exc import GeocoderTimedOut


import os                               #  file processing

# Import file with constants used in this code that are relevant to the operation
import constants as k

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