#!/usr/bin/env python
""" This module manage all database operations for the appCataloga scripts """

# Import libraries for:
import mysql.connector
import os

import sys
import time
import numpy as np

from typing import List, Union

# Import file with constants used in this code that are relevant to the operation
import config as k
import shared as sh

class dbHandler():
    """Class associated with the database operations for the appCataloga scripts
    """

    def __init__(self, database=k.RFM_DATABASE_NAME, log=sh.log()):
        """Initialize a new instance of the DBHandler class.

        Args:
            db_file (str): The path to the SQLite database file.
        """
        
        self.db_connection = None
        self.cursor = None
        self.database = database
        self.data = None
        self.log = log

    def _connect(self):
        """Try to connect to the database using the parameters in the config.py file

        Raises:
            Exception: from pyodbc.connect
            ValueError: from pyodbc.connect

        Returns:
            self.db_connection: update to the database connection
            self.cursor: update to the database cursor
        """
                
        #connect to database using parameters in the config.py file
        config = {
            'user': k.DB_USER_NAME,
            'password': k.DB_PASSWORD,
            'host': k.SERVER_NAME,
            'database': self.database
        }
        
        self.db_connection = mysql.connector.connect(**config)
        
        self.cursor = self.db_connection.cursor(buffered=True)

    def _disconnect(self):
        """ Disconnect from the database for graceful exit

        Raises:
            Exception: from pyodbc.disconnect
            ValueError: from pyodbc.disconnect

        Returns:
            self.db_connection: update to None
            self.cursor: update to None
        """

        self.cursor.close()
        self.db_connection.close()

    def get_site_id(self,
                    data:dict) -> (int, bool):
        """Get site database id based on the coordinates in the data dictionary and limiting distances in the config.py file

        Args:
            data (dict): {"latitude":float, "longitude":float} Site information with required coordinates. 

        Raises:
            Exception: Error retrieving location coordinates from database

        Returns:
            int: DB key to the site
        """

        self._connect()

        # Get data from the point in the database that is nearest to the measurement location indicated in the file
        query = (f"SELECT"
                f" ID_SITE,"
                f" ST_X(GEO_POINT) as LONGITUDE,"
                f" ST_Y(GEO_POINT) as LATITUDE "
                f"FROM DIM_SPECTRUM_SITE"
                f" ORDER BY ST_Distance_Sphere(GEO_POINT, ST_GeomFromText('POINT({data['longitude']} {data['latitude']})', 4326))"
                f" LIMIT 1;")

        try:
            self.cursor.execute(query)

            nearest_site = self.cursor.fetchone()
        except:
            self._disconnect()
            raise Exception("Error retrieving location coordinates from database")


        try:
            nearest_site_id = int(nearest_site[0])
            nearest_site_longitude = float(nearest_site[1])
            nearest_site_latitude = float(nearest_site[2])

            # Check if the database point is within the expected deviation of the measured location coordinates
            near_in_longitude = (abs(data['longitude'] - nearest_site_longitude) < k.MAXIMUM_GNSS_DEVIATION)
            near_in_latitude = (abs(data['latitude'] - nearest_site_latitude) < k.MAXIMUM_GNSS_DEVIATION)
            location_exist_in_db = (near_in_latitude and near_in_longitude)
        except (IndexError, TypeError, ValueError):
            location_exist_in_db = False
        except Exception as e:
            self._disconnect()
            raise Exception(f"Error retrieving location coordinates from database: {e}")

        self._disconnect()

        if location_exist_in_db:
            return nearest_site_id
        else:
            return False
        
    def update_site(self,   
                    site = int,
                    longitude_raw = [float],
                    latitude_raw = [float],
                    altitude_raw = [float],
                    log=sh.log()) -> None:
        """Update site coordinates in the database for existing site

        Args:
            site (int): The site database id.
            longitude_raw ([float]): List of measured longitude in degrees.
            latitude_raw ([float]): List of measured latitude in degrees.
            altitude_raw ([float]): List of measured altitude in meters.
            log (log): Logging object
            
        Returns:
            none: none
        """

        self._connect()
        
        # get data from the point in the database that is nearest to the measurement location indicated in the file
        query = (f"SELECT"
             f" ST_X(GEO_POINT) as LONGITUDE,"
             f" ST_Y(GEO_POINT) as LATITUDE,"
             f" NU_ALTITUDE,"
             f" NU_GNSS_MEASUREMENTS "
             f"FROM DIM_SPECTRUM_SITE "
             f"WHERE"
             f" ID_SITE = {site};")

        try:
            # Try to get the nearest match
            self.cursor.execute(query)

            nearest_site = self.cursor.fetchone()
        except:
            self._disconnect()
            raise Exception(f"Error retrieving site {self.data['Site_ID']} from database")            

        try:
            db_site_longitude = float(nearest_site[0])
            db_site_latitude = float(nearest_site[1])
            db_site_altitude = float(nearest_site[2])
            db_site_nu_gnss_measurements = int(nearest_site[3])
        except:
            self._disconnect()
            raise Exception(f"Invalid data returned for site {self.data['Site_ID']} from database")

        # if number of measurements in the database greater than the maximum required number of measurements.
        if db_site_nu_gnss_measurements < k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS:

            # add point coordinates in the file to the estimator already in the database
            longitudeSum = longitude_raw.sum() + ( db_site_longitude * db_site_nu_gnss_measurements ) 
            latitudeSum = latitude_raw.sum() + ( db_site_latitude * db_site_nu_gnss_measurements )
            altitudeSum = altitude_raw.sum() + ( db_site_altitude * db_site_nu_gnss_measurements )
            nu_gnss_measurements = db_site_nu_gnss_measurements + len(longitude_raw)
            longitude = longitudeSum / nu_gnss_measurements
            latitude = latitudeSum / nu_gnss_measurements
            altitude = altitudeSum / nu_gnss_measurements

            # construct query update point location in the database
            query = (   f"UPDATE DIM_SPECTRUM_SITE "
                        f" SET GEO_POINT = ST_GeomFromText('POINT({longitude} {latitude})'),"
                        f" NU_ALTITUDE = {altitude},"
                        f" NU_GNSS_MEASUREMENTS = {nu_gnss_measurements} "
                        f"WHERE ID_SITE = {site};")

            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                self._disconnect()
                
                log.entry(f'Updated location at latitude: {latitude}, longitude: {longitude}')    
            except:
                self._disconnect()
                raise Exception(f"Error updating site {self.data['Site_ID']} from database")

        else:
            # Do not update, avoiding unnecessary processing and variable numeric overflow
            log.entry(f'Site {site} at latitude: {db_site_latitude}, longitude: {db_site_longitude} reached the maximum number of measurements. No update performed.')


    def _get_geographic_codes(  self,  
                                data: dict) -> (int,int,int):
        """Get DB keys for state, county and district based on the data in the object

        Args:
            data (dict): {"state":"state name", "county":"city,town name", "district":"suburb name"}

        Raises:
            Exception: Fail to retrive state name from database
            Exception: Fail to retrive county name from database

        Returns:
            (int,int,int): Tuple with the DB keys for state, county and district
        """

        self._connect()
        
        # search database for existing state entry and get the existing key
        query = (   f"SELECT ID_STATE "
                    f"FROM DIM_SITE_STATE "
                    f"WHERE"
                    f" NA_STATE LIKE '{data['state']}';")

        self.cursor.execute(query)
        
        try:
            db_state_id = int(self.cursor.fetchone()[0])
        except:
            raise Exception(f"Error retrieving state name {data['state']}")

        # search database for existing county name entry within the identified State and get the existing key
        # If state_id is 53, handle the special case of DC, with no counties
        if db_state_id == 53:
            db_county_id = 5300108
        else:
            county = data['county'].replace(' ',' AND ')
            query = (   f"SELECT ID_COUNTY "
                        f"FROM DIM_SITE_COUNTY "
                        f"WHERE"
                        f" MATCH(NA_COUNTY) AGAINST('{county})')"
                        f" AND FK_STATE = {db_state_id};")

            self.cursor.execute(query)
            
            try:
                db_county_id = int(self.cursor.fetchone()[0])
            except:
                self._disconnect()
                raise Exception(f"Error retrieving county name {data['County']}")

        #search database for the district name, inserting new value if non existant
        district = data['district'].replace(' ',' AND ')
        query = (f"SELECT ID_DISTRICT "
                f"FROM DIM_SITE_DISTRICT "
                f"WHERE"
                f" MATCH(NA_DISTRICT) AGAINST('{district}')"
                f" AND FK_COUNTY = {db_county_id};")
        
        self.cursor.execute(query)
        
        try:
            db_district_id = int(self.cursor.fetchone()[0])
        except:
            query = (f"INSERT INTO DIM_SITE_DISTRICT"
                    f" (FK_COUNTY,"
                    f" NA_DISTRICT) "
                    f"VALUES"
                    f" ({db_county_id},"
                    f" '{data['district']}');")
            
            self.cursor.execute(query)
            self.db_connection.commit()
            
            db_district_id = int(self.cursor.lastrowid)

        self._disconnect()

        return (db_state_id, db_county_id, db_district_id)

    def insert_site(self,
                    data={  "latitude":0,
                            "longitude":0,
                            "altitude":0,
                            "state":"state name",
                            "county":"city,town name",
                            "district":"suburb name"}) -> int:
        """Create a new site in the database

        Args:
            data (dict): {  "latitude":0,
                            "longitude":0,
                            "altitude":0,
                            "state":"state name",
                            "county":"city,town name",
                            "district":"suburb name"}

        Raises:
            Exception: Error inserting site in the database
            
        Returns:
            int: DB key to the new site
        """

        # TODO: #7 Insert site name and site type
        (db_state_id, db_county_id, db_district_id) = self._get_geographic_codes(data=data)

        self._connect()
        
        # construct query to create new sie in the database
        query = (   f"INSERT INTO DIM_SPECTRUM_SITE"
                            f" (GEO_POINT,"
                            f" NU_ALTITUDE,"
                            f" NU_GNSS_MEASUREMENTS,"
                            f" FK_STATE,"
                            f" FK_COUNTY,"
                            f" FK_DISTRICT) "
                            f"VALUES "
                            f" (ST_GeomFromText('POINT({data['longitude']} {data['latitude']})'),"
                            f" {data['altitude']},"
                            f" {data['nu_gnss_measurements']},"
                            f" {db_state_id},"
                            f" {db_county_id},"
                            f" {db_district_id})")

        try:
            self.cursor.execute(query)
            self.db_connection.commit()
        
            db_site_id = int(self.cursor.lastrowid)

            self._disconnect()
        except:
            raise Exception(f"Error creating new site using query: {query}")
            

        return db_site_id

    def build_path(self, site_id:int) -> str:
        """Build the path to the site folder in the database in the format "LC State Code/county_id/site_id"

        Args:
            site_id (int): DB key to the site

        Raises:
            Exception: Error retrieving site path from database

        Returns:
            str: Path to the site folder in the database
        """

        self._connect()

        query = (f"SELECT"
            f" DIM_SITE_STATE.LC_STATE,"
            f" DIM_SPECTRUM_SITE.FK_COUNTY,"
            f" DIM_SPECTRUM_SITE.ID_SITE "
            f"FROM DIM_SPECTRUM_SITE"
            f" JOIN DIM_SITE_STATE ON DIM_SPECTRUM_SITE.FK_STATE = DIM_SITE_STATE.ID_STATE"
            f" WHERE"
            f" DIM_SPECTRUM_SITE.ID_SITE = {site_id};")
        
        try:
            self.cursor.execute(query)
        except:
            self._disconnect()
            raise Exception(f"Error retrieving site information using query: {query}")

        try:
            site_path = self.cursor.fetchone()
            new_path = f"{site_path[0]}/{site_path[1]}/{site_path[2]}"
        except:
            self._disconnect()
            raise Exception(f"Error building path from site information from query: {query}")

        self._disconnect()

        return new_path

    # method to insert file entry in the database if it does not exist, otherwise return the existing key
    def insert_file(self,
                    filename:str,
                    path:str,
                    volume:str) -> int:
        """Create a new file entry in the database if it does not exist, otherwise return the existing key

        Args:
            filename (str): File name
            path (str): File path
            volume (str): File volume
        Raises:
            Exception: Error inserting file in the database

        Returns:
            int: DB key to the new file
        """

        self._connect()
        
        query = (f"SELECT ID_FILE "
                f"FROM DIM_SPECTRUM_FILE "
                f"WHERE"
                f" NA_FILE = '{filename}' AND"
                f" NA_PATH = '{path}' AND"
                f" NA_VOLUME = '{volume}';")
        
        try:
            self.cursor.execute(query)
        except:
            self._disconnect()
            raise Exception("Error retrieving file using query: {query}")

        try:
            file_id = int(self.cursor.fetchone()[0])
        except:            
            query =(f"INSERT INTO DIM_SPECTRUM_FILE"
                    f" (NA_FILE,"
                    f" NA_PATH,"
                    f" NA_VOLUME) "
                    f"VALUES"
                    f" ('{filename}',"
                    f" '{path}',"
                    f" '{volume}')")
            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                file_id = int(self.cursor.lastrowid)
            except:
                self._disconnect()
                raise Exception(f"Error creating new file entry using query: {query}")
        
        self._disconnect()
        
        return file_id
    
    # method to insert procedure entry in the database if it does not exist, otherwise return the existing key
    def insert_procedure(self, procedure_name: str) -> int:
        """Create a new procedure entry in the database if it does not exist, otherwise return the existing key

        Args:
            procedure_name (str): Procedure name

        Raises:
            Exception: Error inserting procedure in the database

        Returns:
            int: DB key to the new procedure
        """

        self._connect()
        
        query = (f"SELECT ID_PROCEDURE "
                f"FROM DIM_SPECTRUM_PROCEDURE "
                f"WHERE"
                f" NA_PROCEDURE = '{procedure_name}';")
        
        try:
            self.cursor.execute(query)
        except:
            self._disconnect()
            raise Exception("Error retrieving procedure using query: {query}")

        try:
            procedure_id = int(self.cursor.fetchone()[0])
        except:            
            query =(f"INSERT INTO DIM_SPECTRUM_PROCEDURE"
                    f" (NA_PROCEDURE) "
                    f"VALUES"
                    f" ('{procedure_name}')")

            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                procedure_id = int(self.cursor.lastrowid)
            except:
                self._disconnect()
                raise Exception(f"Error creating new procedure entry using query: {query}")
        
        self._disconnect()
        
        return procedure_id

    def _get_equipment_types(self) -> dict:
        """Load all equipment types from the database and create a dictionary with equipmenty_type_uid as key and equipment_type_id as value

        Returns:
            dict: {equipment_type_uid:equipment_type_id}
        """
        
        self._connect()
        
        query = (f"SELECT"
                 f" ID_EQUIPMENT_TYPE,"
                 f" NA_EQUIPMENT_TYPE_UID "
                f"FROM DIM_EQUIPMENT_TYPE;")
        
        try:
            self.cursor.execute(query)
            
            equipment_types = self.cursor.fetchall()
        except:
            self._disconnect()
            raise Exception("Error retrieving equipment types from database")
        
        self._disconnect()
        
        equipment_types_dict = {}
        try:
            for equipment_type in equipment_types:
                equipment_type_id = int(equipment_type[0])
                equipment_type_uid = str(equipment_type[1])
                equipment_types_dict[equipment_type_uid] = equipment_type_id
        except:
            raise Exception("Error parsing equipment types retrieved from database")
        
        return equipment_types_dict

    def insert_equipment(self, equipment:Union[str, List[str]]) -> list:
        """Create a new equipment entry in the database if it does not exist, otherwise return the existing key

        Args:
            equipment (str/[str]): String of list of strings containing the equipment name(s)

        Raises:
            Exception: Invalid input. Expected a string or a list of strings.
            Exception: Error retrieving equipment type for _equipment_name_ from database
            Exception: Error retrieving equipment data for _equipment_name_ from database
            Exception: Error creating new equipment entry for _equipment_name_ in database

        Returns:
            int: list of db keys to the new or existing equipment entries
        """
        
        if isinstance(equipment, str):
            equipment_names = [equipment]
        elif isinstance(equipment, list):
            equipment_names = equipment
        else:
            raise Exception("Invalid input. Expected a string or a list of strings.")
        
        equipment_types = self._get_equipment_types()
        
        equipment_ids = []
        
        self._connect()
        for name in equipment_names:
            name_lower_case = name.lower()
            equipment_type_id = False
            
            for type_uid, type_id in equipment_types.items():
                if name_lower_case.find(type_uid) != -1:
                    equipment_type_id = type_id
                    break
            
            if not equipment_type_id:
                raise Exception(f"Error retrieving equipment type for {name}")
            
            query = (f"SELECT ID_EQUIPMENT "
                    f"FROM DIM_SPECTRUM_EQUIPMENT "
                    f"WHERE"
                    f" LOWER(NA_EQUIPMENT) LIKE '{name_lower_case}';")
            
            try:
                self.cursor.execute(query)
            except:
                self._disconnect()
                raise Exception(f"Error retrieving equipment data using query: {query}")

            try:
                equipment_id = int(self.cursor.fetchone()[0])
            except:
                query =(f"INSERT INTO DIM_SPECTRUM_EQUIPMENT"
                        f" (NA_EQUIPMENT,"
                        f" FK_EQUIPMENT_TYPE) "
                        f"VALUES"
                        f" ('{name}',"
                        f" {equipment_type_id})")

                try:
                    self.cursor.execute(query)
                    self.db_connection.commit()
                
                    equipment_id = int(self.cursor.lastrowid)
                except:
                    self._disconnect()
                    raise Exception(f"Error creating new equipment using query: {query}")
            
            equipment_ids.append(equipment_id)
            
        self._disconnect()
        
        return equipment_ids

    def insert_detector_type(self, detector:str) -> int:
        """Insert detector type in the database if it does not exist, otherwise return the existing key

        Args:
            detector (str): Detector name

        Raises:
            Exception: Error retrieving detector type from database
            Exception: Error creating new detector entry in database
        
        Returns:
            int: DB key to the new detector type
        """
        
        self._connect()
        
        query = (f"SELECT ID_DETECTOR "
                    f"FROM DIM_SPECTRUM_DETECTOR "
                    f"WHERE"
                    f" NA_DETECTOR = '{detector}';")
        
        try:
            self.cursor.execute(query)
        except:
            self._disconnect()
            raise Exception("Error retrieving detector type using query: {query}")

        try:
            detector_id = int(self.cursor.fetchone()[0])
        except:            
            query =(f"INSERT INTO DIM_SPECTRUM_DETECTOR"
                    f" (NA_DETECTOR) "
                    f"VALUES"
                    f" ('{detector}')")

            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                detector_id = int(self.cursor.lastrowid)
            except:
                self._disconnect()
                raise Exception(f"Error creating new detector entry using query: {query}")
        
        self._disconnect()
        
        return detector_id

    
    def insert_trace_type(self, trace_name) -> int:
        """Insert trace type in the database if it does not exist, otherwise return the existing key

        Args:
            processing: The processing time of the trace

        Raises:
            Exception: Error retrieving trace type from database
            Exception: Error creating new trace type entry in database
            
        Returns:
            int: DB key to the new trace time
        """
        self._connect()
        
        query = (f"SELECT ID_TRACE_TYPE "
                 f"FROM DIM_SPECTRUM_TRACE_TYPE "
                 f"WHERE"
                 f" NA_TRACE_TYPE = '{trace_name}';")
        
        try:
            self.cursor.execute(query)
        except:
            self._disconnect()
            raise Exception(f"Error retrieving trace type using query: {query}")

        try:
            trace_type_id = int(self.cursor.fetchone()[0])
        except:            
            query = (f"INSERT INTO DIM_SPECTRUM_TRACE_TYPE"
                     f" (NA_TRACE_TYPE) "
                     f"VALUES"
                     f" ('{trace_name}')")

            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                trace_type_id = int(self.cursor.lastrowid)
            except:
                self._disconnect()
                raise Exception(f"Error creating new trace time entry using query: {query}")
        
        self._disconnect()
        
        return trace_type_id
    
    def insert_measure_unit(self, unit_name:str) -> int:
        """Insert measure unit in the database if it does not exist, otherwise return the existing key

        Args:
            unit_name (str): The name of the measure unit

        Raises::
            Exception: Error retrieving measure unit from database
            Exception: Error creating new measure unit entry in database
            
        Returns:
            int: DB key to the new measure unit
        """
        self._connect()
        
        query = (f"SELECT ID_MEASURE_UNIT "
                 f"FROM DIM_SPECTRUM_UNIT "
                 f"WHERE"
                 f" NA_MEASURE_UNIT = '{unit_name}';")
        
        try:
            self.cursor.execute(query)
        except:
            self._disconnect()
            raise Exception(f"Error retrieving measure unit using query: {query}")

        try:
            measure_unit_id = int(self.cursor.fetchone()[0])
        except:            
            query = (f"INSERT INTO DIM_SPECTRUM_UNIT"
                     f" (NA_MEASURE_UNIT) "
                     f"VALUES"
                     f" ('{unit_name}')")

            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                measure_unit_id = int(self.cursor.lastrowid)
            except:
                self._disconnect()
                raise Exception(f"Error creating new measure unit entry using query: {query}")
        
        self._disconnect()
        
        return measure_unit_id
    
    def insert_spectrum(self, data: dict) -> int:
        """Insert a spectrum entry in the database if it does not exist, otherwise return the existing key. Equality creteria is based on the following fields: same site, time and frequency scope and same resolution in both dimensions

        Args:
            data (dict): Dictionary containing a summary of the measurement spectrum data

        Raises:
            Exception: Error retrieving spectrum from database
            Exception: Error creating new spectrum entry in database
        
        Returns:
            int: DB key to the new spectrum entry
        """
        self._connect()

        # build query to locate a site that mathces data['id_site'] and data['nu_freq_start'] and data['nu_freq_end'] and data['dt_time_start'] and data['dt_time_end'] and data['nu_trace_count'] and data['nu_trace_length']
        query = (f"SELECT ID_SPECTRUM "
                    f"FROM FACT_SPECTRUM "
                    f"WHERE"
                    f" FK_SITE = {data['id_site']} AND"
                    f" FK_TRACE_TYPE = {data['id_trace_type']} AND"
                    f" NU_FREQ_START = {data['nu_freq_start']} AND"
                    f" NU_FREQ_END = {data['nu_freq_end']} AND"
                    f" DT_TIME_START = '{data['dt_time_start']}' AND"
                    f" DT_TIME_END = '{data['dt_time_end']}' AND"
                    f" NU_TRACE_COUNT = {data['nu_trace_count']} AND"
                    f" NU_TRACE_LENGTH = {data['nu_trace_length']};")
        
        try:
            self.cursor.execute(query)
        except:
            self._disconnect()
            raise Exception(f"Error retrieving spectrum using query: {query}")
        
        try:
            spectrum_id = int(self.cursor.fetchone()[0])
        except:
            query = (f"INSERT INTO FACT_SPECTRUM"
                        f" (FK_SITE,"
                        f" FK_PROCEDURE,"
                        f" FK_DETECTOR,"
                        f" FK_TRACE_TYPE,"
                        f" FK_MEASURE_UNIT,"
                        f" NA_DESCRIPTION,"
                        f" NU_FREQ_START,"
                        f" NU_FREQ_END,"
                        f" DT_TIME_START,"
                        f" DT_TIME_END,"
                        f" NU_SAMPLE_DURATION,"
                        f" NU_TRACE_COUNT,"
                        f" NU_TRACE_LENGTH,"
                        f" NU_RBW,"
                        f" NU_ATT_GAIN) "
                        f"VALUES"
                        f" ({data['id_site']},"
                        f" {data['id_procedure']},"
                        f" {data['id_detector_type']},"
                        f" {data['id_trace_type']},"
                        f" {data['id_measure_unit']},"
                        f" '{data['na_description']}',"
                        f" {data['nu_freq_start']},"
                        f" {data['nu_freq_end']},"
                        f" '{data['dt_time_start']}',"
                        f" '{data['dt_time_end']}',"
                        f" {data['nu_sample_duration']},"
                        f" {data['nu_trace_count']},"
                        f" {data['nu_trace_length']},"
                        f" {data['nu_rbw']},"
                        f" {data['nu_att_gain']})")
        
            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                spectrum_id = int(self.cursor.lastrowid)
            except:
                self._disconnect()
                raise Exception(f"Error creating new spectrum entry using query: {query}")

        self._disconnect()

        return spectrum_id
    
    def insert_bridge_spectrum_equipment(self, spectrum_lst:list) -> None:
        """Insert entries connecting spectrum measurements and equipment in the database in a N:N relationship

        Args:
            spectrum_lst (list): Of dictionaries with the following structure:
                [{"spectrum": (int) List of spectrum_id entries in the database to be associated with the equipments in the list
                 "equipment": (list): List of equipment_id entries in the database to be associated with the spectrum measurements}]
            
        Raises:
            Exception: Error creating new spectrum equipment relationship in database
            
        Returns:
            none: none
        """

        self._connect()

        for entry in spectrum_lst:
            for equipment in entry["equipment"]:
                query = (f"INSERT IGNORE INTO BRIDGE_SPECTRUM_EQUIPMENT"
                          f" (FK_SPECTRUM,"
                          f" FK_EQUIPMENT) "
                          f"VALUES"
                          f" ({entry['spectrum']},"
                          f" {equipment}); ")

                try:
                    self.cursor.execute(query)
                except:
                    self._disconnect()
                    raise Exception(f"Error creating new spectrum equipment entry using query: {query}")
        
        self.db_connection.commit()
        self._disconnect()
    
    def insert_bridge_spectrum_file(self,
                                    spectrum_lst:list,
                                    file_lst:list) -> None:
        """Insert entries connecting spectrum measurements and file in the database in a N:N relationship

        Args:
            spectrum_id (list): List of spectrum entries in the database to be associated with the equipments in the list
            file_id (list): List of file entries in the database to be associated with the spectrum measurements
            
        Raises:
            Exception: Error creating new spectrum file relationship in database
            
        Returns:
            none: none
        """
        
        self._connect()
        
        for entry in spectrum_lst:
            for file_id in file_lst:
                query = (f"INSERT IGNORE INTO BRIDGE_SPECTRUM_FILE"
                            f" (FK_SPECTRUM,"
                            f" FK_FILE) "
                            f"VALUES"
                            f" ({entry['spectrum']},"
                            f" {file_id});")

                try:
                    self.cursor.execute(query)
                except:
                    self._disconnect()
                    raise Exception(f"Error creating new spectrum file entry using query: {query}")
                
        self.db_connection.commit()
        
        self._disconnect()
        
    # Internal method to add host to the database
    def _add_host(self, hostid:str, host_uid:str) -> None:
        """This method adds a new host to the database if it does not exist.
            Initialization of host statistics is essential to avoid errors and simplify later database queries and updates.
        
        Args:
            hostid (str): Zabbix host id primary key.

        Returns:
            none: none
        """
        # connect to the database
        self._connect()
        
        # compose query to create a new host entry in the BPDATA database, setting all values to zero. If host already in the database, do nothing
        query = (f"INSERT IGNORE INTO HOST "
                    f"(ID_HOST, NA_HOST_UID, "
                    f"NU_HOST_FILES, "
                    f"NU_PENDING_BACKUP, NU_BACKUP_ERROR, "
                    f"NU_PENDING_PROCESSING, NU_PROCESSING_ERROR) "
                    f"VALUES "
                    f"('{hostid}', '{host_uid}', "
                    f"0, "
                    f"0, 0, "
                    f"0, 0);")
        
        # update database
        self.cursor.execute(query)
        self.db_connection.commit()
        
        self._disconnect()

    # get host status data from the database
    def get_host_task_status(self,hostid="host_id"):        
        
        # connect to the database
        self._connect()

        # compose query to get host data from the BKPDATA database
        query = (f"SELECT "
                    f"ID_HOST, "
                    f"NU_HOST_FILES, "
                    f"NU_PENDING_BACKUP, "
                    f"DT_LAST_BACKUP, "
                    f"NU_PENDING_PROCESSING, "
                    f"DT_LAST_PROCESSING "
                    f"FROM HOST "
                    f"WHERE ID_HOST = '{hostid}';")
        
        # get host data from the database
        self.cursor.execute(query)
        
        db_output = self.cursor.fetchone()
        
        # get the output in a dictionary format, converting datetime objects to epoch time
        try:
            output = {'Host ID': int(db_output[0])}
                
            try:
                output['Total Files'] = int(db_output[1])
            except (IndexError, ValueError) as e:
                raise Exception(f"Error retrieving 'Total Files' for host {hostid} from database")
            except (AttributeError, TypeError):
                pass

            try:
                output['Files to backup'] = int(db_output[2])
            except (IndexError, ValueError):
                raise Exception(f"Error retrieving 'Files to backup' for host {hostid} from database")                
            except (AttributeError, TypeError):
                output['Files to backup'] = "N/A"
                pass
            
            try:
                output['Last Backup date'] = db_output[3].timestamp()
            except (IndexError, ValueError):
                raise Exception(f"Error retrieving 'Last Backup date' for host {hostid} from database")
            except (AttributeError, TypeError):
                output['Last Backup date'] = "N/A"
                pass
                
            try:
                output['Files to process'] = int(db_output[4])
            except (IndexError, ValueError):
                raise Exception(f"Error retrieving 'Files to process' for host {hostid} from database")
            except (AttributeError, TypeError):
                output['Files to process'] = "N/A"
                pass
                
            try:
                output['Last Processing date'] = db_output[5].timestamp()
                pass
            except (IndexError, ValueError):
                raise Exception(f"Error retrieving 'Last Processing date' for host {hostid} from database")
            except (AttributeError, TypeError):
                output['Last Processing date'] = "N/A"
            
            output['Status'] = 1
            output['Message'] = ""
        except Exception as e:
            output = {  "Status": 0, 
                        "Message": f"Error retrieving data for host {hostid}: {e}"}

        self._disconnect()
        
        return output
    
    # Method add a new host to the backup queue
    def add_backup_task(self,
                        hostid:str,
                        host_uid:str,
                        host_addr:str,
                        host_port:str,
                        host_user:str,
                        host_passwd:str) -> None:
        """This method checks if the host is already in the database and if not, adds it to the backup queue
        
        Args:
            hostid (str): Zabbix host id primary key. 
            host_addr (str): Remote host IP/DNS address.
            host_port (str): Remote host SSH access port.
            host_user (str): Remote host access user.
            host_passwd (str): Remote host access password.

        Returns:
            None
        """
        
        # create a new host entry in the database if it does not exist
        self._add_host(hostid,host_uid)

        # connect to the database
        self._connect()
        
        # compose query to add 1 to PENDING_BACKUP in the HOST table in BPDATA database for the given host_id
        query = (f"UPDATE HOST "
                    f"SET NU_PENDING_BACKUP = NU_PENDING_BACKUP + 1 "
                    f"WHERE ID_HOST = '{hostid}';")
        
        # update database
        self.cursor.execute(query)
        self.db_connection.commit()
        
        # compose query to set the backup task in the BPDATA database
        query = (f"INSERT INTO BKP_TASK "
                 f"(FK_HOST, DT_BKP_TASK, NO_HOST_ADDRESS,NO_HOST_PORT,NO_HOST_USER,NO_HOST_PASSWORD) "
                 f"VALUES "
                 f"('{hostid}', NOW(), '{host_addr}', '{host_port}', '{host_user}', '{host_passwd}');")

        # update database
        self.cursor.execute(query)
        self.db_connection.commit()
        self._disconnect()
        
    # get next host in the list for data backup
    def next_backup_task(self):
        """This method gets the next host in the list for data backup

        Returns:
            dict: Dictionary with the pending task information: task_id, host_id, host, port, user, password
        """        
        
        # connect to the database
        self._connect()

        # build query to get the next backup task
        query = (   "SELECT ID_BKP_TASK, FK_HOST, NO_HOST_ADDRESS, NO_HOST_PORT, NO_HOST_USER, NO_HOST_PASSWORD "
                    "FROM BKP_TASK "
                    "ORDER BY DT_BKP_TASK "
                    "LIMIT 1;")
        
        self.cursor.execute(query)
        
        task = self.cursor.fetchone()
        self._disconnect()

        try:
            output = {  "task_id": int(task[0]),
                        "host_id": int(task[1]),
                        "host_add": str(task[2]),
                        "port": int(task[3]),
                        "user": str(task[4]),
                        "password": str(task[5])}
        except:
            output = False
        
        return output

    # Method to update the backup status information in the database
    def update_backup_status(self, task_status):
        """This method updates the backup status information in the database	
        
        Args:
            task_status (dict): {   "host_id": host_id,
                                    "nu_host_files": nu_host_files,
                                    "nu_pending_backup": nu_pending_backup,
                                    "nu_backup_error": nu_backup_error}
        Returns:
            none: none
        """
                
        # connect to the database
        self._connect()

        query_parts = []
        if task_status['nu_host_files'] > 0:
            query_parts.append(f"NU_HOST_FILES = NU_HOST_FILES + {task_status['nu_host_files']}")
        elif task_status['nu_host_files'] < 0:
            query_parts.append(f"NU_HOST_FILES = NU_HOST_FILES - {-task_status['nu_host_files']}")
        else:
            pass

        if task_status['nu_pending_backup'] > 0:
            query_parts.append(f"NU_PENDING_BACKUP = NU_PENDING_BACKUP + {task_status['nu_pending_backup']}")
        elif task_status['nu_pending_backup'] < 0:
            query_parts.append(f"NU_PENDING_BACKUP = NU_PENDING_BACKUP - {-task_status['nu_pending_backup']}")
        else:
            pass
        
        if task_status['nu_backup_error'] > 0:
            query_parts.append(f"NU_BACKUP_ERROR = NU_BACKUP_ERROR + {task_status['nu_backup_error']}")
        elif task_status['nu_backup_error'] < 0:
            query_parts.append(f"NU_BACKUP_ERROR = NU_BACKUP_ERROR - {-task_status['nu_backup_error']}")
        else:
            pass
        
        query = f"UPDATE HOST SET " + f",".join(map(str, query_parts)) + f", DT_LAST_PROCESSING = NOW() WHERE ID_HOST = {task_status['host_id']};"
            
        self.cursor.execute(query)
        self.db_connection.commit()
                
        self._disconnect()

    # Method to remove a completed backup task from the database
    def remove_backup_task(self, task):
        # connect to the database
        self._connect()
        
        # compose and excecute query to delete the backup task from the BKPDATA database
        query = (f"DELETE FROM BKP_TASK "
                 f"WHERE ID_BKP_TASK = {task['task_id']};")
        self.cursor.execute(query)

        # update database statistics for the host
        query = (f"UPDATE HOST "
                 f"SET NU_PENDING_BACKUP = NU_PENDING_BACKUP - 1, "
                 f"DT_LAST_BACKUP = NOW() "
                 f"WHERE ID_HOST = '{task['host_id']}';")
        self.cursor.execute(query)
        self.db_connection.commit()
        
        self._disconnect()

    # Method to add a new processing task to the database
    def add_processing_task(self,
                            hostid:int,
                            done_backup_list=list):
        """This method adds tasks to the processing queue
        
        Args:
            hostid (int): Zabbix host id primary key. Defaults to "1".
            done_backup_list (list): List of files that were recently copied, in the format:[{"remote":remote_file_name,"local":local_file_name}]. Defaults to [].

        Returns:
            _type_: _description_
        """
        # connect to the database
        self._connect()
    
        # compose query to add 1 to PENDING_BACKUP in the HOST table in BPDATA database for the given host_id
        nu_processing = len(done_backup_list)
        query = (f"UPDATE HOST "
                    f"SET NU_PENDING_PROCESSING = NU_PENDING_PROCESSING + {nu_processing} "
                    f"WHERE ID_HOST = '{hostid}';")
        
        # update database
        self.cursor.execute(query)
        self.db_connection.commit()

# convert done_backup_list list of dicionaries into a list of tuples
        for idx, item in enumerate(done_backup_list):
            done_backup_list[idx] = (hostid,
                    os.path.dirname(item["remote"]), os.path.basename(item["remote"]),
                    os.path.dirname(item["local"]), os.path.basename(item["local"]))
                    
        # compose query to set the process task in the database using executemany method
        query = (f"INSERT INTO PRC_TASK "
                    f"(FK_HOST, NO_HOST_FILE_PATH, NO_HOST_FILE_NAME, NO_SERVER_FILE_PATH, NO_SERVER_FILE_NAME, DT_PRC_TASK) "
                    f"VALUES "
                    f"(%s, %s, %s, %s, %s, NOW());")

        # update database
        self.cursor.executemany(query,done_backup_list)
        self.db_connection.commit()
        self._disconnect()

    # Method to get next host in the list for processing
    def next_processing_task(self) -> dict:
        """This method gets the next host in the list for data processing

        Returns:
            dict: "task_id": int(task[0]),
                  "host_id": int(task[1]),
                  "host_uid": str(task[2]),
                  "host path": str(task[3]),
                  "host file": str(task[4]),
                  "server path": str(task[5]),
                  "server file": str(task[6])}
        """
        
        # connect to the database
        self._connect()

        # build query to get the next backup task with host_uid and BO_ERROR_FLAG different from 1
        query = (   "SELECT PRC_TASK.ID_PRC_TASK, "
                            "PRC_TASK.FK_HOST, HOST.NA_HOST_UID, "
                            "PRC_TASK.NO_HOST_FILE_PATH, PRC_TASK.NO_HOST_FILE_NAME, "
                            "PRC_TASK.NO_SERVER_FILE_PATH, PRC_TASK.NO_SERVER_FILE_NAME "
                    "FROM PRC_TASK "
                    "JOIN HOST ON PRC_TASK.FK_HOST = HOST.ID_HOST "
                    "WHERE PRC_TASK.BO_ERROR_FLAG <> 1 "
                    "ORDER BY PRC_TASK.DT_PRC_TASK "
                    "LIMIT 1;")
        
        self.cursor.execute(query)
        
        task = self.cursor.fetchone()
        
        try:
            output = {"task_id": int(task[0]),
                    "host_id": int(task[1]),
                    "host_uid": str(task[2]),
                    "host path": str(task[3]),
                    "host file": str(task[4]),
                    "server path": str(task[5]),
                    "server file": str(task[6])}
        except:
            output = False
        
        self._disconnect()
        
        return output

    # Method to update the processing status information in the database
    def _update_processing_status(self, host_id, pending_processing, processing_error):
        # connect to the database
        self._connect()
        
        # compose and excecute query to update the processing status by adding pending_processing variable to existing value in the database
        query_parts = []
        
        if pending_processing > 0:
            query_parts.append(f"NU_PENDING_PROCESSING = NU_PENDING_PROCESSING + {pending_processing}")
        elif pending_processing < 0:
            query_parts.append(f"NU_PENDING_PROCESSING = NU_PENDING_PROCESSING - {-pending_processing}")
        else:
            pass
        
        if processing_error > 0:
            query_parts.append(f"NU_PROCESSING_ERROR = NU_PROCESSING_ERROR + {processing_error}")
        elif processing_error < 0:
            query_parts.append(f"NU_PROCESSING_ERROR = NU_PROCESSING_ERROR - {-processing_error}")
        else:
            pass
        
        query = f"UPDATE HOST SET " + ",".join(map(str, query_parts)) + f", DT_LAST_PROCESSING = NOW() WHERE ID_HOST = {host_id};"

        self.cursor.execute(query)
        self.db_connection.commit()
        
        
        query = (f"UPDATE HOST SET "
                    f"NU_PENDING_PROCESSING = NU_PENDING_PROCESSING + {pending_processing}, "
                    f"DT_LAST_PROCESSING = NOW() "
                    f"WHERE ID_HOST = {host_id};")
        
        self.cursor.execute(query)
        self.db_connection.commit()
        
        self._disconnect()
        
    # Method to set processing task as completed with success
    def processing_task_success(self,
                                host_id:int,
                                task_id:int) -> None:
        """Set processing task as completed with success

        Args:
            host_id (int): Host database primary key
            task_id (int): Task database prumary key
        """
        
        self._update_processing_status( host_id=host_id,
                                        pending_processing=-1,
                                        processing_error=0)
        
        # connect to the database
        self._connect()
        
        # compose and excecute query to delete the processing task from the BPDATA database
        query = (f"DELETE FROM PRC_TASK "
                 f"WHERE ID_PRC_TASK = {task_id};")
        self.cursor.execute(query)

        # update database statistics for the host
        query = (f"UPDATE HOST "
                    f"SET NU_PENDING_PROCESSING = NU_PENDING_PROCESSING - 1, "
                    f"DT_LAST_PROCESSING = NOW() "
                    f"WHERE ID_HOST = '{host_id}';")
        self.cursor.execute(query)
        self.db_connection.commit()
        
        self._disconnect()    

    # Method to set processing task as completed with error
    def processing_task_error(self,
                              task:dict) -> None:
        """Set processing task as completed with error

        Args:
            task (dict): Dictionary including the following keys:
                {"host_id": host_id,
                 "task_id": task_id,
                 "server path": server_path}
        """
                
        self._update_processing_status( host_id=task["host_id"],
                                        pending_processing=-1,
                                        processing_error=1)
        # connect to the database
        self._connect()
        
        # compose and excecute query to set BO_ERROR_FLAG to 1 and server path in the BPDATA database
        query = (f"UPDATE PRC_TASK "
                    f"SET BO_ERROR_FLAG = 1, "
                    f"NO_SERVER_FILE_PATH = '{task['server path']}' "
                    f"WHERE ID_PRC_TASK = {task['task_id']};")
        
        self.cursor.execute(query)
        self.db_connection.commit()
        
        self._disconnect()    
