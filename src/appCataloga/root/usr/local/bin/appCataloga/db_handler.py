#!/usr/bin/env python
""" This module manage all database operations for the appCataloga scripts """

# Import libraries for:
import mysql.connector
import os
import re

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

        # constants
        self.BACKUP = 1
        self.PROCESS = 2
        self.ERROR = -1
        self.NOTHING = 0
        self.PENDING = 1
        self.EXECUTION = 2

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
        except Exception as e:
            self._disconnect()
            raise Exception("Error retrieving location coordinates from database") from e


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
        except Exception as e:
            self._disconnect()
            raise Exception(f"Error retrieving site {self.data['Site_ID']} from database") from e   

        try:
            db_site_longitude = float(nearest_site[0])
            db_site_latitude = float(nearest_site[1])
            db_site_altitude = float(nearest_site[2])
            db_site_nu_gnss_measurements = int(nearest_site[3])
        except (ValueError, IndexError) as e:
            self._disconnect()
            raise Exception(f"Invalid data returned for site {self.data['Site_ID']} from database") from e

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
            except Exception as e:
                self._disconnect()
                raise Exception(f"Error updating site {self.data['Site_ID']} from database") from e

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
        except Exception as e:
            raise Exception(f"Error retrieving state name {data['state']}") from e

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
            except Exception as e:
                self._disconnect()
                raise Exception(f"Error retrieving county name {data['County']}") from e

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
        except (TypeError, ValueError):
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
        except Exception as e:
            raise Exception(f"Error creating new site using query: {query}") from e
            

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
        except Exception as e:
            self._disconnect()
            raise Exception(f"Error retrieving site information using query: {query}") from e

        try:
            site_path = self.cursor.fetchone()
            new_path = f"{site_path[0]}/{site_path[1]}/{site_path[2]}"
        except Exception as e:
            self._disconnect()
            raise Exception(f"Error building path from site information from query: {query}") from e

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
        except Exception as e:
            self._disconnect()
            raise Exception("Error retrieving file using query: {query}") from e

        try:
            file_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
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
            except Exception as e:
                self._disconnect()
                raise Exception(f"Error creating new file entry using query: {query}") from e
        
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
        except Exception as e:
            self._disconnect()
            raise Exception("Error retrieving procedure using query: {query}") from e

        try:
            procedure_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            query =(f"INSERT INTO DIM_SPECTRUM_PROCEDURE"
                    f" (NA_PROCEDURE) "
                    f"VALUES"
                    f" ('{procedure_name}')")

            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                procedure_id = int(self.cursor.lastrowid)
            except Exception as e:
                self._disconnect()
                raise Exception(f"Error creating new procedure entry using query: {query}") from e
        
        self._disconnect()
        
        return procedure_id

    def _get_equipment_types(self) -> dict:
        """Load all equipment types from the database and create a dictionary with equipmenty_type_uid as key and equipment_type_id as value

        Returns:
            dict: {equipment_type_uid:equipment_type_id}
        """
        
        self._connect()
        
        query = ("SELECT"
                 " ID_EQUIPMENT_TYPE,"
                 " NA_EQUIPMENT_TYPE_UID "
                 "FROM DIM_EQUIPMENT_TYPE;")
        
        try:
            self.cursor.execute(query)
            
            equipment_types = self.cursor.fetchall()
        except Exception as e:
            self._disconnect()
            raise Exception("Error retrieving equipment types from database") from e
        
        self._disconnect()
        
        equipment_types_dict = {}
        try:
            for equipment_type in equipment_types:
                equipment_type_id = int(equipment_type[0])
                equipment_type_uid = str(equipment_type[1])
                equipment_types_dict[equipment_type_uid] = equipment_type_id
        except Exception as e:
            raise Exception("Error parsing equipment types retrieved from database") from e
        
        return equipment_types_dict

    def insert_equipment(self, equipment:Union[str, List[str]]) -> dict:
        """Create a new equipment entry in the database if it does not exist, otherwise return the existing key

        Args:
            equipment (str/[str]): String of list of strings containing the equipment name(s)

        Raises:
            Exception: Invalid input. Expected a string or a list of strings.
            Exception: Error retrieving equipment type for _equipment_name_ from database
            Exception: Error retrieving equipment data for _equipment_name_ from database
            Exception: Error creating new equipment entry for _equipment_name_ in database

        Returns:
            dict: {equipment_name:equipment_id}
        """
        
        if isinstance(equipment, str):
            equipment_names = [equipment]
        elif isinstance(equipment, list):
            equipment_names = equipment
        else:
            raise Exception("Invalid input. Expected a string or a list of strings.")
        
        equipment_types = self._get_equipment_types()
        
        equipment_ids = {}
        
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
            except Exception as e:
                self._disconnect()
                raise Exception(f"Error retrieving equipment data using query: {query}") from e

            try:
                equipment_id = int(self.cursor.fetchone()[0])
            except (TypeError, ValueError):
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
                except Exception as e:
                    self._disconnect()
                    raise Exception(f"Error creating new equipment using query: {query}") from e
            
            equipment_ids[name]=equipment_id
            
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
        except Exception as e:
            self._disconnect()
            raise Exception("Error retrieving detector type using query: {query}") from e

        try:
            detector_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            query =(f"INSERT INTO DIM_SPECTRUM_DETECTOR"
                    f" (NA_DETECTOR) "
                    f"VALUES"
                    f" ('{detector}')")

            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                detector_id = int(self.cursor.lastrowid)
            except Exception as e:
                self._disconnect()
                raise Exception(f"Error creating new detector entry using query: {query}") from e
        
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
        except Exception as e:
            self._disconnect()
            raise Exception(f"Error retrieving trace type using query: {query}") from e

        try:
            trace_type_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            query = (f"INSERT INTO DIM_SPECTRUM_TRACE_TYPE"
                     f" (NA_TRACE_TYPE) "
                     f"VALUES"
                     f" ('{trace_name}')")

            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                trace_type_id = int(self.cursor.lastrowid)
            except Exception as e:
                self._disconnect()
                raise Exception(f"Error creating new trace time entry using query: {query}") from e
        
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
        except Exception as e:
            self._disconnect()
            raise Exception(f"Error retrieving measure unit using query: {query}") from e

        try:
            measure_unit_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            query = (f"INSERT INTO DIM_SPECTRUM_UNIT"
                     f" (NA_MEASURE_UNIT) "
                     f"VALUES"
                     f" ('{unit_name}')")

            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                measure_unit_id = int(self.cursor.lastrowid)
            except Exception as e:
                self._disconnect()
                raise Exception(f"Error creating new measure unit entry using query: {query}") from e
        
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
        except Exception as e:
            self._disconnect()
            raise Exception(f"Error retrieving spectrum using query: {query}") from e
        
        try:
            spectrum_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
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
            except Exception as e:
                self._disconnect()
                raise Exception(f"Error creating new spectrum entry using query: {query}") from e

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
                except Exception as e:
                    self._disconnect()
                    raise Exception(f"Error creating new spectrum equipment entry using query: {query}") from e
        
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
                except Exception as e:
                    self._disconnect()
                    raise Exception(f"Error creating new spectrum file entry using query: {query}") from e
                
        self.db_connection.commit()
        
        self._disconnect()
        
    # Internal method to add host to the database
    def _add_host(self, hostid:str, host_uid:str) -> None:
        """This method adds a new host to the database if it does not exist.
            If host already in the database, do nothing.
            When creatining new host, initialize host statistics to zero.
        
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
                    f"NU_PENDING_HOST_TASK, NU_HOST_CHECK_ERROR, "
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
    def get_host_status(self,hostid="host_id"):        
        # TODO #34 Improve task reporting by separating backup tasks from individual backup transactions
        # connect to the database
        self._connect()

        # compose query to get host data from the BKPDATA database
        query = (f"SELECT "
                    f"ID_HOST, "
                    f"NU_HOST_FILES, "
                    f"NU_PENDING_HOST_TASK, "
                    f"DT_LAST_HOST_CHECK, "
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
                raise Exception(f"Error retrieving 'Total Files' for host {hostid} from database") from e
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
    
    def get_host (self, host_id:int) -> dict:
        """This method gets host access data from the database

        Args:
            hostid (int): PK for host in the database.

        Raises:
            Exception: _description_
            Exception: _description_

        Returns:
            dict:   "host_uid": (str) Host UID,
                    "host_add": (str) Host IP address or DNS recognized name,
                    "port": (int) Host SSH port,
                    "user": (str) Host access user,
                    "password": (str) Host access password
        """

        # compose query to get host access data from the BPDATA database
        query = (f"SELECT "
                    f"NA_HOST_UID, "
                    f"NA_HOST_ADDRESS, "
                    f"NA_HOST_PORT, "
                    f"NA_HOST_USER, "
                    f"NA_HOST_PASSWORD "
                f"FROM HOST "
                f"WHERE ID_HOST = '{host_id}';")
        
        # Get the data
        self._connect()
        
        self.cursor.execute(query)
        
        db_output = self.cursor.fetchone()
                    
        self._disconnect()
        
        # get the output in a dictionary format
        try:
            output = {  "host_uid": str(db_output[0]),
                        "host_add": str(db_output[1]),
                        "port": int(db_output[2]),
                        "user": str(db_output[3]),
                        "password": str(db_output[4])}
        except (TypeError, ValueError):
            output = False
        
        return output
    
    # Method add a new host to the backup queue
    def add_host_task(self,
                        task_type:int,
                        host_id:str,
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
        self._add_host(host_id,host_uid)

        # connect to the database
        self._connect()
        
        # compose query to add 1 to PENDING_BACKUP in the HOST table in BPDATA database for the given host_id
        query = (f"UPDATE HOST SET "
                    f"NA_HOST_ADDRESS = '{host_addr}', "
                    f"NA_HOST_PORT = '{host_port}', "
                    f"NA_HOST_USER = '{host_user}', "
                    f"NA_HOST_PASSWORD = '{host_passwd}', "
                    f"NU_PENDING_HOST_TASK = NU_PENDING_HOST_TASK + 1 "
                f"WHERE ID_HOST = '{host_id}';")
        
        # update database
        self.cursor.execute(query)
        self.db_connection.commit()
        
        # compose query to set the backup task in the BPDATA database
        query = (f"INSERT INTO HOST_TASK "
                 f"(FK_HOST, TASK_TYPE, DT_HOST_TASK) "
                 f"VALUES "
                 f"('{host_id}', '{task_type}', NOW();")

        # update database
        self.cursor.execute(query)
        self.db_connection.commit()
        self._disconnect()
        
    # get next host in the list for data backup
    def next_host_task(self,
                         task_id:int=None) -> dict:
        """This method gets the next host in the list for data backup
        
        Args:
            task_id (int): Optional. If set, get the specific task with the given ID

        Returns:
            dict: Dictionary with the pending task information: task_id, host_id, host, port, user, password
        """        
        
        # connect to the database
        self._connect()

        if not task_id:
            # build query to get the next backup task
            query = (   "SELECT ID_HOST_TASK, FK_HOST, NA_HOST_ADDRESS, NA_HOST_PORT, NA_HOST_USER, NA_HOST_PASSWORD "
                        "FROM HOST_TASK "
                        "ORDER BY DT_HOST_TASK "
                        "LIMIT 1;")
        else:
            query = (   "SELECT ID_HOST_TASK, FK_HOST, NA_HOST_ADDRESS, NA_HOST_PORT, NA_HOST_USER, NA_HOST_PASSWORD "
                        "FROM HOST_TASK "
                        f"WHERE ID_HOST_TASK = {task_id};")
        
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
        except (TypeError, ValueError):
            output = False
        
        return output

    def update_host_status( self,
                            host_id:int,
                            equipment_id:int = None,
                            reset:bool = False,
                            host_files:int = None,
                            pending_host_check:int = None,
                            host_check_error:int = None,
                            pending_backup:int = None,
                            backup_error:int = None,
                            pending_processing:int = None,
                            processing_error:int = None) -> None:
        """This method set/update summary information in the database

        Args:
            host_id (int): Zabbix host id primary key.
            equipment_id (int): Equipment id primary key. Default is None.
            reset (bool): If set to True, reset all values to the given values. Default is False.
            
            All other parameters are optional and default to None. 
            - host_files (int): Number of files in the host. 
            - pending_host_check (int): Number of files pending backup.
            - host_check_error (int): Number of files with backup error.
            - pending_backup (int): Number of files pending backup.
            - backup_error (int): Number of files with backup error.
            - pending_processing (int): Number of files pending processing.
            - processing_error (int): Number of files with processing error.
        """
        
        # connect to the database
        self._connect()
        
        # compose and excecute query to update the processing status by adding pending_processing variable to existing value in the database
        query_parts = []
        
        update_data = { "NU_HOST_FILES": host_files,
                        "NU_PENDING_HOST_TASK": pending_host_check,
                        "NU_HOST_CHECK_ERROR": host_check_error,
                        "NU_PENDING_BACKUP": pending_backup,
                        "NU_BACKUP_ERROR": backup_error,
                        "NU_PENDING_PROCESSING": pending_processing,
                        "NU_PROCESSING_ERROR": processing_error}
        
        for column, value in update_data.items():
            if value > 0:
                if reset:
                    query_parts.append(f"{column} = {value}")
                else:
                    query_parts.append(f"{column} = {column} + {value}")
            elif value < 0:
                if reset:
                    query_parts.append(f"{column} = {value}")
                else:
                    query_parts.append(f"{column} = {column} - {-value}")
            elif value == 0:
                if reset:
                    query_parts.append(f"{column} = 0")
                else:
                    continue
            else:
                continue

        query = "UPDATE HOST SET " + ", ".join(map(str, query_parts))
        
        if (pending_host_check is not None) or (host_check_error is not None):
            query = query + ", DT_LAST_HOST_CHECK = NOW()"
        
        if (pending_backup is not None) or (backup_error is not None):
            query = query + ", DT_LAST_BACKUP = NOW()"
        
        if (pending_processing is not None) or (processing_error is not None):
            query = query + ", DT_LAST_PROCESSING = NOW()"

        if equipment_id:
            query = query + f", FK_EQUIPMENT_RFDB = {equipment_id} WHERE ID_HOST = {host_id};"
        else:
            query = query + f" WHERE ID_HOST = {host_id};"

        self.cursor.execute(query)
        self.db_connection.commit()
        
        self._disconnect()

    # Method to remove a completed backup task from the database
    def remove_host_task(self,
                         task_id:int) -> None:
        """

        Args:
            task (dict): _description_
        """
                      
        # connect to the database
        self._connect()
        
        # compose query to get the host_id from the BKPDATA database
        query = (f"SELECT FK_HOST "
                    f"FROM HOST_TASK "
                    f"WHERE ID_HOST_TASK = {task_id};")
        
        self.cursor.execute(query)
        
        try:
            host_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            self._disconnect()
            raise Exception(f"Error retrieving host_id for task_id {task_id} from database")
        
        # compose and excecute query to delete the backup task from the BKPDATA database
        query = (f"DELETE FROM HOST_TASK "
                 f"WHERE ID_HOST_TASK = {task_id};")
        
        self.cursor.execute(query)

        # update database statistics for the host
        query = (f"UPDATE HOST "
                    f"SET NU_PENDING_HOST_TASK = NU_PENDING_HOST_TASK - 1, "
                    f"DT_LAST_HOST_CHECK = NOW() "
                 f"WHERE ID_HOST = '{host_id}';")
        self.cursor.execute(query)
        self.db_connection.commit()
        
        self._disconnect()

    # Method to get next host in the list for processing
    def next_file_task(self, type:int) -> dict:
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

        # build query to get the next backup task with host_uid and NU_STATUS different = 0 (not processed)
        query = (   f"SELECT "
                        f"FILE_TASK.ID_FILE_TASK, "
                        "FILE_TASK.FK_HOST, HOST.NA_HOST_UID, "
                        "FILE_TASK.NA_HOST_FILE_PATH, FILE_TASK.NA_HOST_FILE_NAME, "
                        "FILE_TASK.NA_SERVER_FILE_PATH, FILE_TASK.NA_SERVER_FILE_NAME "
                    f"FROM FILE_TASK "
                        f"JOIN HOST ON FILE_TASK.FK_HOST = HOST.ID_HOST "
                    f"WHERE "
                        f"FILE_TASK.NU_STATUS = 2 AND "
                        f"FILE_TASK.NU_TASK_TYPE = {type} "
                    f"ORDER BY FILE_TASK.DT_FILE_TASK "
                    f"LIMIT 1;")
        
        self.cursor.execute(query)
        
        task = self.cursor.fetchone()
        
        try:
            output = {  "task_id": int(task[0]),
                        "host_id": int(task[1]),
                        "host_uid": str(task[2]),
                        "host path": str(task[3]),
                        "host file": str(task[4]),
                        "server path": str(task[5]),
                        "server file": str(task[6])}
        except (TypeError, ValueError):
            output = False
        
        self._disconnect()
        
        return output

    # Method to retrieve multiple file tasks
    def next_files_for_host(self, type:int, limit:int=None) -> dict:
        """This method gets the next host in the list for data processing

        Args:
            type (int): Task type
            limit (int): Number of tasks to retrieve. Default to None.
            
        Returns:
            dict:   "host_id": (int) host_id,
                    "task_ids": (list)(int) task_ids,
                    "host_files": (list)(str) host file names,
                    "server_files": (list)(str) server file names
        """
        
        # connect to the database
        self._connect()

        # compose a query to retrieve the FK_HOST where:
        # - oldest file task
        # - NU_STATUS = 1 (pending action)
        # - NU_TASK_TYPE = type
        query = (   f"SELECT "
                        f"FILE_TASK.FK_HOST "
                    f"FROM FILE_TASK "
                    f"WHERE "
                        f"FILE_TASK.NU_STATUS = 1 AND "
                        f"FILE_TASK.NU_TASK_TYPE = {type} "
                    f"ORDER BY FILE_TASK.DT_FILE_TASK "
                    f"LIMIT 1;")
        
        self.cursor.execute(query)
        
        task = self.cursor.fetchone()
        
        try:
            host_id = int(task[1])
        except (TypeError, ValueError):
            host_id = False
        
        # if a task is found
        if host_id:
            # build a query to change NU_STATUS to 2 (under execution) of all tasks where:
            # - FK_HOST has the same host_id as the oldest,
            # - task is of the same type and
            # - NU_STATUS = 1 (pending action)
            query = (   f"UPDATE FILE_TASK SET "
                            f"NU_STATUS = 2 "
                        f"WHERE "
                            f"FK_HOST = {host_id} AND "
                            f"NU_STATUS = 1 AND "
                            f"NU_TASK_TYPE = {type};")
            
            self.cursor.execute(query)
            self.db_connection.commit()
        
            # build a query to retrieve list of files associated with the oldest task
            query = (   f"SELECT "
                            f"FILE_TASK.ID_FILE_TASK, "
                            f"FILE_TASK.NA_HOST_FILE_PATH, FILE_TASK.NA_HOST_FILE_NAME, "
                            f"FILE_TASK.NA_SERVER_FILE_PATH, FILE_TASK.NA_SERVER_FILE_NAME "
                        f"FROM FILE_TASK "
                        f"WHERE "
                            f"FILE_TASK.NU_STATUS = 2 AND "
                            f"FILE_TASK.NU_TASK_TYPE = {type} AND "
                            f"FILE_TASK.FK_HOST = {host_id} "
                        f"ORDER BY FILE_TASK.DT_FILE_TASK")
        
        self._disconnect()
        
        if limit:
            query = query + f" LIMIT {limit};"
        else:
            query = query + ";"
        
        self.cursor.execute(query)
        
        tasks = self.cursor.fetchall()
        
        output = {  "host_id": host_id,
                    "task_ids": [],
                    "host_files": [],
                    "server_files": []}
        for task in tasks:
            try:
                output["task_ids"].append(int(task[0]))
            except (TypeError, ValueError):
                continue

            try:
                output["host_files"].append(f"{str(task[1])}/{str(task[2])}")
            except (TypeError, ValueError):
                pass
                
            try:
                output["server_files"].append(f"{str(task[3])}/{str(task[4])}")
            except (TypeError, ValueError):
                pass
        
        # drop task_ids, host_files and server_files if empty
        if not output["task_ids"]:
            output.pop("task_ids")
            
        if not output["host_files"]:
            output.pop("host_files")
            
        if not output["server_files"]:
            output.pop("server_files")
        
        return output

    # Method to add a new processing task to the database
    def add_file_task(  self,
                        host_id:int,
                        task_type:int,
                        volume:str,
                        files:any,
                        reset_processing_queue:bool = False) -> None:
        """This method adds tasks to the processing queue
        
        Args:
            host_id (int): Zabbix host id primary key.
            files (list or set): List strings corresponding to path and file names or set of tuples (filename, path)
            reset_processing_queue (bool): Flag to reset the processing queue. 

        Returns:
            None
        """
                
        # convert list of filenames with path into tuples (hostid, task_type, path, filename)
        if isinstance(files, list):
            files_tuple_list = [(os.path.dirname(item), os.path.basename(item)) for item in files]
        elif isinstance(files, set):
            files_tuple_list = [(filepath, filename) for (filename,filepath) in files]
        else:
            raise Exception("Invalid input. Expected a list strings or a set of tuples.")

        if volume==k.REPO_UID:
            target_column = "NA_SERVER_FILE_PATH, NA_SERVER_FILE_NAME, "
        else:
            target_column = "NA_HOST_FILE_PATH, NA_HOST_FILE_NAME, "

        # compose query to set the process task in the database using executemany method
        query =(f"INSERT INTO FILE_TASK ("
                f"FK_HOST, "
                f"NU_TASK_TYPE, "
                f"{target_column}"
                f"DT_FILE_TASK, "
                f"NU_STATUS)"
                f"VALUES ("
                f"{host_id}, "
                f"{task_type}, "
                f"%s, %s, "
                f"NOW(),"
                f"{task_type});")
        
        # update database
        self._connect()

        self.cursor.executemany(query, files_tuple_list)
        self.db_connection.commit()
        
        if reset_processing_queue:
            # compose query to find how many database entries are in the processing queue with status = -1 for the given host_id
            query = (f"SELECT COUNT(*) "
                        f"FROM FILE_TASK "
                        f"WHERE FK_HOST = {host_id} AND "
                        f"NU_STATUS = -2;")
            
            self.cursor.execute(query)
            
            # get the number of processing errors
            try:
                processing_error = int(self.cursor.fetchone()[0])
            except (TypeError, ValueError):
                processing_error = 0
        else:
            processing_error = 0
            
        self._disconnect()
        
        # update PENDING_PROCESSING in the HOST table in BPDATA database for the given host_id
        nu_processing = len(files_tuple_list) if files_tuple_list else 0
        self.update_host_status(    host_id=host_id,
                                    pending_processing=nu_processing,
                                    processing_error=processing_error,
                                    reset=reset_processing_queue)        
                
    # Method to set processing task as completed with error
    def file_task_error(self,
                        task_id:int,
                        message:str) -> None:
        """Set processing task as completed with error

        Args:
            task (dict): Dictionary including the following keys:
                {"host_id": host_id,
                 "task_id": task_id,
                 "message": message}
        """
                
        # connect to the database
        self._connect()
        
        message = message.replace("'","''")
        # compose and excecute query to set NU_STATUS to -1 (Error) and server path in the BPDATA database
        query = (f"UPDATE FILE_TASK "
                    f"SET NU_STATUS = -1, "
                    f"NA_MESSAGE = '{message}' "
                    f"WHERE ID_FILE_TASK = {task_id};")
        
        self.cursor.execute(query)
        self.db_connection.commit()
        
        self._disconnect()

    def file_task_update(self,
                        task_id:int,
                        host_path:str = None,
                        host_file:str = None,
                        server_path:str = None,
                        server_file:str = None,
                        task_type:int = None,
                        status:int = None,
                        message:str = None ) -> None:
        """Set processing task as completed with error

        Args:
            task_id (int): Task id
            host_path (str): Host path
            host_file (str): Host file
            server_path (str): Server path
            server_file (str): Server file
            task_type (int): Task type: 0=Not set; 1=Backup; 2=Processing
            status (int): Status flag: -1=Error, 0=Nothing to do, 1=Pending action, 2=Under execution
            message (str): Error message
        """
        # compose and excecute query to set NU_STATUS to -1 (Error) and server path in the BPDATA database
        query = "UPDATE FILE_TASK SET "

        if host_path:
            query = query + f"NA_HOST_FILE_PATH = '{host_path}', "
        if host_file:
            query = query + f"NA_HOST_FILE_NAME = '{host_file}', "
        if server_path:
            query = query + f"NA_SERVER_FILE_PATH = '{server_path}', "
        if server_file:
            query = query + f"NA_SERVER_FILE_NAME = '{server_file}', "
        if task_type:
            query = query + f"NU_TASK_TYPE = {type}, "
        if status:
            query = query + f"NU_STATUS = {status}, "
        if message:
            message = message.replace("'","''")
            query = query + f"NA_MESSAGE = '{message}', "
        
        query = query[:-2] + f" WHERE ID_FILE_TASK = {task_id};"
        
        # connect to the database
        self._connect()
        
        self.cursor.execute(query)
        self.db_connection.commit()
        
        self._disconnect()


    # Method to set processing task as completed with success
    def delete_file_task(self,
                            task_id:int,
                            ) -> None:
        """Set processing task as completed with success

        Args:
            task (dict): Dictionary including the following
            equipment_ids (dict): Dictionary with the equipment ids associated with the processed files
        """
        
        # get host_id from the database for the given task_id
        self._connect()
        
        query = (f"SELECT FK_HOST "
                    f"FROM FILE_TASK "
                    f"WHERE ID_FILE_TASK = {task_id};")
        
        self.cursor.execute(query)
        
        try:
            host_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            self.log.error(f"Error retrieving host_id for task_id {task_id} from database")
            self._disconnect()
            raise Exception(f"Error retrieving host_id for task_id {task_id} from database")
        
        self.update_host_status(    host_id=host_id,
                                    pending_processing=-1)
                
        # compose and excecute query to delete the processing task from the BPDATA database
        query = (f"DELETE FROM FILE_TASK "
                 f"WHERE ID_FILE_TASK = {task_id};")
        
        self.cursor.execute(query)

        self.db_connection.commit()
        
        self._disconnect()

    def list_rfdb_files(self) -> set:
        """List files in DIM_SPECTRUM_FILE table that are associated with k.REPO_UID

        Args:
            None

        Returns:
            set: Set of tuples with file name and path of files RFDATA database
        """
        
        # Query to get files from DIM_SPECTRUM_FILE
        query =(f"SELECT "
                    f"NA_FILE, "
                    f"NA_PATH "
                f"FROM "
                    f"DIM_SPECTRUM_FILE "
                f"WHERE "
                    f"NA_VOLUME = '{k.REPO_UID}'")
        
        # connect to the database
        self._connect()
        
        self.cursor.execute(query)
        
        db_files = set((row[0], row[1]) for row in self.cursor.fetchall())
        
        self._disconnect()
        
        return db_files

    def remove_rfdb_files(self, files_to_remove:set) -> None:
        """Remove files in DIM_SPECTRUM_FILE table that match files_not_in_repo set

        Args:
            files_not_in_repo (set): Set of tuples with file name and path of files not in the repository

        Returns:
            None
        """

        # connect to the database
        self._connect()
        
        user_input = input("Do you wish to confirm each entry before deletion? (y/n): ")
        if user_input.lower() == 'y':
            ask_berfore = True
        else:
            ask_berfore = False

        for filename, path in files_to_remove:
            try:
                if ask_berfore:
                    user_input = input(f"Delete {path}/{filename}? (y/n): ")
                    if user_input.lower() != 'y':
                        files_to_remove.pop((filename, path))
                        continue
                
                # Find ID_FILE
                query =(f"SELECT "
                            f"ID_FILE "
                        f"FROM "
                            f"DIM_SPECTRUM_FILE "
                        f"WHERE "
                            f"NA_FILE = '{filename}' AND "
                            f"NA_PATH = '{path}' AND "
                            f"NA_VOLUME = '{k.REPO_UID}'")

                self.cursor.execute(query)

                id_file = int(self.cursor.fetchone()[0])

                # Exclua a linha correspondente na tabela BRIDGE_SPECTRUM_FILE
                query =(f"DELETE FROM "
                            f"BRIDGE_SPECTRUM_FILE "
                        f"WHERE "
                            f"FK_FILE = {id_file}")
                
                self.cursor.execute(query)

                # Exclua a linha correspondente na tabela DIM_SPECTRUM_FILE
                query =("DELETE FROM "
                            f"DIM_SPECTRUM_FILE "
                        f"WHERE "
                            f"ID_FILE = {id_file}")
                
                self.cursor.execute(query)
                
                self.db_connection.commit()
                
                self.log.entry(f"Removed {path}/{filename} from database")
            except Exception as e:
                self.log.error(f"Error removing {path}/{filename} from database: {e}")
                pass
        
        self._disconnect()
        
        return None

    def list_bpdb_files(self, status:int) -> set:
        """List files in FILE_TASK table that match a given status

        Args:
            status (int): Status flag: 0=Not executed; -1=Executed with error

        Returns:
            set: Set of tuples with file name and path of files in the database
        """
        
        # Query to get files from DIM_SPECTRUM_FILE
        query =(f"SELECT "
                    f"NA_SERVER_FILE_NAME, "
                    f"NA_SERVER_FILE_PATH "
                f"FROM "
                    f"FILE_TASK "
                f"WHERE "
                    f"NU_STATUS = {status}")
        
        # connect to the database
        self._connect()
        
        self.cursor.execute(query)
        
        db_files = set((row[0], row[1]) for row in self.cursor.fetchall())
        
        self._disconnect()
        
        return db_files

    def remove_bpdb_files(self, files_to_remove:set) -> None:
        """Remove files in FILE_TASK table from files_to_remove set

        Args:
            files_to_remove (set): Set of tuples with file name and path of files not in the repository, to be removed from the database

        Returns:
            None
        """

        # connect to the database
        self._connect()
        
        user_input = input("Do you wish to confirm each entry before deletion? (y/n): ")
        if user_input.lower() == 'y':
            ask_berfore = True
        else:
            ask_berfore = False

        for filename, path in files_to_remove:
            try:
                if ask_berfore:
                    user_input = input(f"Delete {path}/{filename}? (y/n): ")
                    if user_input.lower() != 'y':
                        files_to_remove.pop((filename, path))
                        continue
                    
                query =(f"DELETE FROM "
                            f"FILE_TASK "
                        f"WHERE "
                            f"NA_SERVER_FILE_NAME = '{filename}' AND "
                            f"NA_SERVER_FILE_PATH = '{path}'")
                
                self.cursor.execute(query)
            
                self.db_connection.commit()
                
                self.log.entry(f"Removed {path}/{filename} from database")
            except Exception as e:
                self.log.error(f"Error removing {path}/{filename} from database: {e}")
                pass
        
        self._disconnect()
        
        return None

    def add_task_from_file(self, file_set:set) -> None:
        """Create a new task entry based only in a set of file names and paths

        Args:
            file_set (set): Set of files to be processed
            db_bp (dbh.dbHandler): Database handler object
            
        returns:
            set: Set of tuples with host_id and dictionary with file name and path
        """
        
        # Regular expression pattern to match "host_uid"
        # TODO: #10: Improve host_uid extraction from filename using list of known host_uids and regex
        # TODO: #25 Add host_uid extraction from file content for missing host_uids
        pattern = re.compile(r"[rR][fF][eE]ye002\d{3}")

        # split the set into subsets based on the Host UID. Try to get host_uid with REGEX and ask user if not found
        subsets = {}
        for filename, path in file_set:
            match = pattern.search(filename)
            if not match:
                host_uid = input(f"Host UID not found in '{filename}'. Please type host UID or press enter to skip: ") # TODO Include function to delete files in the subset with empty key
            else:
                host_uid = match.group(0)
                
            try:
                if host_uid not in subsets:
                    subsets[host_uid] = set()
                
                subsets[host_uid].add((filename, path))
            except Exception as e:
                self.log.entry(f"Ignoring '{path}/{filename}'. No host_uid defined. Error: {e}")
                pass

        # drop empty subset
        try:
            subsets.pop("")
        except KeyError:
            pass
        
        # Loop through the subsets
        # Get database host id from host_uid and ask user if not found
        # Add host to the database if it does not exist but a valid host_id was informed
        # Add processing task to the database
        for host_uid, file_set in subsets.items():          
            
            query =(f"SELECT ID_HOST "
                    f"FROM HOST "
                    f"WHERE NA_HOST_UID = '{host_uid}';")
            
            self._connect()
            self.cursor.execute(query)
            
            try:
                host_id = int(self.cursor.fetchone()[0])
            except TypeError:
                host_id = input(f"Host '{host_uid}' not found in database. Please enter host ID (Zabbix HOST_ID number) or press enter to skip this host: ")
                    
                try:
                    host_id = int(host_id)
                except ValueError:
                    self.log.entry(f"Host '{host_uid}' not found in database and no valid HOST ID was informed. Skipping host.")
                    continue

                self._add_host(host_id, host_uid)
                
                self.log.entry(f"Host '{host_uid}' created in the database with ID {host_id}")

            # TODO: #26 Harmonize file_list format with add_file_task method
            
            self.add_file_task(host_id=host_id,
                                     files_set=file_set,
                                     reset_processing_queue=True)
            
            self.log.entry(f"Added {len(file_set)} files from host {host_uid} to the processing queue")
        
        self._disconnect()
    
    def list_all_host_ids(self) -> list:
        """List all host ids in the database

        Args:
            None

        Returns:
            list: List of host ids in the database
        """
        
        # connect to the database
        self._connect()
        
        # build query to get ID_HOST and FK_EQUIPMENT_RFDB from HOST table
        query = ("SELECT ID_HOST, FK_EQUIPMENT_RFDB, NA_HOST_UID "
                    "FROM HOST;")
        
        self.cursor.execute(query)
        
        host_ids = [(int(row[0]), int(row[1]), row[2]) for row in self.cursor.fetchall()]
        
        self._disconnect()
        
        return host_ids
    
    def count_rfm_host_files(self,
                         equipment_id:int,
                         volume:str = None) -> int:
    
        # TODO #27 Fix equipment id reset in RFM database
        # connect to the database
        self._connect()
        
        # build query to get the number of files in the database, given an equipment_id and a volume. Make use of BRIDGE_SPECTRUM_EQUIPMENT to get the FK_SPECTRUM keys and from there, count the DIN_SPECTRUM_FILE entries with the given volume and FK_SPECTRUM using the BRIDGE_SPECTRUM_FILE table.
        query = (f"SELECT COUNT(DISTINCT DIM_SPECTRUM_FILE.NA_FILE) "
                    f"FROM BRIDGE_SPECTRUM_FILE "
                    f"JOIN BRIDGE_SPECTRUM_EQUIPMENT "
                        f"ON BRIDGE_SPECTRUM_FILE.FK_SPECTRUM = BRIDGE_SPECTRUM_EQUIPMENT.FK_SPECTRUM "
                    f"JOIN DIM_SPECTRUM_FILE "
                        f"ON BRIDGE_SPECTRUM_FILE.FK_FILE = DIM_SPECTRUM_FILE.ID_FILE "
                f"WHERE "
                    f"BRIDGE_SPECTRUM_EQUIPMENT.FK_EQUIPMENT = {equipment_id}")
            
        if volume:
            query = query + f" AND DIM_SPECTRUM_FILE.NA_VOLUME = '{volume}';"
        else:
            query = query + ";"
            
        self.cursor.execute(query)
        
        try:
            volume_count = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            volume_count = 0
        
        self._disconnect()
        
        return volume_count
    
    def count_bp_host_files(self,
                            host_id:int) -> int:
    
        # connect to the database
        self._connect()
        
        # build query to get the number of files in the database for the given host_id, both to any value of NA_VOLUME and for an specific value defined in volume
        query =(f"SELECT COUNT(*) "
                    f"FROM FILE_TASK "
                f"WHERE "
                    f"FK_HOST = {host_id} AND "
                    f"NU_STATUS = 0;")

        self.cursor.execute(query)
        
        try:
            pending_processing = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            pending_processing = 0

        query =(f"SELECT COUNT(*) "
                    f"FROM FILE_TASK "
                f"WHERE "
                    f"FK_HOST = {host_id} AND "
                    f"NU_STATUS = -2;")
        
        self.cursor.execute(query)
        
        try:
            error_processing = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            error_processing = 0

        self._disconnect()
        
        return (pending_processing, error_processing)
    
    def update_host_status(  self,
                            host_id:int,
                            total_files:int,
                            pending_processing:int,
                            error_processing:int) -> None:
        
        # connect to the database
        self._connect()
        
        # build query to update the number of files in the database for the given host_id
        query =(f"UPDATE HOST SET "
                    f"NU_HOST_FILES = {total_files}, "
                    f"NU_PENDING_PROCESSING = {pending_processing}, "
                    f"NU_PROCESSING_ERROR = {error_processing} "
                f"WHERE "
                    f"ID_HOST = {host_id};")
        
        self.cursor.execute(query)
        
        self.db_connection.commit()
        
        self._disconnect()
