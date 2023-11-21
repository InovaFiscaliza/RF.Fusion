#!/usr/bin/env python
""" This module manage all database operations for the appCataloga scripts """

# Import libraries for:
import mysql.connector
import sys
import os
import time

import numpy as np

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
        
        self.cursor = self.db_connection.cursor()

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
                    data=None):
        """Get site information from the database

        Args:
            data (dict, optional): Site information with required coordinates. Defaults to self.data.

        Raises:
            Exception: Error retrieving location coordinates from database
            ValueError: _description_

        Returns:
            If there is a matching site in the database, returns the site ID. If there is no matching site, returns False.
        """

        # if data is not provided, use the data from the object
        if data is None:
            data = self.data
        
        self._connect()
            
        # get data from the point in the database that is nearest to the measurement location indicated in the file
        query = (f"SELECT"
                f" [ID_SITE],"
                f" ST_X(GEO_POINT) as LONGITUDE,"
                f" ST_Y(GEO_POINT) as LATITUDE "
                f"FROM DIM_SPECTRUN_SITE"
                f" ORDER BY ST_Distance_Sphere(GEO_POINT, ST_GeomFromText('POINT({data['Longitude']} {data['Latitude']})', 4326))"
                f" LIMIT 1;")
        
        try:
            # Try to get the nearest match
            self.cursor.execute(query)

            nearest_site = self.cursor.fetchone()
            
            nearest_site_id = int(nearest_site[0])
            nearest_site_longitude = float(nearest_site[1])
            nearest_site_latitude = float(nearest_site[2])

            #Check if the database point is within the the expected deviation of the measured location coordinates 
            near_in_longitude = ( abs(data['Longitude']-nearest_site_longitude) < k.MAXIMUM_GNSS_DEVIATION )
            near_in_latitude = ( abs(data['Latitude']-nearest_site_latitude) < k.MAXIMUM_GNSS_DEVIATION )
            location_exist_in_db = ( near_in_latitude and near_in_longitude )
        except:
            # Confirm If number of rows returned is zero, error is due to the fact that there is no entry in the database
            if self.cursor.rowcount == 0:
                # set flag to create new location
                location_exist_in_db = False
            else:
                # some other error occurred
                raise Exception("Error retrieving location coordinates from database")

        self._disconnect()

        if location_exist_in_db:
            return nearest_site_id
        else:
            return False
        
    def update_site(self,   
                    site = int,
                    longitude_raw = [float],
                    latitude_raw = [float],
                    altitude_raw = [float]) -> None:
        """Update site coordinates in the database for existing site

        Args:
            site (int): The site database id.
            longitude_raw ([float]): List of measured longitude in degrees.
            latitude_raw ([float]): List of measured latitude in degrees.
            altitude_raw ([float]): List of measured altitude in meters.

        Returns:
            none: none
        """

        self._connect()
        
        # get data from the point in the database that is nearest to the measurement location indicated in the file
        query = (f"SELECT"
             f" ST_X(GEO_POINT) as LONGITUDE,"
             f" ST_Y(GEO_POINT) as LATITUDE,"
             f" [NU_ALTITUDE],"
             f" [NU_GNSS_MEASUREMENTS],"
             f"FROM DIM_SPECTRUN_SITE "
             f"WHERE"
             f" ID_SITE = {site};")

        try:
            # Try to get the nearest match
            self.cursor.execute(query)

            nearest_site = self.cursor.fetchone()
        except:
            raise Exception(f"Error retrieving site {self.data['Site_ID']} from database")            

        try:
            db_site_longitude = float(nearest_site[0])
            db_site_latitude = float(nearest_site[1])
            db_site_altitude = float(nearest_site[2])
            db_site_nu_gnss_measurements = int(nearest_site[3])
        except:
            raise Exception(f"Invalid data returned for site {self.data['Site_ID']} from database")

        # if number of measurements in the database greater than the maximum required number of measurements.
        if db_site_nu_gnss_measurements < k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS:

            #add point coordinates in the file to the estimator already in the database
            longitudeSum = np.sum(longitude_raw) + ( db_site_longitude * db_site_nu_gnss_measurements ) 
            latitudeSum = np.sum(latitude_raw) + ( db_site_latitude * db_site_nu_gnss_measurements )
            altitudeSum = np.sum(altitude_raw) + ( db_site_altitude * db_site_nu_gnss_measurements )
            nu_gnss_measurements = db_site_nu_gnss_measurements + len(longitude_raw)
            longitude = longitudeSum / nu_gnss_measurements
            latitude = latitudeSum / nu_gnss_measurements
            altitude = altitudeSum / nu_gnss_measurements

            # construct query update point location in the database
            query = (   f"UPDATE DIM_SPECTRUN_SITE "
                        f" SET GEO_POINT = ST_GeomFromText('POINT({longitude} {latitude})'),"
                        f" NU_ALTITUDE = {altitude},"
                        f" NU_GNSS_MEASUREMENTS = {nu_gnss_measurements} "
                        f"WHERE ID_SITE = {site};")
            
            if k.VERBOSE: print(f'Updated location at Latitude: {latitude}, Longitude: {longitude}, Altitude: {altitude}')

            try:
                self.cursor.execute(query)
                self.db_connection.commit()
            
                self._disconnect()
            except:
                raise Exception(f"Error updating site {self.data['Site_ID']} from database")

        else:
            # Do not update, avoiding unnecessary processing and variable numeric overflow
            if k.VERBOSE: print(f'Location at latitude: {latitude}, Longitude: {longitude} reached the maximum number of measurements. No update performed.')

        self._disconnect()


    def _get_geographic_codes(  self,  
                                data = {"state":"state name",
                                        "county":"city,town name",
                                        "district":"suburb name"}) -> (0,0,0):
        """Get DB keys for state, county and district based on the data in the object

        Args:
            data (dict): {"state":"state name", "county":"city,town name", "district":"suburb name"}

        Raises:
            ValueError: Fail to retrive state name from database
            ValueError: Fail to retrive county name from database
            ValueError: _description_
            Exception: _description_
            Exception: _description_
            Exception: _description_
            Exception: _description_
            Exception: _description_

        Returns:
            (int,int,int): Tuple with the DB keys for state, county and district
        """

        self._connect()

        # if data is not provided, use the data from the object
        if data is None:
            data = self.data
        
        # search database for existing state entry and get the existing key
        query = (   f"SELECT ID_STATE_CODE "
                    f"FROM DIM_SITE_STATE "
                    f"WHERE"
                    f" NA_STATE LIKE '{data['state']}';")

        self.cursor.execute(query)
        
        try:
            db_state_ide = int(self.cursor.fetchone()[0])
        except:
            raise ValueError(f"Error retrieving state name {data['state']}")

        # search database for existing county name entry within the identified State and get the existing key
        # Prepare multi word name to be processed in the full text search by replacing spaces with "AND"
        county = self.data['county'].replace(' ',' AND ')
        query = (   f"SELECT ID_COUNTY_CODE "
                    f"FROM DIM_SITE_COUNTY "
                    f"WHERE"
                    f" MATCH(NA_COUNTY) AGAINST('{county})')"
                    f" AND FK_STATE_CODE = {db_state_ide};")

        self.cursor.execute(query)
        
        try:
            db_county_id = int(self.cursor.fetchone()[0])
        except:
            raise ValueError(f"Error retrieving county name {data['County']}")

        #search database for the district name, inserting new value if non existant
        district = self.data['district'].replace(' ',' AND ')
        query = (f"SELECT ID_DISTRICT "
                f"FROM DIM_SITE_DISTRICT "
                f"WHERE"
                f" MATCH(NA_DISTRICT) AGAINST('{district}')"
                f" AND FK_COUNTY_CODE = {db_county_id};")
        
        self.cursor.execute(query)
        
        try:
            db_district_id = int(self.cursor.fetchone()[0])
        except:
            query = (f"INSERT INTO DIM_SITE_DISTRICT"
                    f" (FK_COUNTY_CODE,"
                    f" NA_DISTRICT) "
                    f"VALUES"
                    f" ({db_county_id},"
                    f" '{self.data['District']}');")
            
            self.cursor.execute(query)
            self.db_connection.commit()
            
            db_district_id = int(self.cursor.lastrowid)

        self._disconnect()

        return (db_state_ide, db_county_id, db_district_id)

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

        self._connect()

        (db_state_id, db_county_id, db_district_id) = self._get_geographic_codes(data=data)
        
        # construct query to create new sie in the database
        query = query = (   f"INSERT INTO DIM_SPECTRUN_SITE"
                            f" (GEO_POINT,"
                            f" ALTITUDE,"
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
            self.cursor.commit()
        
            db_site_id = int(self.cursor.lastrowid)

            self._disconnect()
        except:
            raise Exception(f"Error creating new site {data['Site_ID']} in database")
            

        return db_site_id

    # method to insert file entry in the database if it does not exist, otherwise return the existing key
    def insert_file(self, data={    "file_name":"file name",
                                    "file_path":"file path",
                                    "file_volume":"file volume"}) -> int:
        """Create a new file entry in the database if it does not exist, otherwise return the existing key

        Args:
            data (dict): {  "file_name":"file name UID",
                            "file_path":"file path UID",
                            "file_volume":"file volume UID"}

        Raises:
            Exception: Error inserting file in the database

        Returns:
            int: DB key to the new file
        """

        self._connect()
        
        query = (f"SELECT ID_FILE "
                f"FROM DIM_SPECTRUN_FILE "
                f"WHERE"
                f" NA_FILE = '{data['file_name']}' AND"
                f" NA_PATH = '{data['file_path']}' AND"
                f" NA_VOLUME = '{data['file_volume']}';")
        
        self.cursor.execute(query)

        try:
            file_id = int(self.cursor.fetchone()[0])
        except:            
            query =(f"INSERT INTO DIM_SPECTRUN_FILE"
                    f" (NA_FILE,"
                    f" NA_PATH,"
                    f" NA_VOLUME) "
                    f"VALUES"
                    f" ('{data['file_name']}',"	
                    f" '{data['file_path']}',"
                    f" '{data['file_volume']}')")

            try:
                self.cursor.execute(query)
                self.cursor.commit()
            
                file_id = int(self.cursor.lastrowid)
            except:
                self._disconnect()
                raise Exception(f"Error creating new file entry {data['file_name']} in database")
        
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
                f"FROM DIM_SPECTRUN_PROCEDURE "
                f"WHERE"
                f" NA_PROCEDURE = '{procedure_name}';")
        
        self.cursor.execute(query)

        try:
            procedure_id = int(self.cursor.fetchone()[0])
        except:            
            query =(f"INSERT INTO DIM_SPECTRUN_PROCEDURE"
                    f" (NA_PROCEDURE) "
                    f"VALUES"
                    f" ('{procedure_name}')")

            try:
                self.cursor.execute(query)
                self.cursor.commit()
            
                procedure_id = int(self.cursor.lastrowid)
            except:
                self._disconnect()
                raise Exception(f"Error creating new procedure entry {data['procedure_name']} in database")
        
        self._disconnect()
        
        return procedure_id

    def _get_equipment_types(self) -> dict:
        """Load all equipment types from the database and create a dictionary with equipmenty_type_uid as key and equipment_type_id as value

        Returns:
            dict: {equipment_type_uid:equipment_type_id}
        """
        
        self._connect()
        
        query = (f"SELECT ID_EQUIPMENT_TYPE, NA_EQUIPMENT_TYPE "
                f"FROM DIM_SPECTRUN_EQUIPMENT_TYPE;")
        
        try:
            self.cursor.execute(query)
            
            equipment_types = self.cursor.fetchall()
        except:
            raise Exception("Error retrieving equipment types from database")
        
        self._disconnect()
        
        equipment_types_dict = {}
        try:
            for equipment_type in equipment_types:
                equipment_type_uid = str(equipment_type[1])
                equipment_type_id = int(equipment_type[0])
                equipment_types_dict[equipment_type_uid] = equipment_type_id
        except:
            raise Exception("Error parsing equipment data retrieved from database")
        
        return equipment_types_dict

def insert_equipment(self, equipment_name) -> int:
    """Create a new equipment entry in the database if it does not exist, otherwise return the existing key

    Args:
        equipment_name (str/[str]): String of list of strings containing the equipment name(s)

    Raises:
        Exception: Invalid input. Expected a string or a list of strings.
        Exception: Error retrieving equipment type for _equipment_name_ from database
        Exception: Error retrieving equipment data for _equipment_name_ from database
        Exception: Error creating new equipment entry for _equipment_name_ in database

    Returns:
        int: list of db keys to the new or existing equipment entries
    """
    
    if isinstance(equipment_name, str):
        equipment_names = [equipment_name]
    elif isinstance(equipment_name, list):
        equipment_names = equipment_name
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
            raise Exception(f"Error retrieving equipment type for {name} from database")
        
        query = (f"SELECT ID_EQUIPMENT "
                f"FROM DIM_SPECTRUN_EQUIPMENT "
                f"WHERE"
                f" NA_EQUIPMENT LIKE '{name_lower_case}';")
        
        try:
            self.cursor.execute(query)
        except:
            self._disconnect()
            raise Exception(f"Error retrieving equipment data for {name} from database")

        try:
            equipment_id = int(self.cursor.fetchone()[0])
        except:
            query =(f"INSERT INTO DIM_SPECTRUN_EQUIPMENT"
                    f" (NA_EQUIPMENT,"
                    f" FK_EQUIPMENT_TYPE) "
                    f"VALUES"
                    f" ('{name}',"
                    f" {equipment_type_id})")

            try:
                self.cursor.execute(query)
                self.cursor.commit()
            
                equipment_id = int(self.cursor.lastrowid)
            except:
                self._disconnect()
                raise Exception(f"Error creating new equipment entry for {name} in database")
        
        equipment_ids.append(equipment_id)
        
    self._disconnect()
    
    return equipment_ids



        equipment_types = self._get_equipment_types()
        
        # iterate over the dictionary to match the equipment type based on the equipment name
        name_lower_case = equipment_name.lower()
        equipment_type_id = False
        for type_uid, type_id in equipment_types.items():
            if name_lower_case.find(type_uid) != -1:
                equipment_type_id = type_id
                break
        
        if not equipment_type_id:
            raise Exception(f"Error retrieving equipment type for {equipment_name} from database")
        
        self._connect()
        
        query = (f"SELECT ID_EQUIPMENT "
                f"FROM DIM_SPECTRUN_EQUIPMENT "
                f"WHERE"
                f" NA_EQUIPMENT LIKE '{name_lower_case}';")
        
        self.cursor.execute(query)

        try:
            procedure_id = int(self.cursor.fetchone()[0])
        except:            
            query =(f"INSERT INTO DIM_SPECTRUN_EQUIPMENT"
                    f" (NA_EQUIPMENT,"
                    f" FK_EQUIPMENT_TYPE) "
                    f"VALUES"
                    f" ('{equipment_name}',"
                    f" {equipment_type_id})")

            try:
                self.cursor.execute(query)
                self.cursor.commit()
            
                procedure_id = int(self.cursor.lastrowid)
            except:
                self._disconnect()
                raise Exception(f"Error creating new procedure equipment entry for {data['procedure_name']} in database")
        
        self._disconnect()
        
        return procedure_id

        
        query = (f"INSERT INTO EQUIPMENT"
                 f" (name, data) "
                 f"SELECT '{equipment_name}', '{equipment_type}' "
                 f"WHERE NOT EXISTS (SELECT 1 FROM EQUIPMENT WHERE LOWER(name) = LOWER('{equipment_type}'));")
            
        try:
            self.cursor.execute(query)
            self.db_connection.commit()
            equipment_id = int(self.cursor.lastrowid)
            self._disconnect()
        except:
            raise Exception(f"Error updating site {self.data['Site_ID']} from database")
    
        return(equipment_id)

    def updateFile(self):
        
        dbKeyFile = self.dbMerge(table = "DIM_MEDICAO_ESPECTRO_ARQUIVO",
                                 idColumn = "ID_ARQUIVO",
                                 newDataList = [("NO_ARQUIVO", self.data['File_Name']),
                                                ("NO_DIR_E_ARQUIVO", self.data['fileFullPath']),
                                                ("NO_URL", self.data['URL']),
                                                ("ID_TIPO_ARQUIVO",k.DB_CRFS_BIN_FILE_FILE)],
                                 where_conditionList = [f"NO_ARQUIVO LIKE '{self.data['File_Name']}'"])
        
        return(dbKeyFile)

    # used only to insert new data to the database.
    def insertSpectrum(self, dbKeyFile, dbKeySite, dbKeyProcedure, dbKeyEquipment,):
    
        for _,spectrumData in self.data['Fluxos'].items():

            # get key to the detector
            query = (f"SELECT ID_TIPO_DETECTOR "
                f"FROM DIM_MEDICAO_ESPECTRO_DETECTOR "
                f"WHERE"
                f" NO_TIPO_DETECTOR LIKE '{k.DEFAULT_DETECTOR}';")

            self.cursor.execute(query)
            dbKeyDetector = int(self.cursor.fetchone()[0])

            # get key to the trace type and insert if needed
            dbKeyTrace = self.dbMerge(table = "DIM_MEDICAO_ESPECTRO_TRACO",
                                      idColumn = "ID_TIPO_TRACO",
                                      newDataList = [("NO_TIPO_TRACO", spectrumData['Trace_Type'])],
                                      where_conditionList = [f"NO_TIPO_TRACO LIKE '{spectrumData['Trace_Type']}'"])

            # compose a query to get the measurement unit
            query = (f"SELECT ID_UNIDADE_MEDIDA "
                     f"FROM DIM_MEDICAO_ESPECTRO_UNIDADE "
                     f"WHERE"
                     f" NO_UNIDADE_MEDIDA LIKE N'{spectrumData['Level_Units']}';")

            try:
                self.cursor.execute(query)
                dbKeyMeasurementUnit = int(self.cursor.fetchone()[0])
            except:
                raise ValueError(f"Error retrieving measurement unit in file {self.data.OriginalFileName} {spectrumData['startFrequency']}")

            # compose a query to insert the data. This funcion is called only if file 
            query = (f"INSERT INTO FATO_MEDICAO_ESPECTRO"
                     f" (ID_ARQUIVO,"
                     f" ID_SITE,"
                     f" ID_TIPO_DETECTOR,"
                     f" ID_TIPO_TRACO,"
                     f" ID_UNIDADE_MEDIDA,"
                     f" ID_PROCEDIMENTO,"
                     f" NO_DESCRIPTION,"
                     f" NU_FREQUENCIA_INICIAL,"
                     f" NU_FREQUENCIA_FINAL,"
                     f" DT_TEMPO_INICIAL,"
                     f" DT_TEMPO_FINAL,"
                     f" NU_DURACAO_AMOSTRA,"
                     f" NU_NUMERO_TRACOS,"
                     f" NU_TAMANHO_VETOR,"
                     f" NU_RBW,"
                     f" NU_VBW,"
                     f" NU_ATENUACAO_GANHO) "
                     f"VALUES"
                     f" ({dbKeyFile},"
                     f" {dbKeySite},"
                     f" {dbKeyDetector},"
                     f" {dbKeyTrace},"
                     f" {dbKeyMeasurementUnit},"
                     f" {dbKeyProcedure},"
                     f" '{spectrumData['Description']}',"
                     f" {spectrumData['Start_Frequency']},"
                     f" {spectrumData['Stop_Frequency']},"
                     f" '{spectrumData['Initial_Time']}',"
                     f" '{spectrumData['Timestamp'].max()}',"
                     f" {spectrumData['Sample_Duration']},"
                     f" {spectrumData['Num_Traces']},"
                     f" {spectrumData['Vector_Length']},"
                     f" {spectrumData['RBW']},"
                     f" {k.DEFAULT_VBW},"
                     f" {k.DEFAULT_ATTENUATOR});")

            self.cursor.execute(query)

            # get the key to the newly created entry
            self.cursor.execute("SELECT SCOPE_IDENTITY()")
            dbKeySiteSpectrum = int(self.cursor.fetchone()[0])

            self.db_connection.commit()

            # compose a query to insert data in the bridge to the equipment table.
            query = (f"INSERT INTO PONTE_MEDICAO_ESPECTRO_EQUIPAMENTO"
                     f" (ID_EQUIPAMENTO,"
                     f" ID_MEDICAO_ESPECTRO)"
                     f"VALUES"
                     f" ({dbKeyEquipment},"
                     f" {dbKeySiteSpectrum})")

            self.cursor.execute(query)
            self.db_connection.commit()


    def updateDatabase(self, data):
        
        self._connect()

        self.data = data.metadata

        dbKeySite = self.updateSite()

        dbKeyEquipment = self.updateEquipment()

        dbKeyFile = self.updateFile()

        dbKeyProcedure = self.updateProcedure()
        
        self.insertSpectrum(dbKeyFile, dbKeySite, dbKeyProcedure, dbKeyEquipment)

        self._disconnect()

    def dbFileSearch(self, fileName):
        
        self._connect()
        
        # search database for existing entry with the same filename
        query = (f"SELECT ID_ARQUIVO "
                 f"FROM DIM_MEDICAO_ESPECTRO_ARQUIVO "
                 f"WHERE"
                 f" NO_ARQUIVO LIKE '{fileName}';")

        self.cursor.execute(query)

        # if no record is found, which should be rule, return flag
        if self.cursor.rowcount == 0:
            # set output as a flag that to indicate that there is no data
            dbKeyFile = False
        else:
            # set the output to indicate the entry ID
            dbKeyFile = int(self.cursor.fetchone()[0])

        if dbKeyFile:
            print("dfd")

        # disconnect in case no update should be performed
        self._disconnect()

        return(dbKeyFile)
    
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
                    f"('{hostid}', '{host_uid},"
                    f"'0', "
                    f"'0', '0', "
                    f"'0', '0');")
        
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
        
        if task_status['nu_host_files'] > 0:
            host_files_operation = "+"
        else:
            host_files_operation = "-"
            task_status['nu_host_files'] = -task_status['nu_host_files']

        if task_status['nu_pending_backup'] > 0:
            pending_backup_operation = "+"
        else:
            pending_backup_operation = "-"
            task_status['nu_pending_backup'] = -task_status['nu_pending_backup']

        if task_status['nu_backup_error'] > 0:
            backup_error_operation = "+"
        else:
            backup_error_operation = "-"
            task_status['nu_backup_error'] = -task_status['nu_backup_error']

        # compose and excecute query to update the backup status in the BKPDATA database
        query = (f"UPDATE HOST SET "
                    f"NU_HOST_FILES = NU_HOST_FILES {host_files_operation} {task_status['nu_host_files']}, "
                    f"NU_PENDING_BACKUP = NU_PENDING_BACKUP {pending_backup_operation} {task_status['nu_pending_backup']}, "
                    f"DT_LAST_BACKUP = NOW(), "
                    f"NU_BACKUP_ERROR = NU_BACKUP_ERROR {backup_error_operation} {task_status['nu_backup_error']} "
                    f"WHERE ID_HOST = {task_status['host_id']};")
        
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
    def add_processing_task(self,hostid="1",done_backup_list=[]):
        """This method adds tasks to the processing queue
        
        Args:
            hostid (int): Zabbix host id primary key. Defaults to "1".
            done_backup_list (list): List of files that were recently copied, in the format:[{"remote":remote_file_name,"local":local_file_name}]. Defaults to [].

        Returns:
            _type_: _description_
        """
        
        if len(done_backup_list) > 0:
            # connect to the database
            self._connect()
        
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
    def next_processing_task(self):        
        # connect to the database
        self._connect()

        # build query to get the next backup task with host_uid
        query = (   "SELECT PRC_TASK.ID_PRC_TASK, "
                            "PRC_TASK.FK_HOST, HOST.HOST_UID, "
                            "PRC_TASK.NO_HOST_FILE_PATH, PRC_TASK.NO_HOST_FILE_NAME, "
                            "PRC_TASK.NO_SERVER_FILE_PATH, PRC_TASK.NO_SERVER_FILE_NAME, "
                    "FROM PRC_TASK "
                    "JOIN HOST ON PRC_TASK.FK_HOST = HOST.ID_HOST "
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
    def update_processing_status(self, host_id, pending_processing):
        # connect to the database
        self._connect()
        
        # compose and excecute query to update the processing status by adding pending_processing variable to existing value in the database
        query = (f"UPDATE HOST SET "
                    f"NU_PENDING_PROCESSING = NU_PENDING_PROCESSING + {pending_processing}, "
                    f"DT_LAST_PROCESSING = NOW() "
                    f"WHERE ID_HOST = {host_id};")
        
        self.cursor.execute(query)
        self.db_connection.commit()
        
        self._disconnect()

    # Method to remove a completed backup task from the database
    def remove_processing_task(self, host_id, task_id):
        # connect to the database
        self._connect()
        
        # compose and excecute query to delete the backup task from the BKPDATA database
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


    # method to search database and if value not found, insert
    def db_flex_insert_update (self, table, idColumn, newDataList, where_conditionList):

# TODO: Study use of SQL Merge function. Since watchdog controls the threads, there is little risk of simultaneous insert/update for the same data. Additional complexity of merge and reduced compatibility is not justifiable. Additional limitation concerning the return of new or existing id.**--*

        #create query to search database for existing  entry
        query = (f"SELECT {idColumn} "
                 f"FROM {table} "
                 f"WHERE ")

        for where_condition in where_conditionList:
            query = query + f"{where_condition} AND "

        # remove last " AND " and add ";" to end the query
        query = query[:-5] + f";"

        self.cursor.execute(query)
        
        try:
            #try to retrieve an existing key to the existing district entry    
            dbKey = int(self.cursor.fetchone()[0])

            if k.VERBOSE: print(f'     Using data in {table} with registry ID {dbKey}')
        except:

            # create a query to input the data
            queryColumns = f" ("
            queryValues = f" VALUES ("

            for data in newDataList:
                queryColumns = queryColumns + f"{data[0]}, "
                if isinstance(data[1],str):
                    queryValues = queryValues + f"'{data[1]}', "
                else:
                    queryValues = queryValues + f"{data[1]}, "

            queryColumns = queryColumns[:-2] + f")"
            queryValues = queryValues[:-2] + f");"

            # if there is no key to retrieve, get the 
            query = f"INSERT INTO {table}" + queryColumns + queryValues

            self.cursor.execute(query)

            # get the key to the newly created entry
            self.cursor.execute("SELECT SCOPE_IDENTITY()")
            dbKey = int(self.cursor.fetchone()[0])

            self.cursor.commit()

            if k.VERBOSE: print(f'     New entry created in {table} with registry ID {dbKey}')

        return dbKey