#!/usr/bin/env python
""" This module manage all database operations for the appCataloga scripts """

# Import libraries for:
import mysql.connector
import sys
import os
import time

# Import file with constants used in this code that are relevant to the operation
import config as k

class dbHandler():
#TODO: Improve error handling for database errors

    def __init__(self, database=k.RFM_DATABASE_NAME):
        """Initialize a new instance of the DBHandler class.

        Args:
            db_file (str): The path to the SQLite database file.
        """
        
        self.db_connection = None
        self.cursor = None
        self.database = database
        self.data = None

    def connect(self):
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

    def disconnect(self):
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

    # method to search database and if value not found, insert
    def dbMerge (self, table, idColumn, newDataList, whereConditionList):

# TODO: Study use of SQL Merge function. Since watchdog controls the threads, there is little risk of simultaneous insert/update for the same data. Additional complexity of merge and reduced compatibility is not justifiable. Additional limitation concerning the return of new or existing id.**--*

        #create query to search database for existing  entry
        query = (f"SELECT {idColumn} "
                 f"FROM {table} "
                 f"WHERE ")

        for whereCondition in whereConditionList:
            query = query + f"{whereCondition} AND "

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

    def _get_geographic_codes(self, data=None):
        """_summary_

        Raises:
            Exception: _description_
            ValueError: _description_

        Returns:
            _type_: _description_
        """

# TODO: Handle query errors

        # if data is not provided, use the data from the object
        if data is None:
            data = self.data
        
        # search database for existing state entry and get the existing key
        query = (f"SELECT ID_STATE_CODE "
                f"FROM DIM_SITE_STATE "
                f"WHERE"
                f" NA_STATE LIKE '{data['State']}';")

        self.cursor.execute(query)
        
        try:
            dbKeyState = int(self.cursor.fetchone()[0])
        except:
            raise ValueError(f"Error retrieving state name {data['State']}")

        # search database for existing county name entry within the identified State and get the existing key
        # Prepare multi word name to be processed in the full text search by replacing spaces with "AND"
        county = self.data['County'].replace(' ',' AND ')
        query = (f"SELECT ID_COUNTY_CODE "
                f"FROM DIM_SITE_COUNTY "
                f"WHERE"
                f" MATCH(NA_COUNTY) AGAINST('{county})')"
                f" AND FK_STATE_CODE = {dbKeyState};")

        self.cursor.execute(query)
        try:
            dbKeyCounty = int(self.cursor.fetchone()[0])
        except:
            raise ValueError(f"Error retrieving county name {data['County']}")

        #search database for the district name, inserting new value if non existant
        district = self.data['District'].replace(' ',' AND ')
        query = (f"SELECT ID_DISTRICT "
                f"FROM DIM_SITE_DISTRICT "
                f"WHERE"
                f" MATCH(NA_DISTRICT) AGAINST('{district}')"
                f" AND FK_COUNTY_CODE = {dbKeyCounty};")
        
        self.cursor.execute(query)
        try:
            dbKeyDistrict = int(self.cursor.fetchone()[0])
        except:
            query = (f"INSERT INTO DIM_SITE_DISTRICT"
                    f" (FK_COUNTY_CODE,"
                    f" NA_DISTRICT) "
                    f"VALUES"
                    f" ({dbKeyCounty},"
                    f" '{self.data['District']}');")
            
            self.cursor.execute(query)
            self.db_connection.commit()
            
            dbKeyDistrict = int(self.cursor.lastrowid)

        return (dbKeyState, dbKeyCounty, dbKeyDistrict)

    def get_site_id(self, data=None):
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
        
        if location_exist_in_db:
            return nearest_site[0]
        else:
            return False
            

    def create_site(self, data=none) -> int:
        """Create a new site in the database

        Args:
            data (_dict_, optional): New site info. Defaults to object self.data.

        Returns:
            int: New site ID
        """

        if data is None:
            data = self.data

        (dbKeysState, dbKeysCounty, dbKeysDistrict) = self._get_geographic_codes()
        
                                    'id_site':0,
                            'fk_site_district':0,
                            'fk_county_code':0,
                            'fk_state_code':0,
                            'na_site':"",
                            'geolocation':(0,0),
                            'nu_altutude':0,
                            'nu_gnss_measurements':0},
        
        fileAltitude = self.data['Sum_Altitude']/self.data['Count_GPS']
        # construct query to create new location in the database
        query = (f"DECLARE @g geography = 'POINT({longitude} {latitude} {fileAltitude} NULL)'; "
                    f"INSERT INTO DIM_SPECTRUN_SITE"
                    f" (ID_UF_IBGE,"
                    f" ID_MUNICIPIO_IBGE,"
                    f" ID_DISTRITO,"
                    f" GEO_POINT,"
                    f" NU_GNSS_MEASUREMENTS) "
                    f"VALUES"
                    f" ({dbKeysState},"
                    f" {dbKeysCounty},"
                    f" {dbKeysDistrict},"
                    f" @g,"
                    f" {self.data['Count_GPS']});")

        if k.VERBOSE: print(f"     New Location at Latitude: {latitude}, Longitude: {longitude}, Altitude: {fileAltitude}'")

        self.cursor.execute(query)

        # get the key to the newly created entry
        self.cursor.execute("SELECT SCOPE_IDENTITY()")
        dbKeySite = int(self.cursor.fetchone()[0])

        self.db_connection.commit()
        
        return(dbKeySite)
    
    def update_site(self, data):

# TODO: Use new median feature
        latitude = data['latitude']
        longitude = data['longitude']

        # get data from the point in the database that is nearest to the measurement location indicated in the file
        query = (f"SELECT"
             f" [ID_SITE],"
             f" ST_X(GEO_POINT) as LONGITUDE,"
             f" ST_Y(GEO_POINT) as LATITUDE,"
             f" [NU_ALTITUDE],"
             f" [NU_GNSS_MEASUREMENTS],"
             f" [FK_SITE_DISTRICT],"
             f" [FK_COUNTY_CODE],"
             f" [ID_STATE_CODE] "
             f"FROM DIM_SPECTRUN_SITE"
             f" ORDER BY ST_Distance_Sphere(GEO_POINT, ST_GeomFromText('POINT({longitude} {latitude})', 4326))"
             f" LIMIT 1;")

        try:
            # Try to get the nearest match
            self.cursor.execute(query)

            nearest_site = self.cursor.fetchone()
            
            nearest_site_longitude = float(nearest_site[1])
            nearest_site_latitude = float(nearest_site[2])

            #Check if the database point is within the the expected deviation of the measured location coordinates 
            near_in_longitude = ( abs(longitude-nearest_site_longitude) < k.MAXIMUM_GNSS_DEVIATION )
            near_in_latitude = ( abs(latitude-nearest_site_latitude) < k.MAXIMUM_GNSS_DEVIATION )
            location_exist_in_db = ( near_in_latitude and near_in_longitude)

        except:
                # Confirm If number of rows returned is zero, error is due to the fact that there is no entry in the database
                if self.cursor.rowcount == 0:
                    # set flag to create new location
                    location_exist_in_db = False
                else:
                # some other error occurred
                    raise Exception("Error retrieving location coordinates from database")

        if location_exist_in_db:
            # if number of measurements in the database greater than the maximum required number of measurements.
            if float(nearest_site[4]) > k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS:
                # Do not update, avoiding unnecessary processing and variable numeric overflow
                if k.VERBOSE: print(f'     Location at latitude: {latitude}, Longitude: {longitude} reached the maximum number of measurements. No update performed.')
            
            else:
                #add point coordinates in the file to the estimator already in the database
                longitudeSum = self.data['Sum_Longitude'] + ( near_in_longitude * float(nearest_site[4]) ) 
                latitudeSum = self.data['Sum_Latitude'] + ( near_in_latitude * float(nearest_site[4]) )
                altitudeSum = self.data['Sum_Altitude'] + ( float(nearest_site[3]) * float(nearest_site[4]) )
                numberOfMeasurements = self.data['Count_GPS'] + float(nearest_site[4])
                longitude = longitudeSum / numberOfMeasurements
                latitude = latitudeSum / numberOfMeasurements
                altitude = altitudeSum / numberOfMeasurements

                # construct query update point location in the database
                query = (f"DECLARE @g geography = 'POINT({longitude} {latitude} {altitude})'; "
                         f"UPDATE DIM_SPECTRUN_SITE "
                         f"SET GEO_POINT = @g,"
                         f" NU_GNSS_MEASUREMENTS = {numberOfMeasurements} "
                         f"WHERE ID_SITE = {nearest_site[0]};")

                if k.VERBOSE: print(f'     Updated location at Latitude: {latitude}, Longitude: {longitude}, Altitude: {altitude}')

            dbKeySite = int(nearest_site[0])

            self.cursor.execute(query)
            
            output = dbKeySite

        else:
            output = False

        return(output)

    def updateEquipment(self):

# TODO: Include antenna

        dbKeyEquipment = self.dbMerge(table = "DIM_MEDICAO_ESPECTRO_EQUIPAMENTO",
                                      idColumn = "ID_EQUIPAMENTO",
                                      newDataList = [("NO_EQUIPAMENTO", self.data['Equipment_ID']),
                                                          ("ID_TIPO_EQUIPAMENTO",k.DB_CRFS_BIN_EQUIPMENT_TYPE)],
                                      whereConditionList = [f"NO_EQUIPAMENTO LIKE '{self.data['Equipment_ID']}'"])
        
        return(dbKeyEquipment)

    def updateFile(self):
        
        dbKeyFile = self.dbMerge(table = "DIM_MEDICAO_ESPECTRO_ARQUIVO",
                                 idColumn = "ID_ARQUIVO",
                                 newDataList = [("NO_ARQUIVO", self.data['File_Name']),
                                                ("NO_DIR_E_ARQUIVO", self.data['fileFullPath']),
                                                ("NO_URL", self.data['URL']),
                                                ("ID_TIPO_ARQUIVO",k.DB_CRFS_BIN_FILE_FILE)],
                                 whereConditionList = [f"NO_ARQUIVO LIKE '{self.data['File_Name']}'"])
        
        return(dbKeyFile)

    def updateProcedure(self):
        
        # get key to the trace procedure and insert if needed
        dbKeyProcedure = self.dbMerge(table = "DIM_MEDICAO_PROCEDIMENTO",
                                        idColumn = "ID_PROCEDIMENTO",
                                        newDataList = [("NO_PROCEDIMENTO", self.data['Script_Version'])],
                                        whereConditionList = [f"NO_PROCEDIMENTO LIKE '{self.data['Script_Version']}'"])

        
        return(dbKeyProcedure)

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
                                      whereConditionList = [f"NO_TIPO_TRACO LIKE '{spectrumData['Trace_Type']}'"])

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
        
        self.connect()

        self.data = data.metadata

        dbKeySite = self.updateSite()

        dbKeyEquipment = self.updateEquipment()

        dbKeyFile = self.updateFile()

        dbKeyProcedure = self.updateProcedure()
        
        self.insertSpectrum(dbKeyFile, dbKeySite, dbKeyProcedure, dbKeyEquipment)

        self.disconnect()

    def dbFileSearch(self, fileName):
        
        self.connect()
        
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
        self.disconnect()

        return(dbKeyFile)
    
    # Internal method to add host to the database
    def _add_host(self, hostid="1"):
        """This method adds a new host to the database if it does not exist.
            Initialization of host statistics is essential to avoid errors and simplify later database queries and updates.
        
        Args:
            hostid (str): Zabbix host id primary key. Defaults to "host_id".

        Returns:
            none: none
        """
        # connect to the database
        self.connect()
        
        # compose query to create a new host entry in the BPDATA database, setting all values to zero. If host already in the database, do nothing
        query = (f"INSERT IGNORE INTO HOST "
                    f"(ID_HOST, NU_HOST_FILES, "
                    f"NU_PENDING_BACKUP, NU_BACKUP_ERROR, "
                    f"NU_PENDING_PROCESSING, NU_PROCESSING_ERROR) "
                    f"VALUES "
                    f"('{hostid}', '0', "
                    f"'0', '0', "
                    f"'0', '0');")
        
        # update database
        self.cursor.execute(query)
        self.db_connection.commit()
        
        self.disconnect()

    # get host status data from the database
    def get_host_task_status(self,hostid="host_id"):        
        
        # connect to the database
        self.connect()

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

        self.disconnect()
        
        return output
    
    # Method add a new host to the backup queue
    def add_backup_task(self,
                        hostid="1",
                        host_addr="host_addr",
                        host_port="2800",
                        host_user="user",
                        host_passwd="passwd"):
        """This method checks if the host is already in the database and if not, adds it to the backup queue
        
        Args:
            hostid (str): Zabbix host id primary key. Defaults to "host_id".
            host_addr (str): Remote host IP/DNS address. Defaults to "host_addr".
            host_port (str): Remote host SSH access port. Defaults to "2800".
            host_user (str): Remote host access user. Defaults to "user".
            host_passwd (str): Remote host access password. Defaults to "passwd".

        Returns:
            _type_: _description_
        """
        
        # create a new host entry in the database if it does not exist
        self._add_host(hostid)

        # connect to the database
        self.connect()
        
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
        self.disconnect()
        
    # get next host in the list for data backup
    def next_backup_task(self):
        """This method gets the next host in the list for data backup

        Returns:
            dict: Dictionary with the pending task information: task_id, host_id, host, port, user, password
        """        
        
        # connect to the database
        self.connect()

        # build query to get the next backup task
        query = (   "SELECT ID_BKP_TASK, FK_HOST, NO_HOST_ADDRESS, NO_HOST_PORT, NO_HOST_USER, NO_HOST_PASSWORD "
                    "FROM BKP_TASK "
                    "ORDER BY DT_BKP_TASK "
                    "LIMIT 1;")
        
        self.cursor.execute(query)
        
        task = self.cursor.fetchone()
        self.disconnect()

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
        self.connect()
        
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
        
        self.disconnect()

    # Method to remove a completed backup task from the database
    def remove_backup_task(self, task):
        # connect to the database
        self.connect()
        
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
        
        self.disconnect()

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
            self.connect()
        
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
            self.disconnect()

    # Method to get next host in the list for processing
    def next_processing_task(self):        
        # connect to the database
        self.connect()

        # build query to get the next backup task
        query = (   "SELECT ID_PRC_TASK, FK_HOST, "
                            "NO_HOST_FILE_PATH, NO_HOST_FILE_NAME, "
                            "NO_SERVER_FILE_PATH, NO_SERVER_FILE_NAME "
                    "FROM PRC_TASK "
                    "ORDER BY DT_PRC_TASK "
                    "LIMIT 1;")
        
        self.cursor.execute(query)
        
        task = self.cursor.fetchone()
        self.disconnect()
        
        try:
            output = {"task_id": int(task[0]),
                    "host_id": int(task[1]),
                    "host path": str(task[2]),
                    "host file": str(task[3]),
                    "server path": str(task[4]),
                    "server file": str(task[5])}
        except:
            output = False
        
        return output

    # Method to update the processing status information in the database
    def update_processing_status(self, host_id, pending_processing):
        # connect to the database
        self.connect()
        
        # compose and excecute query to update the processing status by adding pending_processing variable to existing value in the database
        query = (f"UPDATE HOST SET "
                    f"NU_PENDING_PROCESSING = NU_PENDING_PROCESSING + {pending_processing}, "
                    f"DT_LAST_PROCESSING = NOW() "
                    f"WHERE ID_HOST = {host_id};")
        
        self.cursor.execute(query)
        self.db_connection.commit()
        
        self.disconnect()

    # Method to remove a completed backup task from the database
    def remove_processing_task(self, host_id, task_id):
        # connect to the database
        self.connect()
        
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
        
        self.disconnect()    
