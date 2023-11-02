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
        self.metadata = None
        self.database = database

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
    def merge (self, table, idColumn, newDataList, whereConditionList):
        """_summary_

        Raises:
            Exception: _description_
            ValueError: _description_

        Returns:
            _type_: _description_
        """

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

            self.db_connection.commit()

            if k.VERBOSE: print(f'     New entry created in {table} with registry ID {dbKey}')

        return dbKey

    def update_geographic_names(self):
        """_summary_

        Raises:
            Exception: _description_
            ValueError: _description_

        Returns:
            _type_: _description_
        """

# TODO: Handle query errors

        # search database for existing state entry and get the existing key
        query = (f"SELECT ID_UF_IBGE "
                f"FROM DIM_LOCALIZACAO_UF "
                f"WHERE"
                f" NO_UF LIKE '{self.metadata['State']}';")

        self.cursor.execute(query)
        dbKeyState = int(self.cursor.fetchone()[0])

# TODO: Handle query errors            

        # search database for existing County entry within the identified State and get the existing key
        # Prepare multi word name to be processed in the full text search by replacing spaces with "AND"
        county = self.metadata['County'].replace(' ',' AND ')
        query = (f"SELECT ID_MUNICIPIO_IBGE "
                f"FROM DIM_LOCALIZACAO_MUNICIPIO "
                f"WHERE"
                f" CONTAINS(NO_MUNICIPIO,'{county}')"
                f" AND ID_UF_IBGE = {dbKeyState};")

        self.cursor.execute(query)
        dbKeyCounty = int(self.cursor.fetchone()[0])

        #search database for the district name, inserting new value if non existant
        dbKeyDistrict = self.dbMerge(table = "DIM_LOCALIZACAO_DISTRITO",
                                    idColumn = "ID_DISTRITO",
                                    newDataList = [("NO_DISTRITO", self.metadata['District']),
                                                    ("ID_MUNICIPIO_IBGE",dbKeyCounty)],
                                    whereConditionList = [f"NO_DISTRITO LIKE '{self.metadata['District']}'",
                                                            f"ID_MUNICIPIO_IBGE = {dbKeyCounty}"])

        return (dbKeyState, dbKeyCounty, dbKeyDistrict)

    def update_site(self):

        fileLatitude = self.metadata['Sum_Latitude'] / self.metadata['Count_GPS']
        fileLongitude = self.metadata['Sum_Longitude'] / self.metadata['Count_GPS']

        # get data from the point in the database that is nearest to the measurement location indicated in the file
        query = (f"DECLARE @g geography = 'POINT({fileLongitude} {fileLatitude})'; "
                 f"SELECT TOP(1)"
                 f" [ID_LOCALIZACAO],"
                 f" [GEO_PONTO].Long as LONGITUDE,"
                 f" [GEO_PONTO].Lat as LATITUDE,"
                 f" [GEO_PONTO].Z as ALTITUDE,"
                 f" [NU_QUANTIDADE_MEDIDAS_GNSS],"
                 f" [ID_DISTRITO],"
                 f" [ID_MUNICIPIO_IBGE],"
                 f" [ID_UF_IBGE] "
                 f"FROM DIM_MEDICAO_ESPECTRO_LOCALIZACAO"
                 f" WHERE GEO_PONTO.STDistance(@g) IS NOT NULL"
                 f" ORDER BY GEO_PONTO.STDistance(@g);")

        #Alternative SQL to compute and return distance using SGBD method
        #    self.cursor.execute(f"DECLARE @g geography = 'POINT(0.000 0.0001 10)'; DECLARE @h geography; SELECT @h = [GEO_PONTO] FROM DIM_MEDICAO_ESPECTRO_LOCALIZACAO WHERE [ID_LOCALIZACAO]=1; SELECT @g.STDistance(@h);
        
        try:
            # Try to get the nearest match
            self.cursor.execute(query)

            nearestDBLocation = self.cursor.fetchone()
            dbNearestPointLongitude = float(nearestDBLocation[1])
            dbNearestPointLatitude = float(nearestDBLocation[2])

            #Check if the database point is within the the expected deviation of the measured location coordinates 
            nearInLatitude = ( abs(fileLatitude-dbNearestPointLatitude) < k.MAXIMUM_GNSS_DEVIATION )
            nearInLongitude = ( abs(fileLongitude-dbNearestPointLongitude) < k.MAXIMUM_GNSS_DEVIATION )

            # if new location is near to existing point already registered in the database
            if nearInLatitude and nearInLongitude:
                # set flag to update the existing location
                locationAlreadyExist = True
            else:
                # set flag to create new location
                locationAlreadyExist = False

        except:
                # Confirm If number of rows returned is zero, error is due to the fact that there is no entry in the database
                if self.cursor.rowcount == 0:
                    # set flag to create new location
                    locationAlreadyExist = False
                else:
                # some other error occurred
                    raise Exception("Error retrieving location coordinates from database")

        if locationAlreadyExist:
            # if number of measurements in the database greater than the maximum required number of measurements.
            if float(nearestDBLocation[4]) > k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS:
                # Do not update, avoiding unnecessary processing and variable numeric overflow
                if k.VERBOSE: print(f'     Location at latitude: {fileLatitude}, Longitude: {fileLongitude} reached the maximum number of measurements. No update performed.')
            
            else:
                #add point coordinates in the file to the estimator already in the database
                longitudeSum = self.metadata['Sum_Longitude'] + ( dbNearestPointLongitude * float(nearestDBLocation[4]) ) 
                latitudeSum = self.metadata['Sum_Latitude'] + ( dbNearestPointLatitude * float(nearestDBLocation[4]) )
                altitudeSum = self.metadata['Sum_Altitude'] + ( float(nearestDBLocation[3]) * float(nearestDBLocation[4]) )
                numberOfMeasurements = self.metadata['Count_GPS'] + float(nearestDBLocation[4])
                longitude = longitudeSum / numberOfMeasurements
                latitude = latitudeSum / numberOfMeasurements
                altitude = altitudeSum / numberOfMeasurements

                # construct query update point location in the database
                query = (f"DECLARE @g geography = 'POINT({longitude} {latitude} {altitude})'; "
                         f"UPDATE DIM_MEDICAO_ESPECTRO_LOCALIZACAO "
                         f"SET GEO_PONTO = @g,"
                         f" NU_QUANTIDADE_MEDIDAS_GNSS = {numberOfMeasurements} "
                         f"WHERE ID_LOCALIZACAO = {nearestDBLocation[0]};")

                if k.VERBOSE: print(f'     Updated location at Latitude: {latitude}, Longitude: {longitude}, Altitude: {altitude}')

            dbKeySite = int(nearestDBLocation[0])

            self.cursor.execute(query)

        else:
            
            (dbKeysState, dbKeysCounty, dbKeysDistrict) = self.updateGeographicNames()
            
            fileAltitude = self.metadata['Sum_Altitude']/self.metadata['Count_GPS']
            # construct query to create new location in the database
            query = (f"DECLARE @g geography = 'POINT({fileLongitude} {fileLatitude} {fileAltitude} NULL)'; "
                     f"INSERT INTO DIM_MEDICAO_ESPECTRO_LOCALIZACAO"
                     f" (ID_UF_IBGE,"
                     f" ID_MUNICIPIO_IBGE,"
                     f" ID_DISTRITO,"
                     f" GEO_PONTO,"
                     f" NU_QUANTIDADE_MEDIDAS_GNSS) "
                     f"VALUES"
                     f" ({dbKeysState},"
                     f" {dbKeysCounty},"
                     f" {dbKeysDistrict},"
                     f" @g,"
                     f" {self.metadata['Count_GPS']});")

            if k.VERBOSE: print(f"     New Location at Latitude: {fileLatitude}, Longitude: {fileLongitude}, Altitude: {fileAltitude}'")

            self.cursor.execute(query)

            # get the key to the newly created entry
            self.cursor.execute("SELECT SCOPE_IDENTITY()")
            dbKeySite = int(self.cursor.fetchone()[0])

        self.db_connection.commit()

        return(dbKeySite)

    def updateEquipment(self):

# TODO: Include antenna

        dbKeyEquipment = self.dbMerge(table = "DIM_MEDICAO_ESPECTRO_EQUIPAMENTO",
                                      idColumn = "ID_EQUIPAMENTO",
                                      newDataList = [("NO_EQUIPAMENTO", self.metadata['Equipment_ID']),
                                                          ("ID_TIPO_EQUIPAMENTO",k.DB_CRFS_BIN_EQUIPMENT_TYPE)],
                                      whereConditionList = [f"NO_EQUIPAMENTO LIKE '{self.metadata['Equipment_ID']}'"])
        
        return(dbKeyEquipment)

    def updateFile(self):
        
        dbKeyFile = self.dbMerge(table = "DIM_MEDICAO_ESPECTRO_ARQUIVO",
                                 idColumn = "ID_ARQUIVO",
                                 newDataList = [("NO_ARQUIVO", self.metadata['File_Name']),
                                                ("NO_DIR_E_ARQUIVO", self.metadata['fileFullPath']),
                                                ("NO_URL", self.metadata['URL']),
                                                ("ID_TIPO_ARQUIVO",k.DB_CRFS_BIN_FILE_FILE)],
                                 whereConditionList = [f"NO_ARQUIVO LIKE '{self.metadata['File_Name']}'"])
        
        return(dbKeyFile)

    def updateProcedure(self):
        
        # get key to the trace procedure and insert if needed
        dbKeyProcedure = self.dbMerge(table = "DIM_MEDICAO_PROCEDIMENTO",
                                        idColumn = "ID_PROCEDIMENTO",
                                        newDataList = [("NO_PROCEDIMENTO", self.metadata['Script_Version'])],
                                        whereConditionList = [f"NO_PROCEDIMENTO LIKE '{self.metadata['Script_Version']}'"])

        
        return(dbKeyProcedure)

    # used only to insert new data to the database.
    def insertSpectrum(self, dbKeyFile, dbKeySite, dbKeyProcedure, dbKeyEquipment,):
    
        for _,spectrumData in self.metadata['Fluxos'].items():

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
                raise ValueError(f"Error retrieving measurement unit in file {self.metadata.OriginalFileName} {spectrumData['startFrequency']}")

            # compose a query to insert the data. This funcion is called only if file 
            query = (f"INSERT INTO FATO_MEDICAO_ESPECTRO"
                     f" (ID_ARQUIVO,"
                     f" ID_LOCALIZACAO,"
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

        self.metadata = data.metadata

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
        # connect to the database
        self.connect()
        
        # compose query to set host data in the BPDATA database        
        query = (f"INSERT INTO HOST "
                    f"(ID_HOST, NU_PENDING_BACKUP) "
                    f"VALUES "
                    f"('{hostid}', '{1}') "
                    f"ON DUPLICATE KEY UPDATE NU_PENDING_BACKUP = NU_PENDING_BACKUP + 1;")
        
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

        if len(task) > 0:
            output = {"task_id": task[0],
                    "host_id": task[1],
                    "host_add": task[2],
                    "port": task[3],
                    "user": task[4],
                    "password": task[5]}
        else:
            output = False
        
        return output

    # Method to update the backup status information in the database
    def update_backup_status(self, task_status):
        # connect to the database
        self.connect()

        # compose and excecute query to get the number of files previously processed from the host, as stored in the database
        query = (f"SELECT NU_HOST_FILES, NU_BACKUP_ERROR "
                    f"FROM HOST "
                    f"WHERE ID_HOST = {task_status['host_id']};")
        self.cursor.execute(query)
        output = self.cursor.fetchone()
        nu_host_files = output[0]
        nu_backup_error = output[1]
        
        # compose and excecute query to update the backup status in the BKPDATA database
        query = (f"UPDATE HOST SET "
                    f"NU_HOST_FILES = {nu_host_files + task_status['nu_host_files']}, "
                    f"NU_PENDING_BACKUP = {task_status['nu_pending_backup']}, "
                    f"DT_LAST_BACKUP = NOW(), "
                    f"NU_BACKUP_ERROR = {nu_backup_error + task_status['nu_backup_error']} "
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
        # connect to the database
        self.connect()
        
        # convert done_backup_list list of dicionaries into a list of tuples
        for item in done_backup_list:
            item = (hostid,
                    os.path.dirname(item["remote"]), os.path.basename(item["remote"]),
                    os.path.dirname(item["local"]), os.path.basename(item["local"]))
                    
        # compose query to set the process task in the database
        query = (f"INSERT INTO BKP_TASK "
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
        query = (   "SELECT ID_PRC_TASK, FK_HOST, NO_HOST_FILE_PATH, NO_HOST_FILE_NAME, NO_SERVER_FILE_PATH, NO_SERVER_FILE_NAME "
                    "FROM PRC_TASK "
                    "ORDER BY DT_PRC_TASK "
                    "LIMIT 1;")
        
        self.cursor.execute(query)
        
        task = self.cursor.fetchone()
        self.disconnect()
        
        if len(task) > 0:
            output = {"task_id": task[0],
                    "host_id": task[1],
                    "host path": task[2],
                    "host file": task[3],
                    "server path": task[4],
                    "server file": task[5]}
        else:
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
        
        output = self.cursor.fetchone()
        
        output = {'Host ID': output[0],
                  'Total Files': output[1],
                  'Files to backup': output[2],
                  'Last Backup date': output[3],
                  'Files to process': output[4],
                  'Last Processing date': output[5],
                  'Status': 1, 
                  'Message': 'OK'}
        
        self.disconnect()
        
        return output
    
