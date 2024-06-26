#!/usr/bin/env python
"""This module manage all database operations for the appCataloga scripts"""

# Import libraries for:

import sys

sys.path.append("/etc/appCataloga")

import mysql.connector
import os
import re
import pandas as pd
import datetime
from typing import List, Union, Tuple

# Import file with constants used in this code that are relevant to the operation
import config as k


class dbHandler:
    """Class associated with the database operations for the appCataloga scripts"""

    def __init__(self, database: str, log: any) -> None:
        """Initialize a new instance of the DBHandler class.

        Args:
            database (str): The name of the database to connect to.
            log (sh.log): The logging object to use for logging messages.
        """

        self.db_connection = None
        self.cursor = None
        self.database = database
        self.data = None
        self.log = log

        # constants
        self.HOST_TASK_TYPE = 0
        self.FILE_TASK_BACKUP_TYPE = 1
        self.FILE_TASK_PROCESS_TYPE = 2
        self.TASK_SUSPENDED = -2
        self.TASK_ERROR = -1
        self.TASK_NULL = 0
        self.TASK_PENDING = 1
        self.TASK_RUNNING = 2
        self.HOST_WITHOUT_DAEMON = 1
        self.HOST_WITH_HALT_FLAG = 2

    def _connect(self):
        """Try to connect to the database using the parameters in the config.py file

        Raises:
            Exception: from pyodbc.connect
            ValueError: from pyodbc.connect

        Returns:
            self.db_connection: update to the database connection
            self.cursor: update to the database cursor
        """

        # connect to database using parameters in the config.py file
        config = {
            "user": k.DB_USER_NAME,
            "password": k.DB_PASSWORD,
            "host": k.SERVER_NAME,
            "database": self.database,
        }

        self.db_connection = mysql.connector.connect(**config)

        self.cursor = self.db_connection.cursor(buffered=True)

    def _disconnect(self):
        """Disconnect from the database for graceful exit

        Raises:
            Exception: from pyodbc.disconnect
            ValueError: from pyodbc.disconnect

        Returns:
            self.db_connection: update to None
            self.cursor: update to None
        """

        self.cursor.close()
        self.db_connection.close()

    def get_site_id(self, data: dict) -> Tuple[int, bool]:
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
        query = (
            f"SELECT"
            f" ID_SITE,"
            f" ST_X(GEO_POINT) as LONGITUDE,"
            f" ST_Y(GEO_POINT) as LATITUDE "
            f"FROM DIM_SPECTRUM_SITE"
            f" ORDER BY ST_Distance_Sphere(GEO_POINT, ST_GeomFromText('POINT({data['longitude']} {data['latitude']})', 4326))"
            f" LIMIT 1;"
        )

        try:
            self.cursor.execute(query)

            nearest_site = self.cursor.fetchone()
        except Exception as e:
            self._disconnect()
            raise Exception(
                "Error retrieving location coordinates from database"
            ) from e

        try:
            nearest_site_id = int(nearest_site[0])
            nearest_site_longitude = float(nearest_site[1])
            nearest_site_latitude = float(nearest_site[2])

            # Check if the database point is within the expected deviation of the measured location coordinates
            near_in_longitude = (
                abs(data["longitude"] - nearest_site_longitude)
                < k.MAXIMUM_GNSS_DEVIATION
            )
            near_in_latitude = (
                abs(data["latitude"] - nearest_site_latitude) < k.MAXIMUM_GNSS_DEVIATION
            )
            location_exist_in_db = near_in_latitude and near_in_longitude
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

    def update_site(
        self,
        site=int,
        longitude_raw=[float],
        latitude_raw=[float],
        altitude_raw=[float],
    ) -> None:
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
        query = (
            f"SELECT"
            f" ST_X(GEO_POINT) as LONGITUDE,"
            f" ST_Y(GEO_POINT) as LATITUDE,"
            f" NU_ALTITUDE,"
            f" NU_GNSS_MEASUREMENTS "
            f"FROM DIM_SPECTRUM_SITE "
            f"WHERE"
            f" ID_SITE = {site};"
        )

        try:
            # Try to get the nearest match
            self.cursor.execute(query)

            nearest_site = self.cursor.fetchone()
        except Exception as e:
            self._disconnect()
            raise Exception(
                f"Error retrieving site {self.data['Site_ID']} from database"
            ) from e

        try:
            db_site_longitude = float(nearest_site[0])
            db_site_latitude = float(nearest_site[1])
            db_site_altitude = float(nearest_site[2])
            db_site_nu_gnss_measurements = int(nearest_site[3])
        except (ValueError, IndexError) as e:
            self._disconnect()
            raise Exception(
                f"Invalid data returned for site {self.data['Site_ID']} from database"
            ) from e

        # if number of measurements in the database greater than the maximum required number of measurements.
        if db_site_nu_gnss_measurements < k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS:
            # add point coordinates in the file to the estimator already in the database
            longitudeSum = longitude_raw.sum() + (
                db_site_longitude * db_site_nu_gnss_measurements
            )
            latitudeSum = latitude_raw.sum() + (
                db_site_latitude * db_site_nu_gnss_measurements
            )
            altitudeSum = altitude_raw.sum() + (
                db_site_altitude * db_site_nu_gnss_measurements
            )
            nu_gnss_measurements = db_site_nu_gnss_measurements + len(longitude_raw)
            longitude = longitudeSum / nu_gnss_measurements
            latitude = latitudeSum / nu_gnss_measurements
            altitude = altitudeSum / nu_gnss_measurements

            # construct query update point location in the database
            query = (
                f"UPDATE DIM_SPECTRUM_SITE "
                f" SET GEO_POINT = ST_GeomFromText('POINT({longitude} {latitude})'),"
                f" NU_ALTITUDE = {altitude},"
                f" NU_GNSS_MEASUREMENTS = {nu_gnss_measurements} "
                f"WHERE ID_SITE = {site};"
            )

            try:
                self.cursor.execute(query)
                self.db_connection.commit()

                self._disconnect()

                self.log.entry(
                    f"Updated location at latitude: {latitude}, longitude: {longitude}"
                )
            except Exception as e:
                self._disconnect()
                raise Exception(
                    f"Error updating site {self.data['Site_ID']} from database"
                ) from e

        else:
            # Do not update, avoiding unnecessary processing and variable numeric overflow
            self.log.entry(
                f"Site {site} at latitude: {db_site_latitude}, longitude: {db_site_longitude} reached the maximum number of measurements. No update performed."
            )

    def _get_geographic_codes(self, data: dict) -> Tuple[int, int, int]:
        """Get DB keys for state, county and district based on the data in the object

        Args:
            data (dict): {"state":"state name", "county":"city,town name", "district":"suburb name"}

        Raises:
            Exception: Fail to retrive state name from database
            Exception: Fail to retrive county name from database

        Returns:
            Tuple[int, int, int]: Tuple with the DB keys for state, county and district
        """

        self._connect()

        # search database for existing state entry and get the existing key
        query = (
            f"SELECT ID_STATE "
            f"FROM DIM_SITE_STATE "
            f"WHERE"
            f" NA_STATE LIKE '{data['state']}';"
        )

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
            county = data["county"].replace(" ", " AND ")
            query = (
                f"SELECT ID_COUNTY "
                f"FROM DIM_SITE_COUNTY "
                f"WHERE"
                f" MATCH(NA_COUNTY) AGAINST('{county})')"
                f" AND FK_STATE = {db_state_id};"
            )

            self.cursor.execute(query)

            try:
                db_county_id = int(self.cursor.fetchone()[0])
            except Exception as e:
                self._disconnect()
                raise Exception(f"Error retrieving county name {data['County']}") from e

        # search database for the district name, inserting new value if non existant
        district = data["district"].replace(" ", " AND ")
        query = (
            f"SELECT ID_DISTRICT "
            f"FROM DIM_SITE_DISTRICT "
            f"WHERE"
            f" MATCH(NA_DISTRICT) AGAINST('{district}')"
            f" AND FK_COUNTY = {db_county_id};"
        )

        self.cursor.execute(query)

        try:
            db_district_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            query = (
                f"INSERT INTO DIM_SITE_DISTRICT"
                f" (FK_COUNTY,"
                f" NA_DISTRICT) "
                f"VALUES"
                f" ({db_county_id},"
                f" '{data['district']}');"
            )

            self.cursor.execute(query)
            self.db_connection.commit()

            db_district_id = int(self.cursor.lastrowid)

        self._disconnect()

        return (db_state_id, db_county_id, db_district_id)

    def insert_site(
        self,
        data={
            "latitude": 0,
            "longitude": 0,
            "altitude": 0,
            "state": "state name",
            "county": "city,town name",
            "district": "suburb name",
        },
    ) -> int:
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
        (db_state_id, db_county_id, db_district_id) = self._get_geographic_codes(
            data=data
        )

        self._connect()

        # construct query to create new sie in the database
        query = (
            f"INSERT INTO DIM_SPECTRUM_SITE"
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
            f" {db_district_id})"
        )

        try:
            self.cursor.execute(query)
            self.db_connection.commit()

            db_site_id = int(self.cursor.lastrowid)

            self._disconnect()
        except Exception as e:
            raise Exception(f"Error creating new site using query: {query}") from e

        return db_site_id

    def build_path(self, site_id: int) -> str:
        """Build the path to the site folder in the database in the format "LC State Code/county_id/site_id"

        Args:
            site_id (int): DB key to the site

        Raises:
            Exception: Error retrieving site path from database

        Returns:
            str: Path to the site folder in the database
        """

        self._connect()

        query = (
            f"SELECT"
            f" DIM_SITE_STATE.LC_STATE,"
            f" DIM_SPECTRUM_SITE.FK_COUNTY,"
            f" DIM_SPECTRUM_SITE.ID_SITE "
            f"FROM DIM_SPECTRUM_SITE"
            f" JOIN DIM_SITE_STATE ON DIM_SPECTRUM_SITE.FK_STATE = DIM_SITE_STATE.ID_STATE"
            f" WHERE"
            f" DIM_SPECTRUM_SITE.ID_SITE = {site_id};"
        )

        try:
            self.cursor.execute(query)
        except Exception as e:
            self._disconnect()
            raise Exception(
                f"Error retrieving site information using query: {query}"
            ) from e

        try:
            site_path = self.cursor.fetchone()
            new_path = f"{site_path[0]}/{site_path[1]}/{site_path[2]}"
        except Exception as e:
            self._disconnect()
            raise Exception(
                f"Error building path from site information from query: {query}"
            ) from e

        self._disconnect()

        return new_path

    # method to insert file entry in the database if it does not exist, otherwise return the existing key
    def insert_file(self, filename: str, path: str, volume: str) -> int:
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

        query = (
            f"SELECT ID_FILE "
            f"FROM DIM_SPECTRUM_FILE "
            f"WHERE"
            f" NA_FILE = '{filename}' AND"
            f" NA_PATH = '{path}' AND"
            f" NA_VOLUME = '{volume}';"
        )

        try:
            self.cursor.execute(query)
        except Exception as e:
            self._disconnect()
            raise Exception("Error retrieving file using query: {query}") from e

        try:
            file_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            query = (
                f"INSERT INTO DIM_SPECTRUM_FILE"
                f" (NA_FILE,"
                f" NA_PATH,"
                f" NA_VOLUME) "
                f"VALUES"
                f" ('{filename}',"
                f" '{path}',"
                f" '{volume}')"
            )
            try:
                self.cursor.execute(query)
                self.db_connection.commit()

                file_id = int(self.cursor.lastrowid)
            except Exception as e:
                self._disconnect()
                raise Exception(
                    f"Error creating new file entry using query: {query}"
                ) from e

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

        query = (
            f"SELECT ID_PROCEDURE "
            f"FROM DIM_SPECTRUM_PROCEDURE "
            f"WHERE"
            f" NA_PROCEDURE = '{procedure_name}';"
        )

        try:
            self.cursor.execute(query)
        except Exception as e:
            self._disconnect()
            raise Exception("Error retrieving procedure using query: {query}") from e

        try:
            procedure_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            query = (
                f"INSERT INTO DIM_SPECTRUM_PROCEDURE"
                f" (NA_PROCEDURE) "
                f"VALUES"
                f" ('{procedure_name}')"
            )

            try:
                self.cursor.execute(query)
                self.db_connection.commit()

                procedure_id = int(self.cursor.lastrowid)
            except Exception as e:
                self._disconnect()
                raise Exception(
                    f"Error creating new procedure entry using query: {query}"
                ) from e

        self._disconnect()

        return procedure_id

    def _get_equipment_types(self) -> dict:
        """Load all equipment types from the database and create a dictionary with equipmenty_type_uid as key and equipment_type_id as value

        Returns:
            dict: {equipment_type_uid:equipment_type_id}
        """

        self._connect()

        query = (
            "SELECT"
            " ID_EQUIPMENT_TYPE,"
            " NA_EQUIPMENT_TYPE_UID "
            "FROM DIM_EQUIPMENT_TYPE;"
        )

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
            raise Exception(
                "Error parsing equipment types retrieved from database"
            ) from e

        return equipment_types_dict

    def insert_equipment(self, equipment: Union[str, List[str]]) -> dict:
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

            query = (
                f"SELECT ID_EQUIPMENT "
                f"FROM DIM_SPECTRUM_EQUIPMENT "
                f"WHERE"
                f" LOWER(NA_EQUIPMENT) LIKE '{name_lower_case}';"
            )

            try:
                self.cursor.execute(query)
            except Exception as e:
                self._disconnect()
                raise Exception(
                    f"Error retrieving equipment data using query: {query}"
                ) from e

            try:
                equipment_id = int(self.cursor.fetchone()[0])
            except (TypeError, ValueError):
                query = (
                    f"INSERT INTO DIM_SPECTRUM_EQUIPMENT"
                    f" (NA_EQUIPMENT,"
                    f" FK_EQUIPMENT_TYPE) "
                    f"VALUES"
                    f" ('{name}',"
                    f" {equipment_type_id})"
                )

                try:
                    self.cursor.execute(query)
                    self.db_connection.commit()

                    equipment_id = int(self.cursor.lastrowid)
                except Exception as e:
                    self._disconnect()
                    raise Exception(
                        f"Error creating new equipment using query: {query}"
                    ) from e

            equipment_ids[name] = equipment_id

        self._disconnect()

        return equipment_ids

    def insert_detector_type(self, detector: str) -> int:
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

        query = (
            f"SELECT ID_DETECTOR "
            f"FROM DIM_SPECTRUM_DETECTOR "
            f"WHERE"
            f" NA_DETECTOR = '{detector}';"
        )

        try:
            self.cursor.execute(query)
        except Exception as e:
            self._disconnect()
            raise Exception(
                "Error retrieving detector type using query: {query}"
            ) from e

        try:
            detector_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            query = (
                f"INSERT INTO DIM_SPECTRUM_DETECTOR"
                f" (NA_DETECTOR) "
                f"VALUES"
                f" ('{detector}')"
            )

            try:
                self.cursor.execute(query)
                self.db_connection.commit()

                detector_id = int(self.cursor.lastrowid)
            except Exception as e:
                self._disconnect()
                raise Exception(
                    f"Error creating new detector entry using query: {query}"
                ) from e

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

        query = (
            f"SELECT ID_TRACE_TYPE "
            f"FROM DIM_SPECTRUM_TRACE_TYPE "
            f"WHERE"
            f" NA_TRACE_TYPE = '{trace_name}';"
        )

        try:
            self.cursor.execute(query)
        except Exception as e:
            self._disconnect()
            raise Exception(f"Error retrieving trace type using query: {query}") from e

        try:
            trace_type_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            query = (
                f"INSERT INTO DIM_SPECTRUM_TRACE_TYPE"
                f" (NA_TRACE_TYPE) "
                f"VALUES"
                f" ('{trace_name}')"
            )

            try:
                self.cursor.execute(query)
                self.db_connection.commit()

                trace_type_id = int(self.cursor.lastrowid)
            except Exception as e:
                self._disconnect()
                raise Exception(
                    f"Error creating new trace time entry using query: {query}"
                ) from e

        self._disconnect()

        return trace_type_id

    def insert_measure_unit(self, unit_name: str) -> int:
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

        query = (
            f"SELECT ID_MEASURE_UNIT "
            f"FROM DIM_SPECTRUM_UNIT "
            f"WHERE"
            f" NA_MEASURE_UNIT = '{unit_name}';"
        )

        try:
            self.cursor.execute(query)
        except Exception as e:
            self._disconnect()
            raise Exception(
                f"Error retrieving measure unit using query: {query}"
            ) from e

        try:
            measure_unit_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            query = (
                f"INSERT INTO DIM_SPECTRUM_UNIT"
                f" (NA_MEASURE_UNIT) "
                f"VALUES"
                f" ('{unit_name}')"
            )

            try:
                self.cursor.execute(query)
                self.db_connection.commit()

                measure_unit_id = int(self.cursor.lastrowid)
            except Exception as e:
                self._disconnect()
                raise Exception(
                    f"Error creating new measure unit entry using query: {query}"
                ) from e

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
        query = (
            f"SELECT ID_SPECTRUM "
            f"FROM FACT_SPECTRUM "
            f"WHERE"
            f" FK_SITE = {data['id_site']} AND"
            f" FK_TRACE_TYPE = {data['id_trace_type']} AND"
            f" NU_FREQ_START = {data['nu_freq_start']} AND"
            f" NU_FREQ_END = {data['nu_freq_end']} AND"
            f" DT_TIME_START = '{data['dt_time_start']}' AND"
            f" DT_TIME_END = '{data['dt_time_end']}' AND"
            f" NU_TRACE_COUNT = {data['nu_trace_count']} AND"
            f" NU_TRACE_LENGTH = {data['nu_trace_length']};"
        )

        try:
            self.cursor.execute(query)
        except Exception as e:
            self._disconnect()
            raise Exception(f"Error retrieving spectrum using query: {query}") from e

        try:
            spectrum_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            query = (
                f"INSERT INTO FACT_SPECTRUM"
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
                f" {data['nu_att_gain']})"
            )

            try:
                self.cursor.execute(query)
                self.db_connection.commit()

                spectrum_id = int(self.cursor.lastrowid)
            except Exception as e:
                self._disconnect()
                raise Exception(
                    f"Error creating new spectrum entry using query: {query}"
                ) from e

        self._disconnect()

        return spectrum_id

    def insert_bridge_spectrum_equipment(self, spectrum_lst: list) -> None:
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
                query = (
                    f"INSERT IGNORE INTO BRIDGE_SPECTRUM_EQUIPMENT"
                    f" (FK_SPECTRUM,"
                    f" FK_EQUIPMENT) "
                    f"VALUES"
                    f" ({entry['spectrum']},"
                    f" {equipment}); "
                )

                try:
                    self.cursor.execute(query)
                except Exception as e:
                    self._disconnect()
                    raise Exception(
                        f"Error creating new spectrum equipment entry using query: {query}"
                    ) from e

        self.db_connection.commit()
        self._disconnect()

    def insert_bridge_spectrum_file(self, spectrum_lst: list, file_lst: list) -> None:
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
                query = (
                    f"INSERT IGNORE INTO BRIDGE_SPECTRUM_FILE"
                    f" (FK_SPECTRUM,"
                    f" FK_FILE) "
                    f"VALUES"
                    f" ({entry['spectrum']},"
                    f" {file_id});"
                )

                try:
                    self.cursor.execute(query)
                except Exception as e:
                    self._disconnect()
                    raise Exception(
                        f"Error creating new spectrum file entry using query: {query}"
                    ) from e

        self.db_connection.commit()

        self._disconnect()

    def export_parquet(self, file_name: str) -> None:
        # connect to the database
        self._connect()

        # List all tables in the database
        query = "SHOW TABLES;"
        self.cursor.execute(query)
        tables = self.cursor.fetchall()

        # Loop through tables and get data into dataframes
        for table in tables:
            table_name = table[0]
            query = f"SELECT * FROM {table_name};"
            self.cursor.execute(query)
            table_data = self.cursor.fetchall()

            table_df = pd.DataFrame(table_data)

            # get column names from the database and assign to the dataframe
            query = f"SHOW COLUMNS FROM {table_name};"
            self.cursor.execute(query)
            columns = self.cursor.fetchall()
            column_names = [column[0] for column in columns]

            try:
                table_df.columns = column_names
            except ValueError:
                self.log.error(f"Error parsing column names for table {table_name}")
                continue

            # replace null values with "na"
            table_df = table_df.fillna("na")

            composed_file_name = f"{file_name}.{table_name}.parquet"
            # store the dataframe in the db_df
            table_df.to_parquet(composed_file_name)

        self._disconnect()

    def get_latest_processing_time(self) -> datetime:
        # connect to the database
        self._connect()

        # build query to get the latest processing time
        query = "SELECT MAX(DT_FILE_LOGGED) FROM DIM_SPECTRUM_FILE;"

        self.cursor.execute(query)

        try:
            latest_processing_time = self.cursor.fetchone()[0].timestamp()
        except (TypeError, ValueError):
            self.log.error(
                f"Error getting the latest timestamp from DIM_SPECTRUM_FILE table. "
                f"Fetched {self.cursor.fetchone()[0]}"
            )
            pass

        self._disconnect()

        return latest_processing_time

    # Internal method to add host to the database
    def host_create(self, hostid: str, host_uid: str) -> None:
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

        try:
            # compose query to create a new host entry in the BPDATA database, setting all values to zero. If host already in the database, do nothing
            query = (
                f"INSERT IGNORE INTO HOST "
                f"(ID_HOST, NA_HOST_UID, "
                f"NU_HOST_FILES, "
                f"NU_PENDING_HOST_TASK, NU_HOST_CHECK_ERROR, "
                f"NU_PENDING_BACKUP, NU_BACKUP_ERROR, "
                f"NU_PENDING_PROCESSING, NU_PROCESSING_ERROR) "
                f"VALUES "
                f"('{hostid}', '{host_uid}', "
                f"0, "
                f"0, 0, "
                f"0, 0, "
                f"0, 0);"
            )

            # update database
            self.cursor.execute(query)
            self.db_connection.commit()
        except Exception as e:
            message = f"Error adding host {hostid} to the database"
            self.log.error(message)
            raise Exception(message) from e
        finally:
            self._disconnect()

    # get host status data from the database
    def host_read_status(self, hostid: int) -> dict:
        """This method gets host status data from the database

        Args:
            hostid (int): PK for host in the database.

        Returns:
            dict:  "id_host": (int) Host id,
                    "nu_host_files": (int) Number of files in the host,
                    "nu_pending_host_task": (int) Number of pending host tasks,
                    "dt_last_host_check": (int) Last time the host was checked (epoch),
                    "nu_host_check_error": (int) Number of host check errors,
                    "nu_pending_backup": (int) Number of pending backups,
                    "dt_last_backup": (int) Last time the host was backed up (epoch),
                    "nu_backup_error": (int) Number of backup errors,
                    "nu_pending_processing": (int) Number of pending processing tasks,
                    "dt_last_processing": (int) Last time the host was processed (epoch),
                    "nu_processing_error": (int) Number of processing errors,
                    "nu_status": (int) Host status
        """

        # TODO #34 Improve task reporting by separating backup tasks from individual backup transactions
        # connect to the database
        self._connect()

        query_data = [
            "ID_HOST",
            "NU_HOST_FILES",
            "NU_PENDING_HOST_TASK",
            "DT_LAST_HOST_CHECK",
            "NU_HOST_CHECK_ERROR",
            "NU_PENDING_BACKUP",
            "DT_LAST_BACKUP",
            "NU_BACKUP_ERROR",
            "NU_PENDING_PROCESSING",
            "NU_PROCESSING_ERROR",
            "DT_LAST_PROCESSING",
            "NU_STATUS",
        ]

        # compose query to get host status data from the BPDATA database
        query = (
            f"SELECT "
            f"{','.join(query_data)} "
            f"FROM HOST "
            f"WHERE ID_HOST = '{hostid}';"
        )

        # get host data from the database
        self.cursor.execute(query)

        db_output = self.cursor.fetchone()

        # get the output in a dictionary format, converting datetime objects to epoch time
        try:
            output = {}
            for i in range(len(query_data)):
                if query_data[i][:3] == "DT_":
                    try:
                        output[query_data[i][3:].lower()] = db_output[i].timestamp()
                    except AttributeError:
                        output[query_data[i][3:].lower()] = "N/A"
                else:
                    try:
                        output[query_data[i][3:].lower()] = int(db_output[i])
                    except ValueError:
                        output[query_data[i][3:].lower()] = "N/A"

            output["status"] = 1
            output["message"] = ""

        except Exception as e:
            output = {
                "status": 0,
                "message": f"Error retrieving data for host {hostid}: {e}",
            }

        self._disconnect()

        return output

    def host_read_access(self, host_id: int) -> dict:
        """This method gets host access data from the database

        Args:
            hostid (int): PK for host in the database.

        Raises:
            Exception: _description_
            Exception: _description_

        Returns:
            dict:  {"host_id": (int) Host id,
                    "host_uid": (str) Host UID,
                    "host_add": (str) Host IP address or DNS recognized name,
                    "port": (int) Host SSH port,
                    "user": (str) Host access user,
                    "password": (str) Host access password
        """

        # compose query to get host access data from the BPDATA database
        query = (
            f"SELECT "
            f"NA_HOST_UID, "
            f"NA_HOST_ADDRESS, "
            f"NA_HOST_PORT, "
            f"NA_HOST_USER, "
            f"NA_HOST_PASSWORD "
            f"FROM HOST "
            f"WHERE ID_HOST = '{host_id}';"
        )

        # Get the data
        self._connect()

        self.cursor.execute(query)

        db_output = self.cursor.fetchone()

        self._disconnect()

        # get the output in a dictionary format
        try:
            output = {
                "host_id": int(host_id),
                "host_uid": str(db_output[0]),
                "host_add": str(db_output[1]),
                "port": int(db_output[2]),
                "user": str(db_output[3]),
                "password": str(db_output[4]),
            }
        except (TypeError, ValueError):
            output = False

        return output

    def host_read_list(self) -> list:
        """List all host ids in the database

        Args:
            None

        Returns:
            list: List of host ids in the database
        """

        # connect to the database
        self._connect()

        # build query to get ID_HOST and FK_EQUIPMENT_RFDB from HOST table
        query = "SELECT ID_HOST, FK_EQUIPMENT_RFDB, NA_HOST_UID " "FROM HOST;"

        self.cursor.execute(query)

        host_ids = []
        for row in self.cursor.fetchall():
            try:
                host_id = int(row[0])
            except (TypeError, ValueError):
                message = f"Error parsing host_id from database: {row}"
                self.log.error(message)
                raise Exception(message)
            try:
                equipment_id = int(row[1])
            except (TypeError, ValueError):
                equipment_id = None
                pass
            host_uid = row[2]

            host_ids = host_ids + [(host_id, equipment_id, host_uid)]

        self._disconnect()

        return host_ids

    def host_update(
        self,
        host_id: int,
        equipment_id: int = None,
        reset: bool = False,
        host_files: int = None,
        pending_host_check: int = None,
        host_check_error: int = None,
        pending_backup: int = None,
        backup_error: int = None,
        pending_processing: int = None,
        processing_error: int = None,
        status: int = None,
    ) -> None:
        """This method set/update summary information in the database

        Args:
            host_id (int): Zabbix host id primary key.
            equipment_id (int): Equipment id primary key. Default is None.
            reset (bool): If set to True, reset all values to the given values. Default is False.
            host_files (int): Number of files in the host. Default to none, where there will be no change to this value.
            pending_host_check (int): Number of files pending backup. Default to none, where there will be no change to this value.
            host_check_error (int): Number of files with backup error. Default to none, where there will be no change to this value.
            pending_backup (int): Number of files pending backup. Default to none, where there will be no change to this value.
            backup_error (int): Number of files with backup error. Default to none, where there will be no change to this value.
            pending_processing (int): Number of files pending processing. Default to none, where there will be no change to this value.
            processing_error (int): Number of files with processing error. Default to none, where there will be no change to this value.
            status (int): Status flag: 0=No Errors or Warnings, 1=No daemon, 2=Halt flag alert. Default to none, where there will be no change to this value.

        """

        # compose and excecute query to update the processing status by adding pending_processing variable to existing value in the database
        query_parts = []

        update_data = {
            "NU_HOST_FILES": host_files,
            "NU_PENDING_HOST_TASK": pending_host_check,
            "NU_HOST_CHECK_ERROR": host_check_error,
            "NU_PENDING_BACKUP": pending_backup,
            "NU_BACKUP_ERROR": backup_error,
            "NU_PENDING_PROCESSING": pending_processing,
            "NU_PROCESSING_ERROR": processing_error,
            "NU_STATUS": status,
        }

        for column, value in update_data.items():
            if value is None:
                continue
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
            query = (
                query
                + f", FK_EQUIPMENT_RFDB = {equipment_id} WHERE ID_HOST = {host_id};"
            )
        else:
            query = query + f" WHERE ID_HOST = {host_id};"

        try:
            # connect to the database
            self._connect()

            self.cursor.execute(query)
            self.db_connection.commit()

        except Exception as e:
            message = f"Error updating host {host_id} status in the database: {e}"
            self.log.error(message)
            raise Exception(message) from e

        finally:
            self._disconnect()

    # Method to delete a host
    def host_delete(self, host_id: int) -> None:
        """This method deletes a host from the database

        Args:
            host_id (int):  Host id primary key.
        """

        # compose and excecute query to delete the host from the database
        query = f"DELETE FROM HOST WHERE ID_HOST = {host_id};"

        try:
            # connect to the database
            self._connect()

            self.cursor.execute(query)
            self.db_connection.commit()

        except Exception as e:
            message = f"Error deleting host {host_id} from the database: {e}"
            self.log.error(message)
            raise Exception(message) from e

        finally:
            self._disconnect()

    # Method add a new host to the backup queue
    def host_task_create(
        self,
        task_type: int,
        host_id: str,
        host_uid: str,
        host_addr: str,
        host_port: str,
        host_user: str,
        host_passwd: str,
    ) -> None:
        """This adds a host task to the database.
            If the host does not exist, create it.

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
        self.host_create(host_id, host_uid)

        try:
            query = (
                f"UPDATE HOST SET "
                f"NA_HOST_ADDRESS = '{host_addr}', "
                f"NA_HOST_PORT = '{host_port}', "
                f"NA_HOST_USER = '{host_user}', "
                f"NA_HOST_PASSWORD = '{host_passwd}', "
                f"NU_PENDING_HOST_TASK = NU_PENDING_HOST_TASK + 1 "
                f"WHERE ID_HOST = '{host_id}';"
            )

            # update database
            self._connect()
            self.cursor.execute(query)
            self.db_connection.commit()

            # Check if there are any host task for the given host
            query = (
                f"SELECT ID_HOST_TASK "
                f"FROM HOST_TASK "
                f"WHERE FK_HOST = '{host_id}';"
            )

            self.cursor.execute(query)
            task = self.cursor.fetchone()

            try:
                task_id = int(task[0])
            except (TypeError, ValueError):
                task_id = False

            if task_id:
                # reset task timestamp so that the queue can move on.
                query = (
                    f"UPDATE HOST_TASK SET "
                    f"DT_HOST_TASK = NOW(), NU_STATUS = {self.TASK_PENDING}, NA_MESSAGE = 'Refreshed', NU_PID = 0 "
                    f"WHERE ID_HOST_TASK = {task_id};"
                )

                # update database
                self.cursor.execute(query)
                self.db_connection.commit()

            else:
                # set a new host task
                query = (
                    f"INSERT INTO HOST_TASK "
                    f"(FK_HOST, NU_TYPE, DT_HOST_TASK, NU_STATUS, NA_MESSAGE) "
                    f"VALUES "
                    f"('{host_id}', '{task_type}', NOW(), {self.TASK_PENDING}, 'New task');"
                )

                # update database
                self.cursor.execute(query)
                self.db_connection.commit()

                # compose query to add 1 to PENDING_BACKUP in the HOST table in BPDATA database for the given host_id
                self.host_update(host_id=host_id, pending_host_check=1)

        except Exception as e:
            message = f"Error adding host {host_id} to the backup queue: {e}"
            self.log.error(message)
            raise Exception(message) from e
        finally:
            self._disconnect()

    # get next host in the list for data backup
    def host_task_read(self, task_id: int = None) -> dict:
        """This method get the information for a given task_id or the oldest task, if no task_id is provided

        Args:
            task_id (int): Optional. If set, get information for task with the given ID

        Returns:
            dict: Dictionary with the pending task information:
                {   "task_id": (int),
                    "host_id": (int),
                    "host_uid": (str),
                    "host_add": (str),
                    "port": (int),
                    "user": (str),
                    "password": (str)}
        """

        # connect to the database
        self._connect()

        query = (
            "SELECT "
            "HOST_TASK.ID_HOST_TASK, "
            "HOST_TASK.FK_HOST, "
            "HOST.NA_HOST_UID, "
            "HOST.NA_HOST_ADDRESS, "
            "HOST.NA_HOST_PORT, "
            "HOST.NA_HOST_USER, "
            "HOST.NA_HOST_PASSWORD "
            "FROM HOST_TASK "
            "JOIN HOST ON HOST_TASK.FK_HOST = HOST.ID_HOST "
        )

        # if no task_id was provided, get the oldest
        if not task_id:
            query = query + (
                "WHERE HOST_TASK.NU_STATUS = 1 ORDER BY DT_HOST_TASK LIMIT 1;"
            )
        else:
            query = query + (
                f"WHERE ID_HOST_TASK = {task_id} AND HOST_TASK.NU_STATUS = 1;"
            )

        self.cursor.execute(query)

        task = self.cursor.fetchone()
        self._disconnect()

        try:
            output = {
                "task_id": int(task[0]),
                "host_id": int(task[1]),
                "host_uid": str(task[2]),
                "host_add": str(task[3]),
                "port": int(task[4]),
                "user": str(task[5]),
                "password": str(task[6]),
            }
        except (TypeError, ValueError):
            output = False

        return output

    def host_task_read_list(self, status: int) -> dict:
        """List all host tasks of a given status

        Returns:
            dict: {host_id: [task_id, task_id, ...], ...}
        """

        # connect to the database
        self._connect()

        # compose query to get ID_HOST_TASK and FK_HOST from the HOST_TASK table when NU_STATUS is equal to the given status
        query = (
            f"SELECT ID_HOST_TASK, FK_HOST FROM HOST_TASK WHERE NU_STATUS = {status};"
        )

        self.cursor.execute(query)

        task_list = self.cursor.fetchall()

        output = {}
        for task in task_list:
            try:
                task_id = int(task[0])
                host_id = int(task[1])
            except (TypeError, ValueError):
                # if keys are not numeric, try next item in the list.
                continue

            try:
                output[host_id].append(task_id)
            except KeyError:
                output[host_id] = [task_id]

        self._disconnect()

        return output

    def host_task_update(
        self, task_id: int, status: int = None, message: str = None, pid: int = None
    ) -> None:
        """Update host task information

        Args:
            task_id (int): Task ID
            status (int): New status
        """

        # compose and excecute query to update task information
        query = "UPDATE HOST_TASK SET "

        if status:
            query = query + f"NU_STATUS = '{status}', "

            match status:
                case self.TASK_PENDING:
                    query = query + "NU_PID = NULL, "
                case self.TASK_RUNNING:
                    if pid:
                        query = query + f"NU_PID = {pid}, "
                    else:
                        query = query + f"NU_PID = {self.log.pid}, "
        else:
            if pid:
                query = query + f"NU_PID = {pid}, "

        if message:
            message = message.replace("'", "''")
            query = query + f"NA_MESSAGE = '{message}', "

        query = query[:-2] + f" WHERE ID_HOST_TASK = {task_id};"

        # connect to the database
        self._connect()

        self.cursor.execute(query)
        self.db_connection.commit()

        self._disconnect()

    # Method to remove a completed backup task from the database
    def host_task_delete(self, task_id: int) -> None:
        """This method removes a host task from the database

        Args:
            task_id: Task ID
        """

        # connect to the database
        self._connect()

        # compose query to get the host_id from the BKPDATA database
        query = f"SELECT FK_HOST " f"FROM HOST_TASK " f"WHERE ID_HOST_TASK = {task_id};"

        self.cursor.execute(query)

        try:
            host_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            self._disconnect()
            raise Exception(
                f"Error retrieving host_id for task_id {task_id} from database"
            )

        # compose and excecute query to delete the backup task from the BKPDATA database
        query = f"DELETE FROM HOST_TASK " f"WHERE ID_HOST_TASK = {task_id};"

        self.cursor.execute(query)

        # update database statistics for the host
        query = (
            f"UPDATE HOST "
            f"SET NU_PENDING_HOST_TASK = NU_PENDING_HOST_TASK - 1, "
            f"DT_LAST_HOST_CHECK = NOW() "
            f"WHERE ID_HOST = '{host_id}';"
        )
        self.cursor.execute(query)
        self.db_connection.commit()

        self._disconnect()

    # Method to add a new processing task to the database
    def file_task_create(
        self,
        host_id: int,
        task_type: int,
        volume: str,
        files: any,
        task_status: int = 1,
        reset_processing_queue: bool = False,
    ) -> None:
        """This method adds file tasks

        Args:
            host_id (int): Zabbix host id primary key.
            task_type (int): Task type: 0=Not set; 1=Backup; 2=Processing
            volume (str): Volume name
            files (list or set): List strings corresponding to path/filenames or set of tuples (path, filename)
            task_status (int): Task status: -1=Error, 0=Nothing to do, 1=Pending action, 2=Under execution. Default to 1.
            reset_processing_queue (bool): Flag to reset the processing queue. Default to False.

        Returns:
            None
        """

        # convert list of filenames with path into tuples (hostid, task_type, path, filename)
        if isinstance(files, list):
            files_tuple_list = [
                (os.path.dirname(item), os.path.basename(item)) for item in files
            ]
        elif isinstance(files, set):
            files_tuple_list = [(filepath, filename) for (filepath, filename) in files]
        else:
            raise Exception(
                "Invalid input. Expected a list strings or a set of tuples."
            )

        if volume == k.REPO_UID:
            target_columns = "NA_SERVER_FILE_PATH, NA_SERVER_FILE_NAME, "
        else:
            target_columns = "NA_HOST_FILE_PATH, NA_HOST_FILE_NAME, "

        # compose query to set the process task in the database using executemany method
        query = (
            f"INSERT INTO FILE_TASK ("
            f"FK_HOST, "
            f"NU_TYPE, "
            f"{target_columns}"
            f"DT_FILE_TASK, "
            f"NU_STATUS) "
            f"VALUES ("
            f"{host_id}, "
            f"{task_type}, "
            f"%s, %s, "
            f"NOW(), "
            f"{task_status});"
        )

        try:
            # update database
            self._connect()

            self.cursor.executemany(query, files_tuple_list)
            self.db_connection.commit()

            if reset_processing_queue:
                # compose query to find how many database entries are in the file task with status = -1 for the given host_id
                query = (
                    f"SELECT COUNT(*) "
                    f"FROM FILE_TASK "
                    f"WHERE FK_HOST = {host_id} AND "
                    f"NU_STATUS = -2;"
                )

                self.cursor.execute(query)

                # get the number of processing errors to all for a full host status reset
                try:
                    processing_error = int(self.cursor.fetchone()[0])
                except (TypeError, ValueError):
                    processing_error = 0
            else:
                processing_error = 0
        except Exception as e:
            message = f"Error adding file task for host {host_id} to the database: {e}"
            self.log.error(message)
            raise Exception(message) from e
        finally:
            self._disconnect()

        # update PENDING_PROCESSING in the HOST table in BPDATA database for the given host_id
        nu_processing = len(files_tuple_list) if files_tuple_list else 0

        new_status = {"host_id": host_id, "reset": reset_processing_queue}
        match task_type:
            case 1:
                new_status["pending_backup"] = nu_processing
                new_status["backup_error"] = processing_error
            case 2:
                new_status["pending_processing"] = nu_processing
                new_status["processing_error"] = processing_error

        self.host_update(**new_status)

    def file_task_create_from_file(self, file_set: set) -> None:
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
        for filepath, filename in file_set:
            match = pattern.search(filename)
            if not match:
                host_uid = input(
                    f"Host UID not found in '{filename}'. Please type host UID or press enter to skip: "
                )  # TODO Include function to delete files in the subset with empty key
            else:
                host_uid = match.group(0)

            try:
                if host_uid not in subsets:
                    subsets[host_uid] = set()

                subsets[host_uid].add((filepath, filename))
            except Exception as e:
                self.log.entry(
                    f"Ignoring '{filepath}/{filename}'. No host_uid defined. Error: {e}"
                )
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
            query = (
                f"SELECT ID_HOST " f"FROM HOST " f"WHERE NA_HOST_UID = '{host_uid}';"
            )

            self._connect()
            self.cursor.execute(query)

            try:
                host_id = int(self.cursor.fetchone()[0])
            except TypeError:
                host_id = input(
                    f"Host '{host_uid}' not found in database. Please enter host ID (Zabbix HOST_ID number) or press enter to skip this host: "
                )

                try:
                    host_id = int(host_id)
                except ValueError:
                    self.log.entry(
                        f"Host '{host_uid}' not found in database and no valid HOST ID was informed. Skipping host."
                    )
                    continue

                self.host_create(host_id, host_uid)

                self.log.entry(
                    f"Host '{host_uid}' created in the database with ID {host_id}"
                )

            # TODO: #26 Harmonize file_list format with file_task_create method

            self.file_task_create(
                host_id=host_id,
                task_type=self.FILE_TASK_PROCESS_TYPE,
                volume=k.REPO_UID,
                files=file_set,
                reset_processing_queue=True,
            )

            self.log.entry(
                f"Added {len(file_set)} files from host {host_uid} to the processing queue"
            )

        self._disconnect()

    # Method to get next host in the list for processing
    def file_task_read_one(self, task_type: int, task_status: int = 1) -> dict:
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
        query = (
            f"SELECT "
            f"FILE_TASK.ID_FILE_TASK, "
            "FILE_TASK.FK_HOST, HOST.NA_HOST_UID, "
            "FILE_TASK.NA_HOST_FILE_PATH, FILE_TASK.NA_HOST_FILE_NAME, "
            "FILE_TASK.NA_SERVER_FILE_PATH, FILE_TASK.NA_SERVER_FILE_NAME "
            f"FROM FILE_TASK "
            f"JOIN HOST ON FILE_TASK.FK_HOST = HOST.ID_HOST "
            f"WHERE "
            f"FILE_TASK.NU_STATUS = {task_status} AND "
            f"FILE_TASK.NU_TYPE = {task_type} "
            f"ORDER BY FILE_TASK.DT_FILE_TASK "
            f"LIMIT 1;"
        )

        self.cursor.execute(query)

        task = self.cursor.fetchone()

        try:
            output = {
                "task_id": int(task[0]),
                "host_id": int(task[1]),
                "host_uid": str(task[2]),
                "host path": str(task[3]),
                "host file": str(task[4]),
                "server path": str(task[5]),
                "server file": str(task[6]),
            }
        except (TypeError, ValueError):
            output = None

        self._disconnect()

        return output

    # Method to retrieve multiple file tasks
    def file_task_read_list_one_host(
        self, task_type: int, task_status: int = 1, limit: int = None
    ) -> dict:
        """Return list of file tasks associated with a host with the oldest pending file task. Return false if no task is found

        Args:
            type (int): Task type
            task_status (int): Task status: -1=Error, 0=Nothing to do, 1=Pending action, 2=Under execution. Default to 1.
            limit (int): Number of tasks to retrieve. Default to None will return all available

        Returns:
            dict:  {"host_id": (int) host_id,
                    "file_tasks": (dict){ task_id: [
                                                    host_file_path,
                                                    host_file_name,
                                                    server_file_path,
                                                    server_file_name]}
        """

        def _join_lists(host_list: list, server_list: list, host_message: str) -> dict:
            """This method joins the host and server lists into a single list with matching task_id
            Necessary to handle cases were file might be missing in one repository or another.

            Args:
                host_list (list): list with task_id, host_file_path and host_file_name
                server_list (list): list with task_id, server_file_path and server_file_name

            Returns:
                dict: Dictionary with the following structure:
                    {file_tasks : {task_id: [host_file_path, host_file_name, server_file_path, server_file_name]}}
            """

            # check if the host_list is larger then the server list
            if len(host_list) >= len(server_list):
                base_list = host_list
                plus_list = server_list
            else:
                base_list = server_list
                plus_list = host_list

            # Create a result dictionary from the base list
            result = {}
            try:
                for item in base_list:
                    result[int(item[0])] = [item[1], item[2], None, None]
            except (TypeError, ValueError):
                self.log.error(f"Error parsing next file tasks for {host_message}")
                pass

            for item in plus_list:
                try:
                    key = int(item[0])
                except (TypeError, ValueError):
                    self.log.error(
                        f"Error parsing task id while getting next file tasks: {item} for {host_message}"
                    )
                    continue
                try:
                    result[key][2] = item[1]
                    result[key][3] = item[2]
                except KeyError:
                    result[int(item[0])] = [None, None, item[1], item[2]]

            return {"file_tasks": result}

        host_task = self.file_task_read_one(
            task_type=task_type, task_status=task_status
        )

        try:
            # build a query to change NU_STATUS to 2 (under execution) of all tasks where:
            query = (
                f"UPDATE FILE_TASK SET "
                f"NU_STATUS = 2, "
                f"NU_PID = {self.log.pid} "
                f"WHERE "
                f"FK_HOST = {host_task['host_id']} AND "
                f"NU_STATUS = {self.TASK_PENDING} AND "
                f"NU_TYPE = {task_type};"
            )

            # connect to the database
            self._connect()
            self.cursor.execute(query)
            self.db_connection.commit()

            # build a query to retrieve list of HOST files associated with the oldest task
            query_host_files = (
                f"SELECT "
                f"ID_FILE_TASK, "
                f"NA_HOST_FILE_PATH, NA_HOST_FILE_NAME "
                f"FROM FILE_TASK "
                f"WHERE "
                f"NU_STATUS = {self.TASK_RUNNING} AND "
                f"NU_TYPE = {task_type} AND "
                f"FK_HOST = {host_task['host_id']} "
                f"ORDER BY DT_FILE_TASK"
            )

            if limit:
                query_host_files = query_host_files + f" LIMIT {limit};"
            else:
                query_host_files = query_host_files + ";"

            self.cursor.execute(query_host_files)

            host_file_tasks = self.cursor.fetchall()

            # build a query to retrieve list of SERVER files associated with the oldest task
            query_server_files = (
                f"SELECT "
                f"ID_FILE_TASK, "
                f"NA_SERVER_FILE_PATH, NA_SERVER_FILE_NAME "
                f"FROM FILE_TASK "
                f"WHERE "
                f"NU_STATUS = 2 AND "
                f"NU_TYPE = {task_type} AND "
                f"FK_HOST = {host_task['host_id']} "
                f"ORDER BY DT_FILE_TASK"
            )

            if limit:
                query_server_files = query_server_files + f" LIMIT {limit};"
            else:
                query_server_files = query_server_files + ";"

            self.cursor.execute(query_server_files)

            server_file_tasks = self.cursor.fetchall()

            # combine server and host file list
            output = _join_lists(
                host_list=host_file_tasks,
                server_list=server_file_tasks,
                host_message=f"host '{host_task['host_uid']}' (id: host_id {host_task['host_id']}).",
            )

            output["host_id"] = host_task["host_id"]
            output["host_uid"] = host_task["host_uid"]

        except (TypeError, ValueError, IndexError):
            # if no task is found, return False
            output = False

        return output

    def file_task_read_list_all(self, task_type: int, task_status: int) -> dict:
        """List all file tasks for a given status and type grouping then by the host_id key

        Returns:
            dict: {host_id: [task_id, task_id, ...], ...}
        """

        # connect to the database
        self._connect()

        # compose query to get ID_FILE_TASK and FK_HOST from FILE_TASK table where NU_STATUS is equal to the given status and NU_TYPE is equal to the given type
        query = f"SELECT ID_FILE_TASK, FK_HOST FROM FILE_TASK WHERE NU_STATUS = {task_status} AND NU_TYPE = {task_type};"

        self.cursor.execute(query)

        task_list = self.cursor.fetchall()

        output = {}
        for task in task_list:
            try:
                task_id = int(task[0])
                host_id = int(task[1])
            except (TypeError, ValueError):
                # if keys are not numeric, try next item in the list.
                continue

            try:
                output[host_id].append(task_id)
            except KeyError:
                output[host_id] = [task_id]

        self._disconnect()

        return output

    def file_task_update(
        self,
        task_id: int,
        host_path: str = None,
        host_file: str = None,
        server_path: str = None,
        server_file: str = None,
        task_type: int = None,
        status: int = None,
        message: str = None,
    ) -> None:
        """Set processing task as completed with error

        Args:
            task_id (int): Task id
            host_path (str): Host path
            host_file (str): Host file
            server_path (str): Server path
            server_file (str): Server file
            task_type (int): Task type: 0=Not set; 1=Backup; 2=Processing
            status (int): Status flag: -2=Suspended, -1=Error, 0=Nothing to do, 1=Pending action, 2=Under execution
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
            query = query + f"NU_TYPE = {task_type}, "
        if status:
            query = query + f"NU_STATUS = {status}, "

            match status:
                case self.TASK_PENDING:
                    query = query + "NU_PID = NULL, "
                case self.TASK_RUNNING:
                    query = query + f"NU_PID = {self.log.pid}, "
        if message:
            message = message.replace("'", "''")
            query = query + f"NA_MESSAGE = '{message}', "

        query = query[:-2] + f" WHERE ID_FILE_TASK = {task_id};"

        # connect to the database
        self._connect()

        self.cursor.execute(query)
        self.db_connection.commit()

        self._disconnect()

    # Method to set processing task as completed with success
    def file_task_delete(
        self,
        task_id: int,
    ) -> None:
        """Set processing task as completed with success

        Args:
            task (dict): Dictionary including the following
            equipment_ids (dict): Dictionary with the equipment ids associated with the processed files
        """

        # get host_id from the database for the given task_id
        self._connect()

        query = f"SELECT FK_HOST " f"FROM FILE_TASK " f"WHERE ID_FILE_TASK = {task_id};"

        self.cursor.execute(query)

        try:
            host_id = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            self.log.error(
                f"Error retrieving host_id for task_id {task_id} from database"
            )
            self._disconnect()
            raise Exception(
                f"Error retrieving host_id for task_id {task_id} from database"
            )

        # compose and excecute query to delete the processing task from the BPDATA database
        query = f"DELETE FROM FILE_TASK " f"WHERE ID_FILE_TASK = {task_id};"

        self.cursor.execute(query)

        self.db_connection.commit()

        self._disconnect()

        self.host_update(host_id=host_id, pending_processing=-1)

    def list_rfdb_files(self) -> set:
        """List files in DIM_SPECTRUM_FILE table that are associated with k.REPO_UID

        Args:
            None

        Returns:
            set: Set of tuples with file name and path of files RFDATA database
        """

        # Query to get files from DIM_SPECTRUM_FILE
        query = (
            f"SELECT "
            f"NA_PATH, "
            f"NA_FILE "
            f"FROM "
            f"DIM_SPECTRUM_FILE "
            f"WHERE "
            f"NA_VOLUME = '{k.REPO_UID}'"
        )

        # connect to the database
        self._connect()

        self.cursor.execute(query)

        try:
            db_files = set((row[0], row[1]) for row in self.cursor.fetchall())
        except Exception as e:
            self.log.error(f"Error retrieving files from database: {e}")
            db_files = set()

        self._disconnect()

        return db_files

    def remove_rfdb_files(self, files_to_remove: set) -> None:
        """Remove files in DIM_SPECTRUM_FILE table that match files_not_in_repo set

        Args:
            files_not_in_repo (set): Set of tuples with file name and path of files not in the repository

        Returns:
            None
        """

        # connect to the database
        self._connect()

        user_input = input("Do you wish to confirm each entry before deletion? (y/n): ")
        if user_input.lower() == "y":
            ask_berfore = True
        else:
            ask_berfore = False

        for filename, path in files_to_remove:
            try:
                if ask_berfore:
                    user_input = input(f"Delete {path}/{filename}? (y/n): ")
                    if user_input.lower() != "y":
                        files_to_remove.pop((filename, path))
                        continue

                # Find ID_FILE
                query = (
                    f"SELECT "
                    f"ID_FILE "
                    f"FROM "
                    f"DIM_SPECTRUM_FILE "
                    f"WHERE "
                    f"NA_FILE = '{filename}' AND "
                    f"NA_PATH = '{path}' AND "
                    f"NA_VOLUME = '{k.REPO_UID}'"
                )

                self.cursor.execute(query)

                id_file = int(self.cursor.fetchone()[0])

                # Exclua a linha correspondente na tabela BRIDGE_SPECTRUM_FILE
                query = (
                    f"DELETE FROM "
                    f"BRIDGE_SPECTRUM_FILE "
                    f"WHERE "
                    f"FK_FILE = {id_file}"
                )

                self.cursor.execute(query)

                # Exclua a linha correspondente na tabela DIM_SPECTRUM_FILE
                query = (
                    "DELETE FROM "
                    f"DIM_SPECTRUM_FILE "
                    f"WHERE "
                    f"ID_FILE = {id_file}"
                )

                self.cursor.execute(query)

                self.db_connection.commit()

                self.log.entry(f"Removed {path}/{filename} from database")
            except Exception as e:
                self.log.error(f"Error removing {path}/{filename} from database: {e}")
                pass

        self._disconnect()

        return None

    def list_bpdb_files(self, task_status: int, task_type: int) -> set:
        """List files in FILE_TASK table that match a given status

        Args:
            status (int): Status flag: 0=Not executed; -1=Executed with error

        Returns:
            set: Set of tuples with file name and path of files in the database
        """

        # Query to get files from DIM_SPECTRUM_FILE
        query = (
            f"SELECT "
            f"NA_SERVER_FILE_PATH, "
            f"NA_SERVER_FILE_NAME "
            f"FROM "
            f"FILE_TASK "
            f"WHERE "
            f"NU_STATUS = {task_status} AND "
            f"NU_TYPE = {task_type}"
        )

        # connect to the database
        self._connect()

        self.cursor.execute(query)

        try:
            db_files = set((row[0], row[1]) for row in self.cursor.fetchall())
        except Exception as e:
            self.log.error(f"Error retrieving files from database: {e}")
            db_files = set()

        self._disconnect()

        return db_files

    def remove_bpdb_files(self, files_to_remove: set) -> None:
        """Remove files in FILE_TASK table from files_to_remove set

        Args:
            files_to_remove (set): Set of tuples with file name and path of files not in the repository, to be removed from the database

        Returns:
            None
        """

        # connect to the database
        self._connect()

        user_input = input("Do you wish to confirm each entry before deletion? (y/n): ")
        if user_input.lower() == "y":
            ask_berfore = True
        else:
            ask_berfore = False

        for path, filename in files_to_remove:
            try:
                if ask_berfore:
                    user_input = input(f"Delete {path}/{filename}? (y/n): ")
                    if user_input.lower() != "y":
                        files_to_remove.pop((filename, path))
                        continue

                query = (
                    f"DELETE FROM "
                    f"FILE_TASK "
                    f"WHERE "
                    f"NA_SERVER_FILE_NAME = '{filename}' AND "
                    f"NA_SERVER_FILE_PATH = '{path}'"
                )

                self.cursor.execute(query)

                self.db_connection.commit()

                self.log.entry(f"Removed {path}/{filename} from database")
            except Exception as e:
                self.log.error(f"Error removing {path}/{filename} from database: {e}")
                pass

        self._disconnect()

        return None

    def count_rfm_host_files(self, equipment_id: int, volume: str = None) -> int:
        # TODO #27 Fix equipment id reset in RFM database
        # connect to the database
        self._connect()

        # build query to get the number of files in the database, given an equipment_id and a volume. Make use of BRIDGE_SPECTRUM_EQUIPMENT to get the FK_SPECTRUM keys and from there, count the DIN_SPECTRUM_FILE entries with the given volume and FK_SPECTRUM using the BRIDGE_SPECTRUM_FILE table.
        query = (
            f"SELECT COUNT(DISTINCT DIM_SPECTRUM_FILE.NA_FILE) "
            f"FROM BRIDGE_SPECTRUM_FILE "
            f"JOIN BRIDGE_SPECTRUM_EQUIPMENT "
            f"ON BRIDGE_SPECTRUM_FILE.FK_SPECTRUM = BRIDGE_SPECTRUM_EQUIPMENT.FK_SPECTRUM "
            f"JOIN DIM_SPECTRUM_FILE "
            f"ON BRIDGE_SPECTRUM_FILE.FK_FILE = DIM_SPECTRUM_FILE.ID_FILE "
            f"WHERE "
            f"BRIDGE_SPECTRUM_EQUIPMENT.FK_EQUIPMENT = {equipment_id}"
        )

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

    def count_bp_host_files(self, host_id: int) -> int:
        # connect to the database
        self._connect()

        # build query to get the number of files in the database for the given host_id, both to any value of NA_VOLUME and for an specific value defined in volume
        query = (
            f"SELECT COUNT(*) "
            f"FROM FILE_TASK "
            f"WHERE "
            f"FK_HOST = {host_id} AND "
            f"NU_STATUS = 0;"
        )

        self.cursor.execute(query)

        try:
            pending_processing = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            pending_processing = 0

        query = (
            f"SELECT COUNT(*) "
            f"FROM FILE_TASK "
            f"WHERE "
            f"FK_HOST = {host_id} AND "
            f"NU_STATUS = -2;"
        )

        self.cursor.execute(query)

        try:
            error_processing = int(self.cursor.fetchone()[0])
        except (TypeError, ValueError):
            error_processing = 0

        self._disconnect()

        return (pending_processing, error_processing)
