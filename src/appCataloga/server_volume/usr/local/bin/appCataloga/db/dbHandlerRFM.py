
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dbHandlerRFM_refactored_doc_v2.py
---------------------------------

High-level handler focused on *RFM (Radio Frequency / Spectrum)* domain for the
appCataloga ecosystem. This module centralizes database interactions for
spectrum-related entities (hosts, files, tasks, and measurements), reusing the
generic CRUD helpers provided by `DBHandlerBase`.

This version contains **complete Google-Style docstrings** and **technical
comments**, keeping logic minimal and consistent with the project's architecture:
- No raw SQL string interpolation with user data (parameterized queries only)
- Scoped transactions with explicit commit/rollback
- Connect/Disconnect safety via try/finally
- No constant mirroring; constants are read directly from `config as k`

"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime
import pandas as pd


import config as k
from .dbHandlerBase import DBHandlerBase


class dbHandlerRFM(DBHandlerBase):
    """RFM domain handler for spectrum hosts, files, tasks and measurements.

    This class exposes a focused API to manage spectrum ingestion and processing
    workflows. It delegates all low-level SQL operations to `DBHandlerBase`.
    """
    
    # ======================================================================
    # Initialization
    # ======================================================================
    def __init__(self, database: str, log: Any) -> None:
        """Initialize the handler with the target database key and logger.

        Args:
            database (str): Logical key resolved by `config.DB`.
            log (Any): Logger implementing `.entry()`, `.warning()`, `.error()`.
        """
        super().__init__(database=database, log=log)
        self.log.entry(f"[dbHandlerRFM] Initialized for DB '{database}'")
        
    
    def begin_transaction(self) -> None:
        """
        Begin a database transaction.

        Must be called by the service layer before performing
        multiple dependent operations (e.g. processing one BIN file).
        """
        self._connect()
        self.db_connection.autocommit = False
        self.in_transaction = True


    def commit(self) -> None:
        """
        Commit the current transaction.

        Only effective if a transaction is active.
        """
        if self.in_transaction:
            self.db_connection.commit()
            self.in_transaction = False


    def rollback(self) -> None:
        """
        Roll back the current transaction.

        Used when any error occurs during a transactional workflow.
        """
        if self.in_transaction:
            self.db_connection.rollback()
            self.in_transaction = False
            
    def _ensure_transaction(self):
        """
        Ensure that the current connection is operating
        with autocommit disabled.

        This is required because dbHandlerBase._connect()
        enforces autocommit=True.
        """
        if self.in_transaction:
            try:
                self.db_connection.autocommit = False
            except Exception:
                pass
 
        
    # ======================================================================
    # SITE OPERATIONS
    # ======================================================================
    def insert_site(self, data: dict) -> int:
        """
        Insert a new site into DIM_SPECTRUM_SITE.

        The geographic point (GEO_POINT) is inserted using a raw SQL expression
        (ST_GeomFromText), while all other fields use parameter binding.

        This method is transaction-aware:
        • If called inside an active transaction, commit is deferred
        • If called standalone, changes are committed immediately
        """

        if not isinstance(data, dict):
            raise ValueError("data must be a dict")

        try:
            # --------------------------------------------------
            # Resolve geographic foreign keys
            # --------------------------------------------------
            db_state_id, db_county_id, db_district_id = (
                self._get_geographic_codes(data=data)
            )

            self._connect()

            insert_data = {
                "NU_ALTITUDE": data["altitude"],
                "NU_GNSS_MEASUREMENTS": data.get("nu_gnss_measurements", 0),
                "FK_STATE": db_state_id,
                "FK_COUNTY": db_county_id,
                "FK_DISTRICT": db_district_id,
            }

            geom_expr = (
                f"ST_GeomFromText("
                f"'POINT({data['longitude']} {data['latitude']})'"
                f")"
            )

            cols = ", ".join(["GEO_POINT"] + list(insert_data.keys()))
            vals = ", ".join([geom_expr] + ["%s"] * len(insert_data))

            sql = f"""
                INSERT INTO DIM_SPECTRUM_SITE ({cols})
                VALUES ({vals});
            """

            self.cursor.execute(sql, tuple(insert_data.values()))

            # --------------------------------------------------
            # Commit only if NOT inside a managed transaction
            # --------------------------------------------------
            if not self.in_transaction:
                self.db_connection.commit()

            site_id = int(self.cursor.lastrowid)

            if hasattr(self, "log"):
                self.log.entry(
                    f"[DBHandlerRFM] Inserted site ID={site_id} "
                    f"({data['latitude']}, {data['longitude']})"
                )

            return site_id

        except Exception as e:
            if self.in_transaction:
                raise
            try:
                self.db_connection.rollback()
            except Exception:
                pass
            raise Exception(f"Error inserting site in DIM_SPECTRUM_SITE: {e}")

        finally:
            if not self.in_transaction:
                try:
                    self._disconnect()
                except Exception:
                    pass



    def update_site(
        self,
        site: int,
        longitude_raw: list[float],
        latitude_raw: list[float],
        altitude_raw: list[float],
    ) -> None:
        """
        Update geographic coordinates and GNSS statistics of an existing site.

        The site location is updated using weighted averages based on previous
        measurements stored in the database and new raw GNSS samples provided
        by the current acquisition.

        Updates are skipped when the configured maximum number of GNSS
        measurements is reached.

        Args:
            site (int):
                ID_SITE of the record in DIM_SPECTRUM_SITE to be updated.

            longitude_raw (list[float]):
                List of longitude samples (in degrees) from the current BIN file.

            latitude_raw (list[float]):
                List of latitude samples (in degrees) from the current BIN file.

            altitude_raw (list[float]):
                List of altitude samples (in meters) from the current BIN file.

        Raises:
            Exception:
                If the site does not exist or if the update operation fails.
        """

        try:
            # --------------------------------------------------
            # 1) Open / reuse connection
            # --------------------------------------------------
            self._connect()

            # IMPORTANT: this is a WRITE-capable function
            self._ensure_transaction()

            # --------------------------------------------------
            # 2) Read current site state
            # --------------------------------------------------
            rows = self._select_rows(
                table="DIM_SPECTRUM_SITE",
                where={"ID_SITE": site},
                cols=[
                    "ST_X(GEO_POINT) AS LONGITUDE",
                    "ST_Y(GEO_POINT) AS LATITUDE",
                    "NU_ALTITUDE",
                    "NU_GNSS_MEASUREMENTS",
                ],
                limit=1,
            )

            if not rows:
                raise Exception(f"Site {site} not found in DIM_SPECTRUM_SITE")

            site_data = rows[0]
            db_longitude = float(site_data["LONGITUDE"])
            db_latitude = float(site_data["LATITUDE"])
            db_altitude = float(site_data["NU_ALTITUDE"])
            db_nu_gnss = int(site_data["NU_GNSS_MEASUREMENTS"])

            # --------------------------------------------------
            # 3) Check GNSS limit
            # --------------------------------------------------
            if db_nu_gnss >= k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS:
                if hasattr(self, "log"):
                    self.log.entry(
                        f"Site {site} reached {db_nu_gnss} GNSS measurements "
                        f"(limit={k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS}). No update performed."
                    )
                return

            # --------------------------------------------------
            # 4) Compute weighted averages
            # --------------------------------------------------
            lon_sum = sum(longitude_raw) + (db_longitude * db_nu_gnss)
            lat_sum = sum(latitude_raw) + (db_latitude * db_nu_gnss)
            alt_sum = sum(altitude_raw) + (db_altitude * db_nu_gnss)
            nu_total = db_nu_gnss + len(longitude_raw)

            new_longitude = lon_sum / nu_total
            new_latitude = lat_sum / nu_total
            new_altitude = alt_sum / nu_total

            # --------------------------------------------------
            # 5) Perform UPDATE (NO COMMIT HERE)
            # --------------------------------------------------
            sql = (
                "UPDATE DIM_SPECTRUM_SITE "
                "SET GEO_POINT = ST_GeomFromText(%s), "
                "    NU_ALTITUDE = %s, "
                "    NU_GNSS_MEASUREMENTS = %s "
                "WHERE ID_SITE = %s;"
            )

            wkt_point = f"POINT({new_longitude} {new_latitude})"
            self.cursor.execute(sql, (wkt_point, new_altitude, nu_total, site))

            if hasattr(self, "log"):
                self.log.entry(
                    f"Updated site {site}: "
                    f"lat={new_latitude:.6f}, "
                    f"lon={new_longitude:.6f}, "
                    f"alt={new_altitude:.2f}"
                )

        except Exception as e:
            # DO NOT rollback here – let caller decide
            raise Exception(f"Error updating site {site}: {e}")


    def get_site_id(self, data: dict) -> int | bool:
        """Get site database id based on the coordinates in the data dictionary.

        Retrieves the nearest site from DIM_SPECTRUM_SITE using _select_rows and
        checks whether it lies within the GNSS deviation threshold.

        Args:
            data (dict): {"latitude": float, "longitude": float}
                Site information with required coordinates.

        Raises:
            Exception: Error retrieving location coordinates from database.

        Returns:
            int | bool: ID_SITE if location is valid and within deviation,
                        otherwise False.
        """
        try:
            # Ensure connection before querying
            self._connect()

            # Prepare computed columns
            cols = [
                "ID_SITE",
                "ST_X(GEO_POINT) AS LONGITUDE",
                "ST_Y(GEO_POINT) AS LATITUDE",
                f"ST_Distance_Sphere(GEO_POINT, ST_GeomFromText('POINT({data['longitude']} {data['latitude']})', 4326)) AS DISTANCE"
            ]

            # Select nearest site using helper
            rows = self._select_rows(
                table="DIM_SPECTRUM_SITE",
                order_by="DISTANCE ASC",
                limit=1,
                cols=cols
            )

            if not rows:
                return False

            nearest = rows[0]

            nearest_site_id = int(nearest["ID_SITE"])
            nearest_longitude = float(nearest["LONGITUDE"])
            nearest_latitude = float(nearest["LATITUDE"])

            # Validate coordinate proximity
            near_in_longitude = abs(data["longitude"] - nearest_longitude) < k.MAXIMUM_GNSS_DEVIATION
            near_in_latitude = abs(data["latitude"] - nearest_latitude) < k.MAXIMUM_GNSS_DEVIATION
            location_exist_in_db = near_in_latitude and near_in_longitude

            if location_exist_in_db:
                return nearest_site_id
            else:
                return False

        except Exception as e:
            raise Exception(f"Error retrieving location coordinates from database: {e}")

        finally:
            # Always close connection
            try:
                self._disconnect()
            except Exception:
                pass


    def _get_geographic_codes(self, data: dict) -> Tuple[int, int, int]:
        """Retrieve or create DB keys for state, county, and district based on input data.

        Uses dbHandlerBase-style _select_rows and _insert_row for consistent access.

        Args:
            data (dict): Dictionary with region names.
                Example:
                    {
                        "state": "Distrito Federal",
                        "county": "Brasília",
                        "district": "Asa Sul"
                    }

        Raises:
            Exception: If state or county lookup fails.

        Returns:
            Tuple[int, int, int]: (db_state_id, db_county_id, db_district_id)
        """

        try:
            self._connect()

            # -----------------------------------------------------------
            # 1️) Lookup STATE
            # -----------------------------------------------------------
            rows = self._select_rows(
                table="DIM_SITE_STATE",
                where={"NA_STATE": data["state"]},
                cols=["ID_STATE"],
                limit=1,
            )

            if not rows:
                raise Exception(f"State '{data['state']}' not found in DIM_SITE_STATE")

            db_state_id = int(rows[0]["ID_STATE"])

            # -----------------------------------------------------------
            # 2️) Lookup COUNTY
            # -----------------------------------------------------------
            if db_state_id == 53:
                # Special case: Federal District (no counties)
                db_county_id = 5300108
            else:
                rows = self._select_rows(
                    table="DIM_SITE_COUNTY",
                    where={"NA_COUNTY": data["county"], "FK_STATE": db_state_id},
                    cols=["ID_COUNTY"],
                    limit=1,
                )

                if not rows:
                    raise Exception(
                        f"County '{data['county']}' not found for state ID {db_state_id}"
                    )

                db_county_id = int(rows[0]["ID_COUNTY"])

            # -----------------------------------------------------------
            # 3️) Lookup or Insert DISTRICT
            # -----------------------------------------------------------
            rows = self._select_rows(
                table="DIM_SITE_DISTRICT",
                where={"NA_DISTRICT": data["district"], "FK_COUNTY": db_county_id},
                cols=["ID_DISTRICT"],
                limit=1,
            )

            if rows:
                db_district_id = int(rows[0]["ID_DISTRICT"])
            else:
                db_district_id = self._insert_row(
                    table="DIM_SITE_DISTRICT",
                    data={
                        "FK_COUNTY": db_county_id,
                        "NA_DISTRICT": data["district"],
                    },
                )

            # -----------------------------------------------------------
            # 4) Return collected keys
            # -----------------------------------------------------------
            return db_state_id, db_county_id, db_district_id

        except Exception as e:
            raise Exception(f"Error retrieving geographic codes: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass

    # ======================================================================
    # FILE OPERATIONS
    # ======================================================================
    def build_path(self, site_id: int) -> str:
        """Build the path to the site folder in the format "LC_STATE/county_id/site_id".

        Args:
            site_id (int): DB key of the site.

        Raises:
            Exception: If site path retrieval fails.

        Returns:
            str: Formatted path string.
        """
        try:
            self._connect()

            # Use the standard select helper
            rows = self._select_rows(
                table="DIM_SPECTRUM_SITE "
                    "JOIN DIM_SITE_STATE ON DIM_SPECTRUM_SITE.FK_STATE = DIM_SITE_STATE.ID_STATE",
                where={"DIM_SPECTRUM_SITE.ID_SITE": site_id},
                cols=[
                    "DIM_SITE_STATE.LC_STATE",
                    "DIM_SPECTRUM_SITE.FK_COUNTY",
                    "DIM_SPECTRUM_SITE.ID_SITE"
                ],
                limit=1
            )

            if not rows:
                raise Exception(f"Site ID {site_id} not found in DIM_SPECTRUM_SITE")

            site_info = rows[0]
            new_path = f"{site_info['LC_STATE']}/{site_info['FK_COUNTY']}/{site_info['ID_SITE']}"
            return new_path

        except Exception as e:
            raise Exception(f"Error building path for site {site_id}: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass
    def get_file_type_id_by_hostname(
        self,
        HOSTNAME: str,
    ) -> int:
        """
        Resolve ID_TYPE_FILE from DIM_FILE_TYPE based on hostname matching.

        Matching rule:
            DIM_FILE_TYPE.NA_EQUIPMENT must be a substring of hostname (case-insensitive).

        Example:
            hostname = 'rfeye002106'
            matches NA_EQUIPMENT = 'rfeye'

        Args:
            HOSTNAME (str):
                Hostname of the equipment (e.g. 'rfeye002106').

        Returns:
            int:
                ID_TYPE_FILE

        Raises:
            Exception:
                • If no file type matches hostname
                • If more than one file type matches (ambiguous)
        """

        if not isinstance(HOSTNAME, str) or not HOSTNAME:
            raise ValueError("HOSTNAME must be non-empty str")

        hostname = HOSTNAME.lower()

        self._connect()
        try:
            rows = self._select_rows(
                table="DIM_FILE_TYPE",
                cols=["ID_TYPE_FILE", "NA_TYPE_FILE", "NA_EQUIPMENT"],
            )

            matches = []

            for r in rows:
                na_equipment = str(r["NA_EQUIPMENT"]).lower()

                if na_equipment and na_equipment in hostname:
                    matches.append(r)

            if not matches:
                raise Exception(
                    f"No matching file type found for hostname '{HOSTNAME}'"
                )

            if len(matches) > 1:
                details = ", ".join(
                    f"{m['NA_TYPE_FILE']}({m['NA_EQUIPMENT']})" for m in matches
                )
                raise Exception(
                    f"Ambiguous file type for hostname '{HOSTNAME}': {details}"
                )

            return int(matches[0]["ID_TYPE_FILE"])

        finally:
            if not self.in_transaction:
                self._disconnect()

    def insert_file(
        self,
        hostname: str,
        NA_PATH: str,
        NA_FILE: str,
        NA_VOLUME: str,
        NA_EXTENSION: str | None = None,
        VL_FILE_SIZE_KB: int | None = None,
        DT_FILE_CREATED: datetime | None = None,
        DT_FILE_MODIFIED: datetime | None = None,
    ) -> int:
        """
        Insert or retrieve a file entry in DIM_SPECTRUM_FILE.

        Uniqueness:
            (NA_VOLUME, NA_PATH, NA_FILE)

        Transaction behavior:
            • If called inside an active transaction, insertion is deferred
            • If called standalone, changes are committed immediately
        """

        # ------------------------------------------------------
        # Validation
        # ------------------------------------------------------
        if not isinstance(hostname, str) or not hostname:
            raise ValueError("hostname must be non-empty str")

        if not isinstance(NA_VOLUME, str) or not NA_VOLUME:
            raise ValueError("NA_VOLUME must be non-empty str")

        if not isinstance(NA_PATH, str) or not NA_PATH:
            raise ValueError("NA_PATH must be non-empty str")

        if not isinstance(NA_FILE, str) or not NA_FILE:
            raise ValueError("NA_FILE must be non-empty str")

        self._connect()
        try:
            # --------------------------------------------------
            # Resolve file type
            # --------------------------------------------------
            ID_TYPE_FILE = self.get_file_type_id_by_hostname(
                HOSTNAME=hostname
            )

            # --------------------------------------------------
            # Lookup existing file
            # --------------------------------------------------
            rows = self._select_rows(
                table="DIM_SPECTRUM_FILE",
                where={
                    "NA_VOLUME": NA_VOLUME.lower(),
                    "NA_PATH": NA_PATH,
                    "NA_FILE": NA_FILE,
                },
                cols=["ID_FILE"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_FILE"])

            # --------------------------------------------------
            # Insert new file
            # --------------------------------------------------
            file_id = self._insert_row(
                table="DIM_SPECTRUM_FILE",
                data={
                    "ID_TYPE_FILE": ID_TYPE_FILE,
                    "NA_VOLUME": NA_VOLUME.lower(),
                    "NA_PATH": NA_PATH,
                    "NA_FILE": NA_FILE,
                    "NA_EXTENSION": NA_EXTENSION,
                    "VL_FILE_SIZE_KB": VL_FILE_SIZE_KB,
                    "DT_FILE_CREATED": DT_FILE_CREATED,
                    "DT_FILE_MODIFIED": DT_FILE_MODIFIED,
                },
            )

            # --------------------------------------------------
            # Commit only if not in a managed transaction
            # --------------------------------------------------
            if not self.in_transaction:
                self.db_connection.commit()

            return int(file_id)

        except Exception as e:
            if not self.in_transaction:
                try:
                    self.db_connection.rollback()
                except Exception:
                    pass
            raise Exception(
                f"insert_file failed for file '{NA_FILE}' in '{NA_PATH}': {e}"
            )

        finally:
            if not self.in_transaction:
                self._disconnect()



    # ======================================================================
    # PROCEDURE OPERATIONS
    # ======================================================================
    def insert_procedure(self, procedure_name: str) -> int:
        """
        Insert or retrieve a procedure entry in DIM_SPECTRUM_PROCEDURE.

        Args:
            procedure_name (str): Name of the acquisition or processing procedure.

        Returns:
            int: ID_PROCEDURE
        """

        if not isinstance(procedure_name, str) or not procedure_name.strip():
            raise ValueError("procedure_name must be a non-empty str")

        self._connect()
        try:
            # --------------------------------------------------
            # Lookup existing procedure
            # --------------------------------------------------
            rows = self._select_rows(
                table="DIM_SPECTRUM_PROCEDURE",
                where={"NA_PROCEDURE": procedure_name},
                cols=["ID_PROCEDURE"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_PROCEDURE"])

            # --------------------------------------------------
            # Insert new procedure
            # --------------------------------------------------
            procedure_id = self._insert_row(
                table="DIM_SPECTRUM_PROCEDURE",
                data={"NA_PROCEDURE": procedure_name},
            )

            # Commit only if not inside a managed transaction
            if not self.in_transaction:
                self.db_connection.commit()

            return int(procedure_id)

        except Exception as e:
            if not self.in_transaction:
                try:
                    self.db_connection.rollback()
                except Exception:
                    pass
            raise Exception(
                f"insert_procedure failed for '{procedure_name}': {e}"
            )

        finally:
            if not self.in_transaction:
                self._disconnect()

    
    # ======================================================================
    # EQUIPMENT OPERATIONS
    # ======================================================================
    
    def _get_equipment_types(self) -> dict:
        """
        Load all equipment types.

        Returns:
            dict:
                {
                    "rfeye": {
                        "id": 1
                    },
                    "etm": {
                        "id": 2
                    },
                    ...
                }
        """
        try:
            self._connect()

            rows = self._select_rows(
                table="DIM_EQUIPMENT_TYPE",
                cols=[
                    "ID_EQUIPMENT_TYPE",
                    "NA_EQUIPMENT_TYPE_UID",
                ],
            )

            equipment_types = {
                str(r["NA_EQUIPMENT_TYPE_UID"]).strip().lower(): {
                    "id": int(r["ID_EQUIPMENT_TYPE"]),
                }
                for r in rows
            }

            return equipment_types

        except Exception as e:
            raise Exception(f"Error retrieving equipment types: {e}")

        finally:
            if not self.in_transaction:
                self._disconnect()

    
    def get_or_create_spectrum_equipment(
        self,
        equipment_name: str
    ) -> int:
        """
        Retrieve or create a spectrum equipment in DIM_SPECTRUM_EQUIPMENT.

        Model:
            • One row per physical equipment
            • No ports or sub-entities

        Args:
            equipment_name (str):
                Logical equipment name (e.g. 'rfeye002106').

        Returns:
            int:
                ID_EQUIPMENT
        """

        if not isinstance(equipment_name, str) or not equipment_name.strip():
            raise ValueError("equipment_name must be a non-empty str")

        name = equipment_name.lower().strip()
        self._connect()

        try:
            # --------------------------------------------------
            # 1) Resolve equipment type
            # --------------------------------------------------
            equipment_types = self._get_equipment_types()
            eq_type = None

            for uid, meta in equipment_types.items():
                if uid in name:
                    eq_type = meta
                    break

            if not eq_type:
                raise Exception(
                    f"Unable to infer equipment type for '{equipment_name}'"
                )

            equipment_type_id = eq_type["id"]

            # --------------------------------------------------
            # 2) Lookup existing equipment
            # --------------------------------------------------
            rows = self._select_rows(
                table="DIM_SPECTRUM_EQUIPMENT",
                where={"NA_EQUIPMENT": name},
                cols=["ID_EQUIPMENT"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_EQUIPMENT"])

            # --------------------------------------------------
            # 3) Insert new equipment
            # --------------------------------------------------
            equipment_id = self._insert_row(
                table="DIM_SPECTRUM_EQUIPMENT",
                data={
                    "FK_EQUIPMENT_TYPE": equipment_type_id,
                    "NA_EQUIPMENT": name,
                },
            )

            # Commit only if not part of a larger transaction
            if not self.in_transaction:
                self.db_connection.commit()

            return int(equipment_id)

        except Exception as e:
            if not self.in_transaction:
                try:
                    self.db_connection.rollback()
                except Exception:
                    pass
            raise Exception(
                f"get_or_create_spectrum_equipment failed for '{equipment_name}': {e}"
            )

        finally:
            if not self.in_transaction:
                self._disconnect()



    # ======================================================================
    # DETECTOR OPERATIONS
    # ======================================================================
    def insert_detector_type(self, detector: str) -> int:
        """
        Insert or retrieve a detector type in DIM_SPECTRUM_DETECTOR.

        Args:
            detector (str):
                Detector name (e.g. 'PMEC').

        Returns:
            int:
                ID_DETECTOR
        """

        if not isinstance(detector, str) or not detector.strip():
            raise ValueError("detector must be a non-empty str")

        detector = detector.strip()
        self._connect()

        try:
            # --------------------------------------------------
            # 1) Lookup existing detector
            # --------------------------------------------------
            rows = self._select_rows(
                table="DIM_SPECTRUM_DETECTOR",
                where={"NA_DETECTOR": detector},
                cols=["ID_DETECTOR"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_DETECTOR"])

            # --------------------------------------------------
            # 2) Insert new detector
            # --------------------------------------------------
            detector_id = self._insert_row(
                table="DIM_SPECTRUM_DETECTOR",
                data={"NA_DETECTOR": detector},
            )

            # Commit only if standalone
            if not self.in_transaction:
                self.db_connection.commit()

            return int(detector_id)

        except Exception as e:
            if not self.in_transaction:
                try:
                    self.db_connection.rollback()
                except Exception:
                    pass
            raise Exception(
                f"insert_detector_type failed for '{detector}': {e}"
            )

        finally:
            if not self.in_transaction:
                self._disconnect()

    
    # ======================================================================
    # MEASUREMENTS OPERATIONS
    # ======================================================================
    def insert_measure_unit(self, unit_name: str) -> int:
        """
        Insert or retrieve a measurement unit in DIM_SPECTRUM_UNIT.

        Args:
            unit_name (str):
                Measurement unit (e.g. 'dBm').

        Returns:
            int:
                ID_MEASURE_UNIT
        """

        if not isinstance(unit_name, str) or not unit_name.strip():
            raise ValueError("unit_name must be a non-empty str")

        unit_name = unit_name.strip()
        self._connect()

        try:
            # --------------------------------------------------
            # 1) Lookup existing measurement unit
            # --------------------------------------------------
            rows = self._select_rows(
                table="DIM_SPECTRUM_UNIT",
                where={"NA_MEASURE_UNIT": unit_name},
                cols=["ID_MEASURE_UNIT"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_MEASURE_UNIT"])

            # --------------------------------------------------
            # 2) Insert new measurement unit
            # --------------------------------------------------
            unit_id = self._insert_row(
                table="DIM_SPECTRUM_UNIT",
                data={"NA_MEASURE_UNIT": unit_name},
            )

            # Commit only if standalone
            if not self.in_transaction:
                self.db_connection.commit()

            return int(unit_id)

        except Exception as e:
            if not self.in_transaction:
                try:
                    self.db_connection.rollback()
                except Exception:
                    pass
            raise Exception(
                f"insert_measure_unit failed for '{unit_name}': {e}"
            )

        finally:
            if not self.in_transaction:
                self._disconnect()

    
    # ======================================================================
    # SPECTRUM OPERATIONS
    # ======================================================================
    def insert_spectrum(self, data: dict) -> int:
        """
        Insert or retrieve a spectrum entry in FACT_SPECTRUM.

        Uniqueness is based on site, equipment, procedure,
        temporal window and spectral range to avoid duplicates
        during retries.

        Args:
            data (dict):
                Fully resolved foreign keys and spectrum metadata.

        Returns:
            int:
                ID_SPECTRUM
        """

        required_keys = (
            "id_site",
            "id_equipment",
            "id_procedure",
            "id_detector_type",
            "id_trace_type",
            "id_measure_unit",
            "nu_freq_start",
            "nu_freq_end",
            "dt_time_start",
            "dt_time_end",
            "nu_trace_length",
        )

        for k in required_keys:
            if k not in data:
                raise ValueError(f"insert_spectrum missing required key: {k}")

        self._connect()
        try:
            # --------------------------------------------------
            # 1) Lookup existing spectrum (idempotency)
            # --------------------------------------------------
            rows = self._select_rows(
                table="FACT_SPECTRUM",
                where={
                    "FK_SITE": data["id_site"],
                    "FK_EQUIPMENT": data["id_equipment"],
                    "FK_PROCEDURE": data["id_procedure"],
                    "FK_TRACE_TYPE": data["id_trace_type"],
                    "NU_FREQ_START": data["nu_freq_start"],
                    "NU_FREQ_END": data["nu_freq_end"],
                    "DT_TIME_START": data["dt_time_start"],
                    "DT_TIME_END": data["dt_time_end"],
                    "NU_TRACE_LENGTH": data["nu_trace_length"],
                },
                cols=["ID_SPECTRUM"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_SPECTRUM"])

            # --------------------------------------------------
            # 2) Normalize JSON metadata
            # --------------------------------------------------
            js_metadata = data.get("js_metadata")
            if isinstance(js_metadata, dict):
                js_metadata = json.dumps(js_metadata)

            # --------------------------------------------------
            # 3) Insert new spectrum
            # --------------------------------------------------
            spectrum_id = self._insert_row(
                table="FACT_SPECTRUM",
                data={
                    "FK_SITE": data["id_site"],
                    "FK_EQUIPMENT": data["id_equipment"],
                    "FK_PROCEDURE": data["id_procedure"],
                    "FK_DETECTOR": data["id_detector_type"],
                    "FK_TRACE_TYPE": data["id_trace_type"],
                    "FK_MEASURE_UNIT": data["id_measure_unit"],
                    "NA_DESCRIPTION": data.get("na_description"),
                    "NU_FREQ_START": data["nu_freq_start"],
                    "NU_FREQ_END": data["nu_freq_end"],
                    "DT_TIME_START": data["dt_time_start"],
                    "DT_TIME_END": data["dt_time_end"],
                    "NU_SAMPLE_DURATION": data.get("nu_sample_duration"),
                    "NU_TRACE_COUNT": data.get("nu_trace_count"),
                    "NU_TRACE_LENGTH": data["nu_trace_length"],
                    "NU_RBW": data.get("nu_rbw"),
                    "NU_VBW": data.get("nu_vbw"),
                    "NU_ATT_GAIN": data.get("nu_att_gain"),
                    "JS_METADATA": js_metadata,
                },
            )

            # Commit only if standalone
            if not self.in_transaction:
                self.db_connection.commit()

            return int(spectrum_id)

        except Exception as e:
            if not self.in_transaction:
                try:
                    self.db_connection.rollback()
                except Exception:
                    pass
            raise Exception(f"insert_spectrum failed: {e}")

        finally:
            if not self.in_transaction:
                self._disconnect()

    
    def insert_trace_type(self, trace_name: str) -> int:
        """
        Insert or retrieve a trace type in DIM_SPECTRUM_TRACE_TYPE.

        Args:
            trace_name (str):
                Trace type name (e.g. 'peak').

        Returns:
            int:
                ID_TRACE_TYPE
        """

        if not isinstance(trace_name, str) or not trace_name.strip():
            raise ValueError("trace_name must be a non-empty str")

        trace_name = trace_name.strip()
        self._connect()

        try:
            # --------------------------------------------------
            # 1) Lookup existing trace type
            # --------------------------------------------------
            rows = self._select_rows(
                table="DIM_SPECTRUM_TRACE_TYPE",
                where={"NA_TRACE_TYPE": trace_name},
                cols=["ID_TRACE_TYPE"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_TRACE_TYPE"])

            # --------------------------------------------------
            # 2) Insert new trace type
            # --------------------------------------------------
            trace_id = self._insert_row(
                table="DIM_SPECTRUM_TRACE_TYPE",
                data={"NA_TRACE_TYPE": trace_name},
            )

            # Commit only if standalone
            if not self.in_transaction:
                self.db_connection.commit()

            return int(trace_id)

        except Exception as e:
            if not self.in_transaction:
                try:
                    self.db_connection.rollback()
                except Exception:
                    pass
            raise Exception(
                f"insert_trace_type failed for '{trace_name}': {e}"
            )

        finally:
            if not self.in_transaction:
                self._disconnect()

    # ======================================================================
    # BRIDGE SPECTRUM OPERATIONS
    # ======================================================================
    def insert_bridge_spectrum_equipment(self, spectrum_lst: list) -> None:
        """
        Insert N:N relationships between spectrum and spectrum-equipment (antenna port).

        Expected input:
            spectrum_lst = [
                {
                    "spectrum": <ID_SPECTRUM>,
                    "equipment": <ID_DIM_SPECTRUM_EQUIPMENT>
                },
                ...
            ]

        Transaction control:
            - No commit
            - No rollback
            - Managed by caller
        """

        self._connect()

        try:
            for entry in spectrum_lst:
                spectrum_id = entry["spectrum"]
                dim_equipment_id = entry["equipment"]

                self.cursor.execute(
                    "INSERT IGNORE INTO BRIDGE_SPECTRUM_EQUIPMENT "
                    "(FK_SPECTRUM, FK_EQUIPMENT) VALUES (%s, %s);",
                    (spectrum_id, dim_equipment_id),
                )

        except Exception as e:
            raise Exception(f"insert_bridge_spectrum_equipment failed: {e}")

        finally:
            if not self.in_transaction:
                self._disconnect()


    # ======================================================================
    # BRIDGE SPECTRUM FILE OPERATIONS
    # ======================================================================
    def insert_bridge_spectrum_file(
        self,
        spectrum_ids: list[int],
        file_ids: list[int],
    ) -> None:
        """
        Insert N:N relationships between spectra and files.

        Model:
            • One file may contain multiple spectra
            • One spectrum may be linked to multiple files (reprocessing)

        Args:
            spectrum_ids (list[int]):
                List of ID_SPECTRUM values.
            file_ids (list[int]):
                List of ID_FILE values.
        """

        if not spectrum_ids or not file_ids:
            return  # nothing to do

        self._connect()
        try:
            for spectrum_id in spectrum_ids:
                for file_id in file_ids:
                    self.cursor.execute(
                        """
                        INSERT IGNORE INTO BRIDGE_SPECTRUM_FILE
                            (FK_SPECTRUM, FK_FILE)
                        VALUES (%s, %s);
                        """,
                        (spectrum_id, file_id),
                    )

            # Commit only if standalone
            if not self.in_transaction:
                self.db_connection.commit()

        except Exception as e:
            if not self.in_transaction:
                try:
                    self.db_connection.rollback()
                except Exception:
                    pass
            raise Exception(f"insert_bridge_spectrum_file failed: {e}")

        finally:
            if not self.in_transaction:
                self._disconnect()

    
    # ======================================================================
    # PARQUET OPERATIONS
    # ======================================================================
    def export_parquet(self, file_name: str) -> None:
        """Export all database tables to Parquet files.

        Each table is exported to a separate file in the format:
            {file_name}.{table_name}.parquet

        Args:
            file_name (str): Base name for output files.

        Raises:
            Exception: If export or query execution fails.
        """
        try:
            self._connect()

            # --- Step 1: listar tabelas ---
            self.cursor.execute("SHOW TABLES;")
            tables = [t[0] for t in self.cursor.fetchall()]

            if not tables:
                raise Exception("No tables found in the current database schema.")

            # --- Step 2: exportar cada tabela ---
            for table_name in tables:
                try:
                    # Buscar todos os dados da tabela
                    query = f"SELECT * FROM {table_name};"
                    self.cursor.execute(query)
                    table_data = self.cursor.fetchall()

                    # Buscar os nomes das colunas
                    self.cursor.execute(f"SHOW COLUMNS FROM {table_name};")
                    columns = [c[0] for c in self.cursor.fetchall()]

                    # Criar DataFrame
                    table_df = pd.DataFrame(table_data, columns=columns)
                    table_df = table_df.fillna("na")

                    # Nome do arquivo parquet
                    composed_file_name = f"{file_name}.{table_name}.parquet"

                    # Exportar para Parquet
                    table_df.to_parquet(composed_file_name)

                    if hasattr(self, "log"):
                        self.log.entry(f"[EXPORT] Table '{table_name}' → {composed_file_name}")

                except Exception as e:
                    if hasattr(self, "log"):
                        self.log.error(f"[EXPORT] Failed exporting '{table_name}': {e}")
                    continue  # não aborta o loop, apenas pula tabela com erro

        except Exception as e:
            raise Exception(f"Error exporting database to parquet: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass


    def get_latest_processing_time(self) -> datetime:
        """Return the latest DT_FILE_LOGGED timestamp from DIM_SPECTRUM_FILE."""
        try:
            self._connect()
            rows = self._select_rows(
                table="DIM_SPECTRUM_FILE",
                cols=["MAX(DT_FILE_LOGGED) AS LATEST"],
                limit=1
            )

            if not rows or not rows[0]["LATEST"]:
                return None

            latest = rows[0]["LATEST"]
            return latest.timestamp() if hasattr(latest, "timestamp") else latest

        except Exception as e:
            raise Exception(f"Error getting latest processing time: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass
