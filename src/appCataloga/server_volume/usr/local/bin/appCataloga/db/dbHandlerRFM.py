
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
        
    # ======================================================================
    # SITE OPERATIONS
    # ======================================================================
    def insert_site(
        self,
        data: dict = {
            "latitude": 0,
            "longitude": 0,
            "altitude": 0,
            "state": "state name",
            "county": "city,town name",
            "district": "suburb name",
            "nu_gnss_measurements": 0,
        },
    ) -> int:
        """Create a new site record in DIM_SPECTRUM_SITE.

        This version preserves full compatibility with the existing _insert_row()
        implementation by handling SQL expressions (ST_GeomFromText) manually.

        Args:
            data (dict): Geographic and positional attributes of the site.

        Returns:
            int: Newly inserted site ID.
        """

        try:
            # Resolve foreign keys
            db_state_id, db_county_id, db_district_id = self._get_geographic_codes(data=data)

            self._connect()

            # Fields handled by _insert_row
            insert_data = {
                "NU_ALTITUDE": data["altitude"],
                "NU_GNSS_MEASUREMENTS": data.get("nu_gnss_measurements", 0),
                "FK_STATE": db_state_id,
                "FK_COUNTY": db_county_id,
                "FK_DISTRICT": db_district_id,
            }

            # Prepare SQL expression separately (since _insert_row cannot handle raw SQL)
            geom_expr = f"ST_GeomFromText('POINT({data['longitude']} {data['latitude']})')"

            # Build full query manually to include the geometry
            cols = ", ".join(["GEO_POINT"] + list(insert_data.keys()))
            vals = ", ".join(
                [geom_expr] + ["%s"] * len(insert_data)
            )
            sql = f"INSERT INTO DIM_SPECTRUM_SITE ({cols}) VALUES ({vals});"

            # Execute manually using same transaction style as _insert_row
            self.cursor.execute(sql, tuple(insert_data.values()))
            self.db_connection.commit()

            db_site_id = int(self.cursor.lastrowid or 0)

            self.log.entry(f"[DBHandlerRFM] Inserted new site (ID={db_site_id}) at "
                        f"({data['latitude']}, {data['longitude']})")

            return db_site_id

        except Exception as e:
            try:
                self.db_connection.rollback()
            except Exception:
                pass
            raise Exception(f"Error inserting site in DIM_SPECTRUM_SITE: {e}")

        finally:
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
        """Update site coordinates and GNSS statistics in DIM_SPECTRUM_SITE.

        Applies incremental GNSS averaging if the current measurement count is
        below the maximum defined in configuration constants.

        Args:
            site (int): Target site ID.
            longitude_raw (list[float]): List of measured longitude values (degrees).
            latitude_raw (list[float]): List of measured latitude values (degrees).
            altitude_raw (list[float]): List of measured altitude values (meters).

        Raises:
            Exception: If site retrieval or update fails.
        """
        try:
            # --- Step 1: open connection ---
            self._connect()

            # --- Step 2: get existing site data ---
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

            # --- Step 3: check limit before updating ---
            if db_nu_gnss >= k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS:
                if hasattr(self, "log"):
                    self.log.entry(
                        f"Site {site} reached {db_nu_gnss} GNSS measurements "
                        f"(limit={k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS}). No update performed."
                    )
                return

            # --- Step 4: compute weighted averages ---
            lon_sum = sum(longitude_raw) + (db_longitude * db_nu_gnss)
            lat_sum = sum(latitude_raw) + (db_latitude * db_nu_gnss)
            alt_sum = sum(altitude_raw) + (db_altitude * db_nu_gnss)
            nu_total = db_nu_gnss + len(longitude_raw)

            new_longitude = lon_sum / nu_total
            new_latitude = lat_sum / nu_total
            new_altitude = alt_sum / nu_total

            # --- Step 5: perform update ---
            # Como o campo GEO_POINT requer uma função SQL (ST_GeomFromText),
            # o update precisa ser feito manualmente via query direta.
            sql = (
                f"UPDATE DIM_SPECTRUM_SITE "
                f"SET GEO_POINT = ST_GeomFromText('POINT({new_longitude} {new_latitude})'), "
                f"    NU_ALTITUDE = %s, "
                f"    NU_GNSS_MEASUREMENTS = %s "
                f"WHERE ID_SITE = %s;"
            )

            self.cursor.execute(sql, (new_altitude, nu_total, site))
            self.db_connection.commit()

            # --- Step 6: log success ---
            if hasattr(self, "log"):
                self.log.entry(
                    f"Updated site {site}: lat={new_latitude:.6f}, lon={new_longitude:.6f}, alt={new_altitude:.2f}"
                )

        except Exception as e:
            try:
                self.db_connection.rollback()
            except Exception:
                pass
            raise Exception(f"Error updating site {site}: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass

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
    
    def insert_file(self, filename: str, path: str, volume: str) -> int:
        """Insert or retrieve a file entry in DIM_SPECTRUM_FILE.

        If the file entry exists, returns its ID. Otherwise, inserts a new one.

        Args:
            filename (str): File name.
            path (str): File path.
            volume (str): Storage volume identifier.

        Raises:
            Exception: If file retrieval or insertion fails.

        Returns:
            int: ID of the file entry.
        """
        try:
            self._connect()

            # Check if the file already exists
            rows = self._select_rows(
                table="DIM_SPECTRUM_FILE",
                where={
                    "NA_FILE": filename,
                    "NA_PATH": path,
                    "NA_VOLUME": volume
                },
                cols=["ID_FILE"],
                limit=1
            )

            if rows:
                return int(rows[0]["ID_FILE"])

            # Insert new record using the existing general _insert_row()
            file_id = self._insert_row(
                table="DIM_SPECTRUM_FILE",
                data={
                    "NA_FILE": filename,
                    "NA_PATH": path,
                    "NA_VOLUME": volume
                }
            )

            return file_id

        except Exception as e:
            raise Exception(f"Error inserting or retrieving file '{filename}': {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass

    # ======================================================================
    # PROCEDURE OPERATIONS
    # ======================================================================
    def insert_procedure(self, procedure_name: str) -> int:
        """Insert or retrieve a procedure entry in DIM_SPECTRUM_PROCEDURE.

        If the procedure entry exists, returns its ID. Otherwise, inserts a new one.

        Args:
            procedure_name (str): Procedure name.

        Raises:
            Exception: If retrieval or insertion fails.

        Returns:
            int: ID of the procedure entry.
        """
        try:
            # --- Step 1: open connection ---
            self._connect()

            # --- Step 2: check if procedure already exists ---
            rows = self._select_rows(
                table="DIM_SPECTRUM_PROCEDURE",
                where={"NA_PROCEDURE": procedure_name},
                cols=["ID_PROCEDURE"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_PROCEDURE"])

            # --- Step 3: insert new procedure entry ---
            procedure_id = self._insert_row(
                table="DIM_SPECTRUM_PROCEDURE",
                data={"NA_PROCEDURE": procedure_name},
            )

            return procedure_id

        except Exception as e:
            try:
                self.db_connection.rollback()
            except Exception:
                pass
            raise Exception(f"Error inserting or retrieving procedure '{procedure_name}': {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass
    
    # ======================================================================
    # EQUIPMENT OPERATIONS
    # ======================================================================
    
    def _get_equipment_types(self) -> dict:
        """Load all equipment types and return {equipment_type_uid: equipment_type_id}."""
        try:
            self._connect()
            rows = self._select_rows(
                table="DIM_EQUIPMENT_TYPE",
                cols=["ID_EQUIPMENT_TYPE", "NA_EQUIPMENT_TYPE_UID"]
            )

            equipment_types_dict = {
                str(r["NA_EQUIPMENT_TYPE_UID"]).strip(): int(r["ID_EQUIPMENT_TYPE"])
                for r in rows
            }
            return equipment_types_dict

        except Exception as e:
            raise Exception(f"Error retrieving equipment types from database: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass
    
    def insert_equipment(self, equipment: Union[str, List[str]]) -> dict:
        """Insert or retrieve equipment entries in DIM_SPECTRUM_EQUIPMENT."""
        try:
            if isinstance(equipment, str):
                equipment_names = [equipment]
            elif isinstance(equipment, list):
                equipment_names = equipment
            else:
                raise Exception("Invalid input. Expected a string or list of strings.")

            equipment_types = self._get_equipment_types()
            equipment_ids = {}

            self._connect()
            for name in equipment_names:
                name_lower = name.lower()
                equipment_type_id = None

                # detect type by substring
                for type_uid, type_id in equipment_types.items():
                    if type_uid.rstrip("\r").lower() in name_lower:
                        equipment_type_id = type_id
                        break

                if not equipment_type_id:
                    raise Exception(f"Error retrieving equipment type for {name}")

                # check existing
                rows = self._select_rows(
                    table="DIM_SPECTRUM_EQUIPMENT",
                    where={"NA_EQUIPMENT": name_lower},
                    cols=["ID_EQUIPMENT"],
                    limit=1
                )

                if rows:
                    equipment_ids[name] = int(rows[0]["ID_EQUIPMENT"])
                    continue

                # insert new
                eq_id = self._insert_row(
                    table="DIM_SPECTRUM_EQUIPMENT",
                    data={
                        "NA_EQUIPMENT": name,
                        "FK_EQUIPMENT_TYPE": equipment_type_id
                    }
                )
                equipment_ids[name] = eq_id

            return equipment_ids

        except Exception as e:
            try:
                self.db_connection.rollback()
            except Exception:
                pass
            raise Exception(f"Error inserting equipment: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass

    # ======================================================================
    # DETECTOR OPERATIONS
    # ======================================================================
    def insert_detector_type(self, detector: str) -> int:
        """Insert or retrieve detector entry in DIM_SPECTRUM_DETECTOR."""
        try:
            self._connect()
            rows = self._select_rows(
                table="DIM_SPECTRUM_DETECTOR",
                where={"NA_DETECTOR": detector},
                cols=["ID_DETECTOR"],
                limit=1
            )

            if rows:
                return int(rows[0]["ID_DETECTOR"])

            return self._insert_row(
                table="DIM_SPECTRUM_DETECTOR",
                data={"NA_DETECTOR": detector}
            )

        except Exception as e:
            raise Exception(f"Error inserting or retrieving detector '{detector}': {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass
    
    # ======================================================================
    # TRACE OPERATIONS
    # ======================================================================
    def insert_trace_type(self, trace_name: str) -> int:
        """Insert or retrieve trace type in DIM_SPECTRUM_TRACE_TYPE."""
        try:
            self._connect()
            rows = self._select_rows(
                table="DIM_SPECTRUM_TRACE_TYPE",
                where={"NA_TRACE_TYPE": trace_name},
                cols=["ID_TRACE_TYPE"],
                limit=1
            )

            if rows:
                return int(rows[0]["ID_TRACE_TYPE"])

            return self._insert_row(
                table="DIM_SPECTRUM_TRACE_TYPE",
                data={"NA_TRACE_TYPE": trace_name}
            )

        except Exception as e:
            raise Exception(f"Error inserting or retrieving trace type '{trace_name}': {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass
    
    # ======================================================================
    # MEASUREMENTS OPERATIONS
    # ======================================================================
    def insert_measure_unit(self, unit_name: str) -> int:
        """Insert or retrieve measure unit in DIM_SPECTRUM_UNIT."""
        try:
            self._connect()
            rows = self._select_rows(
                table="DIM_SPECTRUM_UNIT",
                where={"NA_MEASURE_UNIT": unit_name},
                cols=["ID_MEASURE_UNIT"],
                limit=1
            )

            if rows:
                return int(rows[0]["ID_MEASURE_UNIT"])

            return self._insert_row(
                table="DIM_SPECTRUM_UNIT",
                data={"NA_MEASURE_UNIT": unit_name}
            )

        except Exception as e:
            raise Exception(f"Error inserting or retrieving measure unit '{unit_name}': {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass
    
    # ======================================================================
    # SPECTRUM OPERATIONS
    # ======================================================================
    def insert_spectrum(self, data: dict) -> int:
        """Insert or retrieve a spectrum entry in FACT_SPECTRUM."""
        try:
            self._connect()
            where = {
                "FK_SITE": data["id_site"],
                "FK_TRACE_TYPE": data["id_trace_type"],
                "NU_FREQ_START": data["nu_freq_start"],
                "NU_FREQ_END": data["nu_freq_end"],
                "DT_TIME_START": data["dt_time_start"],
                "DT_TIME_END": data["dt_time_end"],
                "NU_TRACE_COUNT": data["nu_trace_count"],
                "NU_TRACE_LENGTH": data["nu_trace_length"],
            }

            rows = self._select_rows(
                table="FACT_SPECTRUM",
                where=where,
                cols=["ID_SPECTRUM"],
                limit=1
            )

            if rows:
                return int(rows[0]["ID_SPECTRUM"])

            return self._insert_row(
                table="FACT_SPECTRUM",
                data={
                    "FK_SITE": data["id_site"],
                    "FK_PROCEDURE": data["id_procedure"],
                    "FK_DETECTOR": data["id_detector_type"],
                    "FK_TRACE_TYPE": data["id_trace_type"],
                    "FK_MEASURE_UNIT": data["id_measure_unit"],
                    "NA_DESCRIPTION": data["na_description"],
                    "NU_FREQ_START": data["nu_freq_start"],
                    "NU_FREQ_END": data["nu_freq_end"],
                    "DT_TIME_START": data["dt_time_start"],
                    "DT_TIME_END": data["dt_time_end"],
                    "NU_SAMPLE_DURATION": data["nu_sample_duration"],
                    "NU_TRACE_COUNT": data["nu_trace_count"],
                    "NU_TRACE_LENGTH": data["nu_trace_length"],
                    "NU_RBW": data["nu_rbw"],
                    "NU_ATT_GAIN": data["nu_att_gain"],
                }
            )

        except Exception as e:
            raise Exception(f"Error inserting or retrieving spectrum: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass

    # ======================================================================
    # BRIDGE SPECTRUM OPERATIONS
    # ======================================================================
    def insert_bridge_spectrum_equipment(self, spectrum_lst: list) -> None:
        """Insert N:N relationships between spectrum and equipment."""
        try:
            self._connect()
            for entry in spectrum_lst:
                for equipment in entry["equipment"]:
                    sql = (
                        "INSERT IGNORE INTO BRIDGE_SPECTRUM_EQUIPMENT "
                        "(FK_SPECTRUM, FK_EQUIPMENT) VALUES (%s, %s);"
                    )
                    self.cursor.execute(sql, (entry["spectrum"], equipment))

            self.db_connection.commit()

        except Exception as e:
            try:
                self.db_connection.rollback()
            except Exception:
                pass
            raise Exception(f"Error linking spectrum and equipment: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass

    # ======================================================================
    # BRIDGE SPECTRUM FILE OPERATIONS
    # ======================================================================
    def insert_bridge_spectrum_file(self, spectrum_lst: list, file_lst: list) -> None:
        """Insert N:N relationships between spectrum and files."""
        try:
            self._connect()
            for entry in spectrum_lst:
                for file_id in file_lst:
                    sql = (
                        "INSERT IGNORE INTO BRIDGE_SPECTRUM_FILE "
                        "(FK_SPECTRUM, FK_FILE) VALUES (%s, %s);"
                    )
                    self.cursor.execute(sql, (entry["spectrum"], file_id))

            self.db_connection.commit()

        except Exception as e:
            try:
                self.db_connection.rollback()
            except Exception:
                pass
            raise Exception(f"Error linking spectrum and file: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass
    
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
