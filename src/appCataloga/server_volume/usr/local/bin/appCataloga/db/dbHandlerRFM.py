
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database handler for RFDATA persistence and publication.
"""

from __future__ import annotations

import json
from typing import Any, Optional, Tuple
from datetime import datetime
import pandas as pd
import unicodedata
import re

import config as k
from .dbHandlerBase import DBHandlerBase


class dbHandlerRFM(DBHandlerBase):
    """Handler for sites, files, dimensions, spectra and Parquet export."""
    
    # ======================================================================
    # Initialization
    # ======================================================================
    def __init__(self, database: str, log: Any) -> None:
        """Initialize the RFDATA handler and its transaction state.

        `DBHandlerBase` owns the connection primitives. This subclass only
        tracks whether the caller opened an explicit transaction that should
        survive multiple helper calls.
        """
        super().__init__(database=database, log=log)
        self.log.entry(f"[dbHandlerRFM] Initialized for DB '{database}'")
        self.in_transaction: bool = False
        
    
    def begin_transaction(self) -> None:
        """Start an explicit transaction for a multi-step ingestion flow.

        The appAnalise worker persists several related rows in sequence. This
        flag tells the handler to defer commits until the outer flow finishes.
        """
        self.in_transaction = True
        self._connect()
        self.db_connection.autocommit = False



    def commit(self) -> None:
        """Commit the active managed transaction, if one exists.

        Standalone helpers still commit on their own. This method is only for
        callers that previously entered `begin_transaction()`.
        """
        if not self.in_transaction:
            return

        try:
            self.db_connection.commit()
        finally:
            # Restore default connection mode for later standalone calls.
            self.db_connection.autocommit = True
            self.in_transaction = False


    def rollback(self) -> None:
        """Roll back the active managed transaction, if one exists.

        This is the symmetric exit path for `begin_transaction()` when any
        part of the ingestion flow fails.
        """
        if not self.in_transaction:
            return

        try:
            self.db_connection.rollback()
        finally:
            # Restore default connection mode for later standalone calls.
            self.db_connection.autocommit = True
            self.in_transaction = False
            
    def _ensure_transaction(self) -> None:
        """Restore `autocommit=False` after reconnects in managed flows.

        Some helpers may call `_connect()` again while a managed transaction is
        already open. This keeps the connection aligned with the outer flow.
        """
        if self.in_transaction:
            try:
                self.db_connection.autocommit = False
            except Exception:
                pass
 
        
    # ======================================================================
    # SITE OPERATIONS
    # ======================================================================
    def _normalize_site_data(self, data: dict) -> dict:
        """Apply cheap cleanup before geographic key resolution.

        This helper only normalizes the payload enough for catalog lookup. It
        does not create rows or call reverse geocoding.
        """

        # If this helper had to open the connection, it should also be the one
        # to close it again unless a managed transaction is active.
        connection_was_open = self.db_connection is not None

        try:
            if not connection_was_open:
                self._connect()

            # Keep the incoming payload stable before matching against the
            # geography dimensions.
            for key in ["state", "county", "district", "site_name"]:
                if data.get(key):
                    data[key] = data[key].strip()

            # When only the county is present, use the existing catalog as the
            # safest place to infer the missing parent state.
            if not data.get("state") and data.get("county"):
                try:
                    rows = self._select_rows(
                        table="DIM_SITE_COUNTY",
                        where={"NA_COUNTY": data["county"]},
                        cols=["FK_STATE"],
                        limit=1,
                    )

                    if rows:
                        state_row = self._select_rows(
                            table="DIM_SITE_STATE",
                            where={"ID_STATE": rows[0]["FK_STATE"]},
                            cols=["NA_STATE"],
                            limit=1,
                        )

                        if state_row:
                            data["state"] = state_row[0]["NA_STATE"]

                except Exception:
                    # Let `_get_geographic_codes()` decide whether the miss is fatal.
                    pass

            return data

        finally:
            if not connection_was_open and not self.in_transaction:
                try:
                    self._disconnect()
                except Exception:
                    pass

    
    def _normalize_string(self, value: str) -> Optional[str]:
        """Normalize geographic text before deterministic comparisons."""
        if not value:
            return None

        value = value.strip().lower()

        # Match catalog names independent of accents, apostrophes and spacing.
        value = unicodedata.normalize("NFKD", value)
        value = "".join(c for c in value if not unicodedata.combining(c))

        value = re.sub(r"[’'`]", "", value)

        value = re.sub(r"\s+", " ", value)

        return value


    def _resolve_site_name(self, data: dict) -> Optional[str]:
        """Choose the display label stored in `NA_SITE`.

        The site row needs one stable human-readable label. Prefer an explicit
        name first, then fall back to the best available locality.
        """
        for key in ["site_name", "district", "county"]:
            value = data.get(key)
            if isinstance(value, str):
                value = value.strip()
            if value:
                return value

        return None



    def insert_site(self, data: dict) -> int:
        """Insert one row into `DIM_SPECTRUM_SITE`.

        The caller provides the centroid, altitude and already-resolved site
        summary. This method resolves the administrative foreign keys and
        persists the geometry row.
        """

        if not isinstance(data, dict):
            raise ValueError("data must be a dict")

        try:
            # Normalize first so geography resolution sees the cleaned payload.
            data = self._normalize_site_data(data)
            
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
                "NA_SITE": self._resolve_site_name(data),
            }

            # Mobile captures may carry a prepared WKT path in addition to the
            # centroid stored in `GEO_POINT`.
            if data.get("geographic_path"):
                insert_data["GEOGRAPHIC_PATH"] = data["geographic_path"]

            # `GEO_POINT` is written as a spatial expression; the remaining
            # columns still use regular parameter binding.
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
        """Update a fixed site's centroid using new GNSS samples.

        This path is only for fixed stations. Mobile rows keep the prepared
        path stable and are not refined here by repeated centroid averaging.
        """

        try:
            # This helper may run inside a larger ingestion transaction, so it
            # must reuse the caller's connection contract instead of opening an
            # autonomous write flow.
            self._connect()
            self._ensure_transaction()

            # Read the current centroid and historical GNSS count before mixing
            # in the new raw samples from the file being processed.
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

            # After enough observations, keep the site stable and stop moving
            # the centroid on every new file.
            if db_nu_gnss >= k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS:
                if hasattr(self, "log"):
                    self.log.entry(
                        f"Site {site} reached {db_nu_gnss} GNSS measurements "
                        f"(limit={k.MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS}). No update performed."
                    )
                return

            # Fold the new samples into the historical centroid using the
            # stored measurement count as the previous weight.
            lon_sum = sum(longitude_raw) + (db_longitude * db_nu_gnss)
            lat_sum = sum(latitude_raw) + (db_latitude * db_nu_gnss)
            alt_sum = sum(altitude_raw) + (db_altitude * db_nu_gnss)
            nu_total = db_nu_gnss + len(longitude_raw)

            new_longitude = lon_sum / nu_total
            new_latitude = lat_sum / nu_total
            new_altitude = alt_sum / nu_total

            # Update only the numeric GNSS aggregates. State/county/district
            # remain untouched once the site row already exists.
            sql = (
                "UPDATE DIM_SPECTRUM_SITE "
                "SET GEO_POINT = ST_GeomFromText(%s), "
                "    NU_ALTITUDE = %s, "
                "    NU_GNSS_MEASUREMENTS = %s "
                "WHERE ID_SITE = %s;"
            )

            wkt_point = f"POINT({new_longitude} {new_latitude})"
            self.cursor.execute(sql, (wkt_point, new_altitude, nu_total, site))

            # Commit stays with the caller when a managed transaction is open.
            if hasattr(self, "log"):
                self.log.entry(
                    f"Updated site {site}: "
                    f"lat={new_latitude:.6f}, "
                    f"lon={new_longitude:.6f}, "
                    f"alt={new_altitude:.2f}"
                )

        except Exception as e:
            raise Exception(f"Error updating site {site}: {e}")


    def get_site_id(self, data: dict) -> int | bool:
        """Return the matching `ID_SITE`, or `False` when none matches.

        The lookup starts from the nearest stored sites and then applies the
        RF.Fusion matching rule: fixed sites match by centroid tolerance;
        mobile sites also require the same stored `GEOGRAPHIC_PATH`.
        """
        try:
            self._connect()

            # Ask the database for the nearest candidates first. The Python
            # side then applies the fixed/mobile matching rules.
            cols = [
                "ID_SITE",
                "ST_X(GEO_POINT) AS LONGITUDE",
                "ST_Y(GEO_POINT) AS LATITUDE",
                "GEOGRAPHIC_PATH",
                f"ST_Distance_Sphere(GEO_POINT, ST_GeomFromText('POINT({data['longitude']} {data['latitude']})', 4326)) AS DISTANCE"
            ]

            rows = self._select_rows(
                table="DIM_SPECTRUM_SITE",
                order_by="DISTANCE ASC",
                limit=20,
                cols=cols
            )

            if not rows:
                return False

            incoming_path = data.get("geographic_path")

            # The nearest row is not necessarily a valid match; a nearby site
            # from a different locality or a different mobile polygon must be
            # rejected.
            for nearest in rows:
                nearest_site_id = int(nearest["ID_SITE"])
                nearest_longitude = float(nearest["LONGITUDE"])
                nearest_latitude = float(nearest["LATITUDE"])
                stored_path = nearest.get("GEOGRAPHIC_PATH")

                if isinstance(stored_path, bytes):
                    stored_path = stored_path.decode("utf-8", errors="replace")

                near_in_longitude = (
                    abs(data["longitude"] - nearest_longitude)
                    < k.MAXIMUM_GNSS_DEVIATION
                )
                near_in_latitude = (
                    abs(data["latitude"] - nearest_latitude)
                    < k.MAXIMUM_GNSS_DEVIATION
                )
                location_exist_in_db = near_in_latitude and near_in_longitude

                if not location_exist_in_db:
                    continue

                # Mobile rows also require the same stored path.
                if incoming_path:
                    if stored_path == incoming_path:
                        return nearest_site_id
                    continue

                if not stored_path:
                    return nearest_site_id

            return False

        except Exception as e:
            raise Exception(f"Error retrieving location coordinates from database: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass


    def _get_geographic_codes(self, data: dict) -> Tuple[int, int, int]:
        """Resolve `FK_STATE`, `FK_COUNTY` and optional `FK_DISTRICT`.

        Matching is deterministic: try the catalog value as-is first, then
        fall back to normalized comparison. County is always resolved within
        the chosen state. District is optional and may be auto-created when
        that policy is enabled.
        """

        try:
            self._connect()

            # State: exact match first, normalized comparison as fallback.
            rows = self._select_rows(
                table="DIM_SITE_STATE",
                where={"NA_STATE": data["state"]},
                cols=["ID_STATE", "NA_STATE"],
                limit=1,
            )

            if not rows:
                # Reverse geocoding often differs only by accents or apostrophe
                # usage, so the fallback stays deterministic instead of fuzzy.
                all_states = self._select_rows(
                    table="DIM_SITE_STATE",
                    cols=["ID_STATE", "NA_STATE"],
                )

                normalized_input = self._normalize_string(data["state"])
                db_state_id = None

                for row in all_states:
                    if self._normalize_string(row["NA_STATE"]) == normalized_input:
                        db_state_id = int(row["ID_STATE"])
                        break

                if not db_state_id:
                    raise Exception(
                        f"State '{data['state']}' not found in DIM_SITE_STATE"
                    )
            else:
                db_state_id = int(rows[0]["ID_STATE"])

            # Federal District uses the synthetic county key from the catalog.
            if db_state_id == 53:
                db_county_id = 5300108
            else:
                normalized_input = self._normalize_string(data["county"])

                # Small map for known OSM/IBGE spelling differences.
                COUNTY_EXCEPTIONS = {
                    "assu": "acu",        # Assu (OSM) → Açu (IBGE)
                    "iguassu": "iguacu",  # Iguassu → Iguaçu
                }

                if normalized_input in COUNTY_EXCEPTIONS:
                    normalized_input = COUNTY_EXCEPTIONS[normalized_input]

                rows = self._select_rows(
                    table="DIM_SITE_COUNTY",
                    where={"FK_STATE": db_state_id},
                    cols=["ID_COUNTY", "NA_COUNTY"],
                )

                db_county_id = None

                # County must be resolved inside the already-chosen state, so
                # two states can safely have the same county name.
                for row in rows:
                    normalized_db = self._normalize_string(row["NA_COUNTY"])
                    if normalized_db == normalized_input:
                        db_county_id = int(row["ID_COUNTY"])
                        break

                if not db_county_id:
                    raise Exception(
                        f"County '{data['county']}' not found for state ID {db_state_id}"
                    )

            db_district_id = None

            if data.get("district"):
                # District is optional; when present, try to reuse the existing
                # county-scoped catalog before considering auto-creation.
                normalized_input = self._normalize_string(data["district"])

                rows = self._select_rows(
                    table="DIM_SITE_DISTRICT",
                    where={"FK_COUNTY": db_county_id},
                    cols=["ID_DISTRICT", "NA_DISTRICT"],
                )

                for row in rows:
                    if self._normalize_string(row["NA_DISTRICT"]) == normalized_input:
                        db_district_id = int(row["ID_DISTRICT"])
                        break

                # Only create after a deterministic miss in the existing catalog.
                if not db_district_id and k.SITE_DISTRICT_AUTO_CREATE:
                    db_district_id = self._insert_row(
                        table="DIM_SITE_DISTRICT",
                        data={
                            "FK_COUNTY": db_county_id,
                            "NA_DISTRICT": data["district"],
                        },
                    )

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
        """Return the canonical repository subpath for one site.

        The path is derived from the site dimension itself so the filesystem
        layout stays aligned with the geographic catalog.
        """
        try:
            self._connect()

            # The path is built from the stored site/state keys, not from any
            # caller-provided geography string.
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
        """Resolve `ID_TYPE_FILE` from hostname substring matching.

        `DIM_FILE_TYPE` stores short equipment markers. This helper finds the
        single marker that matches the given hostname and falls back to
        `others` when no specific match exists.
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

            # File type rows store equipment fragments such as `rfeye` or
            # `cwsm`, so the match is intentionally substring-based.
            for r in rows:
                na_equipment = str(r["NA_EQUIPMENT"]).lower()

                if na_equipment and na_equipment in hostname:
                    matches.append(r)

            if not matches:
                # Keep a deterministic fallback when the hostname is generic or
                # not yet represented in the file type catalog.
                fallback = next(
                    (
                        r for r in rows
                        if str(r["NA_EQUIPMENT"]).strip().lower() == "others"
                    ),
                    None,
                )

                if fallback:
                    return int(fallback["ID_TYPE_FILE"])

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
        """Insert or retrieve one row from `DIM_SPECTRUM_FILE`.

        The file row represents the stored artifact itself. Deduplication is
        based on repository location, while `hostname` is only used to resolve
        the file type dimension.
        """

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
            # Resolve the type before checking file identity so a newly-seen
            # artifact is always inserted with a complete dimension reference.
            ID_TYPE_FILE = self.get_file_type_id_by_hostname(
                HOSTNAME=hostname
            )

            # Repository location is the file identity contract in RFDATA.
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

            # Insert only when this exact repository artifact is still unknown.
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
        """Insert or retrieve one row from `DIM_SPECTRUM_PROCEDURE`.

        Procedure names are reused across many spectra, so this helper keeps
        the dimension idempotent and cheap for repeated worker runs.
        """

        if not isinstance(procedure_name, str) or not procedure_name.strip():
            raise ValueError("procedure_name must be a non-empty str")

        self._connect()
        try:
            # Procedures are identified by name only.
            rows = self._select_rows(
                table="DIM_SPECTRUM_PROCEDURE",
                where={"NA_PROCEDURE": procedure_name},
                cols=["ID_PROCEDURE"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_PROCEDURE"])

            procedure_id = self._insert_row(
                table="DIM_SPECTRUM_PROCEDURE",
                data={"NA_PROCEDURE": procedure_name},
            )

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
        """Load equipment types keyed by normalized UID.

        This is a small lookup helper used by the spectrum equipment resolver
        to infer `FK_EQUIPMENT_TYPE` from the payload receiver name.
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

            # Keep the shape lightweight because callers only need the type id
            # after matching the UID inside the receiver string.
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
        """Insert or retrieve one row from `DIM_SPECTRUM_EQUIPMENT`.

        The payload may name different receivers even inside one processed
        file. This helper keeps equipment identity per receiver string.
        """

        if not isinstance(equipment_name, str) or not equipment_name.strip():
            raise ValueError("equipment_name must be a non-empty str")

        name = equipment_name.lower().strip()
        self._connect()

        try:
            # Equipment type is inferred from the normalized name using the
            # type UID catalog, not from the operational host.
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

            # Once the type is known, equipment identity is the normalized
            # receiver string itself.
            rows = self._select_rows(
                table="DIM_SPECTRUM_EQUIPMENT",
                where={"NA_EQUIPMENT": name},
                cols=["ID_EQUIPMENT"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_EQUIPMENT"])

            equipment_id = self._insert_row(
                table="DIM_SPECTRUM_EQUIPMENT",
                data={
                    "FK_EQUIPMENT_TYPE": equipment_type_id,
                    "NA_EQUIPMENT": name,
                },
            )

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
        """Insert or retrieve one row from `DIM_SPECTRUM_DETECTOR`.

        Detector names are treated as stable dimension values and reused
        across files and equipment.
        """

        if not isinstance(detector, str) or not detector.strip():
            raise ValueError("detector must be a non-empty str")

        detector = detector.strip()
        self._connect()

        try:
            # Detector identity is just the cleaned detector label.
            rows = self._select_rows(
                table="DIM_SPECTRUM_DETECTOR",
                where={"NA_DETECTOR": detector},
                cols=["ID_DETECTOR"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_DETECTOR"])

            detector_id = self._insert_row(
                table="DIM_SPECTRUM_DETECTOR",
                data={"NA_DETECTOR": detector},
            )

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
        """Insert or retrieve one row from `DIM_SPECTRUM_UNIT`.

        Units such as `dBm` and `dBuV` are tiny dimensions but still need
        deduplication because every spectrum references them.
        """

        if not isinstance(unit_name, str) or not unit_name.strip():
            raise ValueError("unit_name must be a non-empty str")

        unit_name = unit_name.strip()
        self._connect()

        try:
            # Measure unit identity is the cleaned unit label.
            rows = self._select_rows(
                table="DIM_SPECTRUM_UNIT",
                where={"NA_MEASURE_UNIT": unit_name},
                cols=["ID_MEASURE_UNIT"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_MEASURE_UNIT"])

            unit_id = self._insert_row(
                table="DIM_SPECTRUM_UNIT",
                data={"NA_MEASURE_UNIT": unit_name},
            )

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
        """Insert or retrieve one row from `FACT_SPECTRUM`.

        The lookup keys are chosen to make worker retries idempotent without
        requiring a separate deduplication pass.
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
            # The fact row is considered the same spectrum when the resolved
            # site/equipment/procedure/time/frequency window already exists.
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

            # Accept either serialized JSON or a dict.
            js_metadata = data.get("js_metadata")
            if isinstance(js_metadata, dict):
                js_metadata = json.dumps(js_metadata)

            # At this point all foreign keys were already resolved by the
            # caller, so this insert stays purely relational.
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
        """Insert or retrieve one row from `DIM_SPECTRUM_TRACE_TYPE`.

        Trace labels are reused heavily, so this helper keeps them normalized
        in one small dimension table.
        """

        if not isinstance(trace_name, str) or not trace_name.strip():
            raise ValueError("trace_name must be a non-empty str")

        trace_name = trace_name.strip()
        self._connect()

        try:
            # Trace identity is just the cleaned trace label.
            rows = self._select_rows(
                table="DIM_SPECTRUM_TRACE_TYPE",
                where={"NA_TRACE_TYPE": trace_name},
                cols=["ID_TRACE_TYPE"],
                limit=1,
            )

            if rows:
                return int(rows[0]["ID_TRACE_TYPE"])

            trace_id = self._insert_row(
                table="DIM_SPECTRUM_TRACE_TYPE",
                data={"NA_TRACE_TYPE": trace_name},
            )

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
    # BRIDGE SPECTRUM FILE OPERATIONS
    # ======================================================================
    def insert_bridge_spectrum_file(
        self,
        spectrum_ids: list[int],
        file_ids: list[int],
    ) -> None:
        """Insert many-to-many links between spectra and files.

        One processed artifact may map to many spectra, and the same spectrum
        may later be linked to more than one file artifact.
        """

        if not spectrum_ids or not file_ids:
            return

        self._connect()
        try:
            # `INSERT IGNORE` keeps retries safe and avoids duplicate bridge
            # rows when the worker replays the same association.
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
        """Export every table from the current schema as Parquet files.

        This is used by the metadata publisher to materialize a filesystem
        snapshot of the current RFDATA schema.
        """
        try:
            self._connect()

            # Export the whole schema table by table so a failure in one table
            # does not discard the others.
            self.cursor.execute("SHOW TABLES;")
            tables = [t[0] for t in self.cursor.fetchall()]

            if not tables:
                raise Exception("No tables found in the current database schema.")

            for table_name in tables:
                try:
                    # Read rows and column names separately so the Parquet file
                    # preserves the live table layout without hardcoded schema.
                    query = f"SELECT * FROM {table_name};"
                    self.cursor.execute(query)
                    table_data = self.cursor.fetchall()

                    self.cursor.execute(f"SHOW COLUMNS FROM {table_name};")
                    columns = [c[0] for c in self.cursor.fetchall()]

                    table_df = pd.DataFrame(table_data, columns=columns)
                    table_df = table_df.fillna("na")

                    composed_file_name = f"{file_name}.{table_name}.parquet"

                    table_df.to_parquet(composed_file_name)

                    if hasattr(self, "log"):
                        self.log.entry(f"[EXPORT] Table '{table_name}' → {composed_file_name}")

                except Exception as e:
                    if hasattr(self, "log"):
                        self.log.error(f"[EXPORT] Failed exporting '{table_name}': {e}")
                    # Keep exporting the remaining tables.
                    continue

        except Exception as e:
            raise Exception(f"Error exporting database to parquet: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass


    def get_latest_processing_time(self):
        """Return the newest `DT_FILE_LOGGED` value as a UNIX timestamp.

        The metadata publisher compares this value with filesystem mtimes to
        decide whether a new Parquet export is needed.
        """
        try:
            self._connect()

            # A single aggregate is enough here; the caller only needs a
            # coarse "database changed or not" signal.
            rows = self._select_rows(
                table="DIM_SPECTRUM_FILE",
                cols=["MAX(DT_FILE_LOGGED) AS LATEST"],
                limit=1
            )

            if not rows or not rows[0]["LATEST"]:
                return None

            latest = rows[0]["LATEST"]
            # Match the publisher's use of filesystem mtimes when possible.
            return latest.timestamp() if hasattr(latest, "timestamp") else latest

        except Exception as e:
            raise Exception(f"Error getting latest processing time: {e}")

        finally:
            try:
                self._disconnect()
            except Exception:
                pass
