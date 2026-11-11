"""PostgreSQL persistence for the RFDATA analytical domain."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
import json
import re
import unicodedata
from typing import TYPE_CHECKING, cast

from psycopg import sql
from psycopg.types.json import Json

from . import constants as db_constants
from .dbHandlerBase import DbHandler, DbRow

if TYPE_CHECKING:
    from shared import logging_utils


class dbHandlerRFM(DbHandler):
    """Persist sites, dimensions, spectra, files and analytical lineage.

    Sessions and transactions belong to DbHandler and connections open lazily.
    Site operations require PostGIS; workers still require connection migration.

    Attributes:
        schema (str): Case-sensitive analytical schema.
        log (logging_utils.log): Logger supplied by the caller.
        reuse_connection (bool): Whether an idle session is retained.
    """

    def __init__(
        self,
        database: str,
        log: logging_utils.log,
        reuse_connection: bool = True,
        *,
        conninfo: str,
    ) -> None:
        """Configure an analytical schema without opening a session.

        Args:
            database (str): Case-sensitive PostgreSQL schema.
            log (logging_utils.log): Operational logger supplied by the caller.
            reuse_connection (bool): Retain an idle connection between calls.
            conninfo (str): Explicit libpq settings including the database name.

        Returns:
            None.
        """
        super().__init__(
            schema=database,
            log=log,
            conninfo=conninfo,
            reuse_connection=reuse_connection,
        )

    def _get_or_insert(
        self,
        *,
        table: str,
        identity: Mapping[str, object],
        id_field: str,
        data: Mapping[str, object] | None = None,
    ) -> tuple[int, bool]:
        """Return the row matching a unique identity, inserting it when absent.

        The identity must be backed by a unique index. A concurrent insert of
        the same identity makes ON CONFLICT DO NOTHING return no row; the
        winner is then read, so callers never create duplicates.

        Args:
            table (str): Declared table name.
            identity (Mapping[str, object]): Equality filters of the unique identity.
            id_field (str): Generated identity column.
            data (Mapping[str, object] | None): Extra columns used only on insert.

        Returns:
            tuple[int, bool]: Row identity and whether this call inserted it.

        Raises:
            RuntimeError: If the row vanishes between the conflict and the read.
            psycopg.Error: If the database operation fails.
        """
        def read() -> int | None:
            rows = self._select_batch(
                table=table, where=identity, columns=(id_field,), limit=1
            )
            return cast(int, rows[0][id_field]) if rows else None

        if (found := read()) is not None:
            return found, False
        row = self._insert_row(
            table=table,
            data={**identity, **(data or {})},
            skip_duplicates=True,
            returning=(id_field,),
        )
        if row is not None:
            return cast(int, row[id_field]), True
        if (found := read()) is not None:
            return found, False
        raise RuntimeError("Unique identity is neither insertable nor readable.")

    def _dimension_get_or_create(
        self, *, table: str, id_field: str, name_field: str, name: str
    ) -> int:
        """Reuse an exact dimension label or insert it and return its identity.

        Args:
            table (str): Declared dimension table with a unique label index.
            id_field (str): Generated identity column.
            name_field (str): Label column.
            name (str): Exact nonempty label to persist.

        Returns:
            int: Existing or newly generated identity.

        Raises:
            ValueError: If the label is empty or the table contract is invalid.
            psycopg.Error: If the database operation fails.
        """
        if not name.strip():
            raise ValueError("Dimension name must be nonempty.")
        identity = {name_field: name}
        return self._get_or_insert(table=table, identity=identity, id_field=id_field)[0]

    def insert_procedure(self, procedure_name: str) -> int:
        """Find or create a procedure by its exact supplied name.

        Args:
            procedure_name (str): Nonempty measurement procedure name.
        Returns:
            int: Procedure identity.
        """
        return self._dimension_get_or_create(
            table="DIM_SPECTRUM_PROCEDURE",
            id_field="ID_PROCEDURE",
            name_field="NA_PROCEDURE",
            name=procedure_name,
        )

    def insert_detector_type(self, detector: str) -> int:
        """Find or create a detector after trimming surrounding whitespace.

        Args:
            detector (str): Nonempty detector label.
        Returns:
            int: Detector identity.
        """
        return self._dimension_get_or_create(
            table="DIM_SPECTRUM_DETECTOR",
            id_field="ID_DETECTOR",
            name_field="NA_DETECTOR",
            name=detector.strip(),
        )

    def insert_measure_unit(self, unit_name: str) -> int:
        """Find or create a measurement unit after trimming whitespace.

        Args:
            unit_name (str): Nonempty unit label.
        Returns:
            int: Measurement-unit identity.
        """
        return self._dimension_get_or_create(
            table="DIM_SPECTRUM_UNIT",
            id_field="ID_MEASURE_UNIT",
            name_field="NA_MEASURE_UNIT",
            name=unit_name.strip(),
        )

    def insert_trace_type(self, trace_name: str) -> int:
        """Find or create a trace type after trimming whitespace.

        Args:
            trace_name (str): Nonempty trace label.
        Returns:
            int: Trace-type identity.
        """
        return self._dimension_get_or_create(
            table="DIM_SPECTRUM_TRACE_TYPE",
            id_field="ID_TRACE_TYPE",
            name_field="NA_TRACE_TYPE",
            name=trace_name.strip(),
        )

    def insert_bridge_spectrum_file(
        self, spectrum_ids: list[int], file_ids: list[int]
    ) -> None:
        """Associate every supplied spectrum with every supplied file.

        Args:
            spectrum_ids (list[int]): Spectrum identities.
            file_ids (list[int]): File identities.

        Returns:
            None. Existing bridge pairs are retained without duplication.

        Raises:
            psycopg.Error: If any insert or batch transaction fails.
        """
        data = [
            {"FK_SPECTRUM": spectrum_id, "FK_FILE": file_id}
            for spectrum_id in spectrum_ids
            for file_id in file_ids
        ]
        self._insert_batch(
            table="BRIDGE_SPECTRUM_FILE", data=data, skip_duplicates=True
        )

    def _summary_publish_scope(
        self,
        *,
        site_ids: Sequence[int] = (),
        equipment_ids: Sequence[int] = (),
        reason: str | None = None,
    ) -> None:
        """Build site/equipment invalidation scopes and publish them through the base.

        Args:
            site_ids (Sequence[int]): Changed site identities.
            equipment_ids (Sequence[int]): Changed equipment identities.
            reason (str | None): Diagnostic label, truncated to the column size.
        Returns:
            None. Publication failure is logged for periodic reconciliation.
        """
        scopes = {(db_constants['summary']['scopes']['site'], str(i)) for i in site_ids}
        scopes.update((db_constants['summary']['scopes']['equipment'], str(i)) for i in equipment_ids)
        self._publish_summary_scopes(scopes, reason=reason)

    def get_file_type_id_by_hostname(self, HOSTNAME: str) -> int:
        """Resolve a file type from equipment markers, falling back to others.

        Args:
            HOSTNAME (str): Nonempty station hostname.
        Returns:
            int: Matching file type identity.
        Raises:
            ValueError: If hostname is empty, ambiguous, or has no fallback.
        """
        if not HOSTNAME:
            raise ValueError("Hostname must be nonempty.")
        rows = self._select_batch(
            table="DIM_FILE_TYPE",
            columns=("ID_TYPE_FILE", "NA_TYPE_FILE", "NA_EQUIPMENT"),
        )
        matches = [
            row
            for row in rows
            if row["NA_EQUIPMENT"]
            and str(row["NA_EQUIPMENT"]).lower() in HOSTNAME.lower()
        ]
        if len(matches) > 1:
            raise ValueError("Ambiguous file type for hostname.")
        if not matches:
            matches = [
                row
                for row in rows
                if str(row["NA_EQUIPMENT"]).strip().lower() == "others"
            ]
        if not matches:
            raise ValueError("No file type matches the hostname.")
        return cast(int, matches[0]["ID_TYPE_FILE"])

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
        """Create or refresh a file identified by volume, path and filename.

        Args:
            hostname (str): Hostname used to resolve the file type.
            NA_PATH (str): Parent artifact path.
            NA_FILE (str): Artifact filename.
            NA_VOLUME (str): Host or repository volume, normalized to lowercase.
            NA_EXTENSION (str | None): Optional file extension.
            VL_FILE_SIZE_KB (int | None): Optional file size in KiB.
            DT_FILE_CREATED (datetime | None): Optional creation timestamp.
            DT_FILE_MODIFIED (datetime | None): Optional modification timestamp.
        Returns:
            int: Existing or newly generated file identity.
        Raises:
            ValueError: If the identity is incomplete or the file type is unknown.
            psycopg.Error: If the lookup or write fails.
        """
        if not all((hostname, NA_PATH, NA_FILE, NA_VOLUME)):
            raise ValueError("File identity and hostname must be nonempty.")
        type_id = self.get_file_type_id_by_hostname(HOSTNAME=hostname)
        identity = {
            "NA_VOLUME": NA_VOLUME.lower(),
            "NA_PATH": NA_PATH,
            "NA_FILE": NA_FILE,
        }
        # None keeps the stored value of an existing file.
        supplied = {
            name: value
            for name, value in (
                ("ID_TYPE_FILE", type_id),
                ("NA_EXTENSION", NA_EXTENSION),
                ("VL_FILE_SIZE_KB", VL_FILE_SIZE_KB),
                ("DT_FILE_CREATED", DT_FILE_CREATED),
                ("DT_FILE_MODIFIED", DT_FILE_MODIFIED),
            )
            if value is not None
        }
        with self._unit_of_work():
            file_id, created = self._get_or_insert(
                table="DIM_SPECTRUM_FILE",
                identity=identity,
                id_field="ID_FILE",
                data=supplied,
            )
            if not created:
                self._update_row(
                    table="DIM_SPECTRUM_FILE", data=supplied, where={"ID_FILE": file_id}
                )
            return file_id

    @staticmethod
    def _normalize_equipment_type_token(value: str | None) -> str:
        """Normalize a receiver model for matching equipment type UIDs.

        Args:
            value (str | None): Raw receiver name or UID.
        Returns:
            str: Lowercase alphanumeric token.
        """
        return re.sub(r"[^a-z0-9]+", "", (value or "").lower())

    def _get_equipment_types(self) -> dict[str, int]:
        """Load equipment types keyed by their normalized UID token.

        Args:
            None.

        Returns:
            dict[str, int]: Lowercase alphanumeric UID token mapped to type ID;
            UIDs without alphanumeric characters are omitted.
        """
        rows = self._select_batch(
            table="DIM_EQUIPMENT_TYPE",
            columns=("ID_EQUIPMENT_TYPE", "NA_EQUIPMENT_TYPE_UID"),
        )
        types = {
            self._normalize_equipment_type_token(cast(str | None, row["NA_EQUIPMENT_TYPE_UID"])): cast(
                int, row["ID_EQUIPMENT_TYPE"]
            )
            for row in rows
        }
        types.pop("", None)
        return types

    def get_or_create_spectrum_equipment(
        self, equipment_name: str, *, equipment_type_hint: str | None = None
    ) -> int:
        """Resolve equipment identity and infer its type from receiver markers.

        Args:
            equipment_name (str): Nonempty persisted equipment name.
            equipment_type_hint (str | None): Optional receiver model for type inference.
        Returns:
            int: Existing or newly inserted equipment identity.
        Raises:
            ValueError: If no name or matching equipment type exists.
        """
        if not equipment_name.strip():
            raise ValueError("Equipment name must be nonempty.")
        name = equipment_name.strip().lower()
        token = self._normalize_equipment_type_token(equipment_type_hint or name)
        equipment_type = next(
            (type_id for uid, type_id in self._get_equipment_types().items() if uid in token),
            None,
        )
        if equipment_type is None:
            raise ValueError("Unable to infer equipment type.")
        with self._unit_of_work():
            equipment_id, created = self._get_or_insert(
                table="DIM_SPECTRUM_EQUIPMENT",
                identity={"NA_EQUIPMENT": name},
                id_field="ID_EQUIPMENT",
                data={"FK_EQUIPMENT_TYPE": equipment_type},
            )
            if created:
                self._summary_publish_scope(
                    equipment_ids=(equipment_id,),
                    reason="get_or_create_spectrum_equipment",
                )
            return equipment_id

    def insert_spectrum(self, data: Mapping[str, object]) -> int:
        """Reuse or widen a spectrum with the same analytical identity.

        Args:
            data (Mapping[str, object]): Resolved dimension IDs, frequency range,
                time interval, trace length and optional measurement metadata.
        Returns:
            int: Existing, widened, or inserted spectrum identity.
        Raises:
            ValueError: If required identity or interval fields are missing.
            psycopg.Error: If lookup, persistence, or transaction fails.
        """
        mapping = {
            "id_site": "FK_SITE",
            "id_equipment": "FK_EQUIPMENT",
            "id_procedure": "FK_PROCEDURE",
            "id_detector_type": "FK_DETECTOR",
            "id_trace_type": "FK_TRACE_TYPE",
            "id_measure_unit": "FK_MEASURE_UNIT",
            "na_description": "NA_DESCRIPTION",
            "nu_freq_start": "NU_FREQ_START",
            "nu_freq_end": "NU_FREQ_END",
            "dt_time_start": "DT_TIME_START",
            "dt_time_end": "DT_TIME_END",
            "nu_sample_duration": "NU_SAMPLE_DURATION",
            "nu_trace_count": "NU_TRACE_COUNT",
            "nu_trace_length": "NU_TRACE_LENGTH",
            "nu_rbw": "NU_RBW",
            "nu_vbw": "NU_VBW",
            "nu_att_gain": "NU_ATT_GAIN",
            "js_metadata": "JS_METADATA",
        }
        required = (
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
        missing = [name for name in required if name not in data]
        if missing:
            raise ValueError(f"Spectrum fields missing: {', '.join(missing)}")
        payload: DbRow = {column: data.get(name) for name, column in mapping.items()}
        metadata = payload["JS_METADATA"]
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        if metadata is not None:
            payload["JS_METADATA"] = Json(metadata)
        where_conditions = {name: payload[name] for name in db_constants['rfm']['spectrum_identity_fields']}
        start = cast(datetime | None, data["dt_time_start"])
        end = cast(datetime | None, data["dt_time_end"])
        count = data.get("nu_trace_count")
        with self._unit_of_work():
            # Locking the parent site serializes concurrent writers of the same
            # identity, which has no unique index because legacy rows may repeat it.
            self._select_batch(
                table="DIM_SPECTRUM_SITE",
                where={"ID_SITE": payload["FK_SITE"]},
                columns=("ID_SITE",),
                for_update=True,
            )
            rows = self._select_batch(
                table="FACT_SPECTRUM",
                where=where_conditions,
                columns=("ID_SPECTRUM", "DT_TIME_END", "NU_TRACE_COUNT"),
                order_by=(
                    ("DT_TIME_END", "DESC NULLS LAST"),
                    ("NU_TRACE_COUNT", "DESC NULLS LAST"),
                    ("ID_SPECTRUM", "ASC"),
                ),
            )
            # Rows share the start; keep one that covers the interval, else widen one.
            candidate = None
            for row in rows:
                old_end = cast(datetime | None, row["DT_TIME_END"])
                old_count = row["NU_TRACE_COUNT"]
                if old_end == end:
                    if (
                        old_count is None
                        or count is None
                        or int(cast(int, old_count)) >= int(cast(int, count))
                    ):
                        return cast(int, row["ID_SPECTRUM"])
                elif start is None or old_end is None or end is None:
                    continue
                elif old_end > end:
                    return cast(int, row["ID_SPECTRUM"])
                candidate = cast(int, row["ID_SPECTRUM"])
                break
            if candidate is None:
                row = self._insert_row(
                    table="FACT_SPECTRUM", data=payload, returning=("ID_SPECTRUM",)
                )
                if row is None:
                    raise RuntimeError("Spectrum insert returned no identity.")
                candidate = cast(int, row["ID_SPECTRUM"])
            else:
                self._update_row(
                    table="FACT_SPECTRUM",
                    data=payload,
                    where={"ID_SPECTRUM": candidate},
                )
            self._summary_publish_scope(
                site_ids=(cast(int, data["id_site"]),),
                equipment_ids=(cast(int, data["id_equipment"]),),
                reason="insert_spectrum",
            )
            return candidate

    @staticmethod
    def _normalize_string(value: str | None) -> str | None:
        """Normalize locality labels for accent- and spacing-independent matching.

        Args:
            value (str | None): Geographic label from a catalog or geocoder.
        Returns:
            str | None: Normalized label, or None when absent.
        """
        if not value:
            return None
        value = unicodedata.normalize("NFKD", value.strip().lower())
        value = "".join(c for c in value if not unicodedata.combining(c))
        return re.sub(r"\s+", " ", re.sub(r"[’'`]", "", value))

    def _iter_normalized_field_candidates(
        self, data: Mapping[str, object], field: str
    ) -> list[tuple[str, str]]:
        """Deduplicate a direct locality label and its ordered alternatives.

        Args:
            data (Mapping[str, object]): Locality payload and candidate lists.
            field (str): Locality field whose alternatives use the candidates suffix.
        Returns:
            list[tuple[str, str]]: Original and normalized nonempty labels.
        """
        candidates: list[object] = [data.get(field)]
        alternatives = data.get(f"{field}_candidates")
        if isinstance(alternatives, (list, tuple)):
            candidates.extend(alternatives)
        result: list[tuple[str, str]] = []
        seen: set[str] = set()
        for value in candidates:
            if value is None:
                continue
            label = str(value).strip()
            normalized = self._normalize_string(label)
            if normalized and normalized not in seen:
                result.append((label, normalized))
                seen.add(normalized)
        return result

    @staticmethod
    def _resolve_site_name(data: Mapping[str, object]) -> str | None:
        """Choose the first nonempty display name from site, district, and county.

        Args:
            data (Mapping[str, object]): Site locality fields.
        Returns:
            str | None: Display label when one is present.
        """
        for field in ("site_name", "district", "county"):
            value = data.get(field)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _normalize_site_data(self, data: DbRow) -> DbRow:
        """Clean site labels and infer a missing state from the county catalog.

        Args:
            data (DbRow): Mutable site payload including optional locality names.
        Returns:
            DbRow: The supplied mapping with cleaned labels and candidates.
        """
        for field in ("state", "county", "district", "site_name"):
            if isinstance(data.get(field), str):
                data[field] = cast(str, data[field]).strip()
        for field in ("state", "county", "district"):
            key = f"{field}_candidates"
            if isinstance(data.get(key), (list, tuple)):
                values = [
                    name
                    for name, _ in self._iter_normalized_field_candidates(
                        {key: data[key]}, field
                    )
                ]
                if values:
                    data[key] = values
                else:
                    data.pop(key, None)
        if not data.get("state") and data.get("county"):
            rows = self._select_batch(
                table="DIM_SITE_COUNTY",
                where={"NA_COUNTY": data["county"]},
                columns=("FK_STATE",),
                limit=1,
            )
            if rows and rows[0]["FK_STATE"] is not None:
                state = self._select_row(
                    table="DIM_SITE_STATE",
                    where={"ID_STATE": rows[0]["FK_STATE"]},
                    columns=("NA_STATE",),
                )
                if state:
                    data["state"] = state["NA_STATE"]
        return data

    def _get_geographic_codes(
        self, data: DbRow, *, force_create_district: bool = False, dry_run: bool = False
    ) -> tuple[int, int, int | None]:
        """Resolve administrative catalog identities using normalized locality names.

        Args:
            data (DbRow): State and county labels plus optional district candidates.
            force_create_district (bool): Create an unknown district when allowed.
            dry_run (bool): Resolve without inserting a missing district.
        Returns:
            tuple[int, int, int | None]: State, county and optional district IDs.
        Raises:
            ValueError: If state or county cannot be resolved.
        """
        states = self._select_batch(
            table="DIM_SITE_STATE",
            where={"NA_STATE": data["state"]},
            columns=("ID_STATE", "NA_STATE"),
            limit=1,
        )
        if not states:
            normalized = self._normalize_string(cast(str, data["state"]))
            states = [
                row
                for row in self._select_batch(
                    table="DIM_SITE_STATE", columns=("ID_STATE", "NA_STATE")
                )
                if self._normalize_string(cast(str | None, row["NA_STATE"]))
                == normalized
            ]
        if not states:
            raise ValueError("State not found in DIM_SITE_STATE.")
        state_id = cast(int, states[0]["ID_STATE"])
        if state_id == db_constants['rfm']['sites']['federal_district']['state_id']:
            county_id = db_constants['rfm']['sites']['federal_district']['county_id']
        else:
            normalized = self._normalize_string(cast(str, data["county"]))
            if normalized is not None:
                normalized = db_constants['rfm']['sites']['county_name_aliases'].get(normalized, normalized)
            counties = self._select_batch(
                table="DIM_SITE_COUNTY",
                where={"FK_STATE": state_id},
                columns=("ID_COUNTY", "NA_COUNTY"),
            )
            county = next(
                (
                    row
                    for row in counties
                    if self._normalize_string(cast(str | None, row["NA_COUNTY"]))
                    == normalized
                ),
                None,
            )
            if county is None:
                raise ValueError("County not found for the resolved state.")
            county_id = cast(int, county["ID_COUNTY"])
        district_id = None
        candidates = self._iter_normalized_field_candidates(data, "district")
        if candidates:
            districts = self._select_batch(
                table="DIM_SITE_DISTRICT",
                where={"FK_COUNTY": county_id},
                columns=("ID_DISTRICT", "NA_DISTRICT"),
            )
            for label, normalized in candidates:
                match = next(
                    (
                        row
                        for row in districts
                        if self._normalize_string(cast(str | None, row["NA_DISTRICT"]))
                        == normalized
                    ),
                    None,
                )
                if match:
                    district_id = cast(int, match["ID_DISTRICT"])
                    data["district"] = label
                    break
            if (
                district_id is None
                and (force_create_district or db_constants['rfm']['sites']['auto_create_district'])
                and not dry_run
            ):
                row = self._insert_row(
                    table="DIM_SITE_DISTRICT",
                    data={"FK_COUNTY": county_id, "NA_DISTRICT": candidates[0][0]},
                    returning=("ID_DISTRICT",),
                )
                if row is None:
                    raise RuntimeError("District insert returned no identity.")
                district_id = cast(int, row["ID_DISTRICT"])
                data["district"] = candidates[0][0]
        return state_id, county_id, district_id

    def get_site_geography(self, site_id: int) -> DbRow:
        """Read one site's administrative fields and PostGIS point coordinates.

        Args:
            site_id (int): Site identity.
        Returns:
            DbRow: Site IDs, name, longitude, latitude, altitude, GNSS sample
            count and SRID.
        Raises:
            ValueError: If the site does not exist.
        """
        return self._read_site_geography(site_id)

    def _read_site_geography(self, site_id: int, *, for_update: bool = False) -> DbRow:
        """Read site geography, optionally locking the row for a centroid update.

        Args:
            site_id (int): Site identity.
            for_update (bool): Lock the row until the explicit transaction ends.
        Returns:
            DbRow: Same fields as get_site_geography.
        Raises:
            RuntimeError: If for_update is used without an explicit transaction.
            ValueError: If the site does not exist.
        """
        if for_update and not self.in_transaction:
            raise RuntimeError("FOR UPDATE requires an explicit transaction.")
        query = sql.SQL(
            """SELECT "ID_SITE", "FK_STATE", "FK_COUNTY", "FK_DISTRICT", "NA_SITE",
            public.ST_X("GEO_POINT") AS "LONGITUDE", public.ST_Y("GEO_POINT") AS "LATITUDE",
            "NU_ALTITUDE", "NU_GNSS_MEASUREMENTS", public.ST_SRID("GEO_POINT") AS "SRID"
            FROM {} WHERE "ID_SITE" = %s"""
        ).format(sql.Identifier(self.schema, "DIM_SPECTRUM_SITE"))
        if for_update:
            query += sql.SQL(" FOR UPDATE")
        rows = self._select_raw(query=query, params=(site_id,))
        if not rows:
            raise ValueError("Site not found in DIM_SPECTRUM_SITE.")
        return rows[0]

    def get_site_id(self, data: Mapping[str, object]) -> int | bool:
        """Find the nearest site within the configured spherical distance.

        Args:
            data (Mapping[str, object]): Longitude and latitude in decimal degrees.
        Returns:
            int | bool: Nearest site identity, or False outside the threshold.
        Raises:
            KeyError: If longitude or latitude is missing.
            ValueError: If coordinates cannot be converted to decimal degrees.
            psycopg.Error: If the spatial query or database operation fails.
        """
        longitude, latitude = float(cast(float, data["longitude"])), float(
            cast(float, data["latitude"])
        )
        nearest = self._select_nearest_point(
            table="DIM_SPECTRUM_SITE",
            point_column="GEO_POINT",
            identity_column="ID_SITE",
            longitude=longitude,
            latitude=latitude,
        )
        if nearest is None:
            return False
        site_id, distance = nearest
        if distance < db_constants['rfm']['sites']['match_distance_meters']:
            return cast(int, site_id)
        return False

    def insert_site(self, data: DbRow, *, force_create_district: bool = False) -> int:
        """Create a fixed site with resolved geography and a parameterized point.

        Args:
            data (DbRow): Required longitude, latitude and altitude as numeric
                values, state and county as catalog labels; optional district,
                site_name, locality candidate lists and nu_gnss_measurements.
            force_create_district (bool): Create a missing district when needed.
        Returns:
            int: Generated site identity.
        Raises:
            ValueError: If coordinates or required locality labels are invalid.
            RuntimeError: If the insert returns no identity.
            psycopg.Error: If persistence fails, including a missing fixed type
                in DIM_SITE_TYPE. Catalog initialization belongs to installation.
        """
        with self._unit_of_work():
            cleaned = self._normalize_site_data(dict(data))
            state, county, district = self._get_geographic_codes(
                cleaned, force_create_district=force_create_district
            )
            longitude, latitude = float(cast(float, cleaned["longitude"])), float(
                cast(float, cleaned["latitude"])
            )
            payload: DbRow = {
                "FK_TYPE": db_constants['rfm']['sites']['fixed_type'],
                "NU_ALTITUDE": cleaned["altitude"],
                "NU_GNSS_MEASUREMENTS": cleaned.get("nu_gnss_measurements", 0),
                "FK_STATE": state,
                "FK_COUNTY": county,
                "FK_DISTRICT": district,
                "NA_SITE": self._resolve_site_name(cleaned),
                "GEO_POINT": f"POINT({longitude} {latitude})",
            }
            row = self._insert_row(
                table="DIM_SPECTRUM_SITE", data=payload, returning=("ID_SITE",)
            )
            if row is None:
                raise RuntimeError("Site insert returned no identity.")
            site_id = cast(int, row["ID_SITE"])
            self._summary_publish_scope(site_ids=(site_id,), reason="insert_site")
            return site_id

    def update_site(
        self,
        site: int,
        longitude_raw: list[float],
        latitude_raw: list[float],
        altitude_raw: list[float],
    ) -> DbRow:
        """Update one site's weighted centroid using new GNSS samples.

        Args:
            site (int): Existing site identity.
            longitude_raw (list[float]): New longitude samples.
            latitude_raw (list[float]): New latitude samples.
            altitude_raw (list[float]): New altitude samples.
        Returns:
            DbRow: Updated coordinates, sample counts and action.
        Raises:
            ValueError: If sample lengths differ or the site does not exist.
        """
        if (
            not longitude_raw
            or len(longitude_raw) != len(latitude_raw)
            or len(longitude_raw) != len(altitude_raw)
        ):
            raise ValueError("GNSS sample lists must be nonempty and equally sized.")
        with self._unit_of_work():
            # The row lock serializes read/modify/write of concurrent samples.
            current = self._read_site_geography(site, for_update=True)
            previous = int(cast(int, current["NU_GNSS_MEASUREMENTS"] or 0))
            total = previous + len(longitude_raw)

            def weighted(stored: object, samples: list[float]) -> float:
                # A missing stored value contributes no weight.
                if stored is None:
                    return sum(samples) / len(samples)
                return (float(cast(float, stored)) * previous + sum(samples)) / total

            longitude = weighted(current["LONGITUDE"], longitude_raw)
            latitude = weighted(current["LATITUDE"], latitude_raw)
            altitude = weighted(current["NU_ALTITUDE"], altitude_raw)
            point = (
                f'SRID={int(cast(int, current["SRID"]))};POINT({longitude} {latitude})'
            )
            payload = {
                "GEO_POINT": point,
                "NU_ALTITUDE": altitude,
                "NU_GNSS_MEASUREMENTS": total,
            }
            self._update_row(
                table="DIM_SPECTRUM_SITE", data=payload, where={"ID_SITE": site}
            )
            self._summary_publish_scope(site_ids=(site,), reason="update_site")
            return {
                "action": "updated",
                "site_id": site,
                "latitude": latitude,
                "longitude": longitude,
                "altitude": altitude,
                "total_gnss": total,
                "previous_gnss": previous,
            }

    def refresh_site_geography(
        self,
        site_id: int,
        data: DbRow,
        *,
        force_create_district: bool = False,
        dry_run: bool = False,
    ) -> DbRow:
        """Resolve and optionally update a site's administrative locality.

        Args:
            site_id (int): Existing site identity.
            data (DbRow): State, county, district and optional name candidates.
            force_create_district (bool): Permit creation of a missing district.
            dry_run (bool): Report proposed changes without writing.
        Returns:
            DbRow: Action, resolved IDs, site name and pending district information.
        Raises:
            ValueError: If the site or a required geographic catalog row is missing.
        """
        with self._unit_of_work():
            columns = ("FK_STATE", "FK_COUNTY", "FK_DISTRICT", "NA_SITE")
            current = self._select_row(
                table="DIM_SPECTRUM_SITE", where={"ID_SITE": site_id}, columns=columns
            )
            if current is None:
                raise ValueError("Site not found in DIM_SPECTRUM_SITE.")
            cleaned = self._normalize_site_data(dict(data))
            state, county, district = self._get_geographic_codes(
                cleaned, force_create_district=force_create_district, dry_run=dry_run
            )
            name = self._resolve_site_name(cleaned)
            candidates = self._iter_normalized_field_candidates(cleaned, "district")
            district_name = candidates[0][0] if candidates else None
            pending = bool(
                dry_run and force_create_district and district is None and district_name
            )
            changes = {
                "FK_STATE": state,
                "FK_COUNTY": county,
                "FK_DISTRICT": district,
                "NA_SITE": name,
            }
            changed = pending or any(
                current[field] != value for field, value in changes.items()
            )
            action = (
                "dry_run"
                if dry_run and changed
                else "updated"
                if changed
                else "unchanged"
            )
            if changed and not dry_run:
                self._update_row(
                    table="DIM_SPECTRUM_SITE", data=changes, where={"ID_SITE": site_id}
                )
                self._summary_publish_scope(
                    site_ids=(site_id,), reason="refresh_site_geography"
                )
            return {
                "action": action,
                "site_id": site_id,
                "fk_state": state,
                "fk_county": county,
                "fk_district": district,
                "site_name": name,
                "district_name": district_name,
                "would_create_district": pending,
            }

    def build_path(self, site_id: int) -> str:
        """Build a repository path from the stored site and state identities.

        Args:
            site_id (int): Site identity.
        Returns:
            str: State abbreviation, county ID and site ID separated by slashes.
        Raises:
            ValueError: If the site or its state is missing.
        """
        columns = {
            "LC_STATE": ("DIM_SITE_STATE", "LC_STATE"),
            "FK_COUNTY": ("DIM_SPECTRUM_SITE", "FK_COUNTY"),
            "ID_SITE": ("DIM_SPECTRUM_SITE", "ID_SITE"),
        }
        rows = self._select_batch(
            table="DIM_SPECTRUM_SITE",
            where={"ID_SITE": site_id},
            columns=columns,
            join=("DIM_SITE_STATE", "FK_STATE", "ID_STATE"),
            limit=1,
        )
        if not rows:
            raise ValueError("Site or state not found.")
        row = rows[0]
        return f'{row["LC_STATE"]}/{row["FK_COUNTY"]}/{row["ID_SITE"]}'

    def _select_file_ids_by_artifacts(
        self, *, artifacts: Sequence[tuple[str, str, str]]
    ) -> list[int]:
        """Resolve analytical file IDs for exact volume/path/name identities.

        Args:
            artifacts (Sequence[tuple[str, str, str]]): Volume, path and filename triples.

        Returns:
            list[int]: Distinct matching file identities in ascending order.
        """
        wanted = {(volume.lower(), path, name) for volume, path, name in artifacts}
        if not wanted:
            return []
        # Member sets select a superset; exact triples are matched below.
        where_conditions = {
            "NA_FILE__in": sorted({name for _, _, name in wanted}),
            "NA_PATH__in": sorted({path for _, path, _ in wanted}),
            "NA_VOLUME__in": sorted({volume for volume, _, _ in wanted}),
        }
        rows = self._select_batch(
            table="DIM_SPECTRUM_FILE",
            where=where_conditions,
            columns=("ID_FILE", "NA_VOLUME", "NA_PATH", "NA_FILE"),
        )
        return sorted(
            cast(int, row["ID_FILE"])
            for row in rows
            if (row["NA_VOLUME"], row["NA_PATH"], row["NA_FILE"]) in wanted
        )

    def _select_spectrum_merge_rows_by_file_ids(
        self, *, file_ids: list[int]
    ) -> list[DbRow]:
        """Read spectra linked to selected artifacts without duplicating shared rows.

        Args:
            file_ids (list[int]): Analytical file identities.
        Returns:
            list[DbRow]: Unique spectra with their merge identity and time interval.
        """
        if not file_ids:
            return []
        columns = (
            "ID_SPECTRUM",
            *db_constants['rfm']['spectrum_identity_fields'],
            "DT_TIME_END",
            "NU_TRACE_COUNT",
        )
        rows = self._select_batch(
            table="FACT_SPECTRUM",
            columns=columns,
            join=("BRIDGE_SPECTRUM_FILE", "ID_SPECTRUM", "FK_SPECTRUM"),
            joined_where={"FK_FILE__in": file_ids},
        )
        return list({cast(int, row["ID_SPECTRUM"]): row for row in rows}.values())

    def _build_spectrum_merge_key(self, row: Mapping[str, object]) -> tuple[object, ...]:
        """Build the stable logical spectrum identity used during reprocessing.

        Args:
            row (Mapping[str, object]): Persisted spectrum dimensions and properties.

        Returns:
            tuple[object, ...]: Values of rfm.spectrum_identity_fields in order.
        """
        return tuple(row.get(field) for field in db_constants['rfm']['spectrum_identity_fields'])

    def _select_redundant_spectrum_ids(self, *, spectra: list[DbRow]) -> list[int]:
        """Identify spectra contained by the widest sibling with the same identity.

        Args:
            spectra (list[DbRow]): File-linked spectra with timestamps and counts.
        Returns:
            list[int]: IDs whose intervals are covered by the selected sibling.
        """
        groups: dict[tuple[object, ...], list[DbRow]] = {}
        for row in spectra:
            groups.setdefault(self._build_spectrum_merge_key(row), []).append(row)
        redundant: list[int] = []
        for rows in groups.values():
            if len(rows) < 2:
                continue
            ordered = sorted(
                rows,
                key=lambda row: (
                    -(
                        cast(datetime, row["DT_TIME_END"])
                        - cast(datetime, row["DT_TIME_START"])
                    ).total_seconds(),
                    -int(cast(int, row["NU_TRACE_COUNT"]))
                    if row.get("NU_TRACE_COUNT") is not None
                    else 1,
                    cast(int, row["ID_SPECTRUM"]),
                ),
            )
            survivor = ordered[0]
            for row in ordered[1:]:
                if cast(datetime, survivor["DT_TIME_START"]) <= cast(
                    datetime, row["DT_TIME_START"]
                ) and cast(datetime, survivor["DT_TIME_END"]) >= cast(
                    datetime, row["DT_TIME_END"]
                ):
                    redundant.append(cast(int, row["ID_SPECTRUM"]))
        return redundant

    def _select_orphans(
        self,
        *,
        table: str,
        id_field: str,
        bridge_field: str,
        ids: Sequence[int],
        columns: Sequence[str],
    ) -> list[DbRow]:
        """Select candidate rows that no longer have a spectrum/file bridge.

        Callers run this inside the transaction that removed the links.

        Args:
            table (str): Declared spectrum or file table.
            id_field (str): Primary key in the selected table.
            bridge_field (str): Corresponding BRIDGE_SPECTRUM_FILE column.
            ids (Sequence[int]): Candidate identities to check.
            columns (Sequence[str]): Declared fields to return.

        Returns:
            list[DbRow]: Candidate rows with no remaining bridge.
        """
        if not ids:
            return []
        linked = self._select_batch(
            table="BRIDGE_SPECTRUM_FILE",
            where={f"{bridge_field}__in": tuple(ids)},
            columns=(bridge_field,),
        )
        orphan_ids = sorted(set(ids) - {row[bridge_field] for row in linked})
        if not orphan_ids:
            return []
        return self._select_batch(
            table=table, where={f"{id_field}__in": orphan_ids}, columns=columns
        )

    def _remove_lineage(
        self, *, file_ids: list[int], spectrum_ids: list[int], reason: str
    ) -> dict[str, int]:
        """Remove selected bridge links and prune only newly orphaned spectra.

        Args:
            file_ids (list[int]): File identities whose links may be removed.
            spectrum_ids (list[int]): Spectra whose links to those files are removed;
                empty removes nothing.
            reason (str): Summary invalidation reason.

        Returns:
            dict[str, int]: Counts of files, file links, emitter links and spectra.
        """
        counts = {
            "file_ids": len(file_ids),
            "removed_file_links": 0,
            "removed_emitter_links": 0,
            "removed_spectra": 0,
        }
        if not file_ids or not spectrum_ids:
            return counts
        counts["removed_file_links"] = self._delete_batch(
            table="BRIDGE_SPECTRUM_FILE",
            where={"FK_FILE__in": file_ids, "FK_SPECTRUM__in": spectrum_ids},
        )
        orphans = self._select_orphans(
            table="FACT_SPECTRUM",
            id_field="ID_SPECTRUM",
            bridge_field="FK_SPECTRUM",
            ids=spectrum_ids,
            columns=("ID_SPECTRUM", "FK_SITE", "FK_EQUIPMENT"),
        )
        if not orphans:
            return counts
        orphan_ids = [cast(int, row["ID_SPECTRUM"]) for row in orphans]
        counts["removed_emitter_links"] = self._delete_batch(
            table="BRIDGE_SPECTRUM_EMITTER", where={"FK_SPECTRUM__in": orphan_ids}
        )
        counts["removed_spectra"] = self._delete_batch(
            table="FACT_SPECTRUM", where={"ID_SPECTRUM__in": orphan_ids}
        )
        self._summary_publish_scope(
            site_ids=[cast(int, row["FK_SITE"]) for row in orphans if row["FK_SITE"] is not None],
            equipment_ids=[
                cast(int, row["FK_EQUIPMENT"])
                for row in orphans
                if row["FK_EQUIPMENT"] is not None
            ],
            reason=reason,
        )
        return counts

    def reset_reprocessed_file_lineage(
        self,
        *,
        host_volume: str,
        host_path: str,
        host_file: str,
        repository_volume: str,
        previous_repository_path: str | None = None,
        previous_repository_file: str | None = None,
        repository_path: str,
        repository_file: str,
    ) -> dict[str, int]:
        """Remove old source/repository links before an authoritative reprocessing.

        Args:
            host_volume (str): Source volume.
            host_path (str): Source directory.
            host_file (str): Source filename.
            repository_volume (str): Repository volume.
            previous_repository_path (str | None): Former repository directory.
            previous_repository_file (str | None): Former repository filename.
            repository_path (str): Current repository directory.
            repository_file (str): Current repository filename.
        Returns:
            dict[str, int]: Counts of file IDs, links, emitter links and spectra.
        """
        artifacts = [
            (host_volume, host_path, host_file),
            (repository_volume, repository_path, repository_file),
        ]
        if previous_repository_path and previous_repository_file:
            artifacts.append(
                (repository_volume, previous_repository_path, previous_repository_file)
            )
        with self._unit_of_work():
            file_ids = self._select_file_ids_by_artifacts(artifacts=artifacts)
            linked = self._select_spectrum_merge_rows_by_file_ids(file_ids=file_ids)
            spectrum_ids = sorted(cast(int, row["ID_SPECTRUM"]) for row in linked)
            result = self._remove_lineage(
                file_ids=file_ids,
                spectrum_ids=spectrum_ids,
                reason="reset_reprocessed_file_lineage",
            )
            if spectrum_ids:
                orphan_files = self._select_orphans(
                    table="DIM_SPECTRUM_FILE",
                    id_field="ID_FILE",
                    bridge_field="FK_FILE",
                    ids=file_ids,
                    columns=("ID_FILE",),
                )
                if orphan_files:
                    self._delete_batch(
                        table="DIM_SPECTRUM_FILE",
                        where={"ID_FILE__in": [row["ID_FILE"] for row in orphan_files]},
                    )
            return result

    def reconcile_reprocessed_file_lineage(
        self,
        *,
        host_volume: str,
        host_path: str,
        host_file: str,
        repository_volume: str,
        repository_path: str,
        repository_file: str,
    ) -> dict[str, int]:
        """Prune contained duplicate spectra associated with a source/repository pair.

        Args:
            host_volume (str): Source volume.
            host_path (str): Source directory.
            host_file (str): Source filename.
            repository_volume (str): Repository volume.
            repository_path (str): Repository directory.
            repository_file (str): Repository filename.
        Returns:
            dict[str, int]: Counts of file IDs, links, emitter links and spectra.
        """
        artifacts = [
            (host_volume, host_path, host_file),
            (repository_volume, repository_path, repository_file),
        ]
        with self._unit_of_work():
            file_ids = self._select_file_ids_by_artifacts(artifacts=artifacts)
            spectra = self._select_spectrum_merge_rows_by_file_ids(file_ids=file_ids)
            return self._remove_lineage(
                file_ids=file_ids,
                spectrum_ids=self._select_redundant_spectrum_ids(spectra=spectra),
                reason="reconcile_reprocessed_file_lineage",
            )
