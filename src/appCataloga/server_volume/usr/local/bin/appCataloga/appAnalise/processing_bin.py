"""
appAnalise domain handler for one claimed processing ``FILE_TASK``.

``appCataloga_file_bin_process_appAnalise.py`` owns the queue lifecycle. This
module owns the domain work inside one processing attempt and exposes only:
    - ``should_export``: export policy by station family
    - ``run_processing_flow``: appAnalise call, SITE and dimension resolution,
      RFDATA persistence, artifact promotion and completion log

Private helpers follow the flow order:
    1. SITE resolution and fixed-site centroid updates
    2. repository destination path
    3. dimension resolution (procedure, detector, equipment, labels)
    4. FACT insertion and analytical file lineage
    5. completion log
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
import re
from typing import TYPE_CHECKING, Any, TypedDict

from db import constants as db_constants
import config as k
from appAnalise.payload_parser import canonicalize_equipment_identifier
from shared import file_utils, geolocation_utils

if TYPE_CHECKING:
    from appAnalise.appAnalise_connection import AppAnaliseConnection
    from db.dbHandlerRFM import dbHandlerRFM
    from shared.logging_utils import log as logger_type


NON_ALNUM_RE = re.compile(r"[^a-z0-9]+", re.IGNORECASE)
# Station families whose cataloged equipment is the host itself, with the
# type hint used to infer the equipment type.
STATION_EQUIPMENT_FAMILIES = (
    (("ermx", "emrx"), "ermx"),
    (("ums",), "ums300"),
)
SiteData = dict[str, Any]
SiteCacheKey = tuple[float, float, float]


def should_export(hostname: str) -> bool:
    """Decide whether appAnalise should export a ``.mat`` artifact.

    Station-family rule:
        - CelPlan (`CWSM` hostname prefix): accept the `.mat` export as the
          canonical artifact.
        - All other families: keep the original binary payload as the
          canonical artifact. appAnalise is invoked for metadata extraction
          only, not file export.

    The function stays isolated so future host-family policy changes do not
    leak into the worker loop.

    Args:
        hostname (str): Measurement station hostname.

    Returns:
        bool: ``True`` for CelPlan/CWSM hosts; otherwise ``False``.
    """
    normalized = (hostname or "").strip().lower()
    return normalized.startswith("cwsm")


def run_processing_flow(
    db_rfm: dbHandlerRFM,
    task: dict[str, Any],
    app_analise: AppAnaliseConnection,
    *,
    logger: logger_type,
    service_name: str,
) -> dict[str, Any]:
    """Run the appAnalise domain pipeline for one claimed ``FILE_TASK``.

    The worker entrypoint still owns queue state transitions, retries, and the
    outer elapsed time contract. This helper owns the domain steps inside one
    processing attempt:
        - call appAnalise and read the normalized payload
        - resolve SITE and spectrum dimensions
        - persist spectra and analytical file lineage in RFDATA
        - promote the final artifact to its canonical repository location
        - emit the standardized domain completion log

    Returns the artifacts and metadata that the worker later uses to finalize
    ``FILE_TASK`` and ``FILE_TASK_HISTORY`` in BPDATA.

    Args:
        db_rfm (dbHandlerRFM): RFDATA handler used for analytical persistence.
        task (dict[str, Any]): Claimed queue task and source artifact metadata.
        app_analise (AppAnaliseConnection): Client that processes the source file.
        logger (logger_type): Structured event logger.
        service_name (str): Worker service name.

    Returns:
        dict[str, Any]: File metadata, destination, normalized payload, SITE ids,
            spectrum ids, accepted answer, and raw decoded payload.

    Raises:
        Exception: If processing, resolution, persistence, or artifact promotion fails.
    """
    work_started_at = time.monotonic()

    # Transport and payload validation stay inside the appAnalise adapter.
    # This flow only consumes the accepted domain payload and artifact.
    phase_started_at = time.monotonic()
    bin_data, file_meta, answer, payload = app_analise.process(
        file_path=task["server_path"],
        file_name=task["server_name"],
        export=task["export"],
    )
    process_elapsed_sec = round(time.monotonic() - phase_started_at, 3)

    # SITE resolution stays outside the transaction because geocoding and
    # locality reconciliation are slower and fail differently from DB writes.
    phase_started_at = time.monotonic()
    resolved_site_ids = _resolve_spectrum_sites(db_rfm, bin_data, logger=logger)
    site_elapsed_sec = round(time.monotonic() - phase_started_at, 3)

    # The DB phase writes FACT rows first, then records both host and
    # repository file lineage explicitly around the artifact promotion step.
    phase_started_at = time.monotonic()
    new_path = _build_repository_destination_path(
        db_rfm,
        bin_data,
        task["hostname_db"],
    )
    db_rfm.begin_transaction()

    # Clear old host/repository lineage first: reprocessing can change the
    # analytical shape of the file, so the fresh payload is authoritative.
    db_rfm.reset_reprocessed_file_lineage(
        host_volume=task["hostname_db"],
        host_path=task["host_path"],
        host_file=task["host_file_name"],
        repository_volume=k.REPO_VOLUME_NAME,
        previous_repository_path=task["server_path"],
        previous_repository_file=task["server_name"],
        repository_path=new_path,
        repository_file=file_meta["file_name"],
    )

    # Resolve all dimension ids before inserting FACT rows so the insert step
    # does not need to know station-family or analyzer-specific rules.
    _resolve_spectrum_dimensions(db_rfm, bin_data, hostname_db=task["hostname_db"])

    # Insert spectra after all dimension ids were resolved in memory.
    spectrum_ids = _insert_spectra_batch(
        db_rfm=db_rfm,
        bin_data=bin_data,
    )

    # Record the original host-side source file for these spectra.
    _register_spectrum_file_lineage(
        db_rfm,
        spectrum_ids=spectrum_ids,
        hostname=task["hostname_db"],
        volume=task["hostname_db"],
        path=task["host_path"],
        file_name=task["host_file_name"],
        extension=task["extension"],
        size_kb=task["vl_file_size_kb"],
        dt_created=task["dt_created"],
        dt_modified=task["dt_modified"],
    )
    db_elapsed_sec = round(time.monotonic() - phase_started_at, 3)

    # Finalization promotes the canonical repository artifact and then records
    # that second file lineage against the same persisted spectra.
    phase_started_at = time.monotonic()
    file_meta = file_utils.promote_final_artifact(
        new_path=new_path,
        file_meta=file_meta,
        source_file_meta=task["source_file_meta"],
        export=task["export"],
        filename=task["filename"],
        logger=logger,
    )

    # Record the canonical repository artifact for the same spectra.
    _register_spectrum_file_lineage(
        db_rfm,
        spectrum_ids=spectrum_ids,
        hostname=task["hostname_db"],
        volume=k.REPO_VOLUME_NAME,
        path=new_path,
        file_name=file_meta["file_name"],
        extension=file_meta["extension"],
        size_kb=file_meta["size_kb"],
        dt_created=file_meta["dt_created"],
        dt_modified=file_meta["dt_modified"],
    )

    db_rfm.commit()
    finalize_elapsed_sec = round(time.monotonic() - phase_started_at, 3)
    _log_processing_completion(
        logger=logger,
        service_name=service_name,
        task=task,
        work_started_at=work_started_at,
        process_elapsed_sec=process_elapsed_sec,
        site_elapsed_sec=site_elapsed_sec,
        db_elapsed_sec=db_elapsed_sec,
        finalize_elapsed_sec=finalize_elapsed_sec,
        resolved_site_ids=resolved_site_ids,
        spectrum_ids=spectrum_ids,
        new_path=new_path,
        file_meta=file_meta,
    )

    # The entrypoint owns final queue state and receives only domain artifacts.
    return {
        "file_meta": file_meta,
        "new_path": new_path,
        "bin_data": bin_data,
        "resolved_site_ids": resolved_site_ids,
        "spectrum_ids": spectrum_ids,
        "answer": answer,
        "payload": payload,
    }


# --- SITE resolution ---


class _SiteSamples(TypedDict):
    """Collect GNSS samples from distinct point summaries of one fixed site.

    Attributes:
        longitude_raw (list[float]): Longitude samples, initially empty.
        latitude_raw (list[float]): Latitude samples, initially empty.
        altitude_raw (list[float]): Altitude samples, initially empty.
        occurrences (int): Number of contributing point summaries, initially zero.
    """

    longitude_raw: list[float]
    latitude_raw: list[float]
    altitude_raw: list[float]
    occurrences: int


FixedSiteUpdates = dict[int, _SiteSamples]


def _resolve_spectrum_sites(
    db_rfm: dbHandlerRFM,
    bin_data: dict[str, Any],
    *,
    logger: logger_type | None = None,
) -> list[int]:
    """Assign fixed sites per spectrum, then update each existing centroid once.

    Cache and GNSS samples belong to this file only. Repeated point summaries
    contribute once; different points may resolve to the same site or distinct
    sites. This is the current fixed-station policy, not a drive-test resolver.

    Args:
        db_rfm (dbHandlerRFM): RFDATA handler used for SITE operations.
        bin_data (dict[str, Any]): Required spectrum list of normalized objects
            with site_data mappings as documented by _resolve_fixed_site. Replaces
            the list with retained spectra, assigns integer site_id to each, and
            increments optional integer discarded_spectrum_count on local defects.
        logger (logger_type | None): Optional discard and centroid event logger.

    Returns:
        list[int]: Site IDs in retained spectrum order, including repeated IDs.

    Raises:
        ValueError: If site_data is malformed or no spectrum resolves to a site.
        Exception: Service, database and unexpected failures propagate unchanged.
    """
    site_cache: dict[SiteCacheKey, int] = {}
    site_updates: FixedSiteUpdates = {}
    spectra = bin_data["spectrum"]
    retained: list[Any] = []

    for spectrum in spectra:
        site_data = getattr(spectrum, "site_data", None)
        if not isinstance(site_data, dict):
            raise ValueError("spectrum.site_data must be a dict")
        site_key = _build_site_cache_key(site_data)

        try:
            if site_key not in site_cache:
                site_cache[site_key] = _resolve_fixed_site(
                    db_rfm, dict(site_data), site_updates=site_updates
                )
        except ValueError as exc:
            # Only locality defects permit discarding a spectrum.
            if logger is not None:
                logger.warning_event(
                    "appanalise_site_resolution_discard",
                    component="appanalise_processing",
                    operation="resolve_spectrum_sites",
                    spectrum=getattr(spectrum, "description", None),
                    reason=str(exc),
                )
            continue

        spectrum.site_id = site_cache[site_key]
        retained.append(spectrum)

    if not retained:
        raise ValueError("No spectra remained after SITE resolution filtering")

    discarded = len(spectra) - len(retained)
    if discarded:
        bin_data["discarded_spectrum_count"] = (
            int(bin_data.get("discarded_spectrum_count", 0)) + discarded
        )
    _flush_fixed_site_updates(db_rfm, site_updates, logger=logger)
    bin_data["spectrum"] = retained
    return [spectrum.site_id for spectrum in retained]


def _build_site_cache_key(site_data: SiteData) -> SiteCacheKey:
    """Build a deterministic cache key for a point summary.

    Args:
        site_data (dict[str, Any]): Summary with longitude, latitude, and altitude.

    Returns:
        tuple[float, float, float]: Rounded coordinate cache key.
    """
    return (
        round(float(site_data["longitude"]), 6),
        round(float(site_data["latitude"]), 6),
        round(float(site_data["altitude"]), 3),
    )


def _resolve_fixed_site(
    db_rfm: dbHandlerRFM,
    site_data: SiteData,
    *,
    site_updates: FixedSiteUpdates,
) -> int:
    """Resolve fixed-site geography and queue samples for an existing site.

    Geocoding stays outside the spectrum transaction. New sites already receive
    their samples on insert; existing centroids are updated after file resolution.

    Args:
        db_rfm (dbHandlerRFM): RFDATA handler used for lookup and persistence.
        site_data (SiteData): Required numeric longitude, latitude and altitude;
            longitude_raw, latitude_raw and altitude_raw lists of float samples;
            optional integer nu_gnss_measurements and locality labels.
        site_updates (FixedSiteUpdates): Caller-owned buckets keyed by site ID,
            containing raw sample lists and an integer occurrences count.

    Returns:
        int: Resolved or inserted ``DIM_SPECTRUM_SITE.ID_SITE``.

    Raises:
        ValueError: If the geocoder or administrative catalog cannot resolve locality.
        Exception: If geocoding service, persistence or input contract fails.
    """
    site_id = db_rfm.get_site_id(site_data)

    if not site_id:
        site_data = geolocation_utils.reverse_geocode_site_data(
            site_data,
            user_agent=k.NOMINATIM_USER,
            required_address_field=k.REQUIRED_ADDRESS_FIELD,
        )
        # Geocoding may correct a latitude sign. Reuse the corrected point first.
        site_id = db_rfm.get_site_id(site_data)
        if not site_id:
            return db_rfm.insert_site(site_data, force_create_district=True)
    else:
        stored_site = db_rfm.get_site_geography(site_id)
        if stored_site.get("FK_DISTRICT") is None:
            refreshed_site_data = geolocation_utils.reverse_geocode_site_data(
                site_data,
                user_agent=k.NOMINATIM_USER,
                required_address_field=k.REQUIRED_ADDRESS_FIELD,
            )
            db_rfm.refresh_site_geography(
                site_id,
                refreshed_site_data,
                force_create_district=True,
            )

    _queue_fixed_site_update(site_updates, site_id, site_data)
    return site_id


def _queue_fixed_site_update(
    site_updates: FixedSiteUpdates,
    site_id: int,
    site_data: SiteData,
) -> None:
    """Accumulate raw GNSS samples for one SITE update.

    Args:
        site_updates (FixedSiteUpdates): Caller-owned raw sample lists and
            occurrences count keyed by site ID.
        site_id (int): Resolved SITE identifier.
        site_data (SiteData): Required longitude_raw, latitude_raw and altitude_raw
            lists of float samples from one point summary.

    Returns:
        None.
    """
    update_bucket = site_updates.setdefault(
        site_id,
        {
            "longitude_raw": [],
            "latitude_raw": [],
            "altitude_raw": [],
            "occurrences": 0,
        },
    )
    update_bucket["longitude_raw"].extend(site_data["longitude_raw"])
    update_bucket["latitude_raw"].extend(site_data["latitude_raw"])
    update_bucket["altitude_raw"].extend(site_data["altitude_raw"])
    update_bucket["occurrences"] += 1


def _flush_fixed_site_updates(
    db_rfm: dbHandlerRFM,
    site_updates: FixedSiteUpdates,
    *,
    logger: logger_type | None = None,
) -> None:
    """Apply one aggregated centroid update per SITE.

    Args:
        db_rfm (dbHandlerRFM): RFDATA handler used to update centroids.
        site_updates (FixedSiteUpdates): Raw longitude, latitude and altitude
            sample lists and integer occurrences count keyed by site ID.
        logger (logger_type | None): Optional structured event logger.

    Returns:
        None.

    Raises:
        Exception: If a SITE update fails.
    """
    for site_id, payload in site_updates.items():
        update_result = db_rfm.update_site(
            site=site_id,
            longitude_raw=payload["longitude_raw"],
            latitude_raw=payload["latitude_raw"],
            altitude_raw=payload["altitude_raw"],
        )

        if logger is None:
            continue

        logger.event(
            "site_gnss_updated",
            component="appanalise_processing",
            operation="flush_fixed_site_updates",
            site_id=site_id,
            latitude=round(update_result["latitude"], 6),
            longitude=round(update_result["longitude"], 6),
            altitude=round(update_result["altitude"], 2),
            occurrences=payload["occurrences"],
            gnss_samples=len(payload["longitude_raw"]),
        )


# --- Repository destination ---


def _build_repository_destination_path(
    db_rfm: dbHandlerRFM,
    bin_data: dict[str, Any],
    hostname_db: str,
) -> str:
    """Resolve the repository folder for the final artifact.

    Fixed payloads still inherit the traditional site-based repository path.
    When one processed file spans several resolved sites, the artifact is moved
    to a neutral appAnalise bucket instead of pretending that the whole file
    belongs to only one locality.

    Args:
        db_rfm (dbHandlerRFM): RFDATA handler used to build a SITE path.
        bin_data (dict[str, Any]): Payload with resolved spectrum SITE ids.
        hostname_db (str): Operational hostname for multi-SITE fallback paths.

    Returns:
        str: Absolute repository directory for the final artifact.
    """
    year = bin_data["spectrum"][0].start_dateidx.year
    site_ids = {
        getattr(spectrum, "site_id", None)
        for spectrum in bin_data["spectrum"]
        if getattr(spectrum, "site_id", None) is not None
    }

    if len(site_ids) == 1:
        site_id = next(iter(site_ids))
        return f"{k.REPO_FOLDER}/{year}/{db_rfm.build_path(site_id)}"

    host_key = _build_repository_hostname_key(hostname_db)
    return (
        f"{k.REPO_FOLDER}/{year}/"
        f"{k.APP_ANALISE_MULTI_SITE_REPO_SUBDIR}/{host_key}"
    )


def _build_repository_hostname_key(hostname: str) -> str:
    """Build a stable folder-safe hostname key.

    Args:
        hostname (str): Raw station hostname.

    Returns:
        str: Lowercase hostname safe for a repository directory name.
    """
    normalized = NON_ALNUM_RE.sub("_", (hostname or "").strip().lower()).strip("_")
    return normalized or "unknown_host"


# --- Dimension resolution ---


@dataclass(frozen=True)
class _EquipmentIdentity:
    """Store the catalog identity and type hint for one spectrum equipment.

    Use this immutable value as the per-payload cache key for equipment lookup.

    Attributes:
        persisted_equipment_name (str): Name stored in the equipment dimension.
        equipment_type_hint (str): Value used to infer the equipment type.
    """

    persisted_equipment_name: str
    equipment_type_hint: str


def _resolve_spectrum_dimensions(
    db_rfm: dbHandlerRFM,
    bin_data: dict[str, Any],
    *,
    hostname_db: str,
) -> None:
    """Resolve every FACT dimension before the insert phase.

    The payload carries one procedure and one default detector; equipment,
    trace type and measure unit are resolved per spectrum and cached per
    payload. Station-family rules stay in equipment resolution only, so the
    insert phase remains purely relational.

    Args:
        db_rfm (dbHandlerRFM): RFDATA handler used for dimension lookup.
        bin_data (dict[str, Any]): Payload with a ``method`` string and normalized
            spectra. Receives integer ``procedure_id`` and ``detector_id``; each
            spectrum receives ``equipment_id``, ``trace_type_id`` and
            ``measure_unit_id``.
        hostname_db (str): Operational hostname from the queue task.

    Returns:
        None.
    """
    spectra = bin_data["spectrum"]
    bin_data["procedure_id"] = db_rfm.insert_procedure(bin_data["method"])
    bin_data["detector_id"] = db_rfm.insert_detector_type(k.DEFAULT_DETECTOR)
    _resolve_spectrum_equipment(db_rfm, spectra, hostname_db=hostname_db)
    _resolve_spectrum_labels(spectra, "processing", "trace_type_id", db_rfm.insert_trace_type)
    _resolve_spectrum_labels(spectra, "level_unit", "measure_unit_id", db_rfm.insert_measure_unit)


def _resolve_spectrum_equipment(
    db_rfm: dbHandlerRFM,
    spectra: list[Any],
    *,
    hostname_db: str,
) -> None:
    """Attach the equipment identity and identifier to each spectrum.

    Args:
        db_rfm (dbHandlerRFM): RFDATA handler used for equipment lookup.
        spectra (list[Any]): Normalized spectra with ``equipment_name``; receive
            ``persisted_equipment_name`` and ``equipment_id``.
        hostname_db (str): Operational hostname from the queue task.

    Returns:
        None.
    """
    equipment_cache: dict[_EquipmentIdentity, int] = {}

    for spectrum in spectra:
        # One payload can expose the analyzer label while the catalog must
        # persist the operational station identity instead.
        identity = _resolve_equipment_identity(
            hostname_db=hostname_db,
            spectrum_equipment_name=spectrum.equipment_name,
        )
        if identity not in equipment_cache:
            equipment_cache[identity] = db_rfm.get_or_create_spectrum_equipment(
                identity.persisted_equipment_name,
                equipment_type_hint=identity.equipment_type_hint,
            )
        spectrum.persisted_equipment_name = identity.persisted_equipment_name
        spectrum.equipment_id = equipment_cache[identity]


def _resolve_equipment_identity(
    *,
    hostname_db: str,
    spectrum_equipment_name: str,
) -> _EquipmentIdentity:
    """Resolve the catalog identity and type hint for one spectrum.

    Most station families persist the equipment identity coming from the
    payload. ERMx/EMRx and UMS300 are the exceptions: the operational asset is
    the measurement station (the hostname), while the payload may expose the
    analyzer attached to it (UMS surfaces its embedded EB500). Those families
    persist the hostname and keep a fixed type hint, so a station keeps one
    stable equipment type even when its analyzer hardware changes.

    Args:
        hostname_db (str): Operational hostname from the queue task.
        spectrum_equipment_name (str): Receiver identifier in the payload.

    Returns:
        _EquipmentIdentity: Name to persist and type hint to resolve.

    Raises:
        errors.BinValidationError: If no canonical equipment identifier exists.
    """
    normalized_host = (hostname_db or "").strip().lower()
    for prefixes, type_hint in STATION_EQUIPMENT_FAMILIES:
        if normalized_host.startswith(prefixes):
            return _EquipmentIdentity(
                persisted_equipment_name=normalized_host,
                equipment_type_hint=type_hint,
            )

    canonical_name = canonicalize_equipment_identifier(
        spectrum_equipment_name.strip(),
        fallback_hostname=hostname_db,
    )
    return _EquipmentIdentity(
        persisted_equipment_name=canonical_name,
        equipment_type_hint=canonical_name,
    )


def _resolve_spectrum_labels(
    spectra: list[Any],
    label_field: str,
    id_field: str,
    lookup: Callable[[str], int],
) -> None:
    """Resolve a label dimension once per distinct label in the payload.

    Args:
        spectra (list[Any]): Normalized spectra exposing ``label_field``.
        label_field (str): Spectrum attribute holding the dimension label.
        id_field (str): Spectrum attribute that receives the dimension ID.
        lookup (Callable[[str], int]): Handler method that finds or creates the label.

    Returns:
        None.
    """
    cache: dict[str, int] = {}
    for spectrum in spectra:
        label = getattr(spectrum, label_field)
        if label not in cache:
            cache[label] = lookup(label)
        setattr(spectrum, id_field, cache[label])


# --- FACT insertion and analytical lineage ---


def _insert_spectra_batch(
    db_rfm: dbHandlerRFM,
    bin_data: dict[str, Any],
) -> list[int]:
    """Persist resolved spectra for one normalized payload.

    Spectrum deduplication:
        ``spectrum_id_cache`` guards against exact duplicate spectrum rows that
        can appear when one appAnalise payload contains repeated measurement
        periods. Duplicate rows reuse the same ``FACT_SPECTRUM.ID_SPECTRUM`` for
        later file-lineage registration instead of inserting two identical fact
        rows.

    SITE ownership:
        Each spectrum carries a pre-resolved ``site_id`` attribute set by
        ``_resolve_spectrum_sites()`` before this function is called.

    Dimension ownership:
        Procedure, detector, equipment, trace type, and measure unit must also
        be resolved before this function runs. The batch insert phase does not
        perform dimension inference or lookup anymore.

    Side effects:
        - Inserts into ``FACT_SPECTRUM`` within the caller's open RFDATA
          transaction.
        - Does **not** commit; the caller is responsible for the transaction.

    Args:
        db_rfm (dbHandlerRFM): RFDATA handler with an open transaction.
        bin_data (dict[str, Any]): Payload with resolved dimensions and spectra.

    Returns:
        list[int]: FACT identifiers, including repeated ids for in-batch duplicates.

    Raises:
        ValueError: If a spectrum is missing a resolved dimension.
        Exception: If fact persistence fails.
    """
    spectrum_id_cache = {}
    procedure_id = bin_data.get("procedure_id")
    detector_id = bin_data.get("detector_id")
    spectrum_ids = []

    # Each normalized spectrum already carries a resolved SITE reference here.
    for spectrum in bin_data["spectrum"]:
        _ensure_spectrum_dimensions_resolved(
            spectrum,
            procedure_id=procedure_id,
            detector_id=detector_id,
        )
        spectrum_row = _build_resolved_spectrum_row(
            spectrum,
            procedure_id=procedure_id,
            detector_id=detector_id,
        )

        # Deduplicate only inside the current payload. Historical idempotency
        # stays owned by the database layer and bridge table rules.
        spectrum_identity_key = _build_spectrum_identity_key(spectrum_row)

        if spectrum_identity_key in spectrum_id_cache:
            # Exact duplicate detected within this payload: reuse the existing
            # FACT_SPECTRUM row for the bridge table instead of inserting a
            # second identical measurement. appAnalise occasionally emits
            # repeated rows when trace periods overlap at file boundaries.
            spectrum_ids.append(spectrum_id_cache[spectrum_identity_key])
            continue

        spectrum_id = db_rfm.insert_spectrum(spectrum_row)
        spectrum_id_cache[spectrum_identity_key] = spectrum_id
        spectrum_ids.append(spectrum_id)

    return spectrum_ids


def _ensure_spectrum_dimensions_resolved(
    spectrum: Any,
    *,
    procedure_id: int | None,
    detector_id: int | None,
) -> None:
    """Fail fast when a spectrum reaches the insert phase unresolved.

    Args:
        spectrum (Any): Normalized spectrum with resolved dimension attributes.
        procedure_id (int | None): Payload procedure identifier.
        detector_id (int | None): Payload detector identifier.

    Returns:
        None.

    Raises:
        ValueError: If any required dimension identifier is missing.
    """
    for field in ("site_id", "equipment_id", "trace_type_id", "measure_unit_id"):
        if getattr(spectrum, field, None) is None:
            raise ValueError(f"spectrum.{field} must be resolved before insert")
    if procedure_id is None:
        raise ValueError("bin_data['procedure_id'] must be resolved before insert")
    if detector_id is None:
        raise ValueError("bin_data['detector_id'] must be resolved before insert")


def _build_resolved_spectrum_row(
    spectrum: Any,
    *,
    procedure_id: int,
    detector_id: int,
) -> dict[str, Any]:
    """Build one ``FACT_SPECTRUM`` row from a resolved spectrum.

    At this point every foreign key dimension has already been attached to the
    spectrum object. The helper's only job is to translate the normalized
    spectrum fields plus those pre-resolved ids into the row shape expected by
    ``db_rfm.insert_spectrum()``.

    Args:
        spectrum (Any): Spectrum with resolved SITE and dimension identifiers.
        procedure_id (int): Resolved procedure identifier.
        detector_id (int): Resolved detector identifier.

    Returns:
        dict[str, Any]: FACT row payload accepted by ``insert_spectrum``.
    """
    # Dimension ids are already attached to the spectrum object so this helper
    # can stay focused on shaping the FACT row only.
    return {
        "id_site": spectrum.site_id,
        "id_procedure": procedure_id,
        "id_detector_type": detector_id,
        "id_trace_type": spectrum.trace_type_id,
        "id_equipment": spectrum.equipment_id,
        "id_measure_unit": spectrum.measure_unit_id,
        "na_description": spectrum.description,
        "nu_freq_start": spectrum.start_mega,
        "nu_freq_end": spectrum.stop_mega,
        "dt_time_start": spectrum.start_dateidx,
        "dt_time_end": spectrum.stop_dateidx,
        "nu_sample_duration": k.DEFAULT_SAMPLE_DURATION,
        "nu_trace_count": spectrum.trace_length,
        "nu_trace_length": spectrum.ndata,
        "nu_rbw": spectrum.bw,
        "nu_att_gain": k.DEFAULT_ATTENUATION_GAIN,
        "js_metadata": json.dumps(spectrum.metadata),
        "allow_time_end_growth_dedup": spectrum.persisted_equipment_name.startswith(
            "rfeye"
        ),
    }


def _build_spectrum_identity_key(spectrum_row: dict[str, Any]) -> tuple[Any, ...]:
    """Build the in-memory idempotence key for one payload insert.

    RFeye fixed stations can republish the same logical spectrum from a
    growing partial file. For that family only, `DT_TIME_END` is excluded so
    later parses collapse into the original logical spectrum instead of
    creating a second in-batch identity.

    Args:
        spectrum_row (dict[str, Any]): Resolved FACT row payload.

    Returns:
        tuple[Any, ...]: Key that identifies one spectrum within the batch.
    """
    key = (
        spectrum_row["id_site"],
        spectrum_row["id_equipment"],
        spectrum_row["id_procedure"],
        spectrum_row["id_trace_type"],
        spectrum_row["nu_freq_start"],
        spectrum_row["nu_freq_end"],
        spectrum_row["dt_time_start"],
        spectrum_row["nu_trace_length"],
    )
    if spectrum_row.get("allow_time_end_growth_dedup"):
        return key
    return key + (spectrum_row["dt_time_end"],)


def _register_spectrum_file_lineage(
    db_rfm: dbHandlerRFM,
    *,
    spectrum_ids: list[int],
    hostname: str,
    volume: str,
    path: str,
    file_name: str,
    extension: str,
    size_kb: int,
    dt_created: datetime,
    dt_modified: datetime,
) -> None:
    """Register one analytical file artifact and link it to spectra.

    The processing flow records two file perspectives for the same spectra:
    the original source file seen on the host and the final canonical artifact
    stored in the repository. Both use the same analytical lineage pattern:
    insert one row in ``DIM_SPECTRUM_FILE`` and then bridge that file to every
    ``FACT_SPECTRUM`` id produced from the payload.

    This helper keeps that two-step contract in one place so the caller only
    needs to provide the file metadata that changes between host and repository
    contexts.

    Args:
        db_rfm (dbHandlerRFM): RFDATA handler used for file and bridge inserts.
        spectrum_ids (list[int]): Persisted ``FACT_SPECTRUM`` identifiers.
        hostname (str): Hostname associated with the artifact.
        volume (str): Logical storage volume.
        path (str): Artifact directory.
        file_name (str): Artifact filename.
        extension (str): Artifact extension.
        size_kb (int): Artifact size in kilobytes.
        dt_created (datetime): Artifact creation timestamp.
        dt_modified (datetime): Artifact modification timestamp.

    Returns:
        None.
    """
    file_id = db_rfm.insert_file(
        hostname=hostname,
        NA_VOLUME=volume,
        NA_PATH=path,
        NA_FILE=file_name,
        NA_EXTENSION=extension,
        VL_FILE_SIZE_KB=size_kb,
        DT_FILE_CREATED=dt_created,
        DT_FILE_MODIFIED=dt_modified,
    )
    db_rfm.insert_bridge_spectrum_file(spectrum_ids, [file_id])


# --- Completion log ---


def _log_processing_completion(
    *,
    logger: logger_type,
    service_name: str,
    task: dict,
    work_started_at: float,
    process_elapsed_sec: float,
    site_elapsed_sec: float,
    db_elapsed_sec: float,
    finalize_elapsed_sec: float,
    resolved_site_ids: list[int],
    spectrum_ids: list[int],
    new_path: str,
    file_meta: dict,
) -> None:
    """Emit the final event for a successful processing flow.

    The worker already owns the queue lifecycle logs. This helper records the
    domain-side completion snapshot after spectra were persisted and the final
    artifact was promoted. It keeps the per-file success log consistent across
    payload families while centralizing the timing and output fields in one
    place.

    Args:
        logger (logger_type): Structured event logger.
        service_name (str): Worker service name.
        task (dict): Queue task being processed.
        work_started_at (float): Monotonic start timestamp.
        process_elapsed_sec (float): appAnalise transport duration.
        site_elapsed_sec (float): SITE resolution duration.
        db_elapsed_sec (float): RFDATA persistence duration.
        finalize_elapsed_sec (float): Artifact promotion duration.
        resolved_site_ids (list[int]): SITE identifiers used by the payload.
        spectrum_ids (list[int]): Persisted FACT identifiers.
        new_path (str): Final repository directory.
        file_meta (dict): Final artifact metadata with ``file_name``.

    Returns:
        None.
    """
    elapsed_sec = round(time.monotonic() - work_started_at, 3)
    logger.task_phase(
        service_name,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=db_constants['tasks']['file_types']['processing'],
        phase="processing_completed",
        elapsed_sec=elapsed_sec,
        since_start_sec=elapsed_sec,
        file=task["filename"],
        export=task["export"],
        process_sec=process_elapsed_sec,
        site_sec=site_elapsed_sec,
        db_sec=db_elapsed_sec,
        finalize_sec=finalize_elapsed_sec,
        resolved_sites=len(set(resolved_site_ids)) if resolved_site_ids else 0,
        persisted_spectra=len(spectrum_ids),
        final_file=os.path.join(new_path, file_meta["file_name"]),
    )
