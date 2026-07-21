"""
Worker-side orchestration helpers for the appAnalise-backed FILE_TASK flow.

This module owns the domain plumbing around one processed payload:
    - export policy
    - SITE resolution
    - spectrum persistence
    - retry requeueing
    - final artifact resolution and history updates

The entrypoint can then stay focused on daemon lifecycle and the high-level
processing phases of one FILE_TASK attempt.

Reading guide:
    1. artifact/path helpers
    2. SITE and spectrum persistence
    3. retry requeue policy
    4. final success/error resolution of the FILE_TASK

That split mirrors the worker lifecycle in
`appCataloga_file_bin_process_appAnalise.py`.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
import re
from typing import TYPE_CHECKING, Any

import config as k
from appAnalise.payload_parser import canonicalize_equipment_identifier
from geopy.exc import GeocoderServiceError
from shared import errors, file_utils, geolocation_utils, tools

if TYPE_CHECKING:
    from appAnalise.appAnalise_connection import AppAnaliseConnection
    from db.dbHandlerRFM import dbHandlerRFM
    from shared.logging_utils import log as logger_type


NON_ALNUM_RE = re.compile(r"[^a-z0-9]+", re.IGNORECASE)
ERMX_FAMILY_PREFIXES = ("ermx", "emrx")
UMS_FAMILY_PREFIXES = ("ums",)
ERMX_EQUIPMENT_TYPE_HINT = "ermx"
UMS_EQUIPMENT_TYPE_HINT = "ums300"
# Module-level buffer used exclusively within `resolve_spectrum_sites()` call frames.
# `resolve_spectrum_sites()` sets this to a fresh dict at entry and restores None
# on exit (via try/finally), so `upsert_site()` can accumulate per-call GNSS
# aggregation without requiring an extra parameter across the call chain.
# This pattern is intentionally non-reentrant: it is safe only because every
# worker is a single-threaded process that processes one file at a time.
_FIXED_SITE_UPDATE_AGGREGATOR = None


SiteData = dict[str, Any]
SiteCacheKey = tuple[float, float, float, str]
FixedSiteUpdateBucket = dict[str, list[float] | int]
FixedSiteUpdates = dict[int, FixedSiteUpdateBucket]


@dataclass(frozen=True)
class EquipmentIdentity:
    """Catalog identity used to persist and type one spectrum equipment."""

    persisted_equipment_name: str
    equipment_type_hint: str


def is_transient_filesystem_error(exc: Exception) -> bool:
    """
    Return whether a filesystem failure is worth retrying later.

    These errors usually come from busy or stale files on shared storage. They
    are operationally noisy, but they do not mean the payload itself is bad.
    """
    return file_utils.is_transient_filesystem_error(exc)


def should_export(hostname: str) -> bool:
    """
    Decide whether appAnalise should export a `.mat` artifact for this host.

    Station-family rule:
        - CelPlan (`CWSM` hostname prefix): accept the `.mat` export as the
          canonical artifact.
        - All other families: keep the original binary payload as the
          canonical artifact. appAnalise is invoked for metadata extraction
          only, not file export.

    The function stays isolated so future host-family policy changes do not
    leak into the worker loop.
    """
    normalized = (hostname or "").strip().lower()
    return normalized.startswith("cwsm")


def resolve_equipment_persistence_identity(
    *,
    hostname_db: str,
    spectrum_equipment_name: str | None,
) -> EquipmentIdentity:
    """
    Resolve the catalog identity and equipment-type hint for one spectrum.

    Most station families must persist the equipment identity coming from the
    payload itself. ERMx/EMRx and UMS300 are the exceptions: the operational
    asset is the Windows measurement station (the hostname), while the payload
    may expose the analyzer model attached to that station. We persist the
    station hostname as the equipment name for those families and keep a
    separate type hint only for equipment-type inference.

    ERMx/EMRx stations are classified as the station family itself, not by the
    attached analyzer model. That keeps one stable equipment type even when the
    same station changes analyzer hardware over time.

    Returns:
        EquipmentIdentity:
            - persisted_equipment_name: stored in
              ``DIM_SPECTRUM_EQUIPMENT.NA_EQUIPMENT``
            - equipment_type_hint: used to infer ``FK_EQUIPMENT_TYPE``
    """
    normalized_host = (hostname_db or "").strip().lower()
    raw_spectrum_name = (
        str(spectrum_equipment_name).strip()
        if spectrum_equipment_name is not None
        else ""
    )

    if normalized_host.startswith(ERMX_FAMILY_PREFIXES):
        # Guard preserved for future callers that might skip the startswith check.
        # Unreachable in the current call path: startswith() returning True
        # guarantees normalized_host is non-empty.
        if not normalized_host:
            raise ValueError("hostname_db is required for ERMx/EMRx equipment resolution")

        return EquipmentIdentity(
            persisted_equipment_name=normalized_host,
            equipment_type_hint=ERMX_EQUIPMENT_TYPE_HINT,
        )

    if normalized_host.startswith(UMS_FAMILY_PREFIXES):
        # appAnalise surfaces the embedded EB500 receiver string here, but the
        # cataloged asset is the UMS station itself.
        if not normalized_host:
            raise ValueError("hostname_db is required for UMS equipment resolution")

        return EquipmentIdentity(
            persisted_equipment_name=normalized_host,
            equipment_type_hint=UMS_EQUIPMENT_TYPE_HINT,
        )

    canonical_name = canonicalize_equipment_identifier(
        raw_spectrum_name,
        fallback_hostname=hostname_db,
    )
    return EquipmentIdentity(
        persisted_equipment_name=canonical_name,
        equipment_type_hint=canonical_name,
    )


def _build_repository_hostname_key(hostname: str) -> str:
    """Build a stable folder-safe hostname key for repository fallback paths."""
    normalized = NON_ALNUM_RE.sub("_", (hostname or "").strip().lower()).strip("_")
    return normalized or "unknown_host"


def upsert_site(db_rfm: dbHandlerRFM, site_data: SiteData) -> int:
    """
    Resolve or create one SITE referenced by a normalized spectrum row.

    SITE ownership is intentionally separate from the larger spectrum
    transaction. Geocoding or SITE creation can be slower and noisier than the
    actual spectrum inserts, so the worker resolves these references first and
    enters the RFDATA transaction only once every spectrum already knows which
    `ID_SITE` it will use.
    """
    site_id = db_rfm.get_site_id(site_data)

    if site_id:
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

        # Fixed stations still refine their centroid over time. Mobile captures
        # carry a prepared GEOGRAPHIC_PATH and therefore keep the stored site
        # geometry stable once the summary polygon is already known.
        if not site_data.get("geographic_path"):
            if _FIXED_SITE_UPDATE_AGGREGATOR is not None:
                _queue_fixed_site_update(
                    _FIXED_SITE_UPDATE_AGGREGATOR,
                    site_id,
                    site_data,
                )
            else:
                db_rfm.update_site(
                    site=site_id,
                    longitude_raw=site_data["longitude_raw"],
                    latitude_raw=site_data["latitude_raw"],
                    altitude_raw=site_data["altitude_raw"],
                )
        return site_id

    site_data = geolocation_utils.reverse_geocode_site_data(
        site_data,
        user_agent=k.NOMINATIM_USER,
        required_address_field=k.REQUIRED_ADDRESS_FIELD,
    )

    # Reverse geocoding may repair malformed fixed coordinates such as an
    # inverted latitude sign. Re-check after enrichment so we reuse an
    # existing SITE row instead of inserting the corrected point twice.
    site_id = db_rfm.get_site_id(site_data)
    if site_id:
        if not site_data.get("geographic_path"):
            if _FIXED_SITE_UPDATE_AGGREGATOR is not None:
                _queue_fixed_site_update(
                    _FIXED_SITE_UPDATE_AGGREGATOR,
                    site_id,
                    site_data,
                )
            else:
                db_rfm.update_site(
                    site=site_id,
                    longitude_raw=site_data["longitude_raw"],
                    latitude_raw=site_data["latitude_raw"],
                    altitude_raw=site_data["altitude_raw"],
                )
        return site_id

    return db_rfm.insert_site(
        site_data,
        force_create_district=True,
    )


def _queue_fixed_site_update(
    site_updates: FixedSiteUpdates,
    site_id: int,
    site_data: SiteData,
) -> None:
    """Accumulate raw GNSS samples for one fixed site update.

    One payload can contain many spectra that map to the same fixed SITE. This
    helper merges their raw longitude, latitude, and altitude samples into one
    in-memory bucket so ``resolve_spectrum_sites()`` can call
    ``db_rfm.update_site()`` once per site after the full payload is resolved.
    """
    update_bucket = site_updates.setdefault(
        int(site_id),
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
    """Apply one aggregated centroid update per fixed site and log once."""
    for site_id, payload in site_updates.items():
        update_result = db_rfm.update_site(
            site=site_id,
            longitude_raw=payload["longitude_raw"],
            latitude_raw=payload["latitude_raw"],
            altitude_raw=payload["altitude_raw"],
            log_result=False,
        )

        if logger is None:
            continue

        occurrences = int(payload["occurrences"])
        if update_result["action"] == "skipped_limit":
            logger.event(
                "site_gnss_update_skipped_limit",
                component="appanalise_processing",
                operation="flush_fixed_site_updates",
                site_id=site_id,
                existing_gnss=update_result["existing_gnss"],
                limit=update_result["limit"],
                occurrences=occurrences,
            )
            continue

        logger.event(
            "site_gnss_updated",
            component="appanalise_processing",
            operation="flush_fixed_site_updates",
            site_id=site_id,
            latitude=round(update_result["latitude"], 6),
            longitude=round(update_result["longitude"], 6),
            altitude=round(update_result["altitude"], 2),
            occurrences=occurrences,
            gnss_samples=len(payload["longitude_raw"]),
        )


def _build_spectrum_identity_key(spectrum_row: dict[str, Any]) -> tuple[Any, ...]:
    """Build the in-memory idempotence key used during one payload insert.

    RFeye fixed stations can republish the same logical spectrum from a
    growing partial file. For that family only, `DT_TIME_END` is excluded so
    later parses collapse into the original logical spectrum instead of
    creating a second in-batch identity.
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


def _build_site_cache_key(site_data: SiteData) -> SiteCacheKey:
    """Build one deterministic cache key for a normalized site summary."""
    return (
        round(float(site_data["longitude"]), 6),
        round(float(site_data["latitude"]), 6),
        round(float(site_data["altitude"]), 3),
        site_data.get("geographic_path") or "",
    )


def _is_infrastructure_site_resolution_error(exc: Exception) -> bool:
    """
    Return whether a SITE-resolution failure should still abort the whole file.

    Spectrum-level discard is meant for deterministic locality defects in one
    row, not for shared infrastructure failures such as DB/geocoder outages.
    """
    if isinstance(exc, GeocoderServiceError):
        return True

    message = str(exc).lower()
    infrastructure_markers = (
        "database",
        "dbhandler",
        "connection",
        "cursor",
        "rollback",
        "transaction",
        "server has gone away",
        "geocoder",
        "timed out",
        "temporarily unavailable",
        "service unavailable",
    )
    return any(marker in message for marker in infrastructure_markers)


def resolve_spectrum_sites(
    db_rfm: dbHandlerRFM,
    bin_data: dict[str, Any],
    *,
    logger: logger_type | None = None,
) -> list[int]:
    """
    Resolve ``ID_SITE`` for every normalized spectrum row before insertion.

    Multiple spectra in one file can point to different localities. The worker
    therefore resolves SITE ownership per spectrum, while caching repeated
    fixed/mobile summaries so the same payload does not geocode or update the
    same SITE over and over again.
    """
    # One processed payload can repeat the same fixed point or the same mobile
    # bounding geometry many times. Cache the SITE resolution locally so a
    # single file does not geocode or touch the same SITE more than once.
    global _FIXED_SITE_UPDATE_AGGREGATOR

    site_cache: dict[SiteCacheKey, int] = {}
    fixed_site_updates: FixedSiteUpdates = {}
    resolved_ids: list[int] = []
    resolved_spectra: list[Any] = []
    discarded_here = 0
    previous_site_update_aggregator = _FIXED_SITE_UPDATE_AGGREGATOR
    _FIXED_SITE_UPDATE_AGGREGATOR = fixed_site_updates

    try:
        # SITE ownership is now a property of each normalized spectrum row, not of
        # the file as a whole. That keeps mixed-location payloads honest when they
        # eventually become FACT_SPECTRUM rows.
        for spectrum in bin_data["spectrum"]:
            site_data = getattr(spectrum, "site_data", None)

            if not isinstance(site_data, dict):
                raise ValueError("spectrum.site_data must be a dict")

            site_key = _build_site_cache_key(site_data)

            try:
                if site_key not in site_cache:
                    # The first time a spatial summary appears in this payload, resolve
                    # it against DIM_SPECTRUM_SITE and keep the resulting ID cached for
                    # all later spectra that share the same summary.
                    site_cache[site_key] = int(upsert_site(db_rfm, dict(site_data)))
            except Exception as exc:
                # Site detection can fail for one spectrum because that one GPS
                # summary does not map cleanly to a locality. In that case we
                # discard only the bad spectrum and preserve the rest of the file.
                #
                # Shared infrastructure failures are still fatal because discarding
                # around a DB/geocoder outage would silently hide a system problem.
                if _is_infrastructure_site_resolution_error(exc):
                    raise

                discarded_here += 1

                if logger is not None:
                    logger.warning_event(
                        "appanalise_site_resolution_discard",
                        component="appanalise_processing",
                        operation="resolve_spectrum_sites",
                        spectrum=getattr(spectrum, "description", None),
                        reason=str(exc),
                    )
                continue

            # Persist the resolved SITE directly on the spectrum object so the
            # insert phase can stay simple and purely relational.
            spectrum.site_id = site_cache[site_key]
            resolved_ids.append(spectrum.site_id)
            resolved_spectra.append(spectrum)

        if not resolved_spectra:
            raise ValueError(
                "No spectra remained after SITE resolution filtering"
            )

        if discarded_here:
            # Keep the normalized payload self-describing in memory without pushing
            # discard bookkeeping into JS_METADATA or relational columns.
            bin_data["discarded_spectrum_count"] = (
                int(bin_data.get("discarded_spectrum_count", 0)) + discarded_here
            )

        _flush_fixed_site_updates(
            db_rfm,
            fixed_site_updates,
            logger=logger,
        )

        bin_data["spectrum"] = resolved_spectra

        return resolved_ids
    finally:
        _FIXED_SITE_UPDATE_AGGREGATOR = previous_site_update_aggregator


def resolve_spectrum_procedure(db_rfm: dbHandlerRFM, bin_data: dict[str, Any]) -> int:
    """Resolve the payload-level procedure dimension once per file.

    appAnalise exposes one collection method at the payload level, not per
    spectrum row. This helper resolves that dimension once and stores the
    resulting id on ``bin_data`` so later steps can reuse it without
    repeating the lookup.
    """
    procedure_id = db_rfm.insert_procedure(bin_data["method"])
    bin_data["procedure_id"] = procedure_id
    return procedure_id


def resolve_spectrum_detector(db_rfm: dbHandlerRFM, bin_data: dict[str, Any]) -> int:
    """Resolve the payload-level detector dimension once per file.

    The current pipeline persists one default detector for every imported
    spectrum. Keeping this lookup outside ``insert_spectra_batch()`` makes the
    later FACT insert phase consume only already-resolved dimension ids.
    """
    detector_id = db_rfm.insert_detector_type(k.DEFAULT_DETECTOR)
    bin_data["detector_id"] = detector_id
    return detector_id


def resolve_spectrum_equipment(
    db_rfm: dbHandlerRFM,
    bin_data: dict[str, Any],
    *,
    hostname_db: str,
) -> dict[EquipmentIdentity, int]:
    """Resolve equipment identity and ids for every normalized spectrum.

    This is the only dimension resolver that still carries station-family
    rules. Hybrid families such as ERMx and UMS persist the operational host
    identity but infer the equipment type from a different payload field. The
    helper attaches the resolved ``equipment_id`` and related metadata to each
    spectrum so the insert phase stays purely relational.
    """
    equipment_cache: dict[EquipmentIdentity, int] = {}

    for spectrum in bin_data["spectrum"]:
        # One payload can expose the analyzer label while the catalog must
        # persist the operational station identity instead.
        equipment_identity = resolve_equipment_persistence_identity(
            hostname_db=hostname_db,
            spectrum_equipment_name=(
                getattr(spectrum, "equipment_name", None) or hostname_db
            ),
        )

        # Keep the resolved identity on the spectrum and reuse one equipment id
        # per identity inside the current payload.
        spectrum.persisted_equipment_name = equipment_identity.persisted_equipment_name
        spectrum.equipment_type_hint = equipment_identity.equipment_type_hint
        spectrum.equipment_id = equipment_cache.get(equipment_identity)

        if spectrum.equipment_id is None:
            spectrum.equipment_id = db_rfm.get_or_create_spectrum_equipment(
                equipment_identity.persisted_equipment_name,
                equipment_type_hint=equipment_identity.equipment_type_hint,
            )
            equipment_cache[equipment_identity] = spectrum.equipment_id

    return equipment_cache


def resolve_spectrum_trace_types(
    db_rfm: dbHandlerRFM,
    bin_data: dict[str, Any],
) -> dict[str, int]:
    """Resolve trace-type ids for every normalized spectrum.

    Multiple spectra in one payload often reuse the same processing label.
    This helper deduplicates those lookups within the current batch and stores
    the resolved ``trace_type_id`` directly on each spectrum object.
    """
    trace_type_cache: dict[str, int] = {}

    for spectrum in bin_data["spectrum"]:
        trace_name = spectrum.processing
        if trace_name not in trace_type_cache:
            trace_type_cache[trace_name] = db_rfm.insert_trace_type(trace_name)
        spectrum.trace_type_id = trace_type_cache[trace_name]

    return trace_type_cache


def resolve_spectrum_measure_units(
    db_rfm: dbHandlerRFM,
    bin_data: dict[str, Any],
) -> dict[str, int]:
    """Resolve measure-unit ids for every normalized spectrum.

    appAnalise may repeat the same engineering unit across many spectra in one
    file. This helper resolves each distinct unit once per batch and annotates
    every spectrum with the resulting ``measure_unit_id`` for the insert phase.
    """
    measure_unit_cache: dict[str, int] = {}

    for spectrum in bin_data["spectrum"]:
        measure_unit = spectrum.level_unit
        if measure_unit not in measure_unit_cache:
            measure_unit_cache[measure_unit] = db_rfm.insert_measure_unit(
                measure_unit
            )
        spectrum.measure_unit_id = measure_unit_cache[measure_unit]

    return measure_unit_cache


def _ensure_spectrum_dimensions_resolved(
    spectrum: Any,
    *,
    procedure_id: int,
    detector_id: int,
) -> None:
    """Validate that one spectrum is ready for FACT insertion.

    ``insert_spectra_batch()`` no longer resolves dimensions on demand. It now
    assumes SITE, equipment, trace type, measure unit, procedure, and detector
    were all resolved earlier in the flow. This guard fails fast when that
    contract is broken.
    """
    # `insert_spectra_batch()` now assumes all dimension lookups happened
    # earlier in the flow. Failing fast here keeps that contract explicit.
    if getattr(spectrum, "site_id", None) is None:
        raise ValueError("spectrum.site_id must be resolved before insert")
    if getattr(spectrum, "equipment_id", None) is None:
        raise ValueError("spectrum.equipment_id must be resolved before insert")
    if getattr(spectrum, "trace_type_id", None) is None:
        raise ValueError("spectrum.trace_type_id must be resolved before insert")
    if getattr(spectrum, "measure_unit_id", None) is None:
        raise ValueError("spectrum.measure_unit_id must be resolved before insert")
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
    """Build one ``FACT_SPECTRUM`` payload from a resolved spectrum row.

    At this point every foreign key dimension has already been attached to the
    spectrum object. The helper's only job is to translate the normalized
    spectrum fields plus those pre-resolved ids into the row shape expected by
    ``db_rfm.insert_spectrum()``.
    """
    metadata = spectrum.metadata if hasattr(spectrum, "metadata") else {}
    persisted_equipment_name = getattr(spectrum, "persisted_equipment_name", "")

    # Dimension ids are already attached to the spectrum object so this helper
    # can stay focused on shaping the FACT row only.
    return {
        "id_site": spectrum.site_id,
        "id_procedure": procedure_id,
        "id_detector_type": detector_id,
        "id_trace_type": spectrum.trace_type_id,
        "id_equipment": spectrum.equipment_id,
        "id_measure_unit": spectrum.measure_unit_id,
        "na_description": getattr(spectrum, "description", None),
        "nu_freq_start": spectrum.start_mega,
        "nu_freq_end": spectrum.stop_mega,
        "dt_time_start": spectrum.start_dateidx,
        "dt_time_end": spectrum.stop_dateidx,
        "nu_sample_duration": k.DEFAULT_SAMPLE_DURATION,
        "nu_trace_count": spectrum.trace_length,
        "nu_trace_length": spectrum.ndata,
        "nu_rbw": getattr(spectrum, "bw", None),
        "nu_att_gain": k.DEFAULT_ATTENUATION_GAIN,
        "js_metadata": json.dumps(metadata),
        "allow_time_end_growth_dedup": persisted_equipment_name.startswith("rfeye"),
    }


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
    log_success: bool = True,
) -> int:
    """Register one analytical file artifact and link it to persisted spectra.

    The processing flow records two file perspectives for the same spectra:
    the original source file seen on the host and the final canonical artifact
    stored in the repository. Both use the same analytical lineage pattern:
    insert one row in ``DIM_SPECTRUM_FILE`` and then bridge that file to every
    ``FACT_SPECTRUM`` id produced from the payload.

    This helper keeps that two-step contract in one place so the caller only
    needs to provide the file metadata that changes between host and repository
    contexts.
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
        log_success=log_success,
    )
    db_rfm.insert_bridge_spectrum_file(spectrum_ids, [file_id])
    return file_id


def _reset_reprocessed_file_lineage(
    db_rfm: dbHandlerRFM,
    *,
    task: dict[str, Any],
    repository_path: str,
    repository_file_name: str,
) -> dict[str, int]:
    """Clear stale lineage before the same file is processed again.

    Reprocessing can change the analytical shape of one source artifact, not
    only its time coverage. The DB handler therefore removes the existing
    host/repository lineage for that artifact pair inside the current
    transaction so the fresh appAnalise payload becomes the only source of
    truth for the subsequent FACT and bridge inserts.
    """
    return db_rfm.reset_reprocessed_file_lineage(
        host_volume=task["hostname_db"],
        host_path=task["host_path"],
        host_file=task["host_file_name"],
        repository_volume=k.REPO_VOLUME_NAME,
        repository_path=repository_path,
        repository_file=repository_file_name,
    )


def insert_spectra_batch(
    db_rfm: dbHandlerRFM,
    bin_data: dict[str, Any],
) -> list[int]:
    """
    Persist all already-resolved spectra for one normalized payload.

    Spectrum deduplication:
        ``spectrum_id_cache`` guards against exact duplicate spectrum rows that
        can appear when one appAnalise payload contains repeated measurement
        periods. Duplicate rows reuse the same ``FACT_SPECTRUM.ID_SPECTRUM`` for
        later file-lineage registration instead of inserting two identical fact
        rows.

    SITE ownership:
        Each spectrum carries a pre-resolved ``site_id`` attribute set by
        ``resolve_spectrum_sites()`` before this function is called.

    Dimension ownership:
        Procedure, detector, equipment, trace type, and measure unit must also
        be resolved before this function runs. The batch insert phase does not
        perform dimension inference or lookup anymore.

    Side effects:
        - Inserts into ``FACT_SPECTRUM`` within the caller's open RFDATA
          transaction.
        - Does **not** commit; the caller is responsible for the transaction.
    """
    spectrum_id_cache = {}
    procedure_id = int(bin_data.get("procedure_id") or 0)
    detector_id = int(bin_data.get("detector_id") or 0)
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
    """Emit the final structured log event for one successful processing flow.

    The worker already owns the queue lifecycle logs. This helper records the
    domain-side completion snapshot after spectra were persisted and the final
    artifact was promoted. It keeps the per-file success log consistent across
    payload families while centralizing the timing and output fields in one
    place.
    """
    logger.task_phase(
        service_name,
        host_id=task["host_id"],
        task_id=task["file_task_id"],
        task_type=k.FILE_TASK_PROCESS_TYPE,
        phase="processing_completed",
        elapsed_sec=round(time.monotonic() - work_started_at, 3),
        since_start_sec=round(time.monotonic() - work_started_at, 3),
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


def run_processing_flow(
    db_rfm: dbHandlerRFM,
    task: dict[str, Any],
    app_analise: AppAnaliseConnection,
    *,
    logger: logger_type,
    service_name: str,
) -> dict[str, Any]:
    """
    Orchestrate the appAnalise domain pipeline for one claimed ``FILE_TASK``.

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
    resolved_site_ids = resolve_spectrum_sites(db_rfm, bin_data, logger=logger)
    site_elapsed_sec = round(time.monotonic() - phase_started_at, 3)

    # The DB phase writes FACT rows first, then records both host and
    # repository file lineage explicitly around the artifact promotion step.
    phase_started_at = time.monotonic()
    new_path = build_repository_destination_path(
        db_rfm,
        bin_data,
        task["hostname_db"],
    )
    db_rfm.begin_transaction()

    # Reprocessing must clear the previous lineage for this artifact pair
    # before the fresh spectra are inserted. The new payload is authoritative.
    _reset_reprocessed_file_lineage(
        db_rfm,
        task=task,
        repository_path=new_path,
        repository_file_name=file_meta["file_name"],
    )

    # Resolve all dimension ids before inserting FACT rows so the insert step
    # does not need to know station-family or analyzer-specific rules.
    resolve_spectrum_procedure(db_rfm, bin_data)
    resolve_spectrum_detector(db_rfm, bin_data)
    resolve_spectrum_equipment(
        db_rfm,
        bin_data,
        hostname_db=task["hostname_db"],
    )
    resolve_spectrum_trace_types(db_rfm, bin_data)
    resolve_spectrum_measure_units(db_rfm, bin_data)
    
    # Insert spectra after all dimension ids were resolved in memory.
    spectrum_ids = insert_spectra_batch(
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
        log_success=False,
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

    # The worker finalizes queue state later. The domain returns only the
    # artifacts needed to persist DONE/ERROR against FILE_TASK history.
    return {
        "file_meta": file_meta,
        "new_path": new_path,
        "bin_data": bin_data,
        "resolved_site_ids": resolved_site_ids,
        "spectrum_ids": spectrum_ids,
        "answer": answer,
        "payload": payload,
    }


def build_repository_destination_path(
    db_rfm: dbHandlerRFM,
    bin_data: dict[str, Any],
    hostname_db: str,
) -> str:
    """
    Resolve the canonical repository folder for the final processing artifact.

    Fixed payloads still inherit the traditional site-based repository path.
    When one processed file spans several resolved sites, the artifact is moved
    to a neutral appAnalise bucket instead of pretending that the whole file
    belongs to only one locality.
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
