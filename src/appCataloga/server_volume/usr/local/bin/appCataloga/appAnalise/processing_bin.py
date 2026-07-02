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
UMS_EQUIPMENT_TYPE_HINT = "ums300"
# Module-level buffer used exclusively within `resolve_spectrum_sites()` call frames.
# `resolve_spectrum_sites()` sets this to a fresh dict at entry and restores None
# on exit (via try/finally), so `upsert_site()` can accumulate per-call GNSS
# aggregation without requiring an extra parameter across the call chain.
# This pattern is intentionally non-reentrant: it is safe only because every
# worker is a single-threaded process that processes one file at a time.
_FIXED_SITE_UPDATE_AGGREGATOR = None


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
) -> tuple[str, str]:
    """
    Resolve the catalog identity and equipment-type hint for one spectrum.

    Most station families must persist the equipment identity coming from the
    payload itself. ERMx/EMRx and UMS300 are the exceptions: the operational
    asset is the Windows measurement station (the hostname), while the payload
    may expose the analyzer model attached to that station. We persist the
    station hostname as the equipment name for those families and keep a
    separate type hint only for equipment-type inference.

    Returns:
        tuple[str, str]:
            - equipment_name: persisted in ``DIM_SPECTRUM_EQUIPMENT.NA_EQUIPMENT``
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

        type_hint = raw_spectrum_name or hostname_db
        return normalized_host, type_hint

    if normalized_host.startswith(UMS_FAMILY_PREFIXES):
        # appAnalise surfaces the embedded EB500 receiver string here, but the
        # cataloged asset is the UMS station itself.
        if not normalized_host:
            raise ValueError("hostname_db is required for UMS equipment resolution")

        return normalized_host, UMS_EQUIPMENT_TYPE_HINT

    canonical_name = canonicalize_equipment_identifier(
        raw_spectrum_name,
        fallback_hostname=hostname_db,
    )
    return canonical_name, canonical_name


def _build_repository_hostname_key(hostname: str) -> str:
    """
    Normalize host labels before using them in repository fallback paths.
    """
    normalized = NON_ALNUM_RE.sub("_", (hostname or "").strip().lower()).strip("_")
    return normalized or "unknown_host"


def upsert_site(db_rfm, site_data):
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
    return db_rfm.insert_site(
        site_data,
        force_create_district=True,
    )


def _queue_fixed_site_update(site_updates, site_id, site_data):
    """Aggregate fixed-site raw GNSS samples for one later `update_site()`."""
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


def _flush_fixed_site_updates(db_rfm, site_updates, *, logger=None):
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


def _build_spectrum_identity_key(spectrum_row):
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


def _build_site_cache_key(site_data):
    """
    Build a deterministic cache key for one spectrum-level site summary.
    """
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


def resolve_spectrum_sites(db_rfm, bin_data, *, logger=None):
    """
    Resolve `ID_SITE` for every normalized spectrum row before DB insertion.

    Multiple spectra in one file can point to different localities. The worker
    therefore resolves SITE ownership per spectrum, while caching repeated
    fixed/mobile summaries so the same payload does not geocode or update the
    same SITE over and over again.
    """
    # One processed payload can repeat the same fixed point or the same mobile
    # bounding geometry many times. Cache the SITE resolution locally so a
    # single file does not geocode or touch the same SITE more than once.
    global _FIXED_SITE_UPDATE_AGGREGATOR

    site_cache = {}
    fixed_site_updates = {}
    resolved_ids = []
    resolved_spectra = []
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


def insert_spectra_batch(
    db_rfm: dbHandlerRFM,
    bin_data: dict[str, Any],
    hostname_db: str,
    host_path: str,
    host_file_name: str,
    extension: str,
    vl_file_size_kb: int,
    dt_created: datetime,
    dt_modified: datetime,
    *,
    logger: logger_type | None = None,
) -> list[int]:
    """
    Persist source-file lineage and all normalized spectra.

    Dimension rows (trace type, equipment, measure unit) are resolved once per
    unique value and cached locally for the duration of this batch, so the
    same file does not issue repeated ``INSERT IGNORE`` lookups for identical
    dimension keys.

    Spectrum deduplication:
        ``spectrum_id_cache`` guards against exact duplicate spectrum rows that
        can appear when one appAnalise payload contains repeated measurement
        periods. Duplicate rows reuse the same ``FACT_SPECTRUM.ID_SPECTRUM`` for
        the bridge table instead of inserting two identical fact rows.

    SITE ownership:
        Each spectrum carries a pre-resolved ``site_id`` attribute set by
        ``resolve_spectrum_sites()`` before this function is called. The batch
        therefore does not perform any geocoding or SITE lookups.

    Side effects:
        - Inserts into ``DIM_SPECTRUM_FILE``, ``FACT_SPECTRUM``, and
          ``BRIDGE_SPECTRUM_FILE`` within the caller's open RFDATA transaction.
        - Does **not** commit; the caller is responsible for the transaction.
    """
    # These caches keep one batch from repeating the same dimension lookups.
    host_file_id = None
    detector_id = db_rfm.insert_detector_type(k.DEFAULT_DETECTOR)
    trace_type_cache = {}
    equipment_cache = {}
    measure_unit_cache = {}
    spectrum_id_cache = {}

    # The original host-side file is always part of the lineage. The finalized
    # repository artifact is registered later in the flow as a second file row.
    host_file_id = db_rfm.insert_file(
        hostname=hostname_db,
        NA_VOLUME=hostname_db,
        NA_PATH=host_path,
        NA_FILE=host_file_name,
        NA_EXTENSION=extension,
        VL_FILE_SIZE_KB=vl_file_size_kb,
        DT_FILE_CREATED=dt_created,
        DT_FILE_MODIFIED=dt_modified,
    )

    procedure_id = db_rfm.insert_procedure(bin_data["method"])
    spectrum_ids = []

    # Each normalized spectrum already carries a resolved SITE reference here.
    for spectrum in bin_data["spectrum"]:
        site_id = getattr(spectrum, "site_id", None)

        if site_id is None:
            raise ValueError("spectrum.site_id must be resolved before insert")

        # Hybrid station families may persist the host identity while still
        # inferring type from a different payload field.
        equipment_name = (
            getattr(spectrum, "equipment_name", None)
            or hostname_db
        )
        persisted_equipment_name, equipment_type_hint = resolve_equipment_persistence_identity(
            hostname_db=hostname_db,
            spectrum_equipment_name=equipment_name,
        )
        equipment_cache_key = (
            persisted_equipment_name,
            equipment_type_hint,
        )
        trace_name = spectrum.processing
        measure_unit = spectrum.level_unit

        if trace_name not in trace_type_cache:
            trace_type_cache[trace_name] = db_rfm.insert_trace_type(trace_name)

        if equipment_cache_key not in equipment_cache:
            # Cache by both persisted name and hint because hybrid families can
            # reuse one hostname with analyzer metadata that differs from the key.
            equipment_cache[equipment_cache_key] = (
                db_rfm.get_or_create_spectrum_equipment(
                    persisted_equipment_name,
                    equipment_type_hint=equipment_type_hint,
                )
            )

        if measure_unit not in measure_unit_cache:
            measure_unit_cache[measure_unit] = db_rfm.insert_measure_unit(
                measure_unit
            )

        # appAnalise may carry per-spectrum metadata blobs such as Antenna and
        # Others that do not map to first-class RFDATA columns yet. We
        # preserve only that upstream metadata in JSON so the processing step
        # stays lossless without mixing in worker-side telemetry like discard
        # counters.
        metadata = spectrum.metadata if hasattr(spectrum, "metadata") else {}
        spectrum_row = {
            "id_site": site_id,
            "id_procedure": procedure_id,
            "id_detector_type": detector_id,
            "id_trace_type": trace_type_cache[trace_name],
            "id_equipment": equipment_cache[equipment_cache_key],
            "id_measure_unit": measure_unit_cache[measure_unit],
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

    if host_file_id is not None:
        db_rfm.insert_bridge_spectrum_file(spectrum_ids, [host_file_id])

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
    """Emit one standardized per-file completion log for the domain flow."""
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
    task: dict,
    app_analise: AppAnaliseConnection,
    *,
    logger: logger_type,
    service_name: str,
) -> dict:
    """
    Execute the full appAnalise domain pipeline for one FILE_TASK.

    The worker entrypoint owns queue lifecycle and total elapsed time. This
    domain flow owns the processing stages and emits one completion log for
    the per-file domain pipeline.
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

    # The DB phase writes the host-side source lineage and the spectra rows
    # before any repository artifact is promoted to its final location.
    phase_started_at = time.monotonic()
    new_path = build_repository_destination_path(
        db_rfm,
        bin_data,
        task["hostname_db"],
    )
    db_rfm.begin_transaction()
    spectrum_ids = insert_spectra_batch(
        db_rfm=db_rfm,
        bin_data=bin_data,
        hostname_db=task["hostname_db"],
        host_path=task["host_path"],
        host_file_name=task["host_file_name"],
        extension=task["extension"],
        vl_file_size_kb=task["vl_file_size_kb"],
        dt_created=task["dt_created"],
        dt_modified=task["dt_modified"],
        logger=logger,
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
    server_file_id = db_rfm.insert_file(
        hostname=task["hostname_db"],
        NA_VOLUME=k.REPO_VOLUME_NAME,
        NA_PATH=new_path,
        NA_FILE=file_meta["file_name"],
        NA_EXTENSION=file_meta["extension"],
        VL_FILE_SIZE_KB=file_meta["size_kb"],
        DT_FILE_CREATED=file_meta["dt_created"],
        DT_FILE_MODIFIED=file_meta["dt_modified"],
        log_success=False,
    )
    db_rfm.insert_bridge_spectrum_file(spectrum_ids, [server_file_id])
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


def build_repository_destination_path(db_rfm, bin_data, hostname_db):
    """
    Resolve the canonical repository folder for the finalized processing artifact.

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
