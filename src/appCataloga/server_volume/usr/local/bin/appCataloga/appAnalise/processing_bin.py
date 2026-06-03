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

import config as k
from appAnalise.payload_parser import canonicalize_equipment_identifier
from geopy.exc import GeocoderServiceError
from shared import errors, file_utils, geolocation_utils, tools


NON_ALNUM_RE = re.compile(r"[^a-z0-9]+", re.IGNORECASE)
ERMX_FAMILY_PREFIXES = ("ermx", "emrx")
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


def should_map_host_source_file(hostname: str) -> bool:
    """
    Decide whether the worker should register the original host-side source file.

    Only station families with a trustworthy 1:1 lineage between the queued
    FILE_TASK and the physical source file should create a `host_file` entry.
    Aggregated analyzer exports can still persist spectra and the canonical
    repository artifact without inventing a fake host-side lineage row.
    """
    normalized = (hostname or "").strip().lower()

    return any(
        normalized.startswith(family)
        for family in k.APP_ANALISE_SOURCE_LINEAGE_FAMILIES
    )


def resolve_equipment_persistence_identity(
    *,
    hostname_db: str,
    spectrum_equipment_name: str | None,
) -> tuple[str, str]:
    """
    Resolve the catalog identity and equipment-type hint for one spectrum.

    Most station families use the same canonical identifier both as the
    persisted equipment name and as the type-inference source. ERMx/EMRx is
    the exception: the operational asset is the Windows measurement station
    (the hostname), while the payload's equipment string names the analyzer
    model attached to that station. We persist the station hostname as the
    equipment name but use the analyzer string to infer the equipment type.

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

    canonical_name = canonicalize_equipment_identifier(
        raw_spectrum_name or hostname_db,
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
            logger.entry(
                f"Site {site_id} reached {update_result['existing_gnss']} GNSS "
                f"measurements (limit={update_result['limit']}). No update "
                f"performed. occurrences={occurrences}"
            )
            continue

        logger.entry(
            f"Updated site {site_id}: "
            f"lat={update_result['latitude']:.6f}, "
            f"lon={update_result['longitude']:.6f}, "
            f"alt={update_result['altitude']:.2f}, "
            f"occurrences={occurrences}, "
            f"gnss_samples={len(payload['longitude_raw'])}"
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
                    logger.warning(
                        "appanalise_site_resolution_discard "
                        f"spectrum={getattr(spectrum, 'description', None)!r} "
                        f"reason={exc}"
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
    db_rfm,
    bin_data,
    hostname_db,
    host_path,
    host_file_name,
    extension,
    vl_file_size_kb,
    dt_created,
    dt_modified,
):
    """
    Persist source-file lineage (when applicable) and all normalized spectra.

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
    host_file_id = None
    detector_id = db_rfm.insert_detector_type(k.DEFAULT_DETECTOR)
    trace_type_cache = {}
    equipment_cache = {}
    measure_unit_cache = {}
    spectrum_id_cache = {}
    duplicate_spectrum_hits = 0
    spectrum_batch_started = time.monotonic()

    if should_map_host_source_file(hostname_db):
        # Only 1:1 station families register a host-side lineage file. Mobile
        # analyzer exports can aggregate several physical source files, so
        # forcing one synthetic host file row would misrepresent provenance.
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

    for spectrum in bin_data["spectrum"]:
        site_id = getattr(spectrum, "site_id", None)

        if site_id is None:
            raise ValueError("spectrum.site_id must be resolved before insert")

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
        spectrum_identity_key = _build_spectrum_identity_key(spectrum_row)

        if spectrum_identity_key in spectrum_id_cache:
            # Exact duplicate detected within this payload: reuse the existing
            # FACT_SPECTRUM row for the bridge table instead of inserting a
            # second identical measurement. appAnalise occasionally emits
            # repeated rows when trace periods overlap at file boundaries.
            spectrum_ids.append(spectrum_id_cache[spectrum_identity_key])
            duplicate_spectrum_hits += 1
            continue

        spectrum_id = db_rfm.insert_spectrum(spectrum_row)
        spectrum_id_cache[spectrum_identity_key] = spectrum_id
        spectrum_ids.append(spectrum_id)

    if host_file_id is not None:
        db_rfm.insert_bridge_spectrum_file(spectrum_ids, [host_file_id])

    if hasattr(db_rfm, "log"):
        db_rfm.log.event(
            "fact_spectrum_batch_staged",
            spectra=len(spectrum_ids),
            unique_spectrum_keys=len(spectrum_id_cache),
            duplicate_spectrum_hits=duplicate_spectrum_hits,
            unique_trace_types=len(trace_type_cache),
            unique_equipments=len(equipment_cache),
            unique_measure_units=len(measure_unit_cache),
            bridge_file_links=(len(spectrum_ids) if host_file_id is not None else 0),
            duration_sec=round(time.monotonic() - spectrum_batch_started, 3),
        )
    return spectrum_ids


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


