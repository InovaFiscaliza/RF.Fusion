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
`appCataloga_file_bin_proces_appAnalise.py`.
"""

from __future__ import annotations

import errno
import json
import os
import time
from datetime import datetime
import re

import config as k
from appAnalise.payload_parser import canonicalize_equipment_identifier
from geopy.exc import GeocoderServiceError
from shared import errors, geolocation_utils, tools


NON_ALNUM_RE = re.compile(r"[^a-z0-9]+", re.IGNORECASE)
TRANSIENT_FILESYSTEM_ERRNOS = {
    errno.EBUSY,
    errno.EAGAIN,
    errno.ESTALE,
    errno.ETXTBSY,
}

ERMX_FAMILY_PREFIXES = ("ermx", "emrx")


def _structured_error_fields_for_handler(err, *, message=None):
    """Build explicit structured error fields for worker persistence."""
    return errors.persisted_error_fields_from_handler(
        err,
        message=message,
        clear_when_empty=True,
    )


def is_transient_filesystem_error(exc: Exception) -> bool:
    """
    Return whether a filesystem failure is worth retrying later.

    These errors usually come from busy or stale files on shared storage. They
    are operationally noisy, but they do not mean the payload itself is bad.
    """
    return isinstance(exc, OSError) and exc.errno in TRANSIENT_FILESYSTEM_ERRNOS


def file_move(filename, path, new_path, *, refresh_mtime: bool = False):
    """
    Move one artifact into its next canonical directory.

    This helper intentionally does not decide *why* the file is being moved.
    Callers use it for three different semantic outcomes:
        - promote the final artifact into the repository tree
        - quarantine the original source payload
        - quarantine superseded exported leftovers

    `refresh_mtime=True` is reserved for quarantine folders whose retention is
    driven by filesystem age. In that case we "touch" the moved file so the GC
    clock starts when the artifact entered quarantine, not when it was first
    created by the station or by appAnalise.
    """
    source = f"{path}/{filename}"
    target = f"{new_path}/{filename}"

    os.makedirs(new_path, exist_ok=True)

    for attempt in range(3):
        try:
            os.rename(source, target)
            break
        except OSError as exc:
            if not is_transient_filesystem_error(exc) or attempt == 2:
                raise OSError(
                    exc.errno,
                    f"{exc.strerror}: {source} -> {target}",
                ) from exc
            time.sleep(0.5)

    if refresh_mtime:
        for attempt in range(3):
            try:
                os.utime(target, None)
                break
            except OSError as exc:
                if not is_transient_filesystem_error(exc) or attempt == 2:
                    raise OSError(
                        exc.errno,
                        f"{exc.strerror}: {target}",
                    ) from exc
                time.sleep(0.5)

    return {"filename": filename, "path": new_path}


def should_export(hostname: str) -> bool:
    """
    Decide whether appAnalise should export a `.mat` artifact for this host.

    Today the rule is mostly station-family based:
        - RFeye: keep the original payload as the canonical artifact
        - CW/CelPlan and others: accept/export the derived artifact

    The function stays isolated so future host-family policy changes do not
    leak into the worker loop.
    """
    normalized = (hostname or "").lower()

    if "rfeye" in normalized:
        return False

    if "cw" in normalized:
        return True

    return True


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

    Most station families can persist the same canonical identifier both as the
    equipment name and as the type-inference source. ERMx/EMRx is different:
    the operational asset is the Windows station itself, while the payload
    receiver string names the analyzer model attached to that station.

    Returns:
        tuple[str, str]:
            - equipment_name persisted in `DIM_SPECTRUM_EQUIPMENT.NA_EQUIPMENT`
            - equipment_type_hint used to infer `FK_EQUIPMENT_TYPE`
    """
    normalized_host = (hostname_db or "").strip().lower()
    raw_spectrum_name = (
        str(spectrum_equipment_name).strip()
        if spectrum_equipment_name is not None
        else ""
    )

    if normalized_host.startswith(ERMX_FAMILY_PREFIXES):
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


def resolve_history_file_metadata(
    file_was_processed,
    file_meta,
    server_name,
    extension,
    vl_file_size_kb,
    dt_created,
    dt_modified,
):
    """
    Resolve which file metadata should be written to FILE_TASK_HISTORY.

    History should describe the artifact that became canonical after the
    attempt:
        - success: the processed/exported artifact when one exists
        - ordinary error: the original server payload that was attempted
        - post-export semantic error: the exported artifact rejected by RF.Fusion
    """
    if file_was_processed and file_meta:
        return build_history_metadata_from_file_meta(file_meta)

    return {
        "name": server_name,
        "extension": extension,
        "size_kb": vl_file_size_kb,
        "dt_created": dt_created,
        "dt_modified": dt_modified,
    }


def build_history_metadata_from_file_meta(file_meta):
    """
    Project worker file metadata into the FILE_TASK_HISTORY column set.

    Several resolution branches already operate on the richer worker-side
    metadata dict (`file_name`, `file_path`, timestamps, ...). This helper
    extracts only the history-facing subset so error and success branches can
    reuse the same shape without retyping the mapping.
    """
    return {
        "name": file_meta["file_name"],
        "extension": file_meta["extension"],
        "size_kb": file_meta["size_kb"],
        "dt_created": file_meta["dt_created"],
        "dt_modified": file_meta["dt_modified"],
    }


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
        # Fixed stations still refine their centroid over time. Mobile captures
        # carry a prepared GEOGRAPHIC_PATH and therefore keep the stored site
        # geometry stable once the summary polygon is already known.
        if not site_data.get("geographic_path"):
            db_rfm.update_site(
                site=site_id,
                longitude_raw=site_data["longitude_raw"],
                latitude_raw=site_data["latitude_raw"],
                altitude_raw=site_data["altitude_raw"],
            )
        return site_id

    location = geolocation_utils.reverse_geocode_with_retry(
        site_data,
        user_agent=k.NOMINATIM_USER,
    )
    site_data = geolocation_utils.map_location_to_site_data(
        location,
        site_data,
        k.REQUIRED_ADDRESS_FIELD,
    )
    return db_rfm.insert_site(site_data)


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
    site_cache = {}
    resolved_ids = []
    resolved_spectra = []
    discarded_here = 0

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
                site_cache[site_key] = upsert_site(db_rfm, dict(site_data))
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

    bin_data["spectrum"] = resolved_spectra

    return resolved_ids


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
    Persist source lineage (when applicable) and all normalized spectra.

    SITE ownership now lives on each spectrum row, not on the file as a whole.
    The batch still shares one processing procedure, but every spectrum may
    resolve to a different `ID_SITE`.
    """
    host_file_id = None

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

        # appAnalise may carry per-spectrum metadata blobs such as Antenna and
        # Others that do not map to first-class RFDATA columns yet. We
        # preserve only that upstream metadata in JSON so the processing step
        # stays lossless without mixing in worker-side telemetry like discard
        # counters.
        metadata = spectrum.metadata if hasattr(spectrum, "metadata") else {}

        spectrum_ids.append(
            db_rfm.insert_spectrum(
                {
                    "id_site": site_id,
                    "id_procedure": procedure_id,
                    "id_detector_type": db_rfm.insert_detector_type(
                        k.DEFAULT_DETECTOR
                    ),
                    "id_trace_type": db_rfm.insert_trace_type(
                        spectrum.processing
                    ),
                    "id_equipment": db_rfm.get_or_create_spectrum_equipment(
                        persisted_equipment_name,
                        equipment_type_hint=equipment_type_hint,
                    ),
                    "id_measure_unit": db_rfm.insert_measure_unit(
                        spectrum.level_unit
                    ),
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
                }
            )
        )

    if host_file_id is not None:
        db_rfm.insert_bridge_spectrum_file(spectrum_ids, [host_file_id])
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


def return_task_to_pending(db_bp, file_task_id, err):
    """
    Requeue the current FILE_TASK after a transient appAnalise failure.

    This is the only retry path in the worker. It exists for dependency
    outages, not for definitive payload defects.

    The helper is retained for future policies where transient failures should
    return to the live queue automatically instead of being frozen for manual
    review.
    """
    message = tools.compose_message(
        task_type=k.FILE_TASK_PROCESS_TYPE,
        task_status=k.TASK_PENDING,
        detail="APP_ANALISE transient failure, task returned for retry",
        error=err.format_error(),
    )

    db_bp.file_task_update(
        task_id=file_task_id,
        NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
        NU_STATUS=k.TASK_PENDING,
        DT_FILE_TASK=datetime.now(),
        NA_MESSAGE=message,
        **_structured_error_fields_for_handler(err, message=message),
    )


def freeze_task_for_manual_review(
    db_bp,
    *,
    file_task_id,
    host_id,
    host_file_name,
    host_path,
    err,
    detail,
):
    """
    Freeze one PROCESS FILE_TASK and its processing history for manual review.

    This helper keeps the live row and `FILE_TASK_HISTORY` aligned on
    `TASK_FROZEN` while preserving the underlying artifact on disk.
    """
    message = tools.compose_message(
        task_type=k.FILE_TASK_PROCESS_TYPE,
        task_status=k.TASK_FROZEN,
        detail=detail,
        error=err.format_error(),
    )

    db_bp.file_task_update(
        task_id=file_task_id,
        NU_TYPE=k.FILE_TASK_PROCESS_TYPE,
        NU_STATUS=k.TASK_FROZEN,
        NU_PID=None,
        DT_FILE_TASK=datetime.now(),
        NA_MESSAGE=message,
        **_structured_error_fields_for_handler(err, message=message),
    )

    db_bp.file_history_update(
        task_type=k.FILE_TASK_PROCESS_TYPE,
        host_id=host_id,
        host_file_path=host_path,
        host_file_name=host_file_name,
        NU_STATUS_PROCESSING=k.TASK_FROZEN,
        NA_MESSAGE=message,
        **_structured_error_fields_for_handler(err, message=message),
    )

    db_bp.host_task_statistics_create(host_id=host_id)


def freeze_task_after_processing_timeout(
    db_bp,
    *,
    file_task_id,
    host_id,
    host_file_name,
    host_path,
    err,
):
    """
    Freeze one PROCESS FILE_TASK after appAnalise returned a structured timeout.

    Unlike transport outages, a `ReadTimeout` reply means appAnalise stayed
    responsive enough to answer, but this specific payload exceeded the remote
    processing budget. We keep both the live FILE_TASK and the processing phase
    in FILE_TASK_HISTORY on hold for manual review instead of retrying or
    finalizing the file as a definitive processing error.
    """
    freeze_task_for_manual_review(
        db_bp,
        file_task_id=file_task_id,
        host_id=host_id,
        host_file_name=host_file_name,
        host_path=host_path,
        err=err,
        detail="APP_ANALISE read timeout, task frozen for manual review",
    )


def is_same_file(file_a, file_b):
    """
    Check whether two metadata dictionaries point to the same filesystem path.
    """
    if not file_a or not file_b:
        return False

    path_a = os.path.normpath(file_a["full_path"])
    path_b = os.path.normpath(file_b["full_path"])
    return path_a == path_b


def move_file_if_present(
    file_meta,
    destination_path,
    *,
    refresh_mtime: bool = False,
):
    """
    Move a file when it still exists and return its new metadata.

    The helper is intentionally forgiving: by the time final resolution runs,
    the worker may already have partially moved or deleted one of the
    candidate artifacts. Returning `None` keeps callers idempotent.

    Quarantine moves may opt into `refresh_mtime=True` so later filesystem
    sweeps treat the move time as the start of retention.
    """
    if not file_meta or not os.path.exists(file_meta["full_path"]):
        return None

    file_move(
        filename=file_meta["file_name"],
        path=file_meta["file_path"],
        new_path=destination_path,
        refresh_mtime=refresh_mtime,
    )

    moved_meta = dict(file_meta)
    moved_meta["file_path"] = destination_path
    moved_meta["full_path"] = os.path.join(
        destination_path,
        file_meta["file_name"],
    )
    return moved_meta


def build_resolved_files_trash_path():
    """
    Return the dedicated quarantine for export-resolved leftovers.

    This keeps "main trash" and "superseded derived artifacts" separated, which
    makes post-mortem inspection less noisy.
    """
    return (
        f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}/"
        f"{k.RESOLVED_FILES_TRASH_SUBDIR}"
    )


def finalize_successful_processing(
    db_rfm,
    spectrum_ids,
    bin_data,
    hostname_db,
    file_meta,
    source_file_meta,
    export,
    filename,
    *,
    logger=None,
):
    """
    Move the final artifact, register it in RFDATA, and retire superseded input.

    Success has two filesystem concerns:
        1. promote the canonical artifact into the repository tree
        2. retire any superseded source payload when export created a new file
    """
    # Repository placement stays site-based when the processed payload maps to
    # one locality only. Multi-site payloads move into a neutral appAnalise
    # bucket so the archive does not pretend the whole file belongs to a
    # single arbitrary site.
    new_path = build_repository_destination_path(
        db_rfm=db_rfm,
        bin_data=bin_data,
        hostname_db=hostname_db,
    )
    final_file_meta = move_file_if_present(file_meta, new_path)

    if final_file_meta is None:
        raise FileNotFoundError(
            f"Final output file unavailable: {file_meta}"
        )

    server_file_id = db_rfm.insert_file(
        hostname=hostname_db,
        NA_VOLUME=k.REPO_VOLUME_NAME,
        NA_PATH=new_path,
        NA_FILE=final_file_meta["file_name"],
        NA_EXTENSION=final_file_meta["extension"],
        VL_FILE_SIZE_KB=final_file_meta["size_kb"],
        DT_FILE_CREATED=final_file_meta["dt_created"],
        DT_FILE_MODIFIED=final_file_meta["dt_modified"],
    )

    db_rfm.insert_bridge_spectrum_file(spectrum_ids, [server_file_id])

    if export and not is_same_file(source_file_meta, final_file_meta):
        # Export mode can produce a new canonical artifact (`.mat`) while the
        # original source payload remains on disk. Once the server file is
        # registered, the source payload becomes only an operator-inspection
        # artifact and is moved out of the live inbox. Refresh its mtime so
        # `trash/resolved_files` retention starts when it entered quarantine.
        move_file_if_present(
            source_file_meta,
            build_resolved_files_trash_path(),
            refresh_mtime=True,
        )

    if logger is not None:
        logger.event(
            "processing_completed",
            file=filename,
            export=export,
            final_file=final_file_meta["full_path"],
        )

    return new_path, final_file_meta


def finalize_task_resolution(
    db_bp,
    *,
    file_task_id,
    host_id,
    host_file_name,
    host_path,
    server_name,
    extension,
    vl_file_size_kb,
    dt_created,
    dt_modified,
    file_was_processed,
    new_path,
    file_meta,
    source_file_meta,
    export,
    err,
):
    """
    Apply the final FILE_TASK resolution once retry is no longer an option.

    This is the definitive branch of the worker lifecycle: the live FILE_TASK
    is retired, the history row is updated, and any on-disk leftovers are
    moved into their canonical success or trash locations.

    Success and fatal error share this helper on purpose so queue state,
    FILE_TASK_HISTORY, and filesystem cleanup cannot drift apart.

    Important error contract:
        - if no stable export exists, the original payload remains the error artifact
        - if appAnalise already produced a stable export and RF.Fusion rejects it
          later, the export becomes the error artifact and the original source
          is treated as a resolved input
    """
    history_meta_override = None
    history_server_path = new_path

    if not file_was_processed and new_path is None:
        trash_path = f"{k.REPO_FOLDER}/{k.TRASH_FOLDER}"
        resolved_trash_path = build_resolved_files_trash_path()
        distinct_export_artifact = (
            export
            and file_meta
            and not is_same_file(file_meta, source_file_meta)
        )

        if distinct_export_artifact:
            # If appAnalise already produced a stable exported artifact, then
            # RF.Fusion's later semantic rejection is about that derived file,
            # not about the original source payload. Preserve the source as a
            # resolved input and treat the export as the error artifact.
            resolved_source_meta = move_file_if_present(
                source_file_meta,
                resolved_trash_path,
                refresh_mtime=True,
            )
            trashed_export_meta = move_file_if_present(
                file_meta,
                trash_path,
            )

            if trashed_export_meta:
                new_path = trashed_export_meta["file_path"]
                history_server_path = trashed_export_meta["file_path"]
                history_meta_override = build_history_metadata_from_file_meta(
                    trashed_export_meta
                )
            else:
                # If the export unexpectedly disappeared before finalization,
                # fall back to the original payload as the operator-facing
                # error artifact instead of leaving history without a file.
                fallback_source_meta = resolved_source_meta or source_file_meta
                trashed_source_meta = move_file_if_present(
                    fallback_source_meta,
                    trash_path,
                )
                if trashed_source_meta:
                    new_path = trashed_source_meta["file_path"]
                    history_server_path = trashed_source_meta["file_path"]
        else:
            # Fatal processing failures without a stable exported artifact keep
            # the original payload as the error artifact for operator review.
            trashed_source_meta = move_file_if_present(
                source_file_meta,
                trash_path,
            )

            if trashed_source_meta:
                new_path = trashed_source_meta["file_path"]
                history_server_path = trashed_source_meta["file_path"]

    # The processing stage is terminal for the live queue row. Success or
    # failure is represented afterwards only in FILE_TASK_HISTORY.
    db_bp.file_task_delete(task_id=file_task_id)

    status = k.TASK_DONE if file_was_processed else k.TASK_ERROR

    history_meta = history_meta_override or resolve_history_file_metadata(
        file_was_processed=file_was_processed,
        file_meta=file_meta,
        server_name=server_name,
        extension=extension,
        vl_file_size_kb=vl_file_size_kb,
        dt_created=dt_created,
        dt_modified=dt_modified,
    )

    if (
        not file_was_processed
        and history_server_path is None
        and source_file_meta
    ):
        # If resolution failed before we managed to move anything, history
        # should still point to the original server payload location so the
        # operator can find the artifact referenced by the error row.
        history_server_path = source_file_meta["file_path"]

    # The history message is the durable operator-facing explanation of what
    # happened in this processing attempt, so success and error both converge
    # into one composed message here.
    na_message = tools.compose_message(
        task_type=k.FILE_TASK_PROCESS_TYPE,
        task_status=status,
        path=new_path if file_was_processed else None,
        name=history_meta["name"] if file_was_processed else None,
        error=err.format_error() if err.triggered else None,
    )
    processed_at = datetime.now()
    structured_error_fields = _structured_error_fields_for_handler(
        err if err.triggered else None,
        message=na_message,
    )

    db_bp.file_history_update(
        host_id=host_id,
        task_type=k.FILE_TASK_PROCESS_TYPE,
        host_file_name=host_file_name,
        host_file_path=host_path,
        DT_PROCESSED=processed_at,
        NA_SERVER_FILE_NAME=history_meta["name"],
        NA_SERVER_FILE_PATH=history_server_path,
        NA_EXTENSION=history_meta["extension"],
        VL_FILE_SIZE_KB=history_meta["size_kb"],
        DT_FILE_CREATED=history_meta["dt_created"],
        DT_FILE_MODIFIED=history_meta["dt_modified"],
        NU_STATUS_PROCESSING=status,
        NA_MESSAGE=na_message,
        **structured_error_fields,
    )

    # Statistics are updated after history so host-level counters see the same
    # finalized state that the operator would read from FILE_TASK_HISTORY.
    db_bp.host_task_statistics_create(host_id=host_id)
    return {
        "status": status,
        "new_path": history_server_path,
        "history_meta": history_meta,
        "final_file": (
            os.path.join(history_server_path, history_meta["name"])
            if history_server_path else None
        ),
    }
