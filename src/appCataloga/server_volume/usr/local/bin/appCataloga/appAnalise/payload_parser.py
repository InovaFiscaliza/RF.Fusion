"""
Payload validation and normalization helpers for appAnalise responses.

`appAnalise_connection.py` owns socket/protocol transport. This module owns the
semantic contract of the returned payload and the output artifact selected from
that payload.
"""

from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Iterable
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, Optional

import config as k
from shared import errors


RFEYE_CANONICAL_RE = re.compile(r"(rfeye\d{6})", re.IGNORECASE)
CWSM_SHORT_RE = re.compile(r"^cwsm(\d{6})$", re.IGNORECASE)
CWSM_LONG_RE = re.compile(r"^cwsm(\d{8})$", re.IGNORECASE)
CWSM_SHORT_TO_LONG_PREFIX = {
    "211": "2110",
    "212": "2112",
    "220": "2201",
}
CWSM_SHORT_TO_LONG_OVERRIDES = {
    # Legacy host naming diverges from the canonical receiver emitted by the
    # processing chain for this fixed station.
    "211007": "22010007",
}


def _normalize_equipment_text(value: Any) -> str:
    """
    Normalize one candidate equipment/host label for matching.
    """
    if value is None:
        return ""

    return str(value).strip().lower()


def _canonicalize_single_equipment_identifier(value: Any) -> str | None:
    """
    Resolve one raw identifier into the canonical RFDATA equipment key.

    RFeye identifiers already expose a stable 1:1 key inside longer receiver
    strings. CelPlan/CWSM identifiers can surface in two operational forms:
        - short host form: `CWSM211005`
        - long receiver form: `cwsm21100005`

    The FACT layer should persist the long receiver form whenever the family is
    known. Malformed CWSM values return `None` so the caller can fall back to a
    more trustworthy host-level identifier instead of persisting garbage.
    """
    normalized = _normalize_equipment_text(value)

    if not normalized or normalized in AppAnalisePayloadParser.INVALID_HOSTNAME_VALUES:
        return None

    rfeye_match = RFEYE_CANONICAL_RE.search(normalized)
    if rfeye_match:
        return rfeye_match.group(1).lower()

    if not normalized.startswith("cwsm"):
        return normalized

    long_match = CWSM_LONG_RE.fullmatch(normalized)
    if long_match:
        return normalized

    short_match = CWSM_SHORT_RE.fullmatch(normalized)
    if not short_match:
        return None

    digits = short_match.group(1)
    override = CWSM_SHORT_TO_LONG_OVERRIDES.get(digits)
    if override:
        return f"cwsm{override}"

    family_prefix = CWSM_SHORT_TO_LONG_PREFIX.get(digits[:3])
    if not family_prefix:
        return None

    station_suffix = f"{int(digits[-3:]):04d}"
    return f"cwsm{family_prefix}{station_suffix}"


def canonicalize_equipment_identifier(
    raw_value: Any,
    *,
    fallback_hostname: Any = None,
) -> str:
    """
    Return the canonical equipment identifier used by RFDATA persistence.

    The per-spectrum receiver from appAnalise has priority. If that value is
    malformed for a known family, the worker may safely fall back to the host
    identifier already trusted by the orchestration layer.
    """
    primary = _canonicalize_single_equipment_identifier(raw_value)
    if primary:
        return primary

    fallback = _canonicalize_single_equipment_identifier(fallback_hostname)
    if fallback:
        return fallback

    raise errors.BinValidationError(
        "Unable to resolve canonical equipment identifier"
    )


class AppAnalisePayloadParser:
    """
    Validate, normalize, and materialize appAnalise response payloads.

    Responsibilities:
        1. validate top-level protocol semantics after JSON decoding
        2. classify service-reported source-file errors
        3. normalize `Spectra` into canonical RF.Fusion `bin_data`
        4. resolve the filesystem artifact that becomes the processing output
    """

    PROCESSOR_NAME = "appAnalise"
    INVALID_HOSTNAME_VALUES = {
        "none", "(none)", "null", "(null)", "unknown", ""
    }
    SOURCE_FILE_MISSING_SNIPPETS = (
        "filenotfound",
        "nosuchfile",
        "cannotfindfile",
        "pathnotfound",
    )
    READ_TIMEOUT_SNIPPETS = (
        "filereadhandler:readtimeout",
        "readtimeout",
    )
    GPS_SENTINEL_VALUE = -1

    @staticmethod
    def _coerce_non_negative_float_field(field_name: str, value: Any) -> float:
        """
        Parse one optional numeric field as a non-negative float.
        """
        parsed = AppAnalisePayloadParser._coerce_float_field(field_name, value)

        if parsed < 0:
            raise errors.BinValidationError(
                f"APP_ANALISE returned negative {field_name}: {value}"
            )

        return parsed

    @staticmethod
    def _format_wkt_coordinate(value: float) -> str:
        """
        Format coordinates deterministically so identical site summaries compare equal.
        """
        return f"{float(value):.6f}"

    def _is_mobile_task(self, task_name: str | None) -> bool:
        """
        Return whether the related-file task name clearly points to mobile capture.
        """
        normalized = (task_name or "").strip().lower()

        if not normalized:
            return False

        return any(
            marker in normalized for marker in k.APP_ANALISE_MOBILE_TASK_MARKERS
        )

    def _build_geographic_path(
        self,
        *,
        latitude: float,
        longitude: float,
        latitude_std: float,
        longitude_std: float,
    ) -> str | None:
        """
        Build a deterministic bounding polygon for mobile captures.

        The current goal is catalog visibility, not full route reconstruction.
        A simple rectangle derived from the spectrum centroid and GPS standard
        deviation is enough to distinguish mobile captures from fixed stations.
        """
        # We intentionally summarize mobility as a coarse bounding box instead
        # of trying to reconstruct the real route. For RF.Fusion's current
        # catalog use case, "where this spectrum moved around" is enough.
        lat_delta = latitude_std * k.APP_ANALISE_MOBILE_PATH_STD_MULTIPLIER
        lon_delta = longitude_std * k.APP_ANALISE_MOBILE_PATH_STD_MULTIPLIER

        if lat_delta <= 0 or lon_delta <= 0:
            return None

        # The polygon is deterministic and axis-aligned so equal mobile
        # summaries compare equal and can be cached/deduplicated downstream.
        min_lat = self._format_wkt_coordinate(latitude - lat_delta)
        max_lat = self._format_wkt_coordinate(latitude + lat_delta)
        min_lon = self._format_wkt_coordinate(longitude - lon_delta)
        max_lon = self._format_wkt_coordinate(longitude + lon_delta)

        # WKT polygons must repeat the first coordinate at the end to close
        # the outer ring explicitly.
        return (
            "POLYGON(("
            f"{min_lon} {min_lat}, "
            f"{max_lon} {min_lat}, "
            f"{max_lon} {max_lat}, "
            f"{min_lon} {max_lat}, "
            f"{min_lon} {min_lat}"
            "))"
        )

    def _build_spectrum_site_data(
        self,
        *,
        gps: Dict[str, Any],
        task_name: str | None,
        latitude: float,
        longitude: float,
        altitude: float,
    ) -> Dict[str, Any]:
        """
        Build the site summary attached to one normalized spectrum row.

        Fixed captures stay point-only. Mobile captures still keep the same
        centroid in `GEO_POINT`, but also carry a bounding polygon in
        `GEOGRAPHIC_PATH` so the database can distinguish them later.
        """
        # appAnalise already returns a summarized GPS object per spectrum. We
        # keep that granularity here because a single processed file may mix
        # several localities and receivers.
        latitude_std = self._coerce_non_negative_float_field(
            "GPS.Latitude_std",
            gps.get("Latitude_std", 0),
        )
        longitude_std = self._coerce_non_negative_float_field(
            "GPS.Longitude_std",
            gps.get("Longitude_std", 0),
        )

        # Mobility can be explicit in the task name or inferred from GNSS
        # dispersion. Either signal is enough to switch this spectrum from a
        # pure point to a point + geographic path summary.
        is_mobile = (
            self._is_mobile_task(task_name)
            or latitude_std >= k.APP_ANALISE_MOBILE_GPS_STD_THRESHOLD
            or longitude_std >= k.APP_ANALISE_MOBILE_GPS_STD_THRESHOLD
        )

        return {
            # Keep the centroid and raw samples even for mobile captures. The
            # centroid still feeds GEO_POINT while the raw values remain
            # available for fixed-site refinement and future analysis.
            "longitude": longitude,
            "latitude": latitude,
            "altitude": altitude,
            "longitude_raw": [longitude],
            "latitude_raw": [latitude],
            "altitude_raw": [altitude],
            "nu_gnss_measurements": 1,
            "latitude_std": latitude_std,
            "longitude_std": longitude_std,
            "is_mobile": is_mobile,
            # Fixed spectra stay point-only; mobile spectra expose the
            # additional bounding geometry that DIM_SPECTRUM_SITE can persist.
            # if is_mobile==false then geographic_path is None, which signals the database to ignore it
            "geographic_path": (
                self._build_geographic_path(
                    latitude=latitude,
                    longitude=longitude,
                    latitude_std=latitude_std,
                    longitude_std=longitude_std,
                )
                if is_mobile else None
            ),
        }

    def _extract_spectrum_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Preserve non-dimensional metadata that still matters operationally.

        The relational model already owns the main analytical columns. This
        helper keeps only the metadata that remains useful for later operator
        inspection without inflating FACT_SPECTRUM with first-class columns.
        """
        extra = {}
        antenna = metadata.get("Antenna")
        others = metadata.get("Others")

        if isinstance(antenna, dict) and antenna:
            extra["antenna"] = antenna

        if isinstance(others, str) and others.strip():
            try:
                extra["others"] = json.loads(others)
            except Exception:
                extra["others"] = others
        elif others not in (None, "", {}):
            extra["others"] = others

        return extra

    @staticmethod
    def _coerce_float_field(field_name: str, value: Any) -> float:
        """
        Parse one required numeric payload field as float.
        """
        try:
            return float(value)
        except Exception:
            raise errors.BinValidationError(
                f"APP_ANALISE returned invalid {field_name}: {value}"
            )

    @staticmethod
    def _coerce_positive_int_field(field_name: str, value: Any) -> int:
        """
        Parse one required payload field as a positive integer.
        """
        try:
            parsed = int(value)
        except Exception:
            raise errors.BinValidationError(
                f"APP_ANALISE returned invalid {field_name}: {value}"
            )

        if parsed <= 0:
            raise errors.BinValidationError(
                f"APP_ANALISE returned non-positive {field_name}: {value}"
            )

        return parsed

    @staticmethod
    def _parse_related_timestamp(field_name: str, value: Any) -> datetime:
        """
        Parse one RelatedFiles timestamp using the expected appAnalise format.
        """
        if not isinstance(value, str) or not value.strip():
            raise errors.BinValidationError(
                f"APP_ANALISE returned invalid {field_name}: {value}"
            )

        try:
            return datetime.strptime(value, "%d-%b-%Y %H:%M:%S")
        except Exception:
            raise errors.BinValidationError(
                f"APP_ANALISE returned invalid {field_name}: {value}"
            )

    def _is_valid_hostname(self, value):
        """
        Return whether a hostname extracted from the payload is usable.
        """
        if not isinstance(value, str):
            return False

        normalized = value.strip().lower()
        return normalized not in self.INVALID_HOSTNAME_VALUES

    def _normalize_equipment_hostname(self, raw_hostname: Any) -> str:
        """
        Normalize one receiver/equipment identifier into RF.Fusion form.

        This normalization now applies at spectrum granularity because one
        processed payload can aggregate several different receivers.
        """
        # Reject placeholder receiver values before they can create synthetic
        # equipment rows in DIM_SPECTRUM_EQUIPMENT.
        hostname_valid = self._is_valid_hostname(raw_hostname)

        if not hostname_valid:
            raise errors.BinValidationError(
                "Hostname resolution failed: invalid hostname"
            )

        resolved = raw_hostname.strip()
        match = RFEYE_CANONICAL_RE.search(resolved)

        # RFeye receivers often arrive with longer descriptive strings. Reduce
        # them to the stable equipment key already used across RF.Fusion.
        if match:
            resolved = match.group(1).lower()
        elif resolved.strip().lower().startswith("cwsm"):
            # Keep the payload-level hostname summary aligned with the
            # canonical CelPlan receiver form whenever the identifier is valid.
            # Malformed CWSM values are left untouched here so later worker
            # logic can fall back to the trusted host-side identifier instead
            # of discarding the whole payload prematurely.
            canonical = _canonicalize_single_equipment_identifier(resolved)
            if canonical:
                resolved = canonical

        return resolved

    @staticmethod
    def _collect_spectrum_hostnames(spectra: Iterable[Any]) -> list[str]:
        """
        Return the unique `equipment_name` values carried by normalized spectra.
        """
        hostnames = []
        seen = set()

        for spectrum in spectra:
            equipment_name = getattr(spectrum, "equipment_name", None)

            if not isinstance(equipment_name, str) or not equipment_name.strip():
                continue

            if equipment_name not in seen:
                seen.add(equipment_name)
                hostnames.append(equipment_name)

        return hostnames

    def _finalize_hostnames(self, bin_data: Dict[str, Any]) -> None:
        """
        Finalize the hostname contract for the whole normalized payload.

        One processed file may contain several receivers, so the real hostname
        ownership already lives on each spectrum row by the time this helper
        runs. Here we only derive the payload-level summary fields from that
        per-spectrum truth.
        """
        normalized_hostnames = self._collect_spectrum_hostnames(
            bin_data.get("spectrum", [])
        )

        if not normalized_hostnames:
            raise errors.BinValidationError(
                "Hostname list missing or invalid"
            )

        bin_data["hostnames"] = normalized_hostnames
        bin_data["hostname"] = (
            normalized_hostnames[0] if len(normalized_hostnames) == 1 else None
        )

    def _coerce_validated_gps_triplet(
        self,
        *,
        latitude_value: Any,
        longitude_value: Any,
        altitude_value: Any,
    ) -> tuple[float, float, float]:
        """
        Parse and validate one GPS triplet shared by root and spectrum checks.
        """
        lat = self._coerce_float_field("GPS.Latitude", latitude_value)
        lon = self._coerce_float_field("GPS.Longitude", longitude_value)
        alt = self._coerce_float_field("GPS.Altitude", altitude_value)

        if (
            lat == self.GPS_SENTINEL_VALUE
            and lon == self.GPS_SENTINEL_VALUE
            and (alt == self.GPS_SENTINEL_VALUE or alt == 0)
        ):
            raise errors.BinValidationError(
                "Invalid GPS reading: GNSS unavailable sentinel"
            )

        if not (-90.0 <= lat <= 90.0):
            raise errors.BinValidationError(
                "GPS invalid latitude range"
            )

        if not (-180.0 <= lon <= 180.0):
            raise errors.BinValidationError(
                "GPS invalid longitude range"
            )

        return lat, lon, alt

    def _validate_gps(self, bin_data: Dict[str, Any]) -> None:
        """
        Validate the normalized GPS object and coerce numeric fields to float.
        """
        gps = bin_data.get("gps")

        # The parser already filters bad spectra individually, but the root
        # payload still exposes one representative GPS object used by existing
        # code paths. Keep this final gate so that object cannot drift into an
        # invalid state after selective filtering.
        if gps is None:
            raise errors.BinValidationError("GPS metadata missing")

        for attr in ("latitude", "longitude", "altitude"):
            if not hasattr(gps, attr):
                raise errors.BinValidationError(
                    f"GPS missing attribute: {attr}"
                )

        # Legacy appColeta/appAnalise flows use (-1, -1, -1/0) as "GNSS not
        # available". That sentinel is never a valid locality and must not be
        # allowed to survive as the representative payload GPS.
        lat, lon, alt = self._coerce_validated_gps_triplet(
            latitude_value=gps.latitude,
            longitude_value=gps.longitude,
            altitude_value=gps.altitude,
        )

        # Coerce the representative GPS object in place so downstream callers
        # see canonical numeric values instead of raw parser leftovers.
        gps.latitude = lat
        gps.longitude = lon
        gps.altitude = alt

    def _validate_spectrum_container(self, bin_data: Dict[str, Any]) -> None:
        """
        Ensure the normalized spectrum list is iterable and non-empty.
        """
        spectra = bin_data.get("spectrum")

        if not isinstance(spectra, Iterable):
            raise errors.BinValidationError(
                "Spectrum is not iterable"
            )

        if not spectra:
            raise errors.BinValidationError(
                "Spectrum list is empty"
            )

    @staticmethod
    def _classify_spectrum_discard_reason(exc: Exception) -> str:
        """
        Classify one per-spectrum validation failure into a coarse discard reason.

        The worker only needs a small amount of detail here. In particular, if
        every spectrum in the payload was discarded because of GPS defects, the
        final error should say so explicitly instead of falling back to the
        generic "no valid spectra survived" message.
        """
        message = str(exc).lower()

        if "gps" in message or "gnss" in message:
            return "gps"

        return "other"

    @classmethod
    def _is_missing_source_file_error(cls, message: object) -> bool:
        """
        Return whether an appAnalise error string points to a missing source file.
        """
        if not isinstance(message, str):
            return False

        normalized = "".join(message.strip().lower().split())
        return any(
            snippet in normalized for snippet in cls.SOURCE_FILE_MISSING_SNIPPETS
        )

    @staticmethod
    def validate_source_file(full_path: str) -> None:
        """
        Ensure the requested source file still exists before contacting appAnalise.
        """
        if not os.path.isfile(full_path):
            raise errors.BinValidationError(
                f"APP_ANALISE source file unavailable before request: {full_path}"
            )

    def _raise_answer_error(
        self,
        answer_error: str,
        requested_full_path: Optional[str] = None,
    ) -> None:
        """
        Classify string errors returned in `Answer`.
        """
        normalized_error = str(answer_error).strip().lower()

        if any(snippet in normalized_error for snippet in self.READ_TIMEOUT_SNIPPETS):
            raise errors.AppAnaliseReadTimeoutError(
                f"APP_ANALISE returned FileRead timeout: {answer_error}"
            )

        if self._is_missing_source_file_error(answer_error):
            # When appAnalise says "file not found" but the source file still
            # exists locally, the most likely problem is service visibility or
            # timing, not a definitively bad FILE_TASK.
            if requested_full_path and os.path.isfile(requested_full_path):
                raise errors.ExternalServiceTransientError(
                    "APP_ANALISE reported missing source file, but it still "
                    f"exists locally: {requested_full_path} ({answer_error})"
                )

            # If the source file is genuinely gone from disk, retrying would
            # only repeat the same semantic defect.
            if requested_full_path:
                raise errors.BinValidationError(
                    "APP_ANALISE reported missing source file and it is "
                    f"absent locally: {requested_full_path} ({answer_error})"
                )

        # Any other string in `Answer` is treated as a definitive service-side
        # semantic error rather than a transport failure.
        raise errors.BinValidationError(
            f"APP_ANALISE returned error in Answer: {answer_error}"
        )

    def detect_protocol_error(
        self,
        payload: Dict,
        requested_full_path: Optional[str] = None,
    ) -> None:
        """
        Validate the top-level protocol structure before normalization begins.
        """
        if not isinstance(payload, dict):
            raise errors.BinValidationError(
                "APP_ANALISE returned invalid payload type"
            )

        err = payload.get("Error")
        if err is not None:
            # `Error` is a protocol-level failure channel that bypasses the
            # normal success shape of `Answer`.
            if isinstance(err, (list, tuple)) and len(err) >= 2:
                code = err[0]
                message = err[1]
            else:
                code = "APP_ANALISE_ERROR"
                message = str(err)

            raise errors.BinValidationError(
                f"APP_ANALISE returned error: {code} - {message}"
            )

        answer = payload.get("Answer")

        if answer is None:
            raise errors.BinValidationError(
                "APP_ANALISE response missing Answer payload"
            )

        # appAnalise sometimes returns string errors directly in `Answer`
        # instead of a dict-shaped success payload.
        if isinstance(answer, str):
            self._raise_answer_error(answer, requested_full_path)

        if not isinstance(answer, dict):
            raise errors.BinValidationError(
                f"APP_ANALISE returned invalid Answer payload type: {answer}"
            )

        if not answer:
            raise errors.BinValidationError(
                f"APP_ANALISE returned empty Answer payload: {answer}"
            )

        if "Spectra" not in answer:
            raise errors.BinValidationError(
                f"APP_ANALISE response missing Answer.Spectra: {answer}"
            )

        # Normalize the container shape early so later stages can assume
        # `Spectra` is list-like even when one single spectrum came back.
        self._coerce_spectra_payload(answer.get("Spectra"), allow_empty=True)

    def _coerce_spectra_payload(
        self,
        spectra: Any,
        *,
        allow_empty: bool = False,
    ) -> list[Dict[str, Any]]:
        """
        Normalize `Answer.Spectra` into a list of spectrum dictionaries.
        """
        if isinstance(spectra, str):
            raise errors.BinValidationError(
                f"APP_ANALISE returned invalid Spectra payload: {spectra}"
            )

        # appAnalise is inconsistent here: one spectrum may come back as a
        # dict, several as a list. Collapse both shapes to one list contract.
        if isinstance(spectra, dict):
            spectra_list = [spectra]
        elif isinstance(spectra, (list, tuple)):
            spectra_list = list(spectra)
        else:
            raise errors.BinValidationError(
                f"APP_ANALISE returned invalid Answer.Spectra type: {spectra}"
            )

        if not allow_empty and not spectra_list:
            raise errors.BinValidationError(
                "APP_ANALISE returned empty Spectra list"
            )

        # Keep the protocol boundary strict: every member must already be a
        # dict before semantic spectrum normalization begins.
        for index, spectrum in enumerate(spectra_list):
            if not isinstance(spectrum, dict):
                raise errors.BinValidationError(
                    "APP_ANALISE returned invalid spectrum entry "
                    f"at index {index}: {spectrum}"
                )

        return spectra_list

    def _resolve_output_location(
        self,
        *,
        answer: Dict,
        file_path: str,
        file_name: str,
        export: bool,
    ) -> tuple[str, str, str]:
        """
        Resolve the path/name/full_path of the artifact owned by this request.

        `export=False` means the original BIN remains authoritative.
        `export=True` means appAnalise generated a derived artifact described in
        `Answer.General`, and that exported file becomes the authoritative one.
        """
        if export:
            # In export mode the worker must stop treating the source BIN as the
            # canonical artifact and switch to the file materialized by
            # appAnalise in `Answer.General`.
            general = answer.get("General")

            if not isinstance(general, dict):
                raise errors.BinValidationError(
                    f"APP_ANALISE response missing valid Answer.General: {general}"
                )

            path = general.get("FilePath")
            name = self._resolve_general_filename(general.get("FileName"))
        else:
            # Without export, the source payload itself remains the authoritative
            # artifact even though appAnalise still parsed and validated it.
            path = file_path
            name = file_name

        # Keep the artifact contract strict before any filesystem probing:
        # callers should only continue with a concrete path/name pair.
        if not isinstance(path, str) or not path.strip():
            raise errors.BinValidationError(
                f"APP_ANALISE returned invalid output file path: {path}"
            )

        if not isinstance(name, str) or not name.strip():
            raise errors.BinValidationError(
                f"APP_ANALISE returned invalid output file name: {name}"
            )

        full_path = os.path.join(path, name)
        return path, name, full_path

    @staticmethod
    def _resolve_general_filename(file_name) -> str:
        """
        Normalize `Answer.General.FileName` into a single filename string.
        """
        if isinstance(file_name, str):
            return file_name

        if isinstance(file_name, (list, tuple)) and file_name:
            return "".join(str(part) for part in file_name)

        return file_name

    @staticmethod
    def _get_output_artifact_size(full_path: str) -> int:
        """
        Read the current size of the output artifact or classify it as transient.
        """
        try:
            return os.path.getsize(full_path)
        except OSError as e:
            raise errors.ExternalServiceTransientError(
                f"APP_ANALISE output artifact unavailable: {full_path} ({e})"
            )

    @staticmethod
    def _stat_output_artifact(full_path: str) -> os.stat_result:
        """
        Stat the output artifact or classify the missing file as transient.
        """
        try:
            return os.stat(full_path)
        except OSError as e:
            raise errors.ExternalServiceTransientError(
                f"APP_ANALISE output artifact unavailable: {full_path} ({e})"
            )

    def normalize_response(self, payload: Dict) -> Dict:
        """
        Convert a validated appAnalise payload into canonical RF.Fusion `bin_data`.

        This step is intentionally narrower than `normalize_payload(...)`.
        Here we only reshape the raw appAnalise structures into the RF.Fusion
        bin_data skeleton. Cross-field integrity checks that apply to the whole
        normalized object still happen afterwards in `normalize_payload(...)`.

        Important behavior:
            - malformed spectrum entries are discarded selectively
            - the payload only fails when no valid spectra remain afterwards

        That mirrors the intended RF.Fusion contract: local corruption inside
        one spectrum should not erase the healthy spectra that came in the same
        file.
        """
        # Phase 1: assert the already-decoded protocol payload is still shaped
        # like an appAnalise success response before we touch any spectrum.
        answer = payload.get("Answer")

        if not isinstance(answer, dict):
            raise errors.BinValidationError(
                f"APP_ANALISE returned invalid Answer payload type: {answer}"
            )

        spectra_payload = self._coerce_spectra_payload(
            answer.get("Spectra", [])
        )

        spectrums = []
        gps_obj = None
        method = None
        discarded_spectra = 0
        discarded_gps_spectra = 0

        # Phase 2: validate and normalize each raw spectrum independently. One
        # bad spectrum should be dropped, not allowed to poison the whole file.
        for spec in spectra_payload:
            try:
                # The outer spectrum object provides the shared metadata that
                # every expanded `RelatedFiles` row will inherit.
                metadata = spec.get("MetaData", {})
                gps = spec.get("GPS", {})
                related = spec.get("RelatedFiles", [])
                receiver = self._normalize_equipment_hostname(spec.get("Receiver"))

                if not isinstance(metadata, dict):
                    raise errors.BinValidationError(
                        f"APP_ANALISE returned invalid MetaData payload: {metadata}"
                    )

                if not isinstance(gps, dict):
                    raise errors.BinValidationError(
                        f"APP_ANALISE returned invalid GPS payload: {gps}"
                    )

                if not isinstance(related, (list, tuple)):
                    raise errors.BinValidationError(
                        f"APP_ANALISE returned invalid RelatedFiles payload: {related}"
                    )

                # Coerce one validated GPS summary for this spectrum before it
                # becomes both the future SITE candidate and the root payload
                # representative GPS accumulator.
                lat, lon, alt = self._coerce_validated_gps_triplet(
                    latitude_value=gps.get("Latitude"),
                    longitude_value=gps.get("Longitude"),
                    altitude_value=gps.get(
                        "Altitude",
                        self.GPS_SENTINEL_VALUE,
                    ),
                )

                start_mega = self._coerce_float_field(
                    "MetaData.FreqStart",
                    metadata.get("FreqStart"),
                ) / 1e6
                stop_mega = self._coerce_float_field(
                    "MetaData.FreqStop",
                    metadata.get("FreqStop"),
                ) / 1e6
                ndata = self._coerce_positive_int_field(
                    "MetaData.DataPoints",
                    metadata.get("DataPoints"),
                )

                if start_mega <= 0 or stop_mega <= 0 or stop_mega <= start_mega:
                    raise errors.BinValidationError(
                        "APP_ANALISE returned invalid frequency range"
                    )

                level_unit = metadata.get("LevelUnit")
                if not isinstance(level_unit, str) or not level_unit.strip():
                    raise errors.BinValidationError(
                        f"APP_ANALISE returned invalid MetaData.LevelUnit: {level_unit}"
                    )

                trace_mode = metadata.get("TraceMode", "peak")
                if not isinstance(trace_mode, str) or not trace_mode.strip():
                    raise errors.BinValidationError(
                        f"APP_ANALISE returned invalid MetaData.TraceMode: {trace_mode}"
                    )

                rbw = metadata.get("Resolution")
                if rbw is not None:
                    rbw = self._coerce_float_field("MetaData.Resolution", rbw)

                normalized_rows = []
                local_method = None

                # Phase 2b: expand the spectrum into one normalized RF.Fusion
                # row per RelatedFiles entry. appAnalise can summarize several
                # temporal windows under the same spectral envelope.
                for rf in related:
                    if not isinstance(rf, dict):
                        raise errors.BinValidationError(
                            f"APP_ANALISE returned invalid RelatedFiles entry: {rf}"
                        )

                    task_name = rf.get("Task") or rf.get("task")

                    if local_method is None:
                        # The first related-file task name becomes the processing
                        # method exposed to the worker unless Answer.Method later
                        # provides a stronger explicit label.
                        if isinstance(task_name, str) and task_name.strip():
                            local_method = task_name.strip()

                    start_time = self._parse_related_timestamp(
                        "RelatedFiles.BeginTime",
                        rf.get("BeginTime"),
                    )
                    end_time = self._parse_related_timestamp(
                        "RelatedFiles.EndTime",
                        rf.get("EndTime"),
                    )

                    if end_time < start_time:
                        raise errors.BinValidationError(
                            "APP_ANALISE returned RelatedFiles.EndTime before BeginTime"
                        )

                    trace_count = rf.get("NumSweeps")
                    if trace_count is None:
                        trace_count = rf.get("nSweeps")
                    trace_count = self._coerce_positive_int_field(
                        "RelatedFiles.NumSweeps",
                        trace_count,
                    )

                    normalized_rows.append(
                        SimpleNamespace(
                            start_mega=start_mega,
                            stop_mega=stop_mega,
                            ndata=ndata,
                            trace_length=trace_count,
                            level_unit=level_unit.strip(),
                            processing=trace_mode.lower().strip(),
                            start_dateidx=start_time,
                            stop_dateidx=end_time,
                            bw=rbw,
                            description=rf.get("Description"),
                            equipment_name=receiver,
                            site_data=self._build_spectrum_site_data(
                                gps=gps,
                                task_name=(
                                task_name if isinstance(task_name, str) else None
                                ),
                                latitude=lat,
                                longitude=lon,
                                altitude=alt,
                            ),
                            # Preserve the auxiliary upstream metadata once per
                            # normalized row so later persistence stays
                            # lossless even if the relational schema is
                            # intentionally narrower.
                            metadata=self._extract_spectrum_metadata(metadata),
                        )
                    )

                if not normalized_rows:
                    raise errors.BinValidationError(
                        "APP_ANALISE spectrum has no usable RelatedFiles entries"
                    )

            except errors.BinValidationError as exc:
                # Invalid spectra are discarded locally; only the complete
                # absence of healthy spectra will fail the payload later.
                discarded_spectra += 1
                if self._classify_spectrum_discard_reason(exc) == "gps":
                    discarded_gps_spectra += 1
                continue

            # Phase 3: fold the surviving spectrum into the payload-level
            # summaries that older code paths still expect to exist.
            if gps_obj is None:
                # Keep one representative GPS fix exposed on the root object
                # while still accumulating all valid raw points for downstream
                # site resolution logic.
                gps_obj = SimpleNamespace(
                    longitude=lon,
                    latitude=lat,
                    altitude=alt,
                    _longitude=[lon],
                    _latitude=[lat],
                    _altitude=[alt],
                )
            else:
                gps_obj._longitude.append(lon)
                gps_obj._latitude.append(lat)
                gps_obj._altitude.append(alt)

            if method is None and local_method:
                method = local_method

            spectrums.extend(normalized_rows)

        # Phase 4: after selective filtering, a payload only remains valid if
        # at least one normalized spectrum survived.
        if not spectrums:
            if discarded_spectra and discarded_spectra == discarded_gps_spectra:
                raise errors.BinValidationError(
                    "Invalid GPS reading: GNSS unavailable sentinel | "
                    "all spectra in payload failed GPS validation"
                )

            raise errors.BinValidationError(
                "APP_ANALISE payload has no valid spectra after per-spectrum validation"
            )

        # Build the payload-level hostname summary only from the spectra that
        # actually survived validation and expansion.
        hostnames = self._collect_spectrum_hostnames(spectrums)

        return {
            # `hostname` is kept only as a convenience for homogeneous payloads.
            # Mixed payloads expose no synthetic single receiver anymore.
            "hostname": hostnames[0] if len(hostnames) == 1 else None,
            "hostnames": hostnames,
            "method": method or answer.get("Method", self.PROCESSOR_NAME),
            "gps": gps_obj,
            "spectrum": spectrums,
            # Keep discard count only on the in-memory normalized payload so
            # operators and future worker logic can observe partial cleanup
            # without polluting per-spectrum JS_METADATA in RFDATA.
            "discarded_spectrum_count": discarded_spectra,
        }

    def resolve_output_file(
        self,
        *,
        answer: Dict,
        file_path: str,
        file_name: str,
        export: bool,
    ) -> Dict:
        """
        Wait for the output artifact to settle and return its filesystem metadata.

        appAnalise can finish the socket response slightly before the output
        file stops growing on disk. This helper waits for the artifact size to
        stabilize so the worker does not capture half-written metadata.
        """
        path, name, full_path = self._resolve_output_location(
            answer=answer,
            file_path=file_path,
            file_name=file_name,
            export=export,
        )
        last_size = self._get_output_artifact_size(full_path)

        for _ in range(10):
            time.sleep(0.2)
            size = self._get_output_artifact_size(full_path)

            if size == last_size:
                # Two consecutive identical sizes are our cheap "settled on
                # disk" signal. At that point we can safely stat once and
                # expose the artifact to the worker.
                break
            last_size = size

        stat = self._stat_output_artifact(full_path)

        if stat.st_size <= 0:
            raise errors.BinValidationError(
                f"APP_ANALISE output artifact is empty: {full_path}"
            )

        return {
            "file_path": path,
            "file_name": name,
            "extension": os.path.splitext(name)[1].lower(),
            "size_kb": int(stat.st_size / 1024),
            "dt_created": datetime.fromtimestamp(stat.st_ctime),
            "dt_modified": datetime.fromtimestamp(stat.st_mtime),
            "full_path": full_path,
        }

    def normalize_payload(self, payload: Dict) -> Dict:
        """
        Normalize and validate one already-decoded appAnalise payload.

        This is the final semantic gate before the worker can write anything to
        RFDATA. The sequence matters:
            1. reshape the raw payload into canonical bin_data
            2. normalize the hostname into RF.Fusion's expected format
            3. validate the remaining root-level integrity such as GPS and spectra

        Only after this function succeeds should the worker treat the payload
        as safe to persist downstream.
        """
        # Step 1: reshape appAnalise's response into RF.Fusion's canonical
        # bin_data structure without yet claiming the whole object is valid.
        bin_data = self.normalize_response(payload)

        # Step 2: coerce the hostname into the canonical representation used by
        # RF.Fusion tables and downstream deduplication logic. This happens in
        # one place now: every spectrum keeps its own normalized equipment
        # hostname, and the payload only exposes a root hostname when all
        # spectra share the same receiver.
        self._finalize_hostnames(bin_data)

        # Step 3: validate the remaining root object. Hostname normalization is
        # already closed in Step 2; what remains here is the non-hostname
        # integrity gate before persistence.
        self._validate_gps(bin_data)
        self._validate_spectrum_container(bin_data)
        return bin_data
