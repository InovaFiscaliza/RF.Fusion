"""
RFeye station validation and enrichment logic.
"""

from collections.abc import Iterable
import numpy as np

from .base import Station
from shared import errors
import re


# =================================================
# Local (non-fatal) validation error
# =================================================
class SpectrumValidationError(Exception):
    """Raised when a single spectrum entry is invalid."""
    pass


class RFEyeStation(Station):
    """
    RFeye station handler with strict validation and deterministic enrichment.

    CONTRACT:
    If process() returns successfully, the BIN is guaranteed to be
    fully insertable into RF.Fusion without further validation.

    Global corruption → BIN discarded
    Local corruption  → spectrum discarded
    """

    # =================================================
    # RFeye-specific constants
    # =================================================

    INVALID_HOSTNAME_VALUES = {
        "none", "(none)", "null", "(null)", "unknown", ""
    }

    DEFAULT_METHOD_NAME = "rfeye_crfs_default"

    RBW_LOOKUP_TABLE = [
        (36_000, 36_914),
        (72_000, 73_828),
        (144_000, 147_656),
        (288_000, 295_312),
        (576_000, 590_625),
        (1_000_000, 1_181_250),
    ]

    OCCUPANCY_MAX_RANGE = 100
    RFEYE_CANONICAL_RE = re.compile(r"(rfeye\d{6})", re.IGNORECASE)

    # =================================================
    # Public contract
    # =================================================
    def process(self) -> dict:
        """
        Execute the full RFeye validation and enrichment pipeline.

        CONTRACT:
            - If this method returns successfully, the BIN is guaranteed
            to be fully insertable into RF.Fusion without further checks.
            - Any fatal inconsistency at BIN level results in
            errors.BinValidationError and discards the entire BIN.
            - Spectrum-level inconsistencies discard only the affected
            spectrum entry.

        Validation model:
            - Global metadata is resolved and validated first (fatal).
            - Spectrum container integrity is validated next (fatal).
            - Individual spectra are then validated and enriched
            independently (non-fatal per spectrum).
        """

        # -------------------------------------------------
        # Operate IN-PLACE on the raw BIN data
        # (allowed by Station contract)
        # -------------------------------------------------
        self.bin_data = self._bin_data_raw

        # -------------------------------------------------
        # Global metadata resolution and validation (FATAL)
        # -------------------------------------------------

        # Resolve and normalize hostname deterministically.
        # This step is authoritative and may discard the BIN
        # if hostname resolution is impossible.
        self._normalize_hostname()

        # Defensive assertion: hostname must be present and valid
        # after successful normalization.
        self._validate_hostname()

        # Validate GPS metadata according to RF.Fusion requirements.
        # Any failure here invalidates the entire BIN.
        self._validate_gps()

        # -------------------------------------------------
        # Spectrum container validation (FATAL)
        # -------------------------------------------------

        # Ensure the BIN contains a non-empty iterable of spectra.
        self._validate_spectrum_container()

        # -------------------------------------------------
        # Per-spectrum validation and enrichment (NON-FATAL)
        # -------------------------------------------------
        valid_spectra: list = []
        discarded: list[str] = []

        for idx, spectrum in enumerate(self.bin_data["spectrum"], start=1):
            try:
                # Validate and enrich a single spectrum entry.
                # Any failure here discards only this spectrum.
                self._validate_and_enrich_spectrum(spectrum, idx)
                valid_spectra.append(spectrum)

            except SpectrumValidationError as e:
                # Local corruption: keep processing remaining spectra.
                discarded.append(str(e))
                continue

        # Replace original spectrum list with only valid spectra.
        self.bin_data["spectrum"] = valid_spectra

        # If no valid spectra remain, the BIN is semantically useless.
        if not valid_spectra:
            raise errors.BinValidationError(
                "BIN discarded: no valid spectra after validation"
            )

        # Preserve diagnostics about discarded spectra, if any.
        if discarded:
            self.bin_data["_discarded_spectra"] = discarded

        return self.bin_data


    # =================================================
    # Hostname handling
    # =================================================
    def _is_valid_hostname(self, value: object) -> bool:
        """
        Determine whether a hostname value is semantically valid.

        A hostname is considered valid if:
            - It is a string
            - After stripping and lowercasing, it does NOT match any
            known invalid or sentinel values (e.g. "none", "null", "unknown")

        This method performs **semantic validation only**.
        It does NOT normalize, mutate, or resolve the hostname.

        Args:
            value (object): Raw hostname value extracted from BIN metadata
                            or provided as host_uid.

        Returns:
            bool: True if the hostname is valid, False otherwise.
        """
        if not isinstance(value, str):
            return False

        normalized = value.strip().lower()
        return normalized not in self.INVALID_HOSTNAME_VALUES


   
    def _normalize_hostname(self):
        """
        Resolve and normalize the effective hostname for the BIN.

        This method deterministically resolves the hostname using the
        BIN metadata and the externally resolved host_uid, and then
        applies RFeye-specific canonicalization rules to avoid
        duplicate station identities.

        Resolution rules (authoritative and exhaustive):

            1) BIN hostname invalid AND host_uid invalid
            → FATAL: the BIN is discarded.

            2) BIN hostname invalid AND host_uid valid
            → hostname := host_uid

            3) BIN hostname valid AND host_uid invalid
            → hostname := BIN hostname

            4) BIN hostname valid AND host_uid valid
            → hostname := BIN hostname

        RFeye canonicalization rules:
            - If the resolved hostname contains an RFeye identifier
            in the form 'rfeyeXXXXXX', only this canonical prefix
            is retained.
            - Any suffixes or variants (e.g. '-vcp', '-lab', '-backup')
            are discarded.
            - The canonical hostname is always lowercased.

        CONTRACT:
            - This method MUST NOT set hostname to None.
            - This method MUST raise BinValidationError on fatal ambiguity.
            - On successful return, bin_data["hostname"] is guaranteed
            to be a valid, canonical, non-empty string.
            - No heuristics beyond the explicit rules above are allowed.

        Raises:
            errors.BinValidationError:
                If neither BIN hostname nor host_uid are valid.
        """

        raw_hostname = self.bin_data.get("hostname")
        host_uid = self._host_uid

        hostname_valid = self._is_valid_hostname(raw_hostname)
        host_uid_valid = self._is_valid_hostname(host_uid)

        # -------------------------------------------------
        # Case 1: both hostname sources are invalid
        # → global corruption, BIN must be discarded
        # -------------------------------------------------
        if not hostname_valid and not host_uid_valid:
            raise errors.BinValidationError(
                "Hostname resolution failed: BIN hostname and host_uid are both invalid"
            )

        # -------------------------------------------------
        # Resolve authoritative hostname source
        # -------------------------------------------------
        if not hostname_valid and host_uid_valid:
            resolved = host_uid.strip()
        else:
            resolved = raw_hostname.strip()

        # -------------------------------------------------
        # RFeye canonicalization (identity normalization)
        # -------------------------------------------------
        match = self.RFEYE_CANONICAL_RE.search(resolved)
        if match:
            # Preserve only the canonical RFeye identifier
            resolved = match.group(1).lower()

        # -------------------------------------------------
        # Final assignment
        # -------------------------------------------------
        self.bin_data["hostname"] = resolved


    def _validate_hostname(self):
        """
        Defensive validation of the resolved hostname.

        This validation should never fail if _normalize_hostname()
        executed successfully. It exists solely as a safeguard
        against future regressions or incorrect mutations.

        Raises:
            errors.BinValidationError:
                If hostname is missing or not a string.
        """
        hostname = self.bin_data.get("hostname")

        if not isinstance(hostname, str) or not hostname.strip():
            raise errors.BinValidationError("Hostname missing or invalid")


    # =================================================
    # GPS validation (generic rules from Station)
    # =================================================
    def _validate_gps(self):
        """
        Validate GPS metadata for RF.Fusion ingestion.

        RF.Fusion requires a usable geographic position for regional
        attribution only. GNSS precision, fix quality and satellite
        count are NOT enforced.

        Fatal condition:
            - GNSS unavailable sentinel values (lat=lon=alt=-1)
        """

        gps = self.bin_data.get("gps")
        if gps is None:
            raise errors.BinValidationError("GPS metadata missing")

        # ---- required attributes ----
        for attr in ("latitude", "longitude", "altitude"):
            if not hasattr(gps, attr):
                raise errors.BinValidationError(f"GPS missing attribute: {attr}")

        lat = gps.latitude
        lon = gps.longitude
        alt = gps.altitude

        # ---- numeric validation ----
        for name, val in (("latitude", lat), ("longitude", lon), ("altitude", alt)):
            if not isinstance(val, (int, float, np.integer, np.floating)):
                raise errors.BinValidationError(f"GPS invalid {name} type")

        # ---- sentinel check (RFeye default GNSS unavailable) ----
        if (
            lat == self.GPS_SENTINEL_VALUE
            and lon == self.GPS_SENTINEL_VALUE
            and alt == self.GPS_SENTINEL_VALUE
        ):
            raise errors.BinValidationError(
                "Invalid GPS reading: lat=lon=alt=-1 (GNSS unavailable sentinel)"
            )

        # ---- physical range validation (coarse) ----
        if not (-90.0 <= lat <= 90.0):
            raise errors.BinValidationError("GPS invalid latitude range")

        if not (-180.0 <= lon <= 180.0):
            raise errors.BinValidationError("GPS invalid longitude range")

        # ---- satellite count intentionally ignored ----
        # num_satellites may be missing, fractional or aggregated.
        # It does not impact RF.Fusion spatial semantics.



    def _validate_spectrum_container(self):
        """
        Validate the presence and structural integrity of the spectrum container.

        This validation is BIN-level and therefore FATAL.

        CONTRACT:
        - The BIN must contain a 'spectrum' field.
        - 'spectrum' must be an iterable collection.
        - The collection must not be empty.

        Rationale:
        RF.Fusion operates on spectral data. A BIN without spectra is
        semantically meaningless and cannot be partially recovered.
        """

        spectra = self.bin_data.get("spectrum")

        # The spectrum container must be iterable (e.g. list, tuple)
        if not isinstance(spectra, Iterable):
            raise errors.BinValidationError("Spectrum is not iterable")

        # An empty spectrum list means there is no spectral data to ingest
        if not spectra:
            raise errors.BinValidationError("Spectrum list is empty")


    # =================================================
    # Per-spectrum validation + enrichment (non-fatal)
    # =================================================
    def _validate_and_enrich_spectrum(self, s, idx: int):
        """
        Validate and enrich a single spectrum entry.

        This validation is SPECTRUM-level and therefore NON-FATAL.
        Any failure here discards ONLY the spectrum, not the entire BIN.

        CONTRACT:
            - Mandatory structural spectrum attributes must exist and be
            semantically valid.
            - Structural consistency between levels, frequencies and timestamps
            must be guaranteed.
            - Timestamp is the authoritative temporal source.
            - start_dateidx and stop_dateidx are derived metadata and may be inferred.
            - Operational and descriptive metadata (e.g. antuid, processing,
            description) may be normalized to safe defaults when missing or invalid.
            - After successful execution, the spectrum is fully insertable
            into RF.Fusion without further validation.

        Args:
            s: Spectrum object extracted from the BIN.
            idx (int): 1-based index of the spectrum within the BIN
                    (used for diagnostics only).

        Raises:
            SpectrumValidationError:
                If the spectrum is structurally or semantically invalid.
        """

        ctx = f"spectrum[{idx}]"

        # =================================================
        # Basic spectrum metadata (STRUCTURAL — mandatory)
        # =================================================

        # ---- frequency range ----
        if not hasattr(s, "start_mega") or not hasattr(s, "stop_mega"):
            raise SpectrumValidationError(f"{ctx}: missing frequency metadata")

        if s.start_mega > s.stop_mega:
            raise SpectrumValidationError(f"{ctx}: invalid frequency range")

        # ---- ndata ----
        if not hasattr(s, "ndata"):
            raise SpectrumValidationError(f"{ctx}: missing ndata")

        if not isinstance(s.ndata, int) or s.ndata < self.MIN_NDATA:
            raise SpectrumValidationError(f"{ctx}: invalid ndata")

        # =================================================
        # Operational metadata (NON-FATAL normalization)
        # =================================================

        # ---- antenna UID ----
        # Operational metadata used for antenna association.
        # Absence or invalid values are normalized to a safe default (0).
        if not hasattr(s, "antuid") or not isinstance(s.antuid, int) or s.antuid < 0:
            s.antuid = 0
            s.antuid_source = "default"
        else:
            s.antuid_source = "native"

        # =================================================
        # Levels / Frequencies / Timestamp (STRUCTURAL)
        # =================================================

        # ---- levels ----
        if not hasattr(s, "levels"):
            raise SpectrumValidationError(f"{ctx}: missing levels")

        try:
            levels = s.levels
        except Exception:
            raise SpectrumValidationError(f"{ctx}: levels not array-like")

        if levels.ndim != 2:
            raise SpectrumValidationError(f"{ctx}: levels must be 2D")

        n_traces, n_bins = levels.shape

        if n_traces == 0 or n_bins == 0:
            raise SpectrumValidationError(f"{ctx}: empty levels matrix")

        # ---- frequencies ----
        if not hasattr(s, "frequencies"):
            raise SpectrumValidationError(f"{ctx}: missing frequencies")

        try:
            freqs = s.frequencies
        except Exception:
            raise SpectrumValidationError(f"{ctx}: frequencies not array-like")

        if freqs.ndim != 1:
            raise SpectrumValidationError(f"{ctx}: frequencies must be 1D")

        if freqs.size != n_bins:
            raise SpectrumValidationError(
                f"{ctx}: frequencies size ({freqs.size}) != levels bins ({n_bins})"
            )

        # ---- timestamp (authoritative temporal source) ----
        if not isinstance(s.timestamp, np.ndarray):
            try:
                s.timestamp = np.asarray(s.timestamp.items)
            except Exception:
                raise SpectrumValidationError(f"{ctx}: timestamp not array-like")

        ts = s.timestamp



        # =================================================
        # Temporal metadata resolution (DERIVED)
        # =================================================

        ts_start = ts[0]
        ts_stop = ts[-1]

        if not hasattr(s, "start_dateidx") or s.start_dateidx != ts_start:
            s.start_dateidx = ts_start
            s.start_dateidx_source = "inferred_from_timestamp"

        if not hasattr(s, "stop_dateidx") or s.stop_dateidx != ts_stop:
            s.stop_dateidx = ts_stop
            s.stop_dateidx_source = "inferred_from_timestamp"

        if s.start_dateidx >= s.stop_dateidx:
            raise SpectrumValidationError(f"{ctx}: invalid temporal span")

        # =================================================
        # Descriptive metadata normalization (NON-FATAL)
        # =================================================

        # ---- processing ----
        if not hasattr(s, "processing") or not isinstance(s.processing, str) or not s.processing.strip():
            s.processing = "unknown"
            s.processing_source = "default"
        else:
            s.processing = s.processing.strip()
            s.processing_source = "native"

        # ---- description ----
        if not hasattr(s, "description") or not isinstance(s.description, str) or not s.description.strip():
            try:
                f_start = float(freqs[0])
                f_stop = float(freqs[-1])

                s.description = (
                    f"Descrição automática - {s.processing} "
                    f"na faixa de {f_start} até {f_stop} MHz"
                )
                s.description_source = "inferred_from_frequencies"
            except Exception:
                s.description = "Descrição automática"
                s.description_source = "default"
        else:
            s.description = s.description.strip()
            s.description_source = "native"

        # =================================================
        # Enrichment (SAFE AFTER FULL VALIDATION)
        # =================================================

        self._infer_bw(s)
        #self._infer_level_semantics(s)
        self._infer_level_semantics_light(s)
        self._normalize_method(s)


        
    # =================================================
    # RBW inference (RFeye-specific)
    # =================================================
    def _infer_bw(self, s):
        """
        Infer the effective resolution bandwidth (RBW) for a spectrum.

        CONTRACT:
            - If the spectrum already provides a native 'bw' attribute,
            it is preserved and marked as native.
            - If 'bw' is missing or empty, it is deterministically inferred
            from the frequency span and number of data points.
            - This method MUST NOT fail for structurally valid spectra.

        Resolution model:
            - Data resolution is computed as:
                (stop_mega - start_mega) / ndata  [MHz per bin]
            - The value is converted to Hz and mapped to the closest
            equivalent RBW supported by RFeye hardware.

        Args:
            s: Spectrum object assumed to be fully validated.
        """

        # -------------------------------------------------
        # Preserve native bandwidth when explicitly provided
        # -------------------------------------------------
        if getattr(s, "bw", None):
            s.bw_source = self.LEVEL_SOURCE_NATIVE
            return

        # -------------------------------------------------
        # Infer data resolution from frequency span and bins
        # -------------------------------------------------
        data_resolution_hz = (
            (s.stop_mega - s.start_mega) / s.ndata
        ) * 1_000_000

        # -------------------------------------------------
        # Map resolution to the nearest supported RBW value
        # -------------------------------------------------
        s.bw = self._lookup_equivalent_rbw(data_resolution_hz)
        s.bw_source = self.LEVEL_SOURCE_INFERRED


    @classmethod
    def _lookup_equivalent_rbw(cls, res_hz: float) -> int:
        """
        Map a data resolution (in Hz) to an equivalent RFeye RBW value.

        This method performs a deterministic lookup against a
        predefined resolution-to-RBW table, selecting the smallest
        RBW that can accommodate the given data resolution.

        CONTRACT:
            - The lookup table is ordered by increasing resolution.
            - The first RBW whose maximum resolution is >= res_hz
            is selected.
            - If res_hz exceeds all known thresholds, the largest
            RBW in the table is returned.

        Args:
            res_hz (float): Data resolution in Hz derived from spectrum metadata.

        Returns:
            int: Equivalent RBW value in Hz supported by RFeye hardware.
        """

        for max_res, rbw in cls.RBW_LOOKUP_TABLE:
            if res_hz <= max_res:
                return rbw

        # Fallback: resolution exceeds known limits, use the largest RBW
        return cls.RBW_LOOKUP_TABLE[-1][1]


    def _infer_level_semantics(self, s):
        """
        Infer the semantic meaning and unit of spectrum level data.

        This method classifies the numeric values contained in the spectrum
        `levels` matrix into a semantic measurement type based exclusively
        on the statistical distribution of the values.

        ARCHITECTURAL CONTRACT
        ----------------------
        • Inference is deterministic and rule-based.
        • No producer metadata (e.g. dtype, unit labels) is trusted for inference.
        Such metadata may be incorrect, generic, or misleading.
        • Only numeric value distribution is used.
        • The method MUST NOT raise exceptions.
        • Exactly one semantic type is selected, or UNKNOWN is assigned.
        • Weak signals and noise-floor measurements are considered valid data.

        SEMANTIC GOAL
        -------------
        RF.Fusion does not need laboratory-grade precision. It needs to
        understand WHAT a spectrum represents, not how accurate it is.

        Supported semantic types:
            - Occupancy      : channel usage percentage (0–100%)
            - Power          : received power in dBm (including noise floor)
            - Field Strength : electric field strength in dBµV/m
            - Unknown        : structurally valid but semantically ambiguous data
        """

        # -------------------------------------------------
        # Flatten level matrix for global statistical analysis
        #
        # The original matrix shape (time x frequency, etc.)
        # is irrelevant for semantic inference.
        # -------------------------------------------------
        levels = s.levels
        flat = levels.ravel()

        # -------------------------------------------------
        # Defensive fallback
        #
        # An empty or malformed level matrix cannot be
        # semantically classified.
        # -------------------------------------------------
        if flat.size == 0:
            s.level_type = self.LEVEL_TYPE_UNKNOWN
            s.level_unit = self.LEVEL_UNIT_UNKNOWN
            s.level_source = self.LEVEL_SOURCE_UNCLASSIFIED
            return

        # -------------------------------------------------
        # Preserve producer-declared dtype (audit only)
        #
        # This value is NEVER trusted for inference, but is
        # kept for traceability and debugging.
        # -------------------------------------------------
        s.user_dtype = getattr(s, "dtype", None)

        # -------------------------------------------------
        # Robust distribution metrics
        #
        # Percentiles are preferred over min/max to avoid
        # single-bin outliers, clipping artifacts, or invalid
        # samples dominating the inference logic.
        # -------------------------------------------------
        p01 = float(np.percentile(flat, 1))    # robust lower bound
        p50 = float(np.percentile(flat, 50))   # median (distribution center)
        p95 = float(np.percentile(flat, 95))   # robust upper bound
        p99 = float(np.percentile(flat, 99))   # extreme upper tail

        # Dominant dynamic range of the spectrum
        vrange = p99 - p01

        # Ratio of exact zero values (key discriminator for occupancy)
        zero_ratio = float(np.mean(flat == 0))

        # =================================================
        # Occupancy detection (percentage)
        # =================================================
        # Occupancy spectra represent channel usage over time.
        #
        # Typical characteristics:
        #   • Values are non-negative
        #   • Upper bound constrained to 100%
        #   • Large proportion of exact zeros (idle channels)
        #   • Limited dynamic range
        # =================================================
        if (
            p01 >= 0
            and p99 <= self.OCCUPANCY_MAX_PERCENT
            and zero_ratio >= self.OCCUPANCY_ZERO_RATIO_THRESHOLD
            and vrange <= self.OCCUPANCY_MAX_RANGE
        ):
            s.level_type = self.LEVEL_TYPE_OCCUPANCY
            s.level_unit = self.LEVEL_UNIT_PERCENT
            s.level_source = self.LEVEL_SOURCE_INFERRED
            return

        # =================================================
        # Power detection (dBm)
        # =================================================
        # Power spectra represent received power levels.
        #
        # Important notes:
        #   • Power values are typically centered well below 0 dB.
        #   • Noise floor measurements are still valid power data.
        #   • Dynamic range may be small (quiet band) or large (active band).
        #
        # Range is NOT used as a discriminator here.
        # =================================================
        if (
            p50 <= self.POWER_CENTER_MAX_DBM
            and p95 <= self.POWER_MAX_DBM
        ):
            s.level_type = self.LEVEL_TYPE_POWER
            s.level_unit = self.LEVEL_UNIT_DBM
            s.level_source = self.LEVEL_SOURCE_INFERRED
            return

        # =================================================
        # Field Strength detection (dBµV/m)
        # =================================================
        # Field strength represents electric field intensity.
        #
        # Critical design decision:
        #   • Field strength may be strong or weak.
        #   • Near-noise-floor or even negative dBµV/m values are valid.
        #   • Wideband spectra with both noise and strong emitters are valid.
        #
        # Dynamic range is NOT a disqualifier.
        # =================================================
        if (
            self.FIELD_STRENGTH_MIN_DB <= p50 <= self.FIELD_STRENGTH_MAX_DB
            and p95 <= self.FIELD_STRENGTH_MAX_DB
        ):
            s.level_type = self.LEVEL_TYPE_FIELD_STRENGTH
            s.level_unit = self.LEVEL_UNIT_DBUVM
            s.level_source = self.LEVEL_SOURCE_INFERRED
            return

        # =================================================
        # Fallback: unknown semantic meaning
        #
        # The spectrum is structurally valid but does not
        # match any known semantic model with sufficient
        # confidence.
        # =================================================
        s.level_type = self.LEVEL_TYPE_UNKNOWN
        s.level_unit = self.LEVEL_UNIT_UNKNOWN
        s.level_source = self.LEVEL_SOURCE_UNCLASSIFIED

    def _infer_level_semantics_light(self, s):

        # Fast C-level scan
        levels = s.levels  # ndarray
        min_val = float(levels.min())
        max_val = float(levels.max())


        if min_val < -50:
            s.level_type = self.LEVEL_TYPE_POWER
            s.level_unit = self.LEVEL_UNIT_DBM
            s.level_source = self.LEVEL_SOURCE_INFERRED
            return

        if min_val >= 0 and max_val <= 100:
            s.level_type = self.LEVEL_TYPE_OCCUPANCY
            s.level_unit = self.LEVEL_UNIT_PERCENT
            s.level_source = self.LEVEL_SOURCE_INFERRED
            return

        s.level_type = self.LEVEL_TYPE_FIELD_STRENGTH
        s.level_unit = self.LEVEL_UNIT_DBUVM
        s.level_source = self.LEVEL_SOURCE_INFERRED



    # =================================================
    # Method normalization
    # =================================================
    def _normalize_method(self, s):
        """
        Normalize the processing method metadata for a spectrum.

        This method ensures that every spectrum has a valid and
        explicitly classified processing method, which is required
        for downstream traceability and insertion into RF.Fusion.

        CONTRACT:
            - If the spectrum provides a native 'method' attribute,
            it is preserved and marked as native.
            - If 'method' is missing or empty, a deterministic default
            method is assigned.
            - This method MUST NOT raise exceptions.
            - On return, both 'method' and 'method_source' are guaranteed
            to be present.

        Args:
            s: Spectrum object assumed to be fully validated.
        """

        # -------------------------------------------------
        # Assign default method when none is provided
        # -------------------------------------------------
        if not getattr(s, "method", None):
            s.method = self.DEFAULT_METHOD_NAME
            s.method_source = self.DEFAULT_METHOD_SOURCE

        # -------------------------------------------------
        # Preserve native method and mark its origin
        # -------------------------------------------------
        else:
            s.method_source = self.NATIVE_METHOD_SOURCE

