"""
RFeye station validation and enrichment logic.
"""

from collections.abc import Iterable
import numpy as np

from .base import Station
import shared as sh
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
            sh.BinValidationError and discards the entire BIN.
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
            raise sh.BinValidationError(
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
            sh.BinValidationError:
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
            raise sh.BinValidationError(
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
            sh.BinValidationError:
                If hostname is missing or not a string.
        """
        hostname = self.bin_data.get("hostname")

        if not isinstance(hostname, str) or not hostname.strip():
            raise sh.BinValidationError("Hostname missing or invalid")


    # =================================================
    # GPS validation (generic rules from Station)
    # =================================================
    def _validate_gps(self):
        """
        Validate GPS metadata for RF.Fusion ingestion.

        CONTRACT:
        RF.Fusion requires every BIN to have a valid and usable
        geographic position. GNSS-unavailable sentinel values
        (e.g. latitude = longitude = altitude = -1) are NOT accepted
        and must invalidate the entire BIN.

        Accepted:
            - latitude  ∈ [-90, 90]
            - longitude ∈ [-180, 180]
            - altitude  ∈ ℝ
            - num_satellites >= 0

        Rejected (fatal):
            - Missing GPS attributes
            - Non-numeric coordinate values
            - Coordinates outside valid ranges
            - Sentinel values indicating GNSS unavailability
            - Negative satellite count
        """

        gps = self.bin_data.get("gps")

        if gps is None:
            raise sh.BinValidationError("GPS metadata missing")

        # ---- attribute presence ----
        for attr in ("latitude", "longitude", "altitude"):
            if not hasattr(gps, attr):
                raise sh.BinValidationError(f"GPS missing attribute: {attr}")

        lat = gps.latitude
        lon = gps.longitude
        alt = gps.altitude
        sats = getattr(gps, "num_satellites", None)

        # ---- type validation ----
        for name, val in (("latitude", lat), ("longitude", lon), ("altitude", alt)):
            if not isinstance(val, (int, float)):
                raise sh.BinValidationError(f"GPS invalid {name} type")

        # ---- sentinel check (GNSS unavailable) ----
        if (
            lat == self.GPS_SENTINEL_VALUE
            and lon == self.GPS_SENTINEL_VALUE
            and alt == self.GPS_SENTINEL_VALUE
        ):
            raise sh.BinValidationError(
                "GPS indicates GNSS unavailable (sentinel values not accepted)"
            )

        # ---- range validation ----
        if not (self.GPS_LAT_RANGE[0] <= lat <= self.GPS_LAT_RANGE[1]):
            raise sh.BinValidationError("GPS invalid latitude range")

        if not (self.GPS_LON_RANGE[0] <= lon <= self.GPS_LON_RANGE[1]):
            raise sh.BinValidationError("GPS invalid longitude range")

        # ---- satellite count ----
        if sats is not None:
            if not isinstance(sats, (int, float, np.integer, np.floating)):
                raise sh.BinValidationError("GPS invalid satellite count type")

        # semantic integer check (e.g. 5.0 is OK, 5.3 is not)
        if sats < 0 or int(sats) != sats:
            raise sh.BinValidationError("GPS invalid satellite count value")


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
            raise sh.BinValidationError("Spectrum is not iterable")

        # An empty spectrum list means there is no spectral data to ingest
        if not spectra:
            raise sh.BinValidationError("Spectrum list is empty")


    # =================================================
    # Per-spectrum validation + enrichment (non-fatal)
    # =================================================
    def _validate_and_enrich_spectrum(self, s, idx: int):
        """
        Validate and enrich a single spectrum entry.

        This validation is SPECTRUM-level and therefore NON-FATAL.
        Any failure here discards ONLY the spectrum, not the entire BIN.

        CONTRACT:
        - Mandatory spectrum attributes must exist and be semantically valid.
        - Structural consistency between levels, frequencies and timestamps
        must be guaranteed.
        - Timestamp is the authoritative temporal source.
        - start_dateidx and stop_dateidx are derived metadata and may be inferred.
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
        # Basic spectrum metadata (mandatory fields)
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

        # ---- antenna UID ----
        if not hasattr(s, "antuid"):
            raise SpectrumValidationError(f"{ctx}: missing antuid")

        if not isinstance(s.antuid, int) or s.antuid < 0:
            raise SpectrumValidationError(f"{ctx}: invalid antuid")

        # ---- processing ----
        # Mandatory for trace classification and downstream insertion
        if not hasattr(s, "processing"):
            raise SpectrumValidationError(f"{ctx}: missing processing")

        if not isinstance(s.processing, str) or not s.processing.strip():
            raise SpectrumValidationError(f"{ctx}: invalid processing")

        # =================================================
        # Levels / Frequencies / Timestamp structural validation
        # =================================================

        # ---- levels ----
        if not hasattr(s, "levels"):
            raise SpectrumValidationError(f"{ctx}: missing levels")

        try:
            levels = np.asarray(s.levels)
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
            freqs = np.asarray(s.frequencies)
        except Exception:
            raise SpectrumValidationError(f"{ctx}: frequencies not array-like")

        if freqs.ndim != 1:
            raise SpectrumValidationError(f"{ctx}: frequencies must be 1D")

        if freqs.size != n_bins:
            raise SpectrumValidationError(
                f"{ctx}: frequencies size ({freqs.size}) != levels bins ({n_bins})"
            )

        # ---- timestamp (authoritative temporal source) ----
        if not hasattr(s, "timestamp"):
            raise SpectrumValidationError(f"{ctx}: missing timestamp")

        try:
            ts = np.asarray(s.timestamp)
        except Exception:
            raise SpectrumValidationError(f"{ctx}: timestamp not array-like")

        if ts.ndim != 1 or ts.size != n_traces:
            raise SpectrumValidationError(
                f"{ctx}: timestamp size ({ts.size}) != levels traces ({n_traces})"
            )

        # =================================================
        # Temporal metadata resolution (derived from timestamp)
        # =================================================

        # Timestamp defines the real acquisition interval
        ts_start = ts[0]
        ts_stop = ts[-1]

        # Infer or correct start_dateidx
        if not hasattr(s, "start_dateidx") or s.start_dateidx != ts_start:
            s.start_dateidx = ts_start
            s.start_dateidx_source = "inferred_from_timestamp"

        # Infer or correct stop_dateidx
        if not hasattr(s, "stop_dateidx") or s.stop_dateidx != ts_stop:
            s.stop_dateidx = ts_stop
            s.stop_dateidx_source = "inferred_from_timestamp"

        # Temporal interval must be strictly increasing
        # Instantaneous or inverted spectra are analytically invalid
        if s.start_dateidx >= s.stop_dateidx:
            raise SpectrumValidationError(f"{ctx}: invalid temporal span")

        # =================================================
        # Enrichment (safe after full validation)
        # =================================================

        # From this point on, the spectrum is structurally
        # and semantically consistent.
        self._infer_bw(s)
        self._infer_level_semantics(s)
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


    # =================================================
    # Level semantic inference
    # =================================================
    def _infer_level_semantics(self, s):
        """
        Infer the semantic type and unit of spectrum level data.

        This method classifies the numeric values contained in the
        spectrum 'levels' matrix into a semantic measurement type,
        based solely on value distribution and magnitude.

        Supported inferred level types:
            - Occupancy      (percent)
            - Power          (dBm)
            - Field strength (dBµV/m)
            - Unknown        (fallback)

        CONTRACT:
            - This method operates only on spectra that are already
            structurally and semantically valid.
            - Classification is deterministic and rule-based.
            - No probabilistic, heuristic, or metadata-based inference
            is performed.
            - Exactly one classification is selected, or the spectrum
            is marked as UNKNOWN.
            - The method MUST NOT raise exceptions.

        Args:
            s: Spectrum object assumed to be fully validated.
        """

        # -------------------------------------------------
        # Flatten level matrix for global statistical analysis
        # -------------------------------------------------
        levels = np.asarray(s.levels, dtype=float)
        flat = levels.ravel()

        # -------------------------------------------------
        # Basic distribution metrics used for classification
        # -------------------------------------------------
        vmin = float(flat.min())
        vmax = float(flat.max())
        vrange = vmax - vmin
        zero_ratio = float(np.mean(flat == 0))

        # Preserve any user-provided dtype for traceability
        s.user_dtype = getattr(s, "dtype", None)

        # -------------------------------------------------
        # Occupancy detection (percentage-based levels)
        # -------------------------------------------------
        # Characteristics:
        #   - Values are non-negative
        #   - Upper bound constrained to percentage scale
        #   - Significant proportion of zero values
        #   - Limited dynamic range
        if (
            vmin >= 0
            and vmax <= self.OCCUPANCY_MAX_PERCENT
            and zero_ratio > self.OCCUPANCY_ZERO_RATIO_THRESHOLD
            and vrange <= self.OCCUPANCY_MAX_RANGE
        ):
            s.level_type = self.LEVEL_TYPE_OCCUPANCY
            s.level_unit = self.LEVEL_UNIT_PERCENT
            s.level_source = self.LEVEL_SOURCE_INFERRED
            return

        # -------------------------------------------------
        # Power detection (dBm)
        # -------------------------------------------------
        # Characteristics:
        #   - Negative values are present
        #   - Upper bound below typical RF saturation levels
        #   - Sufficient dynamic range to represent power variation
        if (
            vmin < 0
            and vmax < self.POWER_MAX_DBM
            and vrange > self.POWER_MIN_RANGE_DB
        ):
            s.level_type = self.LEVEL_TYPE_POWER
            s.level_unit = self.LEVEL_UNIT_DBM
            s.level_source = self.LEVEL_SOURCE_INFERRED
            return

        # -------------------------------------------------
        # Field strength detection (dBµV/m)
        # -------------------------------------------------
        # Characteristics:
        #   - Values strictly positive
        #   - Magnitudes exceed typical power scale
        if (
            vmax > self.FIELD_STRENGTH_MAX_DB
            and vmin > self.FIELD_STRENGTH_MIN_DB
        ):
            s.level_type = self.LEVEL_TYPE_FIELD_STRENGTH
            s.level_unit = self.LEVEL_UNIT_DBUVM
            s.level_source = self.LEVEL_SOURCE_INFERRED
            return

        # -------------------------------------------------
        # Fallback: unknown or unsupported level semantics
        # -------------------------------------------------
        s.level_type = self.LEVEL_TYPE_UNKNOWN
        s.level_unit = None
        s.level_source = self.LEVEL_SOURCE_UNCLASSIFIED


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

