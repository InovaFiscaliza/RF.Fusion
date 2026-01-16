"""
Abstract base class for spectrum acquisition stations.

This module defines the formal contract that all Station
implementations must follow.
"""

from abc import ABC, abstractmethod


class Station(ABC):
    """
    Abstract base class for spectrum acquisition stations.

    Contract:
    - A Station receives RAW parsed BIN data.
    - A Station MUST NOT mutate the input data.
    - A Station MUST return a NEW, enriched data structure.
    """
    # =================================================
    # Generic spectrum rules (station-agnostic)
    # =================================================
    MIN_NDATA: int = 1
    MIN_TEMPORAL_SPAN: int = 1  # start < stop

    # -------------------------------------------------
    # GPS semantics (generic)
    # -------------------------------------------------
    GPS_LAT_RANGE = (-90, 90)
    GPS_LON_RANGE = (-180, 180)
    GPS_SENTINEL_VALUE = -1
    GPS_NO_SATELLITES = 0

    # -------------------------------------------------
    # Level semantics (generic)
    # -------------------------------------------------
    OCCUPANCY_MAX_PERCENT = 100
    OCCUPANCY_ZERO_RATIO_THRESHOLD = 0.05

    POWER_MAX_DBM = 50
    POWER_MIN_RANGE_DB = 10

    FIELD_STRENGTH_MIN_DB = -20
    FIELD_STRENGTH_MAX_DB = 40

    # -------------------------------------------------
    # Method defaults (generic)
    # -------------------------------------------------
    DEFAULT_METHOD_SOURCE = "default"
    NATIVE_METHOD_SOURCE = "native"
    
    # =================================================
    # Level classification (generic)
    # =================================================
    LEVEL_TYPE_OCCUPANCY = "occupancy"
    LEVEL_TYPE_POWER = "power"
    LEVEL_TYPE_FIELD_STRENGTH = "field_strength"
    LEVEL_TYPE_UNKNOWN = "unknown"

    LEVEL_UNIT_PERCENT = "percent"
    LEVEL_UNIT_DBM = "dBm"
    LEVEL_UNIT_DBUVM = "dBµV/m"

    LEVEL_SOURCE_INFERRED = "inferred"
    LEVEL_SOURCE_NATIVE = "native"
    LEVEL_SOURCE_UNCLASSIFIED = "unclassified"
    
    
    def __init__(self, bin_data: dict, host_uid: str):
        """
        Initialize a Station instance.

        Args:
            bin_data (dict): Parsed BIN metadata and measurement data.
                             This object MUST be treated as read-only.
        """
        self._bin_data_raw = bin_data
        self._host_uid = host_uid

    @abstractmethod
    def process(self) -> dict:
        """
        Validate and enrich BIN data.

        Returns:
            dict: A NEW dictionary semantically equivalent to the input,
                  enriched and normalized for the PROCESS stage.

        Raises:
            BinValidationError: On any fatal inconsistency.

        Notes:
            - This method MUST NOT mutate the input data.
            - This method is the ONLY entry point for station logic.
        """
        raise NotImplementedError
