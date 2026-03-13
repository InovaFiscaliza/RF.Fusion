"""
Station factory and registration module.
"""

from .rfeye import RFEyeStation
from .celplan import CelplanStation
from .appColeta import AppColetaStation
from stations.appAnaliseConnection import AppAnaliseConnection


def station_factory(*, bin_data: dict, host_uid: str):
    """
    Create and return a Station instance based on resolved host UID.

    Args:
        bin_data (dict): Parsed BIN metadata and measurement data.
        host_uid (str): Canonical host identifier resolved from database.

    Returns:
        Station: A concrete Station subclass.

    Design rules:
        - The factory MUST NOT mutate input data.
        - The factory MUST NOT perform validation or enrichment.
        - Station detection is authoritative and deterministic.
        - No heuristics based on BIN metadata are allowed.
    """

    if not isinstance(host_uid, str) or not host_uid.strip():
        raise ValueError("station_factory requires a valid host_uid string")

    h = host_uid.strip().lower()

    if "rfeye" in h:
        return RFEyeStation(bin_data,host_uid)

    if "cw" in h or "celplan" in h:
        return CelplanStation(bin_data,host_uid)

    # Generic / legacy / unknown origin
    return AppColetaStation(bin_data, host_uid)
