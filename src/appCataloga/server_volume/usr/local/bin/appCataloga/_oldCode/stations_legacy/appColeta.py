"""
Generic legacy station fallback.
"""

from .base import Station
from shared import errors


class AppColetaStation(Station):
    """Fallback station used when no concrete legacy adapter matches."""

    def process(self) -> dict:
        raise errors.BinValidationError(
            "appColeta validation not implemented yet"
        )
