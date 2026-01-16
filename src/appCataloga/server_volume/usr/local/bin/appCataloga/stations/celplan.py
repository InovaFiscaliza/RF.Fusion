# stations/celplan.py

from .base import Station
import shared as sh


class CelplanStation(Station):

    def validate(self) -> None:
        raise sh.BinValidationError(
            "Celplan validation not implemented yet"
        )
