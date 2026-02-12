# stations/appcoleta.py

from .base import Station
import shared as sh


class AppColetaStation(Station):
    """
    Station representing appColeta acquisition.
    Validation rules may differ from RFeye and Celplan.
    """

    def validate(self) -> None:
        raise sh.BinValidationError(
            "appColeta validation not implemented yet"
        )
