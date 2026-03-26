"""
Shared geolocation helpers for processing workers.

These helpers keep reverse geocoding and Brazilian state-name enrichment out of
individual worker files so the workers can focus on their processing flow.
"""

from __future__ import annotations

import time
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim


BRAZIL_UF_TO_STATE = {
    "RO": "Rondônia",
    "AC": "Acre",
    "AM": "Amazonas",
    "RR": "Roraima",
    "PA": "Pará",
    "AP": "Amapá",
    "TO": "Tocantins",
    "MA": "Maranhão",
    "PI": "Piauí",
    "CE": "Ceará",
    "RN": "Rio Grande do Norte",
    "PB": "Paraíba",
    "PE": "Pernambuco",
    "AL": "Alagoas",
    "SE": "Sergipe",
    "BA": "Bahia",
    "MG": "Minas Gerais",
    "ES": "Espírito Santo",
    "RJ": "Rio de Janeiro",
    "SP": "São Paulo",
    "PR": "Paraná",
    "SC": "Santa Catarina",
    "RS": "Rio Grande do Sul",
    "MS": "Mato Grosso do Sul",
    "MT": "Mato Grosso",
    "GO": "Goiás",
    "DF": "Distrito Federal",
}


def reverse_geocode_with_retry(
    data,
    *,
    user_agent: str,
    attempt: int = 1,
    max_attempts: int = 10,
) -> object:
    """
    Perform reverse geocoding using Nominatim with bounded retry logic.
    """
    point = (data["latitude"], data["longitude"])
    geocoding = Nominatim(user_agent=user_agent, timeout=5)

    try:
        return geocoding.reverse(point, timeout=5 + attempt, language="pt")
    except GeocoderTimedOut:
        if attempt < max_attempts:
            time.sleep(2)
            return reverse_geocode_with_retry(
                data,
                user_agent=user_agent,
                attempt=attempt + 1,
                max_attempts=max_attempts,
            )
        raise


def map_location_to_site_data(location, data, required_address_field: dict) -> dict:
    """
    Map a Nominatim location payload into the internal SITE structure.
    """
    address = location.raw.get("address", {})

    for field, candidates in required_address_field.items():
        data[field] = None
        for candidate in candidates:
            if candidate in address:
                data[field] = address[candidate]
                break

    if not data.get("state"):
        iso = address.get("ISO3166-2-lvl4")
        if iso and iso.startswith("BR-"):
            data["state"] = BRAZIL_UF_TO_STATE.get(iso[3:])

    return data
