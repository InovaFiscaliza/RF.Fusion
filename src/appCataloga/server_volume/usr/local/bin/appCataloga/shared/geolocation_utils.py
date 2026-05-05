"""
Shared geolocation helpers for processing workers.

These helpers keep reverse geocoding and Brazilian state-name enrichment out of
individual worker files so the workers can focus on their processing flow.
"""

from __future__ import annotations

import re
import time
import unicodedata
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

DISTRICT_FALLBACK_ADDRESS_KEYS = (
    "district",
    "borough",
    "quarter",
    "hamlet",
    "village",
    "residential",
)


def _normalize_admin_value(value: str | None) -> str | None:
    """Normalize one administrative label for stable comparisons."""
    if not value:
        return None

    normalized = unicodedata.normalize("NFKD", str(value).strip().lower())
    normalized = "".join(
        char for char in normalized if not unicodedata.combining(char)
    )
    normalized = re.sub(r"[’'`]", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized or None


def _collect_address_candidates(address: dict, keys: list[str]) -> list[str]:
    """Collect unique non-empty address values in priority order."""
    candidates = []
    seen = set()

    for key in keys:
        value = address.get(key)
        if value is None:
            continue

        candidate = str(value).strip()
        if not candidate:
            continue

        normalized = _normalize_admin_value(candidate)
        if not normalized or normalized in seen:
            continue

        candidates.append(candidate)
        seen.add(normalized)

    return candidates


def _assign_address_field(
    data: dict,
    *,
    field: str,
    address: dict,
    configured_keys: list[str],
) -> None:
    """Map one logical locality field plus its ordered fallback candidates."""
    candidate_keys = list(configured_keys)
    if field == "district":
        for key in DISTRICT_FALLBACK_ADDRESS_KEYS:
            if key not in candidate_keys:
                candidate_keys.append(key)

    values = _collect_address_candidates(address, candidate_keys)

    data[field] = values[0] if values else None

    candidates_key = f"{field}_candidates"
    if values:
        data[candidates_key] = values
    else:
        data.pop(candidates_key, None)


def _drop_redundant_district_candidates(data: dict) -> None:
    """Discard district labels that merely repeat the county or state."""
    redundant_values = {
        _normalize_admin_value(data.get("county")),
        _normalize_admin_value(data.get("state")),
    }
    redundant_values.discard(None)

    candidates = data.get("district_candidates") or []
    filtered = [
        candidate
        for candidate in candidates
        if _normalize_admin_value(candidate) not in redundant_values
    ]

    if filtered:
        data["district_candidates"] = filtered
        data["district"] = filtered[0]
        return

    data["district"] = None
    data.pop("district_candidates", None)


def _apply_county_as_last_district_fallback(data: dict) -> None:
    """Fallback to the municipality when Nominatim offers no better district."""
    if data.get("district"):
        return

    county = data.get("county")
    normalized_county = _normalize_admin_value(county)
    if not normalized_county:
        return

    data["district"] = county
    data["district_candidates"] = [county]


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


def reverse_geocode_site_data(
    data: dict,
    *,
    user_agent: str,
    required_address_field: dict,
) -> dict:
    """Reverse geocode one centroid and project it into site-data fields."""
    site_data = dict(data)
    location = reverse_geocode_with_retry(
        site_data,
        user_agent=user_agent,
    )
    return map_location_to_site_data(
        location,
        site_data,
        required_address_field,
    )


def map_location_to_site_data(location, data, required_address_field: dict) -> dict:
    """
    Map a Nominatim location payload into the internal SITE structure.
    """
    address = location.raw.get("address", {})

    for field, candidates in required_address_field.items():
        _assign_address_field(
            data,
            field=field,
            address=address,
            configured_keys=list(candidates),
        )

    if not data.get("state"):
        iso = address.get("ISO3166-2-lvl4")
        if iso and iso.startswith("BR-"):
            data["state"] = BRAZIL_UF_TO_STATE.get(iso[3:])
            if data.get("state"):
                data["state_candidates"] = [data["state"]]

    _drop_redundant_district_candidates(data)
    _apply_county_as_last_district_fallback(data)

    return data
