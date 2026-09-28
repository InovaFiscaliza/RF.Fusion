"""Validate and store uploads inside the fixed repository folder."""

import os
from pathlib import Path
from time import monotonic

from werkzeug.datastructures import FileStorage
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.utils import secure_filename

from modules.upload import config as k
from modules.host.service import get_all_hosts


class StationListUnavailable(RuntimeError):
    """Signal that the station catalog could not be read."""


def get_stations() -> list[dict]:
    """Read registered stations, including offline stations.

    Args:
        None.
    Returns:
        list[dict]: Rows with ID_HOST (int) and NA_HOST_NAME (str).
    Raises:
        StationListUnavailable: The catalog query failed.
    """
    try:
        return get_all_hosts()
    except Exception as exc:
        raise StationListUnavailable("Não foi possível consultar as estações. Tente novamente.") from exc


def _destination(category: str, station: str, drive_type: str) -> Path:
    """Resolve a validated classification to a storage directory.

    Args:
        category (str): Configured upload category.
        station (str): Registered host ID, required only for fixed stations.
        drive_type (str): Configured subtype, required only for drive-tests.
    Returns:
        Path: Existing destination below the deployed upload root.
    Raises:
        ValueError: Invalid classification or symbolic directory.
        StationListUnavailable: The station catalog is unavailable.
        OSError: The upload root or child directory is unavailable.
    """
    match category:
        case k.FIXED_CATEGORY:
            if drive_type or not station or station not in {str(row["ID_HOST"]) for row in get_stations()}:
                raise ValueError("Selecione uma estação fixa cadastrada.")
            parts = (k.FIXED_CATEGORY, station)
        case k.DRIVE_TEST_CATEGORY:
            if station or drive_type not in k.DRIVE_TEST_TYPES:
                raise ValueError("Selecione o tipo de drive-test.")
            parts = (k.DRIVE_TEST_CATEGORY, drive_type)
        case k.RNI_CATEGORY:
            if station or drive_type:
                raise ValueError("RNI não aceita estação ou tipo de drive-test.")
            parts = (k.RNI_CATEGORY,)
        case _:
            raise ValueError("Selecione o tipo de upload.")

    # Deployment owns the root; uploads may create only classified children.
    if not k.UPLOAD_FOLDER.is_dir():
        raise FileNotFoundError("Upload root is unavailable")
    folder = k.UPLOAD_FOLDER
    for part in parts:
        folder = folder / part
        if folder.is_symlink():
            raise ValueError("A pasta de destino não pode ser um link simbólico.")
        folder.mkdir(exist_ok=True)
    return folder


def save_upload(upload: FileStorage, category: str, station: str = "", drive_type: str = "") -> dict[str, str | int | float]:
    """Store one file without overwriting an existing file or symbolic link.

    Args:
        upload: Multipart file with a filename and binary stream. Type: FileStorage.
        category (str): Fixed station, drive-test or RNI category.
        station (str): Host ID for fixed stations; empty otherwise.
        drive_type (str): Drive-test subtype; empty otherwise.

    Returns:
        dict with name (str), size (int), elapsed_sec (float), folder (str).

    Raises:
        ValueError: Invalid filename, classification or symbolic directory.
        StationListUnavailable: The station catalog is unavailable.
        FileExistsError: The safe filename is already present.
        RequestEntityTooLarge: The stream exceeds the configured file limit.
        OSError: Storage is unavailable or writing fails.
    """
    started = monotonic()
    original = upload.filename or ""
    if "/" in original or "\\" in original or "\x00" in original:
        raise ValueError("Selecione um arquivo, sem caminhos no nome.")
    name = secure_filename(original)
    if not name or len(name.encode("utf-8")) > k.MAX_FILENAME_BYTES:
        raise ValueError("O nome do arquivo é inválido ou muito longo.")

    folder = _destination(category, station, drive_type)
    destination = folder / name
    size = 0
    target = destination.open("xb")
    try:
        with target:
            while chunk := upload.stream.read(k.COPY_CHUNK_BYTES):
                size += len(chunk)
                if size > k.MAX_FILE_BYTES:
                    raise RequestEntityTooLarge()
                target.write(chunk)
            target.flush()
            os.fsync(target.fileno())
    except BaseException:
        # Close first: CIFS may refuse to unlink an open file.
        # Exclusive creation means this request owns the file being removed.
        destination.unlink(missing_ok=True)
        raise
    return {"name": name, "size": size, "elapsed_sec": monotonic() - started,
            "folder": folder.relative_to(k.UPLOAD_FOLDER).as_posix()}
