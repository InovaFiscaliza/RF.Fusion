from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(slots=True, frozen=True)
class FileMetadata:
    # --- identity / path ---
    NA_FULL_PATH: str
    NA_PATH: str
    NA_FILE: str
    NA_EXTENSION: str

    # --- size ---
    VL_FILE_SIZE_KB: int

    # --- timestamps ---
    DT_FILE_CREATED: Optional[datetime]
    DT_FILE_MODIFIED: Optional[datetime]
    DT_FILE_ACCESSED: Optional[datetime]

    # --- ownership / permissions ---
    NA_OWNER: str
    NA_GROUP: str
    NA_PERMISSIONS: str
