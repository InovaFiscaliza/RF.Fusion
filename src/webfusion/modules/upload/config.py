"""Define upload storage and resource limits."""

from pathlib import Path

UPLOAD_FOLDER = Path("/mnt/reposfi/upload")
MAX_FILE_BYTES = 512 * 1024 * 1024
MAX_REQUEST_BYTES = MAX_FILE_BYTES + 1024 * 1024
COPY_CHUNK_BYTES = 1024 * 1024
MAX_FILENAME_BYTES = 240
MAX_FORM_PARTS = 4
REQUEST_HEADER = "X-WebFusion-Upload"
REQUEST_HEADER_VALUE = "1"
FIXED_CATEGORY = "fixas"
DRIVE_TEST_CATEGORY = "drive-test"
RNI_CATEGORY = "rni"
DRIVE_TEST_TYPES = {"smp-romes": "SMP ROMES", "espectro": "Espectro"}
FORM_FIELDS = frozenset({"category", "station", "drive_type"})
