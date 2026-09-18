"""Define Microsoft Graph photo transport and local image storage limits."""

from pathlib import Path

GRAPH_BASE_URL = "https://graph.microsoft.com/v1.0"
TOKEN_ENV_NAME = "MICROSOFT_GRAPH_ACCESS_TOKEN"
TOKEN_FILE = Path(__file__).with_name(".env")
PROFILE_IMAGE_DIRECTORY = Path(__file__).resolve().parents[2] / "static/img/profiles"
PROFILE_IMAGE_URL_PREFIX = "/rffusion/static/img/profiles"
HTTP_TIMEOUT_SECONDS = 5
MAX_IMAGE_BYTES = 4 * 1024 * 1024
IMAGE_FORMATS = {
    "image/jpeg": (".jpg", b"\xff\xd8\xff"),
    "image/png": (".png", b"\x89PNG\r\n\x1a\n"),
}
