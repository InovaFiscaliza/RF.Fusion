"""Expose the email-to-photo URL connector for Microsoft Graph."""

from .client import (
    MicrosoftNotConfigured,
    MicrosoftPhotoError,
    MicrosoftPhotoNotFound,
    get_profile_image_url,
)

__all__ = [
    "get_profile_image_url", "MicrosoftNotConfigured",
    "MicrosoftPhotoError", "MicrosoftPhotoNotFound",
]
