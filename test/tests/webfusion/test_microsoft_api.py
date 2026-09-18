"""Verify Microsoft photo transport and storage without remote credentials."""

from __future__ import annotations

from email.message import Message
import importlib
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError


ROOT = Path(__file__).resolve().parents[3] / "src/webfusion"


class TestMicrosoftPhotos(unittest.TestCase):
    """Exercise the public email-to-URL function with isolated HTTP and storage.

    Attributes:
        client: Imported connector module. Type: module.
        directory: Temporary cache and configuration directory. Type: Path.
        response: Mocked HTTP response. Type: MagicMock.
        open_request: Mocked opener call. Type: MagicMock.
    """

    def setUp(self) -> None:
        """Replace HTTP, token configuration, and image storage for each test.

        Args:
            None.

        Returns:
            None.
        """
        sys.path.insert(0, str(ROOT))
        self.client = importlib.import_module("api.microsoft_api.client")
        folder = TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.directory = Path(folder.name)
        patches = [
            patch.object(self.client.k, "TOKEN_FILE", self.directory / ".env"),
            patch.object(self.client.k, "PROFILE_IMAGE_DIRECTORY", self.directory / "profiles"),
            patch.dict(os.environ, {"MICROSOFT_GRAPH_ACCESS_TOKEN": "test-token"}),
            patch.object(self.client, "build_opener"),
        ]
        for context in patches:
            result = context.start()
            self.addCleanup(context.stop)
        self.open_request = result.return_value.open
        self.response = MagicMock()
        self.response.status = 200
        self.response.headers = Message()
        self.response.headers["Content-Type"] = "image/jpeg"
        self.response.read.return_value = b"\xff\xd8\xfftest-photo"
        self.open_request.return_value.__enter__.return_value = self.response

    def test_photo_content_changes_url_and_preserves_old_file(self) -> None:
        """Only changed photo content produces a new file and public URL.

        Args:
            None.

        Returns:
            None.
        """
        first = self.client.get_profile_image_url(" MARIA@example.org ")
        same = self.client.get_profile_image_url("maria@example.org")
        self.assertEqual(first, same)
        self.assertNotIn("maria", first)
        self.assertNotIn("test-token", first)
        self.assertEqual(len(list((self.directory / "profiles").iterdir())), 1)
        request = self.open_request.call_args.args[0]
        self.assertIn("maria%40example.org/photo/$value", request.full_url)
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")
        self.response.read.return_value = b"\xff\xd8\xffupdated-photo"
        changed = self.client.get_profile_image_url("maria@example.org")
        self.assertNotEqual(first, changed)
        self.assertEqual(len(list((self.directory / "profiles").iterdir())), 2)

    def test_absent_token_does_not_contact_graph(self) -> None:
        """Missing credentials produce a typed configuration exception.

        Args:
            None.

        Returns:
            None.
        """
        with patch.dict(os.environ, {"MICROSOFT_GRAPH_ACCESS_TOKEN": ""}):
            with self.assertRaises(self.client.MicrosoftNotConfigured):
                self.client.get_profile_image_url("maria@example.org")
        self.open_request.assert_not_called()

    def test_local_token_file_can_be_configured_later(self) -> None:
        """A mounted token file enables the connector without hardcoded secrets.

        Args:
            None.

        Returns:
            None.
        """
        self.client.k.TOKEN_FILE.write_text("MICROSOFT_GRAPH_ACCESS_TOKEN=file-token\n")
        with patch.dict(os.environ, {"MICROSOFT_GRAPH_ACCESS_TOKEN": ""}):
            self.client.get_profile_image_url("maria@example.org")
        self.assertEqual(self.open_request.call_args.args[0].get_header("Authorization"), "Bearer file-token")

    def test_transport_errors_do_not_create_images(self) -> None:
        """HTTP failures, expiry, throttling, and timeouts do not become URLs.

        Args:
            None.

        Returns:
            None.
        """
        for code in (401, 403, 404, 429, 500):
            with self.subTest(status=code):
                self.open_request.side_effect = HTTPError("https://graph.microsoft.com", code, "error", {}, None)
                expected = self.client.MicrosoftPhotoNotFound if code == 404 else self.client.MicrosoftPhotoError
                with self.assertRaises(expected):
                    self.client.get_profile_image_url("maria@example.org")
        self.open_request.side_effect = URLError("timeout")
        with self.assertRaises(self.client.MicrosoftPhotoError):
            self.client.get_profile_image_url("maria@example.org")
        self.assertFalse((self.directory / "profiles").exists())

    def test_rejects_invalid_or_oversized_image_content(self) -> None:
        """Only bounded JPEG or PNG image payloads reach static storage.

        Args:
            None.

        Returns:
            None.
        """
        for content in (b"", b"<html>login</html>", b"\xff\xd8\xff" + b"x" * self.client.k.MAX_IMAGE_BYTES):
            with self.subTest(size=len(content)):
                self.response.read.return_value = content
                with self.assertRaises(self.client.MicrosoftPhotoError):
                    self.client.get_profile_image_url("maria@example.org")
        self.response.headers.replace_header("Content-Type", "image/svg+xml")
        self.response.read.return_value = b"<svg/>"
        with self.assertRaises(self.client.MicrosoftPhotoError):
            self.client.get_profile_image_url("maria@example.org")
        self.assertFalse((self.directory / "profiles").exists())

    def test_redirects_cannot_forward_bearer_credentials(self) -> None:
        """Redirects fail before another origin can receive the Graph token.

        Args:
            None.

        Returns:
            None.
        """
        with self.assertRaises(self.client.MicrosoftPhotoError):
            self.client._RejectRedirects().redirect_request(None, None, 302, "Moved", {}, "https://other.example")
