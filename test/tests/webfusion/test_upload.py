"""Exercise upload HTTP contracts and filesystem safety without production data."""

import errno
import importlib
import io
from pathlib import Path
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

WEBFUSION_ROOT = Path(__file__).resolve().parents[3] / "src/webfusion"
sys.path.insert(0, str(WEBFUSION_ROOT))

from werkzeug.datastructures import FileStorage, MultiDict
from modules.upload import config as k, service


class TestUpload(unittest.TestCase):
    """Validate the actual app and exclusive writes using an isolated folder.

    Attributes:
        client: Flask test client bound to the production app with mocked identity I/O.
        folder: Path to a disposable upload directory.
        headers: dict[str, str] containing a proxy email and the upload header.
    """

    def setUp(self) -> None:
        """Prepare storage and stub external identity persistence.

        Args:
            None.
        Returns:
            None.
        """
        module = importlib.import_module("app")
        module.app.config.update(TESTING=True)
        self.client = module.app.test_client()
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.folder = Path(temporary.name)
        self.destination = self.folder / "rni"
        stations = patch.object(service, "get_all_hosts", return_value=[{"ID_HOST": 42, "NA_HOST_NAME": "Estação teste"}])
        stations.start()
        self.addCleanup(stations.stop)
        storage = patch.object(k, "UPLOAD_FOLDER", self.folder)
        storage.start()
        self.addCleanup(storage.stop)
        for method in ("record_observed_user", "load_profile_image", "load_access_role"):
            stub = patch.object(module.AUTH_SERVICE, method)
            stub.start()
            self.addCleanup(stub.stop)
        self.headers = {"X-User-Email": "upload@example.org", k.REQUEST_HEADER: k.REQUEST_HEADER_VALUE}

    def _post(self, name: str = "sample.bin", data: bytes = b"sample"):
        """Send a binary multipart upload through the actual app.

        Args:
            name: Submitted filename (str).
            data: File content (bytes).
        Returns:
            Flask test response with JSON metadata or an error.
        """
        return self.client.post("/api/upload", headers=self.headers,
                                data={"category": "rni", "file": (io.BytesIO(data), name)})

    def test_upload_saves_exact_bytes_and_safe_name(self) -> None:
        """Check persistence and name normalization. Args: None. Returns: None."""
        response = self._post("medição RF.bin", b"\x00\xffRF")
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json["name"], "medicao_RF.bin")
        self.assertEqual(response.json["size"], 4)
        self.assertEqual((self.destination / response.json["name"]).read_bytes(), b"\x00\xffRF")

    def test_empty_file_is_valid(self) -> None:
        """Accept zero-byte files. Args: None. Returns: None."""
        self.assertEqual(self._post(data=b"").status_code, 201)

    def test_existing_file_is_not_overwritten(self) -> None:
        """Preserve a previous upload. Args: None. Returns: None."""
        self._post(data=b"first")
        self.assertEqual(self._post(data=b"second").status_code, 409)
        self.assertEqual((self.destination / "sample.bin").read_bytes(), b"first")

    def test_symlink_is_not_followed(self) -> None:
        """Reject existing symbolic links. Args: None. Returns: None."""
        self.destination.mkdir()
        target = self.folder / "target.bin"
        target.write_bytes(b"original")
        (self.destination / "sample.bin").symlink_to(target)
        self.assertEqual(self._post().status_code, 409)
        self.assertEqual(target.read_bytes(), b"original")

    def test_invalid_names_do_not_escape_destination(self) -> None:
        """Reject path traversal and unusable names. Args: None. Returns: None."""
        for name in ("../outside.bin", "/tmp/outside.bin", "..\\outside.bin", "...", "a" * 241, ""):
            with self.subTest(name=name):
                self.assertEqual(self._post(name).status_code, 400)
        self.assertEqual(list(self.folder.iterdir()), [])

    def test_anonymous_and_invalid_identity_are_rejected(self) -> None:
        """Require the existing validated identity. Args: None. Returns: None."""
        for email in ("", "not-an-email"):
            self.headers["X-User-Email"] = email
            self.assertEqual(self._post().status_code, 401)
            self.assertEqual(self.client.get("/upload", headers=self.headers).status_code, 401)
        self.assertEqual(list(self.folder.iterdir()), [])

    def test_cross_origin_form_cannot_upload(self) -> None:
        """Require the non-simple browser header. Args: None. Returns: None."""
        self.headers.pop(k.REQUEST_HEADER)
        self.assertEqual(self._post().status_code, 403)
        response = self.client.options("/api/upload", headers={
            "Origin": "https://other.example", "Access-Control-Request-Headers": k.REQUEST_HEADER})
        self.assertNotIn("Access-Control-Allow-Origin", response.headers)

    def test_missing_or_multiple_files_are_rejected(self) -> None:
        """Keep each request's result unambiguous. Args: None. Returns: None."""
        for data in ({}, MultiDict([("file", (io.BytesIO(b"a"), "a.bin")),
                                   ("file", (io.BytesIO(b"b"), "b.bin"))])):
            response = self.client.post("/api/upload", headers=self.headers, data=data)
            self.assertEqual(response.status_code, 400)
        self.assertEqual(list(self.folder.iterdir()), [])

    def test_file_limit_cleans_partial_file(self) -> None:
        """Enforce stream size and clean failed writes. Args: None. Returns: None."""
        with patch.object(k, "MAX_FILE_BYTES", 3), patch.object(k, "COPY_CHUNK_BYTES", 2):
            self.assertEqual(self._post(data=b"123").status_code, 201)
            self.assertEqual(self._post("large.bin", b"1234").status_code, 413)
        self.assertFalse((self.destination / "large.bin").exists())

    def test_request_limit_returns_json(self) -> None:
        """Enforce multipart body size. Args: None. Returns: None."""
        with patch.object(k, "MAX_REQUEST_BYTES", 8):
            response = self._post()
        self.assertEqual(response.status_code, 413)
        self.assertIn("error", response.json)

    def test_storage_errors_are_actionable(self) -> None:
        """Report unavailable and full storage. Args: None. Returns: None."""
        for code, status in ((errno.EROFS, 503), (errno.ENOSPC, 507)):
            with patch.object(service, "save_upload", side_effect=OSError(code, "test")):
                self.assertEqual(self._post().status_code, status)

    def test_flush_failure_removes_file(self) -> None:
        """Clean a file whose persistence failed. Args: None. Returns: None."""
        with patch.object(service.os, "fsync", side_effect=OSError(errno.ENOSPC, "test")):
            self.assertEqual(self._post().status_code, 507)
        self.assertEqual(list(self.destination.iterdir()), [])

    def test_concurrent_same_name_has_one_winner(self) -> None:
        """Prevent concurrent overwrites. Args: None. Returns: None."""
        uploads = [FileStorage(stream=io.BytesIO(data), filename="race.bin") for data in (b"one", b"two")]
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(service.save_upload, upload, "rni") for upload in uploads]
        outcomes = [future.exception() for future in futures]
        self.assertEqual(sum(error is None for error in outcomes), 1)
        self.assertEqual(sum(isinstance(error, FileExistsError) for error in outcomes), 1)
        self.assertIn((self.destination / "race.bin").read_bytes(), (b"one", b"two"))

    def test_classification_routes_identical_names_to_separate_folders(self) -> None:
        """Separate categories, subtypes and stations. Args: None. Returns: None."""
        cases = [({"category": "fixas", "station": "42"}, "fixas/42"),
                 ({"category": "drive-test", "drive_type": "smp-romes"}, "drive-test/smp-romes"),
                 ({"category": "drive-test", "drive_type": "espectro"}, "drive-test/espectro"),
                 ({"category": "rni"}, "rni")]
        for metadata, folder in cases:
            with self.subTest(folder=folder):
                response = self.client.post("/api/upload", headers=self.headers,
                    data={**metadata, "file": (io.BytesIO(folder.encode()), "same.bin")})
                self.assertEqual(response.status_code, 201)
                self.assertEqual(response.json["folder"], folder)
                self.assertEqual((self.folder / folder / "same.bin").read_bytes(), folder.encode())

    def test_invalid_classification_is_rejected(self) -> None:
        """Reject untrusted metadata before creating folders. Args: None. Returns: None."""
        cases = [{}, {"category": "../escape"}, {"category": "fixas"},
                 {"category": "fixas", "station": "999"},
                 {"category": "fixas", "station": "../42"},
                 {"category": "drive-test", "drive_type": "unknown"},
                 {"category": "rni", "station": "42"},
                 {"category": "rni", "folder": "somewhere"}]
        for metadata in cases:
            response = self.client.post("/api/upload", headers=self.headers,
                data={**metadata, "file": (io.BytesIO(b"data"), "file.bin")})
            self.assertEqual(response.status_code, 400)
        self.assertEqual(list(self.folder.iterdir()), [])

    def test_symbolic_directory_is_rejected(self) -> None:
        """Do not follow a linked classification folder. Args: None. Returns: None."""
        with tempfile.TemporaryDirectory() as outside:
            self.destination.symlink_to(outside, target_is_directory=True)
            self.assertEqual(self._post().status_code, 400)
            self.assertEqual(list(Path(outside).iterdir()), [])

    def test_catalog_failure_does_not_block_other_categories(self) -> None:
        """Fixed station selection fails closed. Args: None. Returns: None."""
        with patch.object(service, "get_all_hosts", side_effect=RuntimeError("unavailable")):
            response = self.client.post("/api/upload", headers=self.headers,
                data={"category": "fixas", "station": "42", "file": (io.BytesIO(b"data"), "file.bin")})
            self.assertEqual(response.status_code, 503)
            self.assertEqual(self._post().status_code, 201)
            page = self.client.get("/upload", headers=self.headers)
            self.assertEqual(page.status_code, 200)
            self.assertIn("Não foi possível consultar".encode(), page.data)

    def test_page_uses_public_prefix_and_common_navigation(self) -> None:
        """Render the real template behind the deployment prefix. Args: None. Returns: None."""
        response = self.client.get("/upload", headers={**self.headers, "X-Forwarded-Prefix": "/rffusion"})
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn('data-upload-url="/rffusion/api/upload"', html)
        self.assertIn('href="/rffusion/upload"', html)
        self.assertIn('type="file" id="upload-input" multiple', html)


if __name__ == "__main__":
    unittest.main()
