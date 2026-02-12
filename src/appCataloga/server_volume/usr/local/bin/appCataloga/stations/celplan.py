"""
CelPlan station handler.

This station does not parse DBM files locally.
Instead, it delegates processing to a remote CelPlan service
via TCP socket and receives normalized spectrum metadata.

RF.Fusion integration note:
    RF.Fusion provides file_path and file_name separately.
    CelPlan requires an absolute filepath.
    This class is responsible for composing the full path.
"""
import sys
import socket
import json
import re
import os

from typing import Dict

from .base import Station
from shared import errors

# ---------------------------------------------------------------------
# Config import path (as in original code). We keep the behavior so the
# module remains drop-in compatible with existing deployments.
# ---------------------------------------------------------------------
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402  (must be available at runtime)


class CelplanStation(Station):
    """
    CelPlan station implementation.

    Processing model:
        - Input: file_path + file_name (RF.Fusion contract)
        - Processing: remote CelPlan service via socket
        - Output: normalized spectrum metadata

    This class is stateless and request/response based.
    """

    # -------------------------------------------------
    # Protocol constants
    # -------------------------------------------------
    START_TAG = "<JSON>"
    END_TAG = "</JSON>"

    BUFFER_SIZE = 4096
    SOCKET_TIMEOUT = 10

    PROCESSOR_NAME = "celplan"

    # -------------------------------------------------
    # Public contract
    # -------------------------------------------------
    def process(self, file_path: str, file_name: str) -> Dict:
        """
        Process a DBM file using the CelPlan remote service.

        Args:
            file_path (str): Directory where the file is located.
            file_name (str): DBM file name.

        Returns:
            dict: Normalized spectrum metadata ready for RF.Fusion.

        Raises:
            BinValidationError: On any fatal processing or protocol error.
        """

        if not isinstance(file_path, str) or not file_path:
            raise errors.BinValidationError(
                "CelPlanStation: invalid file_path"
            )

        if not isinstance(file_name, str) or not file_name:
            raise errors.BinValidationError(
                "CelPlanStation: invalid file_name"
            )

        if not file_name.lower().endswith(".dbm"):
            raise errors.BinValidationError(
                "CelPlanStation supports only .dbm files"
            )

        # -------------------------------------------------
        # Compose absolute file path (adapter responsibility)
        # -------------------------------------------------
        full_path = os.path.join(file_path, file_name)

        raw_payload = self._call_celplan_service(full_path)
        return self._normalize_response(raw_payload, file_path, file_name)

    # =================================================
    # Socket communication
    # =================================================
    def _call_celplan_service(self, full_path: str) -> Dict:
        """
        Send a FileRead request to the CelPlan service and
        return the parsed JSON payload.
        """

        request_payload = {
            "Key": k.APP_ANALISE_KEY,
            "ClientName": k.APP_ANALISE_CLIENT_NAME,
            "Request": {
                "type": "FileRead",
                "filepath": full_path
            }
        }

        request_bytes = (
            json.dumps(request_payload, ensure_ascii=False) + "\r\n"
        ).encode("utf-8")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(k.APP_ANALISE_SOCKET_TIMEOUT)

        try:
            sock.connect((
                k.APP_ANALISE_HOST_ADD,
                k.APP_ANALISE_HOST_PORT
            ))
            sock.sendall(request_bytes)

            raw_response = self._receive_all(sock)

        except Exception as e:
            raise errors.BinValidationError(
                f"APP_ANALISE socket error: {e}"
            )

        finally:
            sock.close()

        response_text = self._safe_decode(raw_response)
        return self._extract_json(response_text)


    # =================================================
    # Helpers
    # =================================================
    @staticmethod
    def _safe_decode(data: bytes) -> str:
        """
        Decode socket payload using UTF-8 with Latin-1 fallback.

        CelPlan legacy services may return ISO-8859-1 encoded data.
        """
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data.decode("latin-1")

    def _receive_all(self, sock: socket.socket) -> bytes:
        """
        Receive all data from the socket until close or timeout.
        """
        chunks = []

        while True:
            try:
                chunk = sock.recv(k.APP_ANALISE_BUFFER_SIZE)
                if not chunk:
                    break
                chunks.append(chunk)
            except socket.timeout:
                break

        return b"".join(chunks)

    def _extract_json(self, payload: str) -> Dict:
        """
        Extract and parse JSON wrapped inside <JSON>...</JSON> tags.
        """

        match = re.search(
            rf"{self.START_TAG}(.*?){self.END_TAG}",
            payload,
            re.DOTALL | re.IGNORECASE
        )

        if not match:
            raise errors.BinValidationError(
                "CelPlan response does not contain <JSON> block"
            )

        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError as e:
            raise errors.BinValidationError(
                f"Invalid JSON returned by CelPlan service: {e}"
            )

    # =================================================
    # Normalization
    # =================================================
    def _normalize_response(
        self,
        payload: Dict,
        file_path: str,
        file_name: str
    ) -> Dict:
        """
        Normalize CelPlan payload into RF.Fusion canonical format.
        """

        answer = payload.get("Answer", {})
        metadata = answer.get("MetaData", {})
        gps = answer.get("GPS", {})

        return {
            # Identification
            "equipment_uid": answer.get("Receiver"),
            "processor": self.PROCESSOR_NAME,

            # File (preserve RF.Fusion model)
            "file_path": file_path,
            "file_name": file_name,

            # Spectrum metadata
            "datatype": metadata.get("DataType"),
            "freq_start_hz": metadata.get("FreqStart"),
            "freq_stop_hz": metadata.get("FreqStop"),
            "datapoints": metadata.get("DataPoints"),
            "resolution_hz": metadata.get("Resolution"),
            "level_unit": metadata.get("LevelUnit"),

            # Geolocation
            "latitude": gps.get("Latitude"),
            "longitude": gps.get("Longitude"),
            "location": gps.get("Location"),
        }
