"""
CelPlan station handler.

This legacy adapter still delegates processing to the remote CelPlan/appAnalise
service and exists so older station routing paths continue to work after the
package reorganization.
"""

import json
import os
import re
import socket
import sys
from typing import Dict

from .base import Station
from shared import errors


BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402


class CelplanStation(Station):
    """Legacy CelPlan adapter kept for station-factory compatibility."""

    START_TAG = "<JSON>"
    END_TAG = "</JSON>"
    PROCESSOR_NAME = "celplan"

    def process(self, file_path: str, file_name: str) -> Dict:
        if not isinstance(file_path, str) or not file_path:
            raise errors.BinValidationError("CelPlanStation: invalid file_path")

        if not isinstance(file_name, str) or not file_name:
            raise errors.BinValidationError("CelPlanStation: invalid file_name")

        if not file_name.lower().endswith(".dbm"):
            raise errors.BinValidationError("CelPlanStation supports only .dbm files")

        full_path = os.path.join(file_path, file_name)
        raw_payload = self._call_celplan_service(full_path)
        return self._normalize_response(raw_payload, file_path, file_name)

    def _call_celplan_service(self, full_path: str) -> Dict:
        request_payload = {
            "Key": k.APP_ANALISE_KEY,
            "ClientName": k.APP_ANALISE_CLIENT_NAME,
            "Request": {"type": "FileRead", "filepath": full_path},
        }

        request_bytes = (
            json.dumps(request_payload, ensure_ascii=False) + "\r\n"
        ).encode("utf-8")

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(k.APP_ANALISE_SOCKET_TIMEOUT)

        try:
            sock.connect((k.APP_ANALISE_HOST_ADD, k.APP_ANALISE_HOST_PORT))
            sock.sendall(request_bytes)
            raw_response = self._receive_all(sock)
        except Exception as exc:
            raise errors.BinValidationError(
                f"APP_ANALISE socket error: {exc}"
            ) from exc
        finally:
            sock.close()

        return self._extract_json(self._safe_decode(raw_response))

    @staticmethod
    def _safe_decode(data: bytes) -> str:
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data.decode("latin-1")

    def _receive_all(self, sock: socket.socket) -> bytes:
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
        match = re.search(
            rf"{self.START_TAG}(.*?){self.END_TAG}",
            payload,
            re.DOTALL | re.IGNORECASE,
        )

        if not match:
            raise errors.BinValidationError(
                "CelPlan response does not contain <JSON> block"
            )

        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError as exc:
            raise errors.BinValidationError(
                f"Invalid JSON returned by CelPlan service: {exc}"
            ) from exc

    def _normalize_response(self, payload: Dict, file_path: str, file_name: str) -> Dict:
        answer = payload.get("Answer", {})
        metadata = answer.get("MetaData", {})
        gps = answer.get("GPS", {})

        return {
            "equipment_uid": answer.get("Receiver"),
            "processor": self.PROCESSOR_NAME,
            "file_path": file_path,
            "file_name": file_name,
            "datatype": metadata.get("DataType"),
            "freq_start_hz": metadata.get("FreqStart"),
            "freq_stop_hz": metadata.get("FreqStop"),
            "datapoints": metadata.get("DataPoints"),
            "resolution_hz": metadata.get("Resolution"),
            "level_unit": metadata.get("LevelUnit"),
            "latitude": gps.get("Latitude"),
            "longitude": gps.get("Longitude"),
            "altitude": gps.get("Altitude"),
        }
