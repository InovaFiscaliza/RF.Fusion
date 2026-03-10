"""
appAnaliseConnection
====================

Adapter responsible for communicating with the external MATLAB-based
processing service **appAnalise**.

This module allows RF.Fusion workers to offload spectrum file parsing
to a remote processing engine through a TCP socket.

The service receives a request containing a file path and returns a
JSON structure describing spectrum metadata and optional exported
artifacts (e.g. .mat files).

The adapter converts the response into the canonical RF.Fusion
structure (`bin_data`) so that existing processing pipelines remain
unchanged.

Architecture
------------

RF.Fusion Worker
        │
        │ socket request
        ▼
appAnalise (MATLAB processing engine)
        │
        │ JSON payload
        ▼
AppAnaliseConnection
        │
        ▼
RF.Fusion canonical structure (bin_data)

Design principles
-----------------

• RF.Fusion workers must not understand file formats.
• All format-specific logic belongs to appAnalise.
• This adapter validates and normalizes responses.
• No database operations occur here.
• No filesystem manipulation occurs except export validation.

Error model
-----------

Any protocol or validation failure raises:

    errors.BinValidationError

This preserves compatibility with the existing RF.Fusion worker
error handling model.
"""

import os
import sys
import re
import socket
import json
import time

from datetime import datetime
from types import SimpleNamespace
from typing import Dict

from shared import errors


# =========================================================
# Resolve configuration path
# =========================================================

BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa


# =========================================================
# Adapter
# =========================================================

class AppAnaliseConnection:
    """
    Adapter responsible for interacting with the appAnalise service.
    """

    START_TAG = "<JSON>"
    END_TAG = "</JSON>"

    PROCESSOR_NAME = "appAnalise"

    MAX_RESPONSE_SIZE = 10 * 1024 * 1024  # 10 MB safety limit
    NETWORK_RETRIES = 2

    # =================================================
    # Socket request
    # =================================================

    def _request_process(self, full_path: str, export: bool = False) -> Dict:
        """
        Send processing request to appAnalise.
        """

        request_payload = {
            "Key": k.APP_ANALISE_KEY,
            "ClientName": k.APP_ANALISE_CLIENT_NAME,
            "Request": {
                "type": "FileRead",
                "filepath": full_path,
                "export": export
            }
        }

        request_bytes = (
            json.dumps(request_payload, ensure_ascii=False) + "\r\n"
        ).encode("utf-8")

        last_error = None

        for attempt in range(self.NETWORK_RETRIES):

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:

                # Connection timeout
                sock.settimeout(k.APP_ANALISE_CONNECT_TIMEOUT)
                sock.connect(
                    (k.APP_ANALISE_HOST_ADD, k.APP_ANALISE_HOST_PORT)
                )

                # Processing timeout
                sock.settimeout(k.APP_ANALISE_PROCESS_TIMEOUT)
                sock.sendall(request_bytes)
                raw_response = self._receive_all(sock)
                response_text = self._safe_decode(raw_response)

                return self._extract_json(response_text)

            except Exception as e:

                last_error = e
                time.sleep(0.5)

            finally:

                try:
                    sock.close()
                except Exception:
                    pass

        raise errors.BinValidationError(
            f"APP_ANALISE socket failure after retries: {last_error}"
        )

    # =================================================
    # Receive socket stream
    # =================================================

    def _receive_all(self, sock: socket.socket) -> bytes:

        buffer = b""

        while True:
            try:
                chunk = sock.recv(k.APP_ANALISE_BUFFER_SIZE)

            except socket.timeout:
                raise errors.BinValidationError(
                    "APP_ANALISE processing timeout"
                )

            if not chunk:
                break

            buffer += chunk

            if len(buffer) > self.MAX_RESPONSE_SIZE:
                raise errors.BinValidationError(
                    "APP_ANALISE response exceeded maximum size"
                )

            start = buffer.find(b"<JSON>")
            end = buffer.find(b"</JSON>")

            if start != -1 and end != -1 and end > start:
                end += len(b"</JSON>")
                return buffer[start:end]

        raise errors.BinValidationError(
            "APP_ANALISE response did not contain a complete JSON block"
        )

    # =================================================
    # Decode payload
    # =================================================

    @staticmethod
    def _safe_decode(data: bytes) -> str:

        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data.decode("latin-1")

    # =================================================
    # Extract JSON
    # =================================================

    def _extract_json(self, payload: str) -> Dict:

        match = re.search(
            rf"{self.START_TAG}(.*?){self.END_TAG}",
            payload,
            re.DOTALL | re.IGNORECASE
        )

        if not match:
            raise errors.BinValidationError(
                "APP_ANALISE response missing JSON block"
            )

        try:
            return json.loads(match.group(1).strip())

        except json.JSONDecodeError as e:
            raise errors.BinValidationError(
                f"Invalid JSON returned by APP_ANALISE: {e}"
            )

    # =================================================
    # Payload validation
    # =================================================

    def _validate_payload(self, payload: Dict):

        if "Answer" not in payload:
            raise errors.BinValidationError(
                "APP_ANALISE response missing 'Answer'"
            )

        answer = payload["Answer"]

        if not answer.get("Receiver"):
            raise errors.BinValidationError(
                "APP_ANALISE response missing Receiver"
            )

        metadata = answer.get("MetaData")

        if not metadata:
            raise errors.BinValidationError(
                "APP_ANALISE response missing MetaData"
            )

        freq_start = metadata.get("FreqStart")
        freq_stop = metadata.get("FreqStop")

        if freq_start is None or freq_stop is None:
            raise errors.BinValidationError(
                "APP_ANALISE MetaData missing frequency limits"
            )

        if freq_start <= 0 or freq_stop <= 0:
            raise errors.BinValidationError(
                "APP_ANALISE invalid frequency values"
            )

        if freq_start >= freq_stop:
            raise errors.BinValidationError(
                "APP_ANALISE frequency start >= stop"
            )

        datapoints = metadata.get("DataPoints")

        if datapoints is None or datapoints <= 0:
            raise errors.BinValidationError(
                "APP_ANALISE invalid DataPoints"
            )

        gps = answer.get("GPS")

        if not gps:
            raise errors.BinValidationError(
                "APP_ANALISE response missing GPS"
            )

        if gps.get("Latitude") is None or gps.get("Longitude") is None:
            raise errors.BinValidationError(
                "APP_ANALISE GPS coordinates missing"
            )

        related = answer.get("RelatedFiles")

        if not related:
            raise errors.BinValidationError(
                "APP_ANALISE response missing RelatedFiles"
            )

        required_rf = ["BeginTime", "EndTime", "nSweeps"]

        for rf in related:
            for field in required_rf:
                if rf.get(field) is None:
                    raise errors.BinValidationError(
                        f"APP_ANALISE RelatedFiles missing {field}"
                    )

    # =================================================
    # Export validation
    # =================================================

    def _validate_export(self, export_info: Dict) -> str:

        if not export_info:
            raise errors.BinValidationError(
                "APP_ANALISE export requested but Export block missing"
            )

        path = export_info.get("Path")
        name = export_info.get("FileName")

        if not path or not name:
            raise errors.BinValidationError(
                "APP_ANALISE export metadata incomplete"
            )

        if not name.lower().endswith(".mat"):
            raise errors.BinValidationError(
                "APP_ANALISE exported file must be .mat"
            )

        full_path = os.path.join(path, name)

        if not os.path.exists(full_path):
            raise errors.BinValidationError(
                f"Exported MAT file not found: {full_path}"
            )

        size = os.path.getsize(full_path)

        if size == 0:
            raise errors.BinValidationError(
                f"Exported MAT file is empty: {full_path}"
            )

        last = size

        for _ in range(10):

            time.sleep(0.1)
            size = os.path.getsize(full_path)

            if size == last:
                return full_path

            last = size

        return full_path

    # =================================================
    # Normalize response
    # =================================================

    def _normalize_response(self, payload: Dict) -> Dict:

        answer = payload.get("Answer", {})
        metadata = answer.get("MetaData", {})
        gps = answer.get("GPS", {})
        related = answer.get("RelatedFiles", [])

        lat = gps.get("Latitude")
        lon = gps.get("Longitude")

        gps_obj = SimpleNamespace(
            longitude=lon,
            latitude=lat,
            altitude=None,
            _longitude=[lon],
            _latitude=[lat],
            _altitude=[None]
        )

        freq_start = metadata.get("FreqStart")
        freq_stop = metadata.get("FreqStop")

        start_mega = freq_start / 1e6 if freq_start else None
        stop_mega = freq_stop / 1e6 if freq_stop else None

        spectrums = []

        for rf in related:

            description = rf.get("Description")

            try:

                start_time = datetime.fromisoformat(
                    rf.get("BeginTime")
                )

                end_time = datetime.fromisoformat(
                    rf.get("EndTime")
                )

            except Exception:

                start_time = datetime.utcnow()
                end_time = start_time

            spectrum = SimpleNamespace(

                start_mega=start_mega,
                stop_mega=stop_mega,
                ndata=metadata.get("DataPoints"),
                trace_length=rf.get("nSweeps"),
                level_unit=metadata.get("LevelUnit"),
                processing=(
                    metadata.get("TraceMode", "peak").lower()
                ),
                start_dateidx=start_time,
                stop_dateidx=end_time,
                bw=metadata.get("Resolution"),
                description=description,
                metadata={}
            )

            spectrums.append(spectrum)

        if not spectrums:

            raise errors.BinValidationError(
                "APP_ANALISE returned empty spectrum list"
            )

        return {
            "hostname": answer.get("Receiver"),
            "method": answer.get("Method", self.PROCESSOR_NAME),
            "gps": gps_obj,
            "spectrum": spectrums
        }

    # =================================================
    # Public API
    # =================================================

    def process(
        self,
        file_path: str,
        file_name: str,
        export: bool = False
    ) -> Dict:

        if not isinstance(file_path, str) or not file_path:
            raise errors.BinValidationError(
                "AppAnalise: invalid file_path"
            )

        if not isinstance(file_name, str) or not file_name:
            raise errors.BinValidationError(
                "AppAnalise: invalid file_name"
            )

        full_path = os.path.join(file_path, file_name)

        if not os.path.exists(full_path):
            raise errors.BinValidationError(
                f"Input file not found: {full_path}"
            )

        payload = self._request_process(full_path, export)
        self._validate_payload(payload)

        if export:
            export_info = payload["Answer"].get("Export")
            self._validate_export(export_info)

        return self._normalize_response(payload)