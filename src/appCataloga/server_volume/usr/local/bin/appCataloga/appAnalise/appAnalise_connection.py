"""
Socket client for the external MATLAB-based `appAnalise` service.

This module now focuses on transport and protocol framing only:
    - build the request payload
    - talk to the remote socket server
    - decode the tagged JSON response

Semantic validation, payload normalization, and output-artifact resolution live
in `payload_parser.py`, which keeps this client readable and easier to debug.

Reading guide:
    1. connection bootstrap and lightweight reachability check
    2. request/response transport over the tagged socket protocol
    3. explicit handoff from transport to the payload parser
    4. the public `process()` orchestration method
"""

import json
import os
import socket
import sys
import time

from typing import Any, Dict, Optional

from shared import errors
from appAnalise.payload_parser import AppAnalisePayloadParser


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


class AppAnaliseConnection:
    """
    Transport adapter for the external `appAnalise` service.

    Responsibilities:
        1. open and close the TCP socket safely
        2. send one processing request
        3. extract the tagged JSON response
        4. delegate semantic validation to `AppAnalisePayloadParser`

    This class intentionally does not perform database writes.
    """

    START_TAG = "<JSON>"
    END_TAG = "</JSON>"
    MAX_RESPONSE_SIZE = 10 * 1024 * 1024
    NETWORK_RETRIES = 2

    def __init__(self) -> None:
        """
        Keep the latest protocol artifacts available for post-failure inspection.

        The worker should still raise on failures, but these snapshots make it
        possible to inspect what appAnalise returned before the request was
        classified as transient or definitive.
        """
        self.payload_parser = AppAnalisePayloadParser()
        self.bin_data: Dict[str, Any] = {}
        self.last_requested_file: Optional[str] = None
        self.last_response_text: Optional[str] = None
        self.last_payload: Optional[Dict[str, Any]] = None
        self.last_answer: Optional[Dict[str, Any]] = None
        self.last_output_meta: Optional[Dict[str, Any]] = None

    def _reset_last_result(self) -> None:
        """
        Clear per-request debug state before a new processing attempt.

        This avoids stale protocol artifacts leaking from one FILE_TASK into
        the next when operators inspect the client object after a failure.
        """
        self.bin_data = {}
        self.last_requested_file = None
        self.last_response_text = None
        self.last_payload = None
        self.last_answer = None
        self.last_output_meta = None

    def _build_request_payload(self, full_path: str, export: bool) -> Dict:
        """
        Build the request payload expected by the appAnalise socket API.

        This helper is intentionally tiny: the socket contract should stay easy
        to audit here instead of being rebuilt ad hoc inside `_request_process`.
        """
        return {
            "Key": k.APP_ANALISE_KEY,
            "ClientName": k.APP_ANALISE_CLIENT_NAME,
            "Request": {
                "type": "FileRead",
                "filepath": full_path,
                "export": export,
            },
        }

    @staticmethod
    def _close_socket(sock: socket.socket) -> None:
        """
        Close a socket defensively without leaking cleanup exceptions.

        Transport cleanup must never overwrite the real request failure with a
        secondary `shutdown()` or `close()` exception.
        """
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass

        try:
            sock.close()
        except Exception:
            pass

    def check_connection(self) -> bool:
        """
        Perform a lightweight TCP reachability check against appAnalise.

        This is only a preflight. It proves the service is reachable right now,
        not that a later processing request will succeed end-to-end.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            sock.settimeout(k.APP_ANALISE_CONNECT_TIMEOUT)
            sock.connect((k.APP_ANALISE_HOST_ADD, k.APP_ANALISE_HOST_PORT))
            return True
        except Exception as e:
            raise errors.ExternalServiceTransientError(
                f"APP_ANALISE preflight connection failed: {e}"
            )
        finally:
            self._close_socket(sock)

    def _request_process(self, full_path: str, export: bool = False) -> Dict:
        """
        Submit a processing request and return the decoded protocol payload.

        Transport retries live here because they are about socket stability,
        not payload semantics. Once a complete JSON payload is received, the
        parser layer decides whether it is valid or defective.
        """
        request_payload = self._build_request_payload(full_path, export)
        request_bytes = (
            json.dumps(request_payload, ensure_ascii=False) + "\r\n"
        ).encode("utf-8")

        last_error = None

        for _ in range(self.NETWORK_RETRIES):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                # Connection timeout protects the handshake phase; processing
                # timeout then covers the longer server-side MATLAB execution.
                sock.settimeout(k.APP_ANALISE_CONNECT_TIMEOUT)
                sock.connect((k.APP_ANALISE_HOST_ADD, k.APP_ANALISE_HOST_PORT))
                sock.settimeout(k.APP_ANALISE_PROCESS_TIMEOUT)
                sock.sendall(request_bytes)
                raw_response = self._receive_all(sock)
                response_text = self._safe_decode(raw_response)
                self.last_response_text = response_text
                return self._extract_json(response_text)
            except Exception as e:
                # Retries are intentionally transport-level only. Semantic
                # payload defects are handled after JSON extraction, not here.
                last_error = e
                time.sleep(0.5)
            finally:
                self._close_socket(sock)

        raise errors.ExternalServiceTransientError(
            f"APP_ANALISE socket failure after retries: {last_error}"
        )

    def _receive_all(self, sock: socket.socket) -> bytes:
        """
        Read from the socket until the tagged JSON block is complete.

        The appAnalise protocol is a tagged stream, not a length-prefixed one,
        so the client must keep reading until both `<JSON>` and `</JSON>` are
        present in the accumulated buffer.
        """
        buffer = b""

        while True:
            try:
                chunk = sock.recv(k.APP_ANALISE_BUFFER_SIZE)
            except socket.timeout:
                raise errors.ExternalServiceTransientError(
                    "APP_ANALISE processing timeout"
                )

            if not chunk:
                break

            buffer += chunk

            if len(buffer) > self.MAX_RESPONSE_SIZE:
                # A runaway response is treated as a transport problem. Letting
                # it continue would risk memory blow-ups before semantic parsing.
                raise errors.ExternalServiceTransientError(
                    "APP_ANALISE response exceeded maximum size"
                )

            start = buffer.find(self.START_TAG.encode())
            end = buffer.find(self.END_TAG.encode(), start)

            if start != -1 and end != -1:
                # Return only the tagged JSON block even if the service emits
                # framing noise around it.
                end += len(self.END_TAG)
                return buffer[start:end]

        raise errors.ExternalServiceTransientError(
            "APP_ANALISE response did not contain a complete JSON block"
        )

    @staticmethod
    def _safe_decode(data: bytes) -> str:
        """
        Decode response bytes using UTF-8 with Latin-1 fallback.

        UTF-8 is the preferred contract, but the fallback keeps diagnostics
        readable when the service returns mixed or legacy encodings.
        """
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data.decode("latin-1")

    def _extract_json(self, payload: str) -> Dict:
        """
        Extract the JSON payload wrapped by the protocol tags.

        By the time this method runs, transport has already succeeded. Failures
        here are protocol/payload defects, so they are classified as
        `BinValidationError`, not transport retry conditions.
        """
        start = payload.find(self.START_TAG)
        end = payload.find(self.END_TAG, start)

        if start == -1 or end == -1:
            raise errors.BinValidationError(
                "APP_ANALISE response missing JSON block"
            )

        raw_json = payload[start + len(self.START_TAG):end].strip()

        try:
            return json.loads(raw_json)
        except json.JSONDecodeError as e:
            raise errors.BinValidationError(
                f"Invalid JSON returned by APP_ANALISE: {e}"
            )

    def process(
        self,
        file_path: str,
        file_name: str,
        export: bool = False,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Process one source file through the full appAnalise client pipeline.

        This method is the handoff point between two layers:
            1. transport/protocol owned by `AppAnaliseConnection`
            2. semantic validation owned by `AppAnalisePayloadParser`

        Flow:
            1. fail fast if the requested source file already vanished locally
            2. execute the socket request and decode one JSON payload
            3. validate that the payload is a real appAnalise success contract
            4. resolve which output artifact belongs to this request
            5. normalize the accepted payload into canonical RF.Fusion `bin_data`

        Returns:
            tuple[dict, dict]:
                - Canonical RF.Fusion `bin_data`
                - Filesystem metadata for the output artifact associated with
                  this processing request
        """
        full_path = os.path.join(file_path, file_name)
        self._reset_last_result()
        self.last_requested_file = full_path

        # Phase 1: if the source file already disappeared locally, retrying the
        # external processor cannot make this task recoverable.
        self.payload_parser.validate_source_file(full_path)

        # Phase 2: transport first, semantics second. The connection layer only
        # knows how to obtain one decoded payload from the socket server.
        self.last_payload = self._request_process(full_path, export)

        # Phase 3: from here on the payload exists, so failures are no longer
        # about TCP reachability. The parser now decides whether appAnalise
        # returned a usable success payload or a defective one.
        self.payload_parser.detect_protocol_error(
            self.last_payload,
            requested_full_path=full_path,
        )
        self.last_answer = self.last_payload["Answer"]

        # Phase 4: resolve and stabilize the filesystem artifact owned by this
        # request. Export mode points to `Answer.General`; otherwise the source
        # BIN itself remains the authoritative file.
        self.last_output_meta = self.payload_parser.resolve_output_file(
            answer=self.last_answer,
            file_path=file_path,
            file_name=file_name,
            export=export,
        )

        # Phase 5: only after the payload contract and output artifact are both
        # accepted do we expose canonical `bin_data` to the worker.
        self.bin_data = self.payload_parser.normalize_payload(self.last_payload)

        return self.bin_data, self.last_output_meta
