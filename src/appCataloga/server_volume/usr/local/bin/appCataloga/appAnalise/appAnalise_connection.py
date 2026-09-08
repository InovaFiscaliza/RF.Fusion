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


class _OutageTracker:
    """Track one appAnalise outage and throttle repeated warning logs.

    Use this class for preflight failures that can repeat in the worker loop.

    Attributes:
        _interval (int): Minimum seconds between repeated warning events.
        _down (bool): Whether an outage is currently active.
        _current_error (str | None): Most recent failure text.
        _first_failure (float | None): Monotonic timestamp of the outage start.
        _last_warning (float | None): Monotonic timestamp of the last warning.
        _suppressed_since (int): Warnings suppressed since the last event.
        _suppressed_total (int): Warnings suppressed during the outage.
    """

    def __init__(self, log_interval_sec: int) -> None:
        """Initialize an outage tracker.

        Args:
            log_interval_sec (int): Minimum interval between repeated warnings.

        Returns:
            None.
        """
        self._interval = log_interval_sec
        self._down = False
        self._current_error: str | None = None
        self._first_failure: float | None = None
        self._last_warning: float | None = None
        self._suppressed_since: int = 0
        self._suppressed_total: int = 0

    def _get_monotonic(self) -> float:
        """Return a patchable monotonic timestamp.

        Returns:
            float: Current monotonic time in seconds.
        """
        return time.monotonic()

    def _outage_sec(self, now: float) -> int:
        """Calculate the active outage duration.

        Args:
            now (float): Current monotonic time in seconds.

        Returns:
            int: Elapsed outage time in whole seconds.
        """
        return int(max(0.0, now - float(self._first_failure or now)))

    def reset(self) -> None:
        """Clear the active outage state.

        Returns:
            None.
        """
        self._down = False
        self._current_error = None
        self._first_failure = None
        self._last_warning = None
        self._suppressed_since = 0
        self._suppressed_total = 0

    def record_failure(self, error_text: str, logger) -> None:
        """Record a preflight failure and emit a warning when due.

        Args:
            error_text (str): Failure description returned by the transport.
            logger: Logger that provides ``warning_event``.

        Returns:
            None.
        """
        now = self._get_monotonic()

        if not self._down:
            self._down = True
            self._current_error = error_text
            self._first_failure = now
            self._last_warning = now
            self._suppressed_since = 0
            self._suppressed_total = 0
            logger.warning_event("appanalise_unavailable_retry", error=error_text)
            return

        if self._current_error != error_text:
            # Error changed. Log the new failure immediately.
            logger.warning_event(
                "appanalise_unavailable_retry",
                error=error_text,
                previous_error=self._current_error,
                outage_sec=self._outage_sec(now),
                suppressed_retries=self._suppressed_since,
                suppressed_retries_total=self._suppressed_total,
            )
            self._current_error = error_text
            self._last_warning = now
            self._suppressed_since = 0
            return

        if (
            self._last_warning is not None
            and now - float(self._last_warning) < self._interval
        ):
            self._suppressed_since += 1
            self._suppressed_total += 1
            return

        # The interval elapsed. Emit a "still down" summary.
        logger.warning_event(
            "appanalise_unavailable_still_down",
            error=error_text,
            outage_sec=self._outage_sec(now),
            suppressed_retries=self._suppressed_since,
            suppressed_retries_total=self._suppressed_total,
        )
        self._last_warning = now
        self._suppressed_since = 0

    def record_recovery(self, logger) -> None:
        """Emit a recovery event for an active outage, then reset state.

        Args:
            logger: Logger that provides ``event``.

        Returns:
            None.
        """
        if not self._down:
            return

        now = self._get_monotonic()
        logger.event(
            "appanalise_recovered",
            outage_sec=self._outage_sec(now),
            previous_error=self._current_error,
            suppressed_retries=self._suppressed_since,
            suppressed_retries_total=self._suppressed_total,
        )
        self.reset()


class AppAnaliseConnection:
    """Provide socket transport for the external ``appAnalise`` service.

    Use this client to request processing for one local source file. It owns
    TCP framing and delegates response semantics to ``AppAnalisePayloadParser``.
    It never writes to the database.

    Attributes:
        payload_parser (AppAnalisePayloadParser): Semantic response validator.
        bin_data (dict[str, Any]): Latest normalized payload; empty before use.
        last_requested_file (str | None): Latest source file path.
        last_response_text (str | None): Latest decoded socket response.
        last_payload (dict[str, Any] | None): Latest decoded protocol payload.
        last_answer (dict[str, Any] | None): Latest accepted ``Answer`` object.
        last_output_meta (dict[str, Any] | None): Latest resolved artifact metadata.
        _outage_tracker (_OutageTracker): State for preflight warning throttling.
    """

    START_TAG = "<JSON>"
    END_TAG = "</JSON>"
    MAX_RESPONSE_SIZE = 10 * 1024 * 1024
    NETWORK_RETRIES = 2

    def __init__(self) -> None:
        """Initialize a client with empty request diagnostics.

        The snapshots support failure inspection without suppressing errors.

        Returns:
            None.
        """
        self.payload_parser = AppAnalisePayloadParser()
        self.bin_data: Dict[str, Any] = {}
        self.last_requested_file: Optional[str] = None
        self.last_response_text: Optional[str] = None
        self.last_payload: Optional[Dict[str, Any]] = None
        self.last_answer: Optional[Dict[str, Any]] = None
        self.last_output_meta: Optional[Dict[str, Any]] = None
        self._outage_tracker = _OutageTracker(
            log_interval_sec=int(getattr(k, "APP_ANALISE_PREFLIGHT_LOG_INTERVAL_SEC", 300))
        )

    def _reset_last_result(self) -> None:
        """Clear diagnostics from the previous processing request.

        This prevents one FILE_TASK from exposing stale artifacts from another.

        Returns:
            None.
        """
        self.bin_data = {}
        self.last_requested_file = None
        self.last_response_text = None
        self.last_payload = None
        self.last_answer = None
        self.last_output_meta = None

    def _resolve_request_timeout_seconds(
        self,
        timeout_seconds: Optional[int] = None,
    ) -> Optional[int]:
        """Resolve a safe timeout for the remote ``FileRead`` request.

        Args:
            timeout_seconds (int | None): Explicit timeout, or ``None`` to use config.

        Returns:
            int | None: Timeout sent to appAnalise, or ``None`` when disabled.
        """
        raw_value = (
            timeout_seconds
            if timeout_seconds is not None
            else getattr(k, "APP_ANALISE_REQUEST_TIMEOUT_SECONDS", None)
        )

        if raw_value is None:
            return None

        try:
            resolved = int(raw_value)
        except (TypeError, ValueError):
            return None

        if resolved <= 0:
            return 0

        process_timeout = int(getattr(k, "APP_ANALISE_PROCESS_TIMEOUT", 0) or 0)
        if process_timeout > 0 and resolved >= process_timeout:
            return max(1, process_timeout - 1)

        return resolved

    def _build_request_payload(
        self,
        full_path: str,
        export: bool,
        timeout_seconds: Optional[int] = None,
    ) -> Dict:
        """Build the payload required by the appAnalise socket API.

        Args:
            full_path (str): Absolute source-file path.
            export (bool): Whether appAnalise should export an output artifact.
            timeout_seconds (int | None): Optional remote processing timeout.

        Returns:
            dict: Request with ``Key``, ``ClientName``, and ``Request`` keys.
        """
        request = {
            "Key": k.APP_ANALISE_KEY,
            "ClientName": k.APP_ANALISE_CLIENT_NAME,
            "Request": {
                "type": "FileRead",
                "filepath": full_path,
                "export": export,
            },
        }

        resolved_timeout = self._resolve_request_timeout_seconds(timeout_seconds)
        if resolved_timeout is not None:
            request["Request"]["timeoutSeconds"] = resolved_timeout

        return request

    @staticmethod
    def _close_socket(sock: socket.socket) -> None:
        """Close a socket without replacing the primary transport failure.

        Args:
            sock (socket.socket): Connected or partially initialized socket.

        Returns:
            None.
        """
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass

        try:
            sock.close()
        except Exception:
            pass

    def check_connection_with_log(self, logger) -> bool:
        """Check service reachability and record a throttled outage event.

        Args:
            logger: Logger used to emit outage and recovery events.

        Returns:
            bool: ``True`` when reachable; ``False`` for a transient outage.
        """
        try:
            self.check_connection()
            self._outage_tracker.record_recovery(logger)
            return True
        except errors.ExternalServiceTransientError as e:
            self._outage_tracker.record_failure(str(e), logger)
            return False

    def check_connection(self) -> bool:
        """Perform a lightweight TCP reachability check.

        Returns:
            bool: Always ``True`` when the TCP connection succeeds.

        Raises:
            errors.ExternalServiceTransientError: If the service cannot be reached.
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

    def _request_process(
        self,
        full_path: str,
        export: bool = False,
        timeout_seconds: Optional[int] = None,
    ) -> Dict:
        """Submit a request and return its decoded protocol payload.

        Args:
            full_path (str): Absolute source-file path sent to appAnalise.
            export (bool): Whether appAnalise should export an artifact.
            timeout_seconds (int | None): Optional remote processing timeout.

        Returns:
            dict: JSON object extracted from the tagged socket response.

        Raises:
            errors.ExternalServiceTransientError: If all transport attempts fail.
        """
        request_payload = self._build_request_payload(
            full_path,
            export,
            timeout_seconds=timeout_seconds,
        )
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
        """Read a tagged JSON block from a socket stream.

        Args:
            sock (socket.socket): Connected socket that returns response bytes.

        Returns:
            bytes: Complete response block from ``<JSON>`` through ``</JSON>``.

        Raises:
            errors.ExternalServiceTransientError: If the response times out,
                exceeds the size limit, or has incomplete framing.
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
        """Decode response bytes, preserving legacy diagnostic text.

        Args:
            data (bytes): Raw response bytes.

        Returns:
            str: UTF-8 text, or Latin-1 text when UTF-8 fails.
        """
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data.decode("latin-1")

    def _extract_json(self, payload: str) -> Dict:
        """Extract and decode JSON from a tagged protocol response.

        Args:
            payload (str): Decoded response containing protocol tags.

        Returns:
            dict: Decoded JSON object inside the tags.

        Raises:
            errors.BinValidationError: If tags are missing or JSON is invalid.
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
        timeout_seconds: Optional[int] = None,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """Process one source file through transport and payload validation.

        Args:
            file_path (str): Directory containing the source file.
            file_name (str): Source-file name within ``file_path``.
            export (bool): Whether appAnalise should export an output artifact.
            timeout_seconds (int | None): Optional remote processing timeout.

        Returns:
            tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
                Normalized ``bin_data``, output metadata, accepted ``Answer``,
                and the decoded protocol payload, in that order.

        Raises:
            errors.BinValidationError: If the file, protocol, or payload is invalid.
            errors.ExternalServiceTransientError: If socket transport fails.
        """
        full_path = os.path.join(file_path, file_name)
        self._reset_last_result()
        self.last_requested_file = full_path

        # Phase 1: if the source file already disappeared locally, retrying the
        # external processor cannot make this task recoverable.
        self.payload_parser.validate_source_file(full_path)

        # Transport only returns decoded payloads. The parser owns semantics.
        self.last_payload = self._request_process(
            full_path,
            export,
            timeout_seconds=timeout_seconds,
        )

        # Phase 3: from here on the payload exists, so failures are no longer
        # The parser now validates the returned appAnalise payload.
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

        return self.bin_data, self.last_output_meta, self.last_answer, self.last_payload
