"""
Socket-level helpers for the appCataloga TCP control service.

Reading guide:
    This module owns the request lifecycle at the transport layer:
        - parse one TCP payload
        - normalize transport-facing failures
        - frame and send one response

The business meaning of a request stays outside this module. `socket_handler`
owns only transport/protocol flow.
"""

from __future__ import annotations

import json
import socket
from typing import Any, TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from shared.errors import ErrorHandler
    from shared.logging_utils import log as logger_type


SocketPayload: TypeAlias = dict[str, Any]
HostRequest: TypeAlias = dict[str, Any]


def open_listening_socket(*, port: int, backlog: int) -> socket.socket:
    """
    Create the TCP listening socket used by the appCataloga daemon.

    This keeps the low-level socket bring-up in one place so the entrypoint
    can read more like service orchestration than socket boilerplate.

    This helper owns only local socket setup. Selector registration and daemon
    lifecycle stay in the entrypoint.
    """
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("", port))
    server_socket.listen(backlog)
    return server_socket


def frame_payload(payload: SocketPayload, *, start_tag: str, end_tag: str) -> str:
    """
    Wrap a JSON payload inside the wire tags expected by clients.
    """
    return f"{start_tag}{json.dumps(payload)}{end_tag}"


def parse_socket_message(
    *,
    peername: tuple[str, int],
    data: str,
    none_filter: dict,
    logger: logger_type,
) -> HostRequest:
    """
    Parse one short TCP request received by the appCataloga control socket.

    This helper intentionally normalizes malformed payloads into a predictable
    dictionary shape instead of raising directly. The caller can then route all
    request failures through the same `ErrorHandler` path and produce one
    consistent error response.

    The output format is the transport contract consumed by the service-layer
    request handler in `appCataloga.py`.
    """
    peer_ip, peer_port = peername

    try:
        payload = json.loads(data)

        filter_raw = payload.get("filter")
        if isinstance(filter_raw, dict):
            filter_dict = filter_raw
        elif isinstance(filter_raw, str):
            try:
                filter_dict = json.loads(filter_raw)
            except Exception:
                filter_dict = none_filter
        else:
            filter_dict = none_filter

        return {
            "peer": {"ip": peer_ip, "port": peer_port},
            "command": payload.get("query_tag"),
            "host_id": int(payload.get("host_id")),
            "host_uid": payload.get("host_uid"),
            "host_addr": payload.get("host_add"),
            "host_port": int(payload.get("host_port")),
            "user": payload.get("user"),
            "password": payload.get("passwd"),
            "filter": filter_dict,
        }

    except Exception as exc:
        # Parsing failures are normalized instead of raised so the caller can
        # keep all request errors on one response path.
        logger.warning_event(
            "socket_message_parse_failed",
            component="socket_handler",
            operation="parse_message",
            peer_ip=peer_ip,
            peer_port=peer_port,
            payload_size=len(data),
            reason="invalid_json_payload",
            error=exc,
        )
        return {
            "peer": {"ip": peer_ip, "port": peer_port},
            "command": None,
            "host_id": None,
            "host_uid": None,
            "host_addr": None,
            "host_port": None,
            "user": None,
            "password": None,
            "filter": none_filter,
        }


def close_client_socket(client_socket: socket.socket) -> None:
    """
    Close a client socket without surfacing cleanup failures.

    Socket finalization is intentionally fail-closed. Once the request is
    over, cleanup noise should not compete with the real request outcome.
    """
    try:
        client_socket.close()
    except Exception:
        pass


def get_client_peer_ip(client_socket: socket.socket) -> str:
    """
    Best-effort peer IP resolution for logging and error reporting.
    """
    try:
        peer_ip, _ = client_socket.getpeername()
        return str(peer_ip)
    except Exception:
        return "unknown"


def send_response(
    *,
    client_socket: socket.socket,
    payload: SocketPayload,
    peer_ip: str,
    logger: logger_type,
    start_tag: str,
    end_tag: str,
) -> None:
    """
    Send a framed response to one client and log the result.

    All outbound responses pass through this helper so framing and logging
    stay consistent for both success and failure paths.
    """
    try:
        framed = frame_payload(payload, start_tag=start_tag, end_tag=end_tag)
        client_socket.sendall(framed.encode("utf-8"))
        logger.event(
            "response_sent",
            component="socket_handler",
            operation="send_response",
            peer_ip=peer_ip,
        )
    except Exception as exc:
        logger.warning_event(
            "response_send_failed",
            component="socket_handler",
            operation="send_response",
            peer_ip=peer_ip,
            error=exc,
        )


def read_host_request(
    *,
    client_socket: socket.socket,
    logger: logger_type,
    err: ErrorHandler,
    none_filter: dict,
) -> HostRequest:
    """
    Read one bounded control payload from the accepted client socket.

    Flow:
        1. receive raw bytes
        2. reject empty sockets explicitly
        3. parse/normalize the JSON payload
    """
    raw_message = client_socket.recv(2048)
    if not raw_message:
        err.capture("Empty request", stage="READ")
        raise ValueError("Empty request")

    return parse_socket_message(
        data=raw_message.decode(),
        peername=client_socket.getpeername(),
        none_filter=none_filter,
        logger=logger,
    )


def finalize_client_request(
    *,
    client_socket: socket.socket,
    peer_ip: str,
    response_payload: SocketPayload,
    err: ErrorHandler,
    host_id: int | None,
    logger: logger_type,
    start_tag: str,
    end_tag: str,
) -> None:
    """
    Convert the current request outcome into one framed response and close.

    This is the single exit point for client sockets. Successful and failed
    requests both end here, which keeps the write/close behavior stable and
    easy to audit.
    """
    if err.triggered:
        # ErrorHandler remains the single source of truth for the response
        # message shape when request processing fails at any stage.
        err.log_error(host_id=host_id, peer_ip=peer_ip)
        response_payload = {
            "status": 0,
            "message": err.format_error() or err.msg,
        }

    # The final response and socket close intentionally stay adjacent: once
    # the outcome is known, the request is finished and this transport layer
    # should not leave the connection hanging for any extra branch.
    send_response(
        client_socket=client_socket,
        payload=response_payload,
        peer_ip=peer_ip,
        logger=logger,
        start_tag=start_tag,
        end_tag=end_tag,
    )
    close_client_socket(client_socket)
