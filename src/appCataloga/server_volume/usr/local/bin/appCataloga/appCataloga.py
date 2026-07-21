#!/usr/bin/env python3
"""
TCP gateway for the appCataloga control service.

This daemon accepts short host requests from Zabbix, ensures the target HOST
exists in `BPDATA`, and queues the matching `HOST_TASK` for downstream workers.

This entrypoint is a gateway, not a queue worker:
- the loop waits on `accept()` with a short timeout
- each accepted request is handled synchronously
- socket transport stays in `server_handler/socket_handler.py`
"""

import socket
import sys
from datetime import datetime
from typing import Any

from utils.bootstrap_paths import bootstrap_app_paths
PROJECT_ROOT = bootstrap_app_paths(__file__)


# =================================================
# Internal modules
# =================================================
import config as k
from db.dbHandlerBKP import dbHandlerBKP
from server_handler import process_control, signal_runtime, socket_handler
from shared import errors, logging_utils


# ======================================================================
# Service constants
# ======================================================================
SERVICE_NAME = "appCataloga"
SCRIPT_NAME = "appCataloga.py"
SHUTDOWN_PAYLOAD = {"status": 0, "message": "Server shutting down"}
COMMAND_TASK_MAP = {
    k.BACKUP_QUERY_TAG: k.HOST_TASK_CHECK_TYPE,
    k.STOP_QUERY_TAG: k.HOST_TASK_BACKLOG_ROLLBACK_TYPE,
}


# ======================================================================
# Global state
# ======================================================================
process_status = {"running": True}
log = logging_utils.log()


# ======================================================================
signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
    log_fields={"action": "shutdown"},
)


def _build_success_response(task_result: dict, host_filter: dict[str, Any]) -> dict:
    """Build the payload returned after one HOST_TASK is queued."""
    response = dict(task_result)
    response.update(
        {
            "status": 1,
            "message": f"Host task created successfully at {datetime.now()}",
            "filter": f"{host_filter}",
        }
    )
    return response


def _stop_service_siblings() -> None:
    """Ask sibling `appCataloga.py` processes to terminate."""
    process_control.stop_self_service(
        script_name=SCRIPT_NAME,
        logger=log,
    )


def _init_db() -> dbHandlerBKP:
    """Create the BPDATA handler owned by this gateway process."""
    return dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)


def _init_server_socket() -> socket.socket:
    """Open the listening socket owned by this daemon instance."""
    server_socket = socket_handler.open_listening_socket(
        port=k.SERVER_PORT,
        backlog=k.TOTAL_CONNECTIONS,
    )
    server_socket.settimeout(k.GATEWAY_SELECT_TIMEOUT_SEC)
    log.event("server_listening", port=k.SERVER_PORT)
    return server_socket


def _resolve_task_type(command: str, err: errors.ErrorHandler) -> int:
    """Translate one request command into the queued HOST_TASK type."""
    task_type = COMMAND_TASK_MAP.get(command)
    if task_type is None:
        err.capture("Unsupported command", stage="COMMAND")
        raise ValueError("Unsupported command")
    return task_type


def _validate_host_request(
    host: dict[str, Any],
    err: errors.ErrorHandler,
) -> tuple[int, dict[str, Any]]:
    """Validate the minimal request fields needed by the gateway."""
    host_id = host.get("host_id")
    if host_id is None or host_id <= 0:
        err.capture("Invalid host_id", stage="PARSE")
        raise ValueError("Invalid host_id")

    return int(host_id), host["filter"]


def _read_host_status(
    db: dbHandlerBKP,
    host_id: int,
    err: errors.ErrorHandler,
) -> dict[str, Any]:
    """Read the current HOST status used by request guards."""
    try:
        return db.host_read_status(host_id=host_id) or {"status": 0}
    except Exception as exc:
        err.capture(
            "Failed to read HOST status",
            stage="HOST_READ",
            exc=exc,
            host_id=host_id,
        )
        raise


def _guard_offline_backup_request(
    *,
    command: str,
    host_id: int,
    host_status: dict[str, Any],
    err: errors.ErrorHandler,
) -> None:
    """Reject backup requests for hosts already marked offline."""
    if (
        command == k.BACKUP_QUERY_TAG
        and int(host_status.get("status", 0)) == 1
        and bool(host_status.get("IS_OFFLINE"))
    ):
        err.capture(
            "Backup request skipped because HOST is offline",
            stage="HOST_STATUS",
            host_id=host_id,
        )
        raise ValueError("HOST is offline")


def _ensure_host(
    db: dbHandlerBKP,
    host: dict[str, Any],
    *,
    host_id: int,
    host_status: dict[str, Any],
    err: errors.ErrorHandler,
) -> None:
    """Ensure the HOST row exists with the latest connection fields."""
    try:
        log.event("host_upsert", host_id=host_id, host_uid=host["host_uid"])
        host_data = {
            "ID_HOST": host_id,
            "NA_HOST_NAME": host["host_uid"],
            "NA_HOST_ADDRESS": host["host_addr"],
            "NA_HOST_PORT": host["host_port"],
            "NA_HOST_USER": host["user"],
            "NA_HOST_PASSWORD": host["password"],
        }
        if int(host_status.get("status", 0)) != 1:
            # New hosts bootstrap online and prove their real state later.
            host_data["IS_OFFLINE"] = False

        db.host_upsert(**host_data)
    except Exception as exc:
        err.capture(
            "Failed to create/ensure HOST",
            stage="HOST_CREATE",
            exc=exc,
            host_id=host_id,
        )
        raise


def _queue_host_task(
    db: dbHandlerBKP,
    *,
    host_id: int,
    task_type: int,
    host_filter: dict[str, Any],
    err: errors.ErrorHandler,
) -> dict[str, Any]:
    """Queue the HOST_TASK requested by the gateway client."""
    try:
        return db.queue_host_task(
            host_id=host_id,
            task_type=task_type,
            task_status=k.TASK_PENDING,
            filter_dict=host_filter,
        )
    except Exception as exc:
        # The HOST row already exists here, so mark it for attention.
        db.host_update(host_id=host_id, NU_HOST_CHECK_ERROR=1)
        err.capture(
            "Failed to queue HOST_TASK",
            stage="QUEUE",
            exc=exc,
            host_id=host_id,
        )
        raise


def _process_host_request(
    host: dict[str, Any],
    err: errors.ErrorHandler,
    db: dbHandlerBKP,
) -> tuple[int | None, dict]:
    """Process one parsed gateway request from start to finish."""
    command = str(host.get("command") or "").strip().lower()
    task_type = _resolve_task_type(command, err)
    host_id, host_filter = _validate_host_request(host, err)
    host_status = _read_host_status(db, host_id, err)
    _ensure_host(
        db,
        host,
        host_id=host_id,
        host_status=host_status,
        err=err,
    )
    # Keep access data current even for offline hosts. A later backup request
    # may be the first one that carries the fixed IP, port, or credentials.
    _guard_offline_backup_request(
        command=command,
        host_id=host_id,
        host_status=host_status,
        err=err,
    )
    task_result = _queue_host_task(
        db,
        host_id=host_id,
        task_type=task_type,
        host_filter=host_filter,
        err=err,
    )
    return host_id, _build_success_response(task_result, host_filter)


def _send_shutdown_response(*, client_socket: socket.socket, client_address) -> None:
    """Reply explicitly when shutdown starts after `accept()`."""
    socket_handler.send_response(
        client_socket=client_socket,
        payload=SHUTDOWN_PAYLOAD,
        peer_ip=str(client_address),
        logger=log,
        start_tag=k.START_TAG,
        end_tag=k.END_TAG,
    )
    socket_handler.close_client_socket(client_socket)


def _serve_client_connection(
    *,
    client_socket: socket.socket,
    db_bp: dbHandlerBKP,
) -> None:
    """Handle one accepted client connection from start to finish."""
    peer_ip = socket_handler.get_client_peer_ip(client_socket)
    err = errors.ErrorHandler(log)
    response_payload: dict[str, Any] = {"status": 0, "message": "Unexpected error"}
    host_id = None

    try:
        # Transport stops at a normalized host payload. From here on we are in
        # gateway business flow: validate, ensure HOST, queue HOST_TASK.
        host = socket_handler.read_host_request(
            client_socket=client_socket,
            logger=log,
            err=err,
            none_filter=k.NONE_FILTER,
        )
        host_id, response_payload = _process_host_request(host, err, db_bp)
    except Exception:
        # Request failures are already normalized through ErrorHandler.
        pass
    finally:
        socket_handler.finalize_client_request(
            client_socket=client_socket,
            peer_ip=peer_ip,
            response_payload=response_payload,
            err=err,
            host_id=host_id,
            logger=log,
            start_tag=k.START_TAG,
            end_tag=k.END_TAG,
        )


# ======================================================================
# Entrypoint
# ======================================================================
def main() -> None:
    """Create the listening socket and run the gateway loop."""
    log.service_start(SERVICE_NAME)
    server_socket = None
    db_bp = None

    try:
        server_socket = _init_server_socket()
        # DB initialization happens only after the socket is listening, so
        # startup failures stay easy to classify.
        db_bp = _init_db()

        while process_status["running"]:
            try:
                # The gateway accepts one client at a time. The short timeout
                # keeps shutdown bounded without extra wakeup plumbing.
                client_socket, client_address = server_socket.accept()
            except socket.timeout:
                continue

            client_socket.setblocking(True)

            if not process_status["running"]:
                # Shutdown may begin between `accept()` and request handling.
                # Reply explicitly instead of dropping the socket.
                _send_shutdown_response(
                    client_socket=client_socket,
                    client_address=client_address,
                )
                continue

            log.event(
                "client_connected",
                component="socket_handler",
                operation="accept_client",
                client_address=client_address,
            )
            _serve_client_connection(
                client_socket=client_socket,
                db_bp=db_bp,
            )

    except Exception as exc:
        err = errors.ErrorHandler(log)
        err.capture(
            reason="Fatal appCataloga service error",
            stage=k.STAGE_MAIN,
            exc=exc,
        )
        err.log_error()
        _stop_service_siblings()
        sys.exit(1)

    finally:
        # Teardown is defensive because the process is already leaving.
        try:
            if server_socket is not None:
                server_socket.close()
        except Exception:
            pass
        _stop_service_siblings()
        log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    main()
