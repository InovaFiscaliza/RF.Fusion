#!/usr/bin/env python3
"""
TCP entrypoint for the appCataloga service.

This service accepts host requests, ensures the HOST exists in the backup
database and queues the corresponding HOST_TASK for downstream processing.

The implementation intentionally stays synchronous and explicit:
- one request is fully handled before the next
- signal handling wakes the selector for fast shutdown
- request validation is centralized through ErrorHandler
"""

import json
import os
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime
from selectors import DefaultSelector, EVENT_READ


# =================================================
# PROJECT ROOT (shared/, db/, stations/)
# =================================================
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


# =================================================
# Config directory (etc/appCataloga)
# =================================================
_CFG_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga")
)
if _CFG_DIR not in sys.path and os.path.isdir(_CFG_DIR):
    sys.path.append(_CFG_DIR)


# =================================================
# DB directory
# =================================================
_DB_DIR = os.path.join(PROJECT_ROOT, "db")
if _DB_DIR not in sys.path and os.path.isdir(_DB_DIR):
    sys.path.append(_DB_DIR)


# =================================================
# Internal modules
# =================================================
import config as k
from db.dbHandlerBKP import dbHandlerBKP
from shared import errors, legacy, logging_utils


# ======================================================================
# Global state
# ======================================================================
process_status = {"running": True}
# The selector waits on both the listening socket and this pipe so that
# signal handlers can wake the loop immediately instead of waiting for
# network activity.
WAKE_R_FD, WAKE_W_FD = os.pipe()
log = logging_utils.log()
db_bp = None


# ======================================================================
# Signal handling
# ======================================================================
def wake_selector() -> None:
    """
    Wake the selector loop by writing a byte to the control pipe.
    """
    try:
        os.write(WAKE_W_FD, b"\0")
    except Exception:
        pass


def _signal_handler(signal_name: str) -> None:
    """
    Register shutdown intent and wake the selector loop.
    """
    # Keep signal handling minimal: record intent, log once and let the
    # main loop unwind in a controlled way.
    process_status["running"] = False
    log.signal_received(signal_name, action="shutdown")
    wake_selector()


def sigterm_handler(signum: int = None, frame=None) -> None:
    """
    Handle SIGTERM by requesting a graceful shutdown.
    """
    _signal_handler("SIGTERM")


def sigint_handler(signum: int = None, frame=None) -> None:
    """
    Handle SIGINT by requesting a graceful shutdown.
    """
    _signal_handler("SIGINT")


signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


# ======================================================================
# Response helpers
# ======================================================================
def frame_payload(payload: dict) -> str:
    """
    Wrap a JSON payload inside the protocol tags expected by clients.
    """
    return f"{k.START_TAG}{json.dumps(payload)}{k.END_TAG}"


def build_success_response(task_result: dict, host_filter) -> dict:
    """
    Build the success response sent after queueing a HOST_TASK.
    """
    response = dict(task_result)
    response.update(
        {
            "status": 1,
            "message": f"Host task created successfully at {datetime.now()}",
            "filter": f"{host_filter}",
        }
    )
    return response


def close_client_socket(client_socket: socket.socket) -> None:
    """
    Close a client socket without surfacing cleanup failures.
    """
    try:
        client_socket.close()
    except Exception:
        pass


def send_response(client_socket: socket.socket, payload: dict, peer_ip: str) -> None:
    """
    Send a framed response to a client and log the result.
    """
    try:
        # Framing is part of the wire contract with existing clients, so
        # every outbound payload must pass through the same helper.
        framed = frame_payload(payload)
        client_socket.sendall(framed.encode("utf-8"))
        log.event("response_sent", peer_ip=peer_ip)
    except Exception as exc:
        log.warning_event("response_send_failed", peer_ip=peer_ip, error=exc)


# ======================================================================
# Service stop: kill by PID discovery (no systemd dependency)
# ======================================================================
def stop_self_service(script_name: str = "appCataloga.py") -> None:
    """
    Stop daemon processes whose command line matches `script_name`.

    This is a best-effort safeguard used when the service needs to
    terminate sibling instances outside a process manager.
    """
    try:
        result = subprocess.run(
            ["pgrep", "-f", script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        pids = [
            int(pid)
            for pid in result.stdout.split()
            if pid.strip().isdigit()
        ]
        current_pid = os.getpid()

        if not pids:
            log.event("service_stop_scan_empty", script_name=script_name)
            return

        # We never signal the current process here. This routine is only for
        # sibling instances discovered through `pgrep -f`.
        for pid in pids:
            if pid == current_pid:
                continue

            try:
                os.kill(pid, signal.SIGTERM)
                log.event(
                    "service_stop_signal_sent",
                    signal="SIGTERM",
                    pid=pid,
                )
            except ProcessLookupError:
                continue
            except Exception as exc:
                log.warning_event(
                    "service_stop_signal_failed",
                    signal="SIGTERM",
                    pid=pid,
                    error=exc,
                )

        time.sleep(2.0)

        # Escalation to SIGKILL is intentionally delayed to give sibling
        # processes a small grace period for clean shutdown.
        for pid in pids:
            if pid == current_pid:
                continue

            try:
                os.kill(pid, 0)
            except OSError:
                continue

            os.kill(pid, signal.SIGKILL)
            log.warning_event(
                "service_stop_signal_sent",
                signal="SIGKILL",
                pid=pid,
            )

    except Exception as exc:
        log.error_event(
            "service_stop_failed",
            script_name=script_name,
            error=exc,
        )


# ======================================================================
# Client handling
# ======================================================================
def serve_client(client_socket: socket.socket) -> None:
    """
    Handle a single TCP client request.

    The request lifecycle is:
    1. Read raw bytes
    2. Parse and validate the socket message
    3. Ensure the HOST exists
    4. Queue the corresponding HOST_TASK
    5. Send a framed success or failure response
    """
    peer_ip = "unknown"
    try:
        peer_ip, _ = client_socket.getpeername()
    except Exception:
        pass

    err = errors.ErrorHandler(log)
    response_payload = {"status": 0, "message": "Unexpected error"}
    host_id = None
    host_filter = None

    try:
        # The service accepts one short request per connection, so the
        # protocol stays intentionally simple and bounded.
        raw_message = client_socket.recv(2048)
        if not raw_message:
            err.capture("Empty request", stage="READ")
            raise ValueError("Empty request")

        host = legacy.parse_socket_message(
            data=raw_message.decode(),
            peername=client_socket.getpeername(),
            log=log,
        )

        if host.get("command") != k.BACKUP_QUERY_TAG:
            err.capture("Unsupported command", stage="COMMAND")
            raise ValueError("Unsupported command")

        host_id = host.get("host_id")
        if host_id is None or host_id <= 0:
            err.capture("Invalid host_id", stage="PARSE")
            raise ValueError("Invalid host_id")

        host_uid = host["host_uid"]
        host_addr = host["host_addr"]
        host_port = host["host_port"]
        user = host["user"]
        password = host["password"]
        host_filter = host["filter"]

        try:
            log.event("host_upsert", host_id=host_id, host_uid=host_uid)
            db_bp.host_upsert(
                ID_HOST=host_id,
                NA_HOST_NAME=host_uid,
                NA_HOST_ADDRESS=host_addr,
                NA_HOST_PORT=host_port,
                NA_HOST_USER=user,
                NA_HOST_PASSWORD=password,
            )
        except Exception as exc:
            err.capture(
                "Failed to create/ensure HOST",
                stage="HOST_CREATE",
                exc=exc,
                host_id=host_id,
            )
            raise

        try:
            task_result = db_bp.queue_host_task(
                host_id=host_id,
                task_type=k.HOST_TASK_CHECK_TYPE,
                task_status=k.TASK_PENDING,
                filter_dict=host_filter,
            )
            response_payload = build_success_response(task_result, host_filter)
        except Exception as exc:
            db_bp.host_update(host_id=host_id, NU_HOST_CHECK_ERROR=1)
            err.capture(
                "Failed to queue HOST_TASK",
                stage="QUEUE",
                exc=exc,
                host_id=host_id,
            )
            raise

    except Exception:
        # Request failures are normalized through ErrorHandler and converted
        # into a structured response in the `finally` block below.
        pass

    finally:
        if err.triggered:
            err.log_error(host_id=host_id, peer_ip=peer_ip)
            response_payload = {
                "status": 0,
                "message": err.format_error() or err.msg,
            }

        send_response(
            client_socket=client_socket,
            payload=response_payload,
            peer_ip=peer_ip,
        )
        close_client_socket(client_socket)


# ======================================================================
# Server loop
# ======================================================================
def serve_forever(server_socket: socket.socket) -> None:
    """
    Run the main TCP accept loop using a selector plus wake-up pipe.
    """
    selector = DefaultSelector()
    selector.register(server_socket, EVENT_READ)
    selector.register(WAKE_R_FD, EVENT_READ)

    while process_status["running"]:
        for key, _ in selector.select():
            if key.fileobj == server_socket:
                try:
                    client_socket, client_address = server_socket.accept()
                    client_socket.setblocking(True)

                    if process_status["running"]:
                        # Request handling stays inline on purpose: this
                        # service is operationally simple and easier to debug
                        # without worker threads or hidden dispatch layers.
                        log.event(
                            "client_connected",
                            client_address=client_address,
                        )
                        serve_client(client_socket)
                    else:
                        # If shutdown started after accept() but before request
                        # handling, return an explicit framed response instead
                        # of dropping the connection silently.
                        shutdown_payload = {
                            "status": 0,
                            "message": "Server shutting down",
                        }
                        send_response(
                            client_socket=client_socket,
                            payload=shutdown_payload,
                            peer_ip=str(client_address),
                        )
                        close_client_socket(client_socket)
                except Exception as exc:
                    err = errors.ErrorHandler(log)
                    err.capture(
                        reason="Accept loop failure",
                        stage="ACCEPT",
                        exc=exc,
                    )
                    err.log_error()

            elif key.fileobj == WAKE_R_FD:
                try:
                    os.read(WAKE_R_FD, 1)
                except Exception:
                    pass


# ======================================================================
# Entrypoint
# ======================================================================
def main() -> None:
    """
    Create the listening socket and run the main server loop.
    """
    log.service_start("appCataloga")
    err = errors.ErrorHandler(log)
    server_socket = None

    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("", k.SERVER_PORT))
        server_socket.listen(k.TOTAL_CONNECTIONS)
        log.event("server_listening", port=k.SERVER_PORT)

        # DB initialization happens only after the socket is listening, so
        # startup failures clearly distinguish network setup from DB setup.
        global db_bp
        db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)

        serve_forever(server_socket)

    except Exception as exc:
        err.capture(
            reason="Fatal appCataloga service error",
            stage="MAIN",
            exc=exc,
        )
        err.log_error()
        stop_self_service(script_name="appCataloga.py")
        sys.exit(1)

    finally:
        try:
            if server_socket:
                server_socket.close()
        except Exception:
            pass

        stop_self_service(script_name="appCataloga.py")
        log.service_stop("appCataloga")


if __name__ == "__main__":
    main()
