#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
appCataloga - Host Backup Orchestrator Daemon

This daemon listens for socket commands to enqueue backup tasks for remote hosts,
interacts with the database layer (dbHandlerBKP), and handles graceful shutdown.
It is designed to run in Linux environments without relying on systemd; shutdown
is implemented by locating PIDs for this script and sending POSIX signals.
"""

import os
import sys
import json
import time
import socket
import signal
import subprocess
from selectors import DefaultSelector, EVENT_READ

# load Config and Database folders
_CFG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga"))
if _CFG_DIR not in sys.path and os.path.isdir(_CFG_DIR):
    sys.path.append(_CFG_DIR)
_DB_DIR = os.path.join(os.path.dirname(__file__), "db")
if _DB_DIR not in sys.path and os.path.isdir(_DB_DIR):
    sys.path.append(_DB_DIR)

# Import customized libraries
from db.dbHandlerBKP import dbHandlerBKP
import shared as sh
import config as k


# ======================================================================
# Global state and logger
# ======================================================================
process_status = {"running": True}

# Pipe used to wake the selector on SIGINT/SIGTERM for fast shutdown
r_pipe, w_pipe = os.pipe()

# Logger using shared.py implementation (configured via config.py by default)
log = sh.log()


# ======================================================================
# Signal handling
# ======================================================================
def sigterm_handler(signum: int = None, frame=None) -> None:
    """Handle SIGTERM: set shutdown flag and wake the selector.

    Args:
        signum (int, optional): Signal number.
        frame (FrameType, optional): Current stack frame.

    Returns:
        None
    """
    process_status["running"] = False
    try:
        os.write(w_pipe, b"\\0")
    except Exception:
        pass


def sigint_handler(signum: int = None, frame=None) -> None:
    """Handle SIGINT: set shutdown flag and wake the selector.

    Args:
        signum (int, optional): Signal number.
        frame (FrameType, optional): Current stack frame.

    Returns:
        None
    """
    process_status["running"] = False
    try:
        os.write(w_pipe, b"\\0")
    except Exception:
        pass


signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


# ======================================================================
# Service stop: kill by PID discovery (no systemd dependency)
# ======================================================================
def stop_self_service(script_name: str = "appCataloga.py") -> None:
    """Stop daemon processes by script name using POSIX signals.

    This function searches for all PIDs whose command line contains the given
    script name (e.g., "appCataloga.py") and attempts a clean termination:
    first sends SIGTERM, waits briefly, and escalates to SIGKILL if needed.

    Args:
        script_name (str): Name to search in the process list. Defaults to "appCataloga.py".

    Returns:
        None
    """
    try:
        result = subprocess.run(
            ["pgrep", "-f", script_name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        pids = [int(pid) for pid in result.stdout.split() if pid.strip().isdigit()]
        current_pid = os.getpid()

        if not pids:
            log.entry(f"No running process found for {script_name}")
            return

        # Send SIGTERM to other matching processes
        for pid in pids:
            if pid == current_pid:
                continue
            try:
                os.kill(pid, signal.SIGTERM)
                log.entry(f"Sent SIGTERM to PID {pid}")
            except ProcessLookupError:
                # Process already exited
                continue
            except Exception as e:
                log.warning(f"Failed to send SIGTERM to PID {pid}: {e}")

        # Wait and escalate to SIGKILL if still alive
        time.sleep(2.0)
        for pid in pids:
            if pid == current_pid:
                continue
            try:
                # os.kill(pid, 0) checks for existence without sending a signal
                os.kill(pid, 0)
            except OSError:
                continue  # Process is gone
            else:
                os.kill(pid, signal.SIGKILL)
                log.warning(f"Sent SIGKILL to stubborn PID {pid}")

    except Exception as e:
        log.error(f"stop_self_service failed: {e}")


# ======================================================================
# Client handling
# ======================================================================
def serve_client(client_socket: socket.socket) -> None:
    """Handle a single TCP client connection and enqueue a backup task if requested.

    This function processes one client request sent to the appCataloga daemon.  
    It expects a message framed by START_TAG/END_TAG tokens and containing all
    required host identification parameters and optional filter settings.
    
    The function performs the following steps:
        1. Receive and decode the socket message (max 256 bytes).
        2. Parse it using `shared.parse_socket_message()`.
        3. Validate the command type (only BACKUP_QUERY_TAG is supported).
        4. Check if the referenced host exists in the database:
            - If not, create a new HOST record.
        5. Enqueue a corresponding HOST_TASK via `db_bp.queue_host_task()`.
        6. Update host status counters and return a structured JSON response.
        7. Always send a response to the client, even in case of error.

    Args:
        client_socket (socket.socket): Active socket connection from the requesting host.

    Raises:
        Exception: Propagated for unexpected internal errors, though most are logged and handled.

    Returns:
        None: The function sends the response directly to the socket and closes the connection.
    """
    
    peer_ip = "unknown"
    try:
        peer_ip, _ = client_socket.getpeername()
    except Exception:
        pass

    response = f'{k.START_TAG}{{"status":0,"message":"Unexpected error"}}{k.END_TAG}'

    try:
        raw_msg = client_socket.recv(256)
        if not raw_msg:
            response = f'{k.START_TAG}{{"status":0,"message":"Empty request"}}{k.END_TAG}'
            return

        try:
            host = sh.parse_socket_message(
                data=raw_msg.decode(),
                peername=client_socket.getpeername(),
                log=log
            )
        except Exception as e:
            log.entry(f"[PARSE] Failed to parse message from {peer_ip}: {e}")
            host = {"command": None, "peer": {"ip": peer_ip}}

        if host.get("command") == k.BACKUP_QUERY_TAG:
            try:
                host_id = int(host.get("host_id") or 0)
                if host_id <= 0:
                    log.warning(f"[REQUEST] Invalid host_id from {peer_ip}. Ignoring request.")
                    return

                log.entry(f"[REQUEST] Backup command received from {peer_ip} (host_id={host_id})")

                # Ensure host exists
                if not db_bp.host_exists(host_id):
                    log.entry(f"[CREATE] Host {host_id} not found. Creating new record.")
                    db_bp.host_create(
                        ID_HOST=host.get("host_id"),
                        NA_HOST_NAME=host.get("host_uid"),
                        NA_HOST_ADDRESS=host.get("host_addr"),
                        NA_HOST_PORT=host.get("host_port"),
                        NA_HOST_USER=host.get("user"),
                        NA_HOST_PASSWORD=host.get("password"),
                    )

                # Queue new host task
                host_statistics = db_bp.queue_host_task(
                    host_id=host_id,
                    task_type=k.HOST_TASK_CHECK_TYPE,
                    task_status=k.TASK_PENDING,
                    filter_dict=host.get("filter"),
                )
                try:
                    response_data = json.dumps(host_statistics)
                except Exception:
                    response_data = '{"status":0,"message":"Invalid response format"}'

                response = f"{k.START_TAG}{response_data}{k.END_TAG}"

            except Exception as e:
                if db_bp.host_exists(host_id):
                    db_bp.host_update(host_id=host_id, NU_HOST_CHECK_ERROR=1)
                    log.warning(f"[UPDATE] Host {host_id} marked with host_check_error=1 due to failure.")
                log.entry(f"[ERROR] Failed to handle backup request from {peer_ip}: {e}")
                response = f'{k.START_TAG}{{"status":0,"message":"Failed to create backup task"}}{k.END_TAG}'
        else:
            log.entry(f"[IGNORE] Unsupported command from {peer_ip}: {raw_msg[:200]}...")
            response = f'{k.START_TAG}{{"status":0,"message":"Unknown host command"}}{k.END_TAG}'

    except Exception as e:
        log.entry(f"[FATAL] Unexpected error while serving {peer_ip}: {e}")

    finally:
        try:
            client_socket.sendall(response.encode("utf-8"))
            log.entry(f"[RESPONSE] Sent to {peer_ip}: {response}")
        except Exception as e:
            log.warning(f"[SEND] Failed to send response to {peer_ip}: {e}")
        try:
            client_socket.close()
        except Exception:
            pass


# ======================================================================
# Server loop
# ======================================================================
def serve_forever(server_socket: socket.socket) -> None:
    """Main TCP accept loop using selectors with pipe wake-up on signals.

    Args:
        server_socket (socket.socket): Listening socket.

    Returns:
        None
    """
    sel = DefaultSelector()
    sel.register(server_socket, EVENT_READ)
    sel.register(r_pipe, EVENT_READ)

    while process_status["running"]:
        for key, _ in sel.select():
            if key.fileobj == server_socket:
                try:
                    client_socket, client_address = server_socket.accept()
                    client_socket.setblocking(True)
                    if process_status["running"]:
                        log.entry(f"Connection established with {client_address}")
                        serve_client(client_socket)
                    else:
                        # If shutting down, return a framed message then close
                        shutdown_resp = f'{k.START_TAG}{{"status":0,"message":"Server shutting down"}}{k.END_TAG}'
                        client_socket.sendall(shutdown_resp.encode("utf-8"))
                        client_socket.close()
                except Exception as e:
                    log.entry(f"Accept/serve error: {e}")

            elif key.fileobj == r_pipe:
                # Drain the wake-up byte; loop condition will break at top
                try:
                    os.read(r_pipe, 1)
                except Exception:
                    pass


# ======================================================================
# Entrypoint
# ======================================================================
def main() -> None:
    """Program entrypoint: create server socket and run the accept loop.

    Returns:
        None
    """
    log.entry("Starting appCataloga service...")

    server_socket = None
    try:
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("", k.SERVER_PORT))
        server_socket.listen(k.TOTAL_CONNECTIONS)
        log.entry(f"Server listening on port {k.SERVER_PORT}")

        # Initialize DB handler after network init to surface socket errors early
        global db_bp
        db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME,log=log)

        serve_forever(server_socket)

    except Exception as e:
        log.error(f"Fatal error: {e}")
        # Attempt to stop other running instances (excludes current PID)
        stop_self_service(script_name="appCataloga.py")
        sys.exit(1)

    finally:
        try:
            if server_socket:
                server_socket.close()
        except Exception:
            pass
        # Attempt to stop other running instances (excludes current PID)
        stop_self_service(script_name="appCataloga.py")
        log.entry("Server shutdown complete.")


if __name__ == "__main__":
    main()
