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
from datetime import datetime
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
        sys.exit(0)
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
        sys.exit(0)
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
    """
    Handle TCP client request using RF.Fusion ErrorHandler.
    """

    peer_ip = "unknown"
    try:
        peer_ip, _ = client_socket.getpeername()
    except Exception:
        pass

    err = sh.ErrorHandler(log)
    response_payload = {"status": 0, "message": "Unexpected error"}
    host = None
    host_id = None

    try:
        # ===============================================================
        # STAGE 1 — RECEIVE RAW MESSAGE
        # ===============================================================
        raw_msg = client_socket.recv(1024)
        if not raw_msg:
            err.set("Empty request", stage="READ")
            raise Exception("Empty request")

        # ===============================================================
        # STAGE 2 — PARSE MESSAGE
        # ===============================================================
        host = sh.parse_socket_message(
            data=raw_msg.decode(),
            peername=client_socket.getpeername(),
            log=log,
        )

        if host.get("command") != k.BACKUP_QUERY_TAG:
            err.set("Unsupported command", stage="COMMAND")
            raise Exception("Unsupported command")

        host_id = host.get("host_id")
        if host_id is None or host_id <= 0:
            err.set("Invalid host_id", stage="PARSE")
            raise Exception("Invalid host_id")

        # All metadata is safe to use (parse_socket_message guarantees structure)
        host_uid   = host["host_uid"]
        host_addr  = host["host_addr"]
        host_port  = host["host_port"]
        user       = host["user"]
        password   = host["password"]
        host_filter = host["filter"]

        # ===============================================================
        # STAGE 3 — ENSURE HOST EXISTS
        # ===============================================================
        try:
            log.entry(f"[HOST] Upsert new HOST entry id={host_id}")
            db_bp.host_upsert(
                ID_HOST=host_id,
                NA_HOST_NAME=host_uid,
                NA_HOST_ADDRESS=host_addr,
                NA_HOST_PORT=host_port,
                NA_HOST_USER=user,
                NA_HOST_PASSWORD=password,
            )
        except Exception as e:
            err.set("Failed to create/ensure HOST", stage="HOST_CREATE", exc=e)
            raise

        # ===============================================================
        # STAGE 4 — QUEUE HOST_TASK
        # ===============================================================
        try:
            result = db_bp.queue_host_task(
                host_id=host_id,
                task_type=k.HOST_TASK_CHECK_TYPE,
                task_status=k.TASK_PENDING,
                filter_dict=host_filter,
            )
            response_payload = result
        except Exception as e:
            db_bp.host_update(host_id=host_id, NU_HOST_CHECK_ERROR=1)
            err.set("Failed to queue HOST_TASK", stage="QUEUE", exc=e)
            raise

    except Exception:
        pass  # All errors handled by err

    finally:
        # ===============================================================
        # FINAL RESPONSE
        # ===============================================================
        if err.triggered:
            err.log_error(host_id=host_id)
            response_payload = {"status": 0, "message": err.msg}
        else:
            response_payload.update({
                "status": 1,
                "message": f"Host task created successfully at {datetime.now()}",
                "filter": f"{host_filter}"
            })

        try:
            framed = f"{k.START_TAG}{json.dumps(response_payload)}{k.END_TAG}"     
            client_socket.sendall(framed.encode("utf-8"))
            
            log.entry(f"[RESPONSE] Sent to {peer_ip}: {framed}")
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
