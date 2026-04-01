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

import os
import sys
from datetime import datetime
from selectors import DefaultSelector, EVENT_READ

from bootstrap_paths import bootstrap_app_paths
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
# The selector waits on both the listening socket and this pipe so that
# signal handlers can wake the loop immediately instead of waiting for
# network activity.
WAKE_R_FD, WAKE_W_FD = os.pipe()
log = logging_utils.log()


# ======================================================================
def _shutdown_cleanup(signal_name: str) -> None:
    """
    Wake the selector loop so shutdown is noticed immediately.
    """
    process_control.wake_selector(WAKE_W_FD)


signal_runtime.install_shutdown_handlers(
    process_status=process_status,
    logger=log,
    on_shutdown=_shutdown_cleanup,
    log_fields={"action": "shutdown"},
)


def build_success_response(task_result: dict, host_filter) -> dict:
    """
    Build the success payload returned after queueing one HOST_TASK.
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


def stop_service_siblings() -> None:
    """
    Ask sibling `appCataloga.py` processes to terminate.

    The entrypoint uses this both after fatal startup/runtime failure and
    during final teardown so the service does not leave duplicate daemons
    behind.
    """
    process_control.stop_self_service(
        script_name=SCRIPT_NAME,
        logger=log,
    )


def build_service_selector(*, server_socket) -> DefaultSelector:
    """
    Create the selector owned by this appCataloga daemon instance.

    The service listens to exactly two readiness sources:
    - the listening TCP socket for incoming clients
    - the wake-up pipe used by signal-driven shutdown
    """
    selector = DefaultSelector()
    selector.register(server_socket, EVENT_READ)
    selector.register(WAKE_R_FD, EVENT_READ)
    return selector


def close_service_runtime(*, selector, server_socket) -> None:
    """
    Best-effort release of local runtime resources owned by `main()`.

    This helper closes only in-process resources. Process-level shutdown of
    sibling daemons is handled separately by `stop_service_siblings()`.
    """
    try:
        if selector is not None:
            selector.close()
    except Exception:
        pass

    try:
        if server_socket:
            server_socket.close()
    except Exception:
        pass


def handle_host_request(host: dict, err, db) -> tuple[int | None, dict]:
    """
    Execute the appCataloga-specific business action for one parsed request.

    This is the only place in this module that knows what a valid
    appCataloga TCP request actually means.

    `socket_handler` stops at transport/protocol concerns and hands us a
    normalized `host` payload. From here on we are in pure service/domain
    flow: validate the request, persist HOST state, queue one HOST_TASK and
    return the payload that will go back to the client.

    Flow:
        1. validate the request contract expected by appCataloga
        2. upsert the HOST row
        3. enqueue the initial HOST_TASK
        4. return `(host_id, response_payload)` for socket finalization
    """
    command = str(host.get("command") or "").strip().lower()
    task_type = COMMAND_TASK_MAP.get(command)

    if task_type is None:
        err.capture("Unsupported command", stage="COMMAND")
        raise ValueError("Unsupported command")

    host_id = host.get("host_id")
    if host_id is None or host_id <= 0:
        err.capture("Invalid host_id", stage="PARSE")
        raise ValueError("Invalid host_id")

    # The filter is carried through to the queued HOST_TASK. Keeping it in a
    # local variable early makes the success and failure paths easier to read.
    host_filter = host["filter"]

    try:
        # Phase 1: ensure the HOST row exists and is refreshed with the
        # connection details received from the client.
        log.event("host_upsert", host_id=host_id, host_uid=host["host_uid"])
        db.host_upsert(
            ID_HOST=host_id,
            NA_HOST_NAME=host["host_uid"],
            NA_HOST_ADDRESS=host["host_addr"],
            NA_HOST_PORT=host["host_port"],
            NA_HOST_USER=host["user"],
            NA_HOST_PASSWORD=host["password"],
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
        # Phase 2: queue the entry task for the requested command.
        #
        # backup -> CHECK host task, which later opens discovery
        # stop   -> direct backlog rollback task, without host connectivity hop
        task_result = db.queue_host_task(
            host_id=host_id,
            task_type=task_type,
            task_status=k.TASK_PENDING,
            filter_dict=host_filter,
        )
        return host_id, build_success_response(task_result, host_filter)
    except Exception as exc:
        # If queueing fails after the HOST row exists, record that the host
        # needs attention. The structured error returned to the socket still
        # comes from ErrorHandler below.
        db.host_update(host_id=host_id, NU_HOST_CHECK_ERROR=1)
        err.capture(
            "Failed to queue HOST_TASK",
            stage="QUEUE",
            exc=exc,
            host_id=host_id,
        )
        raise


# ======================================================================
# Entrypoint
# ======================================================================
def main() -> None:
    """
    Create the listening socket and run the main server loop.

    Reading guide:
        1. bootstrap server resources (socket + DB)
        2. declare the request/runtime objects used by the selector loop
        3. wire the selector owned by this process
        4. dispatch ready events until shutdown flips `process_status`
    """
    log.service_start(SERVICE_NAME)
    err = errors.ErrorHandler(log)
    server_socket = None
    selector = None

    try:
        # --------------------------------------------------------------
        # ACT I — Bring up the listening TCP socket
        # --------------------------------------------------------------
        server_socket = socket_handler.open_listening_socket(
            port=k.SERVER_PORT,
            backlog=k.TOTAL_CONNECTIONS,
        )
        log.event("server_listening", port=k.SERVER_PORT)

        # --------------------------------------------------------------
        # ACT II — Initialize dependencies after the port is live
        # --------------------------------------------------------------
        # DB initialization happens only after the socket is listening, so
        # startup failures clearly distinguish network setup from DB setup.
        db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)

        # --------------------------------------------------------------
        # ACT III — Build the selector owned by this process
        # --------------------------------------------------------------
        selector = build_service_selector(server_socket=server_socket)

        # --------------------------------------------------------------
        # ACT IV — Main service loop
        # --------------------------------------------------------------
        while process_status["running"]:
            for key, _ in selector.select():
                # The selector has only two event sources. Keeping this branch
                # inline makes the daemon easier to debug than bouncing
                # through generic dispatch helpers.
                if key.fileobj == server_socket:
                    socket_handler.handle_ready_server_socket(
                        server_socket=server_socket,
                        process_status=process_status,
                        handle_host_request=handle_host_request,
                        db=db_bp,
                        logger=log,
                        errors_module=errors,
                        none_filter=k.NONE_FILTER,
                        shutdown_payload=SHUTDOWN_PAYLOAD,
                        start_tag=k.START_TAG,
                        end_tag=k.END_TAG,
                    )
                elif key.fileobj == WAKE_R_FD:
                    socket_handler.drain_wakeup_pipe(WAKE_R_FD)

    except Exception as exc:
        # Any exception here means the daemon itself failed, not a single
        # client request. Capture it once, log it, and ask sibling processes
        # with the same script name to terminate as part of self-recovery.
        err.capture(
            reason="Fatal appCataloga service error",
            stage="MAIN",
            exc=exc,
        )
        err.log_error()
        stop_service_siblings()
        sys.exit(1)

    finally:
        # Teardown is intentionally defensive. We are already leaving the
        # service loop, so cleanup must not raise and mask the original exit
        # reason.
        close_service_runtime(
            selector=selector,
            server_socket=server_socket,
        )

        # This best-effort stop mirrors the fatal path above and helps avoid
        # orphaned sibling instances if shutdown happened through signal flow
        # instead of the explicit exception path.
        stop_service_siblings()
        log.service_stop(SERVICE_NAME)


if __name__ == "__main__":
    main()
