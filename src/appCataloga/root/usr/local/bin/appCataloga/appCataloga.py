#!/usr/bin/env python
"""
Listen to socket command to perform backup from a specific host and retuns the current status for said host.
Keep the backup process running in a separate process and restart it if it fails.

    Usage:
        appCataloga

    Parameters: <via socket connection>
        <hostid> single string with unique hostid
        <host_add> single string with host IP or host name known to the available DNS
        <user> single string with user id to be used to access the host
        <pass> single string with user password to be used to access the host

    Returns: (through soket connection to the client)
        (json) =  { "id_host": (int) zabbix host id,
                    "host_files": (int) total files in the host,
                    "pending_host_task": (int) number of pending tasks for the host,
                    "last_host_check": (int) unix timestamp,
                    "host_check_error": (int) number of errors in the last check,
                    "pending_backup": (int) number of files pending backup,
                    "last_backup": (int) unix timestamp,
                    "backup_error": (int) number of errors in the last backup,
                    "pending_processing": (int) number of files pending processing,
                    "processing_error": (int) number of errors in the last processing,
                    "last_processing": (int) unix timestamp,
                    "status": (int) 1=valid data or 0=error in the script,
                    "message": (str) error or warning information}

        Status may be 1=valid data or 0=error in the script
        All keys except "message" are suppresed when Status=0
        Message describe the error or warning information
"""

# Set system path to include modules from /etc/appCataloga
import sys

# sys.path.append('Y:\\RF.Fusion\\src\\appCataloga\\root\\etc\\appCataloga\\')
sys.path.append("/etc/appCataloga")

# Import standard libraries.
import socket
import json
import signal
from selectors import DefaultSelector, EVENT_READ
import os
import time
import inspect

import subprocess

# Import modules for file processing
import config as k
import shared as sh
import db_handler as dbh

process_status = {"conn": None, "halt_flag": None, "running": True}
# Create a pipe
r_pipe, w_pipe = os.pipe()


# function that stop systemd service
def stop_service():
    command = "bash -c " "systemctl stop appCataloga.service"

    subprocess.Popen(
        [command],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        shell=True,
    )


try:  # create a warning message object
    log = sh.log()
except Exception as e:
    stop_service()
    print(f"Error creating log object: {e}")
    exit(1)


# Define a signal handler for SIGTERM (kill command )
def sigterm_handler(signal=None, frame=None) -> None:
    global process_status
    global log

    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Kill signal received at: {current_function}()")
    process_status["running"] = False
    os.write(w_pipe, b"\0")


# Define a signal handler for SIGINT (Ctrl+C)
def sigint_handler(signal=None, frame=None) -> None:
    global process_status
    global log

    current_function = inspect.currentframe().f_back.f_code.co_name
    log.entry(f"Ctrl+C received at: {current_function}()")
    process_status["running"] = False
    os.write(w_pipe, b"\0")


# Register the signal handler function, to handle system kill commands
signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigint_handler)


def queue_task(
    conn: str,
    hostid: str,
    host_uid: str,
    host_addr: str,
    host_port: str,
    host_user: str,
    host_passwd: str,
):
    """Add host to queue task in the database and return current status

    Args:
        conn (str): Socket connection object. Defaults to "ClientIP".
        hostid (str): Target host id. Used as PK in the database host table.
        host_uid (str): Unique physical identifier to the host.
        host_addr (str): IP address or DNS to the host to be contacted.
        host_port (str): SSH port to be used to connect to the host.
        host_user (str): Host user for the SSH connection.
        host_passwd (str): Host password for the SSH connection.

    Returns:
        dict: Dictionary with the current status for the hostid
    """
    global log

    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME, log=log)

    db.add_host_task(
        task_type=db.BACKUP_TASK_TYPE,
        host_id=hostid,
        host_uid=host_uid,
        host_addr=host_addr,
        host_port=host_port,
        host_user=host_user,
        host_passwd=host_passwd,
    )

    host_stat = db.get_host_status(hostid)

    return host_stat


def serve_client(client_socket):
    global log

    receiving_data = True

    while receiving_data:
        try:
            data = client_socket.recv(128)
        except Exception as e:
            log.entry(f"Error receiving data: {e}")
            data = None

        # in case of error or no data received
        if not data:
            receiving_data = False

        else:
            try:
                host = data.decode().split(" ")
            except Exception as e:
                log.entry(f"Error decoding data: {e}")
                host = [None]

            if host[0] == k.BACKUP_QUERY_TAG:
                try:
                    log.entry(
                        f"Backup request received data from {client_socket.getpeername()[0]}"
                    )

                    host[0] = (
                        client_socket.getpeername()
                    )  # replace list first element with client IP address

                    host_statistics = queue_task(
                        *host
                    )  # unpack list to pass as arguments to queue_task

                    response = f"{k.START_TAG}{json.dumps(host_statistics)}{k.END_TAG}"

                except Exception as e:
                    log.entry(f"Error backup request: {e}")
                    response = f'{k.START_TAG}{{"status":0,"message":"Could not create a backup task from the data provided."}}{k.END_TAG}'
                    pass

                receiving_data = False

            elif host[0] == k.CATALOG_QUERY_TAG:
                log.entry(
                    f"Catalog query received data from {client_socket.getpeername()[0]}: {data.decode()}"
                )

                response = f'{k.START_TAG}{{"status":0,"message":"catalog command not implemented"}}{k.END_TAG}'

                receiving_data = False

            else:
                log.entry(
                    f"Ignored data from from {client_socket.getpeername()[0]}. Received: {data.decode()}"
                )

                response = f'{k.START_TAG}{{"status":0,"message":"host command not recognized"}}{k.END_TAG}'

                receiving_data = False

        byte_response = bytes(response, encoding="utf-8")

        client_socket.sendall(byte_response)

        log.entry(f"Response sent to {client_socket.getpeername()[0]}: {response}")

        client_socket.close()


def serve_forever(server_socket, interrupt_read):
    global process_status
    global log

    sel = DefaultSelector()
    sel.register(interrupt_read, EVENT_READ)
    sel.register(server_socket, EVENT_READ)
    sel.register(r_pipe, EVENT_READ)

    while process_status["running"]:
        # Wait for events using selector.select() method
        for key, _ in sel.select():
            # if client tries to connect, accept the connection and serve the client
            if key.fileobj == server_socket:
                client_socket, client_address = server_socket.accept()
                if process_status["running"]:
                    log.entry(f"Connection established with: {client_address}")
                    serve_client(client_socket)
                else:
                    log.entry("Connection attempt rejected. Server is shutting down.")
                    response = f'{k.START_TAG}{{"status":0,"message":"Server shutting down"}}{k.END_TAG}'
                    byte_response = bytes(response, encoding="utf-8")
                    client_socket.sendall(byte_response)
                    client_socket.close()

        # sleep one second to avoid system hang in case of error
        time.sleep(1)


def main():
    global log

    log.entry("Starting....")

    try:
        interrupt_read, interrupt_write = socket.socketpair()

        log.entry(f"Server is listening on port {k.SERVER_PORT}")

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = ("", k.SERVER_PORT)
        server_socket.bind(server_address)
        server_socket.listen(k.TOTAL_CONNECTIONS)

        serve_forever(server_socket=server_socket, interrupt_read=interrupt_read)

        server_socket.close()
        stop_service()

    except Exception as e:
        log.entry(f"Error: {e}")
        stop_service()
        exit(1)

    log.entry("Shutting down....")


if __name__ == "__main__":
    main()
