#!/usr/bin/python3
"""
Call for information from a remote appCataloga module using socket.

Provide feedback to Zabbix about the host or appCataloga service.

This script is unsecure and should only run through a secure encrypted network connection.

Returns:
    JSON string with metadata structure
"""

import socket
import sys
import json
import os
import subprocess
from typing import Dict, Any

import z_shared as zsh
import defaultConfig as k


# ------------------------------------------------------------------------------
# Argument definitions
# ------------------------------------------------------------------------------

ARGUMENTS = {
    "host_id": {
        "set": False,
        "value": k.ACAT_DEFAULT_HOST_ID,
        "message": "Using default host id",
    },
    "host_uid": {
        "set": False,
        "value": k.ACAT_DEFAULT_HOST_UID,
        "message": "Using default host uid",
    },
    "host_add": {
        "set": False,
        "value": k.ACAT_DEFAULT_HOST_ADD,
        "message": "Using default host address",
    },
    "host_port": {
        "set": False,
        "value": k.ACAT_DEFAULT_HOST_PORT,
        "message": "Using default host port",
    },
    "user": {
        "set": False,
        "value": k.ACAT_DEFAULT_USER,
        "message": "Using default user",
    },
    "passwd": {
        "set": False,
        "value": k.ACAT_DEFAULT_PASSWD,
        "message": "Using default password",
    },
    "query_tag": {
        "set": False,
        "value": k.ACAT_DEFAULT_QUERY_TAG,
        "message": "Using file metadata query tag",
    },
    "timeout": {
        "set": False,
        "value": k.ACAT_DEFAULT_TIMEOUT,
        "message": "Using default timeout",
    },
    "filter_lnx": {
        "set": False,
        "value": (
            '{"mode":"NONE","start_date":null,"end_date":null,"last_n_files":null,"extension":".bin","file_path":"/mnt/internal","file_name":null,"agent":"local"}'
        ),
        "message": "Using Linux filter",
    },
    "filter_win": {
        "set": False,
        "value": (
            '{"mode":"NONE","start_date":null,"end_date":null,"last_n_files":null,"extension":".dbm","file_path":"C:/CelPlan/CellWireless RU/Spectrum/Completed","file_name":null,"agent":"local"}'
        ),
        "message": "Using Windows filter",
    },
}


# ------------------------------------------------------------------------------
# Utility
# ------------------------------------------------------------------------------

def normalize_json(value: str) -> Dict[str, Any]:
    """
    Normalize JSON received from Zabbix macros.
    Handles escaped quotes and outer string wrapping.
    """
    if not isinstance(value, str):
        return value

    v = value.strip()

    # Remove outer quotes if present
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1]

    # Unescape quotes coming from Zabbix
    v = v.replace('\\"', '"')

    return json.loads(v)


def hide_sensitive_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of the payload with sensitive fields masked."""
    safe = payload.copy()
    if "passwd" in safe:
        safe["passwd"] = "*****"
    return safe


# ------------------------------------------------------------------------------
# Zabbix trapper sender
# ------------------------------------------------------------------------------

def send_to_zabbix_trapper(hostname: str, json_data: str) -> None:
    """
    Sends JSON result to the Zabbix trapper item appCataloga.discovery.json
    using zabbix_sender located in the same directory as this script.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    sender_path = os.path.join(script_dir, "zabbix_sender")

    try:
        subprocess.run(
            [
                sender_path,
                "-z", "127.0.0.1",
                "-s", hostname,
                "-k", "appCataloga.discovery.json",
                "-o", json_data,
            ],
            check=True,
        )
    except Exception as e:
        print(json.dumps({
            "status_query": 0,
            "message_query": f"Zabbix sender error: {e}",
        }))


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main():
    wm = zsh.warning_msg()
    arg = zsh.argument(wm, ARGUMENTS)
    arg.parse(sys.argv)

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(arg.data["timeout"]["value"])

    try:
        # ------------------------------------------------------------------
        # 1) Connect to appCataloga server
        # ------------------------------------------------------------------
        client_socket.connect((k.ACAT_SERVER_ADD, k.ACAT_SERVER_PORT))

        # ------------------------------------------------------------------
        # 2) Detect host OS (heuristic)
        # ------------------------------------------------------------------
        host_uid = arg.data["host_uid"]["value"]
        is_windows = "CW" in host_uid  # documented heuristic
        
        print('Debug message /n')
        print(arg.data)

        # ------------------------------------------------------------------
        # 3) Select and normalize filter (DICT, not string)
        # ------------------------------------------------------------------
        if is_windows:
            filter_value = normalize_json(arg.data["filter_win"]["value"])
        else:
            filter_value = normalize_json(arg.data["filter_lnx"]["value"])

        # ------------------------------------------------------------------
        # 4) Build JSON payload
        # ------------------------------------------------------------------
        payload = {
            "query_tag": arg.data["query_tag"]["value"],
            "host_id": arg.data["host_id"]["value"],
            "host_uid": arg.data["host_uid"]["value"],
            "host_add": arg.data["host_add"]["value"],
            "host_port": arg.data["host_port"]["value"],
            "user": arg.data["user"]["value"],
            "passwd": arg.data["passwd"]["value"],
            "filter": filter_value,
        }

        client_socket.sendall(json.dumps(payload).encode("utf-8"))

    except Exception as e:
        error_json = json.dumps({
            "status_query": 0,
            "message_query": f"Socket connection error: {e}",
        })
        print(error_json)
        send_to_zabbix_trapper(arg.data["host_uid"]["value"], error_json)
        client_socket.close()
        return

    # ----------------------------------------------------------------------
    # 5) Receive response
    # ----------------------------------------------------------------------
    try:
        response_bytes = b""
        while True:
            chunk = client_socket.recv(4096)
            if not chunk:
                break
            response_bytes += chunk

        client_socket.close()
        response = response_bytes.decode(k.UTF_ENCODING)

    except Exception as e:
        error_json = json.dumps({
            "status_query": 0,
            "message_query": f"Error receiving data: {e}",
        })
        print(error_json)
        send_to_zabbix_trapper(arg.data["host_uid"]["value"], error_json)
        return

    # ----------------------------------------------------------------------
    # 6) Extract JSON payload from server response
    # ----------------------------------------------------------------------
    start_index = response.lower().rfind(k.START_TAG.decode())
    end_index = response.lower().rfind(k.END_TAG.decode())

    json_output = response[start_index + len(k.START_TAG): end_index]

    try:
        dict_output = json.loads(json_output)
        dict_output["request"] = hide_sensitive_data(payload)
        dict_output["status_query"] = 1
        dict_output["message_query"] = wm.warning_msg

        json_final = json.dumps(dict_output)

        send_to_zabbix_trapper(arg.data["host_uid"]["value"], json_final)
        print(json_final)

    except json.JSONDecodeError:
        error_json = json.dumps({
            "status_query": 0,
            "message_query": f"Malformed JSON received: {response}",
        })
        print(error_json)
        send_to_zabbix_trapper(arg.data["host_uid"]["value"], error_json)


if __name__ == "__main__":
    main()
