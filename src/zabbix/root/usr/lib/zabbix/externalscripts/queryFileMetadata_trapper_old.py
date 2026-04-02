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
import re
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
        "types": ["warning", "default"],
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
    "filter": {
        "set": False,
        "value": (
            '{"mode":"NONE","start_date":null,"end_date":null,"last_n_files":null,"extension":null,"file_path":null,"file_name":null}'
        ),
        "message": "Using canonical filter payload",
    },
    "file_path": {
        "set": False,
        "value": "",
        "message": "Using default file path from filter payload",
    },
    "extension": {
        "set": False,
        "value": "",
        "message": "Using default extension from filter payload",
    },
    "filter_lnx": {
        "set": False,
        "value": (
            '{"mode":"NONE","start_date":null,"end_date":null,'
            '"last_n_files":null,"extension":".bin",'
            '"file_path":"/mnt/internal","file_name":null}'
        ),
        "message": "Using Linux filter",
    },
    "filter_win": {
        "set": False,
        "value": (
            '{"mode":"NONE","start_date":null,"end_date":null,'
            '"last_n_files":null,"extension":".bin",'
            '"file_path":"C:/CelPlan/CellWireless RU/Spectrum/Completed",'
            '"file_name":null}'
        ),
        "message": "Using Windows filter",
    },
}


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
# Utility
# ------------------------------------------------------------------------------

def hide_sensitive_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of the payload with sensitive fields masked."""
    safe = payload.copy()
    if "passwd" in safe:
        safe["passwd"] = "*****"
    return safe


def normalize_json(value: str) -> Dict[str, Any]:
    """Normalize JSON received from Zabbix macros."""
    if not isinstance(value, str):
        return value

    v = value.strip()

    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1]

    v = v.replace('\\"', '"')
    return json.loads(v)


def normalize_text(value: str) -> str:
    """Normalize a plain macro value that may arrive wrapped or escaped."""
    if value is None:
        return ""

    text = str(value).strip()

    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        text = text[1:-1]

    text = text.replace('\\"', '"')
    return text.strip()


def build_filter_payload(arg_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build the final filter payload sent to appCataloga.

    Preferred contract:
        - `filter`: base JSON payload
        - `file_path`: optional override
        - `extension`: optional override

    Legacy compatibility:
        - if `filter` was not explicitly provided, keep accepting the old
          `filter_lnx` / `filter_win` pair selected by the host heuristic.
    """
    if arg_data["filter"]["set"]:
        filter_value = normalize_json(arg_data["filter"]["value"])
    else:
        host_uid = arg_data["host_uid"]["value"]
        is_windows = "CW" in host_uid  # documented heuristic

        if is_windows:
            filter_value = normalize_json(arg_data["filter_win"]["value"])
        else:
            filter_value = normalize_json(arg_data["filter_lnx"]["value"])

    file_path = normalize_text(arg_data["file_path"]["value"])
    extension = normalize_text(arg_data["extension"]["value"])

    if file_path:
        filter_value["file_path"] = file_path

    if extension:
        filter_value["extension"] = extension

    return filter_value


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
        client_socket.connect((k.ACAT_SERVER_ADD, k.ACAT_SERVER_PORT))

        # ------------------------------------------------------------------
        # 1) Build the final filter payload
        # ------------------------------------------------------------------
        filter_value = build_filter_payload(arg.data)

        # ------------------------------------------------------------------
        # 2) Build JSON payload (final protocol)
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

        request_bytes = json.dumps(payload).encode("utf-8")
        client_socket.sendall(request_bytes)

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
    # 3) Receive response (robust TCP read)
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
    # 4) Extract JSON payload from server response
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
