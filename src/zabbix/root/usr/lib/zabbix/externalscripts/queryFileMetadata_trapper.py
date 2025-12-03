#!/usr/bin/python3
"""
Call for information from a remote appCataloga module using socket.

Provide feedback to Zabbix about the host or appCataloga service

This script is unsecure and should only run through a secure encrypted network connection

    Usage:
        queryCataloga host_id=<host_id> host_uid=<host_uid> host_add=<host_add> host_port=<host_port> "user=<user>","passwd=<passwd>","query_tag=<query_tag>","timeout=<timeout>"

    Parameters:
        <host_id> Zabbix numerical primary key as defined in the macro {HOST.ID}
        <host_uid> host name used for physical equipment identification
        <host_add> host IP or host name known to the available DNS
        <host_port> port number to be used to access the host
        <user> user id to be used to access the host
        <passwd> user password to be used to access the host
        <query_tag> tag to be used to identify the query type to appCataloga
        <timeout> timeout in seconds to wait for a response from the remote appCataloga module

    Returns:
        JSON string with metadata structure
"""

import socket
import sys
import json
import re
import os
import subprocess

sys.path.append(
    "C:/Users/Fabio/AppData/Local/Temp/scp31195/root/RF.Fusion/src/zabbix/root/usr/lib/zabbix/externalscripts"
)

import z_shared as zsh
import defaultConfig as k

# define arguments as dictionary to associate each argument key to a default value and associated warning messages
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
    "help": {
        "set": False,
        "value": None,
        "message": "** Use queryCataloga host_id=<host_id> host_uid=<host_uid> host_add=<host_add> host_port=<host_port> user=<user> passwd=<passwd> query_tag=<query_tag> timeout=<timeout>. See code for details **",
    },
    "filter": {
        "set": False,
        "value": '{"mode":"NONE","start_date":null,"end_date":null,"last_n_files":null,"extension":".bin", "file_path": "/mnt/internal", "file_name":null, "agent": "local"}',
        "message": "Backup request is {value}",
    },
}

def send_to_zabbix_trapper(hostname: str, json_data: str):
    """
    Sends the JSON result to the Zabbix trapper item appCataloga.discovery.json
    using the zabbix_sender located in the same directory as this script.
    """
    

    # Full path to this script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Use zabbix_sender located in the same directory
    sender_path = os.path.join(script_dir, "zabbix_sender")

    try:
        subprocess.run([
            sender_path,
            "-z", "127.0.0.1",
            "-s", hostname,
            "-k", "appCataloga.discovery.json",
            "-o", json_data
        ], check=True)
    except Exception as e:
        print(f'{{"status_query":0,"message_query":"Zabbix sender error: {e}"}}')


def hide_sensitive_data(request: str, forbidden: list) -> str:
    """Replace sensitive data in the request list with asterisks"""
    for f in forbidden:
        request = re.sub(f, "*****", request)
    return request


def main():
    # create warning message object
    wm = zsh.warning_msg()

    # create an argument object
    arg = zsh.argument(wm, ARGUMENTS)

    # parse the command line arguments
    arg.parse(sys.argv)

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(arg.data["timeout"]["value"])

    try:
        client_socket.connect((k.ACAT_SERVER_ADD, k.ACAT_SERVER_PORT))

        requestS = (
            f'{arg.data["query_tag"]["value"]} '
            f'{arg.data["host_id"]["value"]} '
            f'{arg.data["host_uid"]["value"]} '
            f'{arg.data["host_add"]["value"]} '
            f'{arg.data["host_port"]["value"]} '
            f'{arg.data["user"]["value"]} '
            f'{arg.data["passwd"]["value"]} '
            f'{arg.data["filter"]["value"]} '
        )

        request = bytes(requestS, encoding="utf-8")
        client_socket.sendall(request)

    except Exception as e:
        error_json = f'{{"status_query":0,"message_query":"Error: {e}; Could not establish socket connection"}}'
        print(error_json)
        send_to_zabbix_trapper(arg.data["host_uid"]["value"], error_json)
        client_socket.close()
        return

    try:
        response = client_socket.recv(k.SMALL_BUFFER_SIZE)
        client_socket.close()
    except Exception as e:
        error_json = f'{{"status_query":0,"message_query":"Error: {e}; Error receiving data"}}'
        print(error_json)
        send_to_zabbix_trapper(arg.data["host_uid"]["value"], error_json)
        return

    try:
        response = response.decode(k.UTF_ENCODING)
    except Exception as e:
        error_json = f'{{"status_query":0,"message_query":"Error decoding data: {e}. Raw: {response}"}}'
        print(error_json)
        send_to_zabbix_trapper(arg.data["host_uid"]["value"], error_json)
        return

    # extract JSON from the received text
    start_index = response.lower().rfind(k.START_TAG.decode())
    end_index = response.lower().rfind(k.END_TAG.decode())

    json_output = response[start_index + len(k.START_TAG) : end_index]

    try:
        dict_output = json.loads(json_output)

        dict_output["request"] = hide_sensitive_data(
            request=requestS,
            forbidden=[arg.data["user"]["value"], arg.data["passwd"]["value"]],
        )
        dict_output["status_query"] = 1
        dict_output["message_query"] = wm.warning_msg

        json_final = json.dumps(dict_output)

        # 👉 ENVIA O JSON AO TRAPPER
        send_to_zabbix_trapper(arg.data["host_uid"]["value"], json_final)

        # Retorna o JSON ao Zabbix também
        print(json_final)

    except json.JSONDecodeError:
        error_json = f'{{"status_query":0,"message_query":"Malformed JSON received: {response}"}}'
        print(error_json)
        send_to_zabbix_trapper(arg.data["host_uid"]["value"], error_json)


if __name__ == "__main__":
    main()
