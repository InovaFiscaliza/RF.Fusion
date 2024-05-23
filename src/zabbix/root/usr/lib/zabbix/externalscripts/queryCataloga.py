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
                    "status": (int) 1=valid data or 0=error in the appCataloga server script,
                    "message": (str) error or warning informatiom in the appCataloga server script,
                    "request": (list) list of strings used to compose the request to the server}
                    "status_query": (int) 1=valid data or 0=error in the client query script,
                    "message_query": (str) error or warning information in the client query script}

        Status may be 1=valid data or 0=error in the script
        All keys except "message_query" are suppressed when Status=0
        Message describe the error or warning information
"""

import socket
import sys
import json
import re

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
        "message": "Using default query tag",
    },
    "timeout": {
        "set": False,
        "value": k.ACAT_DEFAULT_TIMEOUT,
        "message": "Using default timeout",
    },
    "help": {
        "set": True,
        "value": None,
        "message": "** Use queryCataloga host_id=<host_id> host_uid=<host_uid> host_add=<host_add> host_port=<host_port> user=<user> passwd=<passwd> query_tag=<query_tag> timeout=<timeout>. See code for details **",
    },
}


def hide_sensitive_data(request: list) -> list:
    """Replace sensitive data in the request list with asterisks

    Args:
        request (list): list of strings to be sanitized

    Returns:
        list: sanitized list of strings
    """

    for request_item in request:
        request_item = re.sub(r"user=.*", "user=*****", request_item)
        request_item = re.sub(r"passwd=.*", "passwd=*****", request_item)

    return request


def main():
    # create a warning message object
    wm = zsh.warning_msg()

    # create an argument object
    arg = zsh.argument(wm, ARGUMENTS)

    # parse the command line arguments
    arg.parse(sys.argv)

    # compose the request to the server
    requestS = (
        f'{arg.data["query_tag"]["value"]} '
        f'{arg.data["host_id"]["value"]} '
        f'{arg.data["host_uid"]["value"]} '
        f'{arg.data["host_add"]["value"]} '
        f'{arg.data["host_port"]["value"]} '
        f'{arg.data["user"]["value"]} '
        f'{arg.data["passwd"]["value"]}'
    )

    request = bytes(requestS, encoding="utf-8")

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(arg.data["timeout"]["value"])

    try:
        client_socket.connect((k.ACAT_SERVER_ADD, k.ACAT_SERVER_PORT))
        client_socket.sendall(request)
    except Exception as e:
        print(
            f'{{"status_query":0,"message_query":"Error: {e}; Could establish socket connection"}}'
        )
        client_socket.close()
        exit()

    try:
        response = client_socket.recv(k.SMALL_BUFFER_SIZE)
        client_socket.close()
    except Exception as e:
        print(
            f'{{"status_query":0,"message_query":"Error: {e}; Error receiving data"}}'
        )
        client_socket.close()
        exit()

    try:
        response = response.decode(k.UTF_ENCODING)
    except Exception as e:
        print(
            f'{{"status_query":0,"message_query":"Error: {e}. Error decoding data with {k.UTF_ENCODING}: {response}"}}'
        )
        client_socket.close()
        exit()

    # extract JSON data from bytestring
    start_index = response.lower().rfind(k.START_TAG.decode())
    end_index = response.lower().rfind(k.END_TAG.decode())

    # extract JSON data removing the last bracket to later splice with the tail json data from this script
    json_output = response[start_index + len(k.START_TAG) : end_index]

    try:
        dict_output = json.loads(json_output)

        # loop through string list within the dict_output['Request'] value and replace strings "user=.*"" and "passwd=.*" with "user=*****" and "passwd=*****"
        dict_output["Request"] = hide_sensitive_data(dict_output["Request"])

        dict_output["status_query"] = 1
        dict_output["message_query"] = wm.warning_msg

        print(json.dumps(dict_output))

    except json.JSONDecodeError as e:
        print(
            f'{"status_query":0,"message_query":"Error: Malformed JSON received. Dumped: {response}"}'
        )


if __name__ == "__main__":
    main()
