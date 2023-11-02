#!/usr/bin/python3
"""
Call for information from a remote appCataloga module using socket.

Provide feedback to Zabbix about the host or appCataloga service

This script is unsecure and should only run through a secure encripted network connection
    
    Usage:
        queryCataloga <query> <host_id> <host_add> <user> <pass>
    
    Parameters:
        <query>
        <host_id> single string with host unique id or key to be used to store reference data
        <host_add> single string with host IP or host name known to the available DNS
        <user> single string with user id to be used to access the host
        <passwd> single string with user password to be used to access the host
        
    Returns:
        (json) =  { 'Total Files': (int),
                    'Files pending backup': (int),
                    'Last Backup': (str)
                    'Days since last backup': (int),
                    'Status': (int), 
                    'Message': (str)}

        Status may be 1=valid data or 0=error in the script
        All keys except "Message" are suppresed when Status=0
        Message describe the error or warning information
"""

import socket
import sys
import json

sys.path.append(
    "C:/Users/Fabio/AppData/Local/Temp/scp31195/root/RF.Fusion/src/zabbix/root/usr/lib/zabbix/externalscripts"
)

import rfFusionLib as rflib

# scritp configuration constants
SERVER_ADD = "192.168.200.30"  # Change this to the server's hostname or IP address
SERVER_PORT = 5555
START_TAG = "<json>"
END_TAG = "</json>"
BUFFER_SIZE = 1024
ENCODING = "utf-8"

# Define default arguments
DEFAULT_HOST_ID = "10"
#DEFAULT_HOST_ADD = "192.168.200.20"
DEFAULT_HOST_ADD = "192.168.10.33"
DEFAULT_HOST_PORT = 22
DEFAULT_USER = "sshUser"  # user should have access to the host with rights to interact with the indexer daemon
DEFAULT_PASSWD = "sshuserpass"
DEFAULT_QUERY_TAG = "backup"
DEFAULT_TIMEOUT = 2

# define arguments as dictionary to associate each argumenbt key to a default value and associated warning messages
ARGUMENTS = {
    "host_id": {
        "set": False,
        "value": DEFAULT_HOST_ID,
        "warning": "Using default host id",
    },
    "host_add": {
        "set": False,
        "value": DEFAULT_HOST_ADD,
        "warning": "Using default host address",
    },
    "host_port": {
        "set": False,
        "value": DEFAULT_HOST_PORT,
        "warning": "Using default host port",
    },    
    "user": {
        "set": False,
        "value": DEFAULT_USER,
        "warning": "Using default user"},
    "passwd": {
        "set": False,
        "value": DEFAULT_PASSWD,
        "warning": "Using default password",
    },
    "query_tag": {
        "set": False,
        "value": DEFAULT_QUERY_TAG,
        "warning": "Using default query tag",
    },
    "timeout": {
        "set": False,
        "value": DEFAULT_TIMEOUT,
        "warning": "Using default timeout",
    },
}


def main():
    # create a warning message object
    wm = rflib.warning_msg()

    # create an argument object
    arg = rflib.argument(wm, ARGUMENTS)

    # parse the command line arguments
    arg.parse(sys.argv)

    # compose the request to the server
    requestS = (
        f'{arg.data["query_tag"]["value"]} '
        f'{arg.data["host_id"]["value"]} '
        f'{arg.data["host_add"]["value"]} '
        f'{arg.data["host_port"]["value"]} '
        f'{arg.data["user"]["value"]} '
        f'{arg.data["passwd"]["value"]}'
    )

    request = bytes(requestS, encoding="utf-8")

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(arg.data["timeout"]["value"])

    try:
        client_socket.connect((SERVER_ADD, SERVER_PORT))
        client_socket.sendall(request)
    except Exception as e:
        print(
            f'{{"Status":0,"Message":"Error: {e}; Could establish socket connection"}}'
        )
        client_socket.close()
        exit()

    try:
        response = client_socket.recv(BUFFER_SIZE)
        client_socket.close()
    except Exception as e:
        print(f'{{"Status":0,"Message":"Error: {e}; Error receiving data"}}')
        client_socket.close()
        exit()

    try:
        response = response.decode(ENCODING)
    except Exception as e:
        print(
            f'{{"Status":0,"Message":"Error: {e}. Error decoding data with {ENCODING}: {response}"}}'
        )
        client_socket.close()
        exit()

    # extract JSON data from bytestring
    start_index = response.lower().rfind(START_TAG)
    end_index = response.lower().rfind(END_TAG)

    # extract JSON data removing the last bracket to later splice with the tail json data from this script
    json_output = response[start_index + len(START_TAG) : end_index]

    try:
        dict_output = json.loads(json_output)

        dict_output["Status"] = 1
        dict_output["Message"] = wm.warning_msg

        print(json.dumps(dict_output))

    except json.JSONDecodeError as e:
        print(
            f'{"Status":0,"Message":"Error: Malformed JSON received. Dumped: {response}"}'
        )


if __name__ == "__main__":
    main()
