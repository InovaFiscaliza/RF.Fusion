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

from email import message
import socket
import sys
import json
import re

sys.path.append(
    "C:/Users/Fabio/AppData/Local/Temp/scp31195/root/RF.Fusion/src/zabbix/root/usr/lib/zabbix/externalscripts"
)



def hide_sensitive_data(request: str, forbidden: list) -> str:
    """Replace sensitive data in the request list with asterisks

    Args:
        request: String to be sanitized
        forbidden: List of forbidden strings to be sanitized

    Returns:
        list: sanitized string
    """

    for f in forbidden:
        request = re.sub(f, "*****", request)

    return request


def main():
    message = {
        "Key": 12345,
        "ClientName": "Matlab",
        "Request": {
            "type": "FileRead",
            "filepath": "/mnt/reposfi/RF.Fusion_Processado/CWSM211001/"
                    "CWSM21100001_E1_A1_Spec Frq=71.000 Span=34.000 "
                    "RBW=100.00000 [2025-09-01,10-14-05-755-3553].dBm"
        }
    }

    json_message = json.dumps(message, ensure_ascii=False)

    # adiciona CR/LF no final
    json_message_crlf = json_message + "\r\n"
  
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(10)

    try:
        ACAT_SERVER_ADD = "10.88.0.2"  # Change this to the server's hostname or IP address
        ACAT_SERVER_PORT = 5555
        ACAT_SERVER_TIMEOUT = 1000
        START_TAG = b"<json>" # used by queryLoggerUDP.py, queryCataloga.py
        END_TAG = b"</json>" # used by queryLoggerUDP.py, queryCataloga.py
        SMALL_BUFFER_SIZE = 2048 # used by queryCataloga.py, queryDigitizer.py
        MID_BUFFER_SIZE = 16384 # used by queryappColeta.py
        LARGE_BUFFER_SIZE = 65536 # used by queryLoggerUDP.py
        TIMEOUT_BUFFER = 1 # used by queryLoggerUDP.py and quryDigitizer.py. Additional time after timeout to wait for data to be received
        UTF_ENCODING = "utf-8" # used by queryCataloga.py
        ISO_ENCODING = "ISO-8859-1" # used by queryappColeta.py
        
        client_socket.connect((ACAT_SERVER_ADD, ACAT_SERVER_PORT))
        request = bytes(json_message_crlf, encoding="utf-8")
        client_socket.sendall(request)
    except Exception as e:
        print(
            f'{{"status_query":0,"message_query":"Error: {e}; Could establish socket connection"}}'
        )
        client_socket.close()
        exit()

    try:
        response = client_socket.recv(SMALL_BUFFER_SIZE)
        client_socket.close()
    except Exception as e:
        print(
            f'{{"status_query":0,"message_query":"Error: {e}; Error receiving data"}}'
        )
        client_socket.close()
        exit()

    try:
        response = response.decode(UTF_ENCODING)
    except Exception as e:
        print(
            f'{{"status_query":0,"message_query":"Error: {e}. Error decoding data with {UTF_ENCODING}: {response}"}}'
        )
        client_socket.close()
        exit()

    # extract JSON data from bytestring
    start_index = response.lower().rfind(START_TAG.decode())
    end_index = response.lower().rfind(END_TAG.decode())

    # extract JSON data removing the last bracket to later splice with the tail json data from this script
    json_output = response[start_index + len(START_TAG) : end_index]

    try:
        dict_output = json.loads(json_output)

        dict_output["request"] = hide_sensitive_data(
            request=json_message_crlf,
        )

        dict_output["status_query"] = 1
        print(json.dumps(dict_output))

    except json.JSONDecodeError as e:
        print(
            f'{"status_query":0,"message_query":"Error: Malformed JSON received. Dumped: {response}"}'
        )


if __name__ == "__main__":
    main()
