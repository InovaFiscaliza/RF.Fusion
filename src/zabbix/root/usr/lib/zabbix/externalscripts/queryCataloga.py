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

SERVER_ADD = "192.168.200.30"  # Change this to the server's hostname or IP address
SERVER_PORT = 5555

user = "your_username"
passwd = "your_password"

# Constants
DEFAULT_HOST_ID = "10"
DEFAULT_HOST_ADD = "rfeye002080.anatel.gov.br"
DEFAULT_USER = "rfeye_user"
DEFAULT_PASSWD = "password"
DEFAULT_QUERY_TAG = "backup"
MAXIMUM_ARGUMENTS = 5
TIMEOUT_BUFFER = 1
START_TAG = "<json>"
END_TAG = "</json>"

NO_WARNING_MSG = "none"


class warning_msg:
    def __init__(self) -> None:
        self.warning_msg = NO_WARNING_MSG

    def compose_warning(self, new_warning):
        if self.warning_msg == NO_WARNING_MSG:
            self.warning_msg = f"Warning: {new_warning}"
        else:
            self.warning_msg = f"{self.warning_msg}, {new_warning}"


wm = warning_msg()


def parse_call():
    """Get command-line arguments"""

    try:
        e = sys.argv[6]

    except IndexError:
        ignored_arg = sys.argv.__len__() - MAXIMUM_ARGUMENTS
        wm.compose_warning(
            "Ignoring {ignored_arg} argument(s) beyond the expected {MAXIMUM_ARGUMENTS} argument"
        )

    except ValueError:
        print('{"Status":0,"Error":"Invalid function call"}')
        exit()

    try:
        query_tag = sys.argv[1]

    except IndexError:
        query_tag = DEFAULT_QUERY_TAG
        wm.compose_warning("Using default query tag")

    except ValueError:
        print('{"Status":0,"Error":"Invalid function call"}')
        exit()

    try:
        host_id = sys.argv[2]

    except IndexError:
        host_id = DEFAULT_HOST_ID
        wm.compose_warning("Using default host id")

    except ValueError:
        print('{"Status":0,"Error":"Invalid function call"}')
        exit()

    try:
        host_add = int(sys.argv[3])

    except IndexError:
        host_add = DEFAULT_HOST_ADD
        wm.compose_warning("Using default host address")

    except ValueError:
        print('{"Status":0,"Error":"Invalid function call"}')
        exit()

    try:
        user = int(sys.argv[4])

    except IndexError:
        user = DEFAULT_USER
        wm.compose_warning("Using default user")

    except ValueError:
        print('{"Status":0,"Error":"Invalid function call"}')
        exit()

    try:
        passwd = int(sys.argv[5])
    except IndexError:
        passwd = DEFAULT_PASSWD
        wm.compose_warning("Using default password")

    except ValueError:
        print('{"Status":0,"Error":"Invalid function call"}')
        exit()

    req_to_server = bytes(
        f"{query_tag} {host_id} {host_add} {user} {passwd}", encoding="utf-8"
    )

    return req_to_server


def main():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        client_socket.connect((SERVER_ADD, SERVER_PORT))
    except Exception as e:
        print(f'{{"Status":0,"Message":"Error: {e}"}}')

    req_to_server = parse_call()

    try:
        client_socket.sendall(req_to_server)

        response = client_socket.recv(1024).decode("utf-8")
    except Exception as e:
        print(f'{{"Status":0,"Message":"Error: {e}"}}')

    finally:
        client_socket.close()

    # extract JSON data from bytestring
    start_index = response.lower().rfind(START_TAG)
    end_index = response.lower().rfind(END_TAG)

    # extract JSON data removing the last bracket to later splice with the tail json data from this script
    json_output = response[start_index + len(START_TAG) : end_index]

    try:
        dict_output = json.loads(json_output)

        if warning_msg != "none":
            if dict_output["Status"] == 1:
                dict_output["Message"] = f'{dict_output["Message"]}. {wm.warning_msg}'
            elif dict_output["Status"] == 0:
                dict_output["Message"] = wm.warning_msg
            print(json.dumps(dict_output))
        else:
            print(json_output)

    except json.JSONDecodeError as e:
        print(
            '{"Status":0,"Message":"Malformed JSON data, check UDP generation from remote server and timeout settings"}'
        )


if __name__ == "__main__":
    main()
