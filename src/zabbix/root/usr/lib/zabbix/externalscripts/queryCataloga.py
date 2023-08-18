#!/usr/bin/python3
"""
Call for information and backup from an specific host by appCataloga service.

Provide feedback to Zabbix about the host.

This script is unsecure and should only run through a secure encripted network connection
    
    Usage:
        queryCataloga <host> <user> <pass>
    
    Parameters:
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

SERVER_ADD = "192.168.200.10"  # Change this to the server's hostname or IP address
SERVER_PORT = 5555

user = "your_username"
passwd = "your_password"

# Constants
DEFAULT_HOST_ID = "rfeye002080"
DEFAULT_HOST_ADD = "rfeye002080.anatel.gov.br"
DEFAULT_USER = "user"
DEFAULT_PASSWD = "password"
TIMEOUT_BUFFER = 1
QUERY_TAG = "query"
START_TAG = "<json>"
END_TAG = "</json>"

warning_msg = "none"

def parse_call():
    # Get command-line arguments
    try:
        e = sys.argv[5]
    except IndexError:
        warning_msg = "Warning: Ignoring arguments"
    except ValueError:
        print('{"Status":0,"Error":"Invalid function call"}')
        exit()

    try:
        host_id = sys.argv[1]
    except IndexError:
        host_id = DEFAULT_HOST_ID
        warning_msg = "Warning: Using default values"
    except ValueError:
        print('{"Status":0,"Error":"Invalid function call"}')
        exit()

    try:
        host_add = int(sys.argv[2])
    except IndexError:
        host_add = DEFAULT_HOST_ADD
        warning_msg = "Warning: Using default values"
    except ValueError:
        print('{"Status":0,"Error":"Invalid function call"}')
        exit()

    try:
        user = int(sys.argv[3])
    except IndexError:
        user = DEFAULT_USER
        warning_msg = "Warning: Using default values"
    except ValueError:
        print('{"Status":0,"Error":"Invalid function call"}')
        exit()

    try:
        passwd = int(sys.argv[4])
    except IndexError:
        passwd = DEFAULT_PASSWD
        warning_msg = "Warning: Using default values"
    except ValueError:
        print('{"Status":0,"Error":"Invalid function call"}')
        exit()

    req_to_server = bytes(f"query {host_id} {host_add} {user} {passwd}", encoding="utf-8")
    
    return(req_to_server)

def main():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((SERVER_ADD, SERVER_PORT))

    req_to_server = parse_call()
    
    try:
        client_socket.sendall(req_to_server)

        response = client_socket.recv(1024).decode("utf-8")
    except Exception as e:
        print("Error:", e)

    finally:
        client_socket.close()

    # extract JSON data from bytestring
    start_index = response.rfind(START_TAG)
    end_index = response.rfind(END_TAG)

    # extract JSON data removing the last bracket to later splice with the tail json data from this script
    json_output = response[start_index+len(START_TAG):end_index]
    
    try:
        dict_output = json.loads(json_output)
        
        if warning_msg != "none":
            if dict_output["Status"]==1:
                dict_output["Message"]=f'{dict_output["Message"]}. {warning_msg}'
            elif dict_output["Status"]==0:
                dict_output["Message"]=warning_msg
            print(json.dumps(dict_output))
        else:
            print(json_output)
                
    except json.JSONDecodeError as e:
        print('{"Status":0,"Message":"Malformed JSON data, check UDP generation from remote server and timeout settings"}')    

if __name__ == "__main__":
    main()