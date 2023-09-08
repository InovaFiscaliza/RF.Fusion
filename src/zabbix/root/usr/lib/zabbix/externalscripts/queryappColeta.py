#!/usr/bin/python3
"""
Use socket to get data from appColeta TCP stream
    
    Usage:
        queryappColeta host=<host> port=<port> key=<key> ClientName=<ClientName> query=<query> timeout=<timeout>
    
    Parameters:
        <host> single string with host IP or host name known to the available DNS
        <port> single string with port number
        <key> single string with host key to access the service
        <ClientName> single string with the name of the client requesting the service
        <query> single string with the query to be sent to appColeta
        <timeout> single string with an integer number of seconds during which the logger stream should continue

    appCataloga accepts the following queries:
        PositionList
        TaskList

        '{"Key":"123456","ClientName":"Zabbix","Request":"PositionList"}'

    Returns:
        (json) =  { <appColeta answer>: (json),
                    'Status': (int), 
                    'Message': (str)}
"""
import socket
import sys
import json
import rfFusionLib as rflib

# scritp configuration constants
START_TAG = "<json>"
END_TAG = "</json>"
BUFFER_SIZE = 2048

# Define default arguments
# DEFAULT_HOST_ADD = "172.24.5.71" # MG
DEFAULT_HOST_ADD = "172.24.5.33" # ES
DEFAULT_PORT = 8910
DEFAULT_KEY = "123456"  # user should have access to the host with rights to interact with the indexer daemon
DEFAULT_CLIENT_NAME = "Zabbix"
# DEFAULT_QUERY_TAG = "PositionList" 
DEFAULT_QUERY_TAG = "Diagnostic"
# DEFAULT_QUERY_TAG = "TaskList"
DEFAULT_TIMEOUT = 2

# define arguments as dictionary to associate each argumenbt key to a default value and associated warning messages
ARGUMENTS = {
    "host": {
        "set": False,
        "value": DEFAULT_HOST_ADD,
        "warning": "Using default host address"
        },
    "port": {
        "set": False,
        "value": DEFAULT_PORT,
        "warning": "Using default port"
        },
    "key": {
        "set": False,
        "value": DEFAULT_KEY,
        "warning": "Using default key"
        },
    "ClientName": {
        "set": False,
        "value": DEFAULT_CLIENT_NAME,
        "warning": "Using default ClientName"
        },
    "query": {
        "set": False,
        "value": DEFAULT_QUERY_TAG,
        "warning": "Using default query tag"
        },
    "timeout": {
        "set": False,
        "value": DEFAULT_TIMEOUT,
        "warning": "Using default timeout"
        }
    }

def main():
    
    # create a warning message object
    wm = rflib.warning_msg()

    # create an argument object
    arg = rflib.argument(wm, ARGUMENTS)
    
    # parse the command line arguments
    arg.parse(sys.argv)
    
    # compose the request to the server
    request_dict = {
        "key" : arg.data["key"]["value"],
        "ClientName" : arg.data["ClientName"]["value"],
        "Request" : arg.data["query"]["value"]
        }
    
    # create a request string in byte format using the json dump from 
    request = bytes(f"{json.dumps(request_dict)}\r\n", encoding="utf-8")

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(arg.data["timeout"]["value"])
    
    try:
        client_socket.connect((arg.data["host"]["value"], int(arg.data["port"]["value"])))
    except Exception as e:
        print(f'{{"Status":0,"Message":"Error: {e}"}}')
        exit()

    try:
        client_socket.sendall(request)
        response = client_socket.recv(BUFFER_SIZE).decode("utf-8")
        client_socket.close()
        
    except Exception as e:
        print(f'{{"Status":0,"Message":"Error: {e}"}}')
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
        print(f'{"Status":0,"Message":"Error: Malformed JSON received. Dumped: {response}"}')

if __name__ == "__main__":
    main()
