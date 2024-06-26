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
                    'status': (int), 
                    'message': (str)}
"""
import socket
import sys
import json

import z_shared as zsh
import defaultConfig as k

# define arguments as dictionary to associate each argumenbt key to a default value and associated warning messages
ARGUMENTS = {
    "host": {
        "set": False,
        "value": k.ACOL_DEFAULT_HOST_ADD,
        "message": "Using default host address"
        },
    "port": {
        "set": False,
        "value": k.ACOL_DEFAULT_PORT,
        "message": "Using default port"
        },
    "key": {
        "set": False,
        "value": k.ACOL_DEFAULT_KEY,
        "message": "Using default key"
        },
    "ClientName": {
        "set": False,
        "value": k.ACOL_DEFAULT_CLIENT_NAME,
        "message": "Using default ClientName"
        },
    "query": {
        "set": False,
        "value": k.ACOL_DEFAULT_QUERY_TAG,
        "message": "Using default query tag"
        },
    "timeout": {
        "set": False,
        "value": k.ACOL_DEFAULT_TIMEOUT,
        "message": "Using default timeout"
        },
    "help" : {
        "set": True,
        "value": None,
        "message": "** USAGE: queryappColeta queryappColeta host=<host> port=<port> key=<key> ClientName=<ClientName> query=<query> timeout=<timeout>. See code inline notes for more details **"
        }
    }

def summarize_diagnostic(appCol_dict):
    # TODO: #19 implement this function according to the new appColeta diagnostic format
    """Include summary in the JSON data received from appColeta"""
    # count the number of peaks in each band
    for band in appCol_dict["Answer"]["taskList"]:
        try:
            band["nPeaks"] = len(band["Mask"]["Peaks"])
        except:
            band["nPeaks"] = 0
        
        try:
            band["name"] = f"{int(band['FreqStart']/1e6)}-{int(band['FreqStop']/1e6)}MHz"
        except:
            band["name"] = "unknown"
                
    return appCol_dict

def summarize_task(appCol_dict:dict) -> dict:
    """Count the number of peaks in each band and define a friendly name.

    Args:
        appCol_dict (dict): JSON data received from appColeta

    Returns:
        dict: _description_
    """

    def _summarize_band(band:dict) -> dict:
        
        # count the number of peaks in each band if the band has a mask and associated peaks
        try:
            band["nPeaks"] = len(band["Mask"]["Peaks"])
        except:
            band["nPeaks"] = 0
        
        # define a friendly name for the band if it has a valid frequency range
        try:
            band["name"] = f"{int(band['FreqStart']/1e6)}-{int(band['FreqStop']/1e6)}MHz"
        except:
            band["name"] = "unknown"

    def _summarize_task(task:dict) -> dict:
        
        try:
            # try to summarize a single band and convert it to a list
            task["Band"] = [_summarize_band(task["Band"])]
        except TypeError:
            # if it is already a list, summarize each band
            for band in task["Band"]:
                _summarize_band(band)
    
        return task
    
    try :
        # try to summarize a single task and convert it to a list
        appCol_dict["Answer"]["taskList"] = [_summarize_task(appCol_dict["Answer"]["taskList"])]
    except TypeError:
        # if it is already a list, summarize each task
        for task in appCol_dict["Answer"]["taskList"]:
            task = _summarize_task(task)
        
    return appCol_dict

def main():
    
    # create a warning message object
    wm = zsh.warning_msg()

    # create an argument object
    arg = zsh.argument(wm, ARGUMENTS)
    
    # parse the command line arguments
    arg.parse(sys.argv)
    
    # compose the request to the server
    request_dict = {
        "Key" : arg.data["key"]["value"],
        "ClientName" : arg.data["ClientName"]["value"],
        "Request" : arg.data["query"]["value"]
        }
    
    # create a request string in byte format using the json dump from 
    request = bytes(f"{json.dumps(request_dict)}\r\n", encoding="utf-8")

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.settimeout(arg.data["timeout"]["value"])
    
    # connect to the server
    try:
        client_socket.connect((arg.data["host"]["value"], int(arg.data["port"]["value"])))
    except Exception as e:
        print(f'{{"status":0,"message":"Error: {e}"}}')
        exit()

    # send the request to the server
    try:
        client_socket.sendall(request)
    except Exception as e:
        print(f'{{"status":0,"message":"Error during request: {e}"}}')
        client_socket.close()
        exit()

    # receive the response from the server
    json_data_rcv = zsh.receive_message(  client_socket=client_socket,
                                            encoding=k.ISO_ENCODING,
                                            buffer_size=k.MID_BUFFER_SIZE,
                                            start_tag=k.START_TAG.decode(k.ISO_ENCODING),
                                            end_tag=k.END_TAG.decode(k.ISO_ENCODING),
                                            timeout=arg.data["timeout"]["value"])

    try:
        dict_output = json.loads(json_data_rcv)
        
        dict_output["status"] = 1
        dict_output["message"] = wm.warning_msg
    except json.JSONDecodeError as e:
        print(f'{"status":0,"message":"Error: Malformed JSON received. Dumped: {json_data_rcv}"}')

    if arg.data["query"]["value"] == "Diagnostic":
        dict_output = summarize_diagnostic(dict_output)
    elif arg.data["query"]["value"] == "TaskList":
        dict_output = summarize_task(dict_output)
        
    print(json.dumps(dict_output))
    
if __name__ == "__main__":
    main()
