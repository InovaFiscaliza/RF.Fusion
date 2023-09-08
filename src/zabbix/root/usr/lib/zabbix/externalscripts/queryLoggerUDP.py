#!/usr/bin/python3
"""
Use socket to get data from rfeye logger udp stream and count the number of occurrences of words PMEC and PMRD
    
    Usage:
        queryLogger <host> <port> <timeout>
    
    Parameters:
        <host> single string with host IP or host name known to the available DNS
        <port> single string with port number
        <timeout> single string with an integer number of seconds during which the logger stream should continue
        
    Returns:
        (json) =  { 'PMEC': (int),
                    'PMRD': (int),
                    'Status': (int), 
                    'Message': (str)}

        PMEC and PMRD keys are suppresed when Status=0
        Status may be 1=valid data or 0=error in the script
        Message describe the error or warning information
"""

import socket
import time
import sys
import json

import rfFusionLib as rflib

# scritp configuration constants
START_TAG = b"<json>"
END_TAG = b"</json>"
TIMEOUT_BUFFER = 1
BUFFER_SIZE = 65536
ENCODING = "utf-8"

# Define default arguments
# DEFAULT_HOST = "rfeye002080.anatel.gov.br"
DEFAULT_HOST = "172.24.1.13"
DEFAULT_PORT = 5555
DEFAULT_CONNECTION_TIMEOUT = 1

# define arguments as dictionary to associate each argumenbt key to a default value and associated warning messages
ARGUMENTS = {
    "host": {
        "set": False,
        "value": DEFAULT_HOST,
        "warning": "Using default host address"
        },
    "port": {
        "set": False,
        "value": DEFAULT_PORT,
        "warning": "Using default port"
        },
    "timeout": {
        "set": False,
        "value": DEFAULT_CONNECTION_TIMEOUT,
        "warning": "Using default timeout"
        },
    }

def main():
    # create a warning message object
    wm = rflib.warning_msg()

    # create an argument object
    arg = rflib.argument(wm, ARGUMENTS)
    
    # parse the command line arguments
    arg.parse(sys.argv)
    
    # get the arguments from the argument object
    connection_request = bytes(f"open {arg.data['timeout']['value']}", encoding="utf-8")

    # Create connection
    try:
        sock = socket.socket(socket.AF_INET, # Internet
                            socket.SOCK_DGRAM) # UDP
        sock.settimeout(arg.data['timeout']['value']+TIMEOUT_BUFFER)  # Set a timeout of 5 seconds for receiving data    
    except socket.error as e:
        print(f'{{"Status":0,"Error":"Socket error: {e}"}}')
        exit()

    # Send handshake
    try:
        sock.sendto(connection_request, (arg.data['host']['value'], arg.data['port']['value']))
    except socket.error as e:
        print(f'{{"Status":0,"Error":"Handshake error: {e}"}}')
        sock.close()
        exit()

    # Initialize data buffer
    raw_data = bytearray(b'')

    # Get data during the timeout period
    t_end = time.time() + arg.data['timeout']['value']
    try:
        while time.time() < t_end:
            # Get data from UDP
            try:
                dataFromUDP, addr = sock.recvfrom(BUFFER_SIZE)  # Buffer size is 65536 bytes
            except socket.timeout:
                print('{"Status":0,"Error":"Timeout without data received"}')
                exit()

            # Transform binary object into bytearray
            dataByteArray = bytearray(dataFromUDP)

            # Remove initial 8 bytes and merge with the raw data vector
            raw_data.extend(dataByteArray[8:])

    except Exception as e:
        print('{"Status":0,"Message":"Error receiving and processing data"}')

    # extract JSON data from bytestring
    start_index = raw_data.lower().rfind(START_TAG)
    end_index = raw_data.lower().rfind(END_TAG)

    # extract JSON data removing the last bracket to later splice with the tail json data from this script
    json_output = raw_data[start_index+len(START_TAG):end_index]
    
    json_output = json_output.decode(ENCODING)

    try:
        dict_output = json.loads(json_output)

        dict_output["Status"] = 1
        dict_output["Message"] = wm.warning_msg

        print(json.dumps(dict_output))

    except json.JSONDecodeError as e:
        print(f'{"Status":0,"Message":"Error: Malformed JSON received. Dumped: {json_output}"}')

if __name__ == "__main__":
    main()
