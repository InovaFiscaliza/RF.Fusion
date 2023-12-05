#!/usr/bin/python3.9
"""
Use socket to get data from remote ThinkRF digitizer

    Simplified netcat SL command: echo -e "*IDN?\n:STATUS:TEMPERATURE?\n:GNSS:POSITION?\n*OPC\n" | nc -C CWSM211008.anatel.gov.br 37001 > teste.txt
    
    Usage:
        queryDigitizer host_add=<host> port=<port> timeout=<timeout>
    
    Parameters:
        <host> single string with host IP or host name known to the available DNS
        
    Returns:
        (dictionary) =  {'Device': {
                            'Manufacturer': (str),
                            'Model': (str),
                            'Serial': (str),
                            'Firmware': (str),}
                        'Temperature': { values according to digitizer configuration, usually degrees centigrades
                            'RF': (float),
                            'Mixer':(float),
                            'Digital':(float)},
                        'GNSS': {
                            'Latitude':(float), coordinate value in degrees
                            'Longitude':(float), coordinate value in degrees
                            'Altitude':(float)}, coordinate value in meters
                        'Status': (int)}, 1=valid data or 0=invalid information,
"""

import socket
import sys
import json

import rfFusionLib as rflib
import defaultConfig as k

ARGUMENTS = {
    "host_add": {
        "set": False,
        "value": k.DIGI_DEFAULT_HOST,
        "message": "Using default host address"
        },
    "port": {
        "set": False,
        "value": k.DIGI_DEFAULT_PORT,
        "message": "Using default port"
        },
    "timeout": {
        "set": False,
        "value": k.DIGI_DEFAULT_TIMEOUT,
        "message": "Using default timeout"
        },
    "help" : {
        "set": True,
        "value": None,
        "message": "** USAGE: queryDigitizer host_add=<host> port=<port> timeout=<timeout>. See code inline notes for more details **"
        }
    }

def main():
    # create a warning message object
    wm = rflib.warning_msg()

    # create an argument object
    arg = rflib.argument(wm, ARGUMENTS)
    
    # parse the command line arguments
    arg.parse(sys.argv)
        
    host = arg.data["host_add"]["value"]
        
    # Create connection
    try:
        sock = socket.socket(socket.AF_INET, # Internet
                            socket.SOCK_STREAM) # TCP
        sock.settimeout(arg.data['timeout']['value']+k.TIMEOUT_BUFFER)  # Set a timeout of 5 seconds for receiving data    
    except socket.error as e:
        print(f'{{"Status":0,"Error":"Socket error: {e}"}}')
        exit()
    
    # Connect to host
    try:
        sock.connect((arg.data['host_add']['value'], arg.data['port']['value']))
    except socket.error as e:
        print(f'{{"Status":0,"Error":"Could not connecto to {arg.data["host_add"]["value"]} using {arg.data["port"]["value"]}: {e}"}}')
        sock.close()
        exit()

    try:
        # Get Id and Version; Temperature and Position
        sock.sendall(b'*IDN?\r\n')
        device = sock.recv(k.SMALL_BUFFER_SIZE)

        sock.sendall(b':STATUS:TEMPERATURE?\r\n')
        temperature = sock.recv(k.SMALL_BUFFER_SIZE)

        sock.sendall(b':GNSS:POSITION?\r\n')
        position = sock.recv(k.SMALL_BUFFER_SIZE)

        # Close connection
        sock.sendall(b'*OPC\r\n')
        
        sock.close()
    except socket.error as e:
        print(f'{{"Status":0,"Error":"Socket error from {arg.data["host_add"]["value"]} using {arg.data["port"]["value"]}: {e}"}}')
        exit()

    try:
        # Parse answer into dictionary
        texts = device.decode('ascii').strip().split(",")
        values = list(map(float,f"{temperature.decode('ascii')},{position.decode('ascii')}".strip().split(",")))
        outputDict = {'Device': {
                        'Manufacturer': texts[0],
                        'Model': texts[1],
                        'Serial': texts[2],
                        'Firmware': texts[3]},
                    'Temperature': {
                        'RF': values[0],
                        'Mixer':values[1],
                        'Digital':values[2]},
                    'GNSS': {
                        'Latitude':values[3],
                        'Longitude':values[4],
                        'Altitude':values[5]},
                    'Status': 1,
                    'Message': wm.warning_msg}

        print(json.dumps(outputDict))
    except Exception as e:
        print(f'{{"Status":0,"Message":"Error parsing data from {arg.data["host_add"]["value"]} using {arg.data["port"]["value"]}: {e}"}}')
        exit()

if __name__ == "__main__":
    main()
