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

import z_shared as zsh
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
    wm = zsh.warning_msg()

    # create an argument object
    arg = zsh.argument(wm, ARGUMENTS)
    
    # parse the command line arguments
    arg.parse(sys.argv)
        
    # Create connection
    try:
        sock = socket.socket(socket.AF_INET, # Internet
                            socket.SOCK_STREAM) # TCP
        sock.settimeout(arg.data['timeout']['value']+k.TIMEOUT_BUFFER)  # Set a timeout of 5 seconds for receiving data    
    except socket.error as e:
        print(f'{{"status":0,"message":"Socket error: {e}"}}')
        exit()
    
    # Connect to host
    try:
        sock.connect((arg.data['host_add']['value'], arg.data['port']['value']))
    except socket.error as e:
        print(f'{{"status":0,"messsage":"Could not connecto to {arg.data["host_add"]["value"]} using {arg.data["port"]["value"]}: {e}"}}')
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
        print(f'{{"status":0,"message":"Socket error from {arg.data["host_add"]["value"]} using {arg.data["port"]["value"]}: {e}"}}')
        exit()

    try:
        # Parse answer into dictionary
        texts = device.decode('ascii').strip().split(",")
        values = list(map(float,f"{temperature.decode('ascii')},{position.decode('ascii')}".strip().split(",")))
        outputDict = {'device': {
                        'manufacturer': texts[0],
                        'model': texts[1],
                        'serial': texts[2],
                        'firmware': texts[3]},
                    'temperature': {
                        'RF': values[0],
                        'mixer':values[1],
                        'digital':values[2]},
                    'GNSS': {
                        'latitude':values[3],
                        'longitude':values[4],
                        'altitude':values[5]},
                    'status': 1,
                    'message': wm.warning_msg}

        print(json.dumps(outputDict))
    except Exception as e:
        print(f'{{"status":0,"message":"Error parsing data from {arg.data["host_add"]["value"]} using {arg.data["port"]["value"]}: {e}"}}')
        exit()

if __name__ == "__main__":
    main()
