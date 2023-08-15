#!/usr/bin/python3.9
"""
Use socket to get data from remote ThinkRF digitizer

    Simplified netcat SL command: echo -e "*IDN?\n:STATUS:TEMPERATURE?\n:GNSS:POSITION?\n*OPC\n" | nc -C CWSM211008.anatel.gov.br 37001 > teste.txt
    
    Usage:
        queryDigityzer <host>
    
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

# Use standard CWSM port to access digitizer and get host from argument 1
PORT = 37001
BUFFSIZE = 1024


MESSAGE="    USAGE: queryDigityzer <host IP ou name>\n  See code for more details\n"
try:
    host = sys.argv[1]
except:
    print("Error in syntax.")
    print(MESSAGE)
    exit()

HELP=['/h','-h','-help','/help','\help','--help']
if any(host in e for e in HELP):
    print("Use socket to get data from remote ThinkRF digitizer\n")
    print("Simplified netcat SL command:\n    echo -e \"*IDN?\\n:STATUS:TEMPERATURE?\\n:GNSS:POSITION?\\n*OPC\\n\" | nc -C CWSM211008.anatel.gov.br 37001 > teste.txt\n")
    print(MESSAGE)
    exit()
    
try: 
    # Open connection
    s = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    s.connect((host, PORT))

    # Get Id and Version; Temperature and Position
    s.sendall(b'*IDN?\r\n')
    device = s.recv(BUFFSIZE)

    s.sendall(b':STATUS:TEMPERATURE?\r\n')
    temperature = s.recv(BUFFSIZE)

    s.sendall(b':GNSS:POSITION?\r\n')
    position = s.recv(BUFFSIZE)

    # Close connection
    s.sendall(b'*OPC\r\n')

    s.close()

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
                  'Status': 1}

    print(json.dumps(outputDict))
except:
    print("{\"Status\":0}")
