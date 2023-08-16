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
        <pass> single string with user password to be used to access the host
        
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
import time
import sys
import json

# Constants
UDP_IP = "localhost"
UDP_PORT = 5555
CONNECTION_TIMEOUT = 1
START_TAG = b"<json>"
END_TAG = b"</json>"

# Constants
DEFAULT_HOST = "rfeye002080.anatel.gov.br"
DEFAULT_USER = "user"
DEFAULT_PASS = "pass"

# initialize warnig variable
warning = ""

# Get command-line arguments
try:
    e = sys.argv[4]
except IndexError:
    warning = "Warning: Ignoring arguments"
except ValueError:
    print('{"Status":0,"Error":"Invalid function call"}')
    exit()

try:
    host = sys.argv[1]
except IndexError:
    host = DEFAULT_HOST
    warning = "Warning: Using default values"
except ValueError:
    print('{"Status":0,"Error":"Invalid function call"}')
    exit()

try:
    user = int(sys.argv[2])
except IndexError:
    user = DEFAULT_USER
    warning = "Warning: Using default values"
except ValueError:
    print('{"Status":0,"Error":"Invalid function call"}')
    exit()

try:
    passwd = int(sys.argv[3])
except IndexError:
    passwd = DEFAULT_PASS
    warning = "Warning: Using default values"
except ValueError:
    print('{"Status":0,"Error":"Invalid function call"}')
    exit()

connection_request = bytes(f"query {host} {user} {passwd}", encoding="utf-8")

# Create connection
try:
    sock = socket.socket(socket.AF_INET, # Internet
                         socket.SOCK_STREAM) # UDP
    sock.settimeout(CONNECTION_TIMEOUT)  # Set a timeout of 5 seconds for receiving data    
except socket.error as e:
    print(f'{{"Status":0,"Error":"Socket error: {e}"}}')
    exit()

# Send handshake
try:
    sock.sendto(connection_request, (UDP_IP, UDP_PORT))
except socket.error as e:
    print(f'{{"Status":0,"Error":"Handshake error: {e}"}}')
    sock.close()
    exit()

# Initialize data buffer
raw_data = bytearray(b'')

# Get data for 10 seconds
t_end = time.time() + CONNECTION_TIMEOUT
try:
    while time.time() < t_end:
        # Get data from UDP
        try:
            data, addr = sock.recvfrom(65536)  # Buffer size is 65536 bytes
        except socket.timeout:
            print('{"Status":0,"Error":"Timeout without data received"}')
            exit()

        # Transform binary object into bytearray
        dataByteArray = bytearray(data)

        # Remove initial 8 bytes and merge with the raw data vector
        raw_data.extend(dataByteArray[8:])

except Exception as e:
    print('{"Status":0,"Message":"Error receiving and processing data"}')

# extract JSON data from bytestring
start_index = raw_data.rfind(START_TAG)
end_index = raw_data.rfind(END_TAG)

# extract JSON data removing the last bracket to later splice with the tail json data from this script
json_data = raw_data[start_index+len(START_TAG):end_index-1].decode('utf8')

try:
    json_dict = json.loads(json_data)
    print(f"{json_data}")
except json.JSONDecodeError as e:
    print('{"Status":0,"Message":"Malformed JSON data, check UDP generation from remote server and timeout settings"}')    
