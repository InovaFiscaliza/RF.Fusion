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

# Constants
DEFAULT_UDP_IP = "rfeye002080.anatel.gov.br"
DEFAULT_UDP_PORT = 5555
DEFAULT_CONNECTION_TIMEOUT = 1
TIMEOUT_BUFFER = 1
START_TAG = b"<json>"
END_TAG = b"</json>"

warning = "none"
# Get command-line arguments
try:
    e = sys.argv[4]
except IndexError:
    warning = "Warning: Ignoring arguments"
except ValueError:
    print('{"Status":0,"Error":"Invalid function call"}')
    exit()

try:
    udp_ip = sys.argv[1]
except IndexError:
    udp_ip = DEFAULT_UDP_IP
    warning = "Warning: Using default values"
except ValueError:
    print('{"Status":0,"Error":"Invalid function call"}')
    exit()

try:
    udp_port = int(sys.argv[2])
except IndexError:
    udp_port = DEFAULT_UDP_PORT
    warning = "Warning: Using default values"
except ValueError:
    print('{"Status":0,"Error":"Invalid function call"}')
    exit()

try:
    timeout = int(sys.argv[3])
except IndexError:
    timeout = DEFAULT_CONNECTION_TIMEOUT
    warning = "Warning: Using default values"
except ValueError:
    print('{"Status":0,"Error":"Invalid function call"}')
    exit()

connection_request = bytes(f"open {timeout}", encoding="utf-8")

# Create connection
try:
    sock = socket.socket(socket.AF_INET, # Internet
                         socket.SOCK_DGRAM) # UDP
    sock.settimeout(timeout+TIMEOUT_BUFFER)  # Set a timeout of 5 seconds for receiving data    
except socket.error as e:
    print(f'{{"Status":0,"Error":"Socket error: {e}"}}')
    exit()

# Send handshake
try:
    sock.sendto(connection_request, (udp_ip, udp_port))
except socket.error as e:
    print(f'{{"Status":0,"Error":"Handshake error: {e}"}}')
    sock.close()
    exit()

# Initialize data buffer
raw_data = bytearray(b'')

# Get data for 10 seconds
t_end = time.time() + timeout
try:
    while time.time() < t_end:
        # Get data from UDP
        try:
            dataFromUDP, addr = sock.recvfrom(65536)  # Buffer size is 65536 bytes
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
json_data = raw_data[start_index+len(START_TAG):end_index-1].decode('utf8')
    
json_data = f'{json_data},"Status":1,"Message":"{warning}"}}'

try:
    json_dict = json.loads(json_data)
    print(f"{json_data}")
except json.JSONDecodeError as e:
    print('{"Status":0,"Message":"Malformed JSON data, check UDP generation from remote server and timeout settings"}')    
