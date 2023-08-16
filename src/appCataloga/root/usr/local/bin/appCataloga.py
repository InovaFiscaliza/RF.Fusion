#!/usr/bin/python3
"""
Listen to socket command to perform backup from a specific host and retuns the current status for said host.
    
    Usage:
        appCataloga
    
    Parameters:
        <host> single string with host IP or host name known to the available DNS
        <user> single string with user id to be used to access the host
        <pass> single string with user password to be used to access the host
        
    Returns:
        (json) =  { 'Total Files': (int),
                    'Files to backup': (int),
                    'Last Backup data': (str)
                    'Days since last backup': (int),
                    'Status': (int), 
                    'Message': (str)}

        Status may be 1=valid data or 0=error in the script
        All keys except "Message" are suppresed when Status=0
        Message describe the error or warning information
"""

# Python program to implement server side of chat room.
import socket
import select
import sys
from _thread import *
 
#constants
SERVER_IP = "rhzbefipdin01.anatel.gov.br"
SERVER_PORT = "5555"
QUERY_TAG = "query"

# initialize warning message variable
warning_msg = ""

#! TEST host_statistics initialization
host_statistics = { "Total Files":1,
                    "Files pending backup":1,
                    "Last Backup":"today",
                    "Days since last backup":0}

# set connection
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
 
server.bind((SERVER_IP, SERVER_PORT))
 
server.listen(500)
 
host_list = []
 
def backup_thread(host=["conn","host_id","host_add","user","passwd"]):
    #nothing
    message = "" 

def get_host_statistics():
    #nothing
    message = ""

while True:
 
    # receive connections
    conn, addr = server.accept()
 
    # read initial message from host
    bin_data = conn.recv(256)

    # parse initial message
    bytearray_data = bytearray(bin_data, encoding="utf-8")
    
    # get a list with 4 strings: ["query",<host_id>,<host_add>,<user>,<passwd>]
    host = bytearray_data.decode().split(' ')
    
    # if first field corresponds to the expected tag, process host indicated
    if host[1] == QUERY_TAG:

        start_new_thread(backup_thread,host)
        
        host_statistics = get_host_statistics(host)
        # answer positive
        message = f'<json>{{"Total Files":{host_statistics["Total Files"]},
                    "Files pending backup":{host_statistics["Files pending backup"]},
                    "Last Backup":{host_statistics["Last Backup"]},
                    "Days since last backup":{host_statistics["Days since last backup"},
                    "Status":0,
                    "Message":{warning_msg}}}</json>"
    else:
        # answer negative
        message = '<json>{"Status":0,"Message":"Message corrupted or in unrecognized format"}</json>'

    conn.send(message)
    conn.close()

server.close()
