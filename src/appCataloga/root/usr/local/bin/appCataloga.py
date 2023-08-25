#!/usr/bin/env python
"""
Listen to socket command to perform backup from a specific host and retuns the current status for said host.
    
    Usage:
        appCataloga 
    
    Parameters: <via socket connection>
        <hostid> single string with unique hostid
        <host_add> single string with host IP or host name known to the available DNS
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
# Set system path to include modules from /etc/appCataloga
import sys
sys.path.append('/etc/appCataloga')

# Import standard libraries.
import socket
import json
import signal
from selectors import DefaultSelector, EVENT_READ

# Import modules for file processing 
import config as k
import dbHandler as dbh
import appCShared as csh

#! TEST ONLY host_statistics initialization
HOST_STATISTICS = { "Total Files":1,
                    "Files pending backup":0,
                    "Files pending processing":0,
                    "Last Backup":"today",
                    "Last Processing":"today",
                    "Days since last backup":0}

# initialize warning message variable
wm = csh.warning_msg()

interrupt_read, interrupt_write = socket.socketpair()

def handler(signum, frame):
    """Handle interrupt signal

    Usage:
        handler(signum, frame)
    
    Parameters:
        <signum>: signal number
        <frame>: current stack frame (None or a frame object
        
    Returns:
        <void>
    """
    print('Signal handler called with signal', signum)
    interrupt_write.send(b'\0')

# start signal handler that control a graceful shutdown 
signal.signal(signal.SIGINT, handler)

def backup_queue(conn="ClientIP",hostid="1",host_addr="host_addr",host_user="user",host_passwd="passwd"):
    """Add host to backup queue and return current status

    Args:
        conn (str): _description_. Defaults to "ClientIP".
        hostid (str): _description_. Defaults to "1".
        host_addr (str): _description_. Defaults to "host_addr".
        host_user (str): _description_. Defaults to "user".
        host_passwd (str): _description_. Defaults to "passwd".

    Returns:
        void
    """
    
    # create db object using databaseHandler class
    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME)
     
    # add host to db task list for backup
    db.addHost(hostid,host_addr,host_user,host_passwd)
    
    host_stat = db.getHost(hostid)
    
    print(host_stat)
    # get from db the backup summary status for the host_id
    return host_stat

def serve_client(client_socket):
    try:
        while True:
            data = client_socket.recv(128)
            if not data:
                break
            
            host = data.decode().split(" ")
            
            if host[0]==k.CATALOG_QUERY_TAG:
                host[0]=client_socket.getpeername() # replace list first element with client IP address
                
                host_statistics = backup_queue(*host) # unpack list to pass as arguments to backup_queue
                
                stat_txt = json.dumps(host_statistics)[:-1]

                response = f'{k.START_TAG}{stat_txt},"Status":1,"Message":"{wm.warning_msg}"}}{k.END_TAG}'

            else:
                print(f"Ignored data from from {client_socket.getpeername()[0]}. Received: {data.decode()}")
                
                response = f'{k.START_TAG}{{"Status":0,"Error":"host command not recognized"}}{k.END_TAG}'
                
            byte_response = bytes(response, encoding="utf-8")
            
            client_socket.sendall(byte_response)
            
    except Exception as e:
        print("Error:", e)
    finally:
        client_socket.close()

def serve_forever(server_socket):
    sel = DefaultSelector()
    sel.register(interrupt_read, EVENT_READ)
    sel.register(server_socket, EVENT_READ)

    while True:
        for key, _ in sel.select():
            if key.fileobj == interrupt_read:
                interrupt_read.recv(1)
                return
            if key.fileobj == server_socket:
                client_socket, client_address = server_socket.accept()
                print("Connection established with:", client_address)
                serve_client(client_socket)

def main():
    
    print(f"Server is listening on port {k.SERVER_PORT}")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('', k.SERVER_PORT)
    server_socket.bind(server_address)
    server_socket.listen(k.TOTAL_CONNECTIONS)

    serve_forever(server_socket)

    print("Shutdown...")
    server_socket.close()

if __name__ == "__main__":
    main()