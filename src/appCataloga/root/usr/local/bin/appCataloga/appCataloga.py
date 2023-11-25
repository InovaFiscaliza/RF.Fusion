#!/usr/bin/env python
"""
Listen to socket command to perform backup from a specific host and retuns the current status for said host.
Keep the backup process running in a separate process and restart it if it fails.
    
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
import time

import subprocess

# Import modules for file processing 
import config as k
import db_handler as dbh

#! TEST ONLY host_statistics initialization remove for production
HOST_STATISTICS = { "Total Files":1,
                    "Files pending backup":0,
                    "Files pending processing":0,
                    "Last Backup":"today",
                    "Last Processing":"today",
                    "Days since last backup":0}

interrupt_read, interrupt_write = socket.socketpair()

def handler(signum, frame):
    """Handle interrupt signal from keyboard

    Usage:
        handler(signum, frame)
    
    Parameters:
        <signum>: signal number
        <frame>: current stack frame (None or a frame object
        
    Returns:
        None
    """
    print('Signal handler called with signal', signum)
    interrupt_write.send(b'\0')

# start signal handler that control a graceful shutdown 
signal.signal(signal.SIGINT, handler)

def backup_queue(   conn:str,
                    hostid:str,
                    host_uid:str,
                    host_addr:str,
                    host_port:str,
                    host_user:str,
                    host_passwd:str):
    """Add host to backup queue in the database and return current status

    Args:
        conn (str): Socket connection object. Defaults to "ClientIP".
        hostid (str): Target host id. Used as PK in the database host table.
        host_uid (str): Unique physical identifier to the host.
        host_addr (str): IP address or DNS to the host to be contacted.
        host_port (str): SSH port to be used to connect to the host.
        host_user (str): Host user for the SSH connection.
        host_passwd (str): Host password for the SSH connection.

    Returns:
        dict: Dictionary with the current status for the hostid
    """
    
    db = dbh.dbHandler(database=k.BKP_DATABASE_NAME)
     
    db.add_backup_task(hostid=hostid,
                       host_uid=host_uid,
                       host_addr=host_addr,
                       host_port=host_port,
                       host_user=host_user,
                       host_passwd=host_passwd)
    
    host_stat = db.get_host_task_status(hostid)
    
    return host_stat

def serve_client(client_socket):
    
    receiving_data = True
    
    while receiving_data:
        
        try:
            data = client_socket.recv(128)
        except Exception as e:
            print("Error receiving data:", e)
            data = None

        # in case of error or no data received
        if not data:
            receiving_data = False
            
        else:
            
            try:
                host = data.decode().split(" ")
            except Exception as e:
                print("Error decoding data:", e)
                host = [None]
            
            if host[0]==k.BACKUP_QUERY_TAG:
                host[0]=client_socket.getpeername() # replace list first element with client IP address
                
                host_statistics = backup_queue(*host) # unpack list to pass as arguments to backup_queue
                                
                response = f'{k.START_TAG}{json.dumps(host_statistics)}{k.END_TAG}'
                
                receiving_data = False

            elif host[0]==k.CATALOG_QUERY_TAG:
                print(f"Received data from {client_socket.getpeername()[0]}. Received: {data.decode()}")
                
                response = f'{k.START_TAG}{{"Status":0,"Error":"catalog command not implemented"}}{k.END_TAG}'
                
                receiving_data = False
                
            else:
                print(f"Ignored data from from {client_socket.getpeername()[0]}. Received: {data.decode()}")
                
                response = f'{k.START_TAG}{{"Status":0,"Error":"host command not recognized"}}{k.END_TAG}'
                
                receiving_data = False
                    
        byte_response = bytes(response, encoding="utf-8")
                
        client_socket.sendall(byte_response)
        
        client_socket.close()

def serve_forever(server_socket):
    
    sel = DefaultSelector()
    sel.register(interrupt_read, EVENT_READ)
    sel.register(server_socket, EVENT_READ)

    running_backup = False
    running_processing = False
    serving_forever = True

    # TODO: change independent running process to a list of processes and use a loop to check if they are running
    # TODO: Include methods to send terminating signals to the running processes

    # Use ProcessPoolExecutor to limit the number of concurrent processes
    while serving_forever or not running_backup or not running_processing:
        
        # Wait for events using selector.select() method
        for key, _ in sel.select():
            # if the interrupt_read (^C), shutdown the server
            if key.fileobj == interrupt_read:
                interrupt_read.recv(1)
                if serving_forever:
                    serving_forever = False
                    if running_backup:
                        print("Server will shut down... Waiting for running tasks to finish.")
                    else:
                        print("Shutting down....")
                        
                else:
                    print("Shutting down but waiting for tasks to finish... please wait.")

            # if client tries to connect, accept the connection and serve the client
            if key.fileobj == server_socket:
                client_socket, client_address = server_socket.accept()
                if serving_forever:
                    print(f"Connection established with: {client_address}")
                    serve_client(client_socket)
                else:
                    print("Connection attempt rejected. Server is shutting down.")
                    response = f'{k.START_TAG}{{"Status":0,"Error":"Server shutting down"}}{k.END_TAG}'
                    byte_response = bytes(response, encoding="utf-8")
                    client_socket.sendall(byte_response)        
                    client_socket.close()

        # Whenever there is an event, check if the backup process is running and if not, start it.
        if not running_backup:
            # start the backup control module as an independent process
            command = ( f'bash -c '
                        f'"source ~/miniconda3/etc/profile.d/conda.sh; '
                        f'conda activate appdata; '
                        f'python3 {k.BACKUP_CONTROL_MODULE}"')                

            backup_process = subprocess.Popen([command],
                                              stdout=subprocess.DEVNULL,
                                              stderr=subprocess.PIPE,
                                              text=True,
                                              shell=True)
            
            print("Backup process started")
            running_backup = True

        elif backup_process.poll() is not None:
            backup_output, backup_errors = backup_process.communicate()
            running_backup = False
            
            if backup_output:
                print(f"Backup process ended with: {backup_output}.")

            if backup_errors:
                running_backup = False
                print(f"Backup process error: {backup_errors}.")

        # Whenever there is an event, check if file processing is running and if not, start it.
        if not running_processing:
            # start the file processing control module as an independent process
            command = ( f'bash -c '
                        f'"source ~/miniconda3/etc/profile.d/conda.sh; '
                        f'conda activate appdata; '
                        f'python3 {k.PROCESSING_CONTROL_MODULE}"')                

            processing_task = subprocess.Popen([command],
                                              stdout=subprocess.DEVNULL,
                                              stderr=subprocess.PIPE,
                                              text=True,
                                              shell=True)
            
            print("File processing started")
            running_processing = True

        elif processing_task.poll() is not None:
            processing_output, processing_errors = processing_task.communicate()
            running_processing = False
            
            if processing_output:
                print(f"File processing ended with: {processing_output}.")

            if processing_errors:
                running_processing = False
                print(f"File processing error: {processing_errors}.")

        # sleep one second to avoid system hang in case of error
        time.sleep(1)
            
def main():
    
    print(f"Server is listening on port {k.SERVER_PORT}")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('', k.SERVER_PORT)
    server_socket.bind(server_address)
    server_socket.listen(k.TOTAL_CONNECTIONS)

    serve_forever(server_socket)
    
    print("Shutting down....")
    server_socket.close()

if __name__ == "__main__":
    main()