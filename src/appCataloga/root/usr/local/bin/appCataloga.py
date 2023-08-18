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
import signal
from selectors import DefaultSelector, EVENT_READ
 
#constants
TCP_PORT = 5555
QUERY_TAG = "query"

#! TEST host_statistics initialization
HOST_STATISTICS = { "Total Files":1,
                    "Files pending backup":1,
                    "Last Backup":"today",
                    "Days since last backup":0}

interrupt_read, interrupt_write = socket.socketpair()

def handler(signum, frame):
    print('Signal handler called with signal', signum)
    interrupt_write.send(b'\0')
    
signal.signal(signal.SIGINT, handler)

def scheddule_backup(host=["conn","host_id","host_add","user","passwd"]):
    #nothing
    print(host)
    return(HOST_STATISTICS)

def serve_client(client_socket):
    try:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            print("Received:", data.decode())
            # You can add your processing logic here if needed
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
    
    # initialize warning message variable
    warning_msg = ""

    print("Server is listening on port 5555")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('', TCP_PORT)
    server_socket.bind(server_address)
    server_socket.listen(50)

    serve_forever(server_socket)

    print("Shutdown...")
    server_socket.close()

if __name__ == "__main__":
    main()