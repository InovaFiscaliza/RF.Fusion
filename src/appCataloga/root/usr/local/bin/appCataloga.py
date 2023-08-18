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
import json
import signal
from selectors import DefaultSelector, EVENT_READ
 
#constants
SERVER_PORT = 5555
TOTAL_CONNECTIONS = 50
QUERY_TAG = "query"
START_TAG = "<json>"
END_TAG = "</json>"

#! TEST host_statistics initialization
HOST_STATISTICS = { "Total Files":1,
                    "Files pending backup":1,
                    "Last Backup":"today",
                    "Days since last backup":0}

# initialize warning message variable
warning_msg = ""

interrupt_read, interrupt_write = socket.socketpair()

def handler(signum, frame):
    print('Signal handler called with signal', signum)
    interrupt_write.send(b'\0')
    
signal.signal(signal.SIGINT, handler)

def backup_queue(host=[("ClientIP",0),"host_id","host_add","user","passwd"]):
    print(host)
    # add host to db backup list
    # get from db the backup summary status for the host_id
    return HOST_STATISTICS

"""
- Loop infinito de gestão
  - Consultar BD os parâmetros de limite e tempo de espera
  - Consultar BD o quantitativo de backups pendentes
  - Consultar BD o quantitativo de processos de catalogação pendentes
  - Se processos de backup em execução < limite_bkp, disparar novo processo
  - Se processos de catalogação em execução < limite_proc, disparar novo processo
  - Aguardar tempo de espera
  
- processo de backup
  - recebe host_add, user e pass na chamada
  - realiza backup
  - atualiza BD de sumarização para o host
  - atualiza BD lista de catalogações pendentes

"""

def serve_client(client_socket):
    try:
        while True:
            data = client_socket.recv(128)
            if not data:
                break
            
            host = data.decode().split(" ")
            
            if host[0]==QUERY_TAG:
                host[0]=client_socket.getpeername()
                
                host_statistics = backup_queue(host)
                
                stat_txt = json.dumps(host_statistics)[:-1]

                response = f'{START_TAG}{stat_txt},"Status":1,"Message":"{warning_msg}"}}{END_TAG}'

            else:
                print(f"Ignored data from from {client_socket.getpeername()[0]}. Received: {data.decode()}")
                
                response = f'{START_TAG}{{"Status":0,"Error":"host command not recognized"}}{END_TAG}'
                
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
    
    print(f"Server is listening on port {SERVER_PORT}")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('', SERVER_PORT)
    server_socket.bind(server_address)
    server_socket.listen(TOTAL_CONNECTIONS)

    serve_forever(server_socket)

    print("Shutdown...")
    server_socket.close()

if __name__ == "__main__":
    main()