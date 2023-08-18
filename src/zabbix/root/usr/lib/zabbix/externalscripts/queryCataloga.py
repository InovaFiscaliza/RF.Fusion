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

host = "localhost"  # Change this to the server's hostname or IP address
port = 5555

user = "your_username"
passwd = "your_password"

connection_request = bytes(f"query {host} {user} {passwd}", encoding="utf-8")

def main():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))

    try:
        client_socket.sendall(connection_request)

        response = client_socket.recv(1024)
        print("Received:", response.decode())

    except Exception as e:
        print("Error:", e)

    finally:
        client_socket.close()

if __name__ == "__main__":
    main()