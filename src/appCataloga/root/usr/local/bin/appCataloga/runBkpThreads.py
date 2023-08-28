#!/usr/bin/env python
"""
Access the backup list from BKPDATA database and starts the backup process threads.
    
    Usage:
        runBackup 
            
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
from selectors import DefaultSelector, EVENT_READ

# Import modules for file processing 
import config as k
import dbHandler as dbh
import appCShared as csh

import paramiko
import os
import concurrent.futures

# Function to copy files from a remote machine
def ssh_copy_files(remote_host, username, password, remote_file, local_dir):
    # The same function as in the previous script

    # List of remote machines with their details
    remote_machines = [
        {"host": "remote1.example.com", "username": "user1", "password": "password1"},
        {"host": "remote2.example.com", "username": "user2", "password": "password2"},
        # Add more remote machines here
    ]

    # Maximum number of concurrent threads
    max_threads = 3  # Adjust as needed

    # Local directory to store copied files
    local_dir = "/mnt/reposfi"  # Adjust the local directory as needed

    # Create the local directory if it doesn't exist
    os.makedirs(local_dir, exist_ok=True)

# Function to copy files from a remote machine and handle exceptions
def copy_files_wrapper(machine_details):
                

            if task:
                task_id = task["ID_BKP_TASK"]
                host_address = task["NO_HOST_ADDRESS"]
                username = task["NO_HOST_USER"]
                password = task["NO_HOST_PASSWORD"]
                
    try:
        ssh_copy_files(
            machine_details["host"],
            machine_details["username"],
            machine_details["password"],
            "/mnt/internal/.sentinela/files.changed",  # Adjust the path as needed
            local_dir
        )
    except Exception as e:
        print(f"Error copying files from {machine_details['host']}: {str(e)}")

# Use ThreadPoolExecutor to limit the number of concurrent threads
with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
    executor.map(copy_files_wrapper, remote_machines)

print("All copy processes initiated. Waiting for completion...")

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