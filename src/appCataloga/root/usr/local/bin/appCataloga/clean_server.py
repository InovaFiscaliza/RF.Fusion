#!/usr/bin/python3
# ! UNFINISHED !
"""This module perform the following tasks:
    - connects to a server via ssh using paramiko, including port forwarding to allow access to mySQL
        - Ask the user for the required authentication information, including user name, password and MFA code;
    - load the configuration file stored in the remote server at /etc/appCataloga/config.py;
    - list files from the repositories TMP_FOLDER, TRASH_FOLDER and REPO_FOLDER;
    - Connect to the mysql in the remote server using the credentials stored in the remote /etc/appCataloga/secret.py file;
    - Access a database named RFDATA
    - Compare the information in the DIM_SPECTRUM_FILE table with file names from REPO_FOLDER and subfolders
            - print a list of files that are not in the database but in the repository and the otherway arround.
            - DIM_SPECTRUM_FILE table have fields NA_FILE, NA_PATH and NA_VOLUME that store the file name and path.
            - The REPO_FOLDER is associated with an specific NA_VOLUME named "repoSFI".
    - After user confirmation, remove entries in the DIM_SPECTRUM_FILE that are not in the REPO_FOLDER;
    - After user confirmation, move files that are in the REPO_FOLDER but not in the DIM_SPECTRUM_FILE to the TMP_FOLDER folder in the remote server;
    - Access the BPDATA database
    - Compare file names from the TRASH_FOLDER and TMP_FOLDER with the files listed in PRC_TASK table in the BPDATA datanase.
        - Print a list of files that are not in the database but in the repository and the otherway arround;
    - After user confirmation, update HOST table in the BPDATA, where:
        - NU_HOST_FILES should be the total number of files for a given host, including those in the 3 folders (mount, tmp and trash);
        - NU_PENDING_PROCESSING should be the number of files in the TMP folder for a given host;
        - NU_PROCESSING_ERROR, should be the number of files in the TRASH folder for a given host.
"""
import sys
# sys.path.append('Y:\\RF.Fusion\\src\\appCataloga\\root\\etc\\appCataloga\\')
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
import shared as sh
import db_handler as dbh


# sys.path.append('/etc/appCataloga')
# sys.path.append('/root/RF.Fusion/test/appCataloga')


CONFIG_PATH = '/etc/appCataloga/config.py'

def connection_data() -> dict:
    """Retrieve connection data from user input and secret file.

    Args:
        none

    Returns:
        dict: {
    """
    
    conn_data={}
    try:
        conn_data['host'] = secret.HOST
    except:
        conn_data['host'] = input("Enter the SSH host: ")
    
    try:
        conn_data['port'] = secret.PORT
    except:
        conn_data['port'] = input("Enter the SSH port: ")
        
    try:
        conn_data['ssh_proxy'] = secret.SSH_PROXY
    except:
        conn_data['ssh_proxy'] = input("Enter the SSH proxy: ")
        
    try:
        conn_data['username'] = secret.USERNAME
    except:
        conn_data['username'] = input("Enter username: ")
    
    try:
        conn_data['password'] = secret.PASSWORD
    except:
        conn_data['password'] = input("Enter password: ")
    
    conn_data['mfa_code'] = input("Enter MFA code: ")
    
    conn_data['user_full_name'] = f"{conn_data['username']}[{conn_data['host']}]{conn_data['mfa_code']}"

    return conn_data

def ssh_connect(conn_data: dict) -> paramiko.SSHClient:
    
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.WarningPolicy())

    try:
        client.connect(conn_data['ssh_proxy'],
                       conn_data['port'],
                       conn_data['user_full_name'],
                       conn_data['password'])
        
    except Exception as e:
        print(f" Failed to connect to {conn_data['ssh_proxy']}:22: {e}")
        sys.exit(1)    
    
    return client

class tunnel(threading.Thread):
    def __init__(self, remote_bind_port, forward_host, forward_port, transport):
        threading.Thread.__init__(self)
        self.remote_bind_port = remote_bind_port
        self.forward_host = forward_host
        self.forward_port = forward_port
        self.transport = transport
        self.daemon = threading.Thread

    def _handler(self) -> None:
        self.transport.request_port_forward("", self.remote_bind_port)
        while True:
            chan = self.transport.accept(1000)
            if chan is None:
                continue

            sock = socket.socket()
            try:
                sock.connect((self.forward_host, self.forward_port))
            except Exception as e:
                print(f"Forwarding request to {self.forward_host}:{self.forward_port} failed: {e}")
                return

            while True:
                r, w, x = select.select([sock, chan], [], [])
                if sock in r:
                    data = sock.recv(1024)
                    if len(data) == 0:
                        break
                    chan.send(data)
                if chan in r:
                    data = chan.recv(1024)
                    if len(data) == 0:
                        break
                    sock.send(data)
            chan.close()
            sock.close()
        
    def open(self):
        self.daemon = threading.Process(target=self._handler)
        self.daemon.setDaemon(True)
        self.daemon.start()
        
    def close(self):
        self.daemon.terminate()
        self.daemon.join()

def load_config(ssh):
    # Load configuration file from remote server at CONFIG_PATH
    
    stdin, stdout, stderr = ssh.exec_command(f"cat {CONFIG_PATH}")
    server_config = stdout.read().decode('utf-8')

    return server_config

def list_files(ssh, folders):
    # List files in specified folders
    for folder in folders:
        stdin, stdout, stderr = ssh.exec_command(f"ls {folder}")
        files = stdout.read().decode('utf-8').split('\n')
        print(f"Files in {folder}:")
        print(files)

def mysql_connect(ssh):
    # Load MySQL credentials from secret.py
    secret_path = '/etc/appCataloga/secret.py'
    stdin, stdout, stderr = ssh.exec_command(f"cat {secret_path}")
    secret_content = stdout.read().decode('utf-8')

    # Process secret_content to extract MySQL credentials

    # MySQL connection parameters
    mysql_host = "localhost"
    mysql_port = 3306
    mysql_user = "your_mysql_username"
    mysql_password = "your_mysql_password"
    mysql_database_rfdata = "RFDATA"
    mysql_database_bpdata = "BPDATA"

    # Establish MySQL connection
    mysql_conn = pymysql.connect(host=mysql_host, port=mysql_port, user=mysql_user, password=mysql_password, database=mysql_database_rfdata)
    return mysql_conn

def compare_files_in_db_and_repo(mysql_conn, repo_folder):
    # Compare files in DIM_SPECTRUM_FILE table with files in REPO_FOLDER
    cursor = mysql_conn.cursor()

    # Query to get files from DIM_SPECTRUM_FILE
    query = "SELECT NA_FILE, NA_PATH FROM DIM_SPECTRUM_FILE WHERE NA_VOLUME = 'repoSFI'"
    cursor.execute(query)
    db_files = set((row[0], row[1]) for row in cursor.fetchall())

    # Get files in REPO_FOLDER
    repo_files = set()
    for root, dirs, files in os.walk(repo_folder):
        for file in files:
            repo_files.add((file, root))

    # Identify files not in the database
    files_not_in_db = repo_files - db_files
    files_not_in_repo = db_files - repo_files

    # Print the results
    print("Files not in the database but in the repository:")
    print(files_not_in_db)
    print("Files not in the repository but in the database:")
    print(files_not_in_repo)

    cursor.close()

def remove_entries_in_db(mysql_conn, files_to_remove):
    # After user confirmation, remove entries in DIM_SPECTRUM_FILE
    confirmation = input("Do you want to remove entries in DIM_SPECTRUM_FILE? (y/n): ")
    if confirmation.lower() == 'y':
        cursor = mysql_conn.cursor()
        for file, path in files_to_remove:
            query = f"DELETE FROM DIM_SPECTRUM_FILE WHERE NA_FILE = '{file}' AND NA_PATH = '{path}'"
            cursor.execute(query)
        mysql_conn.commit()
        cursor.close()

def move_files_to_tmp_folder(ssh, files_to_move, tmp_folder):
    # After user confirmation, move files to TMP_FOLDER
    confirmation = input("Do you want to move files to TMP_FOLDER? (y/n): ")
    if confirmation.lower() == 'y':
        sftp = ssh.open_sftp()
        for file, path in files_to_move:
            remote_src_path = os.path.join(path, file)
            remote_dst_path = os.path.join(tmp_folder, file)
            sftp.rename(remote_src_path, remote_dst_path)
        sftp.close()

def compare_files_in_bpdata(mysql_conn, tmp_folder, trash_folder):
    # Compare files in PRC_TASK table with files in TMP_FOLDER and TRASH_FOLDER
    cursor = mysql_conn.cursor()

    # Query to get files from PRC_TASK
    query = "SELECT FILE_NAME FROM PRC_TASK"
    cursor.execute(query)
    db_files = set(row[0] for row in cursor.fetchall())

    # Get files in TMP_FOLDER and TRASH_FOLDER
    tmp_files = set(Path(tmp_folder).rglob('*'))
    trash_files = set(Path(trash_folder).rglob('*'))

    # Identify files not in the database
    files_not_in_db = (tmp_files | trash_files) - db_files
    files_not_in_repo = db_files - (tmp_files | trash_files)

    # Print the results
    print("Files not in the database but in the repository:")
    print(files_not_in_db)
    print("Files not in the repository but in the database:")
    print(files_not_in_repo)

    cursor.close()

def update_host_table(mysql_conn, host):
    # After user confirmation, update HOST table in BPDATA
    confirmation = input("Do you want to update HOST table in BPDATA? (y/n): ")
    if confirmation.lower() == 'y':
        cursor = mysql_conn.cursor()
        nu_host_files = len(list(Path(host['mount']).rglob('*'))) + len(list(Path(host['tmp']).rglob('*'))) + len(list(Path(host['trash']).rglob('*')))
        nu_pending_processing = len(list(Path(host['tmp']).rglob('*')))
        nu_processing_error = len(list(Path(host['trash']).rglob('*')))
        query = f"UPDATE HOST SET NU_HOST_FILES = {nu_host_files}, NU_PENDING_PROCESSING = {nu_pending_processing}, NU_PROCESSING_ERROR = {nu_processing_error} WHERE HOST_ID = {host['id']}"
        cursor.execute(query)
        mysql_conn.commit()
        cursor.close()

def main():
    try:
        
        conn_data = connection_data()
        
        # Connect to SSH
        client = ssh_connect(conn_data)

        # Load configuration file
        server_config = load_config(client)
        
        # source variables from server_config to local namespace
        exec(server_config, locals())
        repo_folder = locals()['REPO_FOLDER']
        db_user = locals()['DB_USER_NAME']
        db_password = locals()['DB_PASSWORD']
        rfm_database_name = locals()['RFM_DATABASE_NAME']
        bkp_database_name = locals()['BKP_DATABASE_NAME']
        
        mysql_tunnel = tunnel(
            remote_bind_port=MYSQL_REMOTE_PORT,
            forward_host=conn_data['ssh_proxy'],
            forward_port=MYSQL_LOCAL_PORT,
            transport=client.get_transport())
        
        mysql_tunnel.open()
        
        # List files in specified folders
        TMP_FOLDER="tmp"
        TRASH_FOLDER="trash"
        REPO_FOLDER="/mnt/reposfi"
        REPO_UID="repoSFI"
        
        list_files(client, repo_folder)

        # Connect to MySQL
        mysql_conn = mysql_connect(ssh)

        # Compare files in RFDATA database with files in REPO_FOLDER
        repo_folder = '/path/to/REPO_FOLDER'
        compare_files_in_db_and_repo(mysql_conn, repo_folder)

        # Remove entries in DIM_SPECTRUM_FILE
        files_to_remove = {('file1', '/path/to/folder1'), ('file2', '/path/to/folder2')}
        remove_entries_in_db(mysql_conn, files_to_remove)

        # Move files to TMP_FOLDER
        files_to_move = {('file3', '/path/to/folder3'), ('file4', '/path/to/folder4')}
        tmp_folder = '/path/to/TMP_FOLDER'
        move_files_to_tmp_folder(ssh, files_to_move, tmp_folder)

        # Compare files in BPDATA database with files in TMP_FOLDER and TRASH_FOLDER
        tmp_folder = '/path/to/TMP_FOLDER'
        trash_folder = '/path/to/TRASH_FOLDER'
        compare_files_in_bpdata(mysql_conn, tmp_folder, trash_folder)

        # Update HOST table in BPDATA
        host = {'id': 1, 'mount': '/path/to/mount', 'tmp': '/path/to/TMP_FOLDER', 'trash': '/path/to/TRASH_FOLDER'}
        update_host_table(mysql_conn, host)

    finally:
        # Close SSH connection
        ssh.close()
        # Close MySQL connection
        mysql_conn.close()

if __name__ == "__main__":
    main()



