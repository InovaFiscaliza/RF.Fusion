#!/usr/bin/env python
"""Single thread that runs the backup process for a single host.
"""

import paramiko
import os

def ssh_copy_files(remote_host, username, password, remote_file, local_dir):
    # Create an SSH client
    ssh_client = paramiko.SSHClient()

    try:
        # Automatically add the server's host key (this is insecure - see paramiko docs for details!)
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Connect to the remote host
        ssh_client.connect(remote_host, username=username, password=password)

        # SFTP (Secure FTP) connection
        sftp = ssh_client.open_sftp()

        # Change the remote directory to the location of the file
        remote_dir = os.path.dirname(remote_file)
        sftp.chdir(remote_dir)

        # List files in the remote directory
        remote_files = sftp.listdir()

        # Check if the file you want to copy exists
        if os.path.basename(remote_file) not in remote_files:
            print(f"File '{os.path.basename(remote_file)}' not found on the remote server.")
            return

        # Copy the file from remote to local directory
        local_file = os.path.join(local_dir, os.path.basename(remote_file))
        sftp.get(remote_file, local_file)
        print(f"File '{os.path.basename(remote_file)}' copied to '{local_file}'")

    except paramiko.AuthenticationException:
        print("Authentication failed. Please check your credentials.")
    except paramiko.SSHException as e:
        print(f"SSH error: {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Close the SSH client and SFTP connection
        sftp.close()
        ssh_client.close()

if __name__ == "__main__":
    # Input parameters
    remote_host = input("Enter the remote host (IP or DNS): ")
    username = input("Enter the username: ")
    password = input("Enter the password: ")
    remote_file = "/mnt/internal/.sentinela/files.changed"  # Adjust the path as needed
    local_dir = "/mnt/reposfi"  # Adjust the local directory as needed

    # Create the local directory if it doesn't exist
    os.makedirs(local_dir, exist_ok=True)

    # Call the function to copy the file
    ssh_copy_files(remote_host, username, password, remote_file, local_dir)