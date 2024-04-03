#! /home/lobao.lx/miniconda3/envs/rfpye/bin/python3

#! AppAutomato
# File transfer automation and catalog

#* Remote Script: is a crom job that houlry performs the following tasks in each remote host
#       - check halt cookie and exit if interrupt signal is active
#       - activate the halt cookie
#       - remove temp files with suppresed errors
#       - get date-time info from cookie file created during the last backup
#       - if last backup cookie not active, perform initial scan to backup all files.
#       - find files that were changed since last backup and create a temp file
#       - diff temp file to the existing next backup list
#       - compute md5 to new/changed files and append create a temp file with the te corresponding md5 data
#       - replace the next backup file list with the temp and next backup md5 with the new md5
#       - remove the temp files.
#       - deactivate halt cookie

#*  - File cataloguer
#       - load host list from a yaml ansoble conpatible inventory fle
#       - Load pass from a secret file
#       - Connect to hosts and load the remote spectrum data file list diff
#       - If remote file list diff not available, call script to remotly bulid the spectrum data file list diff.
#       - If remote file list is too big, validate list with catalog, removing files that might have been previously copied
#       - check if halt cookie is active in the remote rost
#           - true = skip to next host
#           - false = activate the halt cookie semafore in the remote host
#       - Transfer file list and md5 files
#       - Combine and update pending copy file list
# 
#*  - File transferer 
#       - load host list from a yaml ansoble conpatible inventory fle
#       - Load pass from a secret file
#       - load pending copy file list
#       - Connect to host and transfer file
#       - Check MD5
#       - Include file in the pending processing list
#       - Remove file from pending copy file list
#
#*  - File meta cataloguer
#       - Load pending processing list
#       - extract metadata
#       - Update data catalog
#

from pssh.clients import ParallelSSHClient


hosts = ['localhost', 'localhost']
host_config = [
    HostConfig(port=2222, user='user1',
               password='pass', private_key='my_pkey.pem'),
    HostConfig(port=2223, user='user2',
               password='pass', private_key='my_other_key.pem'),
]

client = ParallelSSHClient(hosts, host_config=host_config)
cmd = 'uname'

output = client.run_command(cmd)
for host_out in output:
    for line in host_out.stdout:
        print(line)