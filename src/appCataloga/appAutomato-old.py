#! /home/lobao.lx/miniconda3/envs/rfpye/bin/python3

# # AppAutomato
# This script perform the following tasks
# - load host list from a yaml ansoble conpatible inventory fle
# - Load pass from a secret file
# - Connect to hosts and load the remote spectrum data file list diff
# - If remote file list diff not available, call script to remotly bulid the spectrum data file list diff.
#       - Script is a crom job that houlry performs the following tasks
#            - suppres errors
#            - remove temp files
#            - find files that were changed since last backup and create a temp file
#            - diff temp file to the existing next backup list
#            - compute md5 to new/changed files and append create a temp file with the te corresponding md5 data
#            - replace the next backup file list with the temp and next backup md5 with thenew md5
#            - remove the temp files.
# - Diff the remote spectrum data file list with central file database
# - Transfer 

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