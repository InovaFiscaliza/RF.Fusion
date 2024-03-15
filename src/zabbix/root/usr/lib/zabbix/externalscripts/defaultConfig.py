#!/usr/bin/python
"""
Define constats used by various modules in RF.Fusion project

Some constants are envorinment specific and should be changed to match the local environment
"""

# shared constants (used by: queryCataloga.py, queryLogger
START_TAG = b"<json>" # used by queryLoggerUDP.py, queryCataloga.py
END_TAG = b"</json>" # used by queryLoggerUDP.py, queryCataloga.py
SMALL_BUFFER_SIZE = 1024 # used by queryCataloga.py, queryDigitizer.py
MID_BUFFER_SIZE = 16384 # used by queryappColeta.py
LARGE_BUFFER_SIZE = 65536 # used by queryLoggerUDP.py
TIMEOUT_BUFFER = 1 # used by queryLoggerUDP.py and quryDigitizer.py. Additional time after timeout to wait for data to be received
UTF_ENCODING = "utf-8" # used by queryCataloga.py
ISO_ENCODING = "ISO-8859-1" # used by queryappColeta.py

# appCataloga constants
ACAT_SERVER_ADD = "192.168.200.30"  # Change this to the server's hostname or IP address
ACAT_SERVER_PORT = 5555

# appCataloga default arguments
ACAT_DEFAULT_HOST_ID = "10367"
ACAT_DEFAULT_HOST_UID = "RFEye002080"
ACAT_DEFAULT_HOST_ADD = "192.168.1.129"
ACAT_DEFAULT_HOST_PORT = 22
ACAT_DEFAULT_USER = "sshUser"  # user should have access to the host with rights to interact with the indexer daemon
ACAT_DEFAULT_PASSWD = "sshuserpass"
ACAT_DEFAULT_QUERY_TAG = "backup"
ACAT_DEFAULT_TIMEOUT = 2

# appColeta constants
None

# appColeta Default arguments
# ACOL_DEFAULT_HOST_ADD = "172.24.5.71" # Uberlândia
# ACOL_DEFAULT_HOST_ADD = "172.24.5.73" # Confins
ACOL_DEFAULT_HOST_ADD = "172.24.5.33" # Vitoria

ACOL_DEFAULT_PORT = 8910
ACOL_DEFAULT_KEY = "123456"  
ACOL_DEFAULT_CLIENT_NAME = "Zabbix"
# ACOL_DEFAULT_QUERY_TAG = "PositionList" 
# ACOL_DEFAULT_QUERY_TAG = "Diagnostic"
ACOL_DEFAULT_QUERY_TAG = "TaskList"
ACOL_DEFAULT_TIMEOUT = 10

# queryDigitizer constants
None
# queryDigitizer default arguments
DIGI_DEFAULT_HOST = "172.24.4.95"
DIGI_DEFAULT_PORT = 37001
DIGI_DEFAULT_TIMEOUT = 2

# queryLoggerUDP constants
BUFFER_SIZE = 65536
ENCODING = "utf-8"

# queryLoggerUDP default arguments
LOGU_DEFAULT_HOST = "172.24.1.13"
LOGU_DEFAULT_PORT = 5555
LOGU_DEFAULT_TIMEOUT = 1
