#!/usr/bin/env python
"""	Constants used in the appCataloga scripts """

# output processing information
LOG_VERBOSE = True
LOG_TARGET = 'file' # 'file' or 'screen'
LOG_FILE = '/var/log/appCataloga.log'

# appCataloga socket service configuration
SERVER_PORT = 5555
TOTAL_CONNECTIONS = 50
CATALOG_QUERY_TAG = "catalog"
BACKUP_QUERY_TAG = "backup"
START_TAG = "<json>"
END_TAG = "</json>"

# database configuration
SERVER_NAME = r'localhost'
RFM_DATABASE_NAME = 'RFDATA'
BKP_DATABASE_NAME = 'BPDATA'
DB_USER_NAME = 'appCataloga' 
DB_PASSWORD = '<app_pass>'

# backup module configuration
BACKUP_CONTROL_MODULE = "/root/RF.Fusion/src/appCataloga/root/usr/local/bin/appCataloga/backup_control.py"
BACKUP_SINGLE_HOST_MODULE = "/root/RF.Fusion/src/appCataloga/root/usr/local/bin/appCataloga/backup_single_host.py"
BKP_MAX_PROCESS = 10
BKP_TASK_EXECUTION_WAIT_TIME = 10
BKP_TASK_REQUEST_WAIT_TIME = 300
BKP_TASK_EXECUTION_TIMEOUT = 3600
BKP_HOST_ALLOTED_TIME_FRACTION = 0.8

# file processing module configuration
PROCESSING_CONTROL_MODULE = "/root/RF.Fusion/src/appCataloga/root/usr/local/bin/appCataloga/processing_control.py"
MAX_NOMINATIN_ATTEMPTS = 10

# general configuration
SECONDS_IN_MINUTE = 60
PYTHON_ENV = ['conda', 'activate', 'myenv']

# daemon standard due for backup file
DAEMON_CFG_FILE="/etc/node/indexerD.cfg"

# Folder configuration
TARGET_TMP_FOLDER="/mnt/repo/tmp"
DEFAULT_VOLUME_NAME="repoSFI"
DEFAULT_VOLUME_MOUNT="/mnt/repo"

# Geographic site definition
MAXIMUM_GNSS_DEVIATION = 0.0005
MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS = 1000

#Nomintim Geocoding parameters
NOMINATIM_USER = '9272749a.anatel.gov.br@amer.teams.ms'

# Nomintim service parameters normalization
REQUIRED_ADDRESS_FIELD = {'state':['state'],
                          'county':['city','town'],
                          'district':['suburb']}

# Default values for CRFS Bin File Translation/Processing
DEFAULT_VBW = 0.0
DEFAULT_DETECTOR = 'RMS'
DEFAULT_SAMPLE_DURATION = 0.0
DEFAULT_ATTENUATION_GAIN = 0.0