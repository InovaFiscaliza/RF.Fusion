#!/usr/bin/env python
"""	Constants used in the appCataloga scripts

    Require additional /etc/appCataloga/.secret file with the following content:
    
            DB_USER_NAME = 'appCataloga' 
            DB_PASSWORD = '<app_pass>'
"""
import importlib.util
import importlib.machinery

SECRET_FILE = "/etc/appCataloga/.secret"

secret_file = importlib.util.spec_from_file_location(
                name="secret",  # note that ".test" is not a valid module name
                location=SECRET_FILE,
                loader=importlib.machinery.SourceFileLoader("secret", SECRET_FILE))

secret = importlib.util.module_from_spec(secret_file)
secret_file.loader.exec_module(secret)

# output processing information
LOG_VERBOSE = True
LOG_TARGET_FILE = True
LOG_TARGET_SCREEN = False # Only for debugging of individual modules. Do not use in production
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
DB_USER_NAME = secret.DB_USER_NAME
DB_PASSWORD = secret.DB_PASSWORD

# backup module configuration
BACKUP_CONTROL_MODULE = "/usr/local/bin/appCataloga/backup_control.py"
BACKUP_SINGLE_HOST_MODULE = "/usr/local/bin/appCataloga/backup_single_host.py"
BKP_MAX_PROCESS = 10
HOST_TASK_EXECUTION_WAIT_TIME = 10
HOST_TASK_REQUEST_WAIT_TIME = 1800 # seconds to wait for the HALT_FLAG release before aborting the task
HALT_FLAG_CHECK_CYCLES = 6 # number of times to check the host while waiting for the HALT_FLAG release
HOST_TASK_EXECUTION_TIMEOUT = 3600
BKP_HOST_ALLOTED_TIME_FRACTION = 0.8

# file processing module configuration
PROCESSING_CONTROL_MODULE = "/usr/local/bin/appCataloga/processing_control.py"
MAX_NOMINATIN_ATTEMPTS = 10

# general configuration
SECONDS_IN_MINUTE = 60
MINICONDA_PATH="/usr/local/bin/appCataloga/miniconda3/etc/profile.d/conda.sh"

# daemon standard due for backup file
DAEMON_CFG_FILE="/etc/node/indexerD.cfg"

# Folder configuration
TMP_FOLDER="tmp"
TRASH_FOLDER="trash"
REPO_FOLDER="/mnt/reposfi"
REPO_UID="repoSFI"
BP_PENDING_TASK_STATUS = 0
BP_ERROR_TASK_STATUS = -1

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