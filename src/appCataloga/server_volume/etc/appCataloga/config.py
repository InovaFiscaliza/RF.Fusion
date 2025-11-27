#!/usr/bin/env python
"""Constants used in the appCataloga scripts

Require additional /etc/appCataloga/.secret file with the following content:

        DB_USER_NAME = 'appCataloga'
        DB_PASSWORD = '<app_pass>'
"""

import os, importlib.util,runpy
import importlib.machinery

#------------------------------------------
# load base dir
#------------------------------------------
base_dir = os.path.dirname(__file__)
secret_path = os.path.join(base_dir, ".secret")
secret = runpy.run_path(secret_path)
#------------------------------------------
# output processing information
#------------------------------------------
LOG_VERBOSE         = True
LOG_TARGET_FILE     = True
LOG_TARGET_SCREEN = (
    False  # Only for debugging of individual modules. Do not use in production
)
LOG_FILE = "/var/log/appCataloga.log"

#------------------------------------------
# appCataloga socket service configuration
#------------------------------------------
SERVER_PORT             = 5555
TOTAL_CONNECTIONS       = 50
BACKUP_QUERY_TAG        = "backup1"
START_TAG               = "<json>"
END_TAG                 = "</json>"
#------------------------------------------
# database configuration
#------------------------------------------
SERVER_NAME             = r"10.88.0.33"
DB_PORT                 = 3306
RFM_DATABASE_NAME       = "RFDATA"
BKP_DATABASE_NAME       = "BPDATA"
DB_USER_NAME            = secret["DB_USER_NAME"]
DB_PASSWORD             = secret["DB_PASSWORD"]
#------------------------------------------
# backup module configuration
#------------------------------------------
MAX_HOST_TASK_WAIT_TIME         = 30        # seconds to wait for a new task. Minimum half, maximum equal to this value
MAX_FILE_TASK_WAIT_TIME         = 30        # seconds to wait for a new task. Minimum half, maximum equal to this value
HOST_TASK_REQUEST_WAIT_TIME     = 1800      # seconds to wait for the HALT_FLAG release before aborting the task
HALT_FLAG_CHECK_CYCLES          = 6         # number of cycles to check for HALT_FLAG (6 x 300s = 30 minutes)
BKP_HOST_ALLOTED_TIME_FRACTION  = 0.8
HOST_BUSY_TIMEOUT               = 18000     # 18000 seconds or 5 hours
BKP_TASK_MAX_WORKERS            = 10
BKP_TASK_WORKER_SERVICE         = "usr/local/bin/appCataloga/appCataloga_file_bkp@"
MIN_FILE_SIZE_KB                = 1         # minimum file size to be backed up in KB
MIN_FILE_AGE_MINUTES            = 30        # minimum file age to be backed up in minutes
#------------------------------------------
# metadata publishing module configuration
#------------------------------------------
PUBLISH_FILE = "/mnt/reposfi/Metadata/rf_metadata"  # filename without extension
#------------------------------------------
# general configuration
#------------------------------------------
SECONDS_IN_MINUTE = 60
#------------------------------------------
# daemon standard indexerD configuration
#------------------------------------------
DEFAULT_DATA_FOLDER = "/mnt/internal"
DAEMON_CFG_FILE = "/etc/node/indexerD.cfg"
LOCAL_INDEXERD  = {
    "LOCAL_REPO"            : "/mnt/internal/data",
    "INDEXERD_FOLDER"       : "/mnt/internal/.indexerD",
    "TEMP_CHANGED"          : "temp.changed.list",
    "DUE_BACKUP"            : "files.changed.list",
    "BACKUP_DONE"           : "backup.done.list",
    "HALT_FLAG"             : ".halt_cookie",
    "HALT_TIMEOUT"          : 300,
    "LAST_FILE_SEARCH_FLAG" : ".last.file.search.cookie"
}
#------------------------------------------
# Folder configuration
#------------------------------------------
TMP_FOLDER      = "tmp"
TRASH_FOLDER    = "trash"
REPO_FOLDER     = "/mnt/reposfi"
REPO_UID        = "repoSFI"
#------------------------------------------
# Geographic site definition
#------------------------------------------
MAXIMUM_GNSS_DEVIATION = 0.0005
MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS = 1000
#------------------------------------------
# Nomintim Geocoding parameters
#------------------------------------------
NOMINATIM_USER = "9272749a.anatel.gov.br@amer.teams.ms"
REQUIRED_ADDRESS_FIELD = {
    "state"     : ["state"],
    "county"    : ["city", "town"],
    "district"  : ["suburb"],
}
#------------------------------------------
# Default values for CRFS Bin File Translation/Processing
#------------------------------------------
DEFAULT_VBW                 = 0.0
DEFAULT_DETECTOR            = "RMS"
DEFAULT_SAMPLE_DURATION     = 0.0
DEFAULT_ATTENUATION_GAIN    = 0.0
#------------------------------------------
# Default None Filter
#------------------------------------------
NONE_FILTER = {
    "mode"          : "NONE",
    "start_date"    : None,
    "end_date"      : None,
    "last_n_files"  : None,
    "extension"     : None,
    "file_name"     : None,
    "file_path"     : "/mnt/internal/data",
    "agent"         : "local"
}
#------------------------------------------
# Database Tasks Type Constants
#------------------------------------------
HOST_TASK_CHECK_TYPE                = 1         # Create a Host Check Task
HOST_TASK_PROCESSING_TYPE           = 2         # Create a Host Processing Task
HOST_TASK_UPDATE_STATISTICS_TYPE    = 3         # Create a Host Statistics Update Task
FILE_TASK_BACKUP_TYPE               = 1         # Create a backup task
FILE_TASK_PROCESS_TYPE              = 2         # Process a backup task of indexerD mapped files
FILE_TASK_DISCOVERY                 = 3         # Create a discovery task to get file metadata
#------------------------------------------
# Task Status Constants
#------------------------------------------
TASK_SUSPENDED          = -2        # Task suspended by offline node
TASK_ERROR              = -1        # Task suspended by error manager
TASK_DONE               = 0         # Task completed successfully
TASK_PENDING            = 1         # Task pending execution
TASK_RUNNING            = 2         # Task running

