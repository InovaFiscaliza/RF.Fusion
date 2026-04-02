#!/usr/bin/env python
"""Constants used in the appCataloga scripts

Require additional /etc/appCataloga/.secret file with the following content:

        DB_USER_NAME = 'appCataloga'
        DB_PASSWORD = '<app_pass>'
"""

import os, runpy

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
LOG_DIR = "/var/log"
LOG_FILE_TEMPLATE = "{logger_name}.log"
LOG_FILE = "/var/log/appCataloga.log"
LOG_MAX_FILE_SIZE_MB = 100        # Rotate one log file after it reaches this size (MB)
LOG_MAX_BACKUP_FILES = 5          # Keep at most this many rotated log generations per file

#------------------------------------------
# appCataloga socket service configuration
#------------------------------------------
SERVER_PORT             = 5555
TOTAL_CONNECTIONS       = 50
BACKUP_QUERY_TAG        = "backup"
STOP_QUERY_TAG          = "stop"
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
# =================================================
# APP_ANALISE remote processing service
# =================================================
APP_ANALISE_HOST_ADD        = "WIMATLABPDIN01"
APP_ANALISE_HOST_PORT       = 8910
APP_ANALISE_SOCKET_TIMEOUT  = 10
APP_ANALISE_BUFFER_SIZE     = 4096
APP_ANALISE_KEY             = "123456"
APP_ANALISE_CLIENT_NAME     = "Matlab"
APP_ANALISE_PROCESS_TIMEOUT = 600
APP_ANALISE_CONNECT_TIMEOUT = 15
APP_ANALISE_WORKER_DETAIL   = "worker=APP_ANALISE"
APP_ANALISE_SOURCE_LINEAGE_FAMILIES = (
    "cwsm",
    "rfeye",
    "miaer",
    "emrx",
    "ermx",
)
APP_ANALISE_MOBILE_TASK_MARKERS = (
    "drive-test",
    "drive test",
)
APP_ANALISE_MOBILE_GPS_STD_THRESHOLD = 0.001
APP_ANALISE_MOBILE_PATH_STD_MULTIPLIER = 2.0
APP_ANALISE_MULTI_SITE_REPO_SUBDIR = "appanalise_multi_site"
#------------------------------------------
# SSH LIMITS
#------------------------------------------
SSH_CONNECT_TIMEOUT    = 30
SSH_BANNER_TIMEOUT     = 30
SSH_AUTH_TIMEOUT       = 30
#------------------------------------------
# ICMP LIMITS
#------------------------------------------
ICMP_TIMEOUT_SEC = 10
HOST_CHECK_ALL_ENABLED = True              # Periodic HOST table sweep outside HOST_TASK queue
HOST_CHECK_ALL_STALE_AFTER_SEC = 300       # Re-check hosts whose DT_LAST_CHECK is older than this
HOST_CHECK_ALL_BATCH_SIZE = 10             # Max hosts per idle sweep batch
HOST_CHECK_ALL_ICMP_TIMEOUT_SEC = 3        # Short ICMP timeout for background sweep
#------------------------------------------
# backup module configuration
#------------------------------------------
MAX_HOST_TASK_WAIT_TIME         = 2         # seconds to wait for a new task. Minimum half, maximum equal to this value
MAX_FILE_TASK_WAIT_TIME         = 30        # seconds to wait for a new task. Minimum half, maximum equal to this value
HOST_BUSY_TIMEOUT               = 18000     # 18000 seconds or 5 hours
HOST_CLEANUP_INTERVAL           = 300       # Interval in seconds to check for and clean up stale host locks
HOST_CLEANUP_NO_TASK_GRACE_SEC  = 30        # Minimum BUSY age before releasing a host with no running tasks detected
SFTP_BUSY_COOLDOWN_SECONDS      = 15        # Temporary host cooldown after transient SSH/SFTP init failure
DISCOVERY_RESERVATION_TTL_SEC   = 300       # Fresh pending CHECK/PROCESSING HOST_TASK reserves the next host window for discovery
HOST_TASK_OPERATIONAL_STALE_SEC = 300       # Recover stale RUNNING operational HOST_TASK when ownership/liveness no longer matches execution reality
HOST_CHECK_SSH_PROBE_TIMEOUT_SEC = 20       # Short SSH probe timeout for host_check operational confirmation
HOST_CHECK_SSH_TIMEOUT_CONFIRMATIONS = 3    # Consecutive SSH timeout confirmations required before suspending a pingable host
HOST_UNLOCKED_PID               = 0         # HOST.NU_PID used when the host is not owned by a worker
HOST_TRANSIENT_BUSY_PID         = 0         # HOST.NU_PID used during short transient SFTP cooldown
BKP_TASK_MAX_WORKERS            = 10        # Number of concurrent backup workers. Set to 1 to avoid overloading the network and SFTP server, which can cause more harm than good when multiple workers are contending for the same resources.
BKP_TASK_IDLE_EXIT_CYCLES       = 3         # extra workers exit after this many idle polls
MIN_FILE_SIZE_KB                = 1         # minimum file size to be backed up in KB
MIN_FILE_AGE_MINUTES            = 30        # minimum file age to be backed up in minutes
FILE_THRESHOLD_SIZE_KB          = 100       # file size threshold for update file
BACKUP_TRANSFER_MAX_SECONDS     = HOST_BUSY_TIMEOUT   # Absolute upper bound for one SFTP transfer
BACKUP_TRANSFER_STALL_TIMEOUT_SECONDS = 900           # Abort when a transfer makes no progress for too long
BACKUP_TRANSFER_PROGRESS_POLL_SECONDS = 30           # How often to inspect callback/local file growth
BACKUP_TRANSFER_HEARTBEAT_SECONDS = 300              # Periodic progress log while a large transfer is still alive
SFTP_BUSY_RETRY_DETAIL          = "sftp connection busy, will retry"
SSH_TIMEOUT_RETRY_DETAIL        = "ssh init timeout, awaiting connectivity confirmation"

#------------------------------------------
# metadata publishing module configuration
#------------------------------------------
PUBLISH_FILE = "/mnt/reposfi/Metadata/rf_metadata"  # filename without extension
#------------------------------------------
# discovery defaults
#------------------------------------------
DEFAULT_DATA_FOLDER     = "/mnt/internal"
DISCOVERY_BATCH_SIZE    = 1000
#------------------------------------------
# Folder configuration
#------------------------------------------
TMP_FOLDER      = "tmp"
TRASH_FOLDER    = "trash"
# appAnalise export success/failure may leave superseded source payloads or
# intermediate exported artifacts that are no longer referenced by BKP history.
# Those files are quarantined here and garbage-collected directly from the
# filesystem instead of through FILE_TASK_HISTORY lookups.
RESOLVED_FILES_TRASH_SUBDIR = "resolved_files"
REPO_FOLDER     = "/mnt/reposfi"
REPO_VOLUME_NAME = "reposfi"
#------------------------------------------
# Geographic site definition
#------------------------------------------
MAXIMUM_GNSS_DEVIATION = 0.0005
MAXIMUM_NUMBER_OF_GNSS_MEASUREMENTS = 1000
SITE_DISTRICT_AUTO_CREATE = False         # Keep district resolution conservative unless explicitly enabled
#------------------------------------------
# Nomintim Geocoding parameters
#------------------------------------------
NOMINATIM_USER = "9272749a.anatel.gov.br@amer.teams.ms"
REQUIRED_ADDRESS_FIELD = {
    "state": [
        "state",              # Official state name (ideal case)
    ],
    "county": [
        "city",               # Municipality (urban areas)
        "town",               # Municipality (smaller towns)
        "village",            # Common for airports, rural and technical areas
    ],
    "district": [
        "suburb",             # Neighborhood
        "city_district",      # Administrative district (POIs / institutions)
        "neighbourhood",      # Generic fallback
    ],
}
#------------------------------------------
# Default values for CRFS Bin File Translation/Processing
#------------------------------------------
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
    "file_path"     : "/mnt/internal/data",
    "file_name"     : None,
}
#------------------------------------------
# Database Tasks Type Constants
#------------------------------------------
HOST_TASK_CHECK_TYPE                = 1         # Create a Host Check Task
HOST_TASK_PROCESSING_TYPE           = 2         # Create a Host Processing Task
HOST_TASK_UPDATE_STATISTICS_TYPE    = 3         # Create a Host Statistics Update Task
HOST_TASK_CHECK_CONNECTION_TYPE     = 4         # Create a Host Connectivity Check Task 
HOST_TASK_BACKLOG_CONTROL_TYPE      = 5         # Promote discovery backlog into backup
HOST_TASK_BACKLOG_ROLLBACK_TYPE     = 6         # Return backup-pending backlog to discovery
FILE_TASK_BACKUP_TYPE               = 1         # Create a backup task
FILE_TASK_PROCESS_TYPE              = 2         # Process a backup task already stored on the server
FILE_TASK_DISCOVERY                 = 3         # Create a discovery task to get file metadata
#------------------------------------------
# Task Status Constants
#------------------------------------------
TASK_SUSPENDED          = -2        # Task suspended by offline node
TASK_ERROR              = -1        # Task suspended by error manager
TASK_DONE               = 0         # Task completed successfully
TASK_PENDING            = 1         # Task pending execution
TASK_RUNNING            = 2         # Task running

#------------------------------------------
# Station Constants
#------------------------------------------
CELPLAN_HOST_TAG    = "CWSM"
CELPLAN_ZIP_TAG     = "_DONE"
RFEYE_HOST_TAG      = "RFEye"
#------------------------------------------
# Garbage Collector Constants
#------------------------------------------
GC_BATCH_SIZE = 500
GC_QUARANTINE_DAYS = 365
# `resolved_files` keeps superseded source/export artifacts only for short-term
# operator inspection and recovery, so its retention can be shorter than the
# main trash that still backs FILE_TASK_HISTORY error rows.
GC_RESOLVED_FILES_QUARANTINE_DAYS = 60
GC_IDLE_SLEEP = 60
GC_LOOP_SLEEP = 5
