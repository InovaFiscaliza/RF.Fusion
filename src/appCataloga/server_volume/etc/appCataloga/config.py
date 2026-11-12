#!/usr/bin/env python
"""Constants used in the appCataloga scripts

Require additional /etc/appCataloga/.secret file with the following content:

        DB_USER_NAME = 'appCataloga'
        DB_PASSWORD = '<app_pass>'
"""

import os, runpy
from psycopg.conninfo import make_conninfo

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
LOG_MAX_FILE_SIZE_MB = 100        # Per-entrypoint limit; oldest lines are dropped in place, keeping the newest half (MB)

#------------------------------------------
# appCataloga socket service configuration
#------------------------------------------
SERVER_PORT             = 5555
TOTAL_CONNECTIONS       = 50
GATEWAY_SELECT_TIMEOUT_SEC = 1
BACKUP_QUERY_TAG        = "backup"
STOP_QUERY_TAG          = "stop"
METRICS_QUERY_TAG       = "metrics"
GATEWAY_BACKUP_SUCCESS_MESSAGE = "Host backup request queued successfully"
GATEWAY_STOP_SUCCESS_MESSAGE = "Host stop request queued successfully"
GATEWAY_METRICS_SUCCESS_MESSAGE = "Host operational metrics read successfully"
START_TAG               = "<json>"
END_TAG                 = "</json>"
#------------------------------------------
# database configuration
#------------------------------------------
SERVER_NAME             = "10.88.0.35"
DB_PORT                 = 5432
PG_DATABASE_NAME        = "RFFUSION"
DB_USER_NAME_PG         = secret.get("DB_USER_NAME_PG", "postgres")
DB_PASSWORD_PG          = secret.get("DB_PASSWORD_PG")
PG_USER_NAME            = os.environ.get("PGUSER") or DB_USER_NAME_PG
RFM_DATABASE_NAME       = "RFDATA"
BKP_DATABASE_NAME       = "BPDATA"
SUMMARY_DATABASE_NAME   = "RFFUSION_SUMMARY"
# Legacy reference settings are not used for PostgreSQL authentication.
DB_USER_NAME            = secret["DB_USER_NAME"]
DB_PASSWORD             = secret["DB_PASSWORD"]
DB_LOG_CONNECTION_LIFECYCLE = False   # Log connect/close/reconnect lifecycle only when explicitly debugging DB sessions
PG_CONNINFO = os.environ.get("RFFUSION_PG_CONNINFO") or make_conninfo(
    host=SERVER_NAME,
    port=DB_PORT,
    dbname=PG_DATABASE_NAME,
    user=PG_USER_NAME,
    password=os.environ.get("PGPASSWORD", DB_PASSWORD_PG),
    connect_timeout=5,
)  # Use dedicated PostgreSQL credentials, never legacy MariaDB settings.
# =================================================
# APP_ANALISE remote processing service
# =================================================
APP_ANALISE_HOST_ADD        = "WIMATLABPDIN01"
#APP_ANALISE_HOST_ADD            = "192.168.104.117"

APP_ANALISE_HOST_PORT       = 8910
APP_ANALISE_BUFFER_SIZE     = 4096
APP_ANALISE_KEY             = "123456"
APP_ANALISE_CLIENT_NAME     = "Matlab"
# Timeout requested from appAnalise itself. Keep it lower than the RF.Fusion
# socket timeout so appAnalise can return a structured ReadTimeout before this
# client gives up on the connection.
APP_ANALISE_REQUEST_TIMEOUT_SECONDS = 540
APP_ANALISE_PROCESS_TIMEOUT = 1800
APP_ANALISE_CONNECT_TIMEOUT = 15
APP_ANALISE_CWSM_FILENAME_UTC_OFFSET_HOURS  = 3

APP_ANALISE_MULTI_SITE_REPO_SUBDIR          = "appanalise_multi_site"
APP_ANALISE_WORKER_DETAIL                   = "worker=APP_ANALISE"
APP_ANALISE_EMPTY_SPEC_DATA_DETAIL          = "handlers:FileReadHandler:EmptySpecData"
APP_ANALISE_NO_SPECTRAL_DATA_DETAIL         = "model:fileReader:CRFSBin:NoSpectralData"
APP_ANALISE_NO_READABLE_FILES_IN_ZIP_DETAIL = "model:SpecDataBase:NoReadableFilesInZip"
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
HOST_CHECK_ALL_BATCH_SIZE = 25             # Max hosts per idle sweep batch
HOST_CHECK_ALL_ICMP_TIMEOUT_SEC = 5       # Short ICMP timeout for background sweep
HOST_CHECK_ALL_ICMP_MAX_WORKERS = HOST_CHECK_ALL_BATCH_SIZE  # Parallel ICMP fan-out for the maintenance sweep
HOST_CHECK_ALL_PROBE_MAX_WORKERS = 8      # Parallel short SSH probe fan-out for the maintenance sweep
#------------------------------------------
# backup module configuration
#------------------------------------------
MAX_HOST_TASK_WAIT_TIME         = 2         # seconds to wait for a new task. Minimum half, maximum equal to this value
MAX_FILE_TASK_WAIT_TIME         = 30        # seconds to wait for a new task. Minimum half, maximum equal to this value
HOST_BUSY_TIMEOUT               = 18000     # 18000 seconds or 5 hours
HOST_CLEANUP_INTERVAL           = 300       # Interval in seconds to check for and clean up stale host locks
HOST_CLEANUP_NO_TASK_GRACE_SEC  = 30        # Minimum BUSY age before releasing a host with no running tasks detected
SFTP_BUSY_COOLDOWN_SECONDS      = 15        # Temporary host cooldown after transient SSH/SFTP init failure
DISCOVERY_RESERVATION_TTL_SEC   = 300       # Fresh pending PROCESSING HOST_TASK reserves the next host window for discovery
HOST_TASK_OPERATIONAL_STALE_SEC = 300       # Recover stale RUNNING operational HOST_TASK when ownership/liveness no longer matches execution reality
HOST_CHECK_SSH_PROBE_TIMEOUT_SEC = 40       # Short SSH probe timeout for host_check operational confirmation
HOST_CHECK_SSH_TIMEOUT_CONFIRMATIONS = 3    # Consecutive SSH timeout confirmations required before suspending a pingable host
BKP_TASK_MAX_WORKERS            = 10        # Number of concurrent backup workers. Set to 1 to avoid overloading the network and SFTP server, which can cause more harm than good when multiple workers are contending for the same resources.
BKP_TASK_IDLE_EXIT_CYCLES       = 3         # extra workers exit after this many idle polls
MIN_FILE_SIZE_KB                = 1         # minimum file size to be backed up in KB
FILE_THRESHOLD_SIZE_KB          = 100       # file size threshold for update file
BACKUP_TRANSFER_MAX_SECONDS     = HOST_BUSY_TIMEOUT   # Absolute upper bound for one SFTP transfer
BACKUP_TRANSFER_STALL_TIMEOUT_SECONDS = 900           # Abort when a transfer makes no progress for too long
BACKUP_TRANSFER_PROGRESS_POLL_SECONDS = 30           # How often to inspect callback/local file growth
BACKUP_TRANSFER_HEARTBEAT_SECONDS = 300              # Periodic progress log while a large transfer is still alive
BACKUP_TRANSFER_PROGRESS_PERSIST_SECONDS = 5         # Minimum interval for live FILE_TASK transfer progress

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
# Error Stage Constants
# Used in err.capture(stage=k.STAGE_*) and err.stage comparisons.
# Never use inline strings — a typo here fails silently at runtime.
#------------------------------------------
STAGE_MAIN          = "MAIN"
STAGE_AUTH          = "AUTH"
STAGE_CONNECT       = "CONNECT"
STAGE_SSH           = "SSH"
STAGE_LOCK_TASK     = "LOCK_TASK"
STAGE_DISCOVERY     = "DISCOVERY"
STAGE_BACKLOG       = "BACKLOG"
STAGE_TRANSFER      = "TRANSFER"
STAGE_PROCESS       = "PROCESS"
STAGE_CONNECTIVITY  = "CONNECTIVITY"
STAGE_SITE          = "SITE"
STAGE_DB            = "DB"
STAGE_FS            = "FS"
STAGE_FINALIZE      = "FINALIZE"
STAGE_LOCK          = "LOCK"
STAGE_HOST_READ     = "HOST_READ"
STAGE_HOST_STATUS   = "HOST_STATUS"
STAGE_UPDATE_STATS  = "UPDATE_STATS"
STAGE_TRANSACTION   = "TRANSACTION"
STAGE_QUEUE         = "QUEUE"
STAGE_HOST_CREATE   = "HOST_CREATE"
STAGE_COMMAND       = "COMMAND"
STAGE_PARSE         = "PARSE"
STAGE_READ          = "READ"

# Connectivity probe state identifiers — used in match/case and comparisons
HOST_CONN_ONLINE     = "online"
HOST_CONN_OFFLINE    = "offline"
HOST_CONN_DEGRADED   = "degraded"
HOST_CONN_AUTH_ERROR = "auth_error"

# Stable operational error codes consumed by host-state persistence and
# dashboard projections.
ERROR_CODE_GPS_GNSS_UNAVAILABLE =       "GPS_GNSS_UNAVAILABLE"
HOST_OFFLINE_DESCRIPTION =              "ICMP unreachable"
SSH_FAILURE_CODE_AUTHENTICATION =       "AUTHENTICATION"
SSH_FAILURE_CODE_CONNECTIVITY =         "CONNECTIVITY"
SSH_FAILURE_DESCRIPTION =               "SSH failure"

# Log event names for host connectivity checks
EVENT_HOST_CHECK       = "host_check"
EVENT_CHECK_CONNECTION = "host_check_connection"

#------------------------------------------
# Station Constants
#------------------------------------------
CELPLAN_HOST_TAG    = "CWSM"
CELPLAN_ZIP_TAG     = "_DONE"

#------------------------------------------
# Garbage Collector Constants
#------------------------------------------
GC_BATCH_SIZE = 500
GC_QUARANTINE_DAYS = 60
# `resolved_files` keeps superseded source/export artifacts only for short-term
# operator inspection and recovery, so its retention can be shorter than the
# main trash that still backs FILE_TASK_HISTORY error rows.
GC_RESOLVED_FILES_QUARANTINE_DAYS = 7
GC_IDLE_SLEEP = 60
# Process one bounded cleanup batch per minute to keep filesystem and database
# pressure predictable on the production VM.
GC_LOOP_SLEEP = 60

#------------------------------------------
# RFFUSION_SUMMARY incremental worker
#------------------------------------------
# These values control the Python worker that consumes
# `RFFUSION_SUMMARY.SUMMARY_OUTBOX`
# and refreshes the public `RFFUSION_SUMMARY` tables without reviving the old
# heavy MariaDB event refresh path.
SUMMARY_WORKER_CONSUMER_NAME = "rffusion_summary_worker"
SUMMARY_WORKER_BATCH_SIZE = 500
# How long the worker can have been offline before the startup reconcile is
# considered mandatory.  If DT_LAST_SUCCESS is more recent than this threshold,
# the startup reconcile is skipped and the worker begins with incremental
# processing, catching up via the outbox.  Default: 1 hour (3600 seconds).
SUMMARY_WORKER_STALE_THRESHOLD_SEC = 3600
