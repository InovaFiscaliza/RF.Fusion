"""Configure operational host read models and activity codes."""

HOST_LOCATION_HISTORY_CACHE_TTL_SECONDS = 120.0

# These values are the durable BPDATA queue contract shared with appCataloga.
# They stay named here because this read-only WebFusion view must distinguish
# a host-level operation from the next file that the backup worker will fetch.
FILE_TASK_BACKUP_TYPE = 1
FILE_TASK_PROCESS_TYPE = 2
FILE_TASK_DISCOVERY_TYPE = 3
TASK_PENDING = 1
TASK_RUNNING = 2
PROCESSED_FILE_SPECTRUM_METADATA_LIMIT = 200

HOST_ACTIVITY_SOURCE = "host-task"
BACKUP_ACTIVITY_SOURCE = "backup-file"
FILE_ACTIVITY_SOURCE = "file-task"

