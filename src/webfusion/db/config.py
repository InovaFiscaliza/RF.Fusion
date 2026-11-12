"""Define SQL identifiers, sort whitelists and persistence result contracts."""

FILE_FILTER_TEMP_TABLE = "tmp_appanalise_filtered_spectra"

FILE_COLUMNS = (
    "ID_FILE", "NA_PATH", "NA_FILE", "NA_EXTENSION", "VL_FILE_SIZE_KB",
    "DT_TIME_START", "DT_TIME_END", "NU_FREQ_START", "NU_FREQ_END",
    "NU_SPECTRA", "LOCALITY_COUNT", "LOCALITY_LABELS", "EQUIPMENT_LABELS",
)

REPOSITORY_VOLUME = "reposfi"

TASK_DONE = 0

TASK_PENDING = 1

SUMMARY_SCHEMA = "summary"

SPECTRUM_SCHEMA = "spectrum"

HOST_SCHEMA = "host"

LOCALITY_COLUMNS = [
    "ID_DISTRICT", "LOCALITY_LABEL", "COUNTY_NAME", "STATE_CODE",
    "SITE_COUNT", "SPECTRUM_COUNT", "DATE_START", "DATE_END",
]

HOST_CONNECTION_COLUMNS = frozenset(
    {
        "NA_HOST_PORT",
        "NA_HOST_USER",
        "NA_HOST_PASSWORD",
    }
)

SPECTRUM_FILE_SORT_FIELDS = {
    "date_start": 'ctx."DT_TIME_START"',
    "date_end": 'ctx."DT_TIME_END"',
    "file_name": 'repos."NA_FILE"',
    "spectrum_count": 'ctx."NU_SPECTRA"',
}
# The table belongs to one database connection and is dropped automatically
# when that connection closes. It prevents file search aggregation from
# re-reading the same filtered FACT_SPECTRUM rows multiple times.
SPECTRUM_FILTER_TEMP_TABLE = "tmp_webfusion_filtered_spectra"

HISTORY_PROCESSED_RECENT_INDEX = "file_task_history_idx_fth_processed_recent"
HISTORY_ORDER_BY_ID = 'h."ID_HISTORY" DESC'
HISTORY_ORDER_BY_PROCESSED = 'h."DT_PROCESSED" DESC NULLS LAST, h."ID_HISTORY" DESC'
HISTORY_DATE_FIELDS = {
    "DT_FILE_CREATED_HOST": 'h."DT_FILE_CREATED_HOST"',
    "DT_DISCOVERED": 'h."DT_DISCOVERED"',
    "DT_BACKUP": 'h."DT_BACKUP"',
    "DT_PROCESSED": 'h."DT_PROCESSED"',
}
FILE_TASK_DATE_FIELDS = {
    "DT_FILE_TASK": 'ft."DT_FILE_TASK"',
    "DT_FILE_CREATED_HOST": 'ft."DT_FILE_CREATED_HOST"',
}
HISTORY_PHASE_STATUS_FIELDS = {
    "discovery_status": 'h."NU_STATUS_DISCOVERY"',
    "backup_status": 'h."NU_STATUS_BACKUP"',
    "processing_status": 'h."NU_STATUS_PROCESSING"',
}

USAGE_COUNTER_TABLE = "WEBFUSION_USAGE_METRIC_MONTHLY"
USAGE_CHECKPOINT_TABLE = "WEBFUSION_USAGE_LOG_CHECKPOINT"
