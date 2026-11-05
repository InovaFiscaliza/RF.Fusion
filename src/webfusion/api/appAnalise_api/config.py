"""Keep the appAnalise HTTP contract and query constants together."""

API_PREFIX = "/api/appanalise"
DEFAULT_PAGE = 1
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 1000
MAX_DISTRICTS = 1000
MAX_BODY_BYTES = 65536
FILE_QUERY_CACHE_TTL_SECONDS = 300
FILE_QUERY_CACHE_MAX_ENTRIES = 16
FILE_QUERY_CACHE_MAX_BYTES = 64 * 1024 * 1024
FILE_QUERY_BUILD_CONCURRENCY = 1
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
FILTER_NAMES = frozenset({
    "equipmentId", "siteId", "districtId", "stateCode", "startDate",
    "endDate", "freqStart", "freqEnd", "description", "page", "pageSize",
})
LOCALITY_COLUMNS = [
    "ID_DISTRICT", "LOCALITY_LABEL", "COUNTY_NAME", "STATE_CODE",
    "SITE_COUNT", "SPECTRUM_COUNT", "DATE_START", "DATE_END",
]
