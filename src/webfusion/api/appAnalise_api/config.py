"""Keep the appAnalise HTTP contract and query constants together."""

API_PREFIX = "/api/appanalise"
DEFAULT_PAGE = 1
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 1000
MAX_DISTRICTS = 1000
MAX_BODY_BYTES = 65536
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
