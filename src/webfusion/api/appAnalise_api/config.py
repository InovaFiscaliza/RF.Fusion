"""Keep the appAnalise HTTP contract limits together."""

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
FILTER_NAMES = frozenset({
    "equipmentId", "siteId", "districtId", "stateCode", "startDate",
    "endDate", "freqStart", "freqEnd", "description", "page", "pageSize",
})
