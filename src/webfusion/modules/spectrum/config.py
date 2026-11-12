"""Configure spectrum query limits, sorting and cache versions."""

ALLOWED_SORT_ORDERS = ["ASC", "DESC"]


SPECTRUM_QUERY_CACHE_TTL_SECONDS = 30
EQUIPMENT_CACHE_TTL_SECONDS = 300
FILE_PATH_CACHE_TTL_SECONDS = 300
# Full file-result pages reuse one heavy grouped query result for a few minutes
# so operators can move across pagination links without re-scanning the catalog.
FILE_RESULT_FULL_CACHE_TTL_SECONDS = 300


# Bump these integers whenever the query shape changes in a way that would make
# cached results from the previous version wrong or incomplete. In-process
# cache entries are keyed by version so old entries are naturally skipped.
FILE_RESULT_CACHE_VERSION = 4
FILTER_OPTION_CACHE_VERSION = 3

