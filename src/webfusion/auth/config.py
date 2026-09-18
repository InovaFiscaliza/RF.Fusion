"""Define the WebFusion user-profile and access-role constants."""

USER_LIST_LIMIT = 200
ADMIN_ROLE = "admin"
DEVELOPER_ROLE = "developer"
USER_ROLES = frozenset({ADMIN_ROLE, DEVELOPER_ROLE})
USER_ROLE_FILTERS = frozenset({"all", "admin", "developer", "none"})
PROFILE_IMAGE_URL_MAX_LENGTH = 2048
PROFILE_IMAGE_REFRESH_SECONDS = 6 * 60 * 60
PROFILE_IMAGE_RETRY_SECONDS = 5 * 60
PROFILE_IMAGE_CACHE_LIMIT = 2000
