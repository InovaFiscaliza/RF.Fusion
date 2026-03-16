"""
Small shared constants and sentinel objects.

The contents of this module are intentionally minimal and stable because they
are imported from several layers of the application.
"""

from __future__ import annotations

NO_MSG = "none"


# =====================================================================
# Used to insert Null values in DB updates
# =====================================================================
class _ExplicitNull:
    """
    Sentinel object used to explicitly request a database field
    to be updated to NULL.

    This is different from passing None, which means:
    'do not update this field'.
    """
    __slots__ = ()

    def __repr__(self):
        return "<SET_NULL>"


SET_NULL = _ExplicitNull()
