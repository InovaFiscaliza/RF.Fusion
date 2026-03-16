"""
Backward-compatibility helpers kept for older call sites.

This module is intentionally thin and mostly delegates to newer shared modules.
It exists to reduce churn while the codebase converges on the newer abstractions.
"""

from __future__ import annotations

import sys
import os
import time
import json
import random
from datetime import datetime
from typing import Any, Dict, Tuple, Optional, Union
from concurrent.futures import TimeoutError as FuturesTimeoutError

from . import logging_utils
from . import ssh_utils
from . import host_context

# ---------------------------------------------------------------------
# Config import path (as in original code). We keep the behavior so the
# module remains drop-in compatible with existing deployments.
# ---------------------------------------------------------------------
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../../../../../")
)

CONFIG_PATH = os.path.join(BASE_DIR, "etc", "appCataloga")

if CONFIG_PATH not in sys.path:
    sys.path.insert(0, CONFIG_PATH)

import config as k  # noqa: E402  (must be available at runtime)


def parse_filter(filter_raw: Union[str, Dict[str, Any], None], log: Optional[Any] = None) -> Dict[str, Any]:
    """Safely parse and normalize a raw filter (legacy wrapper).

    This function is kept for backward compatibility with older code that
    directly calls `parse_filter()` instead of instantiating `Filter`.

    Args:
        filter_raw (str | dict | None): Raw JSON or dict representing the filter.
        log (optional): Optional logger for diagnostics.

    Returns:
        dict: Normalized filter dictionary with keys:
            - mode (str): 'NONE', 'ALL', 'RANGE', 'LAST', or 'FILE'
            - start_date (str|None)
            - end_date (str|None)
            - last_n_files (int|None)
            - extension (str|None)
            - file_name (str|None)
    """
    return filter.Filter(filter_raw, log=log).data


# =====================================================================
# Socket message parser (public API preserved)
# =====================================================================
def parse_socket_message(
    peername: Tuple[str, int],
    data: str,
    log: Optional[logging_utils.log] = None,
) -> Dict[str, Any]:
    """
    Parse a control message coming from a TCP socket.

    Expected payload (JSON):
    {
        "query_tag": str,
        "host_id": int,
        "host_uid": str,
        "host_add": str,
        "host_port": int,
        "user": str,
        "passwd": str,
        "filter": dict | str (JSON string)
    }
    """

    peer_ip, peer_port = peername

    try:
        payload = json.loads(data)

        # --------------------------------------------------------------
        # Mandatory fields
        # --------------------------------------------------------------
        command   = payload.get("query_tag")
        host_id   = int(payload.get("host_id"))
        host_uid  = payload.get("host_uid")
        host_addr = payload.get("host_add")
        host_port = int(payload.get("host_port"))
        user      = payload.get("user")
        password  = payload.get("passwd")

        # --------------------------------------------------------------
        # Filter normalization
        # --------------------------------------------------------------
        filter_raw = payload.get("filter")

        if isinstance(filter_raw, dict):
            filter_dict = filter_raw
        elif isinstance(filter_raw, str):
            try:
                filter_dict = json.loads(filter_raw)
            except Exception:
                filter_dict = getattr(k, "NONE_FILTER", {"mode": "NONE"})
        else:
            filter_dict = getattr(k, "NONE_FILTER", {"mode": "NONE"})

        return {
            "peer": {"ip": peer_ip, "port": peer_port},
            "command": command,
            "host_id": host_id,
            "host_uid": host_uid,
            "host_addr": host_addr,
            "host_port": host_port,
            "user": user,
            "password": password,
            "filter": filter_dict,
        }

    except Exception as e:
        if log:
            log.entry(f"[parse_socket_message] JSON parse failed: {e} | raw={data}")

        return {
            "peer": {"ip": peer_ip, "port": peer_port},
            "command": None,
            "host_id": None,
            "host_uid": None,
            "host_addr": None,
            "host_port": None,
            "user": None,
            "password": None,
            "filter": getattr(k, "NONE_FILTER", {"mode": "NONE"}),
        }

#-------------------------------------------------------------
# Public API
#-------------------------------------------------------------
def init_host_context(host: dict, log):
    """
    Initialize SFTP connection and hostDaemon context for a given host.

    The caller must close both `daemon` and `sftp_conn` after use.
    Typical usage is inside a try/finally cleanup block.

    Expected host dictionary format:
        {
            "HOST__ID_HOST": ...,
            "HOST__NA_HOST_NAME": ...,
            "HOST__NA_HOST_ADDRESS": ...,
            "HOST__NA_HOST_PORT": ...,
            "HOST__NA_HOST_USER": ...,
            "HOST__NA_HOST_PASSWORD": ...
        }

    Args:
        host (dict): Dictionary containing host metadata, usually obtained
            from DB JOIN operations via `_select_custom()`.
        log: Shared logger instance.

    Returns:
        Tuple[sftpConnection, hostDaemon]:
            A live SFTP connection and a hostDaemon object.
    """

    # --------------------------------------------------------------
    # Extract required HOST fields (raises KeyError if missing)
    # --------------------------------------------------------------
    try:
        host_uid  = host["HOST__NA_HOST_NAME"]
        host_addr = host["HOST__NA_HOST_ADDRESS"]
        port      = int(host["HOST__NA_HOST_PORT"])
        user      = host["HOST__NA_HOST_USER"]
        password  = host["HOST__NA_HOST_PASSWORD"]
    except KeyError as e:
        missing = str(e)
        log.error(f"[INIT] Missing field in host metadata: {missing}")
        raise

    # --------------------------------------------------------------
    # Create SFTP connection object
    # --------------------------------------------------------------
    sftp_conn = ssh_utils.sftpConnection(
        host_uid=host_uid,
        host_addr=host_addr,
        port=port,
        user=user,
        password=password,
        log=log,
    )

    # --------------------------------------------------------------
    # Create daemon associated with the same SFTP session
    # --------------------------------------------------------------
    daemon = host_context.hostDaemon(
        sftp_conn=sftp_conn,
        log=log,
    )

    return sftp_conn, daemon


def _random_jitter_sleep() -> None:
    """Sleep a small random interval to reduce worker polling races."""
    time.sleep(random.uniform(0.5, k.MAX_HOST_TASK_WAIT_TIME))
    


    
