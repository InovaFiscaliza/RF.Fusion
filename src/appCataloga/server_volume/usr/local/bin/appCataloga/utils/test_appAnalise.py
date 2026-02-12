#!/usr/bin/python3
"""
Test script for CelplanStation integration.

This script simulates RF.Fusion behavior by providing
file_path and file_name separately to CelplanStation,
which internally composes the absolute filepath and
calls the remote CelPlan processing service.
"""

import sys
import os
import json

# -------------------------------------------------
# Ensure appCataloga root is in PYTHONPATH
# -------------------------------------------------
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# -------------------------------------------------
# Imports AFTER path fix
# -------------------------------------------------
from stations.celplan import CelplanStation
from shared import errors


def main():
    # -------------------------------------------------
    # Test input (RF.Fusion model)
    # -------------------------------------------------
    file_path = r"C:\Celplan"
    file_name = (
        "CWSM21100018_E11_A1_Spec Frq=1940.000 "
        "Span=460.000 RBW=100.00000 "
        "[2025-12-15,11-09-28-538-3475].dbm"
    )

    # -------------------------------------------------
    # CelPlan service endpoint (test environment)
    # -------------------------------------------------
    host_uid = "CWSM21100018"
    host_add = "192.168.104.101"
    host_port = 8910

    # -------------------------------------------------
    # Instantiate station
    # -------------------------------------------------
    station = CelplanStation(
        bin_data=None,
        host_uid=host_uid
    )

    # -------------------------------------------------
    # Execute processing
    # -------------------------------------------------
    try:
        result = station.process(file_path, file_name)

    except errors.BinValidationError as e:
        print(json.dumps({
            "status_query": 0,
            "message_query": str(e)
        }, ensure_ascii=False, indent=2))
        return

    # -------------------------------------------------
    # Success
    # -------------------------------------------------
    print(json.dumps({
        "status_query": 1,
        "response": result
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
