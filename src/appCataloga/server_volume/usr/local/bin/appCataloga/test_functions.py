# ======================================================================
# Imports
# ======================================================================
import sys
import os
import time
import signal
import inspect
import subprocess
import paramiko
from datetime import datetime

# ----------------------------------------------------------------------
# Load configuration and database modules
# ----------------------------------------------------------------------
_CFG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../etc/appCataloga"))
if _CFG_DIR not in sys.path and os.path.isdir(_CFG_DIR):
    sys.path.append(_CFG_DIR)

_DB_DIR = os.path.join(os.path.dirname(__file__), "db")
if _DB_DIR not in sys.path and os.path.isdir(_DB_DIR):
    sys.path.append(_DB_DIR)

import shared as sh
from db.dbHandlerBKP import dbHandlerBKP
import config as k

def main():
    host_uid = "CWSM211001"
    is_windows = "CW" in host_uid
    
    print(is_windows)
if __name__ == "__main__":
    main()