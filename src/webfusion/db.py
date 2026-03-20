"""Small database connection helpers for WebFusion.

WebFusion reads from two schemas:

- ``RFDATA`` for spectrum and repository metadata
- ``BPDATA`` for hosts, queues, and processing history

Keeping the connection helpers in one place makes the service modules easier to
read for anyone who is still getting comfortable with Flask applications.
"""

import pymysql

DB_CFG_RFDATA = {
    "host": "10.88.0.33",
    "port": 3306,
    "user": "root",
    "password": "changeme",
    "database": "RFDATA",
    "cursorclass": pymysql.cursors.DictCursor
}

DB_CFG_BPDATA = {
    "host": "10.88.0.33",
    "port": 3306,
    "user": "root",
    "password": "changeme",
    "database": "BPDATA",
    "cursorclass": pymysql.cursors.DictCursor
}

def get_connection_rfdata():
    """Open a DictCursor connection to the spectrum catalog database."""
    return pymysql.connect(**DB_CFG_RFDATA)


def get_connection_bpdata():
    """Open a DictCursor connection to the operational host/queue database."""
    return pymysql.connect(**DB_CFG_BPDATA)
