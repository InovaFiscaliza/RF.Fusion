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
    return pymysql.connect(**DB_CFG_RFDATA)

def get_connection_bpdata():
    return pymysql.connect(**DB_CFG_BPDATA)
