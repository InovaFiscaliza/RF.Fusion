import pymysql

DB_CFG = {
    "host": "10.88.0.33",
    "port": 3306,
    "user": "root",
    "password": "changeme",
    "database": "RFDATA",
    "cursorclass": pymysql.cursors.DictCursor
}

def get_connection():
    return pymysql.connect(**DB_CFG)
