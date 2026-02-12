#!/usr/bin/python3
"""
TESTE CONTROLADO — RFeye heap investigation

Fonte:
- FILE_TASK_HISTORY (BINs reais, variados)

Pipeline:
1) parse_bin
2) opcional: station.process()

SEM:
- alterar FILE_TASK
- escrever RFDATA
- mover arquivos
"""

import os
import sys
import time
import random
import pandas as pd
import numpy as np
import mysql.connector

# =================================================
# PATH SETUP (idêntico ao worker)
# =================================================
APP_ROOT = "/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga"
if APP_ROOT not in sys.path:
    sys.path.insert(0, APP_ROOT)

CONFIG_PATH = "/RFFusion/src/appCataloga/server_volume/etc/appCataloga"
if CONFIG_PATH not in sys.path:
    sys.path.append(CONFIG_PATH)

# =================================================
# Imports reais
# =================================================
from rfpye.parser import parse_bin
from shared import errors, logging_utils
from db.dbHandlerBKP import dbHandlerBKP
from stations import station_factory
import config as k

log = logging_utils.log(target_screen=True)

# =================================================
# CONFIG
# =================================================
MODE = "validate"      # "parse" | "validate"
BATCH_SIZE = 1000
MAX_TOTAL = 5000
SLEEP_EVERY = 100
SLEEP_SEC = 0.05

# =================================================
# DB
# =================================================
db = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)

log.entry("[TEST] Connecting to DB")
db._connect()

try:
    rows = db._select_custom(
        table="FILE_TASK_HISTORY FTH",
        joins=[
            "JOIN HOST H ON H.ID_HOST = FTH.FK_HOST"
        ],
        where={
            "FTH.NA_EXTENSION": ".bin",
            "FTH.NA_SERVER_FILE_PATH": "NOT_NULL",
            "FTH.NA_SERVER_FILE_NAME": "NOT_NULL",
        },
        order_by="RAND()",   # 🔥 essencial para variabilidade real
        limit=MAX_TOTAL * 2
    )
finally:
    db._disconnect()

if not rows:
    raise RuntimeError("Nenhum BIN encontrado no FILE_TASK_HISTORY")

log.entry(f"[TEST] Loaded {len(rows)} BINs from history")


# Embaralhar → simular produção
random.shuffle(rows)

log.entry(f"[TEST] Loaded {len(rows)} BINs")

processed = 0
parsed_ok = 0
validated_ok = 0
parse_errors = 0
validation_errors = 0
discarded = 0


# =================================================
# MAIN LOOP
# =================================================
for row in rows:

    if processed >= MAX_TOTAL:
        break

    server_path = row["FILE_TASK_HISTORY__NA_SERVER_FILE_PATH"]
    server_name = row["FILE_TASK_HISTORY__NA_SERVER_FILE_NAME"]
    hostname_db = row["HOST__NA_HOST_NAME"]

    filename = f"{server_path}/{server_name}"

    try:
        # -----------------------------
        # ACT I — parse_bin
        # -----------------------------
        raw = parse_bin(filename)

        # -----------------------------
        # ACT II — validation opcional
        # -----------------------------
        if MODE == "parse" and "rfeye" in hostname_db.lower():
            station = station_factory(
                bin_data=raw,
                host_uid=hostname_db
            )
            data= station.process()

        processed += 1
        
        # força alocações grandes pós-parse
        rows = random.randint(500, 4000)
        cols = random.randint(500, 4000)

        dummy = pd.DataFrame(
            np.random.rand(rows, cols)
        )
        
        conn = mysql.connector.connect(
            host="10.88.0.33",
            user="root",
            password="changeme",
            database="BPDATA",
            connection_timeout=1
        )
        conn.close()

    except errors.BinValidationError:
        discarded += 1

    except Exception as e:
        log.error(
            f"[CRITICAL] iter={processed} file={filename} err={e}"
        )
        raise

    finally:
        raw = None
        station = None

    if processed % 50 == 0:
        log.entry(
            f"[PROGRESS] processed={processed} discarded={discarded}"
        )

    if processed % SLEEP_EVERY == 0:
        time.sleep(SLEEP_SEC)

log.entry(
    f"[DONE] processed={processed} discarded={discarded}"
)
