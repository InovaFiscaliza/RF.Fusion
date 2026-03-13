#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
RF.Fusion MeasureDB Export → Parquet (MATLAB compatible)
Streaming export for large datasets.
"""

import os
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



# =================================================
# Configuration
# =================================================

CHUNK_SIZE = 100000

OUTPUT_DIR = f"rf_measure_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
OUTPUT_FILE = "measure_db.parquet"

# =================================================
# Resolve paths
# =================================================

SCRIPT_PATH = Path(__file__).resolve()

SERVER_VOLUME = None
for p in SCRIPT_PATH.parents:
    if p.name == "server_volume":
        SERVER_VOLUME = p
        break

if SERVER_VOLUME is None:
    raise RuntimeError("server_volume not found")

APP_ROOT = SERVER_VOLUME / "usr" / "local" / "bin" / "appCataloga"
DB_ROOT = APP_ROOT / "db"
SHARED_ROOT = APP_ROOT / "shared"
ETC_ROOT = SERVER_VOLUME / "etc" / "appCataloga"


def safe_add_path(path: Path):
    if path.exists() and str(path) not in sys.path:
        sys.path.insert(0, str(path))


safe_add_path(ETC_ROOT)
safe_add_path(APP_ROOT)
safe_add_path(DB_ROOT)
safe_add_path(SHARED_ROOT)

# =================================================
# Imports
# =================================================

import config as k
from db.dbHandlerRFM import dbHandlerRFM
from shared import logging_utils

# =================================================
# Query
# =================================================

QUERY = """
SELECT

    CAST(se.NA_EQUIPMENT AS CHAR(100)) AS NA_EQUIPMENT,

    CAST(ST_Y(ss.GEO_POINT) AS DOUBLE) AS LATITUDE,
    CAST(ST_X(ss.GEO_POINT) AS DOUBLE) AS LONGITUDE,

    SHA1(
        CONCAT(
            FORMAT(ST_Y(ss.GEO_POINT),6,'en_US'),
            ' - ',
            FORMAT(ST_X(ss.GEO_POINT),6,'en_US')
        )
    ) AS _HASH_LATITUDE_LONGITUDE,

    CAST(fs.NA_DESCRIPTION AS CHAR(3000)) AS NA_DESCRIPTION,

    CAST(fs.NU_FREQ_START AS DOUBLE) AS NU_FREQ_START,
    CAST(fs.NU_FREQ_END   AS DOUBLE) AS NU_FREQ_END,

    fs.DT_TIME_START,
    fs.DT_TIME_END,

    CAST(sf.NA_PATH AS CHAR(3000)) AS NA_PATH,
    CAST(sf.NA_FILE AS CHAR(200)) AS NA_FILE,

    CAST(sf.NA_EXTENSION AS CHAR(20)) AS NA_EXTENSION,

    CAST(sf.VL_FILE_SIZE_KB AS SIGNED) AS VL_FILE_SIZE_KB

FROM FACT_SPECTRUM fs

JOIN DIM_SPECTRUM_SITE ss
    ON ss.ID_SITE = fs.FK_SITE

JOIN DIM_SPECTRUM_EQUIPMENT se
    ON se.ID_EQUIPMENT = fs.FK_EQUIPMENT

JOIN BRIDGE_SPECTRUM_FILE bf
    ON bf.FK_SPECTRUM = fs.ID_SPECTRUM

JOIN DIM_SPECTRUM_FILE sf
    ON sf.ID_FILE = bf.FK_FILE
    AND sf.NA_VOLUME = 'reposfi'
"""

# =================================================
# MATLAB dtype normalization
# =================================================

def normalize_matlab_types(df):

    for col in df.columns:

        dtype = df[col].dtype

        try:

            if pd.api.types.is_float_dtype(dtype):
                df[col] = df[col].astype("float32")

            elif pd.api.types.is_integer_dtype(dtype):
                df[col] = df[col].astype("int32")

            elif pd.api.types.is_datetime64_any_dtype(dtype):
                df[col] = df[col].astype("datetime64[ms]")

            else:
                df[col] = df[col].astype(str)

        except Exception as e:
            print(f"[WARNING] dtype conversion failed for {col}: {e}")

    return df


# =================================================
# Streaming Export
# =================================================

def export_parquet_stream(db, query, output_path):

    print("[INFO] Executing query...")

    db.cursor.execute(query)

    columns = [c[0] for c in db.cursor.description]

    writer = None
    total_rows = 0

    start_time = datetime.now()

    while True:

        rows = db.cursor.fetchmany(CHUNK_SIZE)

        if not rows:
            break

        df = pd.DataFrame(rows, columns=columns)

        df = normalize_matlab_types(df)

        table = pa.Table.from_pandas(df, preserve_index=False)

        if writer is None:

            writer = pq.ParquetWriter(
                output_path,
                table.schema,
                compression=None
            )

            print("[INFO] Parquet writer initialized")

        writer.write_table(table)

        total_rows += len(df)

        elapsed = (datetime.now() - start_time).total_seconds()

        rate = total_rows / elapsed if elapsed > 0 else 0

        print(
            f"[INFO] Exported {total_rows:,} rows "
            f"({rate:,.0f} rows/sec)"
        )

    if writer:
        writer.close()

    return total_rows


# =================================================
# Main
# =================================================

def main():

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)

    print("[INFO] Connecting to RFDATA...")
    log = logging_utils.log("export_rf_fusion_sample")
    db = dbHandlerRFM(database=k.RFM_DATABASE_NAME,log=log)
    db._connect()

    print("[INFO] Starting export")

    total = export_parquet_stream(
        db,
        QUERY,
        output_path
    )

    print()
    print("[INFO] Export complete")
    print(f"[INFO] Rows written : {total:,}")
    print(f"[INFO] File         : {output_path}")


if __name__ == "__main__":
    main()