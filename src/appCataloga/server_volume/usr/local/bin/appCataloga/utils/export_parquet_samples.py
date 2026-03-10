#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Export relationally consistent RF.Fusion sample dataset to Parquet.
Fully defensive version.
"""

import os
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd

# =================================================
# Configuration
# =================================================

SAMPLE_SIZE = 10000

OUTPUT_DIR = (
    f"rf_fusion_sample_"
    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
)

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
from shared import logging_utils
from db.dbHandlerBKP import dbHandlerBKP
from db.dbHandlerRFM import dbHandlerRFM

log = logging_utils.log("export_rf_fusion_sample")

# =================================================
# Helpers
# =================================================

def sql_escape(v):
    return str(v).replace("'", "''")


def safe_column_exists(df, column):
    return df is not None and not df.empty and column in df.columns


def safe_get_ids(df, column):
    if not safe_column_exists(df, column):
        return []
    return list(set(df[column].dropna().tolist()))


def save_parquet(df, name):

    if df is None or df.empty:
        log.warning(f"{name}: empty dataset")
        return

    path = os.path.join(OUTPUT_DIR, f"{name}.parquet")

    log.entry(f"Exporting {name} ({len(df)} rows)")

    df.to_parquet(
        path,
        engine="pyarrow",
        compression="snappy",
        index=False
    )


def fetch_df(db, query):

    try:
        db.cursor.execute(query)
        rows = db.cursor.fetchall()

        if not rows:
            return pd.DataFrame()

        columns = [c[0] for c in db.cursor.description]

        return pd.DataFrame(rows, columns=columns)

    except Exception as e:
        log.warning(f"Query failed: {e}")
        return pd.DataFrame()


def fetch_dim(db, table, key, ids):

    if not ids:
        return pd.DataFrame()

    ids = list(set(ids))
    ids_str = ",".join(map(str, ids))

    query = f"""
        SELECT *
        FROM {table}
        WHERE {key} IN ({ids_str})
    """

    return fetch_df(db, query)

# =================================================
# Main
# =================================================

def main():

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    log.entry("Connecting to databases...")

    db_bp = dbHandlerBKP(database=k.BKP_DATABASE_NAME, log=log)
    db_rfm = dbHandlerRFM(database=k.RFM_DATABASE_NAME, log=log)

    db_bp._connect()
    db_rfm._connect()

    # =================================================
    # SAMPLE FACT
    # =================================================

    fact = fetch_df(db_rfm, f"""
        SELECT *
        FROM FACT_SPECTRUM
        ORDER BY RAND()
        LIMIT {SAMPLE_SIZE}
    """)

    save_parquet(fact, "FACT_SPECTRUM")

    if fact.empty:
        log.warning("FACT_SPECTRUM sample empty")
        return

    spectrum_ids = safe_get_ids(fact, "ID_SPECTRUM")

    if not spectrum_ids:
        log.warning("No spectrum IDs found")
        return

    ids_str = ",".join(map(str, spectrum_ids))

    # =================================================
    # BRIDGE FILE
    # =================================================

    bridge_file = fetch_df(db_rfm, f"""
        SELECT *
        FROM BRIDGE_SPECTRUM_FILE
        WHERE FK_SPECTRUM IN ({ids_str})
    """)

    save_parquet(bridge_file, "BRIDGE_SPECTRUM_FILE")

    file_ids = safe_get_ids(bridge_file, "FK_FILE")

    dim_file = fetch_dim(db_rfm, "DIM_SPECTRUM_FILE", "ID_FILE", file_ids)

    save_parquet(dim_file, "DIM_SPECTRUM_FILE")

    # =================================================
    # DIMENSIONS FROM FACT
    # =================================================

    site_ids = safe_get_ids(fact, "FK_SITE")
    detector_ids = safe_get_ids(fact, "FK_DETECTOR")
    trace_ids = safe_get_ids(fact, "FK_TRACE_TYPE")
    unit_ids = safe_get_ids(fact, "FK_MEASURE_UNIT")
    proc_ids = safe_get_ids(fact, "FK_PROCEDURE")
    equip_ids = safe_get_ids(fact, "FK_EQUIPMENT")

    # GEO_POINT convertido para texto + lat/lon

    if site_ids:

        ids_str = ",".join(map(str, site_ids))

        site = fetch_df(db_rfm, f"""
        SELECT
            ID_SITE,
            FK_DISTRICT,
            FK_COUNTY,
            FK_STATE,
            FK_TYPE,
            NA_SITE,
            ST_AsText(GEO_POINT) AS GEO_POINT,
            ST_X(GEO_POINT) AS LONGITUDE,
            ST_Y(GEO_POINT) AS LATITUDE,
            NU_ALTITUDE,
            NU_GNSS_MEASUREMENTS,
            GEOGRAPHIC_PATH
        FROM DIM_SPECTRUM_SITE
        WHERE ID_SITE IN ({ids_str})
        """)

    else:
        site = pd.DataFrame()

    detector = fetch_dim(db_rfm, "DIM_SPECTRUM_DETECTOR", "ID_DETECTOR", detector_ids)
    trace = fetch_dim(db_rfm, "DIM_SPECTRUM_TRACE_TYPE", "ID_TRACE_TYPE", trace_ids)
    unit = fetch_dim(db_rfm, "DIM_SPECTRUM_UNIT", "ID_MEASURE_UNIT", unit_ids)
    procedure = fetch_dim(db_rfm, "DIM_SPECTRUM_PROCEDURE", "ID_PROCEDURE", proc_ids)
    equipment = fetch_dim(db_rfm, "DIM_SPECTRUM_EQUIPMENT", "ID_EQUIPMENT", equip_ids)

    save_parquet(site, "DIM_SPECTRUM_SITE")
    save_parquet(detector, "DIM_SPECTRUM_DETECTOR")
    save_parquet(trace, "DIM_SPECTRUM_TRACE_TYPE")
    save_parquet(unit, "DIM_SPECTRUM_UNIT")
    save_parquet(procedure, "DIM_SPECTRUM_PROCEDURE")
    save_parquet(equipment, "DIM_SPECTRUM_EQUIPMENT")

    # =================================================
    # SITE DEPENDENCIES
    # =================================================

    county_ids = safe_get_ids(site, "FK_COUNTY")
    state_ids = safe_get_ids(site, "FK_STATE")
    district_ids = safe_get_ids(site, "FK_DISTRICT")
    type_ids = safe_get_ids(site, "FK_TYPE")

    save_parquet(fetch_dim(db_rfm, "DIM_SITE_COUNTY", "ID_COUNTY", county_ids), "DIM_SITE_COUNTY")
    save_parquet(fetch_dim(db_rfm, "DIM_SITE_STATE", "ID_STATE", state_ids), "DIM_SITE_STATE")
    save_parquet(fetch_dim(db_rfm, "DIM_SITE_DISTRICT", "ID_DISTRICT", district_ids), "DIM_SITE_DISTRICT")
    save_parquet(fetch_dim(db_rfm, "DIM_SITE_TYPE", "ID_TYPE", type_ids), "DIM_SITE_TYPE")

    # =================================================
    # EQUIPMENT TYPE
    # =================================================

    equip_type_ids = safe_get_ids(equipment, "FK_EQUIPMENT_TYPE")

    save_parquet(
        fetch_dim(db_rfm, "DIM_EQUIPMENT_TYPE", "ID_EQUIPMENT_TYPE", equip_type_ids),
        "DIM_EQUIPMENT_TYPE"
    )

    # =================================================
    # BRIDGE EMITTER
    # =================================================

    bridge_emit = fetch_df(db_rfm, f"""
        SELECT *
        FROM BRIDGE_SPECTRUM_EMITTER
        WHERE FK_SPECTRUM IN ({ids_str})
    """)

    save_parquet(bridge_emit, "BRIDGE_SPECTRUM_EMITTER")

    emit_ids = safe_get_ids(bridge_emit, "FK_EMITTER")

    save_parquet(
        fetch_dim(db_rfm, "DIM_SPECTRUM_EMITTER", "ID_EMITTER", emit_ids),
        "DIM_SPECTRUM_EMITTER"
    )

    # =================================================
    # BPDATA CONSISTENT SAMPLE
    # =================================================

    if safe_column_exists(dim_file, "NA_PATH") and safe_column_exists(dim_file, "NA_FILE"):

        tuples = [
            f"('{sql_escape(row.NA_PATH)}','{sql_escape(row.NA_FILE)}')"
            for row in dim_file.itertuples()
        ]

        if tuples:

            tuple_str = ",".join(tuples)

            history = fetch_df(db_bp, f"""
                SELECT *
                FROM FILE_TASK_HISTORY
                WHERE (NA_SERVER_FILE_PATH, NA_SERVER_FILE_NAME) IN ({tuple_str})
            """)

            save_parquet(history, "FILE_TASK_HISTORY")

            host_ids = safe_get_ids(history, "FK_HOST")

            host = fetch_dim(db_bp, "HOST", "ID_HOST", host_ids)
            save_parquet(host, "HOST")

            save_parquet(
                fetch_dim(db_bp, "HOST_TASK", "FK_HOST", host_ids),
                "HOST_TASK"
            )

    log.entry("Export complete.")

    db_bp.close()
    db_rfm.close()


if __name__ == "__main__":
    main()