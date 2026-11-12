"""Define the scope and limits of the MariaDB dump migration to PostgreSQL."""

from pathlib import Path

# Explicit destination selection; the tool never reuses application settings.
CONNINFO_ENV = "RFFUSION_MIGRATION_PG_CONNINFO"

DDL_DIR = Path(__file__).resolve().parents[1] / "scripts"
# Bootstrap order matters: RFDATA installs PostGIS before the geometry column.
DDL_SCRIPTS = (
    ("BPDATA", "createProcessingDB.sql"),
    ("RFDATA", "createMeasureDB.sql"),
    ("RFFUSION_SUMMARY", "createFusionSummaryDB.sql"),
    ("WEBFUSION", "createWebFusionDB.sql"),
)
TARGET_SCHEMAS = tuple(schema for schema, _ in DDL_SCRIPTS)

# The canonical Summary aggregates are rebuilt by full reconcile; only the usage
# counters and their NGINX checkpoint are restored, and always together.
SUMMARY_SCHEMA = "RFFUSION_SUMMARY"
SUMMARY_RESTORED_TABLES = frozenset({
    "WEBFUSION_USAGE_LOG_CHECKPOINT",
    "WEBFUSION_USAGE_METRIC_MONTHLY",
})

# Unique keys that exist only in PostgreSQL. Duplicated rows must be consolidated
# before the load. Values are reported only for catalog labels, never for paths.
POSTGRES_ONLY_UNIQUE_KEYS = {
    ("RFDATA", "DIM_SPECTRUM_EQUIPMENT"): (("NA_EQUIPMENT",), True),
    ("RFDATA", "DIM_SPECTRUM_DETECTOR"): (("NA_DETECTOR",), True),
    ("RFDATA", "DIM_SPECTRUM_TRACE_TYPE"): (("NA_TRACE_TYPE",), True),
    ("RFDATA", "DIM_SPECTRUM_UNIT"): (("NA_MEASURE_UNIT",), True),
    ("RFDATA", "DIM_SPECTRUM_PROCEDURE"): (("NA_PROCEDURE",), True),
    ("RFDATA", "DIM_SPECTRUM_FILE"): (("NA_VOLUME", "NA_PATH", "NA_FILE"), False),
}

COPY_BUFFER_CHARS = 8 * 1024 * 1024
PROGRESS_ROWS = 1_000_000
GUARD_CHECK_ROWS = 250_000
REPORT_SAMPLE_LIMIT = 20
