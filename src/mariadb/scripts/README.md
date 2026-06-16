# MariaDB Scripts

This directory contains the RF.Fusion database bootstrap material.

In practice, these files define and seed the three project databases:

- `BPDATA`, used for operational state
- `RFDATA`, used for analytical cataloging
- `RFFUSION_SUMMARY`, used for materialized cross-database summaries

The MariaDB container deployment applies these scripts after the database
service is up. So this directory is the schema source of truth for the project
bootstrap process.

## What Each File Does

### Schema scripts

- [createProcessingDB.sql](/RFFusion/src/mariadb/scripts/createProcessingDB.sql)
  Builds `BPDATA`.
  This is the operational database used mainly by `appCataloga`.

- [createMeasureDB.sql](/RFFusion/src/mariadb/scripts/createMeasureDB.sql)
  Builds `RFDATA`.
  This is the measurement and catalog database used by `appCataloga` and
  queried by `webfusion`.

- [createFusionSummaryDB.sql](/RFFusion/src/mariadb/scripts/createFusionSummaryDB.sql)
  Builds `RFFUSION_SUMMARY`.
  This is the materialized summary layer intended to serve `webfusion` and
  external consumers without repeatedly scanning the heaviest source tables.

### Seed files

- [equipmentType.csv](/RFFusion/src/mariadb/scripts/equipmentType.csv)
  Seed data for `DIM_EQUIPMENT_TYPE`.

- [fileType.csv](/RFFusion/src/mariadb/scripts/fileType.csv)
  Seed data for `DIM_FILE_TYPE`.

- [measurementUnit.csv](/RFFusion/src/mariadb/scripts/measurementUnit.csv)
  Seed data for `DIM_SPECTRUM_UNIT`.

- [IBGE-BR_UF_2020_BULKLOAD.csv](/RFFusion/src/mariadb/scripts/IBGE-BR_UF_2020_BULKLOAD.csv)
  State-level geography seed.

- [IBGE-BR_Municipios_2020_BULKLOAD.csv](/RFFusion/src/mariadb/scripts/IBGE-BR_Municipios_2020_BULKLOAD.csv)
  County-level geography seed.

### Documentation and support

- [DB_INTERCONNECTIONS.md](/RFFusion/src/mariadb/scripts/DB_INTERCONNECTIONS.md)
  Explains how `BPDATA`, `RFDATA` and `RFFUSION_SUMMARY` complement each other
  at the application level.

- [README_ORPHANED_MAINTENANCE.md](/RFFusion/src/mariadb/scripts/README_ORPHANED_MAINTENANCE.md)
  Documents the orphaned-task maintenance utilities and their operating model.

- [ANALYSIS_orphaned_file_tasks.md](/RFFusion/src/mariadb/scripts/ANALYSIS_orphaned_file_tasks.md)
  Captures the operational analysis that motivated the maintenance scripts.

- [environment.yml](/RFFusion/src/mariadb/scripts/environment.yml)
  Conda environment file historically used by the project runtime. It is not
  the main schema artifact, but it remains useful when reproducing the RF.Fusion
  environment locally.

## Database Responsibilities

### `BPDATA`

`BPDATA` stores operational workflow state.

Main tables:

- `HOST`
- `HOST_TASK`
- `FILE_TASK`
- `FILE_TASK_HISTORY`

This database answers questions such as:

- which host is offline or busy
- which host task is pending or running
- which file failed discovery, backup or processing

### `RFDATA`

`RFDATA` stores measurement and repository catalog data.

Main areas:

- site dimensions
- equipment dimensions
- file dimensions
- procedure / detector / unit / trace dimensions
- `FACT_SPECTRUM`
- bridge tables

This database answers questions such as:

- where a spectrum was measured
- which equipment produced it
- which repository file contains it

### `RFFUSION_SUMMARY`

`RFFUSION_SUMMARY` stores materialized, cross-database read models.

Main areas:

- host/equipment reconciliation
- site/equipment observation summaries
- map-ready site and station snapshots
- host monthly metrics
- canonicalized error events and grouped error summaries
- server-wide current snapshot cards
- refresh telemetry for the Python summary worker

This database answers questions such as:

- which host most plausibly owns one measurement equipment
- what the current and historical locality of a host is
- which marker state the map should render for a site
- which grouped backup or processing errors are most frequent
- which server-wide counters should be shown without re-scanning history

## Bootstrap Order

The normal order is:

1. create `BPDATA`
2. create `RFDATA`
3. create `RFFUSION_SUMMARY`

That means:

```bash
mysql -u root -p < /RFFusion/src/mariadb/scripts/createProcessingDB.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/createMeasureDB.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/createFusionSummaryDB.sql
```

In practice, the supported path is still the MariaDB container deployment:

- [/RFFusion/install/mariaDB/README.md](/RFFusion/install/mariaDB/README.md)

## Important Operational Notes

### `createMeasureDB.sql` depends on repository paths

The `RFDATA` bootstrap script uses `LOAD DATA INFILE` with paths such as:

- `/RFFusion/src/mariadb/scripts/equipmentType.csv`
- `/RFFusion/src/mariadb/scripts/fileType.csv`
- `/RFFusion/src/mariadb/scripts/measurementUnit.csv`

So the script assumes the project repository is mounted at `/RFFusion` inside
the runtime where MariaDB executes the SQL.

### Geography seed matters to site resolution

`appCataloga` site creation depends on state and county reference data loaded
from the IBGE CSV files.

If those files are missing, broken or not loaded, `RFDATA` site insertion can
fail later even if the schema itself was created successfully.

### `BPDATA`, `RFDATA` and `RFFUSION_SUMMARY` are complementary, not FK-linked

There is no direct cross-database foreign key bridge between them.

The relationship is application-level:

- `appCataloga` writes to both
- `webfusion` reads both
- `RFFUSION_SUMMARY` materializes cross-database joins and grouped aggregates
- repository artifacts help reconcile the operational and analytical worlds

### Summary maintenance is now Python-owned

The canonical refresh path for `RFFUSION_SUMMARY` is no longer the legacy
MariaDB event scheduler.

Today the model is:

- `createFusionSummaryDB.sql` creates `SUMMARY_OUTBOX` and `SUMMARY_WORKER_STATE` in `RFFUSION_SUMMARY`
- `appCataloga` publishers coalesce dirty scopes into that outbox
- `appCataloga_summary_database.py` consumes the outbox and refreshes the public summary tables
- `createFusionSummaryDB.sql` still defines the public summary schema and its diagnostics tables

This preserves the `RFFUSION_SUMMARY` contract for `webfusion`, MATLAB and
other readers while avoiding heavy periodic `INSERT ... SELECT` refreshes on
hot operational tables during the day.

That design is documented in:

- [DB_INTERCONNECTIONS.md](/RFFusion/src/mariadb/scripts/DB_INTERCONNECTIONS.md)

## Editing Guidance

When changing these scripts, keep these rules in mind.

### Treat bootstrap scripts as canonical contracts

The current repository keeps one canonical bootstrap script per database.

If schema shape, seed format or bootstrap semantics change, update the
corresponding `create*` script so a fresh deployment always lands directly on
the current supported state.

For the current repository state, the three `create*` scripts are the only
canonical bootstrap SQL artifacts in this directory.

### Keep seed files aligned with runtime assumptions

Examples:

- `fileType.csv` affects how `appCataloga` classifies repository artifacts
- `equipmentType.csv` affects equipment typing in `RFDATA`
- `measurementUnit.csv` affects spectrum dimensional consistency

### Do not confuse operational and analytical concerns

If a table is about queue state, retries, host lifecycle or workflow auditing,
it belongs in `BPDATA`.

If a table is about sites, equipment, files or spectra, it belongs in
`RFDATA`.

## Related Documentation

- [/RFFusion/README.md](/RFFusion/README.md)
- [/RFFusion/install/mariaDB/README.md](/RFFusion/install/mariaDB/README.md)
- [/RFFusion/src/appCataloga/README.md](/RFFusion/src/appCataloga/README.md)
- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/db/README.md](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/db/README.md)
