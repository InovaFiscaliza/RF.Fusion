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

- [createProcessingDB-v9.sql](/RFFusion/src/mariadb/scripts/createProcessingDB-v9.sql)
  Builds `BPDATA`.
  This is the operational database used mainly by `appCataloga`.

- [createMeasureDB-v5.sql](/RFFusion/src/mariadb/scripts/createMeasureDB-v5.sql)
  Builds `RFDATA`.
  This is the measurement and catalog database used by `appCataloga` and
  queried by `webfusion`.

- [createFusionSummaryDB-v1.sql](/RFFusion/src/mariadb/scripts/createFusionSummaryDB-v1.sql)
  Builds `RFFUSION_SUMMARY`.
  This is the materialized summary layer intended to serve `webfusion` and
  external consumers without repeatedly scanning the heaviest source tables.

### Migration scripts

- [alterProcessingDB-v10-error-fields.sql](/RFFusion/src/mariadb/scripts/alterProcessingDB-v10-error-fields.sql)
  Adds structured error fields to the operational processing tables in
  `BPDATA`.

- [alterFusionSummaryDB-v2-error-aggregation.sql](/RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v2-error-aggregation.sql)
  Refines the `RFFUSION_SUMMARY` error layer by removing the persisted
  per-event staging table and aggregating directly from a virtual canonical
  error view.

- [alterFusionSummaryDB-v3-refresh-events.sql](/RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v3-refresh-events.sql)
  Enables periodic `RFFUSION_SUMMARY` refreshes through MariaDB Events with a
  named lock to prevent overlapping runs.

- [alterFusionSummaryDB-v4-discovered-files.sql](/RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v4-discovered-files.sql)
  Fixes the discovered-file totals in `HOST_CURRENT_SNAPSHOT` so the server
  dashboard no longer depends on the stale `BPDATA.HOST.NU_HOST_FILES`
  counter.

- [alterFusionSummaryDB-v5-atomic-read-refresh.sql](/RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v5-atomic-read-refresh.sql)
  Replaces the read-facing summary refreshes used by `webfusion` with
  shadow-table swaps so the UI keeps seeing the previous snapshot while the
  next one is being rebuilt.

- [alterFusionSummaryDB-v6-safe-refresh-diagnostics.sql](/RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v6-safe-refresh-diagnostics.sql)
  Improves the scheduled `RFFUSION_SUMMARY` refresh wrapper so lock skips are
  reported correctly and real SQL failures carry the original database error.

- [alterMeasureDB-v6-fact-spectrum-performance.sql](/RFFusion/src/mariadb/scripts/alterMeasureDB-v6-fact-spectrum-performance.sql)
  Adds the composite lookup index used by the appAnalise worker idempotency
  check on `RFDATA.FACT_SPECTRUM`.

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
mysql -u root -p < /RFFusion/src/mariadb/scripts/createProcessingDB-v9.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/createMeasureDB-v5.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/createFusionSummaryDB-v1.sql
```

In practice, the supported path is still the MariaDB container deployment:

- [/RFFusion/install/mariaDB/README.md](/RFFusion/install/mariaDB/README.md)

For an environment that already has `RFFUSION_SUMMARY` `v1`, apply the error
aggregation refinement after the bootstrap:

```bash
mysql -u root -p < /RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v2-error-aggregation.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v3-refresh-events.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v4-discovered-files.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v5-atomic-read-refresh.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/alterFusionSummaryDB-v6-safe-refresh-diagnostics.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/alterMeasureDB-v6-fact-spectrum-performance.sql
```

## Important Operational Notes

### `createMeasureDB-v5.sql` depends on repository paths

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

That design is documented in:

- [DB_INTERCONNECTIONS.md](/RFFusion/src/mariadb/scripts/DB_INTERCONNECTIONS.md)

## Editing Guidance

When changing these scripts, keep these rules in mind.

### Treat bootstrap scripts as versioned contracts

The filenames are versioned for a reason.

If a change alters schema shape, seed format or bootstrap semantics in a
meaningful way, prefer introducing a new versioned script instead of silently
rewriting history.

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
