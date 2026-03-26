# MariaDB Scripts

This directory contains the RF.Fusion database bootstrap material.

In practice, these files define and seed the two project databases:

- `BPDATA`, used for operational state
- `RFDATA`, used for analytical cataloging

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
  Explains how `BPDATA` and `RFDATA` complement each other at the application
  level.

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

## Bootstrap Order

The normal order is:

1. create `BPDATA`
2. create `RFDATA`

That means:

```bash
mysql -u root -p < /RFFusion/src/mariadb/scripts/createProcessingDB-v9.sql
mysql -u root -p < /RFFusion/src/mariadb/scripts/createMeasureDB-v5.sql
```

In practice, the supported path is still the MariaDB container deployment:

- [/RFFusion/install/mariaDB/README.md](/RFFusion/install/mariaDB/README.md)

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

### `BPDATA` and `RFDATA` are complementary, not FK-linked

There is no direct cross-database foreign key bridge between them.

The relationship is application-level:

- `appCataloga` writes to both
- `webfusion` reads both
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
