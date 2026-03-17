# RF.Fusion Database Interconnections

This document summarizes the practical relationships between the two main
project databases:

- `BPDATA`
- `RFDATA`

It focuses on the tables that are most relevant to `appCataloga` and
`webfusion`.

## 1. System-Level Data Flow

At the project level, the databases are complementary rather than directly
linked by cross-database foreign keys.

```mermaid
flowchart LR
    appCataloga[appCataloga workers]
    webfusion[webfusion]
    bp[(BPDATA)]
    rf[(RFDATA)]
    repos[/reposfi repository/]

    appCataloga -->|queue, host state, file tasks, history| bp
    appCataloga -->|spectra, files, sites, equipment| rf
    appCataloga -->|reads/writes payloads| repos

    webfusion -->|host query, task creation, operational views| bp
    webfusion -->|spectrum query, map, file lookup| rf
    webfusion -->|download through nginx| repos
```

## 2. Important Architectural Note

There is currently **no direct foreign key bridge between `BPDATA` and
`RFDATA`**.

The interconnection between both databases is mostly **application-level**:

- `appCataloga` discovers, backs up, and processes files
- `BPDATA` stores queue and operational state
- `RFDATA` stores measurement catalog data and repository file metadata
- `webfusion` reads both worlds and reconciles them when needed

This is why some integrations, especially station-to-host correlation, still
depend on naming heuristics rather than explicit relational keys.

## 3. BPDATA Core Relationships

`BPDATA` is the operational database for hosts and pipeline state.

```mermaid
erDiagram
    HOST {
        INT ID_HOST PK
        VARCHAR NA_HOST_NAME
        VARCHAR NA_HOST_ADDRESS
        BOOLEAN IS_OFFLINE
        BOOLEAN IS_BUSY
        INT NU_PID
        DATETIME DT_BUSY
        DATETIME DT_LAST_BACKUP
        DATETIME DT_LAST_PROCESSING
        DATETIME DT_LAST_DISCOVERY
    }

    HOST_TASK {
        INT ID_HOST_TASK PK
        INT FK_HOST FK
        TINYINT NU_TYPE
        DATETIME DT_HOST_TASK
        TINYINT NU_STATUS
        INT NU_PID
        JSON FILTER
        TEXT NA_MESSAGE
    }

    FILE_TASK {
        INT ID_FILE_TASK PK
        INT FK_HOST FK
        DATETIME DT_FILE_TASK
        TINYINT NU_TYPE
        VARCHAR NA_HOST_FILE_PATH
        VARCHAR NA_HOST_FILE_NAME
        VARCHAR NA_SERVER_FILE_PATH
        VARCHAR NA_SERVER_FILE_NAME
        TINYINT NU_STATUS
        INT NU_PID
        VARCHAR NA_EXTENSION
        BIGINT VL_FILE_SIZE_KB
        DATETIME DT_FILE_CREATED
        DATETIME DT_FILE_MODIFIED
        TEXT NA_MESSAGE
    }

    FILE_TASK_HISTORY {
        INT ID_HISTORY PK
        INT FK_HOST FK
        DATETIME DT_DISCOVERED
        DATETIME DT_BACKUP
        DATETIME DT_PROCESSED
        INT NU_STATUS_DISCOVERY
        INT NU_STATUS_BACKUP
        INT NU_STATUS_PROCESSING
        VARCHAR NA_HOST_FILE_PATH
        VARCHAR NA_HOST_FILE_NAME
        VARCHAR NA_SERVER_FILE_PATH
        VARCHAR NA_SERVER_FILE_NAME
        BIGINT VL_FILE_SIZE_KB
        TEXT NA_MESSAGE
    }

    HOST ||--o{ HOST_TASK : queues
    HOST ||--o{ FILE_TASK : owns
    HOST ||--o{ FILE_TASK_HISTORY : audits
```

### BPDATA Role

This database answers operational questions such as:

- which hosts are online, offline, or busy
- which host tasks are pending, running, or failed
- which file tasks are pending backup or processing
- what happened to a given file during discovery, backup, and processing

## 4. RFDATA Core Relationships

`RFDATA` is the measurement and repository catalog database.

```mermaid
erDiagram
    DIM_EQUIPMENT_TYPE {
        INT ID_EQUIPMENT_TYPE PK
        VARCHAR NA_EQUIPMENT_TYPE
        VARCHAR NA_EQUIPMENT_TYPE_UID
    }

    DIM_SPECTRUM_EQUIPMENT {
        INT ID_EQUIPMENT PK
        INT FK_EQUIPMENT_TYPE FK
        VARCHAR NA_EQUIPMENT
    }

    DIM_SITE_STATE {
        INT ID_STATE PK
        VARCHAR NA_STATE
        VARCHAR LC_STATE
    }

    DIM_SITE_COUNTY {
        INT ID_COUNTY PK
        INT FK_STATE FK
        VARCHAR NA_COUNTY
    }

    DIM_SITE_DISTRICT {
        INT ID_DISTRICT PK
        INT FK_COUNTY FK
        VARCHAR NA_DISTRICT
    }

    DIM_SITE_TYPE {
        INT ID_TYPE PK
        VARCHAR NA_TYPE
    }

    DIM_SPECTRUM_SITE {
        INT ID_SITE PK
        INT FK_DISTRICT FK
        INT FK_COUNTY FK
        INT FK_STATE FK
        INT FK_TYPE FK
        VARCHAR NA_SITE
        POINT GEO_POINT
        DECIMAL NU_ALTITUDE
    }

    DIM_FILE_TYPE {
        INT ID_TYPE_FILE PK
        VARCHAR NA_TYPE_FILE
        VARCHAR NA_EQUIPMENT
    }

    DIM_SPECTRUM_FILE {
        INT ID_FILE PK
        INT ID_TYPE_FILE FK
        VARCHAR NA_FILE
        VARCHAR NA_PATH
        VARCHAR NA_VOLUME
        VARCHAR NA_EXTENSION
        VARCHAR NU_MD5
        BIGINT VL_FILE_SIZE_KB
        DATETIME DT_FILE_CREATED
        DATETIME DT_FILE_MODIFIED
    }

    FACT_SPECTRUM {
        INT ID_SPECTRUM PK
        INT FK_SITE FK
        INT FK_EQUIPMENT FK
        VARCHAR NA_DESCRIPTION
        DECIMAL NU_FREQ_START
        DECIMAL NU_FREQ_END
        DATETIME DT_TIME_START
        DATETIME DT_TIME_END
        INT NU_TRACE_COUNT
        DECIMAL NU_RBW
        JSON JS_METADATA
    }

    BRIDGE_SPECTRUM_FILE {
        INT FK_FILE FK
        INT FK_SPECTRUM FK
    }

    DIM_EQUIPMENT_TYPE ||--o{ DIM_SPECTRUM_EQUIPMENT : classifies
    DIM_SITE_STATE ||--o{ DIM_SITE_COUNTY : contains
    DIM_SITE_STATE ||--o{ DIM_SPECTRUM_SITE : locates
    DIM_SITE_COUNTY ||--o{ DIM_SITE_DISTRICT : contains
    DIM_SITE_COUNTY ||--o{ DIM_SPECTRUM_SITE : locates
    DIM_SITE_DISTRICT ||--o{ DIM_SPECTRUM_SITE : refines
    DIM_SITE_TYPE ||--o{ DIM_SPECTRUM_SITE : classifies
    DIM_FILE_TYPE ||--o{ DIM_SPECTRUM_FILE : classifies
    DIM_SPECTRUM_SITE ||--o{ FACT_SPECTRUM : hosts
    DIM_SPECTRUM_EQUIPMENT ||--o{ FACT_SPECTRUM : produces
    FACT_SPECTRUM ||--o{ BRIDGE_SPECTRUM_FILE : links
    DIM_SPECTRUM_FILE ||--o{ BRIDGE_SPECTRUM_FILE : links
```

### RFDATA Role

This database answers catalog and analysis questions such as:

- where stations are geographically located
- which equipment produced the measurement
- which spectra belong to a site or equipment
- which repository file is linked to one or more spectra

## 5. Practical Interconnection Points Between BPDATA and RFDATA

Even without SQL foreign keys across databases, there are clear business-level
intersections:

### 5.1 appCataloga pipeline

- `BPDATA.FILE_TASK` and `BPDATA.FILE_TASK_HISTORY` track operational progress
- `RFDATA.DIM_SPECTRUM_FILE` stores the repository-side canonical file record
- `RFDATA.FACT_SPECTRUM` and `RFDATA.BRIDGE_SPECTRUM_FILE` connect spectra to
  files

### 5.2 webfusion map

- `RFDATA.DIM_SPECTRUM_SITE` provides coordinates
- `RFDATA.FACT_SPECTRUM` and `DIM_SPECTRUM_EQUIPMENT` provide site/equipment
  context
- `BPDATA.HOST` provides online/offline and host identity
- host resolution is inferred by matching equipment and host names

### 5.3 downloads

- `RFDATA.DIM_SPECTRUM_FILE` provides `NA_PATH`, `NA_FILE`, and `NA_VOLUME`
- the web layer maps those records to the repository mount and serves the file

## 6. What Is Strongly Modeled vs. Heuristic

Strongly modeled:

- host -> host_task
- host -> file_task
- host -> file_task_history
- site -> spectrum
- equipment -> spectrum
- file <-> spectrum

Still heuristic:

- site -> host
- equipment -> host
- operational correlation between a `BPDATA` file task and the final `RFDATA`
  spectrum/file catalog without an explicit shared foreign key

## 7. Recommended Usage of This Diagram

This document is meant to support:

- onboarding
- schema discussions
- future webfusion modules
- operational reasoning around `appCataloga`

It is intentionally focused on the tables that shape the system behavior today,
not on an exhaustive dump of every dimension table.
