## Detailed WebFusion SQL Query Reference

This section provides a detailed, query-by-query breakdown of all SQL interactions used by the `webfusion` interface to populate maps, dashboards, and data tables. Queries are categorized by their functional area and reference the source module/script where they originate or are executed.

### 1. Operational State Management (Source: `src/webfusion/db.py`)

These queries manage the operational state of hosts within the **`BPDATA`** schema.

#### A. Host Existence Check
*   **Purpose:** Verify if a given host ID exists in the system before attempting an update.
*   **SQL Query:** `SELECT ID_HOST FROM HOST WHERE ID_HOST = %s`
*   **Arguments Used:** `%s` (The integer `host_id`).
*   **Table Accessed:** `BPDATA.HOST`

#### B. Host Connection Update
*   **Purpose:** Persist a specific connection value (Port, User, or Password) for an operational host.
*   **SQL Query:** `UPDATE HOST SET {column} = %s WHERE ID_HOST = %s`
*   **Arguments Used:** `{column}` (e.g., `NA_HOST_PORT`), `%s` (the new `value`), and `%s` (the `host_id`).
*   **Table Accessed:** `BPDATA.HOST`

### 2. Map & Dashboard Data Population (Source: `src/webfusion/app.py`)

These queries are responsible for generating the data displayed on the main map view (`/api/map/stations`) and detailed site views (`/api/map/stations/<site_id>`). They primarily join tables from **`RFDATA`** and **`BPDATA`**.

#### A. Initial Station Map Points (Summary View)
*   **Function:** `get_station_map_points()`
*   **Purpose:** Retrieves a summary list of all active stations for the map overview, typically filtered by date range.
*   **Inferred SQL Structure:** This involves joining:
    1.  `DIM_SPECTRUM_SITE` (Primary source for location data).
    2.  `FACT_SPECTRUM` (To check for recent activity/measurements).
    3.  Potentially `BPDATA.HOST` (To determine if the site has an online host).
*   **Example Query Pattern:** A complex `SELECT DISTINCT` statement joining these tables, filtering by date parameters (`start_date`, `end_date`) and checking for non-null values in key fields like `NU_FREQ_START`.

#### B. Detailed Site View (Popup Metadata)
*   **Function:** `get_station_map_site_detail(site_id, start_date, end_date)`
*   **Purpose:** Retrieves comprehensive metadata for a single selected site ID, including all associated stations and host status checks.
*   **Inferred SQL Structure:** This is highly complex, involving multiple joins:
    1.  `DIM_SPECTRUM_SITE` (Filtered by `site_id`).
    2.  Joins to `DIM_SITE_COUNTY`, `DIM_SITE_DISTRICT`, and `DIM_SITE_STATE` for geographical context.
    3.  Joining with `FACT_SPECTRUM` and potentially `BPDATA.HOST` to check connectivity status within the given date range.

### 3. Core Data Cataloging & Processing (Source: Schema Scripts)

These scripts define the foundational tables and contain sample data loading queries, which represent the *source* of truth for webfusion's analytical data.

#### A. Spectrum Measurement Data (`RFDATA` Schema - `createMeasureDB.sql`)
*   **Purpose:** Defines the core fact table containing all measured spectrum events.
*   **Key Tables/Queries:**
    *   **Fact Table:** `FACT_SPECTRUM`. The primary data is inserted here, linking:
        *   `DIM_SITE_STATE`, `DIM_SITE_COUNTY`, `DIM_SITE_DISTRICT`, `DIM_SPECTRUM_SITE` (Location context).
        *   `DIM_EQUIPMENT`, `DIM_SPECTRUM_DETECTOR`, etc. (Equipment/Measurement context).
    *   **Data Loading:** Uses `LOAD DATA INFILE` statements for static dimensions (`equipmentType.csv`, `IBGE-BR_UF_2020_BULKLOAD.csv`, etc.).

#### B. Operational Task History (`BPDATA` Schema - `createProcessingDB.sql`)
*   **Purpose:** Tracks the lifecycle and history of file processing tasks for a given host.
*   **Key Tables/Queries:**
    *   **Task Tracking:** `HOST_TASK` (Tracks scheduled jobs).
    *   **File History:** `FILE_TASK_HISTORY` (Records every discovery, backup, or process attempt on a file).

### 4. Spectrum Measurement Data Querying (Source: `RFDATA` Schema)

This section focuses specifically on the data retrieved from the core analytical fact table, `FACT_SPECTRUM`, which is the primary source for spectrum visualization and dashboard metrics. These queries are executed when viewing detailed spectra or running historical analyses.

*   **Primary Fact Table:** `FACT_SPECTRUM`
    *   **Purpose:** Stores time-series measurements of spectral characteristics (frequency, power, duration).
    *   **Key Columns Queried:** `NU_FREQ_START`, `NU_FREQ_END`, `DT_TIME_START`, `DT_TIME_END`, `NU_SAMPLE_DURATION`, `NU_TRACE_COUNT`.

*   **Filtering Logic (Runtime):** The queries are almost always constrained by time and location:
    1.  **Time Filtering:** Mandatory use of date range parameters (`start_date` and `end_date`) to limit the scope to relevant measurement periods.
    2.  **Location Filtering:** Use of `site_id` (from `DIM_SPECTRUM_SITE`) or specific geographical coordinates/IDs to narrow results to a physical area.

### 5. Summary of Runtime Filtering Arguments

The webfusion interface uses several key arguments passed via API calls or derived from the current context to filter data across all modules:

| Argument | Type | Source/Context | Purpose | Applicable Queries |
| :--- | :--- | :--- | :--- | :--- |
| **`start_date`** | String (YYYY-MM-DD) | URL Query Parameter (`app.py`) | Defines the beginning of the time window for data retrieval. | Map Points, Site Detail, Spectrum Analysis |
| **`end_date`** | String (YYYY-MM-DD) | URL Query Parameter (`app.py`) | Defines the end of the time window for data retrieval. | Map Points, Site Detail, Spectrum Analysis |
| **`site_id`** | Integer | URL Path Parameter (`app.py`) | Filters all results to a specific geographical site ID. | Site Detail View |
| **`host_id`** | Integer | Function Argument (`db.py`) | Filters operational updates/checks to a single host. | Host Connection Update |
| **`column`** | String | Function Argument (`db.py`) | Dynamically set column (e.g., `NA_HOST_PORT`). | Host Connection Update |

This list serves as the definitive reference for SQL interactions within the WebFusion module, covering both runtime operational queries and the underlying analytical data structure definitions.

*   **`/api/spectrum/...`**: Handles all spectrum-related data, including detailed spectral analysis views that go beyond simple map points.
    *   *(Source Module: `modules/spectrum/routes.py`)*
    *   **Example Parameters:** Typically requires date ranges (`?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`) and often accepts a specific site ID or spectrum ID for detailed analysis, e.g., `/api/spectrum/{site_id}/details`.

*   **`/api/host/...`**: Manages APIs related to individual hosts, potentially for status checks or operational commands not covered by the general host update logic in `db.py`.
    *   *(Source Module: `modules/host/routes.py`)*
    *   **Example Parameters:** Often accepts a mandatory `?host_id=<ID>` parameter to target a specific host's status or configuration, e.g., `/api/host/{host_id}/status`.

*   **`/api/server/...`**: Contains endpoints for usage metrics and server-side data reporting (e.g., page view tracking).
    *   *(Source Module: `modules/server/routes.py`)*
    *   **Example Parameters:** May accept parameters like `?metric_type=<TYPE>` or date ranges to filter the reported usage data, e.g., `/api/server/usage?metric_type=PAGE_VIEW&start_date=...`.

*   **`/api/task/...`**: Manages the lifecycle of background tasks, allowing programmatic interaction with task queues or status checks.
    *   *(Source Module: `modules/task/routes.py`)*
    *   **Example Parameters:** Usually requires a task identifier (`?task_id=<ID>`) and potentially a status filter (`?status=PENDING`), e.g., `/api/task/queue?task_id=123&status=PENDING`.

*   **`/api/maintenance/...`**: Provides endpoints for system maintenance operations (e.g., running health checks, triggering cleanups).
    *   *(Source Module: `modules/maintenance/routes.py`)*
    *   **Example Parameters:** Often requires an action identifier (`?action=HEALTH_CHECK`) or a specific component name to target the operation, e.g., `/api/maintenance/run?action=CLEANUP`.

*   **`/api/users/...`**: Handles user-related APIs, likely for managing roles, permissions, or fetching user profiles.
    *   *(Source Module: `modules/users/routes.py`)*
    *   **Example Parameters:** Common parameters include filtering by role (`?role=ADMIN`) or searching by username (`?username=<USER>`), e.g., `/api/users/search?username=john.doe`.

*   **`/api/zabbix_configuration/...`**: Dedicated endpoints for interacting with Zabbix configuration data within the system.
    *   *(Source Module: `modules/zabbix_configuration/routes.py`)*
    *   **Example Parameters:** May require a specific template ID or host group filter (`?group=<GROUP>`), e.g., `/api/zabbix/templates?group=CORE`.

*   **`/api/alarms/...`**: Provides APIs to query, manage, or acknowledge alarms generated by monitored equipment.
    *   *(Source Module: `modules/alarms/routes.py`)*
    *   **Example Parameters:** Typically filtered by time range (`?start_date=...&end_date=...`) and severity level (`?severity=CRITICAL`), e.g., `/api/alarms/list?start_date=...&severity=HIGH`.

### 7. User Context Retrieval (Architectural Note)
**User identity is NOT retrieved via a dedicated API endpoint like `/api/user/me`.** Instead, the application uses **middleware** (`@app.before_request` in `src/webfusion/app.py`) to intercept every request. This middleware reads user credentials from HTTP headers (passed by the proxy) and stores the identity object in the Flask global context (`flask.g.webfusion_user`), making it available throughout the application's lifecycle for authorization checks and data logging.

### 8. Functional Examples using Root Path
To demonstrate how these endpoints are used in a full URL context, here are examples using the root path `https://fiscalizacao.anatel.gov.br/rffusion/`:

*   **Get Map Data (Summary):**
    `https://fiscalizacao.anatel.gov.br/rffusion/api/map/stations?start_date=2026-09-01&end_date=2026-09-30`

*   **Get Detailed Site Info:**
    `https://fiscalizacao.anatel.gov.br/rffusion/api/map/stations/123?start_date=2026-09-01&end_date=2026-09-30`

*   **Get Spectrum Details:**
    `https://fiscalizacao.anatel.gov.br/rffusion/api/spectrum/site/{site_id}/details?start_date=...&end_date=...`