# WebFusion

`webfusion` is the web interface of RF.Fusion. It was built to help operators
and developers inspect the platform without opening SQL clients or manually
triggering maintenance commands.

In practice, WebFusion is the human-facing layer for four recurring jobs:

- inspect station points on the Brazil map
- query spectra and repository files
- inspect host and server operational state
- create `HOST_TASK` entries for appCataloga

## Runtime Model

The web stack is intentionally small:

- Flask provides routing and template rendering
- `waitress` serves the WSGI application
- `nginx` fronts the app in the deployment container
- `nginx` also serves repository downloads directly from `/mnt/reposfi`

This separation matters because the repository is mounted from CIFS. Letting
`nginx` serve the file directly is much faster than pushing the whole payload
through Flask.

## Main Pages

### Home

- route: `/`
- template: [index.html](/RFFusion/src/webfusion/templates/index.html)
- purpose:
  - show the station map
  - provide quick navigation to the other modules
  - expose quick actions from the popup of each plotted site

The home page renders first and loads station points asynchronously so the map
does not block the whole application start.

Important details:

- the map plots processed-spectrum localities, not the full Zabbix/Grafana host
  inventory
- the station filter is a searchable combobox
- duplicate station names in the same city are disambiguated with a more human
  label:
  - district/locality name when available
  - fallback `ponto <ID_SITE>` only when no better distinction exists
- popup actions are loaded on demand so the map can render before host
  reconciliation is complete

### Spectrum

- routes: [routes.py](/RFFusion/src/webfusion/modules/spectrum/routes.py)
- queries: [service.py](/RFFusion/src/webfusion/modules/spectrum/service.py)
- template: [spectrum.html](/RFFusion/src/webfusion/templates/spectrum/spectrum.html)

The spectrum module supports two complementary views:

- `Spectrum` mode
  - one row per spectrum
  - useful when the spectrum itself is the object of analysis

- `File` mode
  - one row per repository file
  - useful when the operator wants to avoid repeating the same official file
    path across many spectra
  - includes expandable linked spectra

Current behavior worth knowing:

- the locality filter follows the selected equipment/station, not the date
  range
- locality options are loaded asynchronously and cached for short periods
- in `File` mode, the page requires at least one narrowing filter:
  - locality
  - start date
  - end date
- when a locality is selected but the result set is empty, the page can show
  the observed date range available for that equipment/locality pair
- downloads prefer direct `nginx` URLs and only fall back to Flask when needed

### Server

- route: `/server`
- route code: [routes.py](/RFFusion/src/webfusion/modules/server/routes.py)
- shared aggregation logic: [service.py](/RFFusion/src/webfusion/modules/host/service.py)
- template: [server.html](/RFFusion/src/webfusion/templates/server/server.html)

The server page is the global operational dashboard. It concentrates:

- total hosts, online, offline, and busy
- monthly backup consumption
- global backup and processing counts
- grouped processing errors
- shared infrastructure status:
  - RAM
  - `/mnt/reposfi`
  - `appAnalise` connectivity

Important rule:
the summary cards are global, while the table at the bottom can be filtered for
navigation.

Performance note:

- heavy global cards are loaded after the page shell appears
- grouped processing diagnostics are fetched only when the panel is expanded

### Host

- route: `/host`
- route code: [routes.py](/RFFusion/src/webfusion/modules/host/routes.py)
- service code: [service.py](/RFFusion/src/webfusion/modules/host/service.py)
- template: [host.html](/RFFusion/src/webfusion/templates/host/host.html)

The host page is station-focused. It should answer questions like:

- is this station online?
- when was the last discovery, backup, or processing event?
- how much backup was consumed this month?
- how many files are pending or failing?
- what processing errors are most common for this host?

The page intentionally avoids loading everything up front:

- grouped processing errors are fetched on demand when the diagnostic panel is
  opened
- reconciled locality history from `RFDATA` is loaded asynchronously after the
  station shell is rendered
- the locality history answers not only "where is this host now?" but also
  "where has this station been observed over time?"

### Task

- route prefix: `/task`
- route code: [routes.py](/RFFusion/src/webfusion/modules/task/routes.py)
- service code: [service.py](/RFFusion/src/webfusion/modules/task/service.py)
- filter builder: [builder.py](/RFFusion/src/webfusion/modules/task/builder.py)
- templates:
  - [task_builder.html](/RFFusion/src/webfusion/templates/task/task_builder.html)
  - [task_list.html](/RFFusion/src/webfusion/templates/task/task_list.html)

This is the write-oriented module of WebFusion. It inserts or refreshes
`HOST_TASK` rows in `BPDATA` for:

- backup
- update statistics
- check connection

The current protection layer is intentionally simple: HTTP Basic Auth with
bootstrap credentials.

## Data Sources

WebFusion reads from two databases:

- `RFDATA`
  - sites
  - equipment
  - spectra
  - repository file metadata

- `BPDATA`
  - host inventory
  - host state counters
  - host task queue
  - file-task history

The map is a good example of how both worlds meet:

1. geographic points come from `RFDATA.DIM_SPECTRUM_SITE`
2. host actions come from `BPDATA.HOST`
3. the association is resolved in Python through tolerant name matching

## Code Guide

For someone less familiar with web services, these files are the best starting
points:

1. [app.py](/RFFusion/src/webfusion/app.py)
   - creates the Flask app
   - registers blueprints
   - exposes `/health`
   - serves the station-map JSON APIs

2. [db.py](/RFFusion/src/webfusion/db.py)
   - centralizes database connection helpers

3. Feature route modules
   - each route module reads HTTP parameters
   - delegates the heavy work to a service module
   - renders the final template

4. Feature service modules
   - contain the SQL and aggregation rules
   - prepare the dictionaries consumed by the templates

This split is intentional: route files stay short, while service files hold the
domain logic.

## Downloads

Repository-backed downloads follow this preferred path:

1. the query identifies the file path
2. the table renders a direct `/downloads/...` URL when possible
3. `nginx` serves the file from `/mnt/reposfi`

Fallback Flask routes still exist, but they are not the preferred path for
large downloads.

## Async APIs

Several pages now rely on small JSON endpoints to keep the first render fast.
The most important ones are:

- `/api/map/stations`
- `/api/map/stations/<site_id>`
- `/api/spectrum/localities`
- `/api/spectrum/file/<file_id>/spectra`
- `/api/server/summary-metrics`
- `/api/server/processing-errors`
- `/api/host/<host_id>/locations`
- `/api/host/<host_id>/processing-errors`

## Directory Layout

```text
src/webfusion/
├── app.py
├── db.py
├── modules/
│   ├── host/
│   ├── map/
│   ├── server/
│   ├── spectrum/
│   └── task/
├── static/
│   ├── css/
│   ├── fonts/
│   ├── img/
│   └── js/
└── templates/
    ├── host/
    ├── popup/
    ├── server/
    ├── spectrum/
    └── task/
```

## Screenshot

If the home screen screenshot should live in the repository, the recommended
path is:

`/RFFusion/src/webfusion/static/img/docs/webfusion-home-screen.png`

And the Markdown reference is:

```md
![WebFusion Home Screen](./static/img/docs/webfusion-home-screen.png)
```

## Deployment

The production-oriented container for WebFusion is documented in:

- [README.MD](/RFFusion/install/webserver/README.MD)

That container keeps `nginx`, `waitress`, and `sshd` in a single runtime.

## Operational Notes

- Database credentials are still embedded in
  [db.py](/RFFusion/src/webfusion/db.py). This is practical, but not a strong
  long-term security posture.
- The task module still uses bootstrap Basic Auth credentials in code.
- Some map host associations remain heuristic because the schema does not
  expose a direct `site -> host` foreign key.
- WebFusion logs go to the container/process `stdout` and `stderr`; no
  application file log is configured inside the Flask app today.
