# webfusion

`webfusion` is the operator-facing web application for RF.Fusion.

Its job is to expose the platform state through a browser-friendly interface so
operators can inspect spectra, monitor hosts, review server health, browse the
repository, and trigger a narrow set of queue-driven actions without touching
SQL clients or the `appCataloga` runtime directly.

This document focuses on the application that lives in this directory. For the
full platform context, start with [README.md](/RFFusion/README.md).

## What WebFusion Is Responsible For

WebFusion has four practical responsibilities:

1. render the landing page and station map used as the main navigation surface
2. expose spectrum and repository-file browsing flows
3. expose server-wide and per-host operational dashboards
4. translate selected operator actions into durable `HOST_TASK` rows

The application is intentionally read-heavy. Most of the code exists to shape
database results into HTML or focused JSON responses that the browser can use
incrementally.

## Runtime Model

The runtime stays deliberately small:

- Flask handles routing, JSON responses, and template rendering
- Jinja renders the initial HTML pages
- `waitress` serves the WSGI app
- `nginx` fronts the container and serves repository-backed downloads when
  possible

That split matters because `/mnt/reposfi` is commonly mounted from CIFS. Large
download delivery is much cheaper through `nginx` than through Python.

## Data Sources

WebFusion reads from three MariaDB schemas, each with a different role.

### `BPDATA`

Operational data used by host and task features:

- host inventory and liveness
- `HOST_TASK` queue state
- `FILE_TASK` queue state
- `FILE_TASK_HISTORY` execution history

### `RFDATA`

Analytical spectrum and repository metadata:

- spectra and locality dimensions
- equipment catalog
- repository file links

### `RFFUSION_SUMMARY`

Materialized read models used to avoid repeated heavy joins and grouped scans.

Current important uses:

- the landing-page map reads from `MAP_SITE_SUMMARY` and
  `MAP_SITE_STATION_SUMMARY`
- the `/server` dashboard reads from `SERVER_CURRENT_SUMMARY`,
  `SERVER_ERROR_SUMMARY`, and `HOST_CURRENT_SNAPSHOT`

This schema is now the official source for those fast global views.

## Request And Rendering Model

Most WebFusion pages combine one server-rendered HTML shell with focused lazy
endpoints.

### Initial page render

1. the browser requests a Flask route such as `/server` or `/spectrum`
2. the route reads query parameters from `request.args`
3. one or more service helpers query and shape the data
4. the route calls `render_template(...)`
5. Jinja returns the final HTML response

### Lazy browser updates

1. the HTML exposes DOM hooks and `data-*` values
2. page JavaScript calls narrow JSON endpoints with `fetch(...)`
3. Flask routes call feature services again for that smaller payload
4. the browser updates only the affected section

This is how WebFusion keeps the first render small while still exposing richer
detail panels, dependent filters, and expandable rows.

## Code Organization

The codebase follows a stable split of responsibilities.

### `app.py`

Application entrypoint.

It creates the Flask app, registers feature blueprints, exposes `/health`, and
owns the landing-page map endpoints.

### `db.py`

Centralized database connection helpers for `BPDATA`, `RFDATA`, and
`RFFUSION_SUMMARY`.

### `modules/*/routes.py`

HTTP entrypoints.

Common responsibilities:

- normalize request parameters
- choose between HTML and JSON responses
- keep failure handling consistent for the browser

### `modules/*/service.py`

Feature data layer.

Common responsibilities:

- issue SQL queries
- shape raw rows into UI-oriented payloads
- apply lightweight per-process caches when useful
- keep feature semantics close to the queries that implement them

### `templates/`

Jinja templates used for the initial HTML render.

### `static/js/`

Browser-side behavior for lazy endpoints, incremental refresh, and table/map
interactions.

## Feature Overview

### Home And Map

- route shell: `/`
- APIs: `/api/map/stations`, `/api/map/stations/<site_id>`
- service: [service.py](/RFFusion/src/webfusion/modules/map/service.py)
- template: [index.html](/RFFusion/src/webfusion/templates/index.html)

The home page renders quickly and lets the browser fetch the station map data
after the shell is visible.

Important behavior:

- map points come from `RFFUSION_SUMMARY`, not from live heavy joins
- popup details are cached alongside the same snapshot used by the points
- marker state distinguishes online/offline and current/historical locality

### Spectrum

- routes: [routes.py](/RFFusion/src/webfusion/modules/spectrum/routes.py)
- service: [service.py](/RFFusion/src/webfusion/modules/spectrum/service.py)
- template: [spectrum.html](/RFFusion/src/webfusion/templates/spectrum/spectrum.html)

This module offers two complementary result grains:

- `Spectrum` mode: one row per spectrum
- `File` mode: one row per repository file, with spectra loaded lazily

Important behavior:

- downloads prefer direct `nginx` delivery
- locality options are loaded dynamically from the active filter state
- file mode intentionally ignores spectrum-specific filters such as frequency
  because one repository file can contain multiple spectra

### Server

- route: `/server`
- route code: [routes.py](/RFFusion/src/webfusion/modules/server/routes.py)
- service support: [service.py](/RFFusion/src/webfusion/modules/host/service.py)
- template: [server.html](/RFFusion/src/webfusion/templates/server/server.html)

This is the global operational dashboard.

It answers questions such as:

- how many hosts are online, offline, or busy
- how many files are pending, queued, done, or failing
- what the dominant grouped backup and processing errors are
- whether shared runtime dependencies such as `appAnalise` and `/mnt/reposfi`
  look healthy

Important behavior:

- summary cards stay global
- page filters affect only the host table used for navigation
- grouped diagnostics are loaded lazily

### Host

- route: `/host`
- route code: [routes.py](/RFFusion/src/webfusion/modules/host/routes.py)
- service: [service.py](/RFFusion/src/webfusion/modules/host/service.py)
- template: [host.html](/RFFusion/src/webfusion/templates/host/host.html)

This page is the per-station drill-down.

It combines:

- current counters from `BPDATA.HOST`
- queue and historical file-task metrics from `BPDATA`
- reconciled locality history from `RFDATA`

The host-to-equipment association is still heuristic because there is no
direct cross-database foreign key.

### Task Builder

- route prefix: `/task`
- route code: [routes.py](/RFFusion/src/webfusion/modules/task/routes.py)
- service: [service.py](/RFFusion/src/webfusion/modules/task/service.py)
- builder support: [builder.py](/RFFusion/src/webfusion/modules/task/builder.py)

This is the write-oriented module of WebFusion.

Current operator-facing actions:

- request backup through the normal host-check pipeline
- remove backup-pending files from the queue

Queue behavior follows the backend singleton model:

- one reusable logical row per `FK_HOST + NU_TYPE`
- pending rows are refreshed in place
- terminal rows are reactivated
- running rows are preserved

### Popup Helper

- route: `/popup`
- template: [popup.html](/RFFusion/src/webfusion/templates/popup/popup.html)

This is a small legacy helper used to preview task filter payloads while older
flows still coexist with the newer task builder.

## Main Async APIs

The most relevant lazy endpoints are:

- `/api/map/stations`
- `/api/map/stations/<site_id>`
- `/api/spectrum/localities`
- `/api/spectrum/file/<file_id>/spectra`
- `/api/server/summary-metrics`
- `/api/server/processing-errors`
- `/api/server/backup-errors`
- `/api/server/hosts`
- `/api/host/<host_id>/locations`
- `/api/host/<host_id>/processing-errors`
- `/api/host/<host_id>/backup-errors`

## Caching Philosophy

WebFusion uses only small in-process TTL caches.

These caches are meant to:

- smooth repeated browser navigation
- avoid rebuilding the same response several times in one worker process
- keep frequently opened panels responsive

They are not shared across processes and they are not authoritative.

The current philosophy is:

- use `RFFUSION_SUMMARY` to remove expensive global queries where possible
- keep only short-lived application caches on top of that

## Downloads

Repository-backed downloads follow this preferred flow:

1. Python resolves the repository path
2. the UI points the browser to `/downloads/...` when possible
3. `nginx` serves the file directly from `/mnt/reposfi`

Flask fallback delivery still exists for cases where direct offload is not
available, but it is not the preferred path for large files.

## How To Run

For container deployment, see
[README.MD](/RFFusion/install/webserver/README.MD).

For a local application-level run:

```bash
cd /RFFusion/src/webfusion
/opt/conda/envs/appdata/bin/python app.py
```

Default listener settings:

- host: `127.0.0.1`
- port: `8000`

Health endpoint:

- `/health`

## Current Limitations

- database credentials are still embedded in [db.py](/RFFusion/src/webfusion/db.py)
- the task module still uses bootstrap HTTP Basic Auth credentials in code
- host-to-equipment and site-to-host reconciliation still depends on naming
  heuristics in some views
- logs currently go to process `stdout` and `stderr`; there is no dedicated
  Flask log sink configured inside the app itself

## Related Documentation

- [README.md](/RFFusion/README.md)
- [README.MD](/RFFusion/install/webserver/README.MD)
- [README.md](/RFFusion/src/appCataloga/README.md)
- [DB_INTERCONNECTIONS.md](/RFFusion/src/mariadb/scripts/DB_INTERCONNECTIONS.md)
