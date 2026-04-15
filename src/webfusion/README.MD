# webfusion

`webfusion` is the operator-facing web interface of RF.Fusion.

It exists to let people inspect the system without opening SQL clients or
touching the `appCataloga` runtime directly.

In practical terms, `webfusion` is used for four recurring jobs:

- inspect monitored localities on the Brazil-centered map
- query spectra and repository files
- inspect host and server operational state
- request operator actions that feed `appCataloga`

If the root [README.md](/RFFusion/README.md) explains the full platform, this
file explains the `webfusion` application itself.

## Runtime Model

The runtime is intentionally small:

- Flask provides routes and template rendering
- `waitress` serves the WSGI app
- `nginx` fronts the application in the container
- `nginx` also serves repository-backed downloads directly from `/mnt/reposfi`

This split matters because the repository is usually mounted from CIFS. Direct
delivery through `nginx` is much more efficient than pushing large files
through Flask.

## How Data Reaches The Browser

In `webfusion`, there are two complementary paths from Python to the screen.

### 1. Initial page render

This is the classic Flask + Jinja flow:

1. the browser requests a route such as `/spectrum`
2. the Flask route reads query-string parameters from `request.args`
3. the route calls one or more service functions to query MariaDB
4. the route calls `render_template(...)`
5. Jinja renders the template with the variables passed by Python
6. the browser receives final HTML and paints the page

In this path, the browser never sees Python objects directly. It only receives
the HTML generated from them.

### 2. Dynamic browser updates after the page is open

Once the page already exists in the browser, JavaScript can ask Python for
more data without reloading the whole screen.

That flow is:

1. the template exposes DOM elements and `data-*` attributes
2. the page JS reads those values
3. the JS calls a narrow Flask JSON endpoint with `fetch(...)`
4. the route calls a service function
5. Flask returns JSON
6. the JS updates only the relevant part of the DOM

This is how dependent filters, expandable details and other lazy UI behaviors
work in `webfusion`.

## HTML, CSS, JS And Python Responsibilities

The codebase is easier to reason about when each layer is seen as having a
different job:

- Python
  - reads request parameters
  - queries or aggregates data
  - decides what the template receives
  - exposes JSON/download endpoints for later browser interactions

- HTML templates
  - define page structure
  - define form fields and containers
  - expose `id` and `data-*` hooks that the JS expects

- CSS
  - defines the visual system and page-specific presentation

- JavaScript
  - listens to user actions
  - calls narrow endpoints when needed
  - updates the already rendered page without a full reload

In practical terms:

- Python decides what exists
- Jinja turns that into HTML
- JS decides what can change later without re-rendering the full page

## Generic Request Flow

Most pages in `webfusion` follow the same high-level shape:

1. a route receives the request
2. the route reads request parameters
3. one or more service functions query or aggregate data
4. the route either:
   - renders a template for the first page load
   - returns JSON/download content for a later browser action

This means the application is easier to reason about if each file is viewed as
having a stable role:

- `routes.py`
  - HTTP entrypoints
  - parameter normalization
  - template or JSON responses

- `service.py`
  - SQL and data shaping
  - lightweight caches when appropriate
  - feature-specific query semantics

- `templates/*.html`
  - page structure
  - Jinja placeholders
  - `id` / `data-*` hooks for later browser behavior

- `static/js/*.js`
  - event handling
  - `fetch(...)` calls to smaller endpoints
  - partial DOM updates after the page is already open

## Example: Server Data Becoming Visible HTML

One point that often causes confusion is this: not every visible value comes
from a URL parameter.

Some values are pushed from Python into the first HTML response.

For example, a route can:

1. call a service function such as `get_equipments()`
2. store the result in a Python variable such as `equipments`
3. pass `equipments=equipments` into `render_template(...)`
4. let the template turn that list into `<option>`, table rows or cards

So there are two different data directions in the system:

- browser -> Python
  - query-string parameters
  - form fields
  - `fetch(...)` requests

- Python -> browser
  - template context rendered into HTML
  - JSON returned to page JavaScript

That is why a populated selector or table does not necessarily imply that its
content came from the URL. It may have been loaded by Python from the database
and injected into the initial template render.

## What `render_template` Is

`render_template` is not implemented inside this repository. It is imported
from Flask.

Its job is:

1. load the Jinja template file
2. inject the variables passed by the route
3. return the final HTML string as the HTTP response

That is the key server-side step that turns Python query results into browser
markup.

In practice, every page that returns HTML depends on this same pattern, even if
the details of the service queries differ by module.

## What webfusion Reads

`webfusion` is read-heavy.

It reads from:

- `BPDATA`
  - host inventory
  - host state
  - host task queue
  - file-task history

- `RFDATA`
  - sites
  - equipment
  - spectra
  - repository file metadata

The UI is where these two worlds meet.

Example:

- the map gets geographic points from `RFDATA`
- host actions and operational context come from `BPDATA`
- the association is reconciled in Python, not by a direct cross-database FK

## Main Pages

### Home

- route: `/`
- template: [index.html](/RFFusion/src/webfusion/templates/index.html)

Purpose:

- render the station map shell
- load station points asynchronously
- provide navigation to host, server, spectrum and task views

Important behavior:

- the map plots processed-spectrum localities, not the full Zabbix inventory
- popup details are loaded on demand
- the page is intentionally rendered before the full map dataset is ready

### Spectrum

- routes: [routes.py](/RFFusion/src/webfusion/modules/spectrum/routes.py)
- service: [service.py](/RFFusion/src/webfusion/modules/spectrum/service.py)
- template: [spectrum.html](/RFFusion/src/webfusion/templates/spectrum/spectrum.html)

This module supports two complementary views:

- `Spectrum` mode
  - one row per spectrum

- `File` mode
  - one row per repository file
  - expandable linked spectra

Important behavior:

- `File` mode is repository-oriented, not `host_file`-oriented
- downloads prefer direct `nginx` URLs
- locality filters are loaded asynchronously

### Server

- route: `/server`
- route code: [routes.py](/RFFusion/src/webfusion/modules/server/routes.py)
- aggregation support: [service.py](/RFFusion/src/webfusion/modules/host/service.py)
- template: [server.html](/RFFusion/src/webfusion/templates/server/server.html)

This is the global operational dashboard.

It answers questions such as:

- how many hosts are online, offline or busy
- how much backup volume was consumed
- how many files failed processing
- which grouped processing errors are most common
- whether shared services such as `appAnalise` and `/mnt/reposfi` look healthy

### Host

- route: `/host`
- route code: [routes.py](/RFFusion/src/webfusion/modules/host/routes.py)
- service: [service.py](/RFFusion/src/webfusion/modules/host/service.py)
- template: [host.html](/RFFusion/src/webfusion/templates/host/host.html)

This is the station-focused operational page.

It is meant to answer:

- is this host online
- when was the last discovery, backup or processing event
- what is pending or failing
- which processing errors are common for this host
- where this station has been observed over time

### Task

- route prefix: `/task`
- route code: [routes.py](/RFFusion/src/webfusion/modules/task/routes.py)
- service: [service.py](/RFFusion/src/webfusion/modules/task/service.py)
- builder support: [builder.py](/RFFusion/src/webfusion/modules/task/builder.py)
- templates:
  - [task_builder.html](/RFFusion/src/webfusion/templates/task/task_builder.html)
  - [task_list.html](/RFFusion/src/webfusion/templates/task/task_list.html)

This is the write-oriented module of `webfusion`.

Current contract:

- the UI exposes only two operator-facing actions:
  - request backup
  - remove backup-pending files from the queue
- internal task types such as connection checks or statistics refresh still
  exist in the backend, but are not offered manually in the UI
- the module reuses the backend queue philosophy instead of creating endless
  new `HOST_TASK` rows

That means:

- one logical reusable row per `FK_HOST + NU_TYPE`
- pending or terminal rows are refreshed in place
- running rows are preserved

The important practical mapping is:

- `Solicitar Backup`
  - queues the conventional `HOST_TASK_CHECK_TYPE`
  - follows the normal host-check -> discovery -> backlog-management flow

- `Retirar da Fila de Backup`
  - queues `HOST_TASK_BACKLOG_ROLLBACK_TYPE`
  - acts only on queued `BACKUP / PENDING` work inside the selected filter
  - does not need a host connectivity round-trip

### Popup Helper

- route: `/popup`
- template: [popup.html](/RFFusion/src/webfusion/templates/popup/popup.html)

This is a small legacy helper still used to preview filter payloads while older
operational flows coexist with the newer task builder.

## Async APIs

Several pages rely on small JSON endpoints so the first render stays fast.

The main ones are:

- `/api/map/stations`
- `/api/map/stations/<site_id>`
- `/api/spectrum/localities`
- `/api/spectrum/file/<file_id>/spectra`
- `/api/server/summary-metrics`
- `/api/server/processing-errors`
- `/api/host/<host_id>/locations`
- `/api/host/<host_id>/processing-errors`

## Application Structure

The most important files are:

### [app.py](/RFFusion/src/webfusion/app.py)

This is the application entrypoint.

It:

- creates the Flask app
- registers the feature blueprints
- exposes `/health`
- keeps the landing page and small cross-module routes

### [db.py](/RFFusion/src/webfusion/db.py)

Centralizes database access helpers.

### `modules/`

This directory contains the application feature modules.

Active modules:

- `host/`
- `map/`
- `server/`
- `spectrum/`
- `task/`

Practical note:

- `map/` is currently service-oriented and is consumed from `app.py`
- `dashboard/` and `popup/` directories exist in the tree, but they are not
  active feature modules in the same way as the directories above

### `templates/`

Main templates:

- [base.html](/RFFusion/src/webfusion/templates/base.html)
- [index.html](/RFFusion/src/webfusion/templates/index.html)
- [host/host.html](/RFFusion/src/webfusion/templates/host/host.html)
- [server/server.html](/RFFusion/src/webfusion/templates/server/server.html)
- [spectrum/spectrum.html](/RFFusion/src/webfusion/templates/spectrum/spectrum.html)
- [task/task_builder.html](/RFFusion/src/webfusion/templates/task/task_builder.html)
- [task/task_list.html](/RFFusion/src/webfusion/templates/task/task_list.html)
- [popup/popup.html](/RFFusion/src/webfusion/templates/popup/popup.html)

### `static/`

Static assets such as CSS, fonts, images and JavaScript.

## Downloads

Repository-backed downloads follow this preferred path:

1. the query resolves the repository file path
2. the page renders a direct `/downloads/...` URL when possible
3. `nginx` serves the file from `/mnt/reposfi`

Fallback Flask delivery still exists for some situations, but it is not the
preferred path for large files.

## How To Run

For container deployment, see:

- [/RFFusion/install/webserver/README.MD](/RFFusion/install/webserver/README.MD)

For a local application-level run, the current entrypoint is:

```bash
cd /RFFusion/src/webfusion
/opt/conda/envs/appdata/bin/python app.py
```

By default, `waitress` listens on:

- host: `127.0.0.1`
- port: `8000`

The health endpoint is:

- `/health`

## Current Limitations

These are the main realities of the current codebase.

- database credentials are still embedded in [db.py](/RFFusion/src/webfusion/db.py)
- the task module still uses bootstrap HTTP Basic Auth credentials in code
- some `site -> host` associations remain heuristic because the schema does not
  expose a direct FK for that relationship
- application logs go to process `stdout` and `stderr`; there is no dedicated
  application file log configured in Flask itself

## Related Documentation

- [/RFFusion/README.md](/RFFusion/README.md)
- [/RFFusion/install/webserver/README.MD](/RFFusion/install/webserver/README.MD)
- [/RFFusion/src/appCataloga/README.md](/RFFusion/src/appCataloga/README.md)
- [/RFFusion/src/mariadb/scripts/DB_INTERCONNECTIONS.md](/RFFusion/src/mariadb/scripts/DB_INTERCONNECTIONS.md)
