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
