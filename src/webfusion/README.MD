# WebFusion

`webfusion` is the RF.Fusion web interface for browsing spectrum data,
inspecting hosts, creating `HOST_TASK` entries, and navigating stations
through a geographic map.

It is intentionally small and pragmatic:

- Flask provides the application and routing layer
- `waitress` serves the WSGI app
- `nginx` fronts the app in the deployment container and serves repository
  downloads directly

## Scope

Today the application covers four practical areas:

1. Home / station map
   - plots station points over Brazil
   - supports filtering by state, station, and online-only hosts
   - exposes quick actions for `Spectrum`, `Host`, and `Task`

2. Spectrum query
   - `Spectrum` mode: one row per spectrum
   - `File` mode: one row per repository file, with expandable linked spectra
   - direct file download through `nginx`

3. Host query
   - lists hosts from `BPDATA`
   - supports online-only filtering
   - shows host statistics

4. Task creation
   - creates `HOST_TASK` entries in `BPDATA`
   - supports individual and collective execution
   - supports file filters for host checks
   - protected by a simple HTTP Basic Auth layer

## Current Home Screen

The current delivery includes a home screen centered on the Brazil station map.

If the screenshot should live inside the repository, the recommended path is:

`/RFFusion/src/webfusion/static/img/docs/webfusion-home-screen.png`

And the README image reference can then be added as:

```md
![WebFusion Home Screen](./static/img/docs/webfusion-home-screen.png)
```

## Internal Architecture

The runtime is split into a small set of layers:

1. Application entrypoint
   - [app.py](/RFFusion/src/webfusion/app.py)
   - registers blueprints
   - exposes `/`, `/health`, and the station-map APIs

2. Database access
   - [db.py](/RFFusion/src/webfusion/db.py)
   - opens direct `pymysql` connections to:
     - `RFDATA`
     - `BPDATA`

3. Feature modules
   - [/RFFusion/src/webfusion/modules/spectrum](/RFFusion/src/webfusion/modules/spectrum)
   - [/RFFusion/src/webfusion/modules/host](/RFFusion/src/webfusion/modules/host)
   - [/RFFusion/src/webfusion/modules/task](/RFFusion/src/webfusion/modules/task)
   - [/RFFusion/src/webfusion/modules/map](/RFFusion/src/webfusion/modules/map)

4. Templates and static assets
   - [/RFFusion/src/webfusion/templates](/RFFusion/src/webfusion/templates)
   - [/RFFusion/src/webfusion/static](/RFFusion/src/webfusion/static)

## Data Sources

`webfusion` reads from two databases:

- `RFDATA`
  - spectrum catalog
  - file metadata
  - equipment and site dimensions

- `BPDATA`
  - host inventory
  - host statistics
  - host task queue

The station map is built primarily from `DIM_SPECTRUM_SITE.GEO_POINT`, with
host resolution enriched afterward through tolerant name matching.

## Main Modules

### Home Map

Map services live in:

- [service.py](/RFFusion/src/webfusion/modules/map/service.py)

Key behaviors:

- fast initial point loading
- lazy or background enrichment of host state
- tolerant equipment-to-host matching
- filtering by state, station, and online status

### Spectrum

Spectrum services and routes live in:

- [service.py](/RFFusion/src/webfusion/modules/spectrum/service.py)
- [routes.py](/RFFusion/src/webfusion/modules/spectrum/routes.py)
- [spectrum.html](/RFFusion/src/webfusion/templates/spectrum/spectrum.html)

Important behaviors:

- optional filters instead of rigid mandatory inputs
- `Spectrum` and `File` query modes
- expandable file detail rows
- short in-memory caching for repeated queries and file-path resolution
- direct repository downloads through `/downloads/` on `nginx`

### Host

Host routes and services live in:

- [routes.py](/RFFusion/src/webfusion/modules/host/routes.py)
- [service.py](/RFFusion/src/webfusion/modules/host/service.py)

The host screen is intentionally simple:

- choose a host
- optionally keep only online hosts
- inspect host-level statistics

### Task

Task routes and helpers live in:

- [routes.py](/RFFusion/src/webfusion/modules/task/routes.py)
- [service.py](/RFFusion/src/webfusion/modules/task/service.py)
- [builder.py](/RFFusion/src/webfusion/modules/task/builder.py)

The task module is the write-oriented part of `webfusion`.

It is currently protected by a minimal HTTP Basic Auth prompt and is used to:

- create host-check tasks
- create statistics-update tasks
- submit them for one host or many hosts

## Downloads

Repository-backed downloads are intentionally not served by Flask end-to-end.

Current strategy:

1. the query layer resolves the repository path
2. the table renders a direct `/downloads/...` URL when possible
3. `nginx` serves the file directly from `/mnt/reposfi`
4. Flask download routes remain as fallback paths

This design was adopted because the repository is mounted from CIFS and the
direct `nginx` path reduces the time-to-first-byte substantially.

## Directory Layout

```text
src/webfusion/
├── app.py
├── db.py
├── modules/
│   ├── host/
│   ├── map/
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
    ├── spectrum/
    └── task/
```

## Deployment Note

The production-oriented container stack for `webfusion` is documented in:

- [README.MD](/RFFusion/install/webserver/README.MD)

That stack uses:

- `nginx`
- `waitress`
- `sshd`

inside a single container.

## Operational Notes

- This module currently keeps database credentials in source code via
  [db.py](/RFFusion/src/webfusion/db.py). That is practical, but not ideal for
  a harder production posture.
- The `Task` module still uses bootstrap credentials in code. It is sufficient
  for a first guardrail, but not a full authentication model.
- Some map host associations are heuristic because the current data model does
  not expose a direct `site -> host` foreign key.
