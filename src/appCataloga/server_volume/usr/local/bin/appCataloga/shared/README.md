# Shared Layer

This folder contains the active shared support layer used across
`appCataloga` entrypoints, workers, adapters, and maintenance scripts.

The goal of `shared/` is to centralize small, stable cross-cutting contracts
without turning the folder into a second service layer.

## Design Intent

The shared layer should contain:

- narrow reusable helpers
- stable value objects
- common logging and error contracts
- filter normalization shared by runtime and UI
- small utilities whose meaning should not drift between workers

The shared layer should not become:

- a place to hide queue ownership or retry policy
- a replacement for `host_handler/`, `server_handler/`, or `appAnalise/`
- a dumping ground for unstable production logic

## What Actually Lives Here Today

### `logging_utils.py`

Structured logger used by the runtime.

Responsibilities:

- standardize the log-line format
- resolve one log file per entrypoint
- rotate oversized files safely
- expose a small structured event API such as `event(...)` and
  `service_start(...)`

See also:

- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/LOGGING.md](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/LOGGING.md)

### `errors.py`

Shared error taxonomy and error-capture helpers.

Responsibilities:

- define reusable domain exceptions such as `BinValidationError`
- classify transient external-service and SSH/SFTP bootstrap failures
- distinguish appAnalise transport failures from structured
  `AppAnaliseReadTimeoutError`
- provide `ErrorHandler` for staged failure capture and formatting
- provide timeout helpers used by long-running operations

### `filter.py`

Canonical parser and evaluator for appCataloga file filters.

Responsibilities:

- normalize raw filter payloads from socket requests and `webfusion`
- validate filter modes and fields
- keep discovery-side metadata filtering and DB-side backlog filtering aligned
- support budget semantics such as `max_total_gb` and `sort_order`

### `file_metadata.py`

Shared value object representing discovered remote file metadata.

This is the canonical payload exchanged between remote discovery, database
ingestion, and maintenance scripts.

### `tools.py`

Small stateless helpers reused by multiple runtime layers.

Current responsibilities:

- compose standardized `NA_MESSAGE` values
- expose canonical labels for non-terminal statuses such as `TASK_FROZEN`
- parse PowerShell timestamps
- check whether a PID still exists

### `geolocation_utils.py`

Lightweight helpers around reverse geocoding and site-data mapping.

Responsibilities:

- retry reverse geocoding calls conservatively
- map geocoder output into the site fields expected by RF.Fusion

### `constants.py`

Small stable constants and sentinel objects shared across layers.

### `__init__.py`

Compatibility package boundary for shared imports.

It still re-exports a few runtime helpers while the codebase finishes the move
toward context-specific packages such as:

- `host_handler`
- `server_handler`
- `appAnalise`

## Modules That No Longer Belong Here

Some older documentation still referenced helpers such as `ssh_utils.py`,
`host_context.py`, and `timeout_utils.py` as if they were active files inside
`shared/`.

That is no longer the current structure:

- host/SSH concerns now belong under `host_handler/`
- runtime/process helpers now belong under `server_handler/`
- appAnalise-specific orchestration belongs under `appAnalise/`

## Supporting Documentation

- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/LOGGING.md](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/LOGGING.md)
- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/SCRIPT_STYLE.md](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/SCRIPT_STYLE.md)

## Usage Guidance

Prefer adding code to `shared/` when all of the following are true:

- the logic is reused or clearly reusable
- the behavior is stable enough to deserve one canonical implementation
- moving it out of a worker or entrypoint makes that owner easier to read

Keep logic out of `shared/` when it still expresses service-specific workflow,
queue policy, retry ownership, or host-family behavior.
