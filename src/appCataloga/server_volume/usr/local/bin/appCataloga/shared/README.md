# Shared Layer

This folder contains the shared support layer used across appCataloga
entrypoints, workers, adapters, and maintenance scripts.

The goal of `shared/` is to host reusable infrastructure and small cross-cutting
helpers without pulling business rules away from the services that own them.

## Design Intent

The shared layer should contain:

- narrow, reusable helpers
- stable value objects
- lightweight orchestration primitives
- logging and error utilities
- transport and timeout helpers

The shared layer should not become:

- a second service layer
- a place to hide workflow decisions that belong in `appCataloga_xxx.py`
- a dumping ground for unstable production logic

## Main Modules

### `logging_utils.py`

Structured logger used by the whole project.

Responsibilities:

- standardize log-line format
- resolve per-script log files
- expose `event=... key=value` helpers
- keep a lightweight interface suitable for long-running scripts

### `errors.py`

Shared exceptions and error-tracking helpers.

Responsibilities:

- define common domain exceptions
- provide `ErrorHandler` for staged failure capture
- provide timeout control used by service code
- classify transient SSH/SFTP bootstrap failures consistently across workers

### `tools.py`

Small stateless helpers reused by workers and handlers.

Current responsibilities:

- compose standardized `NA_MESSAGE` values
- parse PowerShell timestamps
- check process existence

### `filter.py`

Canonical parser and evaluator for appCataloga file filters.

Responsibilities:

- normalize raw filter payloads
- validate filter modes and fields
- generate metadata-side and database-side filter behavior consistently

### `file_metadata.py`

Immutable shared value object representing file metadata discovered on hosts.

This is the canonical payload exchanged between remote discovery, database
ingestion, and maintenance scripts.

### `ssh_utils.py`

SSH/SFTP transport wrapper built on Paramiko.

Responsibilities:

- create durable SSH/SFTP sessions
- expose file operations with project-specific logging
- collect remote file metadata across Linux and Windows hosts

### `host_context.py`

High-level remote host orchestration helpers.

Responsibilities:

- provide the `hostDaemon` abstraction used by server-side discovery flows
- stream remote filesystem metadata through SSH/SFTP
- keep discovery traversal decoupled from database callbacks

### `constants.py`

Small stable constants and sentinel objects shared across layers.

### `timeout_utils.py`

Minimal timeout helper for call sites that only need execution time limits.

## Supporting Documentation

- `LOGGING.md`: logging conventions for the project
- `SCRIPT_STYLE.md`: style conventions for `appCataloga_xxx.py` entrypoints

## Legacy Files

Some files may remain in this folder for compatibility during the ongoing
cleanup effort. When a module looks legacy, the expectation is:

- document it honestly
- avoid expanding its scope
- migrate callers gradually instead of rewriting blindly

`shared old.py` is intentionally excluded from the active shared-layer
documentation because it is considered fully legacy. Host/runtime helpers that
used to live here now belong in `host_handler/` and `server_handler/`.

## Usage Guidance

Prefer adding code to `shared/` when all of the following are true:

- the logic is reused or clearly reusable
- the behavior is stable enough to deserve a single canonical implementation
- moving it out of a service makes that service easier to read

Keep logic out of `shared/` when it still expresses service-specific workflow,
retry policy, or business ownership.
