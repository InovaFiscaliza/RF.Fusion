# appCataloga

`appCataloga` is the operational runtime of RF.Fusion.

This part of the project is responsible for:

- registering and monitoring remote hosts
- discovering candidate files on those hosts
- backing files up into the shared repository
- processing selected files locally or through `appAnalise`
- publishing metadata
- cleaning quarantined artifacts after retention expires

If the root [README.md](/RFFusion/README.md) explains the platform, this file
explains the `appCataloga` runtime that actually moves work forward.

## What Lives Here

The active runtime code lives under:

- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga)

That directory contains the scripts and modules that run inside the
`appCataloga` container or runtime environment.

At a high level:

- `appCataloga.py` receives host registration / trigger requests
- background workers consume `HOST_TASK` and `FILE_TASK`
- shared helpers and DB handlers enforce the workflow contracts

## Runtime Model

The runtime is state-driven.

In broad terms:

1. a host is known in `BPDATA.HOST`
2. a durable `HOST_TASK` represents the current host-level work request
3. discovery creates `FILE_TASK` rows for candidate files
4. backlog management promotes or rolls back queued file work
5. backup copies the file into `/mnt/reposfi`
6. processing validates and catalogs the file
7. `FILE_TASK_HISTORY` becomes the authoritative lifecycle record
8. metadata publication and garbage collection operate on top of that record

The important architectural split is:

- `BPDATA` stores operational state
- `RFDATA` stores analytical state

`appCataloga` touches both, but for different reasons.

## Main Entry Points

These are the scripts that matter most in day-to-day runtime.

### `appCataloga.py`

Main entrypoint for host-facing requests.

It is responsible for:

- registering or refreshing host context
- enqueueing the first `HOST_TASK` for the requested action
- serving as the operational front door of the runtime

Current public command contract:

- `backup`
  - registers or refreshes the host
  - queues `HOST_TASK_CHECK_TYPE`
  - follows the normal connectivity -> discovery -> backlog -> backup flow

- `stop`
  - registers or refreshes the host
  - queues `HOST_TASK_BACKLOG_ROLLBACK_TYPE`
  - skips host connectivity checks because the action is DB-only

### `appCataloga_discovery.py`

Discovers candidate files on remote hosts and creates `FILE_TASK` rows based on
host filter configuration.

Discovery no longer promotes directly into backup. Its responsibility now is:

- discover remote candidates
- persist `FILE_TASK` / `FILE_TASK_HISTORY`
- hand off promotion to the backlog-management worker

### `appCataloga_backlog_management.py`

Applies DB-only backlog transitions after discovery or explicit operator action.

This worker is responsible for:

- promoting `DISCOVERY / DONE` into `BACKUP / PENDING`
- rolling `BACKUP / PENDING` back into `DISCOVERY / DONE`
- honoring explicit STOP-like requests without touching the remote host

### `appCataloga_file_bkp.py`

Moves files from remote hosts into the shared repository.

This is the worker that turns a discovered remote file into a repository
artifact that can later be processed locally.

### `appCataloga_file_bin_proces_appAnalise.py`

Processes files through `appAnalise`.

This worker is responsible for:

- calling `appAnalise`
- validating the returned payload semantically
- cataloging sites, files, equipment and spectra
- resolving the final artifact contract in `FILE_TASK_HISTORY`
- distinguishing transport outages from structured `ReadTimeout` replies

### `appCataloga_host_check.py`

Runs host connectivity checks and updates operational host state.

### `appCataloga_host_maintenance.py`

Performs background maintenance over host-level state.

This reduced the need for the UI to manually create internal host tasks such as
connection checks and statistics refresh.

### `appCataloga_pub_metadata.py`

Publishes derived metadata for downstream use.

### `appCataloga_garbage_collector.py`

Applies retention rules to quarantined repository artifacts.

The collector treats:

- `trash/`
- `trash/resolved_files/`

as different channels with different semantics and retention windows.

## Current Workflow

The active workflow is roughly:

```text
HOST
  -> HOST_TASK
  -> discovery
  -> backlog management
  -> FILE_TASK
  -> backup into /mnt/reposfi
  -> processing
  -> FILE_TASK_HISTORY
  -> metadata publication / garbage collection
```

Operationally, there are now two important queue paths:

- normal backup path
  - `appCataloga.py`
  - `appCataloga_host_check.py`
  - `appCataloga_discovery.py`
  - `appCataloga_backlog_management.py`
  - `appCataloga_file_bkp.py`

- rollback / stop path
  - `appCataloga.py`
  - `appCataloga_backlog_management.py`

For `appAnalise`-based processing, the important nuance is:

- processing may generate an exported artifact such as `.mat`
- RF.Fusion can still reject that artifact semantically afterwards
- in that case the exported artifact becomes the canonical error artifact
- the original source artifact becomes a resolved input and moves to `resolved_files`

There is also a second nuance around long-running source files:

- RF.Fusion now requests a remote `timeoutSeconds` in the appAnalise `FileRead`
  request
- `APP_ANALISE_REQUEST_TIMEOUT_SECONDS` should stay lower than the local
  RF.Fusion socket timeout `APP_ANALISE_PROCESS_TIMEOUT`
- that ordering lets appAnalise return a structured
  `handlers:FileReadHandler:ReadTimeout` reply before the local socket layer
  gives up
- structured `ReadTimeout` is not treated as a transport outage and not treated
  as a definitive payload error
- instead, the live `FILE_TASK` and `FILE_TASK_HISTORY.NU_STATUS_PROCESSING`
  move to `TASK_FROZEN = -3` for manual review

## File Filter Modes

The operational filter contract is centralized in:

- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/filter.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/filter.py)

This same contract is reused by:

- host discovery
- backlog promotion into backup
- operator-triggered task creation from `webfusion`

Canonical filter fields today are:

- `mode`
- `file_path`
- `extension`
- `start_date`
- `end_date`
- `last_n_files`
- `file_name`
- `max_total_gb`
- `sort_order`

Fields are normalized before evaluation. Irrelevant fields are explicitly
nulled per mode so downstream workers can treat the filter as canonical state
instead of guessing which keys still matter.

### Field-by-Field Semantics

- `mode`
  Selects the semantic rule. Unknown values fall back to `NONE`. Legacy
  aliases `LAST_N` and `LAST_N_FILES` are normalized to `LAST`.

- `file_path`
  Scopes remote enumeration during discovery and rediscovery. It is not the
  main selector for DB-side backlog promotion once rows already exist in
  `FILE_TASK`.

- `extension`
  Optional orthogonal filter. It is normalized to lowercase and gains a
  leading dot when missing, so `bin` and `.BIN` both become `.bin`.

- `start_date`
  Lower bound used only by `RANGE`. On the DB side it filters
  `DT_FILE_CREATED`.

- `end_date`
  Upper bound used only by `RANGE`. If `start_date > end_date`, the two bounds
  are swapped during normalization.

- `last_n_files`
  Used only by `LAST`. It is normalized to a positive integer and represents
  the latest N discovered files by `DT_FILE_CREATED`.

- `file_name`
  Used only by `FILE`. Supports wildcard-style matching. If the provided name
  already contains an extension, the separate `extension` field is discarded to
  avoid malformed patterns such as `*.bin.bin`.

- `max_total_gb`
  Backlog budget for promotion into backup. It is converted to KB internally.
  `null`, invalid values, or values `<= 0` mean "no budget cap". In practice
  that means "promote every eligible row that matches the filter".

- `sort_order`
  Accepted values are `newest_first` and `oldest_first`. It matters only when
  the worker must choose an ordered slice of already discovered `FILE_TASK`
  rows, especially for budgeted promotion into backup.

Important shared semantics:

- minimum file size and minimum file age protections run before the semantic
  mode-specific metadata filter
- `NONE` and `REDISCOVERY` are discovery-side modes; they do not define a
  backlog-selection rule on the DB side
- rollback / "Retirar da Fila de Backup" does not use `max_total_gb`
- for `ALL`, `RANGE`, `FILE`, and `LAST`, leaving `max_total_gb = null` means
  "do not stop by volume"
- in `ALL`, leaving `max_total_gb = null` and using `extension = ".bin"` means
  "promote all eligible discovered `.bin` rows for that host"

The principal modes are:

### `NONE`

Default incremental discovery mode.

Behavior:

- scopes discovery by `file_path`
- optionally narrows by `extension`
- uses the last known DB timestamp as the incremental discovery cutoff
- does not define a DB-side backlog selection rule on its own

Use it when the operator wants the normal "keep discovering forward from the
current point" behavior.

### `ALL`

Broad backlog-selection mode.

Behavior:

- keeps `file_path` and optional `extension`
- selects all eligible discovered files for the current queue operation
- if `max_total_gb` is set, it promotes only the ordered slice that fits the
  budget
- if `max_total_gb` is `null`, it promotes every eligible row
- `sort_order` only changes which rows are preferred when a budgeted slice is
  needed

Use it when the operator wants to promote the whole eligible backlog, possibly
with a volume ceiling.

### `RANGE`

Date-window mode.

Behavior:

- accepts `start_date`, `end_date`, or both
- swaps the two bounds if the payload arrives inverted
- keeps optional `extension`
- can still honor `max_total_gb`
- can still honor `sort_order` when a budgeted slice must be chosen

Use it when the operator wants a created-date window instead of "all" or
"latest N".

### `LAST`

Latest-N mode.

Behavior:

- uses `last_n_files`
- orders by `DT_FILE_CREATED DESC, ID_FILE_TASK DESC`
- keeps optional `extension`
- can still honor `max_total_gb` after defining the latest-N slice

Legacy aliases `LAST_N` and `LAST_N_FILES` are normalized to `LAST`.

Use it when the operator wants only the most recent slice of the backlog.

### `FILE`

Explicit file / pattern mode.

Behavior:

- uses `file_name`
- supports wildcard-style matching
- if `file_name` has no extension and `extension` is provided, the extension is
  appended automatically
- discovery intentionally skips the normal deduplication shortcut for this mode
  so a specifically targeted artifact can be revisited
- can still honor `max_total_gb` and `sort_order` on the DB side when multiple
  discovered rows match the pattern

Use it when the operator needs one explicit artifact or a narrow filename
pattern instead of a time-based rule.

### `REDISCOVERY`

Special-purpose rescan mode.

Behavior:

- keeps `file_path` and optional `extension`
- disables the incremental `newer_than` cutoff during remote discovery
- intentionally does not define a DB-side backlog-selection rule

Use it when the operator needs a fresh rescan of a remote path instead of the
normal incremental walk.

### Practical Examples

`NONE` for normal incremental discovery:

```json
{
  "mode": "NONE",
  "extension": ".bin",
  "file_path": "/mnt/internal/data"
}
```

Effect:

- discovers forward from the current incremental cutoff
- only for `.bin`
- does not by itself define a backlog-promotion slice

`ALL` without budget:

```json
{
  "mode": "ALL",
  "extension": ".bin",
  "file_path": "/mnt/internal/data",
  "max_total_gb": null,
  "sort_order": "newest_first"
}
```

Effect:

- on discovery: scopes remote enumeration by path and extension
- on backlog promotion: promotes all eligible discovered `.bin` rows for the
  host
- `sort_order` becomes operationally irrelevant because no budgeted choice is
  needed

`ALL` with backlog budget, oldest first:

```json
{
  "mode": "ALL",
  "extension": ".zip",
  "file_path": "C:/CelPlan/CellWireless RU/Spectrum/Completed",
  "max_total_gb": 50,
  "sort_order": "oldest_first"
}
```

Effect:

- selects all eligible discovered `.zip` rows for the host
- promotes only the oldest slice whose cumulative size still fits inside `50 GB`

`RANGE` with backlog budget:

```json
{
  "mode": "RANGE",
  "start_date": "2026-04-01T00:00:00",
  "end_date": "2026-04-07T23:59:59",
  "extension": ".zip",
  "file_path": "C:/CelPlan/CellWireless RU/Spectrum/Completed",
  "max_total_gb": 50,
  "sort_order": "newest_first"
}
```

Effect:

- restricts the selection to files created inside the given window
- among that window, promotes the newest slice that still fits inside `50 GB`

`LAST` for the newest 100 files:

```json
{
  "mode": "LAST",
  "last_n_files": 100,
  "extension": ".zip",
  "file_path": "C:/CelPlan/CellWireless RU/Spectrum/Completed",
  "max_total_gb": null
}
```

Effect:

- defines the slice as the latest 100 discovered `.zip` rows
- with no budget, promotes that whole slice
- with a budget, promotes only the prefix of that slice that still fits

`FILE` targeting one explicit artifact family:

```json
{
  "mode": "FILE",
  "file_name": "*rfeye002211*",
  "extension": ".bin",
  "file_path": "/mnt/internal/data"
}
```

Effect:

- discovery walks the configured path but only keeps filenames matching the
  wildcard
- DB-side backlog promotion can target the same filename family among already
  discovered rows

`REDISCOVERY` for a full rescan of one path:

```json
{
  "mode": "REDISCOVERY",
  "extension": ".bin",
  "file_path": "/mnt/internal/data"
}
```

Effect:

- disables the normal incremental "newer than last seen" discovery cutoff
- rescans the remote path from scratch
- still does not define a DB-side backlog-promotion slice

## Directory Guide

Inside the runtime directory, the most important areas are:

### `appAnalise/`

Helpers for talking to `appAnalise`, normalizing its payload and resolving the
processing flow.

Important files:

- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appAnalise/appAnalise_connection.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appAnalise/appAnalise_connection.py)
- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appAnalise/payload_parser.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appAnalise/payload_parser.py)
- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appAnalise/task_flow.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/appAnalise/task_flow.py)

### `db/`

Database handlers for the two project databases.

Important files:

- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/db/dbHandlerBKP.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/db/dbHandlerBKP.py)
- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/db/dbHandlerRFM.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/db/dbHandlerRFM.py)

### `host_handler/`

Host-level orchestration helpers such as connectivity, runtime state and
maintenance.

### `server_handler/`

Process control, signal handling, socket logic and worker-pool support.

### `shared/`

Shared infrastructure such as constants, error formatting, logging and general
tools.

### `utils/`

Small auxiliary scripts. Useful, but not part of the main workflow contract.

### `_oldCode/`

Legacy code preserved for historical reference. It should not be treated as the
source of truth for the active runtime.

## Core Contracts

These are the contracts that are easiest to break accidentally.

### `HOST_TASK` Is Durable State

`HOST_TASK` is not an append-only history table.

Current behavior is:

- one logical row per `FK_HOST + NU_TYPE`
- status, timestamps and filter are refreshed in place
- running rows are preserved instead of silently overwritten

This matters because both the backend and `webfusion` are expected to follow
the same queue contract.

It also matters because backlog control now has its own durable task types:

- `HOST_TASK_BACKLOG_CONTROL_TYPE`
- `HOST_TASK_BACKLOG_ROLLBACK_TYPE`

### `FILE_TASK` Is Transient, `FILE_TASK_HISTORY` Is Authoritative

`FILE_TASK` is workflow state.

`FILE_TASK_HISTORY` is the long-lived lifecycle record used by operators and by
garbage collection.

That means:

- `FILE_TASK` can be retried, suspended, resumed or removed
- the final artifact recorded in `FILE_TASK_HISTORY` is what the system treats
  as canonical for history purposes
- `TASK_FROZEN = -3` is the explicit exception to the normal retry/finalize
  split for processing: the task remains live and the processing phase is put
  on hold for manual review
- frozen processing rows are intentionally not reactivated by
  `file_task_resume_by_host()`

### Repository Paths Are Semantic

The shared repository is not just storage.

Current semantics:

- `trash/` holds canonical errored artifacts
- `trash/resolved_files/` holds superseded source artifacts
- successful final artifacts stay in their final repository path

This is especially important for `appAnalise`, where source and exported
artifacts can diverge after semantic validation.

### `appAnalise` Resolves Site Per Spectrum

The active `appAnalise` contract is no longer "one site per file".

Current behavior:

- site resolution is per spectrum
- equipment comes from the payload receiver per spectrum
- bad spectra are discarded selectively when possible
- the whole file only fails when no valid spectra remain
- structured appAnalise `ReadTimeout` replies freeze the processing task
  instead of trashing the artifact or retrying automatically forever

## Dependencies

The runtime depends on:

- MariaDB with both `BPDATA` and `RFDATA`
- shared repository mounted at `/mnt/reposfi`
- SSH/SFTP access to remote hosts
- Zabbix host context
- optional `appAnalise` service for selected processing flows

It does not replace:

- long-term archival policy
- browser UI concerns
- analytical reporting dashboards

## How To Run

Inside the runtime directory, the practical operational scripts are:

- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_start_all.sh](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_start_all.sh)
- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_status_all.sh](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_status_all.sh)
- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_stop_all.sh](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/tool_stop_all.sh)
- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/safe_stop.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/safe_stop.py)

Those scripts now include the backlog-management daemon alongside the
traditional discovery, backup and processing workers.

Typical flow:

```bash
cd /RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga
./tool_start_all.sh
./tool_status_all.sh
./tool_stop_all.sh
```

Note:

- the container runtime itself is documented separately
- the container entrypoint does not automatically orchestrate every worker

For container deployment details, see:

- [/RFFusion/install/appCataloga/README.md](/RFFusion/install/appCataloga/README.md)

## Related Documentation

- [/RFFusion/README.md](/RFFusion/README.md)
- [/RFFusion/install/appCataloga/README.md](/RFFusion/install/appCataloga/README.md)
- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/db/README.md](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/db/README.md)
- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/README.md](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/README.md)
