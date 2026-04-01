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
