# Database Interface

This folder contains the database access layer for appCataloga.

The design is split into one shared base handler plus two domain handlers:

- `dbHandlerBase.py`: reusable MySQL connection and SQL helper layer
- `dbHandlerBKP.py`: operational domain for hosts, host tasks, file tasks, and file history
- `dbHandlerRFM.py`: analytical domain for sites, files, procedures, spectrum entities, and publication support

## Architecture

The intended layering is:

1. Services and workers call `dbHandlerBKP` or `dbHandlerRFM`
2. Domain handlers encode table semantics and workflow rules
3. `DBHandlerBase` provides generic SQL builders and connection management

`DBHandlerBase` should not know appCataloga business rules.
Business meaning belongs in the domain handlers.

## DBHandlerBase

`DBHandlerBase` is the reusable execution layer.

Core responsibilities:

- open, reuse, reconnect, and close MySQL connections
- preserve safe parameter binding
- expose generic insert, update, delete, and select helpers
- support dynamic joined selects through `VALID_FIELDS_*`
- provide raw-query escape hatches for aggregates and special SQL

Main helper methods:

- `_connect()`: create or reuse a live connection
- `_disconnect()`: close the connection when required
- `_insert_row()`: parameterized insert
- `_update_row()`: dictionary-driven update with supported operators
- `_upsert_row()`: single-row UPSERT
- `_delete_row()`: guarded delete
- `_select_rows()`: simple table select
- `_select_custom()`: joined select with aliased result columns
- `_select_raw()`: raw parameterized SELECT
- `_execute_custom()`: custom non-select execution
- `_execute_many_custom()`: batched custom execution
- `_upsert_batch()`: batched UPSERT for large ingestion flows

Important conventions:

- joined selects return normalized keys like `HOST__ID_HOST`
- `#CUSTOM#...` keys are reserved for trusted SQL fragments
- the base layer defaults to autocommit unless a subclass manages an explicit transaction

## dbHandlerBKP

`dbHandlerBKP` owns the orchestration side of the system.

Main entities:

- `HOST`
- `HOST_TASK`
- `FILE_TASK`
- `FILE_TASK_HISTORY`

Main responsibilities:

- register and update hosts
- expose host access and status snapshots
- manage BUSY lock lifecycle
- schedule and update host tasks
- create, update, suspend, resume, and delete file tasks
- maintain immutable file history
- promote discovery backlog into backup backlog
- compute host statistics from file history
- expose garbage-collection candidates

Main interface groups:

Host operations:

- `host_upsert()`
- `host_read_access()`
- `host_read_status()`
- `get_last_discovery()`
- `host_release_by_pid()`
- `host_update()`
- `host_update_statistics()`
- `host_release_safe()`
- `host_start_transient_busy_cooldown()`
- `host_cleanup_stale_locks()`

Host task operations:

- `check_host_task()`
- `queue_host_task()`
- `host_task_create()`
- `host_task_statistics_create()`
- `host_task_read()`
- `host_task_update()`
- `host_task_delete()`
- `host_task_suspend_by_host()`
- `host_task_resume_by_host()`

File task operations:

- `read_file_task()`
- `file_task_create()`
- `file_task_update()`
- `file_task_delete()`
- `file_task_suspend_by_host()`
- `file_task_resume_by_host()`
- `check_file_task()`
- `update_backlog_by_filter()`

File history operations:

- `file_history_create()`
- `file_history_update()`
- `file_history_resume_by_host()`
- `filter_existing_file_batch()`
- `file_history_get_gc_candidates()`

Operational notes:

- `FILE_TASK` is treated as transient workflow state
- `FILE_TASK_HISTORY` is the authoritative lifecycle record
- `DT_FILE_TASK`, `DT_BACKUP`, and `DT_PROCESSED` are caller-owned timestamps; handlers do not inject them implicitly
- transient SFTP contention is represented on `HOST` itself via `IS_BUSY` + `DT_BUSY`, with a short cooldown window instead of auxiliary state files
- `HOST_TASK_CHECK_*` and statistics tasks do not own the host BUSY lock; host exclusivity is reserved for discovery and backup
- host statistics are recomputed from history rather than inferred from transient task tables
- stale host locks may trigger a re-queued connection-check task

## dbHandlerRFM

`dbHandlerRFM` owns the measurement and metadata side of the system.

Main entities:

- `DIM_SPECTRUM_SITE`
- `DIM_SITE_STATE`
- `DIM_SITE_COUNTY`
- `DIM_SITE_DISTRICT`
- `DIM_SPECTRUM_FILE`
- `DIM_SPECTRUM_PROCEDURE`
- equipment and type dimensions
- spectrum fact/bridge tables

Main responsibilities:

- manage explicit transactions for multi-step ingestion
- normalize and resolve geographic metadata
- create and update spectrum sites
- build repository paths from resolved site metadata
- register source and server files
- register procedures, detector types, measure units, and trace types
- insert spectra and bridge relations
- export processed metadata to Parquet

Main interface groups:

Transaction control:

- `begin_transaction()`
- `commit()`
- `rollback()`

Site and geography:

- `_normalize_site_data()`
- `_normalize_string()`
- `insert_site()`
- `update_site()`
- `get_site_id()`
- `_get_geographic_codes()`
- `build_path()`

File and spectrum registration:

- `get_file_type_id_by_hostname()`
- `insert_file()`
- `insert_procedure()`
- `get_or_create_spectrum_equipment()`
- `insert_detector_type()`
- `insert_measure_unit()`
- `insert_trace_type()`
- `insert_spectrum()`
- `insert_bridge_spectrum_file()`

Publication support:

- `export_parquet()`
- `get_latest_processing_time()`

Operational notes:

- `dbHandlerRFM` is transaction-aware; callers are expected to define the transaction boundary
- standalone calls commit immediately when no explicit transaction is active
- geographic resolution is deterministic and avoids fuzzy matching

## Usage Guidance

Use `dbHandlerBKP` when the code is orchestrating work:

- polling tasks
- changing workflow state
- suspending/resuming on host connectivity
- updating lifecycle history

Use `dbHandlerRFM` when the code is persisting processed measurement data:

- sites
- files
- procedures
- spectra
- publication artifacts

Use `DBHandlerBase` only as an implementation dependency of the domain handlers.
Service code should not call the base helper methods directly unless it is
implementing a new domain handler.
