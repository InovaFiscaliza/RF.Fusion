# Database Interface

This folder contains the database access layer for appCataloga.

The design is split into one shared base handler plus three domain handlers:

- `dbHandlerBase.py`: reusable MySQL connection and SQL helper layer
- `dbHandlerBKP.py`: operational domain for hosts, host tasks, file tasks, and file history
- `dbHandlerRFM.py`: analytical domain for sites, files, procedures, spectrum entities, and publication support
- `dbHandlerSummary.py`: summary domain for outbox consumption and `RFFUSION_SUMMARY` refresh writes

## Architecture

The intended layering is:

1. Services and workers call the domain handler that owns the target database concern
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
- `summary_enqueue_refresh()`: append one dirty scope into `BPDATA.SUMMARY_OUTBOX`

Important conventions:

- joined selects return normalized keys like `HOST__ID_HOST`
- `#CUSTOM#...` keys are reserved for trusted SQL fragments
- the base layer defaults to autocommit unless a subclass manages an explicit transaction

## Summary Architecture

`RFFUSION_SUMMARY` is maintained by a Python worker instead of the legacy
MariaDB Event (`EVT_REFRESH_ALL_RFFUSION_SUMMARY_10MIN`) that ran a full
rebuild every 10 minutes regardless of activity.

The flow is:

1. `dbHandlerBKP` publishes host/month invalidation scopes into `BPDATA.SUMMARY_OUTBOX`
   whenever host tasks, file tasks, or backup/processing events change.
2. `dbHandlerRFM` publishes site/equipment invalidation scopes into the same
   outbox whenever spectrum sites or equipment observations change.
3. `appCataloga_rffusion_summary_worker.py` runs as a daemon and drives two
   update strategies through `dbHandlerSummary` and `SummaryRefreshEngine`:

   - **Full reconcile**: rebuilds all summary tables from source on startup and
     nightly at 02:00 BRT (UTC-3).  After each reconcile, both queue tables are
     fully purged — `SUMMARY_OUTBOX` (all rows deleted) and
     `SUMMARY_WORKER_STATE` (consumer row deleted, auto-recreated on next poll
     with `ID_LAST_OUTBOX = 0`).

   - **Incremental update**: between reconciles, reads the next batch of outbox
     rows after the stored checkpoint, identifies the dirty scope, and refreshes
     only the affected summary objects.  After each successful batch the
     checkpoint is advanced and the consumed outbox rows are deleted immediately
     (`drain_consumed_outbox`), keeping `SUMMARY_OUTBOX` small regardless of
     event volume.

4. A MariaDB named lock (`RFFUSION_SUMMARY_PY_WORKER`) held by the worker's
   dedicated `lock_db` connection prevents two instances from running
   concurrently.  The lock is released automatically when the holding connection
   closes.

This keeps the public `RFFUSION_SUMMARY` schema stable for `webfusion`,
MATLAB and other readers while moving refresh orchestration out of hot
`INSERT ... SELECT` paths on `BPDATA`.

Queue table roles:

| Table | Schema | Role |
|-------|--------|------|
| `SUMMARY_OUTBOX` | `BPDATA` | Append-only event queue; publishers write, worker drains |
| `SUMMARY_WORKER_STATE` | `BPDATA` | Single-row consumer checkpoint; stores `ID_LAST_OUTBOX` and health fields |

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
- `host_task_suspend_by_host()`
- `host_task_resume_by_host()`

File task operations:

- `read_file_task()`
- `file_task_create()`
- `file_task_update()`
- `file_task_delete()`
- `file_task_suspend_by_host()`
- `file_task_resume_by_host()`
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
- summary invalidation is best-effort; the dedicated worker still runs a periodic full reconcile to cap drift

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

Operational notes:

- `dbHandlerRFM` is transaction-aware; callers are expected to define the transaction boundary
- standalone calls commit immediately when no explicit transaction is active
- geographic resolution is deterministic and avoids fuzzy matching
- writes that affect summary-facing site/equipment observations publish dirty scopes into `BPDATA.SUMMARY_OUTBOX`

## dbHandlerSummary

`dbHandlerSummary` owns the Python refresh path for `RFFUSION_SUMMARY`.

Main entities:

- `BPDATA.SUMMARY_OUTBOX` — event queue consumed by this handler
- `BPDATA.SUMMARY_WORKER_STATE` — durable consumer checkpoint
- `RFFUSION_SUMMARY.SUMMARY_REFRESH_STATE` — per-object last-run telemetry
- `RFFUSION_SUMMARY.SUMMARY_REFRESH_LOG` — historical refresh audit log
- all public summary tables written during refresh passes

Main responsibilities:

- manage the singleton MariaDB lock that prevents dual worker instances
- read append-only outbox batches in checkpoint order
- persist worker checkpoint, status, and health timestamps
- implement queue drain and post-reconcile reset
- persist per-object refresh telemetry
- replace or upsert summary-table rows during refresh passes

Key method groups:

**Singleton lock**

- `configure_worker_session()` — sets session-level DB parameters (timeouts, etc.)
- `disable_sql_event()` — disables the legacy MariaDB Event at startup if configured

**Worker state (checkpoint)**

- `read_worker_state(consumer_name)` — returns the checkpoint row; auto-creates it
  with `ID_LAST_OUTBOX = 0` on first run so no manual bootstrap is needed
- `mark_worker_start(consumer_name)` — stamps `DT_LAST_START`, sets
  `NA_STATUS = 'running'`; called before every pass for stall detection
- `mark_worker_success(consumer_name, last_outbox_id, ...)` — advances
  `ID_LAST_OUTBOX` and stamps `DT_LAST_SUCCESS`; only called after a
  successful refresh so a failed batch is retried
- `mark_worker_failure(consumer_name, error_message)` — records the error
  without advancing the checkpoint

**Outbox consumption**

- `read_outbox_batch(consumer_name, batch_size)` — returns the next N rows with
  `ID_OUTBOX > ID_LAST_OUTBOX`, ordered ascending so events are processed in
  publish order
- `drain_consumed_outbox(consumer_name)` — deletes all rows with
  `ID_OUTBOX <= ID_LAST_OUTBOX` immediately after a successful incremental
  batch; true queue semantics, prevents unbounded outbox growth
- `reset_after_reconcile(consumer_name)` — purges *all* outbox rows and drops
  the consumer state row; called after a full reconcile because every
  accumulated event is already reflected in the rebuilt summary
- `prune_processed_outbox(consumer_name, keep_days)` — legacy time-gated prune;
  retained for manual maintenance use but no longer called by the worker

**Summary refresh telemetry**

- `summary_refresh_start(object_name)` — upserts a `SUMMARY_REFRESH_STATE` row
  with `IS_SUCCESS = 0` and returns the start timestamp
- `summary_refresh_success(object_name, started_at, row_count)` — updates the
  state row to `IS_SUCCESS = 1` and writes a `SUMMARY_REFRESH_LOG` entry
- `summary_refresh_failure(object_name, started_at, error_message)` — records
  the failure in both the state row and the log

**Summary table writes**

- `replace_table_rows(table, rows)` — shadow-table swap write strategy.
  Writes rows into a `{table}_shadow` staging table then issues one atomic
  `RENAME TABLE` to promote it as the live table.  Readers never see an empty
  or partially-written table.  The `_shadow` table is created automatically on
  the first call, but later schema changes must be migrated explicitly on both
  the live and `_shadow` tables.  If they drift, the worker now fails fast with
  a schema-mismatch error instead of silently repairing the shadow.  Use for
  objects that are always rebuilt in full (`HOST_CURRENT_SNAPSHOT`,
  `MAP_SITE_SUMMARY`, etc.).
- `upsert_table_rows(table, rows)` — row-level UPSERT for tables that support
  partial updates scoped to the dirty hosts

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

Use `dbHandlerSummary` when the code is maintaining the public summary read models:

- consuming `SUMMARY_OUTBOX`
- updating worker checkpoint state
- refreshing `RFFUSION_SUMMARY`
- writing summary refresh telemetry

Use `DBHandlerBase` only as an implementation dependency of the domain handlers.
Service code should not call the base helper methods directly unless it is
implementing a new domain handler.
