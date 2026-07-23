# appCataloga — Target Architecture

This document is the **authoritative specification** for the target architecture
of the appCataloga system. It describes what the code **must be**, not what it
currently is.

Every agent, developer, and pull request must be evaluated against this document.
When in doubt between two approaches, the approach that better satisfies this
document is correct. When this document does not cover a case, it must be updated
before implementing the case.

---

## 1. System Overview

appCataloga is a suite of long-running Python daemon processes that manage the
lifecycle of radio frequency measurement files across a network of remote stations.

The system does three things:
1. **Ingest** — discover files on remote stations via SSH/SFTP and track them in a queue
2. **Process** — transfer files to a central server, parse them, and persist spectra to a database
3. **Maintain** — garbage-collect expired artifacts, refresh statistics, and monitor host health

Each concern is owned by exactly one type of process. No concern crosses a process
boundary at runtime.

---

## 2. Layer Diagram

Strict call direction: **higher layers may call lower layers; lower layers must
never import from higher layers.**

```
┌────────────────────────────────────────────────────────────────┐
│  ENTRYPOINTS  (appCataloga_*.py)                               │
│  Own: daemon loop, queue state transitions, signal handling    │
│  May call: domain handlers, shared, db handlers                │
└─────────────────────────┬──────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐
│ host_handler │  │  appAnalise  │  │   summary_handler    │
│              │  │              │  │                      │
│ bootstrap_   │  │ task_flow    │  │ refresh_engine.py    │
│  flow        │  │ artifact_    │  │                      │
│ host_        │  │  handler     │  └──────────────────────┘
│  connectivity│  │ payload_     │
│ host_runtime │  │  parser      │
│ host_context │  │ appAnalise_  │
│ maintenance  │  │  connection  │
│ ssh_utils    │  └──────────────┘
└──────────────┘
          │               │               │
          └───────────────┼───────────────┘
                          ▼
┌────────────────────────────────────────────────────────────────┐
│  SHARED  (shared/)                                             │
│  errors, logging_utils, tools, filter, file_metadata,         │
│  geolocation_utils, constants                                  │
│  Imported by all layers. Never imports from handlers or        │
│  entrypoints.                                                  │
└────────────────────────┬───────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────┐
│  DB HANDLERS  (db/)                                            │
│  DBHandlerBase → dbHandlerBKP                                  │
│               → dbHandlerRFM                                   │
│               → dbHandlerSummary                               │
│  Own: all SQL. No business logic.                              │
│  May call: shared/. Never imports from handlers or entrypoints.│
└────────────────────────┬───────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────┐
│  MariaDB                                                       │
│  BPDATA (operational)  │  RFDATA (analytical)                  │
└────────────────────────────────────────────────────────────────┘
```

### Layer enforcement rules

- **Entrypoints** own the queue state machine. They never contain domain logic
  that could be tested independently. They never contain raw SQL.
- **Domain handlers** (`host_handler/`, `appAnalise/`, `summary_handler/`) own
  business rules. They call DB handlers for persistence. They never issue
  `continue` or `break` into a caller's loop.
- **Shared** is pure utility. It has no knowledge of workers, handlers, or DB schema.
- **DB handlers** contain only SQL and connection management. No file I/O,
  no SSH, no geocoding, no business decisions.
- **No layer may call upward.** A DB handler must never import from `host_handler`.
  A shared module must never import from `db/`.

---

## 3. Database Architecture

### 3.1 Two databases, two purposes

| Database | Constant | Purpose | Owner handler |
|---|---|---|---|
| `BPDATA` | `k.BKP_DATABASE_NAME` | Operational queue: hosts, tasks, history | `dbHandlerBKP` |
| `RFDATA` | `k.RFM_DATABASE_NAME` | Analytical: spectra, sites, equipment | `dbHandlerRFM` |

Workers that only manage the queue use `dbHandlerBKP` only.
Workers that also persist spectra open both: `db_bp: dbHandlerBKP` for queue
operations and `db_rfm: dbHandlerRFM` for analytical writes.

### 3.1.1 Ownership boundary: operational queue vs analytical persistence

This distinction is mandatory:

- **BPDATA queue lifecycle belongs to the entrypoint.**
  The worker entrypoint owns `HOST_TASK`, `FILE_TASK`, and `FILE_TASK_HISTORY`
  state transitions such as claim, DONE, ERROR, FROZEN, delete, and promote.
- **Domain handlers must not finalize operational queue rows.**
  They do not decide the final state of `FILE_TASK` / `FILE_TASK_HISTORY` and
  do not write the worker's terminal queue resolution on behalf of the
  entrypoint.
- **Domain handlers may persist analytical data in RFDATA when that persistence
  is part of the domain use case itself.**
  Example: the appAnalise processing flow may resolve sites, insert spectra,
  and register analytical file relations inside the `appAnalise/` domain layer,
  because those writes are part of "process this artifact", not part of queue
  ownership.
- **The entrypoint remains the orchestrator even when a domain handler writes
  analytical rows.**
  The worker still reads the queue row, claims it, calls the domain flow,
  and finalizes the operational state afterward.

In short:

- `BPDATA` operational queue ownership: entrypoint
- `RFDATA` analytical persistence: domain handler allowed
- Final queue resolution: entrypoint only

### 3.2 Key tables

**BPDATA:**
- `HOST` — registered measurement stations
- `HOST_TASK` — host-level work queue (discovery, check, statistics)
- `FILE_TASK` — per-file work queue (backup, process)
- `FILE_TASK_HISTORY` — immutable audit trail of every file operation
- `SUMMARY_OUTBOX` — change feed for the summary worker

**RFDATA:**
- `FACT_SPECTRUM` — core analytical fact table
- `DIM_SPECTRUM_SITE` — geographic resolution of measurement locations
- `DIM_SPECTRUM_EQUIPMENT`, `DIM_SPECTRUM_FILE`, etc. — dimension tables

### 3.3 DB handler interface contract

Workers call only **public methods** of DB handlers. Methods prefixed with `_`
(e.g., `_select_rows`, `_insert_row`) are internal to the DB layer. Calling
a protected method from a worker or domain handler is a layer violation.

If a worker needs a query that does not have a public method, the correct fix
is to add a named public method to the DB handler — not to call `_select_rows`
from outside.

### 3.4 Transaction ownership

Any write that spans two or more tables must be wrapped in a transaction:

```python
db.begin_transaction()
try:
    db.table_a_update(...)
    db.table_b_update(...)
    db.commit()
except Exception:
    db.rollback()
    raise
```

The following pairs are always atomic — no exceptions:
- `file_task_update` + `file_history_update`
- `file_task_delete` + `file_history_update`
- `file_task_update` (FROZEN) + `file_history_update` (FROZEN)

---

## 4. Worker Anatomy

Every queue-driven worker is a single Python file (`appCataloga_*.py`) with
exactly this structure, in this order:

```
1.  Shebang + encoding declaration
2.  Module docstring  (what the service does / queue row it owns / one constraint)
3.  Standard library imports
4.  bootstrap_paths setup
5.  Internal imports  (db, handlers, shared, config)
6.  Global constants  (SERVICE_NAME)
7.  Global state      (log, process_status)
8.  Signal handling   (_shutdown_cleanup + install_shutdown_handlers at module level)
9.  Loop helpers      (_read_next_task, _claim_task)
10. Work function     (_do_work)
11. Finalization      (_finalize_success, _finalize_error, _cleanup)
12. main()
13. if __name__ == "__main__":
```

No section may be reordered. No additional top-level structure is permitted.

### 4.1 Mandatory function signatures

All queue-driven workers implement these five functions with these exact names
and signature shapes. The body differs per worker; the interface does not.

```python
def _read_next_task(db: dbHandlerBKP) -> dict | None:
    """Return next claimable row, or None if queue empty. Never raises."""

def _claim_task(db: dbHandlerBKP, task: dict) -> bool:
    """Atomically mark task RUNNING. Return False if race lost. Never raises."""

def _do_work(db: dbHandlerBKP, task: dict) -> dict:
    """Execute domain work. Returns result dict with at least {elapsed_sec: float}.
    MUST raise on any failure — never returns a sentinel."""
    # Workers with two DB connections:
    # def _do_work(db_bp: dbHandlerBKP, db_rfm: dbHandlerRFM, task: dict) -> dict:

def _finalize_success(db: dbHandlerBKP, task: dict, result: dict) -> None:
    """Write DONE to queue. Emit task_done log event. Never raises."""

def _finalize_error(
    db: dbHandlerBKP, task: dict | None, err: errors.ErrorHandler
) -> None:
    """Write ERROR/FROZEN to queue. Emit task_error log event.
    Safe to call when task is None. Never raises."""

def _cleanup(task: dict | None, *, sftp=None) -> None:
    """Release all acquired resources. Called from finally. Never raises."""
```

`_do_work()` may contain multiple internal domain stages, but those stages do
not justify moving queue lifecycle into domain handlers. `_do_work()` delegates
the domain flow; `_finalize_success()` and `_finalize_error()` still own the
operational queue outcome.

### 4.2 The canonical main loop

```python
def main() -> None:
    log.service_start(SERVICE_NAME)
    db = _init_db()

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        task = None

        try:
            # --- read ---
            task = _read_next_task(db)
            if task is None:
                runtime_sleep.random_jitter_sleep()
                continue

            # --- claim ---
            if not _claim_task(db, task):
                runtime_sleep.random_jitter_sleep()
                continue

            # --- work ---
            result = _do_work(db, task)

            # --- finalize ---
            _finalize_success(db, task, result)

        except Exception as e:
            if not err.triggered:
                err.capture(reason="...", stage=k.STAGE_MAIN, exc=e)
            _finalize_error(db, task, err)

        finally:
            _cleanup(task)

        runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME)
```

**Invariants — never violated:**
1. `continue` is used only for "queue empty" and "claim race lost". Never for errors.
2. `finally` handles only resource cleanup. It never writes queue state.
3. Queue state changes (DONE, ERROR, FROZEN) happen only in `_finalize_success`
   and `_finalize_error`.
4. `_finalize_error` must be safe to call when `task is None`.
5. The `skip_to_next_iteration` boolean pattern is forbidden.
6. Helper functions raise on failure. They never call `err.capture()` internally
   or return a sentinel to signal failure.

### 4.3 Worker Logging Contract

All task workers use `shared/logging_utils.py` as their public logging API.
Workers must not invent ad hoc logging helpers for normal lifecycle events.

The purpose of the contract is predictability:

- a reader should know where to look for startup, claim, progress, and failure
- the same event type should mean the same thing in every worker
- timestamps must reflect when the event really happened

#### 4.3.1 Canonical worker events

| Method | When to use | When not to use |
|---|---|---|
| `log.service_start(...)` | Once when the worker process starts | Never for task-level activity |
| `log.service_stop(...)` | Once when the worker process stops cleanly | Never for task-level activity |
| `log.task_claimed(...)` | Right after one queue row is claimed successfully | Never before the claim is confirmed |
| `log.task_phase(...)` | For intermediate progress inside `_do_work()` when one domain phase finishes | Never for final task outcome or process lifecycle |
| `log.task_done(...)` | After `_finalize_success()` persists the successful queue resolution | Never as a substitute for intermediate progress |
| `log.task_frozen(...)` | After `_finalize_error()` persists a `FROZEN` outcome | Never for a terminal `ERROR` outcome |
| `log.task_error(...)` | After `_finalize_error()` persists a terminal `ERROR` outcome | Never for recoverable warnings or claim races |
| `log.warning_event(...)` | For auxiliary warnings outside the normal task lifecycle | Never when a canonical task event exists |
| `log.error_event(...)` | For auxiliary failures outside the normal task lifecycle | Never when a canonical task event exists |

#### 4.3.2 Scope of each event

- `service_start` / `service_stop` describe process lifecycle only.
- `task_claimed`, `task_done`, `task_frozen`, and `task_error` describe queue lifecycle.
- `task_phase` describes domain progress inside `_do_work()`.
- `warning_event` and `error_event` are reserved for side events such as:
  - `task_claim_race`
  - `db_init_failed`
  - `task_finalization_failed`
  - `statistics_update_failed`

If a worker event belongs to the normal queue lifecycle, it must use the
canonical `task_*` method instead of a generic `warning_event` or `error_event`.

#### 4.3.3 Timing semantics

`task_phase` exists because end-of-task timing summaries are not enough for
incident reading. If phase `finalize` completes at `00:05`, the log timestamp
must show `00:05`, not the beginning of `_do_work()`.

The timing fields mean:

- `task_phase.elapsed_sec`: duration of that single phase
- `task_phase.since_start_sec`: optional elapsed time since `_do_work()` began
- `task_done.elapsed_sec`: total duration of the successful work iteration
- `task_error.elapsed_sec`: optional elapsed time before terminal failure
- `task_frozen.elapsed_sec`: optional elapsed time before freeze resolution

`task_phase` always means **phase completed**. It must not be emitted twice as
separate `start` and `done` markers for the same phase. The entrypoint already
provides queue lifecycle context through `task_claimed`, `task_done`,
`task_error`, and `task_frozen`; domain phases only need one completion event
with timing/context.

#### 4.3.4 Phase naming

Phase names belong to the worker domain, but the usage pattern is shared.
Each worker emits `task_phase` at the moment a phase finishes, using names that
are stable within that worker:

- `appCataloga_host_check.py`: `probe`, `persist`, `queue_followup`
- `appCataloga_discovery.py`: `scan`, `queue_backlog`
- `appCataloga_file_bkp.py`: `transfer`
- `appCataloga_file_bin_process_appAnalise.py`: `process`, `site`, `db`, `finalize`
- `appCataloga_backlog_management.py`: `work`

Workers may attach domain metrics to `task_phase` or `task_done`, but these
metrics must be additive context, not a substitute for the canonical fields.

Ownership rule:

- If domain logic emits `task_phase(...)`, the entrypoint must not emit another
  `task_phase(...)` for the same work segment after `_do_work()`.
- Entrypoints may emit `task_phase(...)` only when the corresponding `_do_work()`
  path has no domain-level phase logging and the phase is genuinely local to the
  entrypoint.
- Prefer domain-owned `task_phase(...)` over entrypoint-owned `task_phase(...)`
  whenever the phase belongs to domain work rather than queue orchestration.

#### 4.3.5 Domain metrics for insert summaries

Workers may attach compact domain metrics to `task_phase` and `task_done` when
those metrics help incident reading or DB debugging. The metrics must be
summaries, never per-row lists.

The standard naming convention for sequential inserts is:

- `<entity>_count`
- `<entity>_id_start`
- `<entity>_id_end`

Examples:

- `file_task_count`
- `file_task_id_start`
- `file_task_id_end`
- `file_history_count`
- `file_history_id_start`
- `file_history_id_end`
- `spectrum_count`
- `spectrum_id_start`
- `spectrum_id_end`

Rules:

- Use these fields only when the underlying DB operation can provide them
  reliably through a public DB handler method.
- Never infer an ID range in the worker from assumptions about auto-increment
  behavior or concurrency.
- Never log raw lists of created IDs.
- Prefer `task_phase` for phase-local insert summaries and `task_done` for the
  final compact recap of the most important ranges.

Worker-specific guidance:

- `appCataloga_discovery.py`
  - attach `file_task_*` and `file_history_*` summaries to the phase that
    persists discovered rows, and optionally repeat them in `task_done`
- `appCataloga_file_bin_process_appAnalise.py`
  - attach `spectrum_*` summaries to phase `db`, and optionally repeat them in
    `task_done`

#### 4.3.6 Domain-handler logging contract

Domain handlers such as `host_handler/`, `appAnalise/`, and
`summary_handler/` may participate in observability, but they do not own the
queue lifecycle. Their logs must preserve that boundary.

Allowed methods in domain handlers:

| Method | When to use | When not to use |
|---|---|---|
| `log.task_phase(...)` | When a domain phase completes inside a domain-level orchestration function | Never for queue claim or final task resolution |
| `log.warning_event(...)` | For auxiliary warnings local to the domain flow | Never when a canonical task event exists |
| `log.error_event(...)` | For auxiliary domain failures outside final queue resolution | Never as the persisted terminal task outcome |

Forbidden methods in domain handlers:

- `log.task_claimed(...)`
- `log.task_done(...)`
- `log.task_error(...)`
- `log.task_frozen(...)`

Those events belong to the entrypoint only, because the entrypoint owns
`HOST_TASK`, `FILE_TASK`, and `FILE_TASK_HISTORY` lifecycle.

#### 4.3.7 Domain-handler scope of logging

Within one domain package, only orchestration-level functions may emit
`task_phase(...)`.

Examples:

- `host_handler/host_connectivity.py`
  - `run_check(...)` may emit `task_phase(...)`
  - `probe_host_connectivity(...)` must not emit `task_phase(...)`
- `host_handler/host_context.py`
  - `init_host_context(...)` may emit auxiliary `warning_event(...)` or
    `error_event(...)` when needed
  - low-level SSH/SFTP helpers must not invent task lifecycle logs
- `appAnalise/processing_bin.py`
  - orchestration functions may emit `task_phase(...)`
  - parsing, path-building, and row-shaping helpers stay silent

Rule of thumb:

- orchestration function: may log domain progress
- pure helper: returns data or raises; does not narrate task lifecycle

This keeps `task_phase(...)` meaningful instead of noisy.

#### 4.3.8 Host-handler phase names

The `host_handler/` package must use stable phase names when it emits
`task_phase(...)`:

- `host_connectivity.run_check(...)`
  - `probe`
  - `persist`
  - `queue_followup` when a follow-up queue action is part of the successful
    domain result
- `host_context.init_host_context(...)`
  - `connect`
- backup domain orchestration
  - `transfer`

Helpers below those orchestration seams must not invent new phase names.

#### 4.3.9 Domain-handler logging payload rules

When a domain handler emits `task_phase(...)`, it must receive the minimum
task context from the caller instead of reconstructing it indirectly:

- `service`
- `host_id`
- `task_id`
- `task_type`
- optional `worker_id`
- optional `file`

Domain handlers must not read queue identity from globals, process-local caches,
or hidden DB lookups just to build log payloads.

Low-level utility logs should stay small:

- include the domain-specific detail that explains the issue
- do not duplicate the full final error message that the entrypoint will
  persist later
- do not emit per-row or per-file progress spam when a single summary field is
  enough

#### 4.3.10 Host-handler logging contract

The `host_handler/` package is treated as one logging subsystem. Its modules
must share one vocabulary and one payload style.

The goal is to prevent drift such as:

- one file using `log.event(...)`
- another using `log.entry(...)` with free text
- another embedding pseudo-structured strings like `event=...` inside
  `log.error(...)`

All new logs in `host_handler/` must follow the rules below.

#### 4.3.11 Host-handler methods by layer

| Host-handler layer | Files | Allowed methods | Forbidden methods |
|---|---|---|---|
| Domain orchestration | `host_connectivity.py`, orchestration points in `host_context.py`, `host_maintenance.py` | `log.task_phase(...)`, `log.event(...)`, `log.warning_event(...)`, `log.error_event(...)` | `log.task_claimed(...)`, `log.task_done(...)`, `log.task_error(...)`, `log.task_frozen(...)` |
| Host transport / SSH / SFTP infrastructure | `host_ssh_utils.py` | `log.event(...)`, `log.warning_event(...)`, `log.error_event(...)` | all `task_*` methods, new `log.entry(...)` / `log.warning(...)` / `log.error(...)` free-text logs |
| Runtime helpers | `host_runtime.py` | `log.event(...)`, `log.warning_event(...)`, `log.error_event(...)` | all `task_*` methods, new free-text logs |

Notes:

- Existing legacy `log.entry(...)` calls may exist temporarily during
  migration, but they are not an accepted target pattern.
- `host_handler` modules do not own queue lifecycle, so they never emit final
  task outcome logs.

#### 4.3.12 Host-handler event naming

`host_handler/` events must use stable `snake_case` names that describe the
domain or infrastructure fact, not the Python function name.

Preferred examples:

- `ssh_connected`
- `ssh_connect_failed`
- `metadata_mode_file_skip_dedup`
- `metadata_iteration_started`
- `metadata_iteration_completed`
- `backup_transfer_progress`
- `backup_transfer_abort`
- `backup_transfer_skipped`
- `host_state_transition`
- `host_operational_recovery`
- `host_check_all_batch_done`
- `cleanup_busy_hosts`
- `cleanup_busy_hosts_failed`
- `host_release_failed`

Avoid:

- prefix-tag strings such as `[SSH]`, `[META]`, `[CLEANUP]`
- event names that only mirror implementation details
- function names embedded in the event name unless the function itself is a
  stable domain concept

#### 4.3.13 Host-handler payload fields

`host_handler/` events should use stable field names when the data exists.

Preferred shared fields:

- `component`
- `operation`
- `host`
- `host_id`
- `address`
- `connect_addr`
- `port`
- `user`
- `file`
- `remote_file`
- `local_file`
- `remote_path`
- `server_file`
- `reason`
- `error`

Rules:

- Use `host` for host logical name / UID.
- Use `address` for the configured host address.
- Use `connect_addr` for the resolved address actually used by SSH/SFTP.
- Use `file` for a single logical artifact when no direction distinction is
  needed.
- Use `remote_file` and `local_file` when transfer direction matters.
- Use `reason` for short machine-readable classification.
- Use `error` for the human-readable exception text or detail.

Field names must stay stable across files. Do not use synonyms such as:

- `addr` in one file and `address` in another
- `hostname` in one file and `host` in another
- `path` when the real meaning is `remote_file` or `server_file`

#### 4.3.14 Host-handler patterns that are forbidden

The following are forbidden target patterns in `host_handler/`:

- `log.entry(f"[SSH] ...")`
- `log.entry(f"[META] ...")`
- `log.error(f"event=...")`
- `log.warning(f"event=...")`
- embedding the Python function name as a normal log field just to locate the
  source line

If location context is useful, prefer:

- `component`
- `operation`

instead of a literal function name.

#### 4.3.15 Host-handler logging intent

The purpose of `host_handler/` logging is:

1. show domain progress through `task_phase(...)` only at orchestration seams
2. show infrastructure side events through structured `event(...)`
3. show anomalies through structured `warning_event(...)` and `error_event(...)`

It is not meant to narrate every helper call or every steady-state branch in
free text.

#### 4.3.16 Server-handler logging contract

The `server_handler/` package is also one logging subsystem. It owns daemon
runtime infrastructure such as signal hooks, process control, worker-pool
management, socket transport, and small timing helpers.

Its logs must stay clearly separate from worker queue lifecycle logs.

Allowed methods in `server_handler/`:

| Server-handler area | Files | Allowed methods | Forbidden methods |
|---|---|---|---|
| Signal/runtime infrastructure | `signal_runtime.py`, `process_control.py`, `worker_pool.py`, `timeout_utils.py`, `sleep.py` | `log.event(...)`, `log.warning_event(...)`, `log.error_event(...)`, `log.signal_received(...)` when applicable | all `task_*` methods, new free-text `log.entry(...)` / `log.warning(...)` / `log.error(...)` logs |
| Socket transport | `socket_handler.py` | `log.event(...)`, `log.warning_event(...)`, `log.error_event(...)` | all `task_*` methods, new free-text logs |

Notes:

- `server_handler/` does not own queue lifecycle, so it never emits
  `task_claimed`, `task_phase`, `task_done`, `task_error`, or `task_frozen`.
- Worker lifecycle remains the responsibility of the entrypoint.

#### 4.3.17 Server-handler event naming

`server_handler/` events must describe runtime or transport facts, not the
Python function that emitted them.

Preferred examples:

- `worker_pool_scan`
- `worker_spawned`
- `worker_spawn_failed`
- `worker_pool_shutdown_broadcast`
- `worker_pool_shutdown_broadcast_failed`
- `worker_seed_missing`
- `worker_pool_scale_out_failed`
- `service_stop_scan_empty`
- `service_stop_signal_sent`
- `service_stop_signal_failed`
- `service_stop_failed`
- `client_connected`
- `response_sent`
- `response_send_failed`
- `socket_message_parse_failed`

Avoid:

- free-text tags like `[parse_socket_message]`
- pseudo-structured strings inside `log.error(...)` or `log.warning(...)`
- event names that only expose implementation details instead of runtime facts

#### 4.3.18 Server-handler payload fields

`server_handler/` events should use stable field names when the data exists.

Preferred shared fields:

- `component`
- `operation`
- `script_name`
- `signal`
- `pid`
- `worker_id`
- `active_workers`
- `targets`
- `client_address`
- `peer_ip`
- `reason`
- `error`

Rules:

- Use `component` for coarse runtime area such as `worker_pool`,
  `process_control`, `signal_runtime`, or `socket_handler`.
- Use `operation` for the specific runtime action such as `spawn`,
  `broadcast_shutdown`, `stop_self_service`, or `parse_message`.
- Use `reason` for compact machine-readable classification.
- Use `error` for the exception text or human-readable failure detail.

Do not mix synonyms such as:

- `client` vs `peer_ip` vs `client_address` for the same concept
- `workers` vs `active_workers`
- `addr` vs `peer_ip`

#### 4.3.19 Server-handler patterns that are forbidden

The following are forbidden target patterns in `server_handler/`:

- `logger.warning(f"event=...")`
- `logger.error(f"event=...")`
- `logger.entry("[parse_socket_message] ...")`
- any new free-text log whose content could be expressed as one structured
  event with stable fields

If source location context is useful, prefer:

- `component`
- `operation`

instead of embedding a Python function name in the log text.

#### 4.3.20 Server-handler logging intent

The purpose of `server_handler/` logging is:

1. show runtime control actions such as signal handling, sibling shutdown, and
   worker-pool repair
2. show transport events such as socket connect/send/parse failures
3. report anomalies through structured `warning_event(...)` and
   `error_event(...)`

It is not meant to narrate helper internals in free text or to re-describe the
worker lifecycle already logged by the entrypoints.

### 4.4 Process Categories

Every file in the entrypoints layer belongs to exactly one of four categories.
The category determines which loop skeleton applies.

| Category | Files | Loop driver | Queue |
|---|---|---|---|
| **Gateway** | `appCataloga.py` | `select()` on TCP sockets | None — dispatches incoming requests |
| **Task worker** | `appCataloga_host_check.py`, `appCataloga_discovery.py`, `appCataloga_backlog_management.py`, `appCataloga_file_bkp.py`, `appCataloga_file_bin_process_appAnalise.py` | Polls `HOST_TASK` or `FILE_TASK` | Claim / finalize queue row |
| **Maintenance daemon** | `appCataloga_host_maintenance.py`, `appCataloga_garbage_collector.py`, `appCataloga_summary_database.py` | Timer or checkpoint cursor | No task claim — interval-driven |
| **Bootstrap / lifecycle** | `bootstrap_paths.py`, `safe_stop.py` | Not daemons | N/A |

#### Gateway

`appCataloga.py` owns a TCP socket selector and dispatches incoming JSON
requests (from Zabbix) into `HOST_TASK` or `FILE_TASK` rows. It has no claim
loop. The anatomy in §4 and the signatures in §4.1 do **not** apply to it.

#### Task worker

Task workers follow §4 exactly. Multiple instances of the same worker can run
concurrently because claim is atomic.

`HOST_TASK_UPDATE_STATISTICS_TYPE` tasks remain owned by
`appCataloga_host_check.py`. They share the same `HOST_TASK` queue as
connectivity tasks and the same worker priority list ensures they are never
stormed ahead of fresh CHECK tasks.

#### Maintenance daemon

Maintenance daemons do not claim queue rows. They wake on a timer or
checkpoint, apply corrections or refresh derived data, and sleep again.
Their loop skeleton is:

```python
while process_status["running"]:
    err = errors.ErrorHandler(log)
    try:
        if _time_to_run():
            _do_maintenance(db, err)
    except Exception as e:
        err.capture(reason="...", stage=k.STAGE_MAIN, exc=e)
    finally:
        _last_run = datetime.now()
    runtime_sleep.random_jitter_sleep()
```

The mandatory signatures in §4.1 do not apply. The framing invariants
(`process_status`, `signal_runtime`, `log.service_start`/`log.service_stop`)
do apply to all maintenance daemons without exception.

`appCataloga_summary_database.py` uses a checkpoint cursor (SUMMARY_OUTBOX
watermark) rather than a wall-clock timer, but is otherwise a maintenance daemon —
no task claim, no DONE/ERROR finalization.

#### Bootstrap / lifecycle

`bootstrap_paths.py` runs once at import time to ensure the Python path is
correct. `safe_stop.py` is a CLI tool that signals running workers to shut
down cleanly. Neither is a daemon.
---

## 5. Module Ownership Map

Each concern has exactly one home. Adding a function to the wrong module is a
layer violation even if it works.

| Concern | Module |
|---|---|
| Queue read / claim / finalize | `appCataloga_*.py` (entrypoint) |
| SSH/SFTP bootstrap and retry | `host_handler/bootstrap_flow.py` |
| Host connectivity state machine | `host_handler/host_connectivity.py` |
| Host BUSY lock management | `host_handler/host_runtime.py` |
| Host SSH session context | `host_handler/host_context.py` |
| SSH low-level utilities | `host_handler/ssh_utils.py` |
| Backup transfer + integrity check | `host_handler/backup_flow.py` *(target)* |
| appAnalise DB task lifecycle | `appAnalise/task_flow.py` |
| appAnalise filesystem artifacts | `appAnalise/artifact_handler.py` *(target)* |
| appAnalise payload parsing | `appAnalise/payload_parser.py` |
| appAnalise service connection | `appAnalise/appAnalise_connection.py` |
| GC filesystem operations | `gc_handler/gc_maintenance.py` *(target)* |
| Summary refresh engine | `summary_handler/refresh_engine.py` |
| Error capture / classification | `shared/errors.py` |
| Structured logging | `shared/logging_utils.py` |
| File metadata value object | `shared/file_metadata.py` |
| Shared constants (non-config) | `shared/constants.py` |
| Host filter rules | `shared/filter.py` |
| Geocoding utilities | `shared/geolocation_utils.py` |
| All numeric/string config constants | `config.py` |
| BPDATA SQL | `db/dbHandlerBKP.py` |
| RFDATA SQL | `db/dbHandlerRFM.py` |
| Summary DB SQL | `db/dbHandlerSummary.py` |
| Generic SQL / connection | `db/dbHandlerBase.py` |

Modules marked *(target)* do not exist yet. They are created during the refactor
described in `INSTRUCTIONS.md`.

### 5.1 Package naming conventions

Domain handler packages use a bare noun as the directory name — no `_handler`
suffix. The canonical names and their current state:

| Target name | Current name | Purpose |
|---|---|---|
| `host/` | `host_handler/` *(rename target)* | SSH/SFTP host operations |
| `appAnalise/` | `appAnalise/` *(keep)* | appAnalise external service integration |
| `summary/` | `summary_handler/` *(rename target)* | RFFUSION_SUMMARY read-model refresh |
| `runtime/` | `server_handler/` *(rename target)* | Daemon runtime infrastructure: signal handling, sleep, process control, socket utilities |
| `gc/` | *(does not exist yet)* | Garbage collection filesystem operations |

**Rename note:** `server_handler/` is misleadingly named. Its own `__init__.py`
describes it as *"Runtime infrastructure shared by appCataloga entrypoints"*.
The rename to `runtime/` corrects this. The rename requires an import update pass
across all entrypoints and must be done as a single atomic commit.

`socket_handler.py` inside `server_handler/` is the only module that does not
belong in `runtime/` — it is specific to the TCP gateway. On rename, it moves
to `appCataloga.py` directly or a future `gateway/` package.

---

## 6. Type Contracts

### 6.1 DB handler parameters

DB handler parameters are always typed with their concrete class.
`Any`, untyped, or bare `db` names are not permitted in new code.

```python
# Correct
def _do_work(db_bp: dbHandlerBKP, db_rfm: dbHandlerRFM, task: dict) -> dict:

# Forbidden
def _do_work(db, db2, task):
def _do_work(db: Any, task: dict):
```

### 6.2 Task dict schemas (`TypedDict`)

The task dicts returned by DB handlers are plain dicts with fixed key schemas.
These schemas must be declared as `TypedDict` in `shared/task_types.py` (new file)
so the IDE can resolve field access and flag typos at write time.

**`HostTask`** — returned by `db.host_task_read()`:
```python
class HostTask(TypedDict):
    HOST__ID_HOST:               int
    HOST__NA_HOST_NAME:          str
    HOST__NA_HOST_ADDRESS:       str
    HOST__NU_SSH_PORT:           int
    HOST__NA_SSH_USER:           str
    HOST__NA_SSH_PASSWORD:       str
    HOST_TASK__ID_HOST_TASK:     int
    HOST_TASK__NU_TYPE:          int
    HOST_TASK__NU_STATUS:        int
    host_filter:                 dict
```

**`FileTask`** — returned by `db.read_file_task()`:
```python
class FileTask(TypedDict):
    FILE_TASK__ID_FILE_TASK:         int
    FILE_TASK__NA_SERVER_FILE_PATH:  str
    FILE_TASK__NA_SERVER_FILE_NAME:  str
    FILE_TASK__NA_HOST_FILE_PATH:    str
    FILE_TASK__NA_HOST_FILE_NAME:    str
    FILE_TASK__NA_EXTENSION_HOST:    str
    FILE_TASK__DT_FILE_CREATED_HOST: datetime
    FILE_TASK__DT_FILE_MODIFIED_HOST: datetime
    FILE_TASK__VL_FILE_SIZE_KB_HOST: float
    FILE_TASK__NA_EXTENSION_SERVER:  str | None
    FILE_TASK__DT_FILE_CREATED_SERVER: datetime | None
    FILE_TASK__DT_FILE_MODIFIED_SERVER: datetime | None
    FILE_TASK__VL_FILE_SIZE_KB_SERVER: float | None
    HOST__ID_HOST:                   int
    HOST__NA_HOST_NAME:              str
```

**`WorkResult`** — returned by every `_do_work()`:
```python
class WorkResult(TypedDict):
    elapsed_sec:  float          # mandatory in every worker
    # worker-specific fields follow
```

### 6.3 Error stage constants

Stage strings used in `err.capture(stage=...)` and `err.stage` comparisons are
defined in `config.py` as `STAGE_*` constants. Inline strings are forbidden.

```python
# Correct
err.capture(reason="SSH failed", stage=k.STAGE_CONNECT, exc=e)
if err.stage in {k.STAGE_AUTH, k.STAGE_SSH}:

# Forbidden
err.capture(reason="SSH failed", stage="CONNECT", exc=e)
if err.stage in {"AUTH", "SSH"}:
```

---

## 7. Error Handling Contract

### 7.1 One API

`err.capture(reason=..., stage=..., exc=...)` is the only error capture call.
`err.set(...)` is removed from all files.

### 7.2 One call site per iteration

`err.capture()` is called at most once per loop iteration, in the `except` block
of `main()`. Domain handler functions signal failure by raising. They do not
call `err.capture()` internally.

### 7.3 SSH bootstrap exception types

The bootstrap layer raises typed exceptions to distinguish outcomes:

- `bootstrap_flow.TransientBootstrapError` — failure is retryable; the retry
  handler has already been called; the loop should `continue`.
- Any other exception — fatal; the caller must call `err.capture()` and
  `_finalize_error()`.

Checking `if sftp is None` to detect bootstrap failure is forbidden.

---

## 8. Logging Contract

### 8.1 Structured events only

Operational data is logged through the public API of
`shared/logging_utils.py`. Workers must use canonical helpers such as
`log.task_claimed(...)`, `log.task_phase(...)`, `log.task_done(...)`,
`log.task_error(...)`, and `log.task_frozen(...)`.

Direct string assembly such as `log.warning(f"event=foo key=value")` is
forbidden for structured data.

### 8.2 Mandatory events per task lifecycle

Every queue-driven worker follows the canonical event contract defined in
§4.3. Names are fixed; no aliases.

| Event | Method | Required fields |
|---|---|---|
| `task_claimed` | `log.task_claimed` | `task_id`, `host_id` |
| `task_phase` | `log.task_phase` | `task_id`, `host_id`, `phase`, `elapsed_sec` |
| `task_done` | `log.task_done` | `task_id`, `host_id`, `elapsed_sec` |
| `task_error` | `log.task_error` | `task_id`, `host_id`, `stage`, `error` |
| `task_frozen` | `log.task_frozen` | `task_id`, `host_id`, `stage` or freeze detail |

Optional domain metrics may be added to `task_phase` or `task_done` using the
summary naming convention from §4.3.5, such as `file_task_count`,
`file_task_id_start`, `file_task_id_end`, `spectrum_count`,
`spectrum_id_start`, and `spectrum_id_end`.

### 8.3 DB Layer Logging Contract

DB handlers do not narrate task lifecycle. They log only database
infrastructure and handler-local anomalies.

The worker remains responsible for:

- `task_claimed`
- `task_phase`
- `task_done`
- `task_error`
- `task_frozen`

The DB layer must not emit those events.

#### 8.3.1 Allowed DB logging methods

| Method | When to use | When not to use |
|---|---|---|
| `log.event(...)` | Rare, useful infrastructure events | Never for routine CRUD success |
| `log.warning_event(...)` | Non-fatal DB or handler anomalies | Never when the situation is normal and expected |
| `log.error_event(...)` | Connection, SQL, transaction, or publish failures | Never for recoverable business outcomes already handled by the worker |

#### 8.3.2 Allowed DB event categories

| Category | Examples | Owner |
|---|---|---|
| Connection lifecycle | `db_connection_reconnected`, `db_connect_failed`, `db_reconnect_failed`, `db_disconnect_failed` | `DBHandlerBase` |
| Generic SQL failure | `db_insert_failed`, `db_update_failed`, `db_delete_failed`, `db_select_failed`, `db_execute_failed`, `db_executemany_failed` | `DBHandlerBase` |
| Invalid handler input | `db_invalid_input` | `DBHandlerBase` or concrete handler |
| Auxiliary publish failure | `summary_enqueue_failed`, `summary_scope_publish_failed` | concrete handler |

#### 8.3.3 Forbidden DB log patterns

The DB layer must not emit verbose success narration for normal operations.
These patterns are forbidden as steady-state logs:

- `"Initialized for DB ..."`
- `"updated successfully"`
- `"deleted successfully"`
- `"created or updated successfully"`
- handcrafted string prefixes such as `"[DB]"`, `"[DBHandlerBKP]"`,
  `"[DBHandlerRFM]"`, `"[EXPORT]"`, `"[CLEANUP]"`

If a DB helper needs to return useful operational detail, it returns structured
data to the caller. The worker decides whether to surface that data in
`task_phase` or `task_done`.

#### 8.3.4 Required DB event fields

DB events should include enough context to diagnose the failing operation
without repeating business-level task narrative.

| Event type | Required fields |
|---|---|
| Connection events | `db_handler`, `database`, `operation` |
| SQL failure events | `db_handler`, `database`, `operation`, `table`, `error` |
| Auxiliary publish failures | `db_handler`, `database`, `operation`, `reason`, `error` |

Optional fields such as `rows_affected`, `where_mode`, `commit`, or
`full_reconcile` are allowed when they improve diagnosis.

### 8.4 Timestamps

Timestamps must represent when the event actually happened.

- `task_phase` is emitted when the phase finishes, not as a reconstructed
  summary at the end of `_do_work()`
- `task_done`, `task_error`, and `task_frozen` are emitted after queue
  finalization persists the durable outcome

DB column values (`DT_FILE_TASK`, `DT_PROCESSED`, etc.) continue using
`datetime.now()` — that is a DB-layer concern and is unchanged.

---

## 9. Configuration Contract

All constants live in `config.py`. Workers and handlers import it as `import config as k`.

Categories and their naming convention:

| Category | Prefix | Example |
|---|---|---|
| Database connection keys | *(no prefix)* | `BKP_DATABASE_NAME` |
| Task type identifiers | `HOST_TASK_` / `FILE_TASK_` | `HOST_TASK_CHECK_TYPE` |
| Task status values | `TASK_` | `TASK_PENDING`, `TASK_DONE` |
| Error stage identifiers | `STAGE_` | `STAGE_AUTH`, `STAGE_CONNECT` |
| Timing and limits | *(descriptive)* | `SFTP_BUSY_COOLDOWN_SECONDS` |
| Path and folder names | `*_FOLDER` / `*_SUBDIR` | `TRASH_FOLDER` |
| Sentinel values | `*_UNLOCKED_*` / `NONE_*` | `HOST_UNLOCKED_PID`, `NONE_FILTER` |

Inline literals for any of the above are a violation. The only acceptable
inline numeric literals are one-off values with no semantic name, such as
retry counts documented inline (`range(3)  # max retries`).

---

## 10. Single-Process Constraint

Every worker is a single-threaded process. This is an architectural invariant,
not an implementation detail.

**Consequences:**
- No `threading`, `asyncio`, or `multiprocessing` inside a worker process.
- Module-level state (e.g., `process_status`) is safe because access is
  always sequential.
- Any pattern that relies on implicit non-reentrancy (e.g., module globals
  used as implicit parameters) must be refactored to explicit parameters,
  because the single-process assumption is the only thing making it safe.
- To scale throughput, the supervisor (`appCataloga.py`) spawns multiple
  worker *processes*. Parallelism is at the process level, never thread level.

Narrow exception:
- `appCataloga_host_maintenance.py` may use a bounded thread pool for
  network-only host probes owned by `host_handler/host_maintenance.py`.
- The exception exists only to fan out ICMP and short supervisory SSH probes
  during recurring maintenance sweeps.
- All DB writes, queue-state transitions, and persisted host state changes
  must remain sequential on the main thread after probe results are collected.
- The executor must not escape the maintenance sweep module or become a
  general-purpose worker-level concurrency mechanism.

---

## 11. How to Add a New Worker

Checklist — every item is mandatory:

- [ ] Create `appCataloga_<name>.py` following the anatomy in §4.
- [ ] Implement `_read_next_task`, `_claim_task`, `_do_work`, `_finalize_success`,
      `_finalize_error`, `_cleanup` with the signatures defined in §4.1.
- [ ] All DB calls go through a typed handler instance (`db: dbHandlerBKP`).
- [ ] All task dict accesses use a `TypedDict` from `shared/task_types.py` (§6.2).
- [ ] All stage strings use `k.STAGE_*` constants (§6.3).
- [ ] Log the six mandatory events (§8.2).
- [ ] Add an entry to the process table in `appCataloga.py`.
- [ ] Add a corresponding `.sh` launcher script.
- [ ] Add a test file in `test/tests/workers/test_<name>.py`.
- [ ] Update this document's §5 if the new worker introduces a new domain module.

## 12. How to Add a DB Handler Method

- [ ] The method belongs in the handler that owns the table
      (`dbHandlerBKP` for BPDATA, `dbHandlerRFM` for RFDATA).
- [ ] The method name describes the domain operation, not the SQL verb
      (`get_file_history_id`, not `select_file_task_history`).
- [ ] The method is public (no `_` prefix) if it is called from outside the class.
- [ ] The method never contains business logic — only SQL and error handling.
- [ ] Multi-table writes use the transaction primitives from `DBHandlerBase`.
- [ ] Workers must not call `_select_rows`, `_insert_row`, or any other
      `DBHandlerBase` protected method directly. Add a named method instead.

---

## 13. Testing Contract

### 13.1 Test file layout

Every worker and domain handler module has a corresponding test file:

```
test/tests/workers/test_appCataloga_host_check.py
test/tests/workers/test_appCataloga_discovery.py
test/tests/workers/test_appCataloga_file_bkp.py
test/tests/workers/test_appCataloga_file_bin_process_appAnalise.py
test/tests/workers/test_appCataloga_summary_database.py
test/tests/db/test_dbHandlerBKP.py
test/tests/db/test_dbHandlerRFM.py
test/tests/db/test_dbHandlerSummary.py
test/tests/shared/test_errors.py
```

Test files for domain handlers (host_handler, appAnalise, summary_handler) live
under `test/tests/` in a subdirectory matching the handler package name.

### 13.2 What each test file covers

**Worker tests** verify the loop logic without touching the database:

- `_read_next_task` returns `None` on empty result, returns a populated dict otherwise.
- `_claim_task` returns `False` when `rows_affected == 0` (race lost).
- `_finalize_success` calls the correct DB methods with the correct status.
- `_finalize_error` is safe when `task is None`.
- The main loop calls `_finalize_error` (not `_finalize_success`) when
  `_do_work` raises.

**DB handler tests** use a real test database (`BPDATA_TEST` / `RFDATA_TEST`) or
a SQLite in-memory substitute if the MariaDB instance is unavailable:

- Each public method executes without raising on valid inputs.
- `begin_transaction()` / `commit()` / `rollback()` leave the correct rows.

### 13.3 Mock pattern for DB handlers

```python
from unittest.mock import MagicMock, patch

def make_db() -> MagicMock:
    db = MagicMock(spec=dbHandlerBKP)
    db.in_transaction = False
    return db

def test_claim_task_race_lost():
    db = make_db()
    db.host_task_update.return_value = {"rows_affected": 0}
    result = _claim_task(db, {"task_id": 1, "host_id": 99})
    assert result is False
```

Always use `spec=` when creating a `MagicMock` for a DB handler so the mock
rejects calls to methods that don’t exist on the real class.

### 13.4 Test isolation rules

- No test modifies a shared database that other tests also use.
- No test reads from the filesystem paths configured in `config.py`.
- No test starts a real worker daemon process.
- Tests that require a running MariaDB instance are tagged `@pytest.mark.integration`
  and skipped in CI unless `RFF_DB_TEST=1` is set.

---

## 14. Launcher Script (.sh) Anatomy

Every worker has a paired `appCataloga_<name>.sh` launcher in the same directory.
All launcher scripts follow this exact template:

```bash
#!/bin/bash
# =============================================================================
# Script: appCataloga_<name>.sh
# Purpose: <one-line description> (SINGLETON or MULTI-INSTANCE)
# =============================================================================

set -e

APP_NAME="appCataloga_<name>.py"
APP_PATH="/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/$APP_NAME"
PYTHON_BIN="/opt/conda/envs/appdata/bin/python"

PID_DIR="/var/run/appCataloga"
LOG_DIR="/var/log/appCataloga"
LOG_FILE="$LOG_DIR/appCataloga_<name>.log"

mkdir -p "$PID_DIR" "$LOG_DIR"

banner() {
    w=$(tput cols 2>/dev/null || echo 80)
    echo -e "\e[34m$(printf "%0.s=" $(seq 1 $w))\e[0m"
    printf "\e[34m%*s\e[0m\n" $((($w + ${#1}) / 2)) "$1"
    echo -e "\e[34m$(printf "%0.s=" $(seq 1 $w))\e[0m"
}

check_env() {
    [[ -x "$PYTHON_BIN" ]] || { echo "[ERROR] Python not found"; exit 1; }
    [[ -f "$APP_PATH" ]] || { echo "[ERROR] Script not found"; exit 1; }
}

start() {
    banner "STARTING appCataloga_<name>"
    check_env
    pgrep -f "$APP_NAME" >/dev/null && { echo "Already running."; exit 0; }
    cd "$(dirname "$APP_PATH")"
    nohup "$PYTHON_BIN" "$APP_PATH" >> "$LOG_FILE" 2>&1 &
    echo "Started."
}

stop() {
    banner "STOPPING appCataloga_<name>"
    pkill -TERM -f "$APP_NAME" || true
    sleep 2
    pkill -KILL -f "$APP_NAME" 2>/dev/null || true
    echo "Stopped."
}

status() {
    banner "STATUS appCataloga_<name>"
    pgrep -af "$APP_NAME" || echo "Not running."
}

case "$1" in
    start) start ;;
    stop) stop ;;
    restart) stop; start ;;
    status) status ;;
    *) echo "Usage: $0 {start|stop|restart|status}" ;;
esac

echo "bye"
```

**Rules:**
- `APP_NAME` contains only the `.py` filename — `pgrep -f` matches it against
  the full command line.
- `SINGLETON` workers: the `pgrep` guard in `start()` prevents a second
  instance from launching. `MULTI-INSTANCE` workers (e.g. `file_bkp`) omit
  the guard.
- `stop()` always sends `SIGTERM` first, waits 2 s for the clean shutdown
  handler, then `SIGKILL` only as a last resort.
- The `banner()` function prints a full-width coloured separator. No other
  visual formatting is used.
- `set -e` is present in every script. No `|| true` except in `pkill` calls
  where a zero-match exit code is expected.

---

## 15. RFFUSION_SUMMARY Refactor Plan

This section documents the intended rewrite of the summary subsystem.
No code has been changed yet. The rewrite is tracked in `INSTRUCTIONS.md §12`.

### 15.1 Purpose recap

`RFFUSION_SUMMARY` is a denormalised read-model database. It exists so that
**webfusion** (web interface for telecom inspectors) and **appAnalise** (Matlab
client that transfers files and runs deep spectrum analysis on the user’s machine)
can query pre-aggregated data instantly, without running expensive JOINs against
the transactional databases or waiting for stored procedures that previously
caused deadlocks.

### 15.1.1 Public contract tables that must remain stable

Some `RFFUSION_SUMMARY` tables are not just internal WebFusion read models.
They are also consumed by the Matlab-side `appAnalise` client through
`src/webfusion/DBHandler.m`. Because of that, they must be treated as a
public compatibility contract.

The following tables are **contract-stable**:

- `HOST_LOCATION_SUMMARY`
- `MAP_SITE_SUMMARY`
- `MAP_SITE_STATION_SUMMARY`
- `SITE_EQUIPMENT_OBS_SUMMARY`

For these tables, the default rule is:

- do not rename the table
- do not remove columns consumed by external clients
- do not change the row granularity
- do not silently change the semantic meaning of existing columns

Allowed changes require explicit compatibility review:

- adding new nullable columns
- adding new indexes
- backfilling data for consistency without changing meaning
- creating new parallel read models for new consumers

If a future redesign needs different semantics, create a new table or versioned
read model instead of mutating these contract-stable tables in place.

### 15.2 Known problems with current implementation

| Problem | Location | Impact |
|---|---|---|
| `db.reset_after_reconcile(...)` never called — it is inside a comment on the same line as the statement | `appCataloga_summary_database.py` line ~360 | After every nightly reconcile, `SUMMARY_OUTBOX` grows unboundedly and the checkpoint is never reset; incremental resumes from the wrong position |
| `acquire_worker_lock` / `release_worker_lock` in `dbHandlerSummary` close the connection immediately after acquiring the lock | `db/dbHandlerSummary.py` | MariaDB user-locks are connection-scoped; closing the connection releases the lock instantly; the singleton guard does not work |
| Worker bypasses its own DB handler methods and calls `db._select_raw`, `db._connect`, `db._disconnect` directly | `appCataloga_summary_database.py` (`_acquire_db_lock`, `_release_db_lock`) | Layer violation; the lock logic is duplicated instead of using the fixed handler methods |
| `mark_worker_start` is called before the reconcile but has no matching `mark_worker_success` after it (the success is inside `reset_after_reconcile`) | `appCataloga_summary_database.py` | `SUMMARY_WORKER_STATE.NA_STATUS` is stuck at `RUNNING` after every reconcile; monitoring shows the worker as permanently in-progress |
| 442-line file; core logic is ~40 lines; the rest is docstrings and comments that obscure the control flow | whole file | Maintenance burden; hard to audit or modify |

### 15.3 Target implementation

The rewrite must:

1. **Fix `reset_after_reconcile` call** — move it to its own line, call it
   after `engine.refresh_all()` completes.

2. **Fix the singleton lock** — use `lock_db.acquire_worker_lock(WORKER_LOCK_NAME)`
   and `lock_db.release_worker_lock(WORKER_LOCK_NAME)` from `dbHandlerSummary`.
   Fix those two methods so `acquire_worker_lock` holds the connection open on
   success (lock is connection-scoped) and `release_worker_lock` releases then
   disconnects.

3. **Remove `_acquire_db_lock` / `_release_db_lock` from the worker** — they
   are layer violations. The fixed handler methods replace them.

4. **Extract `_run_full_reconcile` and `_run_incremental_batch`** as named
   functions so `main()` becomes a dispatcher, not a 150-line monolith.

5. **Reduce docstrings and comments** to one module docstring + inline comments
   at decision points only. No docstring that re-states what the code already
   says.

6. **Target line count**: ≤180 lines for the entire worker file.

### 15.4 Execution order constraint

Fix `dbHandlerSummary` lock methods first (step 2), then rewrite the worker
(steps 1, 3, 4, 5). Fixing the handler without updating the worker leaves the
worker still using the private `_acquire_db_lock` helper — both changes must
go in the same commit.
