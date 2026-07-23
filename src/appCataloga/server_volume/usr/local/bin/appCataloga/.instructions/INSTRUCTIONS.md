# Worker Refactoring Guide — appCataloga

---

> **Mandatory Refactoring Mandate — apply to every file, every function, every PR.**
>
> *Act as a Senior Software Engineer specializing in refactoring and Clean Code.
> Analyze the code below and refactor it with exclusive focus on
> **standardization and readability**.*
>
> **Strict Rules:**
>
> 1. **Do not alter business logic or current behavior.**
> 2. **Reduce excessive nesting — use guard clauses to avoid multiple nested `if`s.**
> 3. **Apply consistent naming: `snake_case` for all identifiers (variables, functions,
>    parameters, modules). SQL column names passed as keyword arguments retain their
>    original ALL_CAPS form because they map directly to DB columns.**
> 4. **Break giant functions into smaller, specialized, easy-to-maintain functions.**
> 5. **Do not add external libraries that are not already in the original code.**
> 6. **Prefer `match/case` over successive `if/elif/else` chains when branching on
>    a single value with three or more discrete cases. Reserve `if/elif` for
>    boolean conditions, range checks, and cases where `match` would reduce clarity.**
> 7. **Raise, don't return. Helper functions signal failure by raising an exception.
>    Never return a sentinel value (`None`, `False`, `-1`) or set an error object
>    and return normally. The caller must not check `err.triggered` to decide whether
>    the call succeeded — it must either get a result or catch an exception.**
> 8. **Add type annotations to every function signature you touch during refactor.
>    Do not annotate functions you did not change. Parameters use lowercase type names
>    (`str`, `int`, `bool`, `dict`, `list`). Return types are mandatory.
>    DB handler parameters (`db_bp`, `db_rfm`) must be typed with their concrete class
>    (`dbHandlerBKP`, `dbHandlerRFM`) — not `Any`, not a plain name with no annotation.
>    Without this, the IDE cannot resolve methods like `db_rfm.get_site_id()` and the
>    entire codebase becomes impossible to navigate or debug with static analysis.**
> 9. **Use the `_` prefix for every module-level function that is not part of the
>    module's public interface. A function called only from within the same file is
>    private. A function imported by another module is public and must have no `_`.**
> 10. **No magic literals. Numeric constants, status codes, and repeated string keys
>     belong in `config.py` as named constants (`config as k`). Inline literals are
>     only acceptable for one-off values that have no semantic name (e.g., `range(3)`
>     for a retry count already documented in the surrounding comment).
>     Stage identifier strings used in `err.capture(stage=...)` and in
>     `err.stage in {...}` comparisons are the most dangerous class of magic literal:
>     a typo on either side of the comparison fails silently at runtime.
>     These must be defined as constants in `config.py`:
>     `STAGE_MAIN`, `STAGE_AUTH`, `STAGE_CONNECT`, `STAGE_SSH`, `STAGE_LOCK_TASK`,
>     `STAGE_DISCOVERY`, `STAGE_BACKLOG`, `STAGE_TRANSFER`, `STAGE_PROCESS`,
>     `STAGE_CONNECTIVITY`. Usage: `err.capture(stage=k.STAGE_AUTH, ...)` and
>     `if err.stage in {k.STAGE_AUTH, k.STAGE_SSH}`.**
> 11. **Names must be clear but not excessively long. A name is good when a reader
>     can understand its purpose without reading the function body. A name is too long
>     when it restates what the type annotation or module context already says.
>     Target: 2–4 words for functions (`transfer_file`, `claim_task`, `read_next_task`),
>     1–3 words for local variables (`task`, `result`, `elapsed`, `host_id`).
>     Avoid filler words (`do_`, `handle_`, `process_`, `perform_`) unless they add
>     real meaning. Avoid redundant context in the name when the module or class
>     already provides it (`task_flow.freeze_task` is better than
>     `task_flow.freeze_task_for_manual_review_in_queue`).
>     The test: if you can remove one word and the name still means the same thing,
>     remove it.**
> 12. **All code comments must be written in plain English at an intermediate level.
>     Assume the reader understands Python but may not be a native English speaker.
>     Rules for comments:
>     — Use short sentences (under 15 words when possible).
>     — Explain *why*, not *what*. The code already shows what it does.
>     — Avoid idioms, slang, and complex grammar.
>     — Do not write comments that just restate the code in English
>       (`# increment counter` above `count += 1` is useless).
>     — A good comment answers one of: "Why does this exist?",
>       "What happens if this fails?", or "Why this value and not another?"
>     Bad: `# We need to handle the case where the task acquisition process
>       has already been completed by another concurrent worker instance.`
>     Good: `# Another worker may have claimed this task first. Skip it.`
>     Markdown documentation files (`*.md`) must be written in Portuguese
>     (PT-BR). This language rule is separate from code comments: comments stay
>     in English, documentation stays in PT-BR.**

This document is the authoritative refactoring roadmap for all `appCataloga_*.py`
workers. It was written by reading all nine workers end-to-end alongside their
supporting modules (`appAnalise/`, `host_handler/`, `shared/`, `db/`).

The goal is not a stylistic rewrite. The goal is to make each worker readable
top-to-bottom by a new engineer in under five minutes — and to make the same
change pattern obvious across all workers so future bugs are fixed once, not nine
times.

---

## 0. Guiding Principle — Coherent Abstraction Levels

The most important rule in this codebase is **coherence of abstraction level
within a single function**. A function should do one kind of thing at one level
of detail throughout its body.

## 0.1 External read-model compatibility

Some `RFFUSION_SUMMARY` tables are consumed outside the Python/WebFusion codebase
by the Matlab-side `appAnalise` client through `src/webfusion/DBHandler.m`.
These tables are not ordinary internal implementation details. They are part of
an external compatibility contract.

Treat the following tables as contract-stable:

- `HOST_LOCATION_SUMMARY`
- `MAP_SITE_SUMMARY`
- `MAP_SITE_STATION_SUMMARY`
- `SITE_EQUIPMENT_OBS_SUMMARY`

When refactoring or evolving summary code:

1. Do not rename these tables.
2. Do not remove externally consumed columns.
3. Do not change row granularity.
4. Do not silently change the meaning of existing columns.

If a new consumer needs different semantics, create a new table or a parallel
read model instead of mutating these contract-stable tables in place.

The current code violates this in two opposite directions simultaneously:

**Over-abstraction of trivial operations** — wrapper functions that do nothing
beyond renaming a call. These add a call-stack layer without adding clarity.
Example: `check_host_connectivity` in `appCataloga_host_check.py` is 8 lines
that call `probe_host_connectivity` + `log_connectivity_probe` and nothing else.
`resolve_history_file_metadata` and `build_history_metadata_from_file_meta` in
`task_flow.py` are 5–8 line functions that build a plain dict. These are not
abstractions — they are renaming.

**Under-abstraction of complex mixed operations** — large functions that combine
filesystem moves, DB writes, message composition, and error recovery in the same
body without a clear seam between concerns. Example: `finalize_task_resolution`
is 230 lines with 13 parameters. The first half moves files between directories.
The second half writes to the DB. They have no business being in the same
function because they fail differently, recover differently, and need to be read
separately.

The refactoring goal is to reach a middle ground:

- Inline helper functions that are shorter than their call site and do not have
  an independent name worth remembering.
- Split functions that mix two fundamentally different concerns (filesystem vs DB,
  probe vs persist, read vs write) into two functions, each coherent on its own.

---

## 1. Current Problems

### 1.1 Loop control is inconsistent and confusing

Three different patterns exist side-by-side:

| Worker | Pattern |
|---|---|
| `appCataloga_file_bin_process_appAnalise.py` | `try/except/finally` with a `skip_to_next_iteration` boolean flag |
| `appCataloga_discovery.py` | `try/except/finally` with `continue` inside `except` |
| `appCataloga_host_check.py` | `try/except` with no `finally`, guarded by `if task is not None` |
| `appCataloga_backlog_management.py` | `try/except` with no `finally`, clean but silent cleanup |

The `skip_to_next_iteration` flag in `appCataloga_file_bin_process_appAnalise.py`
is the most harmful: it exists only because `finally` cannot issue a `continue`.
The result is 50 lines of flag-guarded branching inside `finally` that is hard
to follow and easy to break when adding a new exit path.

### 1.2 Section naming is inconsistent

Some workers label phases `# ACT I`, `# ACT II`; others use `# Phase 1`,
`# Phase 2`; others have no labels at all. This small inconsistency makes it
harder to orient during an incident.

### 1.3 Business logic is stranded in entrypoints

Several entrypoints contain large helpers that belong in a dedicated module:

- `appCataloga_file_bkp.py` — `transfer_file_task` (150 lines), `build_server_filename`,
  `_has_discovery_metadata_drift` all live in the entrypoint. They should be in
  `host_handler/backup_flow.py`.
- `appCataloga_host_check.py` — the connectivity state machine is fully implemented
  in the entrypoint (`handle_degraded_connectivity_task`, `_handle_auth_error_connectivity_task`,
  `_finalize_connectivity_task`, `_process_connectivity_task`). These belong in
  `host_handler/host_connectivity.py`, which already exists and already has the
  `probe_host_connectivity` and `persist_host_connectivity_state` functions.
- `appCataloga_garbage_collector.py` — eight helper functions for filesystem and
  history operations are stranded in the entrypoint. They belong in a new
  `gc_handler/` module.

### 1.4 `appAnalise/task_flow.py` mixes too many concerns

This single 950-line module contains:

- Task queue lifecycle (`freeze_task_for_manual_review`, `finalize_task_resolution`)
- Filesystem artifact management (`move_file_if_present`, `finalize_successful_processing`,
  `build_resolved_files_trash_path`)
- Spectrum DB operations (`resolve_spectrum_sites`, `insert_spectra_batch`)
- Validation (`should_export`, `is_transient_filesystem_error`)

`finalize_task_resolution` alone is 230 lines with 13 parameters. Reading it
requires jumping between filesystem and DB concerns in the same function.

### 1.5 Function signatures are too long

Several helpers accumulate parameters because they try to do two things at once:

- `_finalize_connectivity_task` — 9 parameters
- `_persist_backup_error` — 9+ keyword-only parameters
- `finalize_task_resolution` — 13 keyword-only parameters

Long signatures are a symptom of a function trying to own too many decisions.
The fix is moving domain context into a small `ctx` object or splitting the function.

### 1.6 Error handling uses two different APIs

`appCataloga_host_check.py` uses `err.set("reason", "STAGE", exc)`.
All other workers use `err.capture(reason=..., stage=..., exc=...)`.
There should be one API used everywhere.

### 1.7 Non-atomic multi-table state transitions (pre-existing, now partially fixed)

`_finalize_successful_backup` in `appCataloga_file_bkp.py` writes
`file_history_update` + `file_task_update` as two separate auto-commit calls.
If the second fails, `FILE_TASK_HISTORY` shows DONE but `FILE_TASK` still shows
RUNNING. This is the same atomicity problem already fixed in `freeze_task_for_manual_review`.

### 1.8 Wrapper functions that add no value

A function that is shorter than its docstring is almost certainly wrong.
These wrappers exist throughout the codebase and only add noise to the call graph:

```python
# BAD — 8 lines to rename two calls
def check_host_connectivity(*, host_id, addr, port, user, password, event_name):
    connectivity = host_connectivity.probe_host_connectivity(addr=addr, ...)
    host_connectivity.log_connectivity_probe(log=log, ...)
    return connectivity

# FIX — call directly in main(), the context is already clear
connectivity = host_connectivity.probe_host_connectivity(addr=task["addr"], ...)
host_connectivity.log_connectivity_probe(log=log, event_name="host_check", ...)
```

```python
# BAD — 5-line dict factory
def build_history_metadata_from_file_meta(file_meta):
    return {
        "name": file_meta["file_name"],
        "extension": file_meta["extension"],
        ...
    }

# FIX — write the dict where you need it, or at most one inline expression
history_meta = {k: file_meta[v] for k, v in _HISTORY_META_KEYS}
```

**Rule**: if the function body is shorter than ~10 lines and contains no
conditional logic, no error handling, and no resource management — inline it.

### 1.9 Functions that mix filesystem and DB operations

`finalize_task_resolution` is the clearest example: the first 60 lines move
files between directories. The next 90 lines write to the DB inside a
transaction. These two concerns must be separated because:

- Filesystem moves can partially succeed (file moved, then DB fails). The
  recovery path is completely different from a pure-DB failure.
- The filesystem half needs a `logger` argument. The DB half needs `db_bp`,
  `host_id`, `file_task_id`, etc. The combined function needs all of them, which
  is why it has 13 parameters.
- Reading the DB transaction is obscured by having to scroll past the filesystem
  decisions first.

**Rule**: a function that both moves files AND writes to the DB must be split.
The filesystem half returns artifact metadata. The DB half receives that metadata
and commits. The caller sequences them.

### 1.10 `_dispatch_claimed_task` — indirection without abstraction

```python
# BAD — match/case with three cases, each calling one other function.
# Adds one call-stack level and one function to read without any simplification.
def _dispatch_claimed_task(db, task, err):
    match task["task_type"]:
        case k.HOST_TASK_UPDATE_STATISTICS_TYPE:
            _process_statistics_task(db, task, err)
        case k.HOST_TASK_CHECK_TYPE:
            _process_connectivity_task(db, task, event_name="host_check", ...)
        case k.HOST_TASK_CHECK_CONNECTION_TYPE:
            _process_connectivity_task(db, task, event_name="host_check_connection", ...)
        case _:
            err.set(...)

---

## 1.11 Domain-handler logging boundary

Refactors in `host_handler/`, `appAnalise/`, and `summary_handler/` must follow
the logging boundary from `ARCHITECTURE.md`.

Rules:

1. Domain handlers may emit `log.task_phase(...)` only from orchestration-level
   functions.
2. Pure helpers stay silent. They return data or raise.
3. `log.task_phase(...)` means **phase completed**. Do not emit separate
   `task_phase(start)` and `task_phase(done)` style markers for the same phase.
   One completion event per phase is the target pattern.
4. Domain handlers must not emit:
   - `log.task_claimed(...)`
   - `log.task_done(...)`
   - `log.task_error(...)`
   - `log.task_frozen(...)`
5. If a domain handler needs to emit `task_phase(...)`, the caller must pass
   the task context explicitly. Do not rebuild queue identity from globals or
   hidden DB reads.
6. If a domain handler already emits `task_phase(...)` for a work segment, the
   entrypoint must not emit a second `task_phase(...)` after `_do_work()` for
   that same segment.

For `host_handler/` specifically:

7. Treat the directory as one logging subsystem. Do not standardize one file
   in isolation.
8. New logs must use structured methods (`event`, `warning_event`,
   `error_event`, and only orchestration-level `task_phase`).
9. Do not introduce new prefix-tag free-text logs such as `[SSH]`, `[META]`,
   or pseudo-structured strings like `event=...` inside `log.error(...)`.
10. Reuse stable field names across files (`host`, `host_id`, `address`,
   `connect_addr`, `remote_file`, `local_file`, `reason`, `error`).

For `server_handler/` specifically:

11. Treat the directory as one runtime logging subsystem. Do not standardize
   `worker_pool.py`, `process_control.py`, or `socket_handler.py` in isolation.
12. New logs must use structured methods (`event`, `warning_event`,
    `error_event`, or `signal_received` where applicable).
13. Do not introduce pseudo-structured strings such as `event=...` inside
    `log.warning(...)` or `log.error(...)`.
14. Reuse stable runtime field names across files (`component`, `operation`,
    `script_name`, `signal`, `pid`, `worker_id`, `active_workers`,
    `client_address`, `peer_ip`, `reason`, `error`).
```

The `match/case` is 12 lines. It should be inlined directly in `main()`. The
reader of `main()` should see the dispatch without having to open another function.

**Rule**: a function whose entire body is a `match/case` or `if/elif` that only
calls other functions is a dispatcher. Dispatchers belong in `main()`, not as
named helpers.

### 1.12 Module-level global as implicit parameter — `_FIXED_SITE_UPDATE_AGGREGATOR`

Using a module-level variable as a side-channel to pass state between two functions
in the same call stack is the worst kind of hidden coupling. It makes the function
signature lie: `upsert_site(db_rfm, site_data)` looks like a pure lookup, but it
silently writes to a global accumulator when called from inside `resolve_spectrum_sites`.

This pattern is intentionally non-reentrant (documented in the source), which means
a single threading assumption is baked into the module. The real fix is to pass the
accumulator explicitly:

```python
# BAD — implicit side-channel through module global
_FIXED_SITE_UPDATE_AGGREGATOR = None   # set before loop, restored in finally

def upsert_site(db_rfm, site_data):    # secretly writes to the global
    if _FIXED_SITE_UPDATE_AGGREGATOR is not None:
        _queue_fixed_site_update(_FIXED_SITE_UPDATE_AGGREGATOR, site_id, site_data)

# GOOD — explicit parameter; the function signature tells the truth
def _upsert_site(
    db_rfm: dbHandlerRFM,
    site_data: dict,
    *,
    site_update_accumulator: dict | None = None,
) -> int:
    if site_update_accumulator is not None:
        _queue_fixed_site_update(site_update_accumulator, site_id, site_data)
```

**Rule**: module-level mutable state used as an implicit function parameter is
forbidden. Pass the value explicitly. If the call chain is too deep, group the
related state into a small dataclass and pass it through.

### 1.13 External timing wrappers instead of self-timing functions

```python
# BAD — caller wraps the call in a context manager to harvest timing
with _timed_phase(phase_durations, "process"):
    bin_data, file_meta = app_analise.process(
        file_path=server_path,
        file_name=server_name,
        export=export,
    )

# GOOD — the function returns its own timing as part of its result
bin_data, file_meta, elapsed_sec = app_analise.process(
    file_path=server_path,
    file_name=server_name,
    export=export,
)
```

External timing wrappers (`_timed_phase`, `_log_processing_phase_timings`) exist
only because `process()` does not own its own timing. They add a context-manager
layer, a `phase_durations` dict, and a second function just to log a number. The
function that does the work is the one that knows when it started and finished.

**Rule**: a function that can take more than a few milliseconds must return its
elapsed time as part of its return value. The caller logs it directly. No external
timing context managers.

### 1.14 Name displacement — helpers named after DB operations that live in worker modules

`upsert_site(db_rfm, site_data)` reads like a DB handler method. It lives in
`appAnalise/task_flow.py`. The mismatch creates constant confusion: is this a DB
primitive I should look for in `dbHandlerRFM`? Or a worker-side orchestration step?

The same naming problem appears throughout `task_flow.py`: `insert_spectra_batch`,
`resolve_spectrum_sites`, `build_repository_destination_path` — some of these sound
like DB operations, some like path utilities, and they all live in the same file
that also owns the task queue lifecycle.

This is **problem displacement without simplification**: the entrypoint's complexity
was moved to `task_flow.py` to reduce line count, but the module now contains four
different vocabularies (filesystem, DB, geocoding, queue state) with no internal
boundary. Moving code between files is not refactoring unless each destination
module becomes coherent on its own.

**Rule**: a module's name must be a true description of everything inside it.
If you cannot describe a module's contents in six words or fewer, it contains too
many concerns. Rename or split before adding more functions to it.

### 1.11 Return-as-error-signal — the `err.triggered` anti-pattern

Using `err.set(...)` + `return` to signal failure forces every caller to check
`err.triggered` before using the result. This is a hidden two-valued return — it
looks like a normal return but silently carries failure state. The pattern
compounds when callers forget to check.

```python
# BAD — caller must remember to check err.triggered after every call
def _process_connectivity_task(db, task, err, ...):
    try:
        result = probe_host_connectivity(...)
    except Exception as e:
        err.set("Connectivity test failed", "CONNECTIVITY", e)
        return                          # silent failure; caller must check err.triggered

    handle_connectivity_state(db, result, err, ...)
    # Did this also fail silently? Caller must check again.

# caller
_process_connectivity_task(db, task, err, ...)
if err.triggered:                       # easy to forget
    _finalize_error(db, task, err)
    continue
_finalize_success(db, task)
```

```python
# GOOD — failure raises; the loop's except block handles all failure uniformly
def _process_connectivity_task(db: ..., task: dict, ...) -> ConnectivityResult:
    result = probe_host_connectivity(...)   # raises on failure
    return handle_connectivity_state(db, result, ...)  # raises on failure

# caller — clean, impossible to forget error handling
try:
    result = _process_connectivity_task(db, task, ...)
    _finalize_success(db, task, result)
except Exception as e:
    err.capture(reason="...", stage="CONNECTIVITY", exc=e)
    _finalize_error(db, task, err)
```

**Rule**: `err.capture()` is called exactly once per loop iteration, in the
`except` block of `main()`. Helper functions raise; they never call
`err.capture()` or `err.set()` internally.

### 1.15 Dead code — functions never called anywhere

Before touching any file, run a dead-code scan (e.g., `vulture` or IDE
"Find Usages"). Functions with zero call sites are deleted, not commented out.
Commented-out code is also deleted. If a function exists for documentation
purposes, it should be a docstring section in the module, not a dangling
function definition.

**Rule**: if a function has no callers inside or outside its module, delete it.
If it might be needed in the future, the git history is the right archive.

### 1.16 Near-duplicate functions — merge threshold 75 %

Two functions that share more than 75 % of their logic — same structure, same
DB calls, same error handling, differing only in a task type constant or a
message string — must be unified.

The unification pattern:
```python
# BAD — two functions with 80% identical bodies
def _finalize_discovery_error(db, err, *, task_id, host_id, ...):
    db.host_task_update(task_id=task_id, NU_STATUS=k.TASK_ERROR, ...)
    db.queue_host_task(host_id=host_id, task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE, ...)

def _finalize_backup_error(db, err, *, task_id, host_id, ...):
    db.host_task_update(task_id=task_id, NU_STATUS=k.TASK_ERROR, ...)
    db.queue_host_task(host_id=host_id, task_type=k.HOST_TASK_CHECK_CONNECTION_TYPE, ...)

# GOOD — one function, the differing part is a parameter
def _finalize_host_task_error(
    db: dbHandlerBKP,
    err: errors.ErrorHandler,
    *,
    task_id: int,
    host_id: int | None,
    task_type: int,          # k.FILE_TASK_DISCOVERY or k.FILE_TASK_BACKUP_TYPE
    ...,
) -> None:
    ...
```

This applies to `_persist_discovery_error` vs `_persist_backup_error`,
`_finalize_successful_backup` vs `finalize_task_resolution` (DB portion), and
any pair of `_finalize_*` helpers that differ only in their `task_type` constant.

### 1.17 SSH bootstrap opacity — callback-in-kwargs anti-pattern

`bootstrap_flow.init_host_context_with_retry` accepts a `transient_retry_handler`
callback and a `retry_handler_kwargs` dict. This is equivalent to passing a
closure through a stringly-typed dict — the IDE cannot infer what keys
`retry_handler_kwargs` expects, and the reader must navigate into
`bootstrap_flow.py` to understand what the callback contract is.

The return value is a bare tuple `(sftp, daemon, preserve_host_busy_cooldown)`,
where `None` for `sftp` signals failure — back to the sentinel-return anti-pattern
(see §1.11).

Refactor target:
```python
# BAD — opaque callback + sentinel return
sftp, daemon, preserve_busy = bootstrap_flow.init_host_context_with_retry(
    task=task,
    log=log,
    err=err,
    transient_retry_handler=_requeue_transient_bootstrap_failure,
    retry_handler_kwargs={"db": db, "host_id": host_id, "task_id": task_id},
    ...
)
if sftp is None:
    continue   # was it transient? fatal? the caller cannot tell

# GOOD — raises on fatal, calls handler and raises BootstrapRetry on transient
try:
    sftp, daemon = bootstrap_flow.init_host_context(
        task=task,
        logger=log,
        on_transient_failure=lambda exc: _requeue_transient_bootstrap_failure(
            db, host_id=host_id, task_id=task_id, exc=exc
        ),
    )
except bootstrap_flow.TransientBootstrapError:
    continue   # retry handler already ran; no state left in err
except Exception as e:
    err.capture(reason="SSH bootstrap failed", stage=k.STAGE_CONNECT, exc=e)
    continue
```

`bootstrap_flow.TransientBootstrapError` is a typed sentinel — the caller
distinguishes transient from fatal without checking `sftp is None`.

---

## 2. Worker File Structure

Every worker must follow this section order. The section headers below are
mandatory; the number of functions per section will vary by worker.

```
1. Shebang + module docstring
2. Standard library imports
3. Bootstrap path setup
4. Internal imports
5. Global state  (SERVICE_NAME, log, process_status)
6. Signal handling  (_shutdown_cleanup + install_shutdown_handlers)
7. Loop helpers   (_read_next_task, _claim_task)
8. Work function  (_do_work — the domain logic unique to this worker)
9. Finalization   (_finalize_success, _finalize_error)
10. main()
11. if __name__ == "__main__":
```

### Rules

- **Sections 7–9 may not import modules at call time.** All imports are at the
  top of the file.
- **Sections 7–9 must not contain nested functions.** A function that is only
  called from one other function is still a module-level function; it is not
  defined inside the caller.
- **Section 8 (`_do_work`) is the only section that may raise.** All exceptions
  from domain logic propagate upward to `main()` and are caught there.
- **Sections 7 and 9 must not raise.** They are infrastructure; they swallow
  their own exceptions and log them.
- **Module docstring**: three things — what the service does, what queue row it
  owns, one operational constraint. No more.

---

## 3. Standard Loop Structure

The canonical pattern for queue-driven workers is:

```python
def main() -> None:
    log.service_start(SERVICE_NAME)
    db = _init_db()

    while process_status["running"]:
        err = errors.ErrorHandler(log)
        task = None

        try:
            # --- Read ---
            task = _read_next_task(db)
            if task is None:
                runtime_sleep.random_jitter_sleep()
                continue

            # --- Claim ---
            if not _claim_task(db, task):
                runtime_sleep.random_jitter_sleep()
                continue

            # --- Work ---
            result = _do_work(db, task)

            # --- Finalize success ---
            _finalize_success(db, task, result)

        except Exception as e:
            if not err.triggered:
                err.capture(reason="...", stage="...", exc=e)
            _finalize_error(db, task, err)

        finally:
            _cleanup(task)             # release host lock, close SFTP, etc.

        runtime_sleep.random_jitter_sleep()

    log.service_stop(SERVICE_NAME)
```

### Key invariants

1. `continue` is only used for "no work" (empty queue) and "lost claim race".
   **Never** for error paths.
2. `finally` handles **only resource cleanup** — close connections, release locks.
   It never changes queue state.
3. Queue state changes (DONE, ERROR, FROZEN) only happen in `_finalize_success`
   and `_finalize_error`.
4. `_finalize_error` is always safe to call even when `task is None`. It must
   guard against that case internally.
5. The `skip_to_next_iteration` pattern is **forbidden**. If you feel the need
   for it, the problem is that `finally` is doing too much.

### Maintenance workers (no queue row)

`appCataloga_host_maintenance.py` does not consume a queue row. Its loop structure
is simpler and correct as-is. Do not apply the queue pattern to it.

---

## 4. Function Placement Rules

| What | Where |
|---|---|
| Task queue read, claim, finalize | Entrypoint (`appCataloga_*.py`) |
| Domain work (SSH, transfer, parse) | Dedicated handler module |
| Connectivity state machine | `host_handler/host_connectivity.py` |
| Backup file transfer and integrity check | `host_handler/backup_flow.py` (new) |
| Server-side filename contract | `host_handler/backup_flow.py` (move from entrypoint) |
| Discovery metadata drift detection | `host_handler/backup_flow.py` (move from entrypoint) |
| GC filesystem helpers | `gc_handler/gc_maintenance.py` (new module) |
| appAnalise filesystem artifact moves | `appAnalise/artifact_handler.py` (split from task_flow.py) |
| appAnalise DB task lifecycle | `appAnalise/task_flow.py` (keep, but slimmed) |
| Spectrum insert batch | `appAnalise/task_flow.py` (keep) |
| Shared error / log / tools | `shared/` (no change) |

### When a function belongs in the entrypoint vs a module

A function belongs **in the entrypoint** when it:
- Owns a queue state transition (PENDING → RUNNING, RUNNING → DONE, etc.)
- Is called only by `main()` and only once per loop iteration
- Writes or resolves `HOST_TASK`, `FILE_TASK`, or `FILE_TASK_HISTORY`
- Decides the operational outcome of a worker pass (`DONE`, `ERROR`, `FROZEN`,
  delete, promote)

A function belongs **in a module** when it:
- Contains domain logic that could be unit-tested independently
- Is longer than ~30 lines
- Would be useful in more than one entrypoint
- Deals with an external system (SSH, filesystem, socket, DB schema)
- Persists analytical data whose natural ownership is the domain flow itself
  (for example `RFDATA` writes during appAnalise processing)

### Boundary rule: BPDATA vs RFDATA

Use this rule during every refactor:

- Keep `BPDATA` operational queue lifecycle in the entrypoint.
- Do not move `FILE_TASK` / `FILE_TASK_HISTORY` finalization into domain handlers.
- It is acceptable for a domain handler to write `RFDATA` when that write is
  part of the business flow being executed.
- If moving code into a handler would force that handler to decide queue status,
  stop. That code still belongs to the entrypoint.

---

## 5. Error Handling Standard

The system uses a single error capture API: `err.capture(reason=..., stage=..., exc=...)`.
Every worker follows the same **unified error flow**:

```
main() loop iteration
  └─ helpers raise on failure
        └─ except block in main() calls err.capture() exactly once
              └─ _finalize_error() writes queue state + logs
```

This flow has three invariants:
1. **One capture site per iteration** — `err.capture()` is called at most once,
   in the outer `except` block of `main()`. No helper calls `err.capture()` or
   `err.set()` internally.
2. **One finalize path** — `_finalize_error()` is called exactly once per failed
   iteration, from the outer `except` block. It is never called from a nested
   `try/except` or from a guard like `if err.triggered`.
3. **Raise, never return-as-error** — helpers signal failure by raising. The
   `err.triggered` check and the `err.set()` + `return` pattern are forbidden.

When these invariants are violated, the error path splits:
```
# BROKEN — three paths, unclear which error wins
try:
    try:
        if not _claim(db, task):
            ...
    except Exception as e:
        err.set("claim failed", "LOCK_TASK", e)   # path A — err.set

    if not err.triggered:
        _do_work(db, task, err)                    # may call err.set inside

    if err.triggered:
        _finalize_error(db, task, err)             # path B — inner guard

except Exception as e:
    err.capture(...)                               # path C — outer except
    _finalize_error(db, task, err)                 # called again?
```

The unified pattern eliminates the ambiguity:
```python
# CORRECT — one capture site, one finalize path
try:
    task = _read_next_task(db)
    if task is None:
        runtime_sleep.random_jitter_sleep()
        continue
    if not _claim_task(db, task):   # raises on DB error; False = race lost
        runtime_sleep.random_jitter_sleep()
        continue
    result = _do_work(db, task)     # raises on any failure
    _finalize_success(db, task, result)
except Exception as e:
    if not err.triggered:
        err.capture(reason="...", stage=k.STAGE_MAIN, exc=e)
    _finalize_error(db, task, err)
finally:
    _cleanup(task)
```

Use `err.capture(reason=..., stage=..., exc=...)` everywhere. The `err.set()`
API used in `appCataloga_host_check.py` must be replaced.

```python
# Good
err.capture(reason="Transfer failed", stage="TRANSFER", exc=e,
            host_id=host_id, task_id=task_id)

# Bad — do not use
err.set("Transfer failed", "TRANSFER", e)
```

### Error flow audit — current state per file

| File | `err.set()` | `err.triggered` guards | `skip_to_next_iteration` | Verdict |
|---|---|---|---|---|
| `appCataloga_host_check.py` | ✅ 5 calls | ✅ 4 guards | ❌ | **BROKEN — 3-path split** |
| `appCataloga_file_bin_process_appAnalise.py` | ❌ | ✅ 2 guards | ✅ 4 uses | **BROKEN — flag + guards** |
| `appCataloga_file_bkp.py` | ❌ | ✅ 2 guards | ❌ | **BROKEN — split capture** |
| `appCataloga_discovery.py` | ❌ | ✅ 2 guards | ❌ | **BROKEN — split capture** |
| `appCataloga_backlog_management.py` | ❌ | ❌ | ❌ | **OK** |
| `appCataloga_host_maintenance.py` | ❌ | ❌ | ❌ | **OK** |
| `appCataloga_garbage_collector.py` | ❌ | ❌ | ❌ | **OK** |
| `appCataloga_summary_database.py` | ❌ | ❌ | ❌ | **OK** |

`appAnalise/task_flow.py` uses `err.triggered` for read-only inspection (deciding
what to log/pass), not for flow control — that is acceptable.

---

## 6. Section Labels

Use `# ---` separator style for sub-phases within `main()`, not `# ACT I`,
`# Phase 1`, or `# ===`:

```python
    while process_status["running"]:
        err = errors.ErrorHandler(log)
        task = None

        try:
            # --- read ---
            ...
            # --- claim ---
            ...
            # --- work ---
            ...
            # --- finalize ---
            ...
```

For module-level section breaks between groups of functions use `# ---` on its own
line followed by the section name as a comment:

```python
# --- loop helpers ---

def _read_next_task(...):
    ...

def _claim_task(...):
    ...


# --- work ---

def _do_work(...):
    ...
```

---

## 7. Per-Worker Change Inventory

The changes below are listed in priority order. Items marked **[BLOCKER]** address
correctness issues; the rest are readability improvements.

---

### 7.1 `appCataloga_file_bin_process_appAnalise.py`

**Problems:**
- `skip_to_next_iteration` flag — the loop is very hard to follow.
- `_log_preflight_outage`, `_log_preflight_recovery_if_needed`, `_reset_preflight_log_state`,
  `_APP_ANALISE_PREFLIGHT_LOG_STATE`, `preflight_app_analise_connection` — 80+ lines
  of throttled-warning logic lives in the entrypoint. Should be a small class or
  function group in `appAnalise/appAnalise_connection.py`, which already owns the
  connection lifecycle.
- The `_timed_phase` context manager is a one-off used only in this file;
  its output `phase_durations` dict is built only to feed `_log_processing_phase_timings`.
  Both can be inlined or collapsed.
- `_format_structured_event` and `_log_warning_event` are workarounds for test
  stubs that don't implement the full log API. Fix the test doubles instead of
  carrying shim code in production.

**Changes:**
1. **[BLOCKER]** Replace `skip_to_next_iteration` with the standard loop structure.
   The `finally` block should only release `db_rfm` transaction and do nothing else.
   Queue state goes in `_finalize_success` / `_finalize_error`.
2. Move the preflight outage throttle state and helpers to
   `appAnalise/appAnalise_connection.py` as a `_OutageTracker` inner class or
   standalone helper group.
3. Remove `_format_structured_event` and `_log_warning_event` — fix test doubles.
4. Collapse `_timed_phase` + `_log_processing_phase_timings` into a single helper
   or fold them into `_do_work`.

---

### 7.2 `appCataloga_host_check.py`

**Problems:**
- `check_host_connectivity` is an 8-line wrapper around two calls with no
  conditional logic. It exists only to capture the module-level `log` global.
  That is not a reason for a function.
- `handle_degraded_connectivity_task` is missing the `_` prefix — it is a
  private module-level helper but is named as if it were a public API.
- `handle_degraded_connectivity_task`, `_handle_auth_error_connectivity_task`,
  `_finalize_connectivity_task` are three domain functions with meaningful
  logic that belong in `host_handler/host_connectivity.py`. They take `db` as
  their first argument and do not depend on the module-level `log` global —
  `_handle_auth_error_connectivity_task` uses `log.event(...)` directly, which
  is the only thing preventing it from moving. Fix: make each accept a `logger`
  argument.
- `_process_connectivity_task` is 50 lines of exception/return-early control
  flow that sequences three domain calls. After the domain functions move to
  `host_connectivity`, this wrapper collapses to ~10 lines that belong directly
  in `main()`.
- `_dispatch_claimed_task` is a pure dispatcher (see §1.10). Inline in `main()`.
- `_process_statistics_task` is 15 lines. Can stay in the entrypoint.
- **Error flow is split across three paths** (see §5). The loop has a nested
  `try/except` for claim with `err.set()`, followed by `if err.triggered` guards,
  followed by an outer `except` with `err.capture()`. `_persist_task_error` is
  called in two different places. This is the anti-pattern described in §5 —
  it is unclear which error wins when multiple branches fire. Fix: restructure
  `main()` to the canonical single-capture pattern.

**Changes:**
1. Add `logger` parameter to `handle_degraded_connectivity_task`,
   `_handle_auth_error_connectivity_task`, and `_finalize_connectivity_task`.
   Move them to `host_handler/host_connectivity.py`.
2. Rename `handle_degraded_connectivity_task` → `_handle_degraded_connectivity_task`
   (add missing `_` prefix) before or during the move.
3. Delete `check_host_connectivity`. Call `probe_host_connectivity` +
   `log_connectivity_probe` directly in `main()`.
4. Delete `_process_connectivity_task`. Its logic reduces to ~10 lines in `main()`:
   probe → match state → call the right domain function.
5. Inline `_dispatch_claimed_task` into `main()`.
6. Restructure `main()` to the canonical single-capture loop (§5): remove nested
   `try/except` for claim, remove `if err.triggered` guards, remove duplicate
   `_persist_task_error` call sites. Helpers raise; `main()` catches once.
7. Replace all `err.set(...)` with `err.capture(...)`.

---

---

### 7.3 `appCataloga_file_bkp.py`

**Problems:**
- `build_server_filename` is labeled "ARCHITECTURAL CONTRACT" in the entrypoint.
  A contract that other code must follow cannot live only in the entrypoint.
- `transfer_file_task` is 150 lines in the entrypoint. It owns the complete
  transfer + integrity-check flow and has no dependency on `process_status` or
  `log` as globals — it takes everything it needs as arguments.
- `_has_discovery_metadata_drift` is pure domain logic with no side effects.
- `_finalize_successful_backup` writes `file_history_update` + `file_task_update`
  as two separate auto-commit calls. **[BLOCKER]** This is non-atomic.
- `parse_arguments` parses `sys.argv` manually at module level; this pattern makes
  the worker hard to test and hides the worker-ID initialization.
- **Error flow is split** (see §5): the main loop contains `if not err.triggered`
  guards and `if err.triggered` guards mid-loop. `_finalize_backup_error` is
  called both from the `if err.triggered` guard and from the outer `except`
  block, creating two possible finalize paths per iteration.

**Changes:**
1. **[BLOCKER]** Wrap `file_history_update` + `file_task_update` inside a
   `db.begin_transaction()` / `db.commit()` / `db.rollback()` block in
   `_finalize_successful_backup`, same as `freeze_task_for_manual_review`.
2. Create `host_handler/backup_flow.py` with:
   - `build_server_filename(host_uid, remote_path, filename) -> str`
   - `transfer_file(sftp, task, local_path, server_filename, config) -> tuple[float, FileMetadata]`
   - `has_metadata_drift(task, remote_metadata) -> bool`
3. Move `parse_arguments` logic into `main()` directly (two lines).
4. Restructure `main()` to the canonical single-capture loop (§5): remove
   `if err.triggered` mid-loop guards, ensure `_finalize_backup_error` is called
   from exactly one place (the outer `except` block).

---

### 7.4 `appAnalise/task_flow.py`

**Core problem — problem displacement, not simplification:**
`task_flow.py` was created to reduce the line count of the entrypoint. But the
module now contains four different vocabularies with no internal boundary:
filesystem moves, DB queue state, geocoding/SITE resolution, and spectrum batch
inserts. Every function in it requires a different mental context to read. The
result is that the complexity was not reduced — it was relocated.

The specific problems are:

- `finalize_task_resolution` is 230 lines with 13 keyword-only parameters.
  The first half (lines 1–60) moves files. The second half (lines 60–230) writes
  to the DB. They should be two functions.
- `resolve_history_file_metadata` and `build_history_metadata_from_file_meta` are
  wrapper functions building plain dicts — no conditional logic, no error handling.
  Both should be inlined at their only call sites.
- `finalize_successful_processing` and `move_file_if_present` belong in a
  filesystem/artifact module, not the task queue module.
- `build_resolved_files_trash_path` is duplicated in both this module and
  `appCataloga_garbage_collector.py`.
- `upsert_site` sounds like a DB handler method but lives here. The name creates
  constant confusion about where to look for it (see §1.14).
- `resolve_spectrum_sites` uses `_FIXED_SITE_UPDATE_AGGREGATOR` as a module-level
  side-channel to pass state into `upsert_site` (see §1.12). The fix is to pass
  the accumulator as an explicit parameter.
- All `db_rfm` and `db_bp` parameters are untyped, making IDE navigation and
  static analysis impossible for any function in this module (see rule 8 in the
  mandate). `db_rfm.get_site_id()` is invisible to the type checker and to VS Code's
  go-to-definition because `db_rfm` has no declared type.

**Changes:**
1. Split `finalize_task_resolution` into two sequential steps called from the
   entrypoint:
   - `quarantine_error_artifacts(file_meta, source_file_meta, export, logger)
     -> dict` — filesystem only; moves files and returns `{"server_path", "history_meta"}`.
     Returns immediately (no side effects) on the success path.
   - `commit_task_resolution(db_bp, *, file_task_id, host_id, ..., artifact)
     -> dict` — DB only; opens transaction, writes history, deletes FILE_TASK,
     commits. `artifact` is the dict returned by step 1.
   The entrypoint calls them in sequence. The filesystem step happens before the
   DB step so a filesystem failure does not leave an open transaction, and a DB
   failure does not leave files in limbo.

2. Inline `resolve_history_file_metadata` and `build_history_metadata_from_file_meta`
   — both are simple dict constructions with no logic worth naming.

3. Create `appAnalise/artifact_handler.py` with:
   - `move_file_if_present(...)`
   - `finalize_successful_processing(...)`
   - `build_resolved_files_trash_path()`
   - `is_same_file()`
   - `is_transient_filesystem_error()`
   This module owns "moving files around". `task_flow.py` owns "DB queue state".

4. Keep in `appAnalise/task_flow.py`:
   - `commit_task_resolution` (new, DB-only replacement for part of finalize_task_resolution)
   - `quarantine_error_artifacts` (new, FS-only replacement for part of finalize_task_resolution)
   - `freeze_task_for_manual_review`
   - `resolve_spectrum_sites`
   - `insert_spectra_batch`
   - `should_export`

5. Replace `gc_handler`'s `build_resolved_files_trash_path` with the one from
   `appAnalise/artifact_handler.py` (single source of truth).

---

---

### 7.5 `appCataloga_garbage_collector.py`

**Problems:**
- Eight helper functions before `main()`.
- `build_resolved_files_trash_path` duplicates the same function in `appAnalise/task_flow.py`.
- The `db_bp.commit()` call after the delete loop is an unexplained bare commit
  that suggests autocommit may have been accidentally disabled somewhere.

**Changes:**
1. Create `gc_handler/gc_maintenance.py` with the eight helper functions.
2. Replace `build_resolved_files_trash_path` with the canonical one from
   `appAnalise/artifact_handler.py` after that split is done (see 7.4).
3. Investigate and remove the bare `db_bp.commit()` at the end of the GC loop.
   If it is needed, add a comment explaining why.

---

### 7.6 `appCataloga_discovery.py`

**Problems:**
- Uses `# ACT I`...`# ACT V` inside the `try` block and `# Phase 1`...`# Phase 5`
  inside `finally`. Two labelling systems in the same function. Unify to `# ---`.
- `_requeue_transient_bootstrap_failure` name implies the retry decision lives here.
  Rename to `_requeue_task_as_pending` for clarity.
- **Error flow is split** (see §5): `err.triggered` is checked as a mid-loop guard
  at two points (line 586 and 624). The first guard decides whether to persist the
  task error; the second decides whether to queue a statistics refresh. Both checks
  belong in the outer `except` block or in `_finalize_error`, not inline.

**Changes:**
1. Rename `# ACT` / `# Phase` labels to `# ---` everywhere in `main()`.
2. Rename `_requeue_transient_bootstrap_failure` → `_requeue_task_as_pending`.
3. Move `err.triggered` guards into the canonical except block: the statistics
   refresh enqueue (currently guarded by `if not err.triggered and processed > 0`)
   should become part of `_finalize_success`, and the error persist (currently
   guarded by `if err.triggered and task_id`) should be the only path through
   `_finalize_error`.

---

### 7.7 `appCataloga_backlog_management.py`

**No structural problems.** This is the closest to the reference pattern.
Error flow is clean: single `except` block, no `err.triggered` guards.

Minor: labels inside `main()` use `# Phase 1`...`# Phase 3`. Change to `# ---`.

---

### 7.8 `appCataloga_host_maintenance.py`

**No problems.** Clean delegation to `maintenance_flow`. No changes needed.

---

### 7.9 `appCataloga_garbage_collector.py` — `SERVICE_NAME` missing

`log.service_start("appCataloga_garbage_collector")` is called with a string
literal instead of the `SERVICE_NAME` constant. Minor but inconsistent.

---

### 7.10 `appCataloga_pub_metadata.py`

**Problems (minor):**
- `wait_random_time` re-implements `runtime_sleep.random_jitter_sleep`. Remove and
  use the shared helper.
- `ensure_parent_directory` is a two-line wrapper around `os.makedirs`. Inline it.

---

### 7.11 `appCataloga_summary_database.py`

**No structural problems.** It correctly delegates all logic to `SummaryRefreshEngine`.

---

## 8. Log Standards

### 8.1 The operator problem

The current logs are dense walls of text. During an incident the operator scans
for one line in hundreds — task resolved? Error? Which error? This means the log
format is failing its primary purpose.

The target: any operator should be able to answer these four questions in under
10 seconds from a live log tail:
1. Is a task being processed right now?
2. Did the last task succeed or fail?
3. If it failed, what was the error and at which stage?
4. How long did it take?

### 8.2 Mandatory log events per task lifecycle

Every queue-driven worker must emit exactly these named events per iteration,
using `log.event(...)` for normal flow and `log.error_event(...)` for failures.
No other format (bare `log.warning("...")` with embedded key=value strings) is
acceptable for structured operational data.

| Phase | Event name | Required fields |
|---|---|---|
| Task claimed | `task_claimed` | `task_id`, `host_id` (or equivalent), `task_type` |
| Work started | `work_started` | `task_id`, `host_id`, `detail` (file/host) |
| Work completed | `work_completed` | `task_id`, `host_id`, `detail`, `elapsed_sec` |
| Task finalized OK | `task_done` | `task_id`, `host_id`, `status=DONE` |
| Task finalized ERROR | `task_error` | `task_id`, `host_id`, `stage`, `reason`, `error` |
| Task finalized FROZEN | `task_frozen` | `task_id`, `host_id`, `detail` |

`elapsed_sec` is required on `work_completed`. Every work function must
time itself and return the elapsed time (see §1.13).

### 8.3 One event name per outcome — no ambiguity

```python
# BAD — three different ways to log "processing done" in appAnalise worker
log.event("processing_completed", file=filename, ...)
log.event("finalize_task", task_id=task_id, ...)
log.warning(f"event=task_error host_id={host_id} error={err}")

# GOOD — exactly one canonical event per outcome
log.event("task_done", task_id=task_id, host_id=host_id,
          file=filename, elapsed_sec=elapsed)
log.error_event("task_error", task_id=task_id, host_id=host_id,
                stage=err.stage, reason=err.reason, error=err.format_error())
```

### 8.4 Timestamp consistency

All timestamps in log events must use `datetime.now(timezone.utc).isoformat()`.
Never use `datetime.now()` (no timezone) in log output — it produces local time
without a zone marker, which is unreadable in cross-server log aggregation.
The DB columns (`DT_FILE_TASK`, `DT_PROCESSED`) continue to use `datetime.now()`
as today — that is a separate concern scoped to the DB layer.

### 8.5 `appCataloga_file_bin_process_appAnalise.py` log requirements

This worker is the hardest to read in production. It must emit:

- `task_claimed` with `file=filename`, `host=hostname`, `export=True/False`
- `work_started` with `file=filename`, `phase="parse"` / `phase="persist"`
- `work_completed` with `file=filename`, `spectra=N`, `elapsed_sec=X.XX`
- `task_done` with `file=filename`, `new_path=path`
- `task_error` with `file=filename`, `stage=k.STAGE_*`, `reason=...`, `error=...`

A successful processing run must be identifiable by grepping a single event name:
`grep task_done appCataloga_file_bin_process_appAnalise.log`

A failed run must be identifiable by:
`grep task_error appCataloga_file_bin_process_appAnalise.log`

---

## 9. Standard Worker Function Signatures

All queue-driven workers must use these exact function names and signature
shapes. The body will differ per worker; the interface must not.

```python
def _read_next_task(db: dbHandlerBKP) -> dict | None:
    """
    Fetch the next claimable queue row. Return None when the queue is empty.
    Must not raise — return None on any DB error and log a warning.
    """

def _claim_task(db: dbHandlerBKP, task: dict) -> bool:
    """
    Atomically mark the task RUNNING. Return False when the race is lost.
    Must not raise — return False on any DB error and log a warning.
    """

def _do_work(db: dbHandlerBKP, task: dict) -> dict:
    """
    Execute the domain work for one task. Return a result dict.
    MUST raise on any failure — never returns a sentinel.
    The result dict always contains at least {"elapsed_sec": float}.
    Workers that use two DB connections (e.g., db_bp + db_rfm) add them
    as parameters: _do_work(db_bp: dbHandlerBKP, db_rfm: dbHandlerRFM, task: dict)
    """

def _finalize_success(db: dbHandlerBKP, task: dict, result: dict) -> None:
    """
    Write DONE status to the queue and emit task_done log event.
    Must not raise.
    """

def _finalize_error(db: dbHandlerBKP, task: dict | None, err: errors.ErrorHandler) -> None:
    """
    Write ERROR/FROZEN status to the queue and emit task_error log event.
    Safe to call when task is None (guard internally).
    Must not raise.
    """

def _cleanup(task: dict | None, *, sftp=None, ...) -> None:
    """
    Release all acquired resources: SFTP connection, host BUSY lock, DB cursors.
    Called unconditionally from finally. Must not raise.
    """
```

### Signature rules

- `db` is always the first positional parameter, typed with the concrete class.
- `task` is always the second positional parameter when present, typed `dict`.
- `err` is always keyword-only: `*, err: errors.ErrorHandler`.
- `logger` is always the last keyword-only parameter: `*, logger=None`.
  When the function is only called from within the same file it receives the
  module-level `log` directly — no `logger` parameter needed.
- Workers that need a secondary DB connection (`db_rfm`) add it as the
  second positional parameter after `db_bp`.

---

## 10. Execution Order

The items above should be executed in this order to minimize merge conflicts
and allow incremental testing:

1. **[BLOCKER] `_finalize_successful_backup` atomicity** (`appCataloga_file_bkp.py`)
   — Already know the pattern from `freeze_task_for_manual_review` fix.

2. **`appAnalise/task_flow.py` split** (`artifact_handler.py` extraction)
   — This is the riskiest change. Do it with tests green before and after.
   No worker changes yet; just move the functions.

3. **`gc_handler/gc_maintenance.py` extraction** from `appCataloga_garbage_collector.py`
   — Low risk; these functions are pure helpers.

4. **`host_handler/backup_flow.py` extraction** from `appCataloga_file_bkp.py`
   — Move functions first, update imports in entrypoint, then restructure `main()`.

5. **`appCataloga_host_check.py` connectivity helpers → `host_handler/host_connectivity.py`**
   — Requires verifying that the moved functions don't rely on the module-level `log` global.
   Each function must accept a `logger` argument instead.

6. **Loop restructure: `appCataloga_file_bin_process_appAnalise.py`**
   — Remove `skip_to_next_iteration`, move preflight state to `appAnalise_connection.py`.
   Remove `if err.triggered` guards; helpers raise; `main()` catches once.

7. **Loop restructure: `appCataloga_file_bkp.py`**
   — Remove `if err.triggered` / `if not err.triggered` mid-loop guards.
   Consolidate `_finalize_backup_error` to a single call site in the outer
   `except` block (see §7.3 change 4). Do after item 4 (backup_flow extraction)
   because the extraction is a prerequisite for the restructure to be clean.

8. **Loop restructure: `appCataloga_discovery.py`**
   — Move `err.triggered` guards out of the `finally` block into the canonical
   `except` path: success enqueue → `_finalize_success`, error persist →
   `_finalize_error` (see §7.6 change 3). Also rename
   `_requeue_transient_bootstrap_failure` → `_requeue_task_as_pending`.

9. **Label normalization** across all workers — mechanical change, low risk.

10. **`err.set()` → `err.capture()` + loop restructure** in `appCataloga_host_check.py`
    — fix split error flow to canonical single-capture pattern (see §5 and §7.2).
    This item covers: removing the nested `try/except` for claim, removing
    `if err.triggered` guards, consolidating `_persist_task_error` to one call
    site, and replacing all `err.set()` calls.

11. **Dead code scan** across all workers and modules — delete unreachable functions.

12. **Near-duplicate merge** — unify `_persist_discovery_error` / `_persist_backup_error`
    and any other pair with >75 % similarity.

13. **`STAGE_*` constants** — add to `config.py`, replace all inline stage strings
    across all workers at once.

14. **Log standardization** — apply §8 event names and fields to all workers.
    Fix timestamp to UTC ISO format in all log output.

15. **SSH bootstrap refactor** — replace `transient_retry_handler` callback-in-kwargs
    pattern with `TransientBootstrapError` (see §1.17).

16. **Standard function signatures** — rename and align `_read_next_task`,
    `_claim_task`, `_do_work`, `_finalize_success`, `_finalize_error`, `_cleanup`
    across all workers (see §9).

17. **Minor cleanups** (`appCataloga_pub_metadata.py`, `SERVICE_NAME` literal, etc.)

18. **[BACKLOG — package renames] Update `tool_start_all.sh`, `tool_stop_all.sh`,
    `tool_status_all.sh`** — when `server_handler/` is renamed to `runtime/` (or
    any other worker/package rename occurs), these three orchestration scripts must
    be updated in the same commit. They reference workers by filename; a rename
    without updating them silently breaks start/stop/status for that worker.

19. **[BACKLOG — post-refactor] Developer expansion guide** — once the refactor is
    complete and the codebase conforms to `ARCHITECTURE.md`, produce a concise
    `CONTRIBUTING.md` in this directory covering:
    - How to add a new worker end-to-end (checklist from `ARCHITECTURE.md §11`
      expanded with concrete examples from the refactored workers).
    - How to add a new DB handler method (checklist from `ARCHITECTURE.md §12`
      with a worked example).
    - How to add a new domain handler module (naming convention from §5.1,
      layer rules from §2, test file placement from §13).
    - How to add a new `HOST_TASK` or `FILE_TASK` type: constant in `config.py`,
      `match/case` branch in the owning worker, DB method if new columns are
      needed.
    - How to add a new config constant: which prefix to use, where to document it.
    - Common pitfalls: layer violations, missing `begin_transaction`, inline stage
      strings, missing mandatory log events.
    This guide is intentionally deferred until the codebase matches the target
    architecture — writing it against the current code would document the wrong
    patterns.

---

## 11. What Must Not Change

- `process_status = {"running": True}` pattern — keep in every worker.
- `signal_runtime.install_shutdown_handlers(...)` — keep at module level, not inside `main()`.
- `runtime_sleep.random_jitter_sleep()` at the end of every loop iteration — keep.
- `log.service_start` / `log.service_stop` framing in `main()` — keep.
- `errors.ErrorHandler` re-created at the top of every loop iteration — keep.
- `err.capture(reason=..., stage=..., exc=...)` as the only error capture API.
- `db.begin_transaction()` / `db.commit()` / `db.rollback()` for any write that
  spans `file_task_update` + `file_history_update` or `file_task_update` + `file_task_delete`.

---

## 12. RFFUSION_SUMMARY Refactor

> **Status: planned — no code changed yet.**
> Full specification is in `ARCHITECTURE.md §15`.

### 12.1 Files to change

| File | Change type |
|---|---|
| `db/dbHandlerSummary.py` | Bug fix: `acquire_worker_lock` / `release_worker_lock` |
| `appCataloga_summary_database.py` | Full rewrite (target ≤180 lines) |

### 12.2 Bug inventory

1. **`reset_after_reconcile` never called** — it is on the same line as a
   comment and never executes. After every nightly reconcile, `SUMMARY_OUTBOX`
   is never purged and the checkpoint is never reset to 0.

2. **Singleton lock does not work** — `acquire_worker_lock` and
   `release_worker_lock` in `dbHandlerSummary` call `_disconnect()` immediately
   after the SQL, which releases the MariaDB user-lock (lock is connection-scoped).
   The worker bypasses these broken methods and uses private `_select_raw` /
   `_connect` / `_disconnect` directly — a layer violation.

3. **`NA_STATUS` stuck at `RUNNING`** — `mark_worker_start` is called before
   the reconcile but the matching success call is inside `reset_after_reconcile`
   which never executes (bug 1). Monitoring sees the worker as perpetually
   in-progress.

### 12.3 Refactor rules

- Do not alter the reconcile/incremental business logic or the 02:00 BRT schedule.
- Do not alter `SummaryRefreshEngine` or `dbHandlerSummary` SQL queries.
- Only fix the three bugs above and reduce the file to ≤180 lines.
- `main()` must delegate to named helpers: `_run_full_reconcile(db, engine, now)`
  and `_run_incremental_batch(db, engine)`.
- Remove `_acquire_db_lock` / `_release_db_lock` from the worker; use the fixed
  handler methods instead.
- Fix `dbHandlerSummary.acquire_worker_lock` to hold the connection open on
  success; fix `release_worker_lock` to release then disconnect in `finally`.
- Both files must be committed together (they have a hard dependency).
