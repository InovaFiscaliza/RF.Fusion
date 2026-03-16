# appCataloga Script Style

This guide defines the preferred style for the `appCataloga_xxx.py` entrypoints.
The goal is not to force every script into the same logic, but to make them feel
like they belong to the same system when reading, debugging, and maintaining them.

## Scope

Applies to:

- `appCataloga.py`
- `appCataloga_discovery.py`
- `appCataloga_file_bkp.py`
- `appCataloga_file_bin_proces.py`
- `appCataloga_file_bin_proces_appAnalise.py`
- `appCataloga_host_check.py`
- `appCataloga_garbage_collector.py`
- `appCataloga_pub_metadata.py`

## Core Principles

- Prefer explicit, linear flow over clever abstractions.
- Keep the main loop readable: helpers should remove noise, not hide business rules.
- Centralize error capture through `ErrorHandler`.
- Centralize log formatting through `logging_utils`.
- Add comments only for decisions, invariants, recovery rules, and protocol quirks.
- Keep lifecycle timestamps explicit at call sites; avoid hidden `DT_*` updates in shared helpers.

## Preferred File Layout

Use this order whenever practical:

1. Shebang and module docstring
2. Standard-library imports
3. Project path/bootstrap setup
4. Third-party imports
5. Internal imports
6. Global state
7. Signal handling
8. Small helpers
9. Main loop helpers
10. `main()`
11. `if __name__ == "__main__":`

## Module Docstring

Each script should begin with a short docstring describing:

- what the service does
- the main data it owns or moves
- one or two operational design constraints

Good examples:

- synchronous TCP entrypoint
- one HOST per worker
- FILE_TASK is transient, FILE_TASK_HISTORY is authoritative

## Imports

- Group imports by standard library, third-party, and internal modules.
- Avoid `import sys,os` on the same line.
- Prefer one import per line unless there is a strong reason not to.
- Keep bootstrap path setup visually identical across scripts when possible.

## Globals

Prefer:

```python
log = logging_utils.log()
process_status = {"running": True}
```

Use extra fields only when they represent durable worker state, for example:

- `worker`
- `idle_cycles`
- `seed_recovery_last_attempt`

## Signal Handling

Preferred conventions:

- keep handlers minimal
- record shutdown intent
- log the signal using the shared logger pattern
- release BUSY resources if the worker owns them

When possible, prefer a shared helper such as `_signal_handler(...)` to avoid
duplicating logic between `SIGTERM` and `SIGINT`.

## Logging

Use the centralized logger and structured events:

```python
log.event("service_start", service="appCataloga_discovery")
log.warning_event("host_busy_retry", host_id=host_id)
log.error_event("db_init_failed", service="appCataloga_file_bkp", error=exc)
```

Guidelines:

- use `event=...` semantics consistently
- prefer `key=value` context over prose-heavy messages
- include IDs that matter in production debugging:
  - `worker_id`
  - `host_id`
  - `task_id`
  - `history_id`
  - `pid`
- reserve free-text details for `detail=...` or formatted error messages

## Error Handling

Prefer `ErrorHandler` as the source of truth for failures:

```python
err = errors.ErrorHandler(log)

try:
    ...
except Exception as exc:
    err.capture(
        reason="Unexpected worker failure",
        stage="MAIN",
        exc=exc,
        worker_id=worker_id,
    )
    err.log_error(worker_id=worker_id)
```

Guidelines:

- use `err.set(...)` for expected local failures inside a larger workflow
- use `err.capture(...)` in broad `except` blocks
- use `err.log_error(...)` once per failure path when possible
- let `finally` handle cleanup and persistent state transitions

## Comments

Comments should explain:

- why a lock is needed
- why a retry is safe
- why a failure is transient or definitive
- why a file is moved to trash or preserved
- why a helper exists for a protocol or naming invariant

Avoid comments that restate obvious code.

## Docstrings

Docstrings are expected for:

- `main()`
- signal/resource cleanup helpers
- protocol and filesystem helpers
- worker lifecycle helpers

Docstrings should stay short and operational.

## Main Loop Style

Inside long-running workers:

- initialize per-iteration state near the top of the loop
- keep stages visually separated
- prefer one `try/except/finally` per cycle
- avoid `continue` in deep nested paths when cleanup must still run
- use small helpers to keep the loop readable

If the script naturally follows stages, label them consistently:

- `ACT I`
- `ACT II`
- `ACT III`
- `FINALLY`

## Naming

Prefer names that reflect the domain:

- `host_id`, `task_id`, `file_task_id`
- `server_file_path`, `server_filename`
- `file_was_processed`, `file_was_transferred`
- `connect_busy`, `idle_cycles`

Avoid generic names like:

- `res`
- `obj`
- `tmp2`

except in very small, obvious scopes.

## Shutdown and Recovery

Production workers should be explicit about:

- how BUSY locks are released
- what happens on transient failures
- what happens on definitive failures
- what state must survive process death
- how detached sibling workers are stopped, if a worker pool exists

If a worker pool exists, recovery rules should be documented near the spawning logic.

## Compatibility Rule

This style guide is directional, not destructive. Existing logic that is already
stable in production should be improved incrementally:

- first readability
- then comments/docstrings
- then helper extraction
- only then structural refactors

Stability beats elegance when there is a tradeoff.
