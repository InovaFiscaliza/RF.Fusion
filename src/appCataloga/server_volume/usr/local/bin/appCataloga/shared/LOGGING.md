# Logging Conventions

This project uses structured log messages in the form:

`event=<name> key=value key=value`

The shared logger already prepends:

`timestamp | level=<...> | logger=<...> | pid=<...> | script=<...> |`

## Event naming

Use short, stable, snake_case event names.

Examples:

- `service_start`
- `service_stop`
- `signal_received`
- `db_init_failed`
- `processing_completed`
- `processing_failed`
- `host_busy_retry`

## Field naming

Prefer concise operational fields:

- `service`
- `worker_id`
- `host_id`
- `task_id`
- `file`
- `path`
- `error`
- `port`
- `online`

## Style rules

- Put the main action in `event=...`
- Keep fields flat; avoid nested structures in log messages
- Prefer machine-friendly values over prose when possible
- Use `error=...` for exception text
- Use `detail=...` only when a short free-form explanation is really needed

## Recommended helpers

Prefer the shared helpers from `logging_utils.log` when suitable:

- `log.event(...)`
- `log.warning_event(...)`
- `log.error_event(...)`
- `log.service_start(...)`
- `log.service_stop(...)`
- `log.signal_received(...)`

These helpers reduce formatting drift across scripts.
