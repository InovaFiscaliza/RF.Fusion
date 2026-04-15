# Logging Conventions

`appCataloga` uses the structured logger implemented in:

- [/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/logging_utils.py](/RFFusion/src/appCataloga/server_volume/usr/local/bin/appCataloga/shared/logging_utils.py)

The logger is intentionally lightweight and does not rely on Python's heavier
`logging.config` machinery. Its job is to keep one deterministic log format
across long-running daemons and small utility scripts.

## Log Line Format

Structured entries are written as:

```text
YYYY-MM-DD HH:MM:SS | level=INFO  | logger=appCataloga_discovery | pid=12345 | script=appCataloga_discovery.py | event=service_start service=appCataloga_discovery
```

The prefix is added automatically:

- timestamp
- `level`
- `logger`
- `pid`
- `script`

The message body should then usually be:

```text
event=<name> key=value key=value
```

## Current File Resolution

The shared logger resolves one log file per entrypoint.

With the current runtime configuration in
[/RFFusion/src/appCataloga/server_volume/etc/appCataloga/config.py](/RFFusion/src/appCataloga/server_volume/etc/appCataloga/config.py),
that means:

- `LOG_DIR = "/var/log"`
- `LOG_FILE_TEMPLATE = "{logger_name}.log"`

So a script such as `appCataloga_discovery.py` naturally writes to:

- `/var/log/appCataloga_discovery.log`

The logger also supports size-based rotation:

- current limit: `100 MB` per file
- current retention: `5` rotated generations
- rotated files become `.1`, `.2`, ... and use a `.lock` file for process-safe
  rotation

## Important Operational Nuance

Several wrapper shell scripts also redirect `stdout` / `stderr` with `nohup`.

That means production deployments can have two related log channels:

- the structured Python log under `/var/log/{logger_name}.log`
- the wrapper redirect target, often under `/var/log/appCataloga/...`

In practice this means:

- normal structured events usually appear in the Python log
- an uncaught traceback may show up in the wrapper redirect log instead

When debugging a crash, check both if the first file looks incomplete.

## Recommended API

Prefer the shared helpers from `logging_utils.log`:

- `log.event(...)`
- `log.warning_event(...)`
- `log.error_event(...)`
- `log.service_start(...)`
- `log.service_stop(...)`
- `log.signal_received(...)`

Use the plain text variants only when a structured event would add little
value:

- `log.entry(...)`
- `log.warning(...)`
- `log.error(...)`

## Event Naming

Use short, stable, `snake_case` event names.

Common examples:

- `service_start`
- `service_stop`
- `signal_received`
- `db_init_failed`
- `processing_completed`
- `processing_error`
- `task_finalization_failed`
- `appanalise_unavailable_retry`

## Field Naming

Prefer concise operational fields such as:

- `service`
- `worker_id`
- `host_id`
- `task_id`
- `file`
- `path`
- `final_file`
- `error`
- `error_type`
- `online`

Field keys are normalized automatically to filesystem/log-friendly names.

## Value Encoding Rules

`logging_utils.log.format_event(...)` applies a few normalizations:

- `None` fields are omitted entirely
- booleans become `true` / `false`
- lists, tuples, and sets become `[a,b,c]`
- other values are stringified directly

Keep fields flat and machine-friendly. Avoid nested JSON blobs unless the
payload itself is the point of the log line.

## Style Rules

- Put the main action in `event=...`
- Keep one durable meaning per event name
- Prefer stable keys over free-form prose
- Use `error=...` for exception text
- Use `detail=...` only when a short human explanation is genuinely useful
- Prefer adding a field to changing the event wording every time

## Small Examples

Good:

```text
event=processing_completed file=a.bin export=false final_file=/mnt/reposfi/2026/...
```

Good:

```text
event=task_finalization_failed host_id=18 task_id=442 error_type=OSError exception=OSError(16, 'Device or resource busy')
```

Avoid:

```text
something bad happened while trying to process the file and now we will retry later
```
