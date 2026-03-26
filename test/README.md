# Test Suite

This directory contains the active RF.Fusion validation suite.

Its purpose is not to prove every line of code mathematically. Its purpose is
to protect the contracts that are easiest to break while the runtime keeps
evolving.

The old `/RFFusion/test` material was archived and is not treated as a
trustworthy base for the current system.

This active suite is intentionally focused on the runtime that still ships.
One-shot migration or reconciliation utilities should not keep permanent test
surface here once they leave the product.

## What This Suite Is Optimized For

The current suite is strongest when validating:

- stable helper contracts
- protocol parsing and payload normalization
- worker decision rules
- queue and history semantics
- selected database-handler behavior
- targeted `webfusion` service rules

It is intentionally biased toward observable effects and workflow contracts,
not toward mock-heavy line coverage for its own sake.

## Structure

- `tests/shared/`
  Small deterministic tests for shared helpers and infrastructure utilities.

- `tests/stations/`
  Adapter and protocol tests, especially around `appAnalise`.

- `tests/workers/`
  Worker rule tests for discovery, backup, host checks, garbage collection and
  `appAnalise` processing.

- `tests/db/`
  Handler and query-shaping tests for `dbHandlerBKP`.

- `tests/webfusion/`
  Targeted tests for `webfusion` service logic.

- `tests/workers/drive_test/`
  Static sample payloads used by the `appAnalise` worker tests.

## What Is Covered Today

### Shared helpers

Covered files:

- `shared.errors`
- `shared.logging_utils`
- `host_handler.ssh_utils`
- `shared.tools`

Main contracts covered:

- canonical error formatting
- first-error retention in `ErrorHandler`
- timeout wrapper behavior
- log rotation behavior
- transient SSH/SFTP classification
- audit message formatting

### appAnalise adapter

Covered file:

- `appAnalise/appAnalise_connection.py`

Main contracts covered:

- protocol-shape validation
- malformed payload rejection
- `Answer` string vs dict behavior
- retryable versus definitive `appAnalise` failures
- per-spectrum validation rules pushed through normalization

### Workers

Covered files:

- `appCataloga_file_bin_proces_appAnalise.py`
- `appCataloga_file_bkp.py`
- `appCataloga_discovery.py`
- `appCataloga_garbage_collector.py`
- `appCataloga_host_check.py`
- `appCataloga_host_maintenance.py`

Main contracts covered:

- export decisions and artifact finalization for `appAnalise`
- per-spectrum site resolution behavior
- retry vs definitive processing failures
- worker-pool detection and shutdown broadcast in backup
- transient discovery bootstrap failures
- cooldown and reconnect behavior
- garbage collection retention-channel separation
- host connectivity tri-state behavior

### Database handlers

Covered file:

- `dbHandlerBKP.py`
- `dbHandlerRFM.py`

Main contracts covered:

- host cooldown preservation and release
- caller-owned timestamps for history/task updates
- garbage-collector candidate query behavior
- file-task identity selection
- host-task queue lifecycle behavior
- site lookup, insert and centroid-update rules
- geographic-code resolution
- file typing and repository-path construction
- dimension idempotency for procedure/equipment/unit/detector/trace
- spectrum insert metadata serialization
- bridge insertion and metadata-publication helpers

### webfusion

Covered files:

- `modules/map/service.py`
- `modules/task/service.py`

Main contracts covered:

- map-side host reconciliation for Celplan/CWSM receiver naming
- task builder reuse of durable `HOST_TASK` rows
- refusal to expose ad-hoc internal task types through the UI service layer

## Main Coverage Gaps

These are the biggest areas still under-covered today.

### `dbHandlerRFM.py` real persistence backend behavior

Direct coverage improved a lot, but some parts still need either deeper mocks
or true integration tests.

The main gaps are:

- rollback coverage in every insert helper
- end-to-end DB behavior for real spatial SQL and Parquet export backends

### `appCataloga.py` entrypoint

The host-facing socket/bootstrap entrypoint still lacks direct validation.

### `appCataloga_pub_metadata.py`

Publication behavior is not yet directly covered.

### `server_handler/` helpers

Some process-control and runtime helpers are still only validated indirectly by
worker tests.

### `webfusion` modules beyond map/task

There are no direct tests yet for:

- `modules/host/service.py`
- `modules/server/routes.py`
- `modules/spectrum/service.py`
- `app.py` route behavior

### True end-to-end integration

The suite is still mostly unit and contract oriented.

It does not yet provide:

- real MariaDB integration tests
- real repository filesystem integration tests
- real `nginx` / `waitress` end-to-end checks

## How To Run

Fast path for the whole active suite:

```bash
cd /RFFusion/test
./test_all.sh
```

The helper accepts normal `pytest` arguments, so it can also be used as a
single entrypoint for filtered runs:

```bash
cd /RFFusion/test
./test_all.sh tests/workers -q
./test_all.sh tests/db/test_dbhandler_rfm.py -q
./test_all.sh tests -k garbage -q
```

Run the full active suite:

```bash
cd /RFFusion/test
/opt/conda/envs/appdata/bin/python -m pytest tests -q
```

Run by area:

```bash
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/shared -q
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/stations -q
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers -q
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/db -q
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/webfusion -q
```

Run a single file:

```bash
/opt/conda/envs/appdata/bin/python -m pytest /RFFusion/test/tests/workers/test_appanalise_worker.py -q
```

## How To Read These Tests

The suite tries to follow a few conventions:

- module docstrings explain what a file is trying to protect
- fake DB and fake logger classes model only the behavior the test needs
- test names describe the contract, not the implementation detail
- comments are used to explain why a scenario matters, not to narrate obvious assertions

When adding new tests, prefer extending the contract of an existing module over
creating a one-off file for a tiny assertion.

## Rule

This directory should contain automated validation artifacts only.

Operational notebooks, ad-hoc SQL dumps, manual experiments and temporary lab
scripts do not belong here.
