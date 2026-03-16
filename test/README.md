# Test Suite

This directory contains the new validation test suite for the current
appCataloga codebase.

The old `/RFFusion/test` contents were archived to:

- `/RFFusion/cemetery/test_legacy_20260316_134816`

That legacy material belonged to earlier RF.Fusion iterations and is not
considered a reliable validation base for the current application.

## Goal

This new test tree is intended for:

- validation of shared helpers
- protocol and adapter validation
- worker rule validation
- database-handler rule validation
- regression protection for recent concurrency, shutdown, and timestamp-ownership fixes

## Structure

- `tests/shared/`: small deterministic unit tests for shared helpers
- `tests/stations/`: adapter and protocol tests
- `tests/workers/`: worker rule and helper tests
- `tests/db/`: handler and query-shaping tests
- `fixtures/`: reusable sample payloads and static test assets

## Initial Strategy

The suite should start with high-value validation targets:

1. `shared.tools.compose_message`
2. `shared.filter.Filter`
3. `stations.appAnaliseConnection`
4. helper rules from `appCataloga_file_bin_proces_appAnalise.py`
5. selected `dbHandlerBKP` and `dbHandlerRFM` methods with mocks

## Current Coverage Highlights

The suite now includes automated checks for:

- shared helpers such as `compose_message`, timeout helpers, and `ErrorHandler`
- `appAnaliseConnection` protocol validation and malformed payload handling
- appAnalise worker resolution rules, including retry vs definitive failure
- backup worker pool behavior, including seed visibility and shutdown broadcast
- `dbHandlerBKP` host cooldown rules and explicit timestamp ownership

These tests are intentionally biased toward observable effects and contract
validation, not optimistic mock-only assertions.

## Rule

This directory should contain automated validation artifacts only.

Operational notebooks, historical experiments, ad-hoc SQL dumps, and manual
lab scripts belong in archived or dedicated utility locations, not here.
