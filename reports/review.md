# Code Review Report

**Timestamp**: 2026-02-21
**Verification Status**: PASS (89/89)
**Review Verdict**: ACCEPT WITH ISSUES

## Summary

The implementation is well-structured, correctly layered, and delivers all 15 requirements. All design interfaces match their specified signatures. The metrics engine is cleanly separated from I/O, parameterised SQL prevents injection, and error handling meets spec. Two major observations are noted: a foreign key constraint that prevents standalone streaming without prior backfill, and a non-obvious backoff reset strategy. Neither blocks delivery.

## Correctness

SMA, VWAP, and volatility computations are correct and verified by holdout tests. Backfill and refresh orchestration are functionally correct. Query date handling correctly covers the full end-of-day window. Duplicate candle suppression via `INSERT OR IGNORE` and metrics upsert via `INSERT OR REPLACE` are correct.

### Issues

| ID | Severity | Category | Description | Location |
|---|---|---|---|---|
| C-001 | Major | Backoff semantics | REQ-004 requires the backoff to reset upon successful reconnection. The implementation resets `backoff = 1.0` inside `on_message` rather than `on_open`. This is functionally equivalent in the normal case (first message confirms a working connection) but is non-obvious: a connection that opens but receives no messages would not reset the backoff. | `src/crypto_pipeline/pipeline.py:143-144` |
| C-002 | Minor | VWAP numeric stability | `cum_vol == 0.0` uses exact floating-point equality. Extremely small cumulative volumes could produce numerically unstable VWAP values. Not a hard defect given the spec only addresses zero-volume. | `src/crypto_pipeline/metrics.py:96` |

## Security

For a local CLI data pipeline with no network-facing surface, the security posture is appropriate. Parameterised queries throughout `storage.py` prevent SQL injection. JSON parsing is confined to `json.loads` with no `eval` or dynamic execution. No credentials or API keys are stored in code. `PRAGMA foreign_keys=ON` is correctly set.

### Issues

| ID | Severity | Category | Description | Location |
|---|---|---|---|---|
| S-001 | Minor | Input handling | The `--db` flag accepts an arbitrary file path with no validation. For a local CLI tool this is acceptable, but an invalid path surfaces as an unhandled `sqlite3.OperationalError` caught by the generic exception handler. | `src/crypto_pipeline/cli.py:105` |
| S-002 | Minor | WebSocket message validation | `float(price_str)` in `_on_message` can raise `ValueError` on a malformed price string. The exception propagates to `_on_error` implicitly via WebSocketApp internals. The tick is skipped but the error message is confusing. | `src/crypto_pipeline/api_client.py:102-103` |

## Quality

Code is well-structured and consistently styled. Layering is respected: no I/O in the metrics engine, no business logic in storage, no direct database access from the CLI. Type hints are used throughout with `from __future__ import annotations` for Python 3.9 compatibility. Docstrings match design spec interfaces.

### Issues

| ID | Severity | Category | Description | Location |
|---|---|---|---|---|
| Q-001 | Minor | Code smell | The `on_message` callback in `stream()` mutates `backoff` via `nonlocal`. Backoff state belongs in the reconnection loop, not in the message callback. | `src/crypto_pipeline/pipeline.py:137-144` |
| Q-002 | Minor | Error handling | `backfill()` and `refresh()` catch `Exception` broadly, losing the distinction between client errors (4xx) and transient network failures. Adequate per spec but limits operability. | `src/crypto_pipeline/pipeline.py:36-38, 43-45` |
| Q-003 | Minor | Performance | `insert_candles` inserts one row at a time. `executemany` would be cleaner for the ~720-candle backfill. | `src/crypto_pipeline/storage.py:96-106` |
| Q-004 | Minor | Performance | `insert_tick` commits after every single tick. During streaming this produces one fsync per price update. A batched commit strategy would reduce I/O overhead. | `src/crypto_pipeline/storage.py:174-178` |

## Completeness

All 15 requirements are implemented. All 10 design interfaces are present and match their specified signatures. The package structure matches the design specification. All four subcommands are present and functional. Query output handles NULL metrics per REQ-006.4. The `--db` flag defaults to `crypto_pipeline.db` per REQ-008.

### Issues

| ID | Severity | Category | Description | Location |
|---|---|---|---|---|
| CP-001 | Major | FK constraint gap | The `ticks` table has a foreign key on `asset_id` referencing `assets`. With `PRAGMA foreign_keys=ON`, inserting a tick for an asset with no `assets` record (e.g., running `stream` without prior `backfill`) will raise `IntegrityError`. The `stream` command does not pre-populate `assets` records. | `src/crypto_pipeline/storage.py:173`, `pipeline.py:140` |
| CP-002 | Minor | UX gap | REQ-005.3 requires the output to indicate no data is available. The output is `asset_id\tN/A\tN/A`, which is technically an indicator but could be mistaken for a formatting artefact. | `src/crypto_pipeline/cli.py:127-129` |
