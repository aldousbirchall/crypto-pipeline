# Build Manifest: crypto-pipeline

## Identity

- **Name**: crypto-pipeline
- **Created**: 2026-02-21
- **Factory Version**: 0.2.0

## Technology Decisions

- **Language**: Python 3.9+
- **HTTP Client**: requests
- **WebSocket**: websocket-client
- **Storage**: sqlite3 (stdlib)
- **Package Manager**: pip
- **Test Framework**: pytest + hypothesis
- **Build Tool**: make + setuptools
- **Rationale**: ETL + streaming data pipeline. requests for REST API, websocket-client for live price feeds, sqlite3 for local storage without ORM overhead.

## Build Phases

| Phase | Status | Started | Completed | Agent | Iterations |
|---|---|---|---|---|---|
| Specification | complete | 2026-02-21 | 2026-02-21 | spec | 1 |
| Test Generation | complete | 2026-02-21 | 2026-02-21 | test | 1 |
| Implementation | complete | 2026-02-21 | 2026-02-21 | dev | 1 |
| Verification | complete | 2026-02-21 | 2026-02-21 | verify | 2 |
| Review | complete | 2026-02-21 | 2026-02-21 | review | 1 |

## Human Checkpoints

- [x] Spec approval: approved 2026-02-21
- [x] Final delivery: 2026-02-21

## Experience Summary

- **Outcome**: Success. 89/89 holdout tests passing, review verdict ACCEPT WITH ISSUES.
- **Verify iterations**: 2. First pass had 42/89 failures (all test-side). Second pass had 3/89 failures (all implementation-side).
- **Test-side fixes**: 5 (subprocess-mock incompatibility, argparse flag order, column naming, timestamp types, schema definition)
- **Implementation fixes**: 3 (volatility off-by-one, SIGINT handling, empty query output)
- **Review findings**: 0 critical, 2 major, 8 minor. No fix cycle required.
- **New knowledge**: 1 anti-pattern (subprocess-mock-incompatibility), 1 pattern (argparse-parent-flag-placement)
- **Key lesson**: Test agent schema inference diverged from design.md because it only sees requirements.md. Consider giving test agent access to the schema section of design.md.
