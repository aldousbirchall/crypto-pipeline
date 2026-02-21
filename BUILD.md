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
| Test Generation | in_progress | 2026-02-21 | - | test | - |
| Implementation | pending | - | - | dev | - |
| Verification | pending | - | - | verify | - |
| Review | pending | - | - | review | - |

## Human Checkpoints

- [x] Spec approval: approved 2026-02-21
- [ ] Final delivery: pending

## Experience Summary

(Filled after build completion.)
