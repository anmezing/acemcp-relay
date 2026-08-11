# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

acemcp-relay is the control plane of the LCE cloud service: a Go (Gin) MCP relay that
authenticates users, meters usage, and orchestrates indexing/retrieval between cloud
clients (the `@anmezing/lce-cloud` stdio client or direct HTTP MCP callers) and the
internal LCE Node backend. State lives in PostgreSQL; Redis holds quota counters and
short-lived caches. The relay is a **single-instance design**: auth cache, start rate
limits, and MCP sessions are in-process state (see README before scaling out).

## Build, Run, Test

```bash
go build ./...
go vet ./...
go test ./...
go run .            # SERVER_ADDR defaults to 127.0.0.1:8080
```

If proxy.golang.org times out in this environment, use `GOPROXY=https://goproxy.cn,direct`.

## Key Files

| Area | File | Notes |
| --- | --- | --- |
| HTTP entry, MCP dispatch, auth middleware | `main.go` | `/mcp` JSON-RPC, tool whitelist, version gating, request logging |
| Indexing job lifecycle + Postgres schema | `index_jobs.go` | `codebase_index` start/upload/complete/fail, sweeper, quotas, migrations |
| MCP `codebase_index` tool surface | `mcp_index_tool.go` | strict arg schemas, start rate limit, client_version gate |
| Root management + retrieval extras | `root_admin.go` | `GET /mcp/roots`, `POST /mcp/delete-root`, `_index_status` injection |
| Tenant assertion signer | `tenant_assertion.go` | HMAC v1 format shared with LCE (golden vectors on both sides) |
| Quotas | `quota.go` | daily request + index-byte metering (Redis) |
| Access control | `access_control.go` | trusted console requests, banned users |
| Metrics + structured logs | `metrics.go`, `obslog.go` | Prometheus registry, logfmt events, X-Request-Id |
| Model config (BYO rerank) | `modelconfig.go` | encrypted per-user rerank config |
| E2E helper | `cmd/e2e-redis` | standalone miniredis for the cross-repo E2E harness |

## Working Rules

- The MCP tool surface, arg field names, and response envelope are cross-repo contracts
  (LCE client, frontend console). The shared source of truth is
  `docs/contracts/cloud-protocol.json` in the LCE repo; changing any contract value
  requires synchronized changes plus golden/contract tests on every side.
- Deployed migration text is immutable: schema changes are new `ALTER ... IF NOT EXISTS`
  statements (or new versioned migrations), never edits to existing CREATE text.
- Every behavior change ships with sqlmock/unit tests; `go build ./... && go vet ./... && go test ./...`
  must pass before claiming completion.
- Path labels in metrics must stay bounded (route templates or whitelisted tool names,
  never raw URLs).
