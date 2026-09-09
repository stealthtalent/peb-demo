# Worker Pool Plan for peb-demo

## Context

**What exists today:**

- `Bus.Publish` — writes domain + outbox row in one tx (transactional outbox). — BUILT in `postgres-event-bus`
- `Bus.RunDispatcher` — single goroutine: outbox → message_queue[channel=group]. Wakes on `pg_notify('outbox_events')` + poll fallback. — BUILT
- `Bus.RunGroup` — single goroutine: message_queue[channel=group] → handler (atomic tx: effect + processed marker + queue delete). Wakes on `pg_notify(group)` + poll fallback. — BUILT
- peb-demo handler: appends to in-memory event log + fires `pg_notify('webui_events', full_event_json)` — the event JSON flows to the browser via pg_eventserv WebSocket. — BUILT (Phase 2 web bridge implemented in peb-demo)

**What's missing:**

The SPEC (§3) lists "Worker — drains the group's queue" as BUILT, and `RunGroup` exists — but it runs a **single worker goroutine per group**. For throughput, the group queue should support **N concurrent workers** competing via `FOR UPDATE SKIP LOCKED`. The integration test `TestGroupWorker_ConcurrentWorkersEachEffectOnce` already exists and passes, but it spins up multiple `Queue.RunAtomic` goroutines directly — `Bus.RunGroup` does not expose that capability.

## Spec Alignment

From SPEC.md §3 Components:
- Producer — BUILT
- Dispatcher — BUILT
- Worker — BUILT (but single-goroutine; needs pool)
- Queue workers (Queue.RunAtomic / RunLease) — BUILT
- Web bridge — PHASE 2 (implemented in peb-demo as handler + pg_notify)

From SPEC.md §4 Delivery Flow:
```
producer tx ──► outbox (seq++)
                   │  LISTEN/NOTIFY (+ periodic poll fallback)
                   ▼
              dispatcher ──► INSERT into the group's queue + advance cursor (one tx)
                   │
                   ▼
              queue:grp_a ──► workers
```

The "workers" (plural) implies multiple concurrent workers. The point-to-point engine already supports this — `claimNext` uses `FOR UPDATE SKIP LOCKED` (see `internal/eventbus/claim.go`), so multiple `RunAtomic` goroutines on the same channel naturally compete. We just need `RunGroup` to spawn N of them.

## Plan: Three Layers

### Layer 1 — postgres-event-bus (the library)

Add `GroupOptions.Workers int` field (default 1 = current behavior).

`Bus.RunGroup` changes from:
```go
return b.q.RunAtomic(ctx, opts, wrapped)
```
to:
```go
n := opts.Workers
if n < 1 { n = 1 }
var wg sync.WaitGroup
for i := 0; i < n; i++ {
    wg.Add(1)
    go func() {
        defer wg.Done()
        _ = b.q.RunAtomic(ctx, WorkerOptions{
            Channel:       opts.Group,
            Logger:        opts.Logger,
            DisableNotify: opts.DisableNotify,
        }, wrapped)
    }()
}
wg.Wait()
return ctx.Err()
```

Note: `WorkerOptions` only has `Channel`, `Logger`, `DisableNotify`. The `wrapped` handler already does idempotency checking via the `processed` table, so multiple workers safely compete via `FOR UPDATE SKIP LOCKED` in the underlying `claimNext` (see `claim.go`).

Changes:
1. `internal/eventbus/bus.go` — add `Workers int` to `GroupOptions`; add `"sync"` import; update `RunGroup` to spawn N goroutines
2. `api/eventbus.go` — `GroupOptions` is aliased, so the new field is automatically exposed. No change needed.
3. `integration/bus_test.go` — add `TestGroupWorker_WorkerPoolProcessesAllEvents` that calls `RunGroup` with `Workers: 4` and 50 events, verifying all are processed exactly once
4. `CLAUDE.md` — add decision note: "Phase 1 supports N concurrent workers per group via GroupOptions.Workers"
5. `SPEC.md` §3 — update Worker line

### Layer 2 — peb-demo (the consumer)

In `server.go` `NewApp`, update the `RunGroup` call:
```go
bus.RunGroup(ctx, eventbus.GroupOptions{
    Group:   "demo-group",
    Workers: 4,   // configurable via env, default 4
}, handler)
```

Add `PGQUEUE_GROUP_WORKERS` env var or a new `DEMO_WORKERS` var with default 4.

### Layer 3 — Observability (Phase 2)

Add metrics counters (Prometheus-style counters on the pool):
- `dispatcher_drain_count`, `dispatcher_events_dispatched_total`
- `group_worker_drain_count`, `group_events_processed_total`, `group_events_dlq_total`

These are PHASE 2 (not built), but the plan documents where they'd slot in.

## Implementation Order (test-first)

1. Add `Workers int` to `GroupOptions` in `internal/eventbus/bus.go`
2. Write integration test: `RunGroup` with `Workers: 3` processes all events — test fails (RunGroup ignores Workers)
3. Implement pool loop in `RunGroup` — test passes
4. Update `CLAUDE.md` decision log + `SPEC.md` §3
5. Update peb-demo `server.go` to set `Workers: 4`
6. Build + test peb-demo end-to-end

## Non-Goals (per SPEC §9)

No new broker primitives. No ordering guarantees (SPEC §5 is DROPPED). No per-key ordering. No content-based routing. No in-bus transformation. No priority delivery. No multi-region. No unbounded retention.

## Files Touched

- `postgres-event-bus/internal/eventbus/bus.go` — GroupOptions+Workers, RunGroup pool loop
- `postgres-event-bus/integration/bus_test.go` — TestGroupWorker_WorkerPoolProcessesAllEvents
- `postgres-event-bus/SPEC.md` — §3, §11, §12 updated
- `peb-demo/server.go` — Workers: 4 in RunGroup call

## Status: COMPLETE

All steps implemented and verified:
- 16/16 integration tests pass (including new TestGroupWorker_WorkerPoolProcessesAllEvents)
- `make test`, `make lint`, `go vet ./...` all clean
- peb-demo server running with Workers: 4
- End-to-end WebSocket delivery verified: POST /api/jobs → outbox → dispatcher → 4-worker pool → handler → pg_notify('webui_events') → pg_eventserv → WebSocket client receives full event JSON
- Worker pool monitoring UI added to Event Log tab with /api/worker-stats endpoint (2s polling)
- Database connections confirmed: 4 LISTEN "demo-group" connections for the 4 worker goroutines
