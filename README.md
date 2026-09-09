# peb-demo

A demo application showcasing the [postgres-event-bus](https://github.com/stealthtalent/postgres-event-bus) transactional outbox pattern with live WebSocket UI updates via `pg_eventserv`.

## Architecture

```
POST /api/jobs  ──────────────────┐
                                   │
POST /api/candidates ───────┐     │
                            │     │
   ┌──────────────┐   tx   │  ┌───▼──┐     ┌──────────┐     ┌────────────┐
   │  middleware  │  + bus.Publish  │  │ outbox │ ──► │ message_queue │ ──► │ processed │
   │  (tx-boundary) │               │  │ table  │     │               │     │ table     │
   └──────┬───────┘               │  └────────┘     └──────────────┘     └────────────┘
         │                      │          │                                     │
         │                      │          │ pg_notify("demo-group")            │
         │                      │          └────────────────┬───────────────────┘
         │                      │                             │
         │                      │  ┌────────────────┐        │
         │                      │  │ RunDispatcher  │        │
         │                      │  │ (outbox→queue) │        │
         │                      │  └────────┬───────┘        │
         │                      │           │                │
         │                      │  ┌────────▼───────┐        │
         │                      │  │  RunGroup      │        │
         │                      │  │ (handler)      │◄───────┼───── in-memory eventlog
         │                      │  └────────┬───────┘        │
         │                      │           │                │
         │                      │           │ pg_notify("webui_events")
         │                      │           └───────────────┐│
         │                      │                           ││
         │                      ▼                           ││
         │              ┌──────────────┐                    ││
         │              │   peb-demo    │                    ││
         │              │  Go server    │                    ││
         │              │  :8666        │                    ││
         │              └──────┬───────┘                    ││
         │                     │                            ││
         └─────────────────────┼────────────────────────────┼┘
                               │                            │
                     ┌─────────▼────────┐         ┌───────▼───────┐
                     │   pg_eventserv    │         │  browser      │
                     │   :7700           │ ──────► │  WebSocket    │
                     │   (Docker)        │   WS    │  ws://:7700/  │
                     └──────────────────┘          │  listen/     │
                                                   │  webui_events│
                                                   └───────────────┘
```

## Components

1. **peb-demo server** (`server.go`) — Go HTTP server on port 8666
   - HTMX forms for creating Jobs and Candidates
   - REST API (`POST /api/jobs`, `POST /api/candidates`, `GET /api/eventlog`)
   - Runs the bus dispatcher and consumer group (group `demo-group`)
   - Handler fires `pg_notify('webui_events', full_event_json)` on commit

2. **pg_eventserv** (Docker, port 7700) — CrunchyData's event server
   - LISTENs on PostgreSQL NOTIFY channel `webui_events`
   - Bridges NOTIFY payloads to WebSocket clients at `ws://localhost:7700/listen/webui_events`

3. **Frontend** (`src/ui/`) — HTMX + Go html/template
   - `layout.html` — page layout with tabs and inline WebSocket JS
   - `eventlog.html` — event log section with WebSocket client that connects to pg_eventserv
   - WebSocket client receives full event JSON (type, id, payload, timestamp) and prepends to the log list

## How it works

1. User submits a form (e.g., creates a Job) via HTMX POST
2. Middleware begins a transaction, INSERTs the job, calls `bus.Publish` (which INSERTs to `outbox` + fires `pg_notify('outbox_events', event_id)`) — all in the same tx
3. Dispatcher's `LISTEN 'outbox_events'` wakes up, reads new outbox rows, INSERTs into `message_queue` + fires `pg_notify('demo-group', event_id)`
4. Group worker's `LISTEN 'demo-group'` wakes up, claims the queue row, invokes the handler
5. Handler appends to in-memory event log **and** fires `pg_notify('webui_events', full_event_json)` inside its transaction
6. pg_eventserv receives the NOTIFY and pushes the JSON payload to all WebSocket clients
7. Browser's WebSocket client receives the event and updates the event log live

## Setup

### Prerequisites
- Go 1.26+
- PostgreSQL (local or Docker)
- Docker (for pg_eventserv)

### Running

```bash
# 1. Start PostgreSQL with the peb_demo database
cd ../postgres-event-bus
make db-up

# 2. Apply migrations
make db-migrate

# 3. Set up the peb-demo database schema
psql -h 127.0.0.1 -p 8432 -U test -d peb_demo -f schemas/demo.sql

# 4. Start pg_eventserv (bridges pg_notify → WebSocket)
docker run -d --name pg_eventserv \
  -p 7700:7700 \
  -e DATABASE_URL="postgres://test:test@host.docker.internal:8432/peb_demo?sslmode=disable" \
  pramsey/pg_eventserv

# 5. Start the peb-demo server
cd ../peb-demo
DB_PASSWORD=test PORT=8666 ./peb-demo

# 6. Open http://localhost:8666 in your browser
```

## Phases

**Phase 1 (BUILT):**
- Job and Candidate entities with CRUD repositories
- Transactional event firing via outbox (Publish in same tx as INSERT)
- Event bus dispatcher + consumer group processing events
- Live WebSocket event log using pg_eventserv

**Phase 2 (planned):**
- Event routing/fanout to multiple consumer groups
- Event replay UI
- Persistent event store (replacing in-memory log)
