-- Schema for the peb-demo application.
--
-- These two tables are the business-data tables the demo app CRUDs against.
-- They are SEPARATE from the postgres-event-bus outbox tables (outbox,
-- dispatcher_cursor, processed, message_queue), which live in the
-- postgres-event-bus migration files. Apply this schema first, then the
-- event-bus migrations, so the outbox Publish calls succeed.

CREATE TABLE IF NOT EXISTS jobs (
    id         TEXT          PRIMARY KEY,
    title      TEXT          NOT NULL,
    created_at TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS candidates (
    id         TEXT          PRIMARY KEY,
    name       TEXT          NOT NULL,
    email      TEXT          NOT NULL,
    created_at TIMESTAMPTZ   NOT NULL DEFAULT now()
);
