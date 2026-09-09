package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	eventbus "github.com/stealthtalent/postgres-event-bus/api"
	"github.com/stealthtalent/peb-demo/src/eventlog"
)

// webuiPayload is the JSON sent to the browser via pg_notify on the webui
// channel. pg_eventserv forwards this to WebSocket clients.
type webuiPayload struct {
	Worker  string         `json:"worker"`
	Type    string         `json:"type"`
	ID      string         `json:"id"`
	Payload map[string]any `json:"payload"`
	Time    string         `json:"time"`
}

// baseWorker provides shared behavior for the concrete JobWorker and
// CandidateWorker: appending to the in-memory event log and firing
// pg_notify on the webui channel. Each sub-type constrains itself to
// a single event type via EventType() and carries a human-readable
// worker name for UI display.
type baseWorker struct {
	name      string
	eventType string
	eventLog  *eventlog.Log
	channel   string
}

// fireNotify marshals the event to webui JSON and executes pg_notify on
// the webui channel within the provided transaction.
func (b *baseWorker) fireNotify(ctx context.Context, tx pgx.Tx, e eventbus.Event, payload map[string]any) {
	now := time.Now()
	wsPayload, _ := json.Marshal(webuiPayload{
		Worker:  b.name,
		Type:    e.Type,
		ID:      e.ID,
		Payload: payload,
		Time:    now.Format("2006-01-02 15:04:05"),
	})
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, b.channel, string(wsPayload)); err != nil {
		log.Printf("[worker] pg_notify webui: %v", err)
	}
}

// logEvent appends the event to the in-memory log (for REST API / initial load).
func (b *baseWorker) logEvent(e eventbus.Event, payload map[string]any) {
	b.eventLog.Append(eventlog.Entry{
		ID:      e.ID,
		Type:    e.Type,
		Payload: payload,
	})
}

// JobEvent is the known event type for job-related events.
const JobEvent = "job.created"

// CandidateEvent is the known event type for candidate-related events.
const CandidateEvent = "candidate.created"

// JobWorker is a concrete Worker implementation for "job.created" events.
// It logs events and pushes them to the web UI via pg_notify.
type JobWorker struct {
	baseWorker
}

// NewJobWorker creates a Worker that handles job.created events.
func NewJobWorker(el *eventlog.Log, channel string) *JobWorker {
	return &JobWorker{
		baseWorker: baseWorker{
			name:      "JobWorker",
			eventType: JobEvent,
			eventLog:  el,
			channel:   channel,
		},
	}
}

func (w *JobWorker) EventType() string { return w.eventType }

// jobProcessingDelay simulates processing time so users can watch events
// flow through the worker in real-time on the demo UI. This is demo-only
// and not part of the library code.
const jobProcessingDelay = 200 * time.Millisecond

func (w *JobWorker) Handle(ctx context.Context, tx pgx.Tx, e eventbus.Event) error {
	// Simulate processing delay for UI visibility (demo only)
	select {
	case <-time.After(jobProcessingDelay):
	case <-ctx.Done():
		return ctx.Err()
	}

	var payload map[string]any
	if len(e.Payload) > 0 {
		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			log.Printf("[job worker] unmarshal payload for %s: %v", e.ID, err)
			payload = nil
		}
	}
	w.logEvent(e, payload)
	w.fireNotify(ctx, tx, e, payload)
	return nil
}

// CandidateWorker is a concrete Worker implementation for "candidate.created"
// events. It logs events and pushes them to the web UI via pg_notify.
type CandidateWorker struct {
	baseWorker
}

// NewCandidateWorker creates a Worker that handles candidate.created events.
func NewCandidateWorker(el *eventlog.Log, channel string) *CandidateWorker {
	return &CandidateWorker{
		baseWorker: baseWorker{
			name:      "CandidateWorker",
			eventType: CandidateEvent,
			eventLog:  el,
			channel:   channel,
		},
	}
}

func (w *CandidateWorker) EventType() string { return w.eventType }

// candidateProcessingDelay simulates processing time so users can watch events
// flow through the worker in real-time on the demo UI. This is demo-only
// and not part of the library code.
const candidateProcessingDelay = 200 * time.Millisecond

func (w *CandidateWorker) Handle(ctx context.Context, tx pgx.Tx, e eventbus.Event) error {
	// Simulate processing delay for UI visibility (demo only)
	select {
	case <-time.After(candidateProcessingDelay):
	case <-ctx.Done():
		return ctx.Err()
	}

	var payload map[string]any
	if len(e.Payload) > 0 {
		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			log.Printf("[candidate worker] unmarshal payload for %s: %v", e.ID, err)
			payload = nil
		}
	}
	w.logEvent(e, payload)
	w.fireNotify(ctx, tx, e, payload)
	return nil
}
