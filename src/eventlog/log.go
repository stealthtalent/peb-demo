// Package eventlog provides an in-memory event log for the peb-demo
// application. The event bus dispatches events through a consumer group worker
// that appends each event to this log; the web UI polls the HTMX endpoint to
// render the log live.
package eventlog

import (
	"sync"
	"time"
)

// Entry is a single event recorded in the log.
type Entry struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	Timestamp time.Time      `json:"timestamp"`
}

// Log is a thread-safe, in-memory append-only event log.
// It is safe for concurrent use: producers call Append from the bus
// consumer goroutine, while HTTP handlers call All from request goroutines.
type Log struct {
	mu      sync.Mutex
	entries []Entry
}

// NewLog creates an empty event log.
func NewLog() *Log {
	return &Log{}
}

// Append adds an entry to the log. The timestamp is set to the current
// time, so callers need not populate it.
func (l *Log) Append(e Entry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e.Timestamp = time.Now()
	l.entries = append(l.entries, e)
}

// All returns a snapshot copy of all entries, safe for the caller to read
// without holding the lock.
func (l *Log) All() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, len(l.entries))
	copy(out, l.entries)
	return out
}

// Len returns the current number of entries.
func (l *Log) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}
