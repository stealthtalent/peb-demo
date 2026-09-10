package main

import (
	"context"
	"encoding/json"
	"embed"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	eventbus "github.com/stealthtalent/postgres-event-bus/api"
	"github.com/stealthtalent/peb-demo/src/eventlog"
	"github.com/stealthtalent/peb-demo/src/middleware"
	"github.com/stealthtalent/peb-demo/src/models"
)

//go:embed src/ui/layout.html src/ui/jobs.html src/ui/candidates.html src/ui/eventlog.html
var templateFS embed.FS

var tmpl = template.Must(template.New("").
	Funcs(template.FuncMap{
		"formatTime": func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05")
		},
	}).
	ParseFS(templateFS, "src/ui/layout.html", "src/ui/jobs.html", "src/ui/candidates.html", "src/ui/eventlog.html"))

// pageData is the root template model for layout.html.
type pageData struct {
	Jobs       []models.Job
	Candidates []models.Candidate
	Events     []eventLogEntry
}

// eventLogEntry is the template-friendly view of an eventlog.Entry.
type eventLogEntry struct {
	ID      string
	Type    string
	Payload string
}

func eventLogEntryFrom(e eventlog.Entry) eventLogEntry {
	var payload string
	if e.Payload != nil {
		b, err := json.MarshalIndent(e.Payload, "", "  ")
		if err != nil {
			payload = fmt.Sprintf("%v", e.Payload)
		} else {
			payload = string(b)
		}
	}
	return eventLogEntry{
		ID:      e.ID,
		Type:    e.Type,
		Payload: payload,
	}
}

// rowTemplates for HTMX prepend swaps after create.
var rowTemplates = template.Must(template.New("").
	Funcs(template.FuncMap{
		"formatTime": func(t time.Time) string {
			return t.Format("2006-01-02 15:04:05")
		},
	}).
	Parse(`
{{define "jobRow"}}
<li class="list-item">
  <span class="id">{{.ID}}</span>
  <span class="title">{{.Title}}</span>
  <span class="created">{{formatTime .CreatedAt}}</span>
</li>
{{end}}

{{define "candidateRow"}}
<li class="list-item">
  <span class="id">{{.ID}}</span>
  <span class="name">{{.Name}}</span>
  <span class="email">{{.Email}}</span>
  <span class="created">{{formatTime .CreatedAt}}</span>
</li>
{{end}}
`))

// webuiNotifyChannel is the pg_notify channel that pg_eventserv listens on to
// push events to WebSocket clients in the browser.
const webuiNotifyChannel = "webui_events"

// App holds all the server's shared dependencies.
type App struct {
	pool     *pgxpool.Pool
	bus      *eventbus.Bus
	jobRepos *middleware.JobRepos
	candRepos *middleware.CandidateRepos
	eventLog *eventlog.Log
}

// NewApp wires up the database pool, event bus, repos, and in-memory event log.
func NewApp(ctx context.Context, dsn string) (*App, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect pool: %w", err)
	}

	bus := eventbus.NewBus(pool, eventbus.Config{})
	jobRepos := middleware.NewJobRepos(pool, bus)
	candRepos := middleware.NewCandidateRepos(pool, bus)
	elog := eventlog.NewLog()

	app := &App{
		pool:     pool,
		bus:      bus,
		jobRepos: jobRepos,
		candRepos: candRepos,
		eventLog: elog,
	}

	// Start the dispatcher: outbox -> group queue.
	go func() {
		if err := bus.RunDispatcher(context.Background(), eventbus.DispatcherOptions{
			Group: "demo-group",
		}); err != nil {
			log.Printf("[dispatcher] exited: %v", err)
		}
	}()

	// Start the consumer group with TWO concrete Workers, each constrained
	// to a single event type:
	// - JobWorker handles "job.created" events
	// - CandidateWorker handles "candidate.created" events
	// The base implementation (concurrency, claim, poll, retry, idempotency,
	// type filtering) lives in RunGroup from the public API. The concrete
	// per-event effects live only in this client application.
	jobWorker := NewJobWorker(elog, webuiNotifyChannel, pool)
	candWorker := NewCandidateWorker(elog, webuiNotifyChannel, pool)
	go func() {
		if err := bus.RunGroup(context.Background(), eventbus.GroupOptions{
			Group:   "demo-group",
			Workers: 2,
		}, jobWorker); err != nil {
			log.Printf("[job worker] exited: %v", err)
		}
	}()
	go func() {
		if err := bus.RunGroup(context.Background(), eventbus.GroupOptions{
			Group:   "demo-group",
			Workers: 2,
		}, candWorker); err != nil {
			log.Printf("[candidate worker] exited: %v", err)
		}
	}()

	return app, nil
}

// Close releases all pooled resources.
func (a *App) Close() {
	a.pool.Close()
}

// --- HTML page handler ---

func (a *App) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	ctx := r.Context()

	jobs, err := a.jobRepos.List(ctx)
	if err != nil {
		http.Error(w, "fetch jobs: "+err.Error(), http.StatusInternalServerError)
		return
	}
	cands, err := a.candRepos.List(ctx)
	if err != nil {
		http.Error(w, "fetch candidates: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Build event log entries for initial page load
	entries := a.eventLog.All()
	events := make([]eventLogEntry, len(entries))
	for i, e := range entries {
		events[i] = eventLogEntryFrom(e)
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "layout.html", pageData{
		Jobs:       jobs,
		Candidates: cands,
		Events:     events,
	}); err != nil {
		log.Printf("[handler] template error: %v", err)
		http.Error(w, "template error: "+err.Error(), http.StatusInternalServerError)
	}
}

// --- HTMX fragment handlers (for list updates after create) ---

func (a *App) handleJobsFragment(w http.ResponseWriter, r *http.Request) {
	jobs, err := a.jobRepos.List(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "jobs", jobs); err != nil {
		log.Printf("[handler] template error: %v", err)
	}
}

func (a *App) handleCandidatesFragment(w http.ResponseWriter, r *http.Request) {
	cands, err := a.candRepos.List(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "candidates", cands); err != nil {
		log.Printf("[handler] template error: %v", err)
	}
}

// --- Create handlers ---

func (a *App) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	title := strings.TrimSpace(r.PostFormValue("title"))
	if title == "" {
		http.Error(w, "title is required", http.StatusBadRequest)
		return
	}

	job, err := a.jobRepos.CreateWithEvents(r.Context(), title)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := rowTemplates.ExecuteTemplate(w, "jobRow", job); err != nil {
		log.Printf("[handler] template error: %v", err)
	}
}

func (a *App) handleCreateCandidate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	name := strings.TrimSpace(r.PostFormValue("name"))
	email := strings.TrimSpace(r.PostFormValue("email"))
	if name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if email == "" {
		http.Error(w, "email is required", http.StatusBadRequest)
		return
	}

	cand, err := a.candRepos.CreateWithEvents(r.Context(), name, email)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := rowTemplates.ExecuteTemplate(w, "candidateRow", cand); err != nil {
		log.Printf("[handler] template error: %v", err)
	}
}

// --- Event log API handler ---

func (a *App) handleEventLog(w http.ResponseWriter, r *http.Request) {
	entries := a.eventLog.All()
	out := make([]map[string]any, len(entries))
	for i, e := range entries {
		out[i] = map[string]any{
			"id":      e.ID,
			"type":    e.Type,
			"payload": e.Payload,
			"time":    e.Timestamp.Format("2006-01-02 15:04:05"),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

// --- Worker pool stats API handler ---

type workerStats struct {
	QueueDepth    int64 `json:"queue_depth"`
	Processed     int64 `json:"processed"`
	OutboxPending int64 `json:"outbox_pending"`
	Workers       int   `json:"workers"`
}

func (a *App) handleWorkerStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stats := workerStats{Workers: 4} // 2 JobWorker + 2 CandidateWorker

	row := a.pool.QueryRow(ctx, `SELECT COALESCE(count(*), 0) FROM message_queue WHERE channel = $1 AND available_at <= now()`, "demo-group")
	_ = row.Scan(&stats.QueueDepth)

	row = a.pool.QueryRow(ctx, `SELECT COALESCE(count(*), 0) FROM processed WHERE consumer_group = $1`, "demo-group")
	_ = row.Scan(&stats.Processed)

	row = a.pool.QueryRow(ctx, `SELECT COALESCE(count(*) - COALESCE((SELECT last_seq FROM dispatcher_cursor), 0), 0) FROM outbox`)
	_ = row.Scan(&stats.OutboxPending)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// --- Routes ---

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", http.HandlerFunc(a.handleRoot))

	// HTMX fragment endpoints for list updates
	mux.Handle("/jobs", http.HandlerFunc(a.handleJobsFragment))
	mux.Handle("/candidates", http.HandlerFunc(a.handleCandidatesFragment))

	// Create endpoints (POST only)
	mux.Handle("/api/jobs", http.HandlerFunc(a.handleCreateJob))
	mux.Handle("/api/candidates", http.HandlerFunc(a.handleCreateCandidate))

	// Event log API (for initial load or polling fallback)
	mux.Handle("/api/eventlog", http.HandlerFunc(a.handleEventLog))

	// Worker pool stats API (for live monitoring)
	mux.Handle("/api/worker-stats", http.HandlerFunc(a.handleWorkerStats))

	return mux
}

// --- main ---

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dbUser := getenv("DB_USER", "test")
		dbPass := getenv("DB_PASSWORD", "")
		if dbPass == "" {
			log.Fatal("DATABASE_URL or DB_PASSWORD environment variable is required")
		}
		dsn = fmt.Sprintf("postgres://%s:%s@127.0.0.1:8432/peb_demo?sslmode=disable",
			url.QueryEscape(dbUser), url.QueryEscape(dbPass))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	app, err := NewApp(ctx, dsn)
	if err != nil {
		log.Fatalf("startup: %v", err)
	}
	defer app.Close()

	port := getenv("PORT", "8666")

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: app.routes(),
	}

	log.Printf("peb-demo server starting on http://localhost:%s", port)
	log.Printf("WebSocket events via pg_eventserv on ws://localhost:7700/listen/%s", webuiNotifyChannel)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")
	_ = srv.Shutdown(context.Background())
	log.Println("done")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
