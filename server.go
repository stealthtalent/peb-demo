package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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
	ID        string
	Type      string
	Payload   string
	Timestamp time.Time
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
		ID:        e.ID,
		Type:      e.Type,
		Payload:   payload,
		Timestamp: e.Timestamp,
	}
}

// rowTemplates holds templates for individual job/candidate rows used in
// HTMX prepend swaps after create.
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

	// Start the dispatcher: outbox → group queue.
	go func() {
		if err := bus.RunDispatcher(context.Background(), eventbus.DispatcherOptions{
			Group: "demo-group",
		}); err != nil {
			log.Printf("[dispatcher] exited: %v", err)
		}
	}()

	// Start the consumer group: group queue → handler (append to event log).
	go func() {
		if err := bus.RunGroup(context.Background(), eventbus.GroupOptions{
			Group: "demo-group",
		}, func(ctx context.Context, tx pgx.Tx, e eventbus.Event) error {
			var payload map[string]any
			if len(e.Payload) > 0 {
				if err := json.Unmarshal(e.Payload, &payload); err != nil {
					log.Printf("[handler] unmarshal payload for %s: %v", e.ID, err)
					return nil
				}
			}
			app.eventLog.Append(eventlog.Entry{
				ID:      e.ID,
				Type:    e.Type,
				Payload: payload,
			})
			return nil
		}); err != nil {
			log.Printf("[group worker] exited: %v", err)
		}
	}()

	return app, nil
}

// Close releases all pooled resources.
func (a *App) Close() {
	a.pool.Close()
}

// ─── HTML page handler ────────────────────────────────────────────────

// handleRoot renders the full page with jobs, candidates, and event log.
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

	entries := a.eventLog.All()
	events := make([]eventLogEntry, len(entries))
	for i, e := range entries {
		events[i] = eventLogEntryFrom(e)
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "src/ui/layout.html", pageData{
		Jobs:       jobs,
		Candidates: cands,
		Events:     events,
	}); err != nil {
		log.Printf("[handler] template error: %v", err)
	}
}

// ─── HTMX fragment handlers ────────────────────────────────────────────

// handleJobsFragment returns just the jobs list content (for HTMX swaps).
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

// handleCandidatesFragment returns just the candidates list content.
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

// handleEventlogFragment is the HTMX endpoint for the live event log.
// It returns just the event log list items, refreshed every 2 seconds by
// the client-side setInterval in eventlog.html.
func (a *App) handleEventlogFragment(w http.ResponseWriter, r *http.Request) {
	entries := a.eventLog.All()
	events := make([]eventLogEntry, len(entries))
	for i, e := range entries {
		events[i] = eventLogEntryFrom(e)
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "eventlog", events); err != nil {
		log.Printf("[handler] template error: %v", err)
	}
}

// ─── Create handlers (HTMX target responses) ──────────────────────────

// handleCreateJob creates a job (publishing an event in the same tx),
// then returns the new job row HTML for HTMX to prepend.
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

// handleCreateCandidate creates a candidate (publishing an event in the same tx),
// then returns the new candidate row HTML for HTMX to prepend.
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

// ─── Routes ───────────────────────────────────────────────────────────

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", http.HandlerFunc(a.handleRoot))

	// HTMX fragment endpoints
	mux.Handle("/jobs", http.HandlerFunc(a.handleJobsFragment))
	mux.Handle("/candidates", http.HandlerFunc(a.handleCandidatesFragment))
	mux.Handle("/api/eventlog", http.HandlerFunc(a.handleEventlogFragment))

	// Create endpoints (POST only)
	mux.Handle("/api/jobs", http.HandlerFunc(a.handleCreateJob))
	mux.Handle("/api/candidates", http.HandlerFunc(a.handleCreateCandidate))

	return mux
}

// ─── main ─────────────────────────────────────────────────────────────

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		// Default matches the docker-compose Postgres on port 8432.
		dsn = "postgres://test:peb_demo@localhost:8432/peb_demo?sslmode=disable"
		// TODO: set DATABASE_URL env var with real credentials in production
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	app, err := NewApp(ctx, dsn)
	if err != nil {
		log.Fatalf("startup: %v", err)
	}
	defer app.Close()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: app.routes(),
	}

	log.Printf("peb-demo server starting on http://localhost:%s", port)
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
