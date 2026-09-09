package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	eventbus "github.com/stealthtalent/postgres-event-bus/api"
	"github.com/stealthtalent/peb-demo/src/events"
	"github.com/stealthtalent/peb-demo/src/models"
)

// EventBus wraps the postgres-event-bus Bus so demo code references a single
// named type instead of reaching for the eventbus package directly.
type EventBus struct {
	bus *eventbus.Bus
}

// NewEventBus constructs an EventBus around an already-configured *eventbus.Bus.
func NewEventBus(bus *eventbus.Bus) *EventBus {
	return &EventBus{bus: bus}
}

// Bus returns the underlying eventbus.Bus so callers can start the dispatcher
// and consumer group.
func (e *EventBus) Bus() *eventbus.Bus {
	return e.bus
}

// JobRepos performs job CRUD together with event publishing inside a single
// transaction — the INSERT and the outbox Publish commit or roll back together,
// which is the transactional-outbox guarantee.
type JobRepos struct {
	pool *pgxpool.Pool
	bus  *eventbus.Bus
}

// NewJobRepos constructs a JobRepos bound to the given pool and event bus.
func NewJobRepos(pool *pgxpool.Pool, bus *eventbus.Bus) *JobRepos {
	return &JobRepos{pool: pool, bus: bus}
}

// CreateWithEvents inserts a job and publishes a "job.created" event in the
// same transaction. On success the job record and the outbox row are both
// committed; on error both are rolled back, so no orphaned events can occur.
func (r *JobRepos) CreateWithEvents(ctx context.Context, title string) (models.Job, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return models.Job{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	j := models.Job{
		ID:        fmt.Sprintf("job_%d", time.Now().UnixNano()),
		Title:     title,
		CreatedAt: time.Now(),
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO jobs (id, title, created_at) VALUES ($1, $2, $3)`,
		j.ID, j.Title, j.CreatedAt); err != nil {
		return models.Job{}, fmt.Errorf("insert job: %w", err)
	}

	payload, err := json.Marshal(map[string]any{
		"id":         j.ID,
		"title":      j.Title,
		"created_at": j.CreatedAt.Format(time.RFC3339),
	})
	if err != nil {
		return models.Job{}, fmt.Errorf("marshal job payload: %w", err)
	}

	if _, err := r.bus.Publish(ctx, tx, eventbus.PublishParams{
		Type:        events.TypeJobCreated,
		OrderingKey: j.ID,
		Payload:     json.RawMessage(payload),
	}); err != nil {
		return models.Job{}, fmt.Errorf("publish job.created: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return models.Job{}, fmt.Errorf("commit: %w", err)
	}
	return j, nil
}

// List returns all jobs ordered by creation time descending.
func (r *JobRepos) List(ctx context.Context) ([]models.Job, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, title, created_at FROM jobs ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []models.Job
	for rows.Next() {
		var j models.Job
		if err := rows.Scan(&j.ID, &j.Title, &j.CreatedAt); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}

// Get fetches a single job by id.
func (r *JobRepos) Get(ctx context.Context, id string) (*models.Job, error) {
	var j models.Job
	err := r.pool.QueryRow(ctx,
		`SELECT id, title, created_at FROM jobs WHERE id = $1`, id).
		Scan(&j.ID, &j.Title, &j.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

// CandidateRepos performs candidate CRUD together with event publishing inside
// a single transaction, mirroring JobRepos.
type CandidateRepos struct {
	pool *pgxpool.Pool
	bus  *eventbus.Bus
}

// NewCandidateRepos constructs a CandidateRepos bound to the given pool and
// event bus.
func NewCandidateRepos(pool *pgxpool.Pool, bus *eventbus.Bus) *CandidateRepos {
	return &CandidateRepos{pool: pool, bus: bus}
}

// CreateWithEvents inserts a candidate and publishes a "candidate.created"
// event in the same transaction.
func (r *CandidateRepos) CreateWithEvents(ctx context.Context, name, email string) (models.Candidate, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return models.Candidate{}, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	c := models.Candidate{
		ID:        fmt.Sprintf("cand_%d", time.Now().UnixNano()),
		Name:      name,
		Email:     email,
		CreatedAt: time.Now(),
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO candidates (id, name, email, created_at) VALUES ($1, $2, $3, $4)`,
		c.ID, c.Name, c.Email, c.CreatedAt); err != nil {
		return models.Candidate{}, fmt.Errorf("insert candidate: %w", err)
	}

	payload, err := json.Marshal(map[string]any{
		"id":         c.ID,
		"name":       c.Name,
		"email":      c.Email,
		"created_at": c.CreatedAt.Format(time.RFC3339),
	})
	if err != nil {
		return models.Candidate{}, fmt.Errorf("marshal candidate payload: %w", err)
	}

	if _, err := r.bus.Publish(ctx, tx, eventbus.PublishParams{
		Type:        events.TypeCandidateCreated,
		OrderingKey: c.ID,
		Payload:     json.RawMessage(payload),
	}); err != nil {
		return models.Candidate{}, fmt.Errorf("publish candidate.created: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return models.Candidate{}, fmt.Errorf("commit: %w", err)
	}
	return c, nil
}

// List returns all candidates ordered by creation time descending.
func (r *CandidateRepos) List(ctx context.Context) ([]models.Candidate, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, name, email, created_at FROM candidates ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []models.Candidate
	for rows.Next() {
		var c models.Candidate
		if err := rows.Scan(&c.ID, &c.Name, &c.Email, &c.CreatedAt); err != nil {
			return nil, err
		}
		candidates = append(candidates, c)
	}
	return candidates, nil
}

// Get fetches a single candidate by id.
func (r *CandidateRepos) Get(ctx context.Context, id string) (*models.Candidate, error) {
	var c models.Candidate
	err := r.pool.QueryRow(ctx,
		`SELECT id, name, email, created_at FROM candidates WHERE id = $1`, id).
		Scan(&c.ID, &c.Name, &c.Email, &c.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &c, nil
}
