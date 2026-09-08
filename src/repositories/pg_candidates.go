package repositories

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stealthtalent/peb-demo/src/models"
)

type pgCandidateRepo struct {
	pool *pgxpool.Pool
}

func NewPGCandidateRepository(pool *pgxpool.Pool) CandidateRepository {
	return &pgCandidateRepo{pool: pool}
}

func (r *pgCandidateRepo) Create(ctx context.Context, c *models.Candidate) error {
	c.ID = fmt.Sprintf("cand_%d", time.Now().UnixNano())
	c.CreatedAt = time.Now()
	_, err := r.pool.Exec(ctx,
		`INSERT INTO candidates (id, name, email, created_at) VALUES ($1, $2, $3, $4)`,
		c.ID, c.Name, c.Email, c.CreatedAt)
	return err
}

func (r *pgCandidateRepo) List(ctx context.Context) ([]models.Candidate, error) {
	rows, err := r.pool.Query(ctx, `SELECT id, name, email, created_at FROM candidates ORDER BY created_at DESC`)
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

func (r *pgCandidateRepo) Get(ctx context.Context, id string) (*models.Candidate, error) {
	var c models.Candidate
	err := r.pool.QueryRow(ctx,
		`SELECT id, name, email, created_at FROM candidates WHERE id = $1`, id).
		Scan(&c.ID, &c.Name, &c.Email, &c.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &c, nil
}
