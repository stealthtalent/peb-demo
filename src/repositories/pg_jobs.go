package repositories

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stealthtalent/peb-demo/src/models"
)

type pgJobRepo struct {
	pool *pgxpool.Pool
}

func NewPGJobRepository(pool *pgxpool.Pool) JobRepository {
	return &pgJobRepo{pool: pool}
}

func (r *pgJobRepo) Create(ctx context.Context, j *models.Job) error {
	j.ID = fmt.Sprintf("job_%d", time.Now().UnixNano())
	j.CreatedAt = time.Now()
	_, err := r.pool.Exec(ctx,
		`INSERT INTO jobs (id, title, created_at) VALUES ($1, $2, $3)`,
		j.ID, j.Title, j.CreatedAt)
	return err
}

func (r *pgJobRepo) List(ctx context.Context) ([]models.Job, error) {
	rows, err := r.pool.Query(ctx, `SELECT id, title, created_at FROM jobs ORDER BY created_at DESC`)
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

func (r *pgJobRepo) Get(ctx context.Context, id string) (*models.Job, error) {
	var j models.Job
	err := r.pool.QueryRow(ctx,
		`SELECT id, title, created_at FROM jobs WHERE id = $1`, id).
		Scan(&j.ID, &j.Title, &j.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &j, nil
}
