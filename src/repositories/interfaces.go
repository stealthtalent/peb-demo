package repositories

import (
	"context"

	"github.com/stealthtalent/peb-demo/src/models"
)

type JobRepository interface {
	Create(ctx context.Context, j *models.Job) error
	List(ctx context.Context) ([]models.Job, error)
	Get(ctx context.Context, id string) (*models.Job, error)
}

type CandidateRepository interface {
	Create(ctx context.Context, c *models.Candidate) error
	List(ctx context.Context) ([]models.Candidate, error)
	Get(ctx context.Context, id string) (*models.Candidate, error)
}
