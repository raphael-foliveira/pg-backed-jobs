package tasks

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PGXEnqueuer struct {
	DB *pgxpool.Pool
}

func NewPGXEnqueuer(db *pgxpool.Pool) *PGXEnqueuer {
	return &PGXEnqueuer{
		DB: db,
	}
}

func (e *PGXEnqueuer) Enqueue(ctx context.Context, task *Task) error {
	query := `INSERT INTO tasks (type, payload) VALUES ($1, $2)`
	_, err := e.DB.Exec(
		ctx,
		query,
		task.Type,
		task.Payload,
	)
	return err
}
