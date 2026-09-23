package tasks

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/raphael-foliveira/pg-backed-jobs/database"
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
	return database.Tx(ctx, e.DB, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, "INSERT INTO tasks (type, payload) VALUES ($1, $2) RETURNING id", task.Type, task.Payload)
		if err != nil {
			return fmt.Errorf("failed to insert task: %w", err)
		}

		id, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
		if err != nil {
			return fmt.Errorf("failed to retrieve task ID: %w", err)
		}

		if _, err := tx.Exec(ctx, "SELECT pg_notify('task_notification', $1);", formatInt(id)); err != nil {
			return fmt.Errorf("failed to send notification: %w", err)
		}

		return nil
	})
}

func formatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}
