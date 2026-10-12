package database

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func AssertTables(ctx context.Context, db *pgxpool.Pool) error {
	createTableErr := func(tableName string, err error) error {
		return fmt.Errorf("failed to create table %s: %w", tableName, err)
	}

	return Tx(ctx, db, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS
		tasks (
			id BIGSERIAL PRIMARY KEY,
			status TEXT NOT NULL DEFAULT 'pending',
			type TEXT NOT NULL,
			payload JSONB NOT NULL,
			retries INT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			lease_until TIMESTAMPTZ,
			started_at TIMESTAMPTZ,
			finished_at TIMESTAMPTZ,
			error TEXT
			);
			
			CREATE INDEX IF NOT EXISTS idx_tasks_status_available_at ON tasks (status, available_at);
			`)
		if err != nil {
			return createTableErr("tasks", err)
		}
		return nil
	})
}
