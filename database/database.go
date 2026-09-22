package database

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, databaseUrl string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseUrl)
}

func NewConnection(ctx context.Context, databaseUrl string) (*pgxpool.Pool, error) {
	return pgxpool.New(ctx, databaseUrl)
}
