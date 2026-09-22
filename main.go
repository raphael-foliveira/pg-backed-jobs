package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/raphael-foliveira/pg-backed-jobs/database"
	"github.com/raphael-foliveira/pg-backed-jobs/users"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dbConfig, err := database.LoadConfig()
	if err != nil {
		log.Fatalf("failed to load database config: %v", err)
	}

	db, err := pgxpool.New(ctx, dbConfig.URL)
	if err != nil {
		log.Fatalf("failed to create database pool: %v", err)
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}

	driver := riverpgxv5.New(db)
	migrator, err := rivermigrate.New(driver, nil)
	if err != nil {
		log.Fatalf("failed to create River migrator: %v", err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		log.Fatalf("failed to migrate River schema: %v", err)
	}

	workers := river.NewWorkers()
	usersEnqueuer := &users.Enqueuer{}
	river.AddWorker(workers, &users.CreateUserWorker{Enqueuer: usersEnqueuer})
	river.AddWorker(workers, &users.SendUserEmailWorker{})

	riverClient, err := river.NewClient(driver, &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		Workers:     workers,
		MaxAttempts: 4,
	})
	if err != nil {
		log.Fatalf("failed to create River client: %v", err)
	}

	usersEnqueuer.Client = riverClient

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	if err := riverClient.Start(runCtx); err != nil {
		log.Fatalf("failed to start River client: %v", err)
	}

	for i := range 150 {
		if err := usersEnqueuer.EnqueueCreateUserTask(ctx, &users.CreateUserRequest{
			Email: fmt.Sprintf("user-%d@email.com", i),
		}); err != nil {
			log.Fatalf("failed to enqueue user %d: %v", i, err)
		}
	}

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := riverClient.Stop(shutdownCtx); err != nil {
		log.Printf("failed to stop River client: %v", err)
	}
}
