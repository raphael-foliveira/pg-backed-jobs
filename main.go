package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/raphael-foliveira/pg-backed-jobs/tasks"
	"github.com/raphael-foliveira/pg-backed-jobs/users"
)

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, "postgres://postgres:postgres@localhost:5432/postgres")
	if err != nil {
		log.Fatalf("failed to start database connection %v", err)
	}

	if err := assertTasksTable(ctx, db); err != nil {
		log.Fatalf("failed to assert tasks table %v", err)
	}

	tasksServer := tasks.NewPGXServer(db)

	taskEnqueuer := tasks.NewPGXEnqueuer(db)

	usersEnqueuer := &users.Enqueuer{
		Enqueuer: taskEnqueuer,
	}

	usersTaskHandler := &users.TaskHandler{
		Enqueuer: usersEnqueuer,
	}

	tasksServer.RegisterHandler(
		users.TaskCreateUser,
		usersTaskHandler.HandleCreateUserTask,
	)
	tasksServer.RegisterHandler(
		users.TaskSendUserEmail,
		usersTaskHandler.HandleSendUserEmailTask,
	)

	forever := make(chan struct{})

	go func() {
		if err := tasksServer.Run(ctx); err != nil {
			log.Fatal(err)
		}
	}()

	for i := range 1500 {
		go func() {
			_ = usersEnqueuer.EnqueueCreateUserTask(ctx, &users.CreateUserRequest{
				Email: fmt.Sprintf("user-%d@email.com", i),
			})
		}()
	}

	<-forever
}

func assertTasksTable(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS
		tasks (
		id BIGSERIAL PRIMARY KEY,
		status TEXT NOT NULL DEFAULT 'pending',
		type TEXT NOT NULL,
		payload JSONB NOT NULL,
		retries INT NOT NULL DEFAULT 0,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		lease_until TIMESTAMPTZ
		);

		CREATE INDEX IF NOT EXISTS idx_tasks_status_available_at ON tasks (status, available_at);
		`)
	return err
}
