package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/raphael-foliveira/pg-backed-jobs/database"
	"github.com/raphael-foliveira/pg-backed-jobs/tasks"
	"github.com/raphael-foliveira/pg-backed-jobs/users"
)

func main() {
	ctx := context.Background()
	dbConfig, err := database.LoadConfig()
	if err != nil {
		log.Fatalf("failed to load database config: %v", err)
	}

	db, err := pgxpool.New(ctx, dbConfig.URL)
	if err != nil {
		log.Fatalf("failed to start database connection %v:", err)
	}

	listenConn, err := pgx.Connect(ctx, dbConfig.URL)
	if err != nil {
		log.Fatalf("failed to start database connection %v:", err)
	}

	go func() {
		handler := func(s string) {
			log.Printf("Received notification: %s", s)
		}
		if err := database.ListenNotification(
			ctx,
			listenConn,
			"chat_messages",
			handler,
		); err != nil {
			log.Println("failed to listen for notifications:", err)
			_ = listenConn.Close(ctx)
		}
	}()

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

	for i := range 150 {
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
		lease_until TIMESTAMPTZ,
		started_at TIMESTAMPTZ,
		finished_at TIMESTAMPTZ,
		error TEXT
		);

		CREATE INDEX IF NOT EXISTS idx_tasks_status_available_at ON tasks (status, available_at);
		`)
	return err
}
